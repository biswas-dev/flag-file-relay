package main

// events.go: analytics events from the SDKs (POST /bulk) are merged per SDK
// key and forwarded upstream in one request per flush interval, instead of
// one request per SDK process. Diagnostic events are forwarded one by one.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// Memory budget (pod limit 128 MiB): event bodies being read <= 24 MiB,
// batched events <= 4 MiB per key (MAX_KEYS 8 => 32 MiB), hand-offs to the
// primary in flight <= 16 MiB.
const (
	maxEventBody           = 4 << 20 // one SDK request
	maxEventBodiesInFlight = 24 << 20
	flushAtBytes           = 512 << 10
	maxBufferedPerKey      = 4 << 20
	maxHandoffBytes        = 16 << 20
	maxEventsTotal         = 32 << 20 // batched + being posted, all keys
	eventOverhead          = 48       // per parsed element (slice header etc.)
)

type bucketKey struct{ key, schema string }

type bucket struct {
	events []json.RawMessage
	bytes  int
}

type eventForwarder struct {
	r       *relay
	mu      sync.Mutex
	buckets map[bucketKey]*bucket
	seen    map[string]time.Time // recent incoming X-LaunchDarkly-Payload-IDs (SDK retries)
	perKey  map[string]int       // buffered bytes per SDK key, across schemas
	fwdSem  chan struct{}        // followers: in-flight hand-offs to the primary
	handoff int                  // bytes held by in-flight hand-offs (under mu)
	total   int                  // bytes batched or being posted, all keys (under mu)
	wake    chan struct{}
	diagSem chan struct{}
	wg      sync.WaitGroup
}

func newEventForwarder(r *relay) *eventForwarder {
	return &eventForwarder{r: r, buckets: map[bucketKey]*bucket{}, seen: map[string]time.Time{}, perKey: map[string]int{},
		wake: make(chan struct{}, 1), diagSem: make(chan struct{}, 4), fwdSem: make(chan struct{}, 64)}
}

// add queues one /bulk body. It returns false when the body is unusable. A
// payload id seen in the last 10 minutes is an SDK retry of a request we
// already accepted, and is dropped so the events are not counted twice.
func (f *eventForwarder) add(key, schema, payloadID string, body []byte) bool {
	if !isJSONArray(body) {
		return false
	}
	f.mu.Lock()
	if payloadID != "" {
		seenKey := keyHash(key)[:16] + "/" + payloadID
		if t, dup := f.seen[seenKey]; dup && time.Since(t) < 10*time.Minute {
			f.mu.Unlock()
			return true
		}
		if len(f.seen) >= 8192 {
			for k, t := range f.seen {
				if time.Since(t) > 10*time.Minute || len(f.seen) >= 8192 {
					delete(f.seen, k)
				}
			}
		}
		f.seen[seenKey] = time.Now()
	}
	bk := bucketKey{key, schema}
	b := f.buckets[bk]
	if b == nil {
		b = &bucket{}
		f.buckets[bk] = b
	}
	// Decode one element at a time and admit each against the budgets before
	// keeping it, so a large array of tiny events cannot allocate past them.
	dec := json.NewDecoder(bytes.NewReader(body))
	_, _ = dec.Token() // '['
	dropped := 0
	for dec.More() {
		var ev json.RawMessage
		if err := dec.Decode(&ev); err != nil {
			break
		}
		cost := len(ev) + eventOverhead
		if f.perKey[key]+cost > maxBufferedPerKey || f.total+cost > maxEventsTotal {
			dropped++
			continue
		}
		b.events = append(b.events, ev)
		b.bytes += len(ev) + 1
		f.perKey[key] += cost
		f.total += cost
	}
	if dropped > 0 {
		f.r.metrics.add("flagrelay_events_dropped_total", float64(dropped))
	}
	full := b.bytes >= flushAtBytes
	f.mu.Unlock()
	if full {
		select {
		case f.wake <- struct{}{}:
		default:
		}
	}
	return true
}

func (f *eventForwarder) run(ctx context.Context) {
	t := time.NewTicker(f.r.cfg.eventsFlush)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-f.wake:
		}
		f.flush(ctx)
	}
}

// flush sends everything buffered. Safe to call concurrently with add.
func (f *eventForwarder) flush(ctx context.Context) {
	f.mu.Lock()
	buckets := f.buckets
	f.buckets = map[bucketKey]*bucket{}
	f.perKey = map[string]int{}
	f.mu.Unlock()
	for bk, b := range buckets {
		if len(b.events) == 0 {
			continue
		}
		var buf bytes.Buffer
		buf.Grow(b.bytes + 2)
		buf.WriteByte('[')
		for i, ev := range b.events {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.Write(ev)
		}
		buf.WriteByte(']')
		err := f.post(ctx, "/bulk", bk.key, bk.schema, buf.Bytes())
		f.mu.Lock()
		f.total -= b.bytes - len(b.events) + len(b.events)*eventOverhead // released only once posted
		if f.total < 0 {
			f.total = 0
		}
		f.mu.Unlock()
		if err != nil {
			f.r.metrics.add("flagrelay_events_dropped_total", float64(len(b.events)))
			log.Printf("events: dropped %d events: %v", len(b.events), err)
			continue
		}
		f.r.metrics.add("flagrelay_events_forwarded_total", float64(len(b.events)))
	}
}

// toPrimary (followers) hands one SDK /bulk payload to the primary relay
// unchanged, keeping the SDK's payload id so the primary can de-duplicate a
// retry that reached both replicas. If the primary cannot take it, the events
// go into this replica's own upstream batch instead.
func (f *eventForwarder) toPrimary(key, schema, payloadID string, body []byte) bool {
	if !isJSONArray(body) {
		return false
	}
	f.mu.Lock()
	over := f.handoff+len(body) > maxHandoffBytes
	if !over {
		f.handoff += len(body)
	}
	f.mu.Unlock()
	if over {
		return f.add(key, schema, payloadID, body) // hand-off budget used up
	}
	select {
	case f.fwdSem <- struct{}{}:
	default:
		f.mu.Lock()
		f.handoff -= len(body)
		f.mu.Unlock()
		return f.add(key, schema, payloadID, body) // primary hand-off saturated
	}
	f.wg.Add(1)
	go func() {
		defer func() {
			<-f.fwdSem
			f.mu.Lock()
			f.handoff -= len(body)
			f.mu.Unlock()
			f.wg.Done()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.r.cfg.primaryURI+"/bulk", bytes.NewReader(body))
		if err == nil {
			req.Header.Set("Authorization", key)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(hdrPeer, "1")
			if schema != "" {
				req.Header.Set("X-LaunchDarkly-Event-Schema", schema)
			}
			if payloadID != "" {
				req.Header.Set("X-LaunchDarkly-Payload-ID", payloadID)
			}
			var resp *http.Response
			if resp, err = f.r.httpClient.Do(req); err == nil {
				io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
				resp.Body.Close()
				if resp.StatusCode < 300 {
					return
				}
			}
		}
		f.add(key, schema, payloadID, body)
	}()
	return true
}

// diagnostic forwards one diagnostic event asynchronously, best effort.
func (f *eventForwarder) diagnostic(key string, body []byte) {
	select {
	case f.diagSem <- struct{}{}:
	default:
		// already 4 in flight: diagnostics are not worth queueing
		f.r.metrics.inc("flagrelay_diagnostics_dropped_total")
		return
	}
	f.wg.Add(1)
	go func() {
		defer func() { <-f.diagSem; f.wg.Done() }()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := f.post(ctx, "/diagnostic", key, "", body); err != nil {
			f.r.metrics.inc("flagrelay_diagnostics_dropped_total")
		}
	}()
}

// post sends with one retry after 1 s on a network error or 5xx, as the LD
// SDKs do. The payload id stays the same across the retry so upstream can
// de-duplicate.
func (f *eventForwarder) post(ctx context.Context, path, key, schema string, body []byte) error {
	payloadID := newUUID()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		req, err := http.NewRequestWithContext(rctx, http.MethodPost, f.r.cfg.eventsURI+path, bytes.NewReader(body))
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("Authorization", key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", userAgent())
		if schema != "" {
			req.Header.Set("X-LaunchDarkly-Event-Schema", schema)
		}
		if path == "/bulk" {
			req.Header.Set("X-LaunchDarkly-Payload-ID", payloadID)
		}
		resp, err := f.r.httpClient.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		cancel()
		if resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		if resp.StatusCode < 500 && resp.StatusCode != 429 && resp.StatusCode != 408 {
			return lastErr // not retryable
		}
	}
	return lastErr
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// isJSONArray checks syntax without materializing the elements.
func isJSONArray(b []byte) bool {
	t := bytes.TrimSpace(b)
	return len(t) > 0 && t[0] == '[' && json.Valid(t)
}
