package main

// env.go: one relayed environment = one SDK key.
//
// Topology. One replica owns the upstream for every key (the "primary",
// StatefulSet pod 0): it streams from flagtgl (or LaunchDarkly). Every other
// replica is a follower: it streams from the primary over the same
// LaunchDarkly protocol, and only falls back to flagtgl directly while the
// primary is unreachable. So an environment normally holds ONE upstream
// stream per key, and all replicas serve the same generation.
//
// Freshness. Every data set carries confirmedAt: the last time the chain to
// upstream confirmed it (an upstream put/patch/poll or keepalive). A replica
// that is not connected to its upstream chain (upstream down, restarted
// offline) adopts a peer's copy only when that copy was confirmed later than
// its own, and always wholesale (never a per-flag merge, so a flag deleted and
// re-created upstream cannot be mixed with its old incarnation).
//
// Durability. Each change is written and fsynced before it is published; a
// failing disk is retried, and only after that is the change published anyway
// (an OFF must not be held back by a full disk), with an alert metric and a
// background retry.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	errUpstreamAuth   = errors.New("upstream rejected the SDK key")
	errPeerStale      = errors.New("primary holds older data than this replica")
	errStaleSnapshot  = errors.New("stream snapshot superseded while connecting")
	errMalformedPut   = errors.New("malformed put")
	errSwitchUpstream = errors.New("switching upstream")
)

// Stream protocols served downstream.
const (
	protoAll   = iota // GET /all: {flags, segments}
	protoFlags        // GET /flags: legacy, flags only, paths "/<key>"
)

// Peer protocol headers (relay <-> relay only; SDKs never send or need them).
const (
	hdrPeer        = "X-Flag-Relay-Peer"
	hdrConfirmedAt = "X-Flag-Relay-Confirmed-At" // unix ms
	hdrUpstream    = "X-Flag-Relay-Upstream"     // connected | disconnected
)

type subscriber struct {
	proto    int
	peer     bool
	ch       chan []byte
	kick     chan struct{}
	kickOnce sync.Once
}

func (s *subscriber) close() { s.kickOnce.Do(func() { close(s.kick) }) }

type env struct {
	r       *relay
	key     string // in memory only, never logged or persisted
	hash    string // hex sha256(key)
	id      string // first 8 hex of hash, used in logs and metrics
	created time.Time

	commitMu sync.Mutex // serializes changes (disk write happens under it)

	mu          sync.Mutex // guards the fields below; never held across I/O
	data        *dataSet
	payload     []byte // live items as {flags, segments}
	etag        string
	view        *dataSet          // what SDKs are served: data with the local overrides (== data when none)
	viewPayload []byte            // view as {flags, segments}
	viewEtag    string            //
	bump        map[string]uint64 // per-flag version offsets keeping served versions increasing (overrides.go)
	source      string            // "upstream" | "primary" | "disk" | "peer"
	gen         uint64            // bumped on every published change
	extEpoch    uint64            // bumped by every change published by a poll or a peer copy
	authority   bool              // direct stream open AND its initial put committed
	chainOK     bool              // the upstream chain is healthy: authority, or a primary that is itself authoritative
	confirmedAt time.Time
	savedAt     time.Time
	persistErr  string
	subs        map[*subscriber]struct{}
	connected   bool   // a stream to the upstream chain is open and accepted
	via         string // "direct" | "primary" while connected
	primaryDown time.Time
	failures    int
	triedOnce   bool
	authFailed  bool
	peersTried  bool
	lastErr     string

	changed chan struct{}
	switchC chan struct{} // asks the stream loop to reconnect (back to primary)
	cancel  context.CancelFunc
}

func newEnv(r *relay, key, hash string, disk *diskEntry) *env {
	e := &env{
		r: r, key: key, hash: hash, id: hash[:8], created: time.Now(),
		subs:    map[*subscriber]struct{}{},
		changed: make(chan struct{}),
		switchC: make(chan struct{}, 1),
	}
	if disk != nil {
		e.setDataLocked(disk.data, "disk")
		e.savedAt, e.confirmedAt = disk.savedAt, disk.confirmedAt
		if disk.dirty {
			e.persistErr = "start-up peer copy not yet written" // persistRetryLoop writes it
		}
	}
	e.rebuildViewLocked(nil, true)
	return e
}

func (e *env) notifyLocked() {
	close(e.changed)
	e.changed = make(chan struct{})
}

func (e *env) setDataLocked(ds *dataSet, source string) {
	e.data = ds
	e.payload = ds.marshal()
	e.etag = etagOf(e.payload)
	e.source = source
}

// rebuildViewLocked recomputes the view served to SDKs from the data and the
// current overrides. full: a put of the whole view goes to every SDK stream
// next, so the version offsets start over; otherwise keys are the flags that
// may have changed, and each is kept at a higher version than the SDKs hold.
func (e *env) rebuildViewLocked(keys []string, full bool) {
	ovs := e.r.overrides.snapshot()
	if full {
		e.bump = nil
		e.view = buildView(e.data, ovs, nil)
	} else {
		e.view, e.bump = monotonic(e.view, e.data, ovs, e.bump, keys)
	}
	switch {
	case e.view == e.data:
		e.viewPayload, e.viewEtag = e.payload, e.etag
	case e.view == nil:
		e.viewPayload, e.viewEtag = nil, ""
	default:
		e.viewPayload = e.view.marshal()
		e.viewEtag = etagOf(e.viewPayload)
	}
}

// overridesChanged re-serves keys after an override change: SDK streams get a
// patch (or delete) per flag whose served form changed; peers see nothing
// (they only ever get the source data).
func (e *env) overridesChanged(keys []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	old := e.view
	sdk := e.refreshViewLocked(keys)
	if old != nil && (sdk[0] != nil || sdk[1] != nil) {
		e.broadcastLocked([2][]byte{}, sdk)
	}
	e.notifyLocked() // an env without data may now be servable (override only)
}

// refreshViewLocked rebuilds the view after a change and returns the SDK
// events for every flag whose served form changed: the keys in must, and any
// other overridden (or version-offset) flag, since the rebuild also takes in
// override changes made concurrently, whose own notification then finds
// nothing left to announce.
func (e *env) refreshViewLocked(must []string) [2][]byte {
	old := e.view
	keys := overlayKeys(must, e.r.overrides.snapshot(), e.bump)
	e.rebuildViewLocked(keys, false)
	return diffMsgs(old, e.view, keys, must)
}

func (e *env) isFollower() bool { return e.r.cfg.primaryURI != "" }

func (e *env) start(ctx context.Context) {
	ctx, e.cancel = context.WithCancel(ctx)
	go e.streamLoop(ctx)
	go e.reconcileLoop(ctx)
	if e.r.peers != nil {
		go e.peerLoop(ctx)
	}
	if e.isFollower() {
		go e.primaryWatch(ctx)
	}
	if e.r.cfg.cacheDir != "" {
		go e.persistRetryLoop(ctx)
	}
}

// ── commit pipeline ─────────────────────────────────────────────────────────

// dsChange says which downstream event a commit sends: a full put, one item
// (patch, or delete when it is a tombstone), or nothing (zero value).
type dsChange struct {
	put       bool
	kind, key string
	it        item
}

// commit publishes next as the env's data: disk first (retried), then memory
// and subscribers. Caller holds commitMu. ch says which event goes
// downstream. fences marks a change (poll or peer copy) that a stream's
// initial put must not undo.
func (e *env) commit(next *dataSet, source string, confirmed time.Time, fences bool, ch dsChange) {
	// The confirmation stored on disk and the one published are the same:
	// never older than what memory already holds (keepalives also take
	// commitMu, so nothing can move it while this commit runs).
	e.mu.Lock()
	if e.confirmedAt.After(confirmed) {
		confirmed = e.confirmedAt
	}
	e.mu.Unlock()
	if e.r.cfg.cacheDir != "" {
		var err error
		fl := e.r.fileLock(e.hash)
		fl.Lock()
		for attempt := 0; attempt < 3; attempt++ {
			if err = writeCacheFile(e.r.cfg.cacheDir, e.hash, next.marshalWithTombstones(), source, confirmed); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		fl.Unlock()
		e.mu.Lock()
		if err != nil {
			e.persistErr = err.Error()
			e.r.metrics.inc("flagrelay_cache_write_errors_total")
			log.Printf("env %s: cache write failed, publishing anyway and retrying in the background: %v", e.id, err)
		} else {
			e.persistErr = ""
			e.savedAt = time.Now()
		}
		e.mu.Unlock()
	}
	// Overrides whose flag the source has changed since are dropped (last
	// write wins), and the change is served, under the override store's lock
	// (see observe).
	dropped := e.r.overrides.observe(e.hash, next, func(dropped []string) { e.publishLocked(next, source, confirmed, fences, ch, dropped) })
	if len(dropped) > 0 {
		e.r.overridesChanged(dropped, e) // every other environment serves the source again too
	}
}

// publishLocked makes next the served data and sends the downstream events.
// Called by commit from inside overrideStore.observe.
func (e *env) publishLocked(next *dataSet, source string, confirmed time.Time, fences bool, ch dsChange, dropped []string) {
	e.mu.Lock()
	e.gen++
	if fences {
		e.extEpoch++
	}
	plain := e.view == e.data && len(e.bump) == 0 // nothing overlaid before this change
	e.setDataLocked(next, source)
	e.confirmedAt = confirmed
	var raw, sdk [2][]byte
	switch {
	case ch.put:
		raw = e.putMsgs()
		e.rebuildViewLocked(nil, true)
		sdk = raw
		if e.view != e.data {
			sdk = putMsgsFor(e.viewPayload, e.view)
		}
	case ch.key != "":
		if ch.it.Deleted {
			raw = deleteMsgs(ch.kind, ch.key, ch.it.Version)
		} else {
			raw = patchMsgs(ch.kind, ch.key, ch.it.Raw)
		}
		keys := dropped
		if ch.kind == "flags" {
			keys = []string{ch.key}
			for _, k := range dropped {
				if k != ch.key {
					keys = append(keys, k)
				}
			}
		}
		msgs := e.refreshViewLocked(keys)
		switch {
		case plain && e.view == e.data:
			sdk = raw
		case ch.kind != "flags":
			sdk = concatMsgs(raw, msgs)
		default:
			sdk = msgs
		}
	default:
		sdk = e.refreshViewLocked(dropped)
	}
	e.broadcastLocked(raw, sdk)
	e.notifyLocked()
	e.mu.Unlock()
}

// persistRetryLoop rewrites the current data while the last write failed.
func (e *env) persistRetryLoop(ctx context.Context) {
	t := time.NewTicker(e.r.cfg.persistRetry)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// Hold the commit lock for the whole read-write-status sequence so a
		// newer commit can neither be overwritten nor have its error cleared.
		e.commitMu.Lock()
		e.mu.Lock()
		failed, ds, src, conf := e.persistErr != "", e.data, e.source, e.confirmedAt
		e.mu.Unlock()
		if failed && ds != nil {
			fl := e.r.fileLock(e.hash)
			fl.Lock()
			err := writeCacheFile(e.r.cfg.cacheDir, e.hash, ds.marshalWithTombstones(), src, conf)
			fl.Unlock()
			e.mu.Lock()
			if err == nil {
				e.persistErr, e.savedAt = "", time.Now()
				log.Printf("env %s: cache write recovered", e.id)
			}
			e.mu.Unlock()
		}
		e.commitMu.Unlock()
	}
}

// ── upstream chain ──────────────────────────────────────────────────────────

type target struct {
	base string // SDK endpoint base URL
	peer bool   // true = the primary relay
}

func (e *env) pickTarget() target {
	if e.isFollower() {
		e.mu.Lock()
		down := time.Since(e.primaryDown) < e.r.cfg.primaryRetry
		e.mu.Unlock()
		if !down {
			return target{base: e.r.cfg.primaryURI, peer: true}
		}
	}
	return target{base: e.r.cfg.streamURI}
}

func (e *env) streamLoop(ctx context.Context) {
	const base, max = time.Second, 30 * time.Second
	delay := base
	staleRetries := 0
	for ctx.Err() == nil {
		t := e.pickTarget()
		began := time.Now()
		err := e.streamOnce(ctx, t)
		if ctx.Err() != nil {
			return
		}
		auth := errors.Is(err, errUpstreamAuth)
		e.mu.Lock()
		e.connected, e.via, e.authority, e.chainOK = false, "", false, false
		if !t.peer {
			e.triedOnce = true // a primary failure fails over at once; only direct counts
		}
		if err != nil {
			e.lastErr = err.Error()
		}
		hasData := e.data != nil
		if auth && !hasData && !t.peer {
			e.authFailed = true
		}
		if t.peer && err != nil && !errors.Is(err, errStaleSnapshot) && !errors.Is(err, errSwitchUpstream) {
			e.primaryDown = time.Now() // next attempt goes direct
		}
		idle := !hasData && len(e.subs) == 0 && time.Since(e.created) > e.r.cfg.idleExpiry
		e.notifyLocked()
		e.mu.Unlock()

		switch {
		case errors.Is(err, errSwitchUpstream):
			continue // primary is back: reconnect to it now
		case errors.Is(err, errStaleSnapshot) && staleRetries < 3:
			staleRetries++
			continue
		case t.peer && err != nil && !errors.Is(err, errStaleSnapshot):
			log.Printf("env %s: primary relay unavailable (%v); streaming from upstream directly until it is back", e.id, err)
			continue // fail over to direct at once
		}
		staleRetries = 0
		e.r.metrics.inc("flagrelay_upstream_reconnects_total", "env", e.id)
		if auth {
			e.r.metrics.inc("flagrelay_upstream_errors_total", "env", e.id, "kind", "auth")
			if !hasData {
				e.r.forget(e)
				return
			}
			log.Printf("env %s: upstream rejected the key; keeping the cached data and retrying", e.id)
		} else if err != nil {
			e.r.metrics.inc("flagrelay_upstream_errors_total", "env", e.id, "kind", "stream")
			log.Printf("env %s: upstream stream ended: %v", e.id, err)
		}
		if idle {
			// A key nobody uses and nobody has data for must not hold a
			// MAX_KEYS slot through a long outage.
			e.r.forget(e)
			return
		}
		if time.Since(began) > time.Minute {
			delay = base
		}
		sleep := delay/2 + time.Duration(rand.Int63n(int64(delay)))
		select {
		case <-ctx.Done():
			return
		case <-e.switchC:
		case <-time.After(sleep):
		}
		if delay *= 2; delay > max {
			delay = max
		}
	}
}

// acceptPeer decides whether data from the primary may replace ours: always
// when the primary is connected to upstream; otherwise only when its copy is
// at least as fresh as ours.
func (e *env) acceptPeer(h http.Header) bool {
	if h.Get(hdrUpstream) == "connected" {
		return true
	}
	ms, _ := strconv.ParseInt(h.Get(hdrConfirmedAt), 10, 64)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.data == nil || !time.UnixMilli(ms).Before(e.confirmedAt)
}

func peerConfirmed(h http.Header) time.Time {
	ms, _ := strconv.ParseInt(h.Get(hdrConfirmedAt), 10, 64)
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func (e *env) streamOnce(ctx context.Context, t target) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.base+"/all", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", e.key)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", userAgent())
	if t.peer {
		req.Header.Set(hdrPeer, "1")
	}
	// Every full put on this stream is refused once a poll or a peer copy has
	// been published after the stream connected: events on the stream cannot
	// acknowledge such a change, only a reconnect (a fresh put) can.
	e.mu.Lock()
	ackEpoch := e.extEpoch
	e.mu.Unlock()
	resp, err := e.r.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return errUpstreamAuth
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("stream from %s: HTTP %d", targetName(t), resp.StatusCode)
	}
	if t.peer && !e.acceptPeer(resp.Header) {
		return errPeerStale
	}
	// Freshness from the primary is held as "pending" until the event it
	// describes has been committed: ":relay-next <ms> <state>" precedes every
	// event (and the response headers describe the initial put), while
	// ":relay-confirmed <ms> <state>" keepalives describe data already sent.
	pendingAt, primaryOK := peerConfirmed(resp.Header), resp.Header.Get(hdrUpstream) == "connected"
	confirmed := func() time.Time { return time.Now() }
	if t.peer {
		confirmed = func() time.Time { return pendingAt }
	}

	first := true
	timer := time.AfterFunc(e.r.cfg.readTimeout, cancel)
	defer timer.Stop()
	rd := newSSEReader(resp.Body, func() { timer.Reset(e.r.cfg.readTimeout) })
	rd.onComment = func(c string) {
		if !t.peer {
			// An upstream keepalive confirms our data only once this stream's
			// initial put is in; before that we still hold the old copy.
			if !first {
				e.touch(time.Now())
			}
			return
		}
		f := strings.Fields(c)
		if len(f) != 3 || (f[0] != "relay-confirmed" && f[0] != "relay-next") {
			return
		}
		ms, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			return
		}
		ok := f[2] == "connected"
		if f[0] == "relay-next" {
			pendingAt, primaryOK = time.UnixMilli(ms), ok
			return
		}
		e.touch(time.UnixMilli(ms)) // describes data we already committed
		e.mu.Lock()
		if !first {
			e.chainOK = ok
		}
		e.mu.Unlock()
	}

	e.mu.Lock()
	e.connected, e.via = true, targetName(t)
	e.mu.Unlock()
	if !t.peer {
		e.r.metrics.set("flagrelay_upstream_connected", 1, "env", e.id)
		defer e.r.metrics.set("flagrelay_upstream_connected", 0, "env", e.id)
	}

	// A direct stream of a follower is dropped as soon as the primary is back.
	var switched bool
	var switchMu sync.Mutex
	if !t.peer && e.isFollower() {
		go func() {
			select {
			case <-e.switchC:
				switchMu.Lock()
				switched = true
				switchMu.Unlock()
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	for {
		ev, err := rd.next()
		if err != nil {
			switchMu.Lock()
			sw := switched
			switchMu.Unlock()
			if sw {
				return errSwitchUpstream
			}
			if err == io.EOF {
				return fmt.Errorf("%s closed the stream", targetName(t))
			}
			return err
		}
		src := "upstream"
		if t.peer {
			src = "primary"
		}
		// Every mutating event (put, patch, delete) is fenced: once a poll or
		// peer copy was published after this stream connected, the stream is
		// superseded and reconnects before anything else it sends applies.
		fence := ackEpoch + 1
		err = e.handleUpstreamEvent(ctx, ev, src, confirmed(), fence, !t.peer || primaryOK)
		if errors.Is(err, errStaleSnapshot) || errors.Is(err, errPeerStale) {
			return err // drop the stream before any later event applies
		}
		if err == nil {
			e.mu.Lock()
			if first && ev.Name == "put" {
				first = false
				if !t.peer {
					e.authority = true // only now may followers trust us as "connected"
				}
			}
			if !first {
				e.chainOK = !t.peer || primaryOK
			}
			e.mu.Unlock()
		}
		if err != nil {
			e.r.metrics.inc("flagrelay_upstream_errors_total", "env", e.id, "kind", "event")
			log.Printf("env %s: bad %q event from %s: %v", e.id, ev.Name, targetName(t), err)
			if errors.Is(err, errMalformedPut) {
				return err
			}
		}
	}
}

func targetName(t target) string {
	if t.peer {
		return "primary"
	}
	return "direct"
}

// touch records that the chain confirmed the current data at time t.
// It takes the commit lock, so a keepalive can never stamp the copy that is
// still published with a newer time while a newer change is being written.
func (e *env) touch(t time.Time) {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	e.touchLocked(t)
}

// touchLocked is touch for callers that already hold commitMu.
func (e *env) touchLocked(t time.Time) {
	e.mu.Lock()
	if t.After(e.confirmedAt) {
		e.confirmedAt = t
	}
	e.mu.Unlock()
}

// fence != 0 marks a full put from a stream: it is refused (errStaleSnapshot)
// when a poll or a peer copy was published after that stream connected
// (external epoch >= fence), checked under the commit lock.
// trusted: the data comes from upstream directly, or from a primary that is
// itself connected upstream; otherwise it only replaces ours when it is at
// least as fresh, checked under the commit lock.
func (e *env) handleUpstreamEvent(ctx context.Context, ev sseEvent, src string, confirmed time.Time, fence uint64, trusted bool) error {
	switch ev.Name {
	case "put":
		var msg struct {
			Path string          `json:"path"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &msg); err != nil {
			return fmt.Errorf("%w: %v", errMalformedPut, err)
		}
		body := msg.Data
		if len(body) == 0 { // older form: the data set itself
			body = json.RawMessage(ev.Data)
		}
		ds, err := parseDataSet(body)
		if err != nil {
			return fmt.Errorf("%w: %v", errMalformedPut, err)
		}
		switch _, verdict := e.applyPutChecked(ds, src, confirmed, false, 0, fence, trusted); verdict {
		case putFenced:
			return errStaleSnapshot
		case putOlderThanOurs:
			return errPeerStale // stop reading this stream before its patches apply
		}
	case "patch":
		var msg struct {
			Path string          `json:"path"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &msg); err != nil {
			return err
		}
		kind, key, ok := kindAndKey(msg.Path)
		if !ok {
			return nil // unknown kind: ignore, as the SDKs do
		}
		it, err := parseItem(kind, msg.Data)
		if err != nil {
			return err
		}
		if !e.applyUpsert(kind, key, it, src, confirmed, fence) {
			return errStaleSnapshot
		}
	case "delete":
		var msg struct {
			Path    string          `json:"path"`
			Version json.RawMessage `json:"version"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &msg); err != nil {
			return err
		}
		version, err := parseU64(msg.Version)
		if err != nil {
			return errors.New("delete without a valid version")
		}
		kind, key, ok := kindAndKey(msg.Path)
		if !ok {
			return nil
		}
		if !e.applyUpsert(kind, key, tombstone(key, version), src, confirmed, fence) {
			return errStaleSnapshot
		}
	case "indirect/put", "indirect/patch":
		return e.pollOnce(ctx)
	}
	return nil
}

// applyPut makes an authoritative full data set current. A poll result is
// dropped when anything was published while it was in flight.
func (e *env) applyPut(ds *dataSet, src string, confirmed time.Time, fromPoll bool, genAtStart uint64, trusted bool) bool {
	changed, _ := e.applyPutChecked(ds, src, confirmed, fromPoll, genAtStart, 0, trusted)
	return changed
}

// Outcome of applyPutChecked besides "applied".
const (
	putApplied       = iota
	putFenced        // a poll or peer copy was published after the stream connected
	putOlderThanOurs // an untrusted copy older than ours
	putDropped       // a poll overtaken by another change while in flight
)

func (e *env) applyPutChecked(ds *dataSet, src string, confirmed time.Time, fromPoll bool, genAtStart, fence uint64, trusted bool) (bool, int) {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	if src == "upstream" {
		confirmed = time.Now() // chosen under the commit lock (see touch)
	}
	e.mu.Lock()
	if fence != 0 && e.extEpoch >= fence {
		e.mu.Unlock()
		return false, putFenced
	}
	if !trusted && e.data != nil && confirmed.Before(e.confirmedAt) {
		// An untrusted copy older than ours (rechecked here, where nothing
		// can advance our confirmation concurrently): keep ours. A stream
		// that sent it is dropped by the caller before its patches apply.
		e.mu.Unlock()
		return false, putOlderThanOurs
	}
	if fromPoll && e.gen != genAtStart {
		e.mu.Unlock()
		return false, putDropped
	}
	cur, curSrc := e.data, e.source
	e.failures, e.triedOnce, e.authFailed = 0, true, false
	e.mu.Unlock()

	if len(ds.invalid) > 0 {
		e.r.metrics.add("flagrelay_invalid_items_total", float64(len(ds.invalid)), "env", e.id)
		log.Printf("env %s: %d malformed item(s) ignored, previous copies kept: %v", e.id, len(ds.invalid), ds.invalid)
	}
	next := ds
	if cur != nil {
		next = cur.replaceFrom(ds)
	}
	changed := cur == nil || !cur.equal(next)
	if !changed && curSrc == src {
		e.touchLocked(confirmed)
		e.mu.Lock()
		e.notifyLocked()
		e.mu.Unlock()
		return false, putApplied
	}
	e.commit(next, src, confirmed, fromPoll, dsChange{put: changed})
	return changed, putApplied
}

// applyUpsert applies a patch or delete; it returns false when the stream
// that sent it has been superseded (fence), so the caller reconnects.
func (e *env) applyUpsert(kind, key string, it item, src string, confirmed time.Time, fence uint64) bool {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	if src == "upstream" {
		confirmed = time.Now()
	}
	e.mu.Lock()
	superseded := fence != 0 && e.extEpoch >= fence
	e.mu.Unlock()
	if superseded {
		return false
	}
	e.mu.Lock()
	cur := e.data
	e.mu.Unlock()
	if cur == nil {
		return true // the protocol always sends a put first
	}
	next := cur.clone()
	if !next.upsert(kind, key, it) {
		e.touchLocked(confirmed)
		return true // not newer than what we hold
	}
	e.commit(next, src, confirmed, false, dsChange{kind: kind, key: key, it: it})
	return true
}

// applyPeerSnapshot adopts a peer's copy wholesale when this replica is not
// connected to its upstream chain and the peer's copy is fresher.
func (e *env) applyPeerSnapshot(ds *dataSet, confirmed time.Time) {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	e.mu.Lock()
	cur, healthy, own := e.data, e.chainOK, e.confirmedAt
	e.mu.Unlock()
	if healthy || (cur != nil && !confirmed.After(own)) {
		return
	}
	next := ds
	if cur != nil {
		next = cur.replaceFrom(ds)
	}
	changed := cur == nil || !cur.equal(next)
	e.commit(next, "peer", confirmed, true, dsChange{put: changed})
	log.Printf("env %s: adopted a fresher copy from a peer (confirmed %s)", e.id, confirmed.UTC().Format(time.RFC3339))
}

// reconcileLoop polls independently of the stream: every reconcileInterval
// while it is up (a stream that keeps its keepalives but missed an update
// still converges), every pollInterval while it is down.
func (e *env) reconcileLoop(ctx context.Context) {
	for {
		e.mu.Lock()
		connected, failed := e.connected, e.authFailed
		e.mu.Unlock()
		base := e.r.cfg.reconcileInterval
		if !connected {
			base = e.r.cfg.pollInterval
		}
		wait := base + time.Duration(rand.Int63n(int64(base/4)+1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if failed {
			continue
		}
		if err := e.pollOnce(ctx); err != nil {
			e.r.metrics.inc("flagrelay_upstream_errors_total", "env", e.id, "kind", "poll")
		}
	}
}

func (e *env) pollOnce(ctx context.Context) error {
	t := e.pickTarget()
	base := t.base
	if !t.peer {
		base = e.r.cfg.pollURI
	}
	e.mu.Lock()
	gen := e.gen
	e.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/sdk/latest-all", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", e.key)
	req.Header.Set("User-Agent", userAgent())
	if t.peer {
		req.Header.Set(hdrPeer, "1")
	}
	resp, err := e.r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("poll %s: HTTP %d", targetName(t), resp.StatusCode)
	}
	if t.peer && !e.acceptPeer(resp.Header) {
		return errPeerStale
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDataSet))
	if err != nil {
		return err
	}
	ds, err := parseDataSet(body)
	if err != nil {
		return err
	}
	src, confirmed := "upstream", time.Now()
	if t.peer {
		src, confirmed = "primary", peerConfirmed(resp.Header)
	}
	if e.applyPut(ds, src, confirmed, true, gen, !t.peer || resp.Header.Get(hdrUpstream) == "connected") {
		e.r.metrics.inc("flagrelay_reconcile_changes_total", "env", e.id)
		log.Printf("env %s: reconciliation poll found changes the stream had not delivered", e.id)
	}
	return nil
}

// primaryWatch (followers) moves the stream back to the primary once it is
// ready again, so the environment returns to one upstream stream per key.
func (e *env) primaryWatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.r.cfg.primaryRetry):
		}
		e.mu.Lock()
		direct := e.via == "direct"
		e.mu.Unlock()
		if !direct {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		req, _ := http.NewRequestWithContext(rctx, http.MethodGet, e.r.cfg.primaryURI+"/readyz", nil)
		resp, err := e.r.httpClient.Do(req)
		cancel()
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			e.mu.Lock()
			e.primaryDown = time.Time{}
			e.mu.Unlock()
			select {
			case e.switchC <- struct{}{}:
			default:
			}
		}
	}
}

// peerLoop pulls the peers' copies while not connected to the upstream chain.
func (e *env) peerLoop(ctx context.Context) {
	for {
		e.mu.Lock()
		need := !e.chainOK
		e.mu.Unlock()
		if need {
			e.syncPeers(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.r.cfg.pollInterval):
		}
	}
}

func (e *env) syncPeers(ctx context.Context) {
	for _, c := range e.r.peers.fetch(ctx, e.hash) {
		e.applyPeerSnapshot(c.data, c.confirmedAt)
	}
	e.mu.Lock()
	e.peersTried = true
	e.notifyLocked()
	e.mu.Unlock()
}

// ── downstream fan-out ──────────────────────────────────────────────────────

const subscriberQueue = 16

// subscribe registers a stream and returns the put to send first; both
// happen under the lock so no change can fall between them.
func (e *env) subscribe(proto int, peer bool) (s *subscriber, put []byte, confirmed time.Time, upstream string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s = &subscriber{proto: proto, peer: peer, ch: make(chan []byte, subscriberQueue), kick: make(chan struct{})}
	e.subs[s] = struct{}{}
	confirmed, upstream = e.peerStateLocked()
	if peer {
		put = append(peerTag("relay-next", confirmed, upstream), e.putMsgs()[proto]...)
	} else {
		put = putMsgsFor(e.viewPayload, e.view)[proto]
	}
	return s, put, confirmed, upstream
}

// peerTag carries freshness in the same ordered stream as the data (SDKs
// ignore comments): "relay-next" describes the event that follows it,
// "relay-confirmed" (keepalive) the data already sent.
func peerTag(tag string, confirmed time.Time, upstream string) []byte {
	return []byte(fmt.Sprintf(":%s %d %s\n\n", tag, confirmed.UnixMilli(), upstream))
}

// peerKeepalive returns a "relay-confirmed" keepalive for a follower stream,
// or nil while events are queued for it. Taken under the same lock as every
// enqueue, so a confirmation can never overtake the data it confirms.
func (e *env) peerKeepalive(s *subscriber) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(s.ch) > 0 {
		return nil
	}
	c, up := e.peerStateLocked()
	return peerTag("relay-confirmed", c, up)
}

func (e *env) unsubscribe(s *subscriber) {
	e.mu.Lock()
	delete(e.subs, s)
	e.mu.Unlock()
}

// broadcastLocked queues raw (source data) for peer streams and sdk (the
// view) for SDK streams.
func (e *env) broadcastLocked(raw, sdk [2][]byte) {
	var pc []byte
	for s := range e.subs {
		msg := sdk[s.proto]
		if s.peer {
			msg = raw[s.proto]
		}
		if msg == nil {
			continue
		}
		if s.peer {
			if pc == nil {
				c, up := e.peerStateLocked()
				pc = peerTag("relay-next", c, up)
			}
			msg = append(append([]byte{}, pc...), msg...)
		}
		select {
		case s.ch <- msg:
		default:
			e.r.metrics.inc("flagrelay_downstream_slow_closed_total")
			s.close()
		}
	}
}

func (e *env) subscribers() []*subscriber {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]*subscriber, 0, len(e.subs))
	for s := range e.subs {
		out = append(out, s)
	}
	return out
}

// peerState is what this replica tells a follower about its freshness.
func (e *env) peerState() (confirmedAt time.Time, upstream string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.peerStateLocked()
}

// peerStateLocked: "connected" only once the direct stream's initial put has
// been committed, so a recovering primary never vouches for its old copy.
func (e *env) peerStateLocked() (time.Time, string) {
	if e.authority {
		return e.confirmedAt, "connected"
	}
	return e.confirmedAt, "disconnected"
}

func (e *env) putMsgs() [2][]byte { return putMsgsFor(e.payload, e.data) }

// putMsgsFor renders the put of ds for both stream protocols; payload is
// ds.marshal() when the caller has it (nil: computed).
func putMsgsFor(payload []byte, ds *dataSet) [2][]byte {
	if ds == nil {
		ds = newDataSet()
		payload = nil
	}
	if payload == nil {
		payload = ds.marshal()
	}
	all := make([]byte, 0, len(payload)+48)
	all = append(all, "event: put\ndata: {\"path\":\"/\",\"data\":"...)
	all = append(all, payload...)
	all = append(all, "}\n\n"...)
	flags := ds.marshalFlags()
	legacy := make([]byte, 0, len(flags)+48)
	legacy = append(legacy, "event: put\ndata: {\"path\":\"/\",\"data\":"...)
	legacy = append(legacy, flags...)
	legacy = append(legacy, "}\n\n"...)
	return [2][]byte{all, legacy}
}

func patchMsgs(kind, key string, raw json.RawMessage) [2][]byte {
	all, _ := json.Marshal(map[string]interface{}{"path": "/" + kind + "/" + key, "data": raw})
	out := [2][]byte{[]byte("event: patch\ndata: " + string(all) + "\n\n"), nil}
	if kind == "flags" {
		legacy, _ := json.Marshal(map[string]interface{}{"path": "/" + key, "data": raw})
		out[protoFlags] = []byte("event: patch\ndata: " + string(legacy) + "\n\n")
	}
	return out
}

func deleteMsgs(kind, key string, version uint64) [2][]byte {
	all, _ := json.Marshal(map[string]interface{}{"path": "/" + kind + "/" + key, "version": version})
	out := [2][]byte{[]byte("event: delete\ndata: " + string(all) + "\n\n"), nil}
	if kind == "flags" {
		legacy, _ := json.Marshal(map[string]interface{}{"path": "/" + key, "version": version})
		out[protoFlags] = []byte("event: delete\ndata: " + string(legacy) + "\n\n")
	}
	return out
}

// snapshot returns the source data (peer: true) or the view served to SDKs.
func (e *env) snapshot(peer bool) (ds *dataSet, payload []byte, etag string, confirmed time.Time, upstream string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	confirmed, upstream = e.peerStateLocked()
	if peer {
		return e.data, e.payload, e.etag, confirmed, upstream
	}
	if e.view == nil { // override-only service lost its last override
		ds = newDataSet()
		payload = ds.marshal()
		return ds, payload, etagOf(payload), confirmed, upstream
	}
	return e.view, e.viewPayload, e.viewEtag, confirmed, upstream
}

// awaitData blocks until the env can answer a request:
//   - data delivered by the upstream chain in this process: at once;
//   - data only from disk/peer: up to initWait for fresh data, unless the
//     chain has already failed since start (then at once);
//   - no data: up to firstWait for the first put; once the chain has failed,
//     the peers are asked; then 503 (retryable), unless local overrides
//     exist: an SDK (never a peer) is then served the overrides alone, so an
//     air-gapped relay that never reached upstream still serves them.
func (e *env) awaitData(ctx context.Context, peer bool) (int, error) {
	start := time.Now()
	for {
		e.mu.Lock()
		hasData, source, tried, authFailed, peersTried := e.data != nil, e.source, e.triedOnce, e.authFailed, e.peersTried
		overridesOnly := !peer && e.view != nil
		ch := e.changed
		e.mu.Unlock()
		live := source == "upstream" || source == "primary"
		switch {
		case hasData && (live || tried || time.Since(start) >= e.r.cfg.initWait):
			return http.StatusOK, nil
		case authFailed:
			return http.StatusServiceUnavailable, errors.New("upstream did not accept the SDK key")
		case !hasData && tried && e.r.peers != nil && !peersTried:
			e.syncPeers(ctx)
			continue
		case !hasData && (tried || time.Since(start) >= e.r.cfg.firstWait) && overridesOnly:
			return http.StatusOK, nil
		case !hasData && tried:
			return http.StatusServiceUnavailable, errors.New("no flag data yet and upstream is unreachable")
		case !hasData && time.Since(start) >= e.r.cfg.firstWait:
			return http.StatusServiceUnavailable, errors.New("timed out waiting for upstream flag data")
		}
		limit := e.r.cfg.firstWait
		if hasData {
			limit = e.r.cfg.initWait
		}
		select {
		case <-ctx.Done():
			return http.StatusServiceUnavailable, ctx.Err()
		case <-ch:
		case <-time.After(limit - time.Since(start)):
		}
	}
}

func (e *env) peerSnapshot() ([]byte, time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.data == nil {
		return nil, time.Time{}
	}
	return e.data.marshalWithTombstones(), e.confirmedAt
}
