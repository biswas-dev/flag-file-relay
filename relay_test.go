package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "sdk-srv-test-key"

// mockUpstream speaks the LaunchDarkly server-side protocol.
type mockUpstream struct {
	t       *testing.T
	mu      sync.Mutex
	data    string // {"flags":..,"segments":..}
	streams map[chan string]struct{}
	bulk    [][]json.RawMessage
	bulkHdr []http.Header
	opens   atomic.Int64
	polls   atomic.Int64
	down    atomic.Bool
	srv     *httptest.Server
	// staleFirst, if set, is sent as the first stream's put after a delay
	// (a snapshot captured before a newer poll result).
	staleFirst      string
	staleFirstDelay time.Duration
}

func newMockUpstream(t *testing.T, data string) *mockUpstream {
	m := &mockUpstream{t: t, data: data, streams: map[chan string]struct{}{}}
	m.srv = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockUpstream) serve(w http.ResponseWriter, r *http.Request) {
	if m.down.Load() {
		http.Error(w, "down", http.StatusBadGateway)
		return
	}
	if r.Header.Get("Authorization") != testKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/all":
		m.opens.Add(1)
		fl := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		ch := make(chan string, 16)
		m.mu.Lock()
		m.streams[ch] = struct{}{}
		data := m.data
		if m.staleFirst != "" && m.opens.Load() == 1 {
			data = m.staleFirst
			m.mu.Unlock()
			time.Sleep(m.staleFirstDelay)
			m.mu.Lock()
		}
		m.mu.Unlock()
		defer func() { m.mu.Lock(); delete(m.streams, ch); m.mu.Unlock() }()
		fmt.Fprintf(w, "event: put\ndata: {\"path\":\"/\",\"data\":%s}\n\n", data)
		fl.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				io.WriteString(w, ev)
				fl.Flush()
			}
		}
	case "/sdk/latest-all":
		m.polls.Add(1)
		m.mu.Lock()
		io.WriteString(w, m.data)
		m.mu.Unlock()
	case "/bulk":
		var evs []json.RawMessage
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &evs)
		m.mu.Lock()
		m.bulk = append(m.bulk, evs)
		m.bulkHdr = append(m.bulkHdr, r.Header.Clone())
		m.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	case "/diagnostic":
		w.WriteHeader(http.StatusAccepted)
	default:
		http.NotFound(w, r)
	}
}

func (m *mockUpstream) send(ev string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for ch := range m.streams {
		ch <- ev
	}
}

func (m *mockUpstream) setData(d string) { m.mu.Lock(); m.data = d; m.mu.Unlock() }

// dropStreams closes every open upstream stream.
func (m *mockUpstream) dropStreams() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for ch := range m.streams {
		close(ch)
		delete(m.streams, ch)
	}
}

func flagJSON(key string, version int, on bool) string {
	return fmt.Sprintf(`{"key":%q,"version":%d,"on":%v,"variations":[true,false],"offVariation":1,"fallthrough":{"variation":0},"targets":[],"rules":[],"prerequisites":[],"salt":%q,"deleted":false}`, key, version, on, key)
}

// dataJSON builds {"flags":{key: flag...},"segments":{}} from flagJSON values.
func dataJSON(flags ...string) string {
	entries := make([]string, 0, len(flags))
	for _, f := range flags {
		var h struct {
			Key string `json:"key"`
		}
		_ = json.Unmarshal([]byte(f), &h)
		entries = append(entries, fmt.Sprintf("%q:%s", h.Key, f))
	}
	return `{"flags":{` + strings.Join(entries, ",") + `},"segments":{}}`
}

func testConfig(upstream, cacheDir string) relayConfig {
	return relayConfig{
		streamURI: upstream, pollURI: upstream, eventsURI: upstream,
		cacheDir: cacheDir, maxKeys: 4,
		initWait: 500 * time.Millisecond, firstWait: 2 * time.Second,
		drainDelay: 0, drainSpread: 200 * time.Millisecond,
		readTimeout: 5 * time.Second, pollInterval: 200 * time.Millisecond,
		eventsFlush: 100 * time.Millisecond, keepalive: time.Second, rejectTTL: time.Minute,
		reconcileInterval: time.Hour, writeTimeout: 5 * time.Second, maxStreams: 1000,
		primaryRetry: time.Second, idleExpiry: time.Minute, peerSyncWait: 2 * time.Second,
		persistRetry: 200 * time.Millisecond,
	}
}

func startRelay(t *testing.T, cfg relayConfig) (*relay, *httptest.Server, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	r := newRelay(ctx, cfg)
	go r.events.run(ctx)
	r.start(ctx)
	srv := httptest.NewServer(r.mux())
	t.Cleanup(func() { srv.Close(); cancel() })
	return r, srv, cancel
}

func get(t *testing.T, url, key string, hdr ...string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", key)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

type streamClient struct {
	events chan sseEvent
	cancel context.CancelFunc
	done   chan struct{}
}

func openStream(t *testing.T, url, key string) *streamClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url+"/all", nil)
	req.Header.Set("Authorization", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("stream status %d", resp.StatusCode)
	}
	sc := &streamClient{events: make(chan sseEvent, 64), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(sc.done)
		defer resp.Body.Close()
		rd := newSSEReader(bufio.NewReader(resp.Body), nil)
		for {
			ev, err := rd.next()
			if err != nil {
				return
			}
			sc.events <- ev
		}
	}()
	t.Cleanup(cancel)
	return sc
}

func (s *streamClient) next(t *testing.T) sseEvent {
	t.Helper()
	select {
	case ev := <-s.events:
		return ev
	case <-time.After(3 * time.Second):
		t.Fatal("no event within 3s")
	}
	return sseEvent{}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestStreamPutPatchDeleteVersionRules(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 2, true), flagJSON("b", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	s := openStream(t, srv.URL, testKey)
	if ev := s.next(t); ev.Name != "put" || !strings.Contains(ev.Data, `"a"`) {
		t.Fatalf("first event %+v", ev)
	}
	// Older patch is ignored, newer one is forwarded verbatim.
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 1, false) + "}\n\n")
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 3, false) + "}\n\n")
	ev := s.next(t)
	if ev.Name != "patch" || !strings.Contains(ev.Data, `"version":3`) {
		t.Fatalf("expected v3 patch, got %+v", ev)
	}
	up.send("event: delete\ndata: {\"path\":\"/flags/b\",\"version\":2}\n\n")
	ev = s.next(t)
	if ev.Name != "delete" || !strings.Contains(ev.Data, `"/flags/b"`) {
		t.Fatalf("expected delete, got %+v", ev)
	}
	code, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey)
	if code != 200 || !strings.Contains(body, `"version":3`) || strings.Contains(body, `"b"`) {
		t.Fatalf("poll after patch/delete: %d %s", code, body)
	}
	// A stale patch for the deleted flag must not resurrect it.
	up.send("event: patch\ndata: {\"path\":\"/flags/b\",\"data\":" + flagJSON("b", 1, true) + "}\n\n")
	time.Sleep(100 * time.Millisecond)
	if _, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey); strings.Contains(body, `"b"`) {
		t.Fatalf("stale patch resurrected a deleted flag: %s", body)
	}
}

func TestOneUpstreamStreamForManyDownstream(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	var subs []*streamClient
	for i := 0; i < 50; i++ {
		s := openStream(t, srv.URL, testKey)
		s.next(t)
		subs = append(subs, s)
	}
	if n := up.opens.Load(); n != 1 {
		t.Fatalf("upstream streams = %d, want 1", n)
	}
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 2, false) + "}\n\n")
	for _, s := range subs {
		if ev := s.next(t); ev.Name != "patch" {
			t.Fatalf("got %+v", ev)
		}
	}
}

func TestAuth(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	if code, _, _ := get(t, srv.URL+"/sdk/latest-all", ""); code != 401 {
		t.Fatalf("no key: %d", code)
	}
	// Upstream refuses the key: retryable 503 (SDKs treat 401 as permanent,
	// and flagtgl has returned spurious 401s under load).
	if code, _, _ := get(t, srv.URL+"/sdk/latest-all", "sdk-srv-wrong"); code != 503 {
		t.Fatalf("wrong key: %d", code)
	}
	if code, _, _ := get(t, srv.URL+"/sdk/latest-all", "Bearer "+testKey); code != 200 {
		t.Fatalf("bearer key: %d", code)
	}
}

func TestUnknownKeyUpstreamDownIs503(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	up.down.Store(true)
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, t.TempDir()))
	code, _, hdr := get(t, srv.URL+"/sdk/latest-all", testKey)
	if code != 503 || hdr.Get("Retry-After") == "" {
		t.Fatalf("got %d", code)
	}
}

// R2: flagtgl down -> relay restart -> SDK restart -> same values.
func TestRestartDuringOutageServesDiskCache(t *testing.T) {
	dir := t.TempDir()
	up := newMockUpstream(t, dataJSON(flagJSON("db-route-pgdog", 7, false)))
	r1, srv1, cancel1 := startRelay(t, testConfig(up.srv.URL, dir))
	if code, _, _ := get(t, srv1.URL+"/sdk/latest-all", testKey); code != 200 {
		t.Fatal(code)
	}
	waitFor(t, "cache file", func() bool { return len(loadCache(dir)) == 1 })
	up.down.Store(true)
	up.dropStreams()
	srv1.Close()
	cancel1()
	_ = r1

	_, srv2, _ := startRelay(t, testConfig(up.srv.URL, dir))
	start := time.Now()
	code, body, _ := get(t, srv2.URL+"/sdk/latest-all", testKey)
	if code != 200 || !strings.Contains(body, `"version":7`) || !strings.Contains(body, `"on":false`) {
		t.Fatalf("after restart in outage: %d %s", code, body)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("disk answer took %s", time.Since(start))
	}
	s := openStream(t, srv2.URL, testKey)
	if ev := s.next(t); ev.Name != "put" || !strings.Contains(ev.Data, `"version":7`) {
		t.Fatalf("stream after restart: %+v", ev)
	}
	// A key that was never cached still gets nothing.
	if code, _, _ := get(t, srv2.URL+"/sdk/latest-all", "sdk-srv-other"); code != 503 {
		t.Fatalf("uncached key during outage: %d", code)
	}
	// Upstream returns with a change: relay converges and re-persists.
	up.setData(dataJSON(flagJSON("db-route-pgdog", 8, true)))
	up.down.Store(false)
	ev := s.next(t)
	if ev.Name != "put" || !strings.Contains(ev.Data, `"version":8`) {
		t.Fatalf("after recovery: %+v", ev)
	}
}

func TestStreamDownFallsBackToPolling(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	get(t, srv.URL+"/sdk/latest-all", testKey)
	// Upstream stream breaks repeatedly; polling still works.
	up.setData(dataJSON(flagJSON("a", 2, false)))
	up.dropStreams()
	waitFor(t, "converged v2", func() bool {
		_, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey)
		return strings.Contains(body, `"version":2`)
	})
	_ = r
}

func TestIndirectPutPolls(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	s := openStream(t, srv.URL, testKey)
	s.next(t)
	up.setData(dataJSON(flagJSON("a", 5, false)))
	up.send("event: indirect/put\ndata: \n\n")
	if ev := s.next(t); ev.Name != "put" || !strings.Contains(ev.Data, `"version":5`) {
		t.Fatalf("got %+v", ev)
	}
}

func TestPollETag(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	_, _, hdr := get(t, srv.URL+"/sdk/latest-all", testKey)
	etag := hdr.Get("ETag")
	if etag == "" {
		t.Fatal("no etag")
	}
	if code, _, _ := get(t, srv.URL+"/sdk/latest-all", testKey, "If-None-Match", etag); code != 304 {
		t.Fatalf("If-None-Match: %d", code)
	}
	if code, body, _ := get(t, srv.URL+"/sdk/latest-flags/a", testKey); code != 200 || !strings.Contains(body, `"key":"a"`) {
		t.Fatalf("latest-flags/a: %d %s", code, body)
	}
}

func TestEventsAreBatched(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	cfg := testConfig(up.srv.URL, "")
	cfg.eventsFlush = 300 * time.Millisecond
	_, srv, _ := startRelay(t, cfg)
	get(t, srv.URL+"/sdk/latest-all", testKey) // makes the key known
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/bulk", strings.NewReader(fmt.Sprintf(`[{"kind":"index","n":%d},{"kind":"summary","n":%d}]`, i, i)))
		req.Header.Set("Authorization", testKey)
		req.Header.Set("X-LaunchDarkly-Event-Schema", "4")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 202 {
			t.Fatalf("bulk: %v %v", err, resp)
		}
		resp.Body.Close()
	}
	waitFor(t, "forwarded", func() bool {
		up.mu.Lock()
		defer up.mu.Unlock()
		n := 0
		for _, b := range up.bulk {
			n += len(b)
		}
		return n == 10
	})
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bulk) != 1 {
		t.Fatalf("upstream bulk posts = %d, want 1", len(up.bulk))
	}
	h := up.bulkHdr[0]
	if h.Get("X-LaunchDarkly-Event-Schema") != "4" || h.Get("X-LaunchDarkly-Payload-ID") == "" {
		t.Fatalf("headers %v", h)
	}
}

func TestDrainSpreadsDisconnects(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	cfg := testConfig(up.srv.URL, "")
	cfg.drainSpread = 1 * time.Second
	r, srv, _ := startRelay(t, cfg)
	var subs []*streamClient
	for i := 0; i < 20; i++ {
		s := openStream(t, srv.URL, testKey)
		s.next(t)
		subs = append(subs, s)
	}
	start := time.Now()
	r.draining.Store(true)
	go r.drain()
	times := make([]time.Duration, len(subs))
	var wg sync.WaitGroup
	for i, s := range subs {
		wg.Add(1)
		go func(i int, s *streamClient) {
			defer wg.Done()
			<-s.done
			times[i] = time.Since(start)
		}(i, s)
	}
	wg.Wait()
	// Streams must not all close at once: the last closes well after the first.
	var minT, maxT time.Duration = time.Hour, 0
	for _, d := range times {
		if d < minT {
			minT = d
		}
		if d > maxT {
			maxT = d
		}
	}
	if maxT-minT < 700*time.Millisecond {
		t.Fatalf("disconnects not spread: first %s last %s", minT, maxT)
	}
	if code, _, _ := get(t, srv.URL+"/readyz", ""); code != 503 {
		t.Fatalf("readyz while draining: %d", code)
	}
}

func TestPersistenceHasNoKeyMaterial(t *testing.T) {
	dir := t.TempDir()
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, dir))
	get(t, srv.URL+"/sdk/latest-all", testKey)
	waitFor(t, "cache", func() bool { return len(loadCache(dir)) == 1 })
	for h := range loadCache(dir) {
		if h != keyHash(testKey) {
			t.Fatalf("cache named %s", h)
		}
	}
}

func TestDenyList(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	cfg := testConfig(up.srv.URL, "")
	cfg.deny = map[string]bool{keyHash(testKey): true}
	_, srv, _ := startRelay(t, cfg)
	if code, _, _ := get(t, srv.URL+"/sdk/latest-all", testKey); code != 401 {
		t.Fatalf("denied key: %d", code)
	}
}

// Codex r1 #1: a stream that keeps its keepalives but misses an update must
// still converge through the reconciliation poll.
func TestReconcilePollCatchesMissedUpdate(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("db-route-pgdog", 1, true)))
	cfg := testConfig(up.srv.URL, "")
	cfg.reconcileInterval = 300 * time.Millisecond
	_, srv, _ := startRelay(t, cfg)
	s := openStream(t, srv.URL, testKey)
	s.next(t)
	up.setData(dataJSON(flagJSON("db-route-pgdog", 2, false))) // no patch sent on the stream
	ev := s.next(t)
	if ev.Name != "put" || !strings.Contains(ev.Data, `"version":2`) {
		t.Fatalf("got %+v", ev)
	}
}

// Codex r1 #3: an update is on disk before any SDK sees it.
func TestPersistBeforePublish(t *testing.T) {
	dir := t.TempDir()
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, dir))
	s := openStream(t, srv.URL, testKey)
	s.next(t)
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 2, false) + "}\n\n")
	s.next(t)
	d := loadCache(dir)[keyHash(testKey)]
	if d == nil || d.data.Flags["a"].Version != 2 {
		t.Fatalf("disk copy not updated before publish: %+v", d)
	}
	up.send("event: delete\ndata: {\"path\":\"/flags/a\",\"version\":3}\n\n")
	s.next(t)
	d = loadCache(dir)[keyHash(testKey)]
	if it := d.data.Flags["a"]; !it.Deleted || it.Version != 3 {
		t.Fatalf("tombstone not persisted: %+v", it)
	}
}

// Codex r1 #2: replica B restarted during an outage with an older copy pulls
// the newer one from replica A before serving.
func TestPeerSyncPicksNewestVersion(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("db-route-pgdog", 10, true)))
	dirA, dirB := t.TempDir(), t.TempDir()
	// B saw v10 (ON) then went down.
	_, srvB, cancelB := startRelay(t, testConfig(up.srv.URL, dirB))
	get(t, srvB.URL+"/sdk/latest-all", testKey)
	waitFor(t, "B cache", func() bool { return len(loadCache(dirB)) == 1 })
	srvB.Close()
	cancelB()
	// A sees v12 (OFF); then upstream goes down.
	up.setData(dataJSON(flagJSON("db-route-pgdog", 12, false)))
	rA, srvA, _ := startRelay(t, testConfig(up.srv.URL, dirA))
	get(t, srvA.URL+"/sdk/latest-all", testKey)
	up.down.Store(true)
	up.dropStreams()
	peerA := httptest.NewServer(rA.peerMux())
	t.Cleanup(peerA.Close)
	// B restarts, offline, with A as its peer.
	cfgB := testConfig(up.srv.URL, dirB)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rB := newRelay(ctx, cfgB)
	rB.peers = &peerSet{static: []string{strings.TrimPrefix(peerA.URL, "http://")}, client: http.DefaultClient}
	rB.start(ctx)
	srvB2 := httptest.NewServer(rB.mux())
	t.Cleanup(srvB2.Close)
	code, body, _ := get(t, srvB2.URL+"/sdk/latest-all", testKey)
	if code != 200 || !strings.Contains(body, `"version":12`) {
		t.Fatalf("B served %d %s, want v12 from peer", code, body)
	}
	// And a brand-new replica C with an empty disk gets data from A too.
	rC := newRelay(ctx, testConfig(up.srv.URL, t.TempDir()))
	rC.peers = &peerSet{static: []string{strings.TrimPrefix(peerA.URL, "http://")}, client: http.DefaultClient}
	rC.start(ctx)
	srvC := httptest.NewServer(rC.mux())
	t.Cleanup(srvC.Close)
	code, body, _ = get(t, srvC.URL+"/sdk/latest-all", testKey)
	if code != 200 || !strings.Contains(body, `"version":12`) {
		t.Fatalf("C served %d %s, want v12 from peer", code, body)
	}
}

func TestLegacyFlagsStream(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/flags", nil)
	req.Header.Set("Authorization", testKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := newSSEReader(resp.Body, nil)
	ev, _ := rd.next()
	var put struct {
		Path string                     `json:"path"`
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(ev.Data), &put); err != nil || ev.Name != "put" || put.Data["a"] == nil || put.Data["flags"] != nil {
		t.Fatalf("legacy put: %s %s", ev.Name, ev.Data)
	}
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 2, false) + "}\n\n")
	ev, _ = rd.next()
	if ev.Name != "patch" || !strings.Contains(ev.Data, `"path":"/a"`) {
		t.Fatalf("legacy patch: %+v", ev)
	}
}

func TestEventRetryDeduplicated(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	get(t, srv.URL+"/sdk/latest-all", testKey)
	for i := 0; i < 2; i++ { // the SDK retries the same payload
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/bulk", strings.NewReader(`[{"kind":"index"}]`))
		req.Header.Set("Authorization", testKey)
		req.Header.Set("X-LaunchDarkly-Payload-ID", "11111111-2222-4333-8444-555555555555")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
	}
	time.Sleep(400 * time.Millisecond)
	up.mu.Lock()
	defer up.mu.Unlock()
	n := 0
	for _, b := range up.bulk {
		n += len(b)
	}
	if n != 1 {
		t.Fatalf("forwarded %d events, want 1", n)
	}
}

// startRelayCfg starts a relay with a fully custom config (peers, primary).
func startRelayCfg(t *testing.T, cfg relayConfig) (*relay, *httptest.Server, *httptest.Server, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	r := newRelay(ctx, cfg)
	go r.events.run(ctx)
	srv := httptest.NewServer(r.mux())
	peer := httptest.NewServer(r.peerMux())
	r.start(ctx)
	t.Cleanup(func() { srv.Close(); peer.Close(); cancel() })
	return r, srv, peer, cancel
}

func hostPort(u string) string { return strings.TrimPrefix(u, "http://") }

// One upstream stream per key for the whole environment: the follower
// streams from the primary, and SDK streams on both replicas see changes.
func TestFollowerStreamsFromPrimary(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srvP, _, _ := startRelayCfg(t, testConfig(up.srv.URL, t.TempDir()))
	cfgF := testConfig(up.srv.URL, t.TempDir())
	cfgF.primaryURI = srvP.URL
	_, srvF, _, _ := startRelayCfg(t, cfgF)
	sP := openStream(t, srvP.URL, testKey)
	sF := openStream(t, srvF.URL, testKey)
	sP.next(t)
	sF.next(t)
	if n := up.opens.Load(); n != 1 {
		t.Fatalf("upstream streams = %d, want 1 (follower must stream from the primary)", n)
	}
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 2, false) + "}\n\n")
	for _, s := range []*streamClient{sP, sF} {
		if ev := s.next(t); ev.Name != "patch" || !strings.Contains(ev.Data, `"version":2`) {
			t.Fatalf("got %+v", ev)
		}
	}
}

func TestFollowerFailsOverAndReturns(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	rP, srvP, _, cancelP := startRelayCfg(t, testConfig(up.srv.URL, t.TempDir()))
	_ = rP
	cfgF := testConfig(up.srv.URL, t.TempDir())
	cfgF.primaryURI = srvP.URL
	rF, srvF, _, _ := startRelayCfg(t, cfgF)
	sF := openStream(t, srvF.URL, testKey)
	sF.next(t)
	// Primary dies: the follower streams from upstream directly.
	srvP.CloseClientConnections()
	srvP.Close()
	cancelP()
	waitFor(t, "follower direct", func() bool {
		for _, s := range rF.statusList() {
			if s.Via == "direct" {
				return true
			}
		}
		return false
	})
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 2, false) + "}\n\n")
	if ev := sF.next(t); ev.Name != "patch" && ev.Name != "put" {
		t.Fatalf("got %+v", ev)
	}
}

// Codex r2 #1: the primary restarted offline with an older disk copy than
// the follower (which streamed direct while the primary was away): the
// primary must adopt the follower's fresher copy before it is ready.
func TestPrimaryAdoptsFresherFollowerCopyAtStart(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("db-route-pgdog", 10, true)))
	dirP := t.TempDir()
	_, srvP0, _, cancelP0 := startRelayCfg(t, testConfig(up.srv.URL, dirP))
	get(t, srvP0.URL+"/sdk/latest-all", testKey)
	waitFor(t, "P cache", func() bool { return len(loadCache(dirP)) == 1 })
	srvP0.Close()
	cancelP0()
	time.Sleep(20 * time.Millisecond)
	// Follower (primary gone) gets v12 OFF directly; then upstream dies.
	up.setData(dataJSON(flagJSON("db-route-pgdog", 12, false)))
	cfgF := testConfig(up.srv.URL, t.TempDir())
	cfgF.primaryURI = "http://127.0.0.1:1" // unreachable primary
	_, srvF, peerF, _ := startRelayCfg(t, cfgF)
	if _, body, _ := get(t, srvF.URL+"/sdk/latest-all", testKey); !strings.Contains(body, `"version":12`) {
		t.Fatalf("follower: %s", body)
	}
	up.down.Store(true)
	up.dropStreams()
	// Primary restarts offline with v10 on disk and the follower as peer.
	cfgP := testConfig(up.srv.URL, dirP)
	cfgP.peerAddrs = []string{hostPort(peerF.URL)}
	_, srvP, _, _ := startRelayCfg(t, cfgP)
	code, body, _ := get(t, srvP.URL+"/sdk/latest-all", testKey)
	if code != 200 || !strings.Contains(body, `"version":12`) {
		t.Fatalf("primary served %d %s, want v12 adopted from the follower", code, body)
	}
}

// Codex r2 #2: a flag deleted and re-created upstream restarts at a low
// version. Adoption is wholesale by freshness, so the old incarnation (higher
// version, older copy) must not win.
func TestRecreatedFlagNotOverriddenByOldIncarnation(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("f", 100, true)))
	dirB := t.TempDir()
	_, srvB0, _, cancelB0 := startRelayCfg(t, testConfig(up.srv.URL, dirB))
	get(t, srvB0.URL+"/sdk/latest-all", testKey)
	waitFor(t, "B cache", func() bool { return len(loadCache(dirB)) == 1 })
	srvB0.Close()
	cancelB0()
	time.Sleep(20 * time.Millisecond)
	up.setData(dataJSON(flagJSON("f", 1, false))) // deleted + re-created
	_, srvA, peerA, _ := startRelayCfg(t, testConfig(up.srv.URL, t.TempDir()))
	get(t, srvA.URL+"/sdk/latest-all", testKey)
	up.down.Store(true)
	up.dropStreams()
	cfgB := testConfig(up.srv.URL, dirB)
	cfgB.peerAddrs = []string{hostPort(peerA.URL)}
	_, srvB, _, _ := startRelayCfg(t, cfgB)
	_, body, _ := get(t, srvB.URL+"/sdk/latest-all", testKey)
	if !strings.Contains(body, `"version":1,`) || !strings.Contains(body, `"on":false`) {
		t.Fatalf("B served %s, want the re-created flag (v1, off)", body)
	}
}

// Codex r2 #6: malformed snapshots never replace good data.
func TestMalformedSnapshotsRejected(t *testing.T) {
	for _, bad := range []string{`null`, `{}`, `{"flags":null}`, `[]`, `{"flags":{},"segments":null}`} {
		if _, err := parseDataSet([]byte(bad)); err == nil {
			t.Errorf("parseDataSet(%s) accepted", bad)
		}
	}
	// Malformed items are set aside one by one (never the whole set).
	for _, bad := range []string{`{"flags":{"a":null}}`, `{"flags":{"a":{"key":"a"}}}`,
		`{"flags":{"a":{"version":123}}}`,
		`{"flags":{"a":{"key":"a","version":1,"on":"yes","variations":[],"fallthrough":{},"targets":[],"rules":[],"prerequisites":[],"salt":"a"}}}`} {
		ds, err := parseDataSet([]byte(bad))
		if err != nil || len(ds.invalid) != 1 || len(ds.Flags) != 0 {
			t.Errorf("parseDataSet(%s): %v %+v", bad, err, ds)
		}
	}
	if _, err := parseDataSet([]byte(`{"flags":{}}`)); err != nil {
		t.Errorf("empty environment rejected: %v", err)
	}
	if _, err := parseDataSet([]byte(`{"flags":{"gone":{"key":"gone","version":4,"deleted":true}},"segments":{}}`)); err != nil {
		t.Errorf("tombstone rejected: %v", err)
	}
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	s := openStream(t, srv.URL, testKey)
	s.next(t)
	up.send("event: put\ndata: {\"path\":\"/\",\"data\":null}\n\n")
	time.Sleep(200 * time.Millisecond)
	if _, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey); !strings.Contains(body, `"a"`) {
		t.Fatalf("malformed put erased data: %s", body)
	}
}

// Codex r2 #9: an SDK retry that lands on the other replica is still counted once.
func TestEventDedupeAcrossReplicas(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	cfgP := testConfig(up.srv.URL, "")
	_, srvP, _, _ := startRelayCfg(t, cfgP)
	cfgF := testConfig(up.srv.URL, "")
	cfgF.primaryURI = srvP.URL
	_, srvF, _, _ := startRelayCfg(t, cfgF)
	get(t, srvP.URL+"/sdk/latest-all", testKey)
	get(t, srvF.URL+"/sdk/latest-all", testKey)
	for _, u := range []string{srvF.URL, srvP.URL} { // first try via F, retry via P
		req, _ := http.NewRequest(http.MethodPost, u+"/bulk", strings.NewReader(`[{"kind":"index"}]`))
		req.Header.Set("Authorization", testKey)
		req.Header.Set("X-LaunchDarkly-Payload-ID", "aaaaaaaa-2222-4333-8444-555555555555")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	up.mu.Lock()
	defer up.mu.Unlock()
	n := 0
	for _, b := range up.bulk {
		n += len(b)
	}
	if n != 1 {
		t.Fatalf("forwarded %d events, want 1", n)
	}
}

// Codex r2 #3: a failing disk does not hold back an update (an OFF must get
// out), and the failure is visible.
func TestPersistFailureStillPublishesAndReports(t *testing.T) {
	dir := t.TempDir()
	// Runs after the relay has stopped (cleanups are LIFO), so a background
	// retry cannot write into the directory while it is being removed.
	t.Cleanup(func() { time.Sleep(50 * time.Millisecond); os.Chmod(dir, 0o700) })
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, dir))
	s := openStream(t, srv.URL, testKey)
	s.next(t)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 2, false) + "}\n\n")
	if ev := s.next(t); ev.Name != "patch" {
		t.Fatalf("got %+v", ev)
	}
	st := r.statusList()
	if len(st) != 1 || st[0].PersistError == "" || r.metrics.get("flagrelay_cache_write_errors_total") == 0 {
		t.Fatalf("persist failure not reported: %+v", st)
	}
}

// Codex r2/r3 #5: a stream's initial put that was captured before a newer
// poll result must not roll the data back.
func TestStaleInitialStreamPutIsFenced(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 2, false)))
	up.staleFirst = dataJSON(flagJSON("a", 1, true))
	up.staleFirstDelay = 700 * time.Millisecond
	cfg := testConfig(up.srv.URL, "")
	cfg.pollInterval = 100 * time.Millisecond
	cfg.firstWait = 3 * time.Second
	_, srv, _ := startRelay(t, cfg)
	go func() { // creates the env; the poll lands v2 first
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/sdk/latest-all", nil)
		req.Header.Set("Authorization", testKey)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	waitFor(t, "v2 from the poll", func() bool {
		_, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey)
		return strings.Contains(body, `"version":2`)
	})
	sub := openStream(t, srv.URL, testKey) // an SDK watching from v2 on
	if ev := sub.next(t); !strings.Contains(ev.Data, `"version":2`) {
		t.Fatalf("first put %+v", ev)
	}
	time.Sleep(1200 * time.Millisecond) // the stale put (v1) has arrived by now
	for {
		select {
		case ev := <-sub.events:
			if strings.Contains(ev.Data, `"version":1`) {
				t.Fatalf("stale initial put reached an SDK: %s %s", ev.Name, ev.Data)
			}
			continue
		default:
		}
		break
	}
	if _, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey); !strings.Contains(body, `"version":2`) {
		t.Fatalf("stale initial put rolled data back: %s", body)
	}
}

// The follower goes back to the primary once it is ready again (one
// upstream stream per key in steady state).
func TestFollowerReturnsToPrimary(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ctxP, cancelP := context.WithCancel(context.Background())
	rP := newRelay(ctxP, testConfig(up.srv.URL, t.TempDir()))
	rP.start(ctxP)
	srvP := &http.Server{Handler: rP.mux()}
	go srvP.Serve(ln)

	cfgF := testConfig(up.srv.URL, t.TempDir())
	cfgF.primaryURI = "http://" + addr
	rF, srvF, _, _ := startRelayCfg(t, cfgF)
	sF := openStream(t, srvF.URL, testKey)
	sF.next(t)
	via := func() string {
		for _, s := range rF.statusList() {
			return s.Via
		}
		return ""
	}
	waitFor(t, "via primary", func() bool { return via() == "primary" })
	srvP.Close()
	cancelP()
	waitFor(t, "via direct", func() bool { return via() == "direct" })
	ln2, err := net.Listen("tcp", addr) // primary back on the same address
	if err != nil {
		t.Fatal(err)
	}
	ctxP2, cancelP2 := context.WithCancel(context.Background())
	defer cancelP2()
	rP2 := newRelay(ctxP2, testConfig(up.srv.URL, t.TempDir()))
	rP2.start(ctxP2)
	srvP2 := &http.Server{Handler: rP2.mux()}
	go srvP2.Serve(ln2)
	defer srvP2.Close()
	waitFor(t, "back via primary", func() bool { return via() == "primary" })
}

func TestSDKEndpointsWaitForStartupSync(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newRelay(ctx, testConfig(up.srv.URL, ""))
	srv := httptest.NewServer(r.mux())
	defer srv.Close()
	if code, _, _ := get(t, srv.URL+"/sdk/latest-all", testKey); code != 503 {
		t.Fatalf("before ready: %d", code)
	}
	r.start(ctx)
	if code, _, _ := get(t, srv.URL+"/sdk/latest-all", testKey); code != 200 {
		t.Fatalf("after ready: %d", code)
	}
}

// One malformed flag must not block a change to another (e.g. the kill
// switch), and must not erase its own last good copy.
func TestMalformedItemDoesNotBlockOtherChanges(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("db-route-pgdog", 1, true), flagJSON("other", 1, true)))
	_, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	s := openStream(t, srv.URL, testKey)
	s.next(t)
	up.send("event: put\ndata: {\"path\":\"/\",\"data\":{\"flags\":{\"db-route-pgdog\":" + flagJSON("db-route-pgdog", 2, false) +
		",\"other\":{\"version\":9}},\"segments\":{}}}\n\n")
	ev := s.next(t)
	if ev.Name != "put" || !strings.Contains(ev.Data, `"version":2`) || !strings.Contains(ev.Data, `"salt":"other"`) {
		t.Fatalf("got %+v", ev)
	}
}

// Real payloads must pass validation: flagtgl's (with a target and a rule,
// captured from flagtgl main) and LaunchDarkly Relay's shape (with a segment).
func TestRealPayloadsValidate(t *testing.T) {
	for _, f := range []string{"testdata/flagtgl_latest_all.json", "testdata/ldrelay_latest_all.json"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		ds, err := parseDataSet(b)
		if err != nil || len(ds.invalid) != 0 || len(ds.Flags) != 1 {
			t.Fatalf("%s: %v invalid=%v", f, err, ds.invalid)
		}
	}
}

func TestNestedInvalidItemsSetAside(t *testing.T) {
	good := func(mut string) string {
		return `{"flags":{"f":{"key":"f","version":1,"on":true,"variations":[true,false],"fallthrough":{"variation":0},"salt":"f","prerequisites":[],` + mut + `}}}`
	}
	for name, mut := range map[string]string{
		"empty rule":      `"targets":[],"rules":[{}]`,
		"numeric rule id": `"targets":[],"rules":[{"id":7,"clauses":[],"trackEvents":false,"variation":0}]`,
		"no trackEvents":  `"targets":[],"rules":[{"id":"7","clauses":[],"variation":0}]`,
		"bad clause":      `"targets":[],"rules":[{"id":"7","clauses":[{"op":"in","values":[]}],"trackEvents":false,"variation":0}]`,
		"target ints":     `"targets":[{"values":[1],"variation":0}],"rules":[]`,
		"target no var":   `"targets":[{"values":["a"]}],"rules":[]`,
		"prereq bad":      `"targets":[],"rules":[],"prerequisites":[{"key":"x"}]`,
		"csa empty":       `"targets":[],"rules":[],"clientSideAvailability":{}`,
		"offVar string":   `"targets":[],"rules":[],"offVariation":"1"`,
		"target var str":  `"targets":[{"values":["a"],"variation":"1"}],"rules":[]`,
		"debug negative":  `"targets":[],"rules":[],"debugEventsUntilDate":-1`,
		"clientSide int":  `"targets":[],"rules":[],"clientSide":1`,
	} {
		ds, err := parseDataSet([]byte(good(mut)))
		if err != nil || len(ds.invalid) != 1 {
			t.Errorf("%s: err=%v invalid=%v", name, err, ds.invalid)
		}
	}
	ds, err := parseDataSet([]byte(good(`"targets":[{"values":["a"],"variation":1}],"rules":[{"id":"1","clauses":[{"attribute":"key","op":"in","values":["a"]}],"trackEvents":false,"rollout":{"variations":[{"variation":0,"weight":100000}]}}]`)))
	if err != nil || len(ds.invalid) != 0 {
		t.Errorf("valid nested flag rejected: %v %v", err, ds.invalid)
	}
}

func TestSegmentContextsNotNull(t *testing.T) {
	ds, err := parseDataSet([]byte(`{"flags":{},"segments":{"s":{"key":"s","version":1,"included":[],"excluded":[],"rules":[],"salt":"s","includedContexts":null}}}`))
	if err != nil || len(ds.invalid) != 1 {
		t.Fatalf("includedContexts:null accepted: %v %v", err, ds.invalid)
	}
}

// Codex r5 #4: a peer copy adopted while a direct stream's initial put is
// delayed fences that (older) put, exactly like a poll does.
func TestStaleInitialPutFencedByPeerAdoption(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 2, false))) // upstream's real state
	up.staleFirst = dataJSON(flagJSON("a", 1, true))
	up.staleFirstDelay = 900 * time.Millisecond
	cfg := testConfig(up.srv.URL, "")
	cfg.reconcileInterval, cfg.pollInterval = time.Hour, time.Hour // no polls: only the peer can win
	r, srv, _ := startRelay(t, cfg)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/sdk/latest-all", nil)
		req.Header.Set("Authorization", testKey)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	var e *env
	waitFor(t, "env", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		e = r.envs[testKey]
		return e != nil
	})
	// A fresher copy (v2 OFF) arrives from a peer while the stream is stuck.
	peerDS, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 2, false))))
	e.applyPeerSnapshot(peerDS, time.Now())
	sub := openStream(t, srv.URL, testKey)
	if ev := sub.next(t); !strings.Contains(ev.Data, `"version":2`) {
		t.Fatalf("first put %+v", ev)
	}
	time.Sleep(1300 * time.Millisecond)
	for {
		select {
		case ev := <-sub.events:
			if strings.Contains(ev.Data, `"version":1`) {
				t.Fatalf("stale initial put overrode the peer copy: %s", ev.Data)
			}
			continue
		default:
		}
		break
	}
}

// Codex r5 #3: keepalives on a direct stream whose initial put has not
// arrived do not make our old copy look fresh.
func TestKeepaliveBeforeInitialPutDoesNotConfirm(t *testing.T) {
	block := make(chan struct{})
	srvUp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/all" {
			http.Error(w, "down", 502)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 20; i++ {
			fmt.Fprint(w, ":\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-block:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}))
	defer srvUp.Close()
	defer close(block)
	dir := t.TempDir()
	old := time.Now().Add(-time.Hour)
	ds, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 1, true))))
	if err := writeCacheFile(dir, keyHash(testKey), ds.marshalWithTombstones(), "upstream", old); err != nil {
		t.Fatal(err)
	}
	r, srv, _ := startRelay(t, testConfig(srvUp.URL, dir))
	get(t, srv.URL+"/sdk/latest-all", testKey)
	time.Sleep(400 * time.Millisecond) // several keepalives, no put
	r.mu.Lock()
	e := r.envs[testKey]
	r.mu.Unlock()
	e.mu.Lock()
	conf := e.confirmedAt
	e.mu.Unlock()
	if conf.After(old.Add(time.Minute)) {
		t.Fatalf("keepalive before the initial put advanced confirmedAt to %s", conf)
	}
}

// Codex r4/r5 #1: a large array of tiny events is admitted element by
// element against the budget.
func TestEventFloodBounded(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	get(t, srv.URL+"/sdk/latest-all", testKey)
	body := "[" + strings.Repeat(`{},`, 1<<20-1) + "{}]" // ~3 MiB, ~1M events
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/bulk", strings.NewReader(body))
	req.Header.Set("Authorization", testKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 202 {
		t.Fatalf("%v %v", err, resp)
	}
	resp.Body.Close()
	r.events.mu.Lock()
	total := r.events.total
	r.events.mu.Unlock()
	if total > maxBufferedPerKey {
		t.Fatalf("buffered %d bytes > per-key cap %d", total, maxBufferedPerKey)
	}
	if r.metrics.get("flagrelay_events_dropped_total") == 0 {
		t.Fatal("overflow not counted")
	}
}

func TestSegmentTargetNeedsContextKind(t *testing.T) {
	ds, err := parseDataSet([]byte(`{"flags":{},"segments":{"s":{"key":"s","version":1,"included":[],"excluded":[],"rules":[],"salt":"s","includedContexts":[{"values":[]}]}}}`))
	if err != nil || len(ds.invalid) != 1 {
		t.Fatalf("segment target without contextKind accepted: %v %v", err, ds.invalid)
	}
}

// Codex r6 #1: on an established stream, a later full put that is older
// than a copy adopted from a peer in the meantime is fenced too.
func TestLaterPutFencedByPeerAdoption(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	sub := openStream(t, srv.URL, testKey)
	sub.next(t)
	r.mu.Lock()
	e := r.envs[testKey]
	r.mu.Unlock()
	// The chain is unhealthy for a moment (as with a primary that reports
	// disconnected) and a fresher OFF copy is adopted from a peer.
	e.mu.Lock()
	e.chainOK = false
	e.mu.Unlock()
	peerDS, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 2, false))))
	e.applyPeerSnapshot(peerDS, time.Now().Add(time.Hour))
	if ev := sub.next(t); !strings.Contains(ev.Data, `"version":2`) {
		t.Fatalf("peer copy not published: %+v", ev)
	}
	// Now the old stream delivers an older full put (ON v1).
	up.setData(dataJSON(flagJSON("a", 2, false)))
	up.send("event: put\ndata: {\"path\":\"/\",\"data\":" + dataJSON(flagJSON("a", 1, true)) + "}\n\n")
	time.Sleep(500 * time.Millisecond)
	for {
		select {
		case ev := <-sub.events:
			if strings.Contains(ev.Data, `"version":1`) {
				t.Fatalf("delayed older put overrode the peer copy: %s", ev.Data)
			}
			continue
		default:
		}
		break
	}
}

// Codex r7 #2-#6: exact-case fields, Rust Kind rules, serde-incompatible
// encodings are set aside; Rust-loadable variants are accepted.
func TestValidationAgainstRustModel(t *testing.T) {
	base := `"key":"f","on":true,"variations":[true,false],"fallthrough":{"variation":0},"salt":"f","prerequisites":[],"rules":[]`
	bad := map[string]string{
		"segment Version":    `{"flags":{},"segments":{"s":{"Version":1,"key":"s","included":[],"excluded":[],"rules":[],"salt":"s"}}}`,
		"Deleted uppercase":  `{"flags":{"f":{"version":1,"Deleted":true,"key":"f"}}}`,
		"kind empty":         `{"flags":{"f":{"version":1,` + base + `,"targets":[{"values":["a"],"variation":0,"contextKind":""}]}}}`,
		"kind multi":         `{"flags":{"f":{"version":1,` + base + `,"targets":[{"values":["a"],"variation":0,"contextKind":"multi"}]}}}`,
		"kind bad chars":     `{"flags":{"f":{"version":1,` + base + `,"targets":[{"values":["a"],"variation":0,"contextKind":"a b"}]}}}`,
		"lone surrogate":     `{"flags":{"f":{"version":1,` + base + `,"targets":[],"x":"\ud800"}}}`,
		"lone low surrogate": `{"flags":{"f":{"version":1,` + base + `,"targets":[],"x":"\udc00"}}}`,
		"number overflow":    `{"flags":{"f":{"version":1,` + base + `,"targets":[],"x":1e400}}}`,
		"duplicate key":      `{"flags":{"f":{"version":1,` + base + `,"targets":[],"key":"g"}}}`,
		"deep nesting":       `{"flags":{"f":{"version":1,` + base + `,"targets":[],"x":` + strings.Repeat("[", 200) + strings.Repeat("]", 200) + `}}}`,
		"segment no version": `{"flags":{},"segments":{"s":{"key":"s","included":[],"excluded":[],"rules":[],"salt":"s"}}}`,
		"debug over u64":     `{"flags":{"f":{"version":1,` + base + `,"targets":[],"debugEventsUntilDate":18446744073709551616}}}`,
	}
	for name, b := range bad {
		ds, err := parseDataSet([]byte(b))
		if err != nil || len(ds.invalid) != 1 {
			t.Errorf("%s: accepted (err=%v invalid=%v)", name, err, ds)
		}
	}
	good := map[string]string{
		"negative variation":   `{"flags":{"f":{"version":1,` + base + `,"targets":[{"values":["a"],"variation":-1}]}}}`,
		"fallthrough null":     `{"flags":{"f":{"version":1,"key":"f","on":true,"variations":[],"fallthrough":null,"salt":"f","prerequisites":[],"rules":[],"targets":[]}}}`,
		"no flag version":      `{"flags":{"f":{` + base + `,"targets":[]}}}`,
		"clause kind numeric":  `{"flags":{"f":{"version":1,"key":"f","on":true,"variations":[],"fallthrough":{},"salt":"f","prerequisites":[],"targets":[],"rules":[{"clauses":[{"attribute":"key","op":"in","values":[],"contextKind":5}],"trackEvents":false}]}}}`,
		"segment weight float": `{"flags":{},"segments":{"s":{"key":"s","version":1,"included":[],"excluded":[],"salt":"s","rules":[{"clauses":[],"weight":50000.5,"rolloutContextKind":7}]}}}`,
		"u64 version":          `{"flags":{"f":{"version":18446744073709551615,` + base + `,"targets":[]}}}`,
		"paired surrogate":     `{"flags":{"f":{"version":1,` + base + `,"targets":[],"x":"\ud83d\ude00"}}}`,
		"kind with dots":       `{"flags":{"f":{"version":1,` + base + `,"targets":[{"values":["a"],"variation":0,"contextKind":"org.tenant-v2_x"}]}}}`,
	}
	for name, b := range good {
		ds, err := parseDataSet([]byte(b))
		if err != nil || len(ds.invalid) != 0 {
			t.Errorf("%s: rejected (err=%v invalid=%v)", name, err, ds.invalid)
		}
	}
}

// Codex r7 #1: a patch arriving after a peer adoption must not let a later,
// older full put on the same stream through.
func TestPatchDoesNotAcknowledgePeerAdoption(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true), flagJSON("b", 1, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	sub := openStream(t, srv.URL, testKey)
	sub.next(t)
	r.mu.Lock()
	e := r.envs[testKey]
	r.mu.Unlock()
	up.mu.Lock()
	var oldStreams []chan string
	for ch := range up.streams {
		oldStreams = append(oldStreams, ch)
	}
	up.mu.Unlock()
	e.mu.Lock()
	e.chainOK = false
	e.mu.Unlock()
	peerDS, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 2, false), flagJSON("b", 1, true))))
	e.applyPeerSnapshot(peerDS, time.Now().Add(time.Hour))
	if ev := sub.next(t); !strings.Contains(ev.Data, `"version":2`) {
		t.Fatalf("peer copy not published: %+v", ev)
	}
	up.setData(dataJSON(flagJSON("a", 2, false), flagJSON("b", 2, false)))
	// The old stream sends an unrelated patch, then an older full put.
	for _, ch := range oldStreams {
		ch <- "event: patch\ndata: {\"path\":\"/flags/b\",\"data\":" + flagJSON("b", 2, false) + "}\n\n"
		ch <- "event: put\ndata: {\"path\":\"/\",\"data\":" + dataJSON(flagJSON("a", 1, true), flagJSON("b", 2, false)) + "}\n\n"
	}
	time.Sleep(500 * time.Millisecond)
	for {
		select {
		case ev := <-sub.events:
			if strings.Contains(ev.Data, `"key":"a","version":1`) {
				t.Fatalf("older put after a patch overrode the peer copy: %s", ev.Data)
			}
			continue
		default:
		}
		break
	}
}

// Codex r8 #1: a keepalive confirmation waits for an in-progress commit, so
// it can never stamp the copy that is about to be replaced.
func TestKeepaliveSerializedWithCommit(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	get(t, srv.URL+"/sdk/latest-all", testKey)
	r.mu.Lock()
	e := r.envs[testKey]
	r.mu.Unlock()
	e.commitMu.Lock() // a commit (disk write) is in progress
	done := make(chan struct{})
	go func() { e.touch(time.Now().Add(time.Hour)); close(done) }()
	select {
	case <-done:
		e.commitMu.Unlock()
		t.Fatal("keepalive confirmation did not wait for the commit")
	case <-time.After(150 * time.Millisecond):
	}
	e.commitMu.Unlock()
	<-done
}

// Codex r9: the persisted confirmation equals the published one, even when a
// keepalive stamped the old copy just before a poll's commit, so after a
// crash a peer holding the old copy with that keepalive time cannot win.
func TestPersistedConfirmationMatchesPublished(t *testing.T) {
	dir := t.TempDir()
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, dir))
	get(t, srv.URL+"/sdk/latest-all", testKey)
	r.mu.Lock()
	e := r.envs[testKey]
	r.mu.Unlock()
	t1 := time.Now()
	t2 := t1.Add(time.Minute) // keepalive on the still-published ON copy
	e.touch(t2)
	off, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 2, false))))
	e.mu.Lock()
	gen := e.gen
	e.mu.Unlock()
	e.applyPut(off, "upstream", t1, true, gen, true) // the poll captured OFF at t1
	d := loadCache(dir)[keyHash(testKey)]
	e.mu.Lock()
	mem := e.confirmedAt
	e.mu.Unlock()
	if d == nil || d.data.Flags["a"].Version != 2 || !d.confirmedAt.Equal(mem.UTC()) && !d.confirmedAt.Equal(mem) {
		t.Fatalf("disk %v/%v vs memory %v", d.confirmedAt, d.data.Flags["a"].Version, mem)
	}
	if d.confirmedAt.Before(t2) {
		t.Fatalf("persisted OFF at %v, older than the keepalive on ON (%v): a peer with ON/%v would win", d.confirmedAt, t2, t2)
	}
}

// Codex r10: a disconnected primary's older copy cannot replace ours even
// if our confirmation advanced after its headers were admitted.
func TestUntrustedPrimaryCopyRecheckedUnderCommitLock(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 2, false)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	get(t, srv.URL+"/sdk/latest-all", testKey) // OFF v2 from upstream
	r.mu.Lock()
	e := r.envs[testKey]
	r.mu.Unlock()
	t1 := time.Now().Add(-time.Minute) // the primary's (disconnected) ON copy
	e.touch(time.Now())                // our OFF confirmed after that
	on, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 1, true))))
	e.mu.Lock()
	gen := e.gen
	e.mu.Unlock()
	e.applyPut(on, "primary", t1, true, gen, false)
	if _, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey); !strings.Contains(body, `"version":2`) {
		t.Fatalf("older untrusted primary copy replaced ours: %s", body)
	}
}

// Codex r11: an untrusted stream put that is older than our copy is refused
// with errPeerStale, so the stream is dropped before its queued patches.
func TestOlderUntrustedStreamPutDropsTheStream(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("f", 1, false)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, ""))
	get(t, srv.URL+"/sdk/latest-all", testKey)
	r.mu.Lock()
	e := r.envs[testKey]
	r.mu.Unlock()
	e.mu.Lock()
	ack := e.extEpoch
	e.mu.Unlock()
	old := time.Now().Add(-time.Hour)
	put := sseEvent{Name: "put", Data: `{"path":"/","data":` + dataJSON(flagJSON("f", 101, true)) + `}`}
	err := e.handleUpstreamEvent(context.Background(), put, "primary", old, ack+1, false)
	if !errors.Is(err, errPeerStale) {
		t.Fatalf("got %v, want errPeerStale", err)
	}
	if _, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey); !strings.Contains(body, `"version":1,`) {
		t.Fatalf("older untrusted put applied: %s", body)
	}
}

// Codex r12: end to end through streamOnce: a refused older put from an
// untrusted primary ends the stream, so its queued patch never applies.
func TestRefusedPrimaryPutStopsQueuedPatch(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("f", 1, false)))
	var serving atomic.Bool
	var relayEnv atomic.Pointer[env]
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !serving.Load() {
			http.Error(w, "starting", 503)
			return
		}
		switch r.URL.Path {
		case "/readyz":
			w.Write([]byte("ok"))
		case "/all":
			e := relayEnv.Load()
			e.mu.Lock()
			own := e.confirmedAt
			e.mu.Unlock()
			w.Header().Set(hdrConfirmedAt, fmt.Sprint(own.UnixMilli()+1)) // passes the header check
			w.Header().Set(hdrUpstream, "disconnected")
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			for i := 0; i < 200; i++ { // until the follower accepted our headers
				e.mu.Lock()
				via := e.via
				e.mu.Unlock()
				if via == "primary" {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			e.touch(time.Now().Add(time.Hour)) // ours becomes fresher before the put lands
			fmt.Fprintf(w, "event: put\ndata: {\"path\":\"/\",\"data\":%s}\n\n", dataJSON(flagJSON("f", 101, true)))
			fmt.Fprintf(w, "event: patch\ndata: {\"path\":\"/flags/f\",\"data\":%s}\n\n", flagJSON("f", 102, true))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.Error(w, "x", 404)
		}
	}))
	defer primary.Close()
	cfg := testConfig(up.srv.URL, "")
	cfg.primaryURI = primary.URL
	r, srv, _, _ := startRelayCfg(t, cfg)
	if _, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey); !strings.Contains(body, `"version":1,`) {
		t.Fatalf("setup: %s", body)
	}
	r.mu.Lock()
	relayEnv.Store(r.envs[testKey])
	r.mu.Unlock()
	serving.Store(true) // primaryWatch moves the follower to the primary
	time.Sleep(2500 * time.Millisecond)
	if _, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey); !strings.Contains(body, `"version":1,`) {
		t.Fatalf("patch from a refused stream applied: %s", body)
	}
}

// Codex r13: after a poll publishes a re-created flag, a queued patch or
// delete from the (now superseded) stream must not apply.
func TestSupersededStreamPatchAndDeleteDoNotApply(t *testing.T) {
	dir := t.TempDir()
	up := newMockUpstream(t, dataJSON(flagJSON("f", 101, true), flagJSON("g", 5, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, dir))
	sub := openStream(t, srv.URL, testKey)
	sub.next(t)
	r.mu.Lock()
	e := r.envs[testKey]
	r.mu.Unlock()
	up.mu.Lock()
	var oldStreams []chan string
	for ch := range up.streams {
		oldStreams = append(oldStreams, ch)
	}
	up.mu.Unlock()
	// Upstream deletes and re-creates f (version restarts) and drops g; the
	// reconciliation poll publishes that.
	up.setData(dataJSON(flagJSON("f", 1, false)))
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ev := sub.next(t); !strings.Contains(ev.Data, `"version":1,`) {
		t.Fatalf("poll result not published: %+v", ev)
	}
	// The old stream still delivers old-incarnation events.
	for _, ch := range oldStreams {
		// delete first (would erase the re-created f), then patches
		ch <- "event: delete\ndata: {\"path\":\"/flags/f\",\"version\":102}\n\n"
		ch <- "event: patch\ndata: {\"path\":\"/flags/f\",\"data\":" + flagJSON("f", 103, true) + "}\n\n"
		ch <- "event: patch\ndata: {\"path\":\"/flags/g\",\"data\":" + flagJSON("g", 6, true) + "}\n\n"
	}
	time.Sleep(500 * time.Millisecond)
	for { // no SDK may ever see the old incarnation again
		select {
		case ev := <-sub.events:
			if ev.Name == "delete" || strings.Contains(ev.Data, `"version":103`) || strings.Contains(ev.Data, `"key":"g"`) {
				t.Fatalf("superseded stream event reached an SDK: %s %s", ev.Name, ev.Data)
			}
			continue
		default:
		}
		break
	}
	_, body, _ := get(t, srv.URL+"/sdk/latest-all", testKey)
	if !strings.Contains(body, `"key":"f","version":1,`) || strings.Contains(body, `"key":"g"`) {
		t.Fatalf("superseded stream events applied: %s", body)
	}
	d := loadCache(dir)[keyHash(testKey)]
	if d == nil || d.data.Flags["f"].Version != 1 || d.data.Flags["f"].Deleted || (d.data.Flags["g"].Version > 5 && !d.data.Flags["g"].Deleted) {
		t.Fatalf("disk copy wrong: %+v", d)
	}
}

// Codex r15: a peer copy adopted at start whose write failed is retried
// until it is on disk, without any further flag change.
func TestStartupPeerAdoptionWriteRetried(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 10, true)))
	up.down.Store(true)
	// A peer with a fresher copy (v12).
	cfgA := testConfig(up.srv.URL, t.TempDir())
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	rA := newRelay(ctxA, cfgA)
	rA.start(ctxA)
	fresh, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 12, false))))
	rA.mu.Lock()
	rA.disk[keyHash(testKey)] = &diskEntry{data: fresh, confirmedAt: time.Now()}
	rA.mu.Unlock()
	peerA := httptest.NewServer(rA.peerMux())
	defer peerA.Close()
	// B has v10 on disk, and its cache dir is read-only when it starts.
	dirB := t.TempDir()
	old, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 10, true))))
	if err := writeCacheFile(dirB, keyHash(testKey), old.marshalWithTombstones(), "upstream", time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	os.Chmod(dirB, 0o500)
	defer os.Chmod(dirB, 0o700)
	cfgB := testConfig(up.srv.URL, dirB)
	cfgB.peerAddrs = []string{hostPort(peerA.URL)}
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	rB := newRelay(ctxB, cfgB)
	rB.start(ctxB)
	if rB.metrics.get("flagrelay_cache_write_errors_total") == 0 {
		t.Fatal("failed start-up write not counted")
	}
	os.Chmod(dirB, 0o700) // disk recovers; no flag changes from here on
	waitFor(t, "retry wrote v12", func() bool {
		d := loadCache(dirB)[keyHash(testKey)]
		return d != nil && d.data.Flags["a"].Version == 12
	})
}

// Codex r16: a start-up retry never runs once an env owns the key, so it
// cannot overwrite the env's newer commit.
func TestDiskRetryYieldsToEnv(t *testing.T) {
	dir := t.TempDir()
	up := newMockUpstream(t, dataJSON(flagJSON("a", 13, false)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, dir))
	old, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 12, true))))
	r.mu.Lock()
	r.disk[keyHash(testKey)] = &diskEntry{data: old, confirmedAt: time.Now().Add(-time.Hour), dirty: true}
	r.mu.Unlock()
	get(t, srv.URL+"/sdk/latest-all", testKey) // env created, commits v13 from upstream
	waitFor(t, "v13 on disk", func() bool {
		d := loadCache(dir)[keyHash(testKey)]
		return d != nil && d.data.Flags["a"].Version == 13
	})
	r.writeDiskEntry(keyHash(testKey)) // a late retry for the old entry
	time.Sleep(3 * cfgPersistRetryForTests)
	if d := loadCache(dir)[keyHash(testKey)]; d == nil || d.data.Flags["a"].Version != 13 {
		t.Fatalf("start-up retry overwrote the env's newer copy: %+v", d)
	}
}

const cfgPersistRetryForTests = 200 * time.Millisecond

// Codex r17: a stalled start-up retry for one key does not block requests
// for another key (r.mu is not held during its disk I/O).
func TestStalledDiskRetryDoesNotBlockOtherKeys(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	r, srv, _ := startRelay(t, testConfig(up.srv.URL, t.TempDir()))
	other := strings.Repeat("ab", 32)
	ds, _ := parseDataSet([]byte(dataJSON(flagJSON("x", 1, true))))
	r.mu.Lock()
	r.disk[other] = &diskEntry{data: ds, dirty: true}
	r.mu.Unlock()
	release, inWrite := make(chan struct{}), make(chan struct{})
	defer close(release)
	hook := func(hash string) {
		if hash == other {
			close(inWrite)
			<-release // the other key's write is stuck on disk
		}
	}
	testWriteHook.Store(&hook)
	defer testWriteHook.Store(nil)
	go r.writeDiskEntry(other)
	<-inWrite
	done := make(chan error, 1)
	go func() {
		code, _, _ := get(t, srv.URL+"/sdk/latest-all", testKey)
		if code != 200 {
			done <- fmt.Errorf("poll %d", code)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/all", nil)
		req.Header.Set("Authorization", testKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- err
			return
		}
		defer resp.Body.Close()
		ev, err := newSSEReader(resp.Body, nil).next()
		if err != nil || ev.Name != "put" {
			done <- fmt.Errorf("stream: %v %+v", err, ev)
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("requests blocked behind another key's stalled disk write")
	}
}
