package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testAdminToken = "admin-token-0123456789abcdef"

// node is one relay replica with its SDK, peer and admin listeners.
type node struct {
	r                *relay
	sdk, peer, admin *httptest.Server
	cancel           context.CancelFunc
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func serveOn(ln net.Listener, h http.Handler) *httptest.Server {
	s := httptest.NewUnstartedServer(h)
	s.Listener.Close()
	s.Listener = ln
	s.Start()
	return s
}

// startNode starts a relay; peerLn/adminLn may be nil (fresh listeners).
func startNode(t *testing.T, cfg relayConfig, peerLn, adminLn net.Listener) *node {
	t.Helper()
	if cfg.adminToken == "" {
		cfg.adminToken = testAdminToken
	}
	if cfg.overridesFile == "" && cfg.cacheDir != "" {
		cfg.overridesFile = filepath.Join(cfg.cacheDir, "overrides.json")
	}
	if cfg.overrideSync == 0 {
		cfg.overrideSync = 200 * time.Millisecond
	}
	if peerLn == nil {
		peerLn = listen(t)
	}
	if adminLn == nil {
		adminLn = listen(t)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := newRelay(ctx, cfg)
	go r.events.run(ctx)
	n := &node{r: r, cancel: cancel}
	n.sdk = httptest.NewServer(r.mux())
	n.peer = serveOn(peerLn, r.peerMux())
	n.admin = serveOn(adminLn, r.admin.mux())
	r.start(ctx)
	t.Cleanup(n.stop)
	return n
}

func (n *node) stop() {
	n.sdk.CloseClientConnections()
	n.sdk.Close()
	n.peer.Close()
	n.admin.Close()
	n.cancel()
}

func adminDo(t *testing.T, method, url, body string, hdr ...string) (int, string, http.Header) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, url, rd)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	for i := 0; i+1 < len(hdr); i += 2 {
		if hdr[i+1] == "" {
			req.Header.Del(hdr[i])
		} else {
			req.Header.Set(hdr[i], hdr[i+1])
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func putOverride(t *testing.T, adminURL, key, body string, hdr ...string) string {
	t.Helper()
	code, b, _ := adminDo(t, http.MethodPut, adminURL+"/admin/flags/"+key+"/override", body, hdr...)
	if code != 200 {
		t.Fatalf("PUT override %s: %d %s", key, code, b)
	}
	return b
}

// servedFlag returns what /sdk/latest-all serves for key (nil: absent),
// after checking the whole document still passes the SDK validation.
func servedFlag(t *testing.T, sdkURL, key string, hdr ...string) *sourceInfo {
	t.Helper()
	code, body, _ := get(t, sdkURL+"/sdk/latest-all", testKey, hdr...)
	if code != 200 {
		t.Fatalf("latest-all: %d %s", code, body)
	}
	ds, err := parseDataSet([]byte(body))
	if err != nil || len(ds.invalid) > 0 {
		t.Fatalf("served data does not validate: %v %v\n%s", err, ds.invalid, body)
	}
	it, ok := ds.Flags[key]
	if !ok {
		return nil
	}
	return defaultValue(it.Raw)
}

func valueOf(t *testing.T, sdkURL, key string, hdr ...string) string {
	t.Helper()
	si := servedFlag(t, sdkURL, key, hdr...)
	if si == nil {
		return "<absent>"
	}
	return string(si.Value)
}

func eventVersion(t *testing.T, ev sseEvent) uint64 {
	t.Helper()
	var m struct {
		Data struct {
			Version uint64 `json:"version"`
		} `json:"data"`
		Version uint64 `json:"version"`
	}
	if err := json.Unmarshal([]byte(ev.Data), &m); err != nil {
		t.Fatalf("event %q: %v", ev.Data, err)
	}
	if m.Version != 0 {
		return m.Version
	}
	return m.Data.Version
}

func TestAdminDisabledWithoutToken(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	cfg := testConfig(up.srv.URL, "")
	r, _, _ := startRelay(t, cfg)
	if r.admin.enabled() {
		t.Fatal("admin API enabled without RELAY_ADMIN_TOKEN")
	}
	if a := newOverrideAdmin(r.overrides, r, "short", nil, "", r.metrics); a.enabled() {
		t.Fatal("a token shorter than 16 characters must disable the admin API")
	}
	// The peer endpoint refuses everything when the admin API is off.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/relay/v1/overrides", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	r.peerMux().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("peer overrides endpoint with admin off: %d", rec.Code)
	}
}

func TestAdminAuth(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	for _, auth := range []string{"", "Bearer wrong-token-0123456789", testAdminToken, "Basic " + testAdminToken} {
		code, _, _ := adminDo(t, http.MethodGet, n.admin.URL+"/admin/flags", "", "Authorization", auth)
		if code != http.StatusUnauthorized {
			t.Fatalf("auth %q: %d, want 401", auth, code)
		}
	}
	code, _, _ := adminDo(t, http.MethodPut, n.admin.URL+"/admin/flags/a/override", `{"value":false}`, "Authorization", "Bearer nope")
	if code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated PUT: %d", code)
	}
	// SDK endpoints are not served on the admin port, nor the admin API on the SDK port.
	if code, _, _ := get(t, n.admin.URL+"/sdk/latest-all", testKey); code != http.StatusNotFound && code != http.StatusUnauthorized {
		t.Fatalf("admin port serves SDK data: %d", code)
	}
	if code, _, _ := adminDo(t, http.MethodGet, n.sdk.URL+"/admin/flags", ""); code != http.StatusNotFound {
		t.Fatalf("SDK port serves the admin API: %d", code)
	}
}

func TestOverrideServedOnEverySDKEndpoint(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 5, true), flagJSON("b", 1, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	sAll := openStream(t, n.sdk.URL, testKey)
	if ev := sAll.next(t); ev.Name != "put" {
		t.Fatalf("first event %+v", ev)
	}
	sLegacy := openLegacyStream(t, n.sdk.URL, testKey)
	sLegacy.next(t)

	putOverride(t, n.admin.URL, "a", `{"value":false,"set_by":"ops@example.com"}`)
	ev := sAll.next(t)
	if ev.Name != "patch" || !strings.Contains(ev.Data, `"/flags/a"`) || !strings.Contains(ev.Data, `"variations":[false]`) {
		t.Fatalf("stream: want patch of a to false, got %+v", ev)
	}
	if v := eventVersion(t, ev); v <= 5 {
		t.Fatalf("patch version %d must be above the version the SDK holds (5)", v)
	}
	if ev := sLegacy.next(t); ev.Name != "patch" || !strings.Contains(ev.Data, `"path":"/a"`) || !strings.Contains(ev.Data, `[false]`) {
		t.Fatalf("legacy stream: %+v", ev)
	}
	if v := valueOf(t, n.sdk.URL, "a"); v != "false" {
		t.Fatalf("/sdk/latest-all a = %s, want false", v)
	}
	if code, body, _ := get(t, n.sdk.URL+"/sdk/latest-flags/a", testKey); code != 200 || !strings.Contains(body, `"variations":[false]`) {
		t.Fatalf("/sdk/latest-flags/a: %d %s", code, body)
	}
	if code, body, _ := get(t, n.sdk.URL+"/sdk/latest-flags", testKey); code != 200 || !strings.Contains(body, `"variations":[false]`) {
		t.Fatalf("/sdk/latest-flags: %d %s", code, body)
	}
	// A new stream gets the override in its put.
	s2 := openStream(t, n.sdk.URL, testKey)
	if ev := s2.next(t); ev.Name != "put" || !strings.Contains(ev.Data, `"variations":[false]`) {
		t.Fatalf("new stream put: %+v", ev)
	}
	// Peers always get the source data.
	if v := valueOf(t, n.sdk.URL, "a", hdrPeer, "1"); v != "true" {
		t.Fatalf("peer poll a = %s, want the upstream value true", v)
	}
	if v := valueOf(t, n.sdk.URL, "b"); v != "true" {
		t.Fatalf("b = %s, untouched flags keep their value", v)
	}

	// The admin API shows source, override and effective value.
	code, body, hdr := adminDo(t, http.MethodGet, n.admin.URL+"/admin/flags/a", "")
	var fe flagEntry
	if code != 200 || json.Unmarshal([]byte(body), &fe) != nil {
		t.Fatalf("GET a: %d %s", code, body)
	}
	if fe.Upstream == nil || string(fe.Upstream.Value) != "true" || fe.Upstream.Version != 5 ||
		fe.Override == nil || string(fe.Override.Value) != "false" || fe.Override.SetBy != "ops@example.com" ||
		fe.Override.UpstreamVersion == nil || *fe.Override.UpstreamVersion != 5 || string(fe.Effective) != "false" || fe.Source != "override" {
		t.Fatalf("GET a: %s", body)
	}
	if hdr.Get("ETag") == `"0"` {
		t.Fatalf("ETag must be the override revision")
	}

	// Reset: the SDKs get the upstream flag back at a higher version than the override.
	before := eventVersion(t, ev)
	code, body, _ = adminDo(t, http.MethodDelete, n.admin.URL+"/admin/flags/a/override?set_by=ops", "")
	if code != 200 {
		t.Fatalf("DELETE: %d %s", code, body)
	}
	ev = sAll.next(t)
	if ev.Name != "patch" || !strings.Contains(ev.Data, `"variations":[true,false]`) || eventVersion(t, ev) <= before {
		t.Fatalf("after reset want upstream flag above v%d, got %+v", before, ev)
	}
	if v := valueOf(t, n.sdk.URL, "a"); v != "true" {
		t.Fatalf("after reset a = %s", v)
	}
	// Later upstream patches still reach the SDK (served version keeps increasing).
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 6, false) + "}\n\n")
	ev = sAll.next(t)
	if ev.Name != "patch" || !strings.Contains(ev.Data, `"on":false`) || eventVersion(t, ev) <= before {
		t.Fatalf("upstream patch after reset: %+v", ev)
	}
}

func openLegacyStream(t *testing.T, url, key string) *streamClient {
	t.Helper()
	// openStream appends /all; reuse it through a tiny proxy path rewrite.
	px := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, url+"/flags", nil)
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		buf := make([]byte, 32<<10)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				w.Write(buf[:n])
				w.(http.Flusher).Flush()
			}
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(px.Close)
	return openStream(t, px.URL, key)
}

func TestOverrideValueKinds(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	get(t, n.sdk.URL+"/sdk/latest-all", testKey)
	for key, v := range map[string]string{
		"bool-flag":   `true`,
		"string-flag": `"debug"`,
		"number-flag": `42.5`,
		"json-flag":   `{"version":1,"services":{"rex-worker":{"level":"debug","pinned":true}}}`,
		"array-flag":  `[1,2,3]`,
	} {
		putOverride(t, n.admin.URL, key, `{"value":`+v+`}`)
		if got := valueOf(t, n.sdk.URL, key); got != v {
			t.Fatalf("%s served %s, want %s", key, got, v)
		}
	}
	for _, bad := range []string{`{"value":null}`, `{}`, `{"value":tru}`, `{"value":1,"extra":2}`, `not json`} {
		if code, body, _ := adminDo(t, http.MethodPut, n.admin.URL+"/admin/flags/x/override", bad); code != 400 {
			t.Fatalf("PUT %s: %d %s, want 400", bad, code, body)
		}
	}
	if code, _, _ := adminDo(t, http.MethodPut, n.admin.URL+"/admin/flags/bad%20key/override", `{"value":1}`); code != 400 {
		t.Fatalf("bad key: %d", code)
	}
}

// A flag upstream does not define is synthesized, and removed again.
func TestSynthesizedFlag(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	s := openStream(t, n.sdk.URL, testKey)
	s.next(t)
	putOverride(t, n.admin.URL, "log-levels", `{"value":"{\"version\":1,\"services\":{}}","local_only":true}`)
	ev := s.next(t)
	if ev.Name != "patch" || !strings.Contains(ev.Data, `"/flags/log-levels"`) {
		t.Fatalf("want a patch creating log-levels, got %+v", ev)
	}
	created := eventVersion(t, ev)
	if v := valueOf(t, n.sdk.URL, "log-levels"); v != `"{\"version\":1,\"services\":{}}"` {
		t.Fatalf("log-levels = %s", v)
	}
	code, body, _ := adminDo(t, http.MethodGet, n.admin.URL+"/admin/flags", "")
	if code != 200 || !strings.Contains(body, `"key":"log-levels"`) || !strings.Contains(body, `"upstream":null`) {
		t.Fatalf("list: %d %s", code, body)
	}
	adminDo(t, http.MethodDelete, n.admin.URL+"/admin/flags/log-levels/override", "")
	ev = s.next(t)
	if ev.Name != "delete" || !strings.Contains(ev.Data, `"/flags/log-levels"`) || eventVersion(t, ev) <= created {
		t.Fatalf("want delete above v%d, got %+v", created, ev)
	}
	if v := valueOf(t, n.sdk.URL, "log-levels"); v != "<absent>" {
		t.Fatalf("log-levels after reset = %s", v)
	}
	if code, _, _ := adminDo(t, http.MethodGet, n.admin.URL+"/admin/flags/log-levels", ""); code != 404 {
		t.Fatalf("GET removed synthesized flag: %d", code)
	}
}

// Last write wins: a later upstream change replaces an override, except a
// local_only one.
func TestUpstreamChangeReplacesOverride(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 3, true), flagJSON("b", 3, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	s := openStream(t, n.sdk.URL, testKey)
	s.next(t)
	putOverride(t, n.admin.URL, "a", `{"value":false}`)
	putOverride(t, n.admin.URL, "b", `{"value":false,"local_only":true}`)
	s.next(t)
	s.next(t)
	// A reconcile poll or reconnect delivering the same version keeps them.
	up.send("event: put\ndata: {\"path\":\"/\",\"data\":" + dataJSON(flagJSON("a", 3, true), flagJSON("b", 3, true)) + "}\n\n")
	time.Sleep(100 * time.Millisecond)
	if valueOf(t, n.sdk.URL, "a") != "false" || valueOf(t, n.sdk.URL, "b") != "false" {
		t.Fatal("an unchanged upstream version must not drop overrides")
	}
	time.Sleep(5 * time.Millisecond) // the upstream change comes after the overrides
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 4, true) + "}\n\n")
	up.send("event: patch\ndata: {\"path\":\"/flags/b\",\"data\":" + flagJSON("b", 4, true) + "}\n\n")
	var sawA, sawB bool
	for i := 0; i < 2; i++ {
		ev := s.next(t)
		t.Logf("event %s %s", ev.Name, ev.Data)
		switch {
		case strings.Contains(ev.Data, `"/flags/a"`):
			sawA = strings.Contains(ev.Data, `"variations":[true,false]`)
		case strings.Contains(ev.Data, `"/flags/b"`):
			sawB = strings.Contains(ev.Data, `"variations":[false]`)
		}
	}
	if !sawA || !sawB {
		t.Fatalf("want a back to upstream (%v) and b still overridden (%v)", sawA, sawB)
	}
	if valueOf(t, n.sdk.URL, "a") != "true" || valueOf(t, n.sdk.URL, "b") != "false" {
		t.Fatal("served values after the upstream change")
	}
	_, body, _ := adminDo(t, http.MethodGet, n.admin.URL+"/admin/flags/a", "")
	if !strings.Contains(body, `"override":null`) {
		t.Fatalf("a's override should be gone: %s", body)
	}
	// The drop is persisted.
	st := newOverrideStore(n.r.cfg.overridesFile, "x", newMetrics())
	if st.get("a").live() || !st.get("b").live() {
		t.Fatalf("persisted state: a=%+v b=%+v", st.get("a"), st.get("b"))
	}
	if st.get("a").Reason != "upstream" {
		t.Fatalf("tombstone reason %q", st.get("a").Reason)
	}
}

func TestOverridesSurviveRestart(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	dir := t.TempDir()
	n := startNode(t, testConfig(up.srv.URL, dir), nil, nil)
	get(t, n.sdk.URL+"/sdk/latest-all", testKey)
	putOverride(t, n.admin.URL, "a", `{"value":false}`)
	putOverride(t, n.admin.URL, "synth", `{"value":"x"}`)
	n.stop()
	fi, err := os.Stat(filepath.Join(dir, "overrides.json"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("overrides file: %v %v", err, fi)
	}
	// Restart while upstream is down: served from the disk cache + overrides.
	up.down.Store(true)
	n2 := startNode(t, testConfig(up.srv.URL, dir), nil, nil)
	if v := valueOf(t, n2.sdk.URL, "a"); v != "false" {
		t.Fatalf("after restart a = %s", v)
	}
	if v := valueOf(t, n2.sdk.URL, "synth"); v != `"x"` {
		t.Fatalf("after restart synth = %s", v)
	}
}

func TestAdminWriteRefusedWhenNotPersisted(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "file")
	os.WriteFile(blocker, []byte("x"), 0o600)
	st := newOverrideStore(filepath.Join(blocker, "overrides.json"), "n", newMetrics()) // parent is a file
	if _, err := st.set("a", json.RawMessage(`true`), "", false, "", nil); err == nil {
		t.Fatal("set must fail when the override cannot be persisted")
	}
	if st.get("a") != nil {
		t.Fatal("a refused write must not be served")
	}
	// Merged (peer) changes are served anyway and flagged for retry.
	st.merge(overrideMap{"b": {Value: json.RawMessage(`1`), Rev: 5, Origin: "p"}})
	if !st.get("b").live() || st.persistError() == "" {
		t.Fatal("peer change should be served with a persist error")
	}
}

func TestIfMatch(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	get(t, n.sdk.URL+"/sdk/latest-all", testKey)
	// "0": only when there is no override yet.
	putOverride(t, n.admin.URL, "a", `{"value":false}`, "If-Match", `"0"`)
	code, body, _ := adminDo(t, http.MethodPut, n.admin.URL+"/admin/flags/a/override", `{"value":true}`, "If-Match", `"0"`)
	if code != http.StatusPreconditionFailed || !strings.Contains(body, `"rev"`) {
		t.Fatalf("stale If-Match: %d %s", code, body)
	}
	_, _, hdr := adminDo(t, http.MethodGet, n.admin.URL+"/admin/flags/a", "")
	etag := hdr.Get("ETag")
	putOverride(t, n.admin.URL, "a", `{"value":true}`, "If-Match", etag)
	if code, _, _ := adminDo(t, http.MethodDelete, n.admin.URL+"/admin/flags/a/override", "", "If-Match", etag); code != 412 {
		t.Fatalf("DELETE with an old revision: %d", code)
	}
}

// Replication: a write on either replica reaches both, followers forward
// writes to the primary, and a replica restarted from an old disk adopts the
// newer records of its peer before it is ready.
func TestOverrideReplication(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	lnPpeer, lnPadmin, lnFpeer := listen(t), listen(t), listen(t)
	dirP := t.TempDir()
	cfgP := testConfig(up.srv.URL, dirP)
	cfgP.peerAddrs = []string{lnFpeer.Addr().String()}
	cfgF := testConfig(up.srv.URL, t.TempDir())
	cfgF.peerAddrs = []string{lnPpeer.Addr().String()}
	primaryAddr := lnPadmin.Addr().String()
	P := startNode(t, cfgP, lnPpeer, lnPadmin)
	cfgF.primaryURI = P.sdk.URL
	cfgF.primaryAdmin = "http://" + primaryAddr
	F := startNode(t, cfgF, lnFpeer, nil)
	sF := openStream(t, F.sdk.URL, testKey)
	sF.next(t)
	get(t, P.sdk.URL+"/sdk/latest-all", testKey)

	// Write on the primary: pushed to the follower before the answer.
	body := putOverride(t, P.admin.URL, "a", `{"value":false}`)
	if !strings.Contains(body, `"replicated":1`) {
		t.Fatalf("PUT on primary: %s", body)
	}
	if ev := sF.next(t); ev.Name != "patch" || !strings.Contains(ev.Data, `[false]`) {
		t.Fatalf("follower SDK stream: %+v", ev)
	}
	// The follower still streams the source data from the primary.
	if v := valueOf(t, P.sdk.URL, "a", hdrPeer, "1"); v != "true" {
		t.Fatalf("primary hands peers the overridden value: %s", v)
	}

	// Write on the follower: forwarded to the primary, which orders it.
	putOverride(t, F.admin.URL, "b", `{"value":"from-follower"}`)
	if rec := P.r.overrides.get("b"); !rec.live() || rec.Origin != P.r.overrides.origin {
		t.Fatalf("forwarded write not applied by the primary: %+v", rec)
	}
	if v := valueOf(t, F.sdk.URL, "b"); v != `"from-follower"` {
		t.Fatalf("follower serves b = %s", v)
	}

	// Primary down: the follower applies writes itself.
	P.stop()
	putOverride(t, F.admin.URL, "c", `{"value":3}`)
	adminDo(t, http.MethodDelete, F.admin.URL+"/admin/flags/a/override", "")
	if v := valueOf(t, F.sdk.URL, "c"); v != "3" {
		t.Fatalf("follower alone: c = %s", v)
	}

	// The primary restarts from its old disk and adopts the follower's newer
	// records before it reports ready.
	P2 := startNode(t, cfgP, listenOn(t, lnPpeer.Addr().String()), listenOn(t, primaryAddr))
	if !P2.r.overrides.get("c").live() || P2.r.overrides.get("a").live() {
		t.Fatalf("restarted primary: c=%+v a=%+v", P2.r.overrides.get("c"), P2.r.overrides.get("a"))
	}
	if v := valueOf(t, P2.sdk.URL, "a"); v != "true" {
		t.Fatalf("restarted primary serves a = %s", v)
	}
}

func listenOn(t *testing.T, addr string) net.Listener {
	t.Helper()
	var ln net.Listener
	var err error
	for i := 0; i < 50; i++ {
		if ln, err = net.Listen("tcp", addr); err == nil {
			return ln
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("listen %s: %v", addr, err)
	return nil
}

// A periodic pull converges a peer that missed a push.
func TestOverridePullConverges(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	lnA, lnB := listen(t), listen(t)
	cfgA := testConfig(up.srv.URL, t.TempDir())
	cfgA.peerAddrs = []string{lnB.Addr().String()}
	cfgB := testConfig(up.srv.URL, t.TempDir())
	cfgB.peerAddrs = []string{lnA.Addr().String()}
	A := startNode(t, cfgA, lnA, nil)
	B := startNode(t, cfgB, lnB, nil)
	// Write straight into A's store (no push), as if the push had failed.
	A.r.overrides.set("x", json.RawMessage(`true`), "", false, "", nil)
	waitFor(t, "B pulls x", func() bool { return B.r.overrides.get("x").live() })
}

// Air-gapped: upstream never answered and nothing is cached; SDKs still get
// the overrides (and nothing else), peers still get 503.
func TestOverridesServedWithoutUpstreamData(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	up.down.Store(true)
	cfg := testConfig(up.srv.URL, t.TempDir())
	cfg.firstWait = 300 * time.Millisecond
	n := startNode(t, cfg, nil, nil)
	if code, _, _ := get(t, n.sdk.URL+"/sdk/latest-all", testKey); code != 503 {
		t.Fatalf("no data, no override: %d", code)
	}
	putOverride(t, n.admin.URL, "log-levels", `{"value":"{}","local_only":true}`)
	if v := valueOf(t, n.sdk.URL, "log-levels"); v != `"{}"` {
		t.Fatalf("override-only serving: %s", v)
	}
	if code, _, _ := get(t, n.sdk.URL+"/sdk/latest-all", testKey, hdrPeer, "1"); code != 503 {
		t.Fatalf("a peer must not get override-only data: %d", code)
	}
	s := openStream(t, n.sdk.URL, testKey)
	if ev := s.next(t); ev.Name != "put" || !strings.Contains(ev.Data, "log-levels") {
		t.Fatalf("stream put: %+v", ev)
	}
	// Upstream comes back: the SDK gets the full data with the override on top.
	up.down.Store(false)
	ev := s.next(t)
	if ev.Name != "put" || !strings.Contains(ev.Data, `"a"`) || !strings.Contains(ev.Data, "log-levels") {
		t.Fatalf("after upstream recovery: %+v", ev)
	}
}

func TestMergeOrdering(t *testing.T) {
	st := newOverrideStore("", "me", newMetrics())
	old := &overrideRecord{Value: json.RawMessage(`1`), Rev: 10, Origin: "a"}
	st.merge(overrideMap{"k": old})
	// Older revision ignored; same revision ordered by origin; tombstone wins when newer.
	if ch := st.merge(overrideMap{"k": {Value: json.RawMessage(`2`), Rev: 9, Origin: "z"}}); ch != nil || string(st.get("k").Value) != "1" {
		t.Fatal("older revision applied")
	}
	st.merge(overrideMap{"k": {Value: json.RawMessage(`3`), Rev: 10, Origin: "b"}})
	if string(st.get("k").Value) != "3" {
		t.Fatal("same revision: higher origin must win")
	}
	st.merge(overrideMap{"k": {Deleted: true, Rev: 11, Origin: "a"}})
	if st.get("k").live() {
		t.Fatal("newer tombstone must win")
	}
	// Same change: recorded source versions are unioned.
	st.merge(overrideMap{"j": {Value: json.RawMessage(`1`), Rev: 20, Origin: "a", Upstream: map[string]uint64{"e1": 1}}})
	st.merge(overrideMap{"j": {Value: json.RawMessage(`1`), Rev: 20, Origin: "a", Upstream: map[string]uint64{"e2": 7}}})
	if u := st.get("j").Upstream; u["e1"] != 1 || u["e2"] != 7 {
		t.Fatalf("upstream versions %v", u)
	}
	// The clock never issues a revision at or below one it has seen.
	st.merge(overrideMap{"far": {Value: json.RawMessage(`1`), Rev: 1 << 60, Origin: "a"}})
	rec, _ := st.set("n", json.RawMessage(`1`), "", false, "", nil)
	if rec.Rev <= 1<<60 {
		t.Fatalf("rev %d not above the highest seen", rec.Rev)
	}
	// Invalid records from a peer are ignored.
	if ch := st.merge(overrideMap{"bad key": {Value: json.RawMessage(`1`), Rev: 1 << 61, Origin: "a"}, "nil": {Value: json.RawMessage(`null`), Rev: 1 << 61, Origin: "a"}}); ch != nil {
		t.Fatalf("invalid records merged: %v", ch)
	}
}

func TestMonotonicVersions(t *testing.T) {
	ds, _ := parseDataSet([]byte(dataJSON(flagJSON("a", 7, true))))
	ovs := overrideMap{}
	v0 := buildView(ds, ovs, nil)
	if v0 != ds {
		t.Fatal("no overrides: the view must be the data itself")
	}
	ovs["a"] = &overrideRecord{Value: json.RawMessage(`false`), Rev: 1, Origin: "x"}
	ovs["s"] = &overrideRecord{Value: json.RawMessage(`"v"`), Rev: 1, Origin: "x"}
	v1, bump := monotonic(v0, ds, ovs, nil, []string{"a", "s"})
	if v1.Flags["a"].Version <= 7 || v1.Flags["s"].Version < 1 {
		t.Fatalf("override versions a=%d s=%d", v1.Flags["a"].Version, v1.Flags["s"].Version)
	}
	ovs2 := overrideMap{"a": {Deleted: true, Rev: 2, Origin: "x"}, "s": {Deleted: true, Rev: 2, Origin: "x"}}
	v2, _ := monotonic(v1, ds, ovs2, bump, []string{"a", "s"})
	if v2.Flags["a"].Version <= v1.Flags["a"].Version || !v2.Flags["s"].Deleted || v2.Flags["s"].Version <= v1.Flags["s"].Version {
		t.Fatalf("after removal a=%+v s=%+v", v2.Flags["a"], v2.Flags["s"])
	}
	for k, it := range v2.Flags {
		if !it.Deleted {
			if _, err := parseItem("flags", it.Raw); err != nil {
				t.Fatalf("%s does not validate: %v", k, err)
			}
		}
	}
}

// ── file-backed (offline) mode ──────────────────────────────────────────────

func TestFileModeOverrides(t *testing.T) {
	dir := t.TempDir()
	flagsFile := filepath.Join(dir, "flags.yaml")
	os.WriteFile(flagsFile, []byte("flags:\n  new-rbac: false\n  max: { value: 15 }\n"), 0o600)
	start := func() (*server, *httptest.Server, *httptest.Server) {
		s := newFileServer(flagsFile, "", filepath.Join(dir, "data", "overrides.json"), testAdminToken, nil)
		if err := s.rebuild(); err != nil {
			t.Fatal(err)
		}
		sdk := httptest.NewServer(s.mux())
		adm := httptest.NewServer(s.admin.mux())
		t.Cleanup(func() { sdk.CloseClientConnections(); sdk.Close(); adm.Close() })
		return s, sdk, adm
	}
	s, sdk, adm := start()
	st := openStream(t, sdk.URL, "any")
	st.next(t)
	putOverride(t, adm.URL, "new-rbac", `{"value":true}`)
	if ev := st.next(t); ev.Name != "patch" || !strings.Contains(ev.Data, `"variations":[true]`) || eventVersion(t, ev) <= 1 {
		t.Fatalf("file mode patch: %+v", ev)
	}
	putOverride(t, adm.URL, "log-levels", `{"value":{"version":1,"services":{}},"local_only":true}`)
	st.next(t)
	putOverride(t, adm.URL, "max", `{"value":30}`)
	st.next(t)
	if v := valueOf(t, sdk.URL, "new-rbac"); v != "true" {
		t.Fatalf("new-rbac = %s", v)
	}
	if v := valueOf(t, sdk.URL, "max"); v != "30" {
		t.Fatalf("max = %s", v)
	}
	// Editing the file keeps the overrides on top (the file has no versions).
	os.WriteFile(flagsFile, []byte("flags:\n  new-rbac: false\n  max: { value: 15 }\n  other: true\n"), 0o600)
	if err := s.rebuild(); err != nil {
		t.Fatal(err)
	}
	if ev := st.next(t); ev.Name != "put" || !strings.Contains(ev.Data, `"other"`) || !strings.Contains(ev.Data, `"variations":[30]`) {
		t.Fatalf("file reload put: %+v", ev)
	}
	code, body, _ := adminDo(t, http.MethodGet, adm.URL+"/admin/flags", "")
	if code != 200 || !strings.Contains(body, `"environment":"file"`) || !strings.Contains(body, `"key":"other"`) {
		t.Fatalf("list: %d %s", code, body)
	}
	adminDo(t, http.MethodDelete, adm.URL+"/admin/flags/new-rbac/override", "")
	if ev := st.next(t); ev.Name != "patch" || !strings.Contains(ev.Data, `"variations":[false]`) {
		t.Fatalf("reset patch: %+v", ev)
	}
	// Restart: overrides come back from disk.
	_, sdk2, _ := start()
	if v := valueOf(t, sdk2.URL, "max"); v != "30" {
		t.Fatalf("after restart max = %s", v)
	}
	if v := valueOf(t, sdk2.URL, "log-levels"); v != `{"services":{},"version":1}` && v != `{"version":1,"services":{}}` {
		t.Fatalf("after restart log-levels = %s", v)
	}
	if v := valueOf(t, sdk2.URL, "new-rbac"); v != "false" {
		t.Fatalf("after restart new-rbac = %s", v)
	}
}

func TestPrimaryAdminURI(t *testing.T) {
	if got := primaryAdminURI("http://flag-relay-0.flag-relay-peer.blues.svc:8080", "8082"); got != "http://flag-relay-0.flag-relay-peer.blues.svc:8082" {
		t.Fatalf("got %s", got)
	}
	if got := primaryAdminURI("", "8082"); got != "" {
		t.Fatalf("primary itself: %q", got)
	}
}

// ADMIN_PORT == PEER_PORT: one listener serves the admin API, the peer
// override replication and the peer snapshots.
func TestSharedAdminAndPeerListener(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	cfg := testConfig(up.srv.URL, t.TempDir())
	cfg.adminToken = testAdminToken
	cfg.overridesFile = filepath.Join(cfg.cacheDir, "overrides.json")
	cfg.peerAddrs = []string{"127.0.0.1:1"}
	r, srv, _ := startRelay(t, cfg)
	get(t, srv.URL+"/sdk/latest-all", testKey)
	h := httptest.NewServer(combinedMux(r.admin.mux(), r.peerMux()))
	t.Cleanup(h.Close)
	putOverride(t, h.URL, "a", `{"value":false}`)
	if code, body, _ := adminDo(t, http.MethodGet, h.URL+"/relay/v1/overrides", ""); code != 200 || !strings.Contains(body, `"a"`) {
		t.Fatalf("peer overrides on the shared listener: %d %s", code, body)
	}
	if code, _, _ := get(t, h.URL+"/relay/v1/snapshot/"+keyHash(testKey), ""); code != 200 {
		t.Fatalf("peer snapshot on the shared listener: %d", code)
	}
}

// Codex r1 #1: a rebuild that takes in two override changes announces both,
// even if only one notification is delivered (the other then finds nothing).
func TestConcurrentOverrideChangesAllAnnounced(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true), flagJSON("b", 1, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	s := openStream(t, n.sdk.URL, testKey)
	s.next(t)
	// Both stored before any env is notified; only a's notification runs first.
	n.r.overrides.set("a", json.RawMessage(`false`), "", false, "", func() map[string]uint64 { return n.r.sourceVersions("a") })
	n.r.overrides.set("b", json.RawMessage(`false`), "", false, "", func() map[string]uint64 { return n.r.sourceVersions("b") })
	n.r.applyOverrides([]string{"a"})
	n.r.applyOverrides([]string{"b"})
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		ev := s.next(t)
		for _, k := range []string{"a", "b"} {
			if ev.Name == "patch" && strings.Contains(ev.Data, `"/flags/`+k+`"`) && strings.Contains(ev.Data, `[false]`) {
				got[k] = true
			}
		}
	}
	if !got["a"] || !got["b"] {
		t.Fatalf("announced %v, want both a and b", got)
	}
}

// Codex r1 #2: an override replicated from a peer that was set against an
// older source version than this replica holds is dropped on arrival.
func TestReplicatedOverrideOlderThanLocalSourceDropped(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 2, true), flagJSON("b", 2, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	get(t, n.sdk.URL+"/sdk/latest-all", testKey)
	h := keyHash(testKey)
	rev := uint64(time.Now().UnixMilli())
	n.r.admin.adopt(overrideMap{
		"a": {Value: json.RawMessage(`false`), Rev: rev, Origin: "peer", Upstream: map[string]uint64{h: 1}},                   // older than our v2
		"b": {Value: json.RawMessage(`false`), Rev: rev, Origin: "peer", Upstream: map[string]uint64{h: 2}},                   // current
		"c": {Value: json.RawMessage(`false`), Rev: rev, Origin: "peer", Upstream: map[string]uint64{h: 0}, LocalOnly: false}, // synthesized
	})
	if n.r.overrides.get("a").live() {
		t.Fatal("an override set against an older source version must be dropped")
	}
	if v := valueOf(t, n.sdk.URL, "a"); v != "true" {
		t.Fatalf("a = %s, want the source value", v)
	}
	if v := valueOf(t, n.sdk.URL, "b"); v != "false" {
		t.Fatalf("b = %s, want the override", v)
	}
	if v := valueOf(t, n.sdk.URL, "c"); v != "false" {
		t.Fatalf("c = %s, want the synthesized override", v)
	}
}

// Codex r2 #2: a revision clock ahead of wall time (after merging a peer's
// revision) must not stop a later source change from replacing an override.
func TestSourceChangeWinsWithClockAhead(t *testing.T) {
	up := newMockUpstream(t, dataJSON(flagJSON("a", 1, true)))
	n := startNode(t, testConfig(up.srv.URL, t.TempDir()), nil, nil)
	s := openStream(t, n.sdk.URL, testKey)
	s.next(t)
	n.r.admin.adopt(overrideMap{"other": {Value: json.RawMessage(`1`), Rev: uint64(time.Now().Add(time.Hour).UnixMilli()), Origin: "peer"}})
	putOverride(t, n.admin.URL, "a", `{"value":false}`)
	up.send("event: patch\ndata: {\"path\":\"/flags/a\",\"data\":" + flagJSON("a", 2, true) + "}\n\n")
	waitFor(t, "override dropped", func() bool { return !n.r.overrides.get("a").live() })
	if v := valueOf(t, n.sdk.URL, "a"); v != "true" {
		t.Fatalf("a = %s, want the source value", v)
	}
}
