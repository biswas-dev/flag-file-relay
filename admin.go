package main

// admin.go: the admin API for local overrides, and their replication.
//
// The admin API listens on its own port (ADMIN_PORT, 8082) and only when
// RELAY_ADMIN_TOKEN is set; every request needs "Authorization: Bearer
// <token>". It is meant for an in-cluster control plane, never for SDKs.
//
//	GET    /admin/flags[?env=<id>]       every flag: source, override, effective value
//	GET    /admin/flags/{key}            one flag (ETag: the override revision)
//	PUT    /admin/flags/{key}/override   {"value": <json>, "set_by": "...", "local_only": bool}
//	DELETE /admin/flags/{key}/override
//
// PUT and DELETE accept If-Match: "<etag>" (the ETag header, or override.etag,
// last read; "0" = there was no override, "*" = there is one) and answer 412
// when the override changed since, so a read-modify-write client can retry.
//
// Replication. On a follower (PRIMARY_URI set) writes are forwarded to the
// primary's admin port while it answers, so the replicas order every write
// the same way; when the primary is down the follower applies the write
// itself. The replica that applied a write pushes the record to every peer
// before it answers, and every replica pulls its peers' records periodically
// and before it reports ready, on the peer port (PEER_PORT) with the same
// bearer token. Records merge by (revision, origin) per key (overrides.go).

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const hdrForwarded = "X-Flag-Relay-Forwarded"

// minAdminToken is the shortest RELAY_ADMIN_TOKEN accepted.
const minAdminToken = 16

// adminEnv is one environment as the admin API shows it.
type adminEnv struct {
	hash string   // sha256(SDK key), or "file"
	raw  *dataSet // source data (nil: none yet)
	view *dataSet // what SDKs are served
}

func (a adminEnv) id() string {
	if len(a.hash) > 8 {
		return a.hash[:8]
	}
	return a.hash
}

// overrideHost is the relay (upstream or file mode) behind the admin API.
type overrideHost interface {
	adminEnvs() []adminEnv
	// sourceVersions returns, per environment with data, the version of the
	// source flag key (0 when it has none).
	sourceVersions(key string) (map[string]uint64, map[string]bool)
	// applyOverrides re-serves keys after an override change.
	applyOverrides(keys []string)
	// reconcile checks the overrides against the source data held now
	// (observe) and returns the overrides it dropped.
	reconcile() []string
}

// adopt merges records from a peer, then checks them against the source
// data this replica already holds: an override set elsewhere against an older
// source version than ours is dropped here and now, not at our next change.
func (a *overrideAdmin) adopt(in overrideMap) {
	changed := a.store.merge(in)
	if len(changed) == 0 {
		return
	}
	dropped := a.host.reconcile()
	a.host.applyOverrides(append(changed, dropped...))
}

type overrideAdmin struct {
	store         *overrideStore
	host          overrideHost
	token         string
	peers         *peerSet     // nil: no replication
	primaryAdmin  string       // followers: the primary's admin base URL
	client        *http.Client // peer replication
	forwardClient *http.Client // writes forwarded to the primary
	metrics       *metrics
}

func adminToken() string { return strings.TrimSpace(envOr("RELAY_ADMIN_TOKEN", "")) }

func newOverrideAdmin(store *overrideStore, host overrideHost, token string, peers *peerSet, primaryAdmin string, m *metrics) *overrideAdmin {
	if token != "" && len(token) < minAdminToken {
		log.Printf("admin API disabled: RELAY_ADMIN_TOKEN is shorter than %d characters", minAdminToken)
		token = ""
	}
	return &overrideAdmin{store: store, host: host, token: token, peers: peers, primaryAdmin: strings.TrimRight(primaryAdmin, "/"),
		client: &http.Client{Timeout: 3 * time.Second}, forwardClient: &http.Client{}, metrics: m}
}

func (a *overrideAdmin) enabled() bool { return a != nil && a.token != "" }

func (a *overrideAdmin) authorized(req *http.Request) bool {
	h := req.Header.Get("Authorization")
	const p = "Bearer "
	if len(h) <= len(p) || !strings.EqualFold(h[:len(p)], p) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(h[len(p):])), []byte(a.token)) == 1
}

func (a *overrideAdmin) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/flags", a.guard(a.handleList))
	mux.HandleFunc("GET /admin/flags/{key}", a.guard(a.handleGet))
	mux.HandleFunc("PUT /admin/flags/{key}/override", a.guard(a.handlePut))
	mux.HandleFunc("DELETE /admin/flags/{key}/override", a.guard(a.handleDelete))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return mux
}

func (a *overrideAdmin) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if !a.authorized(req) {
			a.metrics.inc("flagrelay_admin_requests_total", "code", "401")
			w.Header().Set("WWW-Authenticate", `Bearer realm="flag-relay-admin"`)
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		h(w, req)
	}
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// ── views of a flag ──────────────────────────────────────────────────────────

type sourceInfo struct {
	Value     json.RawMessage `json:"value"`     // what a context without targeting gets (null: rollout or no value)
	Variation *int            `json:"variation"` // index of that value
	On        bool            `json:"on"`
	Version   uint64          `json:"version"`
	Targeting bool            `json:"targeting"` // targets, rules or a rollout: some contexts may get another value
}

type overrideInfo struct {
	Value           json.RawMessage `json:"value"`
	SetBy           string          `json:"set_by"`
	SetAt           time.Time       `json:"set_at"`
	LocalOnly       bool            `json:"local_only"`
	Rev             uint64          `json:"rev"`
	Origin          string          `json:"origin"`
	ETag            string          `json:"etag"`             // If-Match token for PUT/DELETE
	UpstreamVersion *uint64         `json:"upstream_version"` // source version when set, in this environment
}

type flagEntry struct {
	Key       string          `json:"key"`
	Upstream  *sourceInfo     `json:"upstream"`
	Override  *overrideInfo   `json:"override"`
	Effective json.RawMessage `json:"effective"`
	Source    string          `json:"source"` // override | upstream | none
}

// defaultValue evaluates a flag for a context no target or rule matches.
func defaultValue(raw json.RawMessage) *sourceInfo {
	o, ok := asObj(raw)
	if !ok {
		return nil
	}
	si := &sourceInfo{Value: json.RawMessage("null")}
	si.On = string(bytes.TrimSpace(o["on"])) == "true"
	if v, err := parseU64(o["version"]); err == nil {
		si.Version = v
	}
	for _, f := range []string{"targets", "contextTargets", "rules"} {
		if a, ok := asArr(o[f]); ok && len(a) > 0 {
			si.Targeting = true
		}
	}
	vars, _ := asArr(o["variations"])
	idx := -1
	if si.On {
		if ft, ok := asObj(o["fallthrough"]); ok {
			if _, roll := ft["rollout"]; roll && !isNull(ft["rollout"]) {
				si.Targeting = true
			} else if n, err := parseU64(ft["variation"]); err == nil {
				idx = int(n)
			}
		}
	} else if n, err := parseU64(o["offVariation"]); err == nil {
		idx = int(n)
	}
	if idx >= 0 && idx < len(vars) {
		si.Variation = &idx
		si.Value = vars[idx]
	}
	return si
}

func (a *overrideAdmin) entry(env *adminEnv, key string, rec *overrideRecord) flagEntry {
	fe := flagEntry{Key: key, Effective: json.RawMessage("null"), Source: "none"}
	if env != nil && env.raw != nil {
		if it, ok := env.raw.Flags[key]; ok && !it.Deleted {
			fe.Upstream = defaultValue(it.Raw)
		}
	}
	if rec.live() {
		oi := &overrideInfo{Value: rec.Value, SetBy: rec.SetBy, SetAt: rec.SetAt, LocalOnly: rec.LocalOnly, Rev: rec.Rev, Origin: rec.Origin, ETag: rec.etag()}
		if env != nil {
			if v, ok := rec.Upstream[env.hash]; ok {
				oi.UpstreamVersion = &v
			}
		}
		fe.Override = oi
		fe.Effective, fe.Source = rec.Value, "override"
	} else if fe.Upstream != nil {
		fe.Effective, fe.Source = fe.Upstream.Value, "upstream"
	}
	return fe
}

// pickEnv chooses the environment shown: ?env=<id or hash prefix>, else the
// one with the most flags (ties: lowest id).
func pickEnv(envs []adminEnv, want string) (*adminEnv, error) {
	if want != "" {
		for i := range envs {
			if len(want) >= 8 && strings.HasPrefix(envs[i].hash, strings.ToLower(want)) || envs[i].hash == want {
				return &envs[i], nil
			}
		}
		return nil, fmt.Errorf("unknown environment %q", want)
	}
	var best *adminEnv
	count := func(e *adminEnv) int {
		if e.raw == nil {
			return -1
		}
		n := 0
		for _, it := range e.raw.Flags {
			if !it.Deleted {
				n++
			}
		}
		return n
	}
	for i := range envs {
		e := &envs[i]
		if best == nil || count(e) > count(best) || (count(e) == count(best) && e.hash < best.hash) {
			best = e
		}
	}
	return best, nil
}

func (a *overrideAdmin) handleList(w http.ResponseWriter, req *http.Request) {
	envs := a.host.adminEnvs()
	env, err := pickEnv(envs, req.URL.Query().Get("env"))
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	ovs := a.store.snapshot()
	keys := map[string]bool{}
	for k, o := range ovs {
		if o.live() {
			keys[k] = true
		}
	}
	if env != nil && env.raw != nil {
		for k, it := range env.raw.Flags {
			if !it.Deleted {
				keys[k] = true
			}
		}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	flags := make([]flagEntry, 0, len(sorted))
	for _, k := range sorted {
		flags = append(flags, a.entry(env, k, ovs[k]))
	}
	ids := make([]string, 0, len(envs))
	for _, e := range envs {
		ids = append(ids, e.id())
	}
	sort.Strings(ids)
	resp := map[string]interface{}{
		"environment":  nil,
		"environments": ids,
		"flags":        flags,
		"persisted":    a.store.path != "",
	}
	if env != nil {
		resp["environment"] = env.id()
	}
	if pe := a.store.persistError(); pe != "" {
		resp["persist_error"] = pe
	}
	a.metrics.inc("flagrelay_admin_requests_total", "code", "200")
	writeJSON(w, http.StatusOK, resp)
}

func (a *overrideAdmin) oneEntry(req *http.Request, key string) (flagEntry, error) {
	env, err := pickEnv(a.host.adminEnvs(), req.URL.Query().Get("env"))
	if err != nil {
		return flagEntry{}, err
	}
	return a.entry(env, key, a.store.get(key)), nil
}

func revETag(fe flagEntry) string {
	if fe.Override == nil {
		return `"0"`
	}
	return `"` + fe.Override.ETag + `"`
}

func (a *overrideAdmin) handleGet(w http.ResponseWriter, req *http.Request) {
	key := req.PathValue("key")
	if !flagKeyPattern.MatchString(key) {
		writeJSONError(w, http.StatusBadRequest, "bad flag key")
		return
	}
	fe, err := a.oneEntry(req, key)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	if fe.Source == "none" {
		writeJSONError(w, http.StatusNotFound, "no such flag")
		return
	}
	w.Header().Set("ETag", revETag(fe))
	a.metrics.inc("flagrelay_admin_requests_total", "code", "200")
	writeJSON(w, http.StatusOK, fe)
}

func ifMatch(req *http.Request) string {
	v := strings.TrimSpace(req.Header.Get("If-Match"))
	v = strings.TrimPrefix(v, "W/")
	return strings.Trim(v, `"`)
}

type putBody struct {
	Value     json.RawMessage `json:"value"`
	SetBy     string          `json:"set_by"`
	LocalOnly bool            `json:"local_only"`
}

func (a *overrideAdmin) handlePut(w http.ResponseWriter, req *http.Request) {
	key := req.PathValue("key")
	if !flagKeyPattern.MatchString(key) {
		writeJSONError(w, http.StatusBadRequest, "bad flag key")
		return
	}
	b, err := io.ReadAll(io.LimitReader(req.Body, maxOverrideValue+64<<10))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "unreadable body")
		return
	}
	var body putBody
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "body must be {\"value\": <json>, \"set_by\": string, \"local_only\": bool}")
		return
	}
	if err := validOverrideValue(body.Value); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(body.SetBy) > maxOverrideSetBy {
		writeJSONError(w, http.StatusBadRequest, "set_by too long")
		return
	}
	if a.forward(w, req, b) {
		return
	}
	_, err = a.store.set(key, body.Value, body.SetBy, body.LocalOnly, ifMatch(req), func() (map[string]uint64, map[string]bool) { return a.host.sourceVersions(key) })
	a.afterWrite(w, req, key, err)
}

func (a *overrideAdmin) handleDelete(w http.ResponseWriter, req *http.Request) {
	key := req.PathValue("key")
	if !flagKeyPattern.MatchString(key) {
		writeJSONError(w, http.StatusBadRequest, "bad flag key")
		return
	}
	if a.forward(w, req, nil) {
		return
	}
	by := req.URL.Query().Get("set_by")
	if len(by) > maxOverrideSetBy {
		by = by[:maxOverrideSetBy]
	}
	_, err := a.store.remove(key, by, ifMatch(req))
	a.afterWrite(w, req, key, err)
}

// afterWrite serves the change, replicates it and answers with the flag.
func (a *overrideAdmin) afterWrite(w http.ResponseWriter, req *http.Request, key string, err error) {
	switch {
	case errors.Is(err, errPrecondition):
		fe, _ := a.oneEntry(req, key)
		w.Header().Set("ETag", revETag(fe))
		a.metrics.inc("flagrelay_admin_requests_total", "code", "412")
		writeJSON(w, http.StatusPreconditionFailed, map[string]interface{}{"error": "override changed since it was read", "flag": fe})
		return
	case errors.Is(err, errTooMany):
		a.metrics.inc("flagrelay_admin_requests_total", "code", "409")
		writeJSONError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		a.metrics.inc("flagrelay_admin_requests_total", "code", "503")
		writeJSONError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	a.host.applyOverrides([]string{key})
	ok, total := a.push(req.Context(), key)
	fe, _ := a.oneEntry(req, key)
	w.Header().Set("ETag", revETag(fe))
	a.metrics.inc("flagrelay_admin_requests_total", "code", "200")
	writeJSON(w, http.StatusOK, map[string]interface{}{"flag": fe, "replicated": ok, "peers": total})
}

// forward hands a write to the primary (followers only). It returns false
// when the caller must apply the write itself: this is the primary, the
// request was already forwarded, or the primary did not answer.
func (a *overrideAdmin) forward(w http.ResponseWriter, req *http.Request, body []byte) bool {
	if a.primaryAdmin == "" || req.Header.Get(hdrForwarded) != "" {
		return false
	}
	// The primary persists and pushes to its peers (up to 3 s each) before
	// it answers: allow for that.
	ctx, cancel := context.WithTimeout(req.Context(), 15*time.Second)
	defer cancel()
	u := a.primaryAdmin + req.URL.Path
	if req.URL.RawQuery != "" {
		u += "?" + req.URL.RawQuery
	}
	freq, err := http.NewRequestWithContext(ctx, req.Method, u, bytes.NewReader(body))
	if err != nil {
		return false
	}
	for _, h := range []string{"Authorization", "If-Match", "Content-Type"} {
		if v := req.Header.Get(h); v != "" {
			freq.Header.Set(h, v)
		}
	}
	freq.Header.Set(hdrForwarded, "1")
	resp, err := a.forwardClient.Do(freq)
	if err != nil {
		var op *net.OpError
		if errors.As(err, &op) && op.Op == "dial" {
			// Never reached the primary: it is down, apply the write here.
			log.Printf("admin: primary unreachable (%v); applying %s %s here", err, req.Method, req.URL.Path)
			return false
		}
		// The primary may have applied it: do not apply it twice; the client
		// retries (with If-Match) and sees the outcome.
		log.Printf("admin: forwarding %s %s to the primary failed: %v", req.Method, req.URL.Path, err)
		writeJSONError(w, http.StatusGatewayTimeout, "primary did not answer; the write may have been applied, read the flag and retry")
		a.metrics.inc("flagrelay_admin_requests_total", "code", "504")
		return true
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	for _, h := range []string{"Content-Type", "ETag"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	w.Write(rb)
	a.metrics.inc("flagrelay_admin_requests_total", "code", fmt.Sprint(resp.StatusCode))
	return true
}

// ── replication ─────────────────────────────────────────────────────────────

type peerOverrides struct {
	Overrides overrideMap `json:"overrides"`
}

// handlePeer serves the peer endpoints: GET returns every record (tombstones
// included), POST merges the records sent.
func (a *overrideAdmin) handlePeer(w http.ResponseWriter, req *http.Request) {
	if !a.enabled() || !a.authorized(req) {
		http.NotFound(w, req)
		return
	}
	switch req.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, peerOverrides{Overrides: a.store.snapshot()})
	case http.MethodPost:
		var in peerOverrides
		if err := json.NewDecoder(io.LimitReader(req.Body, maxPeerOverrides)).Decode(&in); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		a.adopt(in.Overrides)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *overrideAdmin) peerRequest(ctx context.Context, method, addr string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+"/relay/v1/overrides", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")
	return a.client.Do(req)
}

// push sends the record of key to every peer; it returns how many accepted it
// and how many peers there are.
func (a *overrideAdmin) push(ctx context.Context, key string) (int, int) {
	if a.peers == nil || !a.enabled() {
		return 0, 0
	}
	rec := a.store.get(key)
	if rec == nil {
		return 0, 0
	}
	body, _ := json.Marshal(peerOverrides{Overrides: overrideMap{key: rec}})
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	addrs := a.peers.addrs(ctx)
	var mu sync.Mutex
	ok := 0
	var wg sync.WaitGroup
	for _, ad := range addrs {
		wg.Add(1)
		go func(ad string) {
			defer wg.Done()
			resp, err := a.peerRequest(ctx, http.MethodPost, ad, body)
			if err != nil {
				log.Printf("admin: override %s not pushed to peer %s (it will pull it): %v", key, ad, err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}(ad)
	}
	wg.Wait()
	return ok, len(addrs)
}

// pull merges every peer's records; it returns how many peers answered.
func (a *overrideAdmin) pull(ctx context.Context) int {
	if a.peers == nil || !a.enabled() {
		return 0
	}
	addrs := a.peers.addrs(ctx)
	answered := 0
	var all []overrideMap
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ad := range addrs {
		wg.Add(1)
		go func(ad string) {
			defer wg.Done()
			resp, err := a.peerRequest(ctx, http.MethodGet, ad, nil)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return
			}
			var in peerOverrides
			if json.NewDecoder(io.LimitReader(resp.Body, maxPeerOverrides)).Decode(&in) != nil {
				return
			}
			mu.Lock()
			answered++
			all = append(all, in.Overrides)
			mu.Unlock()
		}(ad)
	}
	wg.Wait()
	for _, m := range all {
		a.adopt(m)
	}
	return answered
}

// startupSync pulls the peers' records before the relay reports ready, waiting
// up to wait for one peer to answer (none may exist, or all may be restarting).
func (a *overrideAdmin) startupSync(ctx context.Context, wait time.Duration) {
	if a.peers == nil || !a.enabled() {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		if a.pull(ctx) > 0 {
			a.metrics.inc("flagrelay_override_startup_sync_total", "result", "ok")
			return
		}
		if addrs, resolved := a.peers.resolve(ctx); resolved && len(addrs) == 0 {
			a.metrics.inc("flagrelay_override_startup_sync_total", "result", "no_peers")
			return
		}
		select {
		case <-ctx.Done():
			a.metrics.inc("flagrelay_override_startup_sync_total", "result", "no_peer_answered")
			log.Printf("overrides: no peer answered within %s; serving the overrides on disk", wait)
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// syncLoop pulls the peers' records every interval (a peer that was down
// during a write, or a push that failed, converges here).
func (a *overrideAdmin) syncLoop(ctx context.Context, every time.Duration) {
	if a.peers == nil || !a.enabled() {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		a.pull(pctx)
		cancel()
	}
}

// primaryAdminURI derives the primary's admin base URL from PRIMARY_URI (same
// host, the admin port), unless PRIMARY_ADMIN_URI is set. "" on the primary.
func primaryAdminURI(primaryURI, adminPort string) string {
	if primaryURI == "" {
		return ""
	}
	if v := envOr("PRIMARY_ADMIN_URI", ""); v != "" {
		return v
	}
	u, err := url.Parse(primaryURI)
	if err != nil || u.Host == "" {
		return ""
	}
	u.Host = net.JoinHostPort(u.Hostname(), adminPort)
	u.Path, u.RawQuery = "", ""
	return u.String()
}

// combinedMux serves the admin API and the peer endpoints on one listener
// (ADMIN_PORT == PEER_PORT): /relay/ goes to the peer mux, the rest to admin.
func combinedMux(admin, peer http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/relay/") {
			peer.ServeHTTP(w, req)
			return
		}
		admin.ServeHTTP(w, req)
	})
}
