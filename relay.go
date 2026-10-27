package main

// relay.go: upstream mode. The relay holds one stream per SDK key to an
// upstream LaunchDarkly-protocol service (flagtgl, or LaunchDarkly itself),
// keeps the last good data (in memory and on disk) and serves the LD
// server-side SDK endpoints to every process in the cluster.

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var version = "dev" // set with -ldflags "-X main.version=..."

func userAgent() string { return "FlagFileRelay/" + version }

type relayConfig struct {
	addr              string
	peerAddr          string
	adminAddr         string // admin API (local overrides); only with adminToken
	adminToken        string
	primaryAdmin      string // followers: the primary's admin API (writes are forwarded there)
	overridesFile     string
	overrideSync      time.Duration
	streamURI         string
	pollURI           string
	eventsURI         string
	cacheDir          string
	maxKeys           int
	maxStreams        int
	initWait          time.Duration
	firstWait         time.Duration
	drainDelay        time.Duration
	drainSpread       time.Duration
	readTimeout       time.Duration
	pollInterval      time.Duration
	reconcileInterval time.Duration
	eventsFlush       time.Duration
	keepalive         time.Duration
	writeTimeout      time.Duration
	rejectTTL         time.Duration
	peerDNS           string
	peerAddrs         []string // static peer host:port list (instead of peerDNS)
	podIP             string
	primaryURI        string // followers: the primary relay; "" on the primary itself
	primaryRetry      time.Duration
	idleExpiry        time.Duration
	peerSyncWait      time.Duration
	persistRetry      time.Duration
	deny              map[string]bool // sha256(key) hashes that are refused
	allow             map[string]bool // if non-empty, only these hashes
}

func loadRelayConfig() (relayConfig, error) {
	base := strings.TrimRight(os.Getenv("UPSTREAM_URI"), "/")
	c := relayConfig{
		addr:              ":" + envOr("PORT", "8080"),
		peerAddr:          ":" + envOr("PEER_PORT", "8081"),
		adminAddr:         ":" + envOr("ADMIN_PORT", "8082"),
		adminToken:        adminToken(),
		overrideSync:      envDur("OVERRIDE_SYNC_INTERVAL", 10*time.Second),
		streamURI:         strings.TrimRight(envOr("UPSTREAM_STREAM_URI", base), "/"),
		pollURI:           strings.TrimRight(envOr("UPSTREAM_POLL_URI", base), "/"),
		eventsURI:         strings.TrimRight(envOr("UPSTREAM_EVENTS_URI", base), "/"),
		cacheDir:          os.Getenv("CACHE_DIR"),
		maxKeys:           envInt("MAX_KEYS", 8),
		maxStreams:        envInt("MAX_STREAMS", 5000),
		initWait:          envDur("INIT_WAIT", 3*time.Second),
		firstWait:         envDur("FIRST_DATA_WAIT", 10*time.Second),
		drainDelay:        envDur("DRAIN_DELAY", 5*time.Second),
		drainSpread:       envDur("DRAIN_SPREAD", 20*time.Second),
		readTimeout:       envDur("UPSTREAM_READ_TIMEOUT", 5*time.Minute),
		pollInterval:      envDur("UPSTREAM_POLL_INTERVAL", 30*time.Second),
		reconcileInterval: envDur("RECONCILE_INTERVAL", 60*time.Second),
		eventsFlush:       envDur("EVENTS_FLUSH", 2*time.Second),
		keepalive:         envDur("KEEPALIVE", 20*time.Second),
		writeTimeout:      envDur("WRITE_TIMEOUT", 10*time.Second),
		rejectTTL:         envDur("REJECT_TTL", time.Minute),
		peerDNS:           os.Getenv("PEER_DNS"),
		podIP:             os.Getenv("POD_IP"),
		primaryURI:        strings.TrimRight(os.Getenv("PRIMARY_URI"), "/"),
		primaryRetry:      envDur("PRIMARY_RETRY", 30*time.Second),
		idleExpiry:        envDur("IDLE_KEY_EXPIRY", 2*time.Minute),
		peerSyncWait:      envDur("PEER_SYNC_WAIT", 10*time.Second),
		persistRetry:      envDur("PERSIST_RETRY", 5*time.Second),
	}
	for _, a := range strings.Split(os.Getenv("PEER_ADDRS"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.peerAddrs = append(c.peerAddrs, a)
		}
	}
	var err error
	if c.deny, err = hashSet(os.Getenv("DENY_KEY_HASHES")); err != nil {
		return c, fmt.Errorf("DENY_KEY_HASHES: %w", err)
	}
	if c.allow, err = hashSet(os.Getenv("ALLOW_KEY_HASHES")); err != nil {
		return c, fmt.Errorf("ALLOW_KEY_HASHES: %w", err)
	}
	// The primary (PRIMARY_HOST, e.g. StatefulSet pod flag-relay-0) streams
	// from upstream; every other replica follows it.
	if ph := os.Getenv("PRIMARY_HOST"); ph != "" {
		if hn, _ := os.Hostname(); hn == ph {
			c.primaryURI = ""
		}
	}
	if c.streamURI == "" || c.pollURI == "" || c.eventsURI == "" {
		return c, errors.New("set UPSTREAM_URI (or all of UPSTREAM_STREAM_URI, UPSTREAM_POLL_URI, UPSTREAM_EVENTS_URI)")
	}
	if c.cacheDir != "" {
		if err := os.MkdirAll(c.cacheDir, 0o700); err != nil {
			return c, fmt.Errorf("CACHE_DIR: %w", err)
		}
	}
	c.overridesFile = overridesPath(c.cacheDir)
	c.primaryAdmin = primaryAdminURI(c.primaryURI, strings.TrimPrefix(c.adminAddr, ":"))
	return c, nil
}

// hashSet parses a comma/space separated list of hex sha256 key hashes. Any
// malformed entry is an error: a typo must not silently empty an allowlist.
func hashSet(s string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, f := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' }) {
		f = strings.ToLower(strings.TrimSpace(f))
		if !validHash(f) {
			return nil, fmt.Errorf("%q is not a hex sha256", f)
		}
		out[f] = true
	}
	return out, nil
}

func upstreamModeConfigured() bool {
	return os.Getenv("UPSTREAM_URI") != "" || os.Getenv("UPSTREAM_STREAM_URI") != ""
}

type relay struct {
	cfg          relayConfig
	metrics      *metrics
	streamClient *http.Client // no overall timeout: streams are long-lived
	httpClient   *http.Client
	events       *eventForwarder
	peers        *peerSet
	overrides    *overrideStore
	admin        *overrideAdmin

	ctx         context.Context
	mu          sync.Mutex
	envs        map[string]*env        // by key
	byHash      map[string]*env        // by sha256(key)
	disk        map[string]*diskEntry  // by hash, loaded at start
	rejected    map[string]time.Time   // by hash: upstream refused it recently
	files       map[string]*sync.Mutex // by hash: serializes every write of that cache file
	ready       atomic.Bool
	draining    atomic.Bool
	streams     atomic.Int64
	eventBodies atomic.Int64 // bytes reserved by event bodies being read
}

func newRelay(ctx context.Context, cfg relayConfig) *relay {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	tr := func() *http.Transport {
		return &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   8,
			ForceAttemptHTTP2:     true,
		}
	}
	r := &relay{
		cfg:          cfg,
		metrics:      newMetrics(),
		streamClient: &http.Client{Transport: tr()},
		httpClient:   &http.Client{Transport: tr(), Timeout: 60 * time.Second},
		peers:        newPeerSet(cfg.peerDNS, cfg.peerAddrs, strings.TrimPrefix(cfg.peerAddr, ":"), cfg.podIP),
		ctx:          ctx,
		envs:         map[string]*env{},
		byHash:       map[string]*env{},
		disk:         loadCache(cfg.cacheDir),
		rejected:     map[string]time.Time{},
		files:        map[string]*sync.Mutex{},
	}
	for h := range cfg.deny {
		delete(r.disk, h)
		removeCacheFile(cfg.cacheDir, h)
	}
	r.overrides = newOverrideStore(cfg.overridesFile, replicaName(), r.metrics)
	r.admin = newOverrideAdmin(r.overrides, r, cfg.adminToken, r.peers, cfg.primaryAdmin, r.metrics)
	r.events = newEventForwarder(r)
	r.metrics.onScrape(r.scrapeGauges)
	if n := len(r.disk); n > 0 {
		log.Printf("loaded %d cached environment(s) from %s", n, cfg.cacheDir)
	}
	return r
}

func (r *relay) denied(hash string) bool {
	return r.cfg.deny[hash] || (len(r.cfg.allow) > 0 && !r.cfg.allow[hash])
}

// errStatus carries the HTTP status for a refused SDK request.
type errStatus struct {
	code int
	msg  string
}

func (e *errStatus) Error() string { return e.msg }

// envFor resolves the SDK key on the request to a relayed environment,
// creating it (and its single upstream stream) on first use. Creation happens
// under r.mu, so concurrent first requests for a key share one validation.
func (r *relay) envFor(req *http.Request) (*env, *errStatus) {
	key := extractKey(req)
	if key == "" {
		return nil, &errStatus{http.StatusUnauthorized, "missing SDK key"}
	}
	r.mu.Lock()
	e := r.envs[key]
	if e == nil {
		h := keyHash(key)
		if r.denied(h) {
			r.mu.Unlock()
			return nil, &errStatus{http.StatusUnauthorized, "SDK key not accepted by this relay"}
		}
		if t, ok := r.rejected[h]; ok && time.Since(t) < r.cfg.rejectTTL {
			r.mu.Unlock()
			return nil, &errStatus{http.StatusServiceUnavailable, "SDK key not accepted upstream; retry later"}
		}
		delete(r.rejected, h)
		if len(r.envs) >= r.cfg.maxKeys {
			r.mu.Unlock()
			return nil, &errStatus{http.StatusServiceUnavailable, "too many SDK keys (MAX_KEYS)"}
		}
		e = newEnv(r, key, h, r.disk[h])
		r.envs[key] = e
		r.byHash[h] = e
		e.start(r.ctx)
		log.Printf("env %s: first request, connecting upstream (cached data: %v)", e.id, r.disk[h] != nil)
	}
	r.mu.Unlock()

	code, err := e.awaitData(req.Context(), req.Header.Get(hdrPeer) != "")
	if err != nil {
		return nil, &errStatus{code, err.Error()}
	}
	return e, nil
}

// forget drops an env whose key upstream refused while nobody had data for it.
func (r *relay) forget(e *env) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.envs[e.key] == e {
		delete(r.envs, e.key)
		delete(r.byHash, e.hash)
	}
	// Bounded negative cache.
	if len(r.rejected) >= 1024 {
		for h, t := range r.rejected {
			if time.Since(t) >= r.cfg.rejectTTL {
				delete(r.rejected, h)
			}
		}
	}
	if len(r.rejected) < 1024 {
		r.rejected[e.hash] = time.Now()
	}
	e.cancel()
	log.Printf("env %s: upstream refused the SDK key; negative-cached for %s", e.id, r.cfg.rejectTTL)
}

// knownKey reports whether events for this key may be forwarded.
func (r *relay) knownKey(key string) bool {
	r.mu.Lock()
	e, ok := r.envs[key]
	r.mu.Unlock()
	if !ok {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.data != nil
}

func extractKey(req *http.Request) string {
	h := strings.TrimSpace(req.Header.Get("Authorization"))
	if parts := strings.SplitN(h, " ", 2); len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
		return strings.TrimSpace(parts[1])
	}
	return h
}

// ── handlers ────────────────────────────────────────────────────────────────

func (r *relay) count(endpoint string, code int) {
	r.metrics.inc("flagrelay_requests_total", "endpoint", endpoint, "code", fmt.Sprint(code))
}

func (r *relay) fail(w http.ResponseWriter, endpoint string, es *errStatus) {
	r.count(endpoint, es.code)
	if es.code == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	http.Error(w, es.msg, es.code)
}

// GET /all ({flags, segments}) and GET /flags (legacy: flags only).
func (r *relay) handleStream(w http.ResponseWriter, req *http.Request) {
	proto, ep := protoAll, "all"
	if req.URL.Path == "/flags" {
		proto, ep = protoFlags, "flags"
	}
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.draining.Load() || !r.ready.Load() {
		r.fail(w, ep, &errStatus{http.StatusServiceUnavailable, "relay is starting or restarting"})
		return
	}
	if r.streams.Add(1) > int64(r.cfg.maxStreams) {
		r.streams.Add(-1)
		r.fail(w, ep, &errStatus{http.StatusServiceUnavailable, "too many streams (MAX_STREAMS)"})
		return
	}
	defer r.streams.Add(-1)
	e, es := r.envFor(req)
	if es != nil {
		r.fail(w, ep, es)
		return
	}
	peer := req.Header.Get(hdrPeer) != ""
	sub, put, confirmed, upstream := e.subscribe(proto, peer)
	defer e.unsubscribe(sub)
	r.metrics.inc("flagrelay_downstream_stream_opens_total")
	r.count(ep, http.StatusOK)

	rc := http.NewResponseController(w)
	write := func(b []byte) bool {
		// A stuck client must not pin this goroutine (or a drain) forever.
		_ = rc.SetWriteDeadline(time.Now().Add(r.cfg.writeTimeout))
		if _, err := w.Write(b); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	keepalive := func() []byte { return []byte(":\n\n") }
	interval := r.cfg.keepalive
	if peer {
		// Followers learn how fresh our data is from these headers (taken
		// with the snapshot) and from a ":relay-confirmed" comment written
		// before every event and as the keepalive (SDKs ignore comments).
		h.Set(hdrConfirmedAt, fmt.Sprint(confirmed.UnixMilli()))
		h.Set(hdrUpstream, upstream)
		keepalive = func() []byte { return e.peerKeepalive(sub) }
		interval = 5 * time.Second
	}
	w.WriteHeader(http.StatusOK)
	if !write(put) {
		return
	}
	ka := time.NewTicker(interval)
	defer ka.Stop()
	for {
		select {
		case <-req.Context().Done():
			return
		case <-sub.kick:
			return
		case msg := <-sub.ch:
			if !write(msg) {
				return
			}
		case <-ka.C:
			msg := keepalive()
			if msg == nil {
				continue // queued events go first; never confirm ahead of them
			}
			if !write(msg) {
				return
			}
		}
	}
}

// GET /sdk/latest-all, /sdk/poll, /sdk/latest-flags[/key], /sdk/latest-segments[/key]
func (r *relay) handlePoll(w http.ResponseWriter, req *http.Request) {
	const ep = "poll"
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !r.ready.Load() {
		r.fail(w, ep, &errStatus{http.StatusServiceUnavailable, "relay is starting"})
		return
	}
	e, es := r.envFor(req)
	if es != nil {
		r.fail(w, ep, es)
		return
	}
	peer := req.Header.Get(hdrPeer) != ""
	ds, payload, etag, confirmed, upstream := e.snapshot(peer)
	if peer {
		w.Header().Set(hdrConfirmedAt, fmt.Sprint(confirmed.UnixMilli()))
		w.Header().Set(hdrUpstream, upstream)
	}
	path := req.URL.Path
	var body []byte
	switch {
	case path == "/sdk/latest-all" || path == "/sdk/poll":
		body = payload
	case path == "/sdk/latest-flags":
		body = ds.marshalFlags()
	case path == "/sdk/latest-segments":
		body = ds.marshalSegments()
	case strings.HasPrefix(path, "/sdk/latest-flags/"), strings.HasPrefix(path, "/sdk/latest-segments/"):
		coll, key := ds.Flags, strings.TrimPrefix(path, "/sdk/latest-flags/")
		if strings.HasPrefix(path, "/sdk/latest-segments/") {
			coll, key = ds.Segments, strings.TrimPrefix(path, "/sdk/latest-segments/")
		}
		it, ok := coll[key]
		if !ok || it.Deleted {
			r.count(ep, http.StatusNotFound)
			http.NotFound(w, req)
			return
		}
		body = it.Raw
	default:
		r.count(ep, http.StatusNotFound)
		http.NotFound(w, req)
		return
	}
	if path != "/sdk/latest-all" && path != "/sdk/poll" {
		etag = etagOf(body)
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "no-cache")
	if inm := req.Header.Get("If-None-Match"); inm != "" && inm == etag {
		r.count(ep, http.StatusNotModified)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	r.count(ep, http.StatusOK)
	if req.Method == http.MethodHead {
		return
	}
	w.Write(body)
}

// POST /bulk and /diagnostic: 202 at once (the SDK must not back off because
// of us); forwarding is asynchronous and batched.
func (r *relay) handleEvents(w http.ResponseWriter, req *http.Request) {
	ep := strings.TrimPrefix(req.URL.Path, "/")
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := extractKey(req)
	if key == "" {
		r.fail(w, ep, &errStatus{http.StatusUnauthorized, "missing SDK key"})
		return
	}
	if !r.ready.Load() {
		r.fail(w, ep, &errStatus{http.StatusServiceUnavailable, "relay is starting"})
		return
	}
	var rd io.Reader = req.Body
	if strings.EqualFold(req.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(req.Body)
		if err != nil {
			r.fail(w, ep, &errStatus{http.StatusBadRequest, "bad gzip body"})
			return
		}
		defer gz.Close()
		rd = gz
	}
	// Bound memory held by event bodies being read and parsed across all
	// requests: reserve twice the expected size (body + parsed elements)
	// until the events have been handed to the batcher or the primary.
	est := req.ContentLength
	if est <= 0 || est > maxEventBody || req.Header.Get("Content-Encoding") != "" {
		est = maxEventBody
	}
	reserve := 2 * est
	if r.eventBodies.Add(reserve) > maxEventBodiesInFlight {
		r.eventBodies.Add(-reserve)
		r.metrics.inc("flagrelay_events_dropped_total")
		r.count(ep, http.StatusAccepted)
		w.WriteHeader(http.StatusAccepted)
		return
	}
	defer r.eventBodies.Add(-reserve)
	body, err := io.ReadAll(io.LimitReader(rd, maxEventBody+1))
	if err != nil || len(body) > maxEventBody {
		r.fail(w, ep, &errStatus{http.StatusRequestEntityTooLarge, "event payload too large or unreadable"})
		return
	}
	if !r.knownKey(key) {
		if req.Header.Get(hdrPeer) != "" {
			// A follower handing over events for a key we do not relay: let it
			// forward them itself.
			r.fail(w, ep, &errStatus{http.StatusConflict, "key not relayed here"})
			return
		}
		// Only keys an SDK here already streams with are forwarded, so the
		// relay never proxies arbitrary keys upstream.
		r.metrics.inc("flagrelay_events_dropped_total")
		r.count(ep, http.StatusAccepted)
		w.WriteHeader(http.StatusAccepted)
		return
	}
	schema, pid := req.Header.Get("X-LaunchDarkly-Event-Schema"), req.Header.Get("X-LaunchDarkly-Payload-ID")
	if ep == "diagnostic" {
		r.events.diagnostic(key, body)
	} else if r.cfg.primaryURI != "" && req.Header.Get(hdrPeer) == "" {
		// Follower: hand the SDK's payload, with its own payload id, to the
		// primary, which batches and de-duplicates for the whole environment.
		if !r.events.toPrimary(key, schema, pid, body) {
			r.fail(w, ep, &errStatus{http.StatusBadRequest, "events must be a JSON array"})
			return
		}
	} else if !r.events.add(key, schema, pid, body) {
		r.fail(w, ep, &errStatus{http.StatusBadRequest, "events must be a JSON array"})
		return
	}
	r.count(ep, http.StatusAccepted)
	w.WriteHeader(http.StatusAccepted)
}

// /readyz: ready once the start-up peer sync of the disk cache has run, and
// until draining starts. Never depends on upstream: a flagtgl outage must not
// empty the Service.
func (r *relay) handleReady(w http.ResponseWriter, _ *http.Request) {
	if r.draining.Load() || !r.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ok"))
}

type envStatus struct {
	Env            string  `json:"env"`
	Connected      bool    `json:"connected"`
	Source         string  `json:"source"`
	Flags          int     `json:"flags"`
	DataAgeSeconds float64 `json:"data_age_seconds,omitempty"`
	SavedAt        string  `json:"saved_at,omitempty"`
	PersistError   string  `json:"persist_error,omitempty"`
	Via            string  `json:"via,omitempty"`
	Subscribers    int     `json:"subscribers"`
	FailedAttempts int     `json:"failed_attempts"`
	LastError      string  `json:"last_error,omitempty"`
}

func (r *relay) statusList() []envStatus {
	r.mu.Lock()
	envs := make([]*env, 0, len(r.envs))
	for _, e := range r.envs {
		envs = append(envs, e)
	}
	r.mu.Unlock()
	out := make([]envStatus, 0, len(envs))
	for _, e := range envs {
		e.mu.Lock()
		s := envStatus{Env: e.id, Connected: e.connected, Source: e.source, Via: e.via, Subscribers: len(e.subs),
			FailedAttempts: e.failures, LastError: e.lastErr, PersistError: e.persistErr}
		if e.data != nil {
			for _, it := range e.data.Flags {
				if !it.Deleted {
					s.Flags++
				}
			}
		}
		if !e.confirmedAt.IsZero() {
			s.DataAgeSeconds = time.Since(e.confirmedAt).Seconds()
		}
		if !e.savedAt.IsZero() {
			s.SavedAt = e.savedAt.UTC().Format(time.RFC3339)
		}
		e.mu.Unlock()
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Env < out[j].Env })
	return out
}

func (r *relay) handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	r.mu.Lock()
	cached := len(r.disk)
	r.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"mode":           "upstream",
		"version":        version,
		"ready":          r.ready.Load(),
		"draining":       r.draining.Load(),
		"streams":        r.streams.Load(),
		"cached_on_disk": cached,
		"peers":          r.peers != nil,
		"overrides":      countLive(r.overrides.snapshot()),
		"admin":          r.admin.enabled(),
		"environments":   r.statusList(),
	})
}

func (r *relay) scrapeGauges(m *metrics) {
	m.set("flagrelay_downstream_streams", float64(r.streams.Load()))
	if r.draining.Load() {
		m.set("flagrelay_draining", 1)
	} else {
		m.set("flagrelay_draining", 0)
	}
	st := r.statusList()
	m.set("flagrelay_environments", float64(len(st)))
	m.set("flagrelay_overrides", float64(countLive(r.overrides.snapshot())))
	for _, s := range st {
		m.set("flagrelay_data_age_seconds", s.DataAgeSeconds, "env", s.Env)
		for _, src := range []string{"upstream", "disk", "peer"} {
			v := 0.0
			if s.Source == src {
				v = 1
			}
			m.set("flagrelay_data_source", v, "env", s.Env, "source", src)
		}
	}
}

func (r *relay) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/all", r.handleStream)
	mux.HandleFunc("/flags", r.handleStream)
	mux.HandleFunc("/sdk/latest-all", r.handlePoll)
	mux.HandleFunc("/sdk/poll", r.handlePoll)
	mux.HandleFunc("/sdk/latest-flags", r.handlePoll)
	mux.HandleFunc("/sdk/latest-flags/", r.handlePoll)
	mux.HandleFunc("/sdk/latest-segments", r.handlePoll)
	mux.HandleFunc("/sdk/latest-segments/", r.handlePoll)
	mux.HandleFunc("/bulk", r.handleEvents)
	mux.HandleFunc("/diagnostic", r.handleEvents)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", r.handleReady)
	mux.HandleFunc("/status", r.handleStatus)
	mux.Handle("/metrics", r.metrics)
	return mux
}

func (r *relay) peerMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/relay/v1/snapshot/", r.handlePeerSnapshot)
	mux.HandleFunc("/relay/v1/overrides", r.admin.handlePeer)
	return mux
}

// drain closes every SDK stream, spread evenly over drainSpread, so the SDKs
// reconnect to the other replica a few at a time instead of all at once.
func (r *relay) drain() {
	r.mu.Lock()
	envs := make([]*env, 0, len(r.envs))
	for _, e := range r.envs {
		envs = append(envs, e)
	}
	r.mu.Unlock()
	var subs []*subscriber
	for _, e := range envs {
		subs = append(subs, e.subscribers()...)
	}
	if len(subs) == 0 {
		return
	}
	rand.Shuffle(len(subs), func(i, j int) { subs[i], subs[j] = subs[j], subs[i] })
	step := r.cfg.drainSpread / time.Duration(len(subs))
	log.Printf("draining %d SDK stream(s) over %s", len(subs), r.cfg.drainSpread)
	for _, s := range subs {
		s.close()
		time.Sleep(step)
	}
}

// start makes the relay ready: merge the disk cache with the peers first.
func (r *relay) start(ctx context.Context) {
	r.syncDiskWithPeers(ctx)
	r.admin.startupSync(ctx, r.cfg.peerSyncWait)
	go r.diskRetryLoop(ctx)
	go r.overrides.retryLoop(ctx.Done(), r.cfg.persistRetry)
	go r.admin.syncLoop(ctx, r.cfg.overrideSync)
	r.ready.Store(true)
}

// serveUpstream runs the relay until SIGTERM/SIGINT, then drains.
func serveUpstream() error {
	cfg, err := loadRelayConfig()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newRelay(ctx, cfg)
	go r.events.run(ctx)

	srv := &http.Server{Addr: cfg.addr, Handler: r.mux(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	errc := make(chan error, 2)
	go func() { errc <- srv.ListenAndServe() }()
	var peerSrv, adminSrv *http.Server
	// ADMIN_PORT may equal PEER_PORT: one listener then serves both.
	shared := r.peers != nil && r.admin.enabled() && cfg.adminAddr == cfg.peerAddr
	if r.peers != nil && !shared {
		peerSrv = &http.Server{Addr: cfg.peerAddr, Handler: r.peerMux(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}
		go func() { errc <- peerSrv.ListenAndServe() }()
	}
	if r.admin.enabled() {
		h := http.Handler(r.admin.mux())
		if shared {
			h = combinedMux(r.admin.mux(), r.peerMux())
		}
		adminSrv = &http.Server{Addr: cfg.adminAddr, Handler: h, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second}
		go func() { errc <- adminSrv.ListenAndServe() }()
		log.Printf("admin API on %s", cfg.adminAddr)
	} else {
		log.Printf("admin API disabled (set RELAY_ADMIN_TOKEN to enable local overrides)")
	}
	log.Printf("flag-file-relay %s upstream mode on %s (stream %s, poll %s, events %s, cache %q, peers %q)",
		version, cfg.addr, cfg.streamURI, cfg.pollURI, cfg.eventsURI, cfg.cacheDir, cfg.peerDNS)
	go r.start(ctx)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case err := <-errc:
		return err
	case s := <-sig:
		log.Printf("%s: draining (delay %s, spread %s)", s, cfg.drainDelay, cfg.drainSpread)
	}
	r.draining.Store(true)
	time.Sleep(cfg.drainDelay) // let the endpoint controller take us out of the Service
	r.drain()
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	_ = srv.Shutdown(sctx)
	r.events.flush(sctx)
	r.events.wg.Wait()
	if peerSrv != nil {
		_ = peerSrv.Shutdown(sctx)
	}
	if adminSrv != nil {
		_ = adminSrv.Shutdown(sctx)
	}
	cancel()
	log.Printf("stopped")
	return nil
}

func envInt(k string, d int) int {
	var v int
	if _, err := fmt.Sscan(os.Getenv(k), &v); err == nil && v > 0 {
		return v
	}
	return d
}

func envDur(k string, d time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil && v >= 0 {
		return v
	}
	return d
}

// fileLock returns the lock that serializes writes of one cache file, shared
// by env commits, env retries and start-up retries. Never held with r.mu.
func (r *relay) fileLock(hash string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	l := r.files[hash]
	if l == nil {
		l = &sync.Mutex{}
		r.files[hash] = l
	}
	return l
}

// ── local overrides (overrideHost) ───────────────────────────────────────────

func (r *relay) envList() []*env {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*env, 0, len(r.envs))
	for _, e := range r.envs {
		out = append(out, e)
	}
	return out
}

// overridesChanged re-serves keys in every environment but except.
func (r *relay) overridesChanged(keys []string, except *env) {
	for _, e := range r.envList() {
		if e != except {
			e.overridesChanged(keys)
		}
	}
}

func (r *relay) applyOverrides(keys []string) { r.overridesChanged(keys, nil) }

// sourceVersions: environments being relayed, plus those only on disk.
func (r *relay) sourceVersions(key string) map[string]uint64 {
	out := map[string]uint64{}
	add := func(hash string, ds *dataSet) {
		if ds == nil {
			return
		}
		var v uint64
		if it, ok := ds.Flags[key]; ok {
			v = it.Version
		}
		out[hash] = v
	}
	for _, e := range r.envList() {
		e.mu.Lock()
		add(e.hash, e.data)
		e.mu.Unlock()
	}
	r.mu.Lock()
	for h, d := range r.disk {
		if _, ok := out[h]; !ok && r.byHash[h] == nil {
			add(h, d.data)
		}
	}
	r.mu.Unlock()
	return out
}

func (r *relay) adminEnvs() []adminEnv {
	var out []adminEnv
	seen := map[string]bool{}
	for _, e := range r.envList() {
		e.mu.Lock()
		out = append(out, adminEnv{hash: e.hash, raw: e.data, view: e.view})
		e.mu.Unlock()
		seen[e.hash] = true
	}
	ovs := r.overrides.snapshot()
	r.mu.Lock()
	for h, d := range r.disk {
		if !seen[h] {
			out = append(out, adminEnv{hash: h, raw: d.data, view: buildView(d.data, ovs, nil)})
		}
	}
	r.mu.Unlock()
	return out
}

// reconcile runs observe for every environment with data (an override merged
// from a peer may be older than the source data held here).
// Environments only on disk (no SDK has asked for them yet) are checked too:
// they are served from that copy as soon as an SDK does.
func (r *relay) reconcile() []string {
	var dropped []string
	seen := map[string]bool{}
	for _, e := range r.envList() {
		e.mu.Lock()
		ds := e.data
		e.mu.Unlock()
		seen[e.hash] = true
		dropped = append(dropped, r.overrides.observe(e.hash, ds, nil)...)
	}
	r.mu.Lock()
	disk := map[string]*dataSet{}
	for h, d := range r.disk {
		if !seen[h] && r.byHash[h] == nil {
			disk[h] = d.data
		}
	}
	r.mu.Unlock()
	for h, ds := range disk {
		dropped = append(dropped, r.overrides.observe(h, ds, nil)...)
	}
	return dropped
}
