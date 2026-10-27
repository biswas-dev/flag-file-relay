// flag-file-relay — a tiny, file-backed, LaunchDarkly-relay-compatible feature
// flag server for fully offline / air-gapped deployments.
//
// It implements the public LaunchDarkly server-side relay-proxy endpoints
// (GET /all streaming, GET /sdk/latest-all polling, POST /bulk events) and
// serves flag data from a local YAML file — no database, no external network,
// no dependency on any hosted flag service. Point a LaunchDarkly server SDK at
// it via `relayProxyUrl` and it works with zero client changes.
//
// It is intentionally standalone and vendor-neutral: the wire format is the
// documented LaunchDarkly streaming/polling protocol; nothing here is tied to
// any particular hosted control plane.
//
// Two modes, one binary:
//
//	flag-file-relay serve            # run the relay (reads $FLAGS_FILE)
//	flag-file-relay list             # show flags + values
//	flag-file-relay get <flag>       # print one flag's value
//	flag-file-relay set <flag> <v> [--tenant T] [--user U]   # edit the file
//
// Env (serve): PORT (8080), FLAGS_FILE (./flags.yaml), SDK_KEY (optional — if
// set, requests must present it; if empty, any key is accepted).
// Local overrides (both modes, see overrides.go / admin.go): RELAY_ADMIN_TOKEN
// enables the admin API on ADMIN_PORT (8082); overrides are kept in
// OVERRIDES_FILE (default $CACHE_DIR/overrides.json) and replicated to
// PEER_DNS / PEER_ADDRS on PEER_PORT (8081).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ── File schema ─────────────────────────────────────────────────────────────
// A human-editable flags file. Each flag has a default value plus optional
// per-tenant / per-user overrides. Values may be bool, number, or string.
//
//	flags:
//	  new-rbac:
//	    value: false
//	    tenants: { prod.elastio.saic.com: true }
//	  new-alerts: false          # shorthand: bare value = default only
//	  max-amount-of-report-schedules:
//	    value: 15

type FlagSpec struct {
	Value   interface{}            `yaml:"value"`
	Tenants map[string]interface{} `yaml:"tenants,omitempty"`
	Users   map[string]interface{} `yaml:"users,omitempty"`
}

type FlagsFile struct {
	Flags map[string]FlagSpec `yaml:"flags"`
}

// UnmarshalYAML lets a flag be either a bare value or a full FlagSpec.
func (f *FlagSpec) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.MappingNode {
		type raw FlagSpec
		return n.Decode((*raw)(f))
	}
	return n.Decode(&f.Value)
}

func loadFlags(path string) (*FlagsFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ff FlagsFile
	if err := yaml.Unmarshal(b, &ff); err != nil {
		return nil, err
	}
	if ff.Flags == nil {
		ff.Flags = map[string]FlagSpec{}
	}
	return &ff, nil
}

// ── LD snapshot builder ─────────────────────────────────────────────────────
// Convert the friendly file into the {flags, segments} object a LaunchDarkly
// server SDK expects. Rule/target ids are strings; no non-standard fields.

func buildSnapshot(ff *FlagsFile) map[string]interface{} {
	flags := map[string]interface{}{}
	for key, spec := range ff.Flags {
		flags[key] = buildFlag(key, spec)
	}
	return map[string]interface{}{"flags": flags, "segments": map[string]interface{}{}}
}

func buildFlag(key string, spec FlagSpec) map[string]interface{} {
	// variations: default first (index 0), then any distinct override values.
	variations := []interface{}{spec.Value}
	idxOf := func(v interface{}) int {
		for i, ev := range variations {
			if fmt.Sprint(ev) == fmt.Sprint(v) {
				return i
			}
		}
		variations = append(variations, v)
		return len(variations) - 1
	}

	var targets []interface{}
	// group tenant overrides by variation index
	addTargets := func(kind string, overrides map[string]interface{}) {
		byVar := map[int][]string{}
		keys := make([]string, 0, len(overrides))
		for k := range overrides {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, ctxKey := range keys {
			byVar[idxOf(overrides[ctxKey])] = append(byVar[idxOf(overrides[ctxKey])], ctxKey)
		}
		vidxs := make([]int, 0, len(byVar))
		for vi := range byVar {
			vidxs = append(vidxs, vi)
		}
		sort.Ints(vidxs)
		for _, vi := range vidxs {
			targets = append(targets, map[string]interface{}{
				"contextKind": kind,
				"values":      byVar[vi],
				"variation":   vi,
			})
		}
	}
	addTargets("tenant", spec.Tenants)
	addTargets("user", spec.Users)

	if targets == nil {
		targets = []interface{}{}
	}
	return map[string]interface{}{
		"key":           key,
		"version":       1,
		"on":            true,
		"variations":    variations,
		"offVariation":  0,
		"fallthrough":   map[string]interface{}{"variation": 0}, // default value
		"targets":       targets,
		"rules":         []interface{}{},
		"prerequisites": []interface{}{},
		"salt":          key,
		"trackEvents":   false,
		"clientSideAvailability": map[string]interface{}{
			"usingMobileKey":     false,
			"usingEnvironmentId": true,
		},
		"deleted": false,
	}
}

// ── Server ──────────────────────────────────────────────────────────────────

type server struct {
	path      string
	sdkKey    string
	overrides *overrideStore
	admin     *overrideAdmin

	mu      sync.Mutex
	rawJSON []byte   // the file as an LD data set, to detect changes
	raw     *dataSet // the file's flags
	view    *dataSet // what SDKs are served: raw with the local overrides
	payload []byte   // view as {flags, segments}
	bump    map[string]uint64
	subs    map[*fileSub]struct{}
}

type fileSub struct {
	proto int
	ch    chan []byte
	kick  chan struct{}
	once  sync.Once
}

func (f *fileSub) close() { f.once.Do(func() { close(f.kick) }) }

func (s *server) rebuild() error {
	ff, err := loadFlags(s.path)
	if err != nil {
		return err
	}
	b, err := json.Marshal(buildSnapshot(ff))
	if err != nil {
		return err
	}
	s.mu.Lock()
	same := string(s.rawJSON) == string(b)
	s.mu.Unlock()
	if same {
		return nil
	}
	ds, err := parseDataSet(b)
	if err != nil {
		return err
	}
	if len(ds.invalid) > 0 {
		return fmt.Errorf("flags not servable: %v", ds.invalid)
	}
	// The file has no versions, so file changes never replace an override
	// (unlike upstream mode): it stays until it is removed.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rawJSON, s.raw, s.bump = b, ds, nil
	s.setViewLocked(buildView(ds, s.overrides.snapshot(), nil))
	s.broadcastLocked(putMsgsFor(s.payload, s.view))
	return nil
}

func (s *server) setViewLocked(v *dataSet) {
	if v == nil {
		v = newDataSet()
	}
	s.view = v
	s.payload = v.marshal()
}

func (s *server) current() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.payload
}

func (s *server) broadcastLocked(msgs [2][]byte) {
	for sub := range s.subs {
		msg := msgs[sub.proto]
		if msg == nil {
			continue
		}
		select {
		case sub.ch <- msg:
		default:
			sub.close() // fell behind: it reconnects and gets a fresh put
		}
	}
}

// applyOverrides (overrideHost) re-serves keys: a patch or delete per flag
// whose served form changed.
func (s *server) applyOverrides(keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.raw == nil {
		return
	}
	old := s.view
	ovs := s.overrides.snapshot()
	all := overlayKeys(keys, ovs, s.bump)
	v, bump := monotonic(old, s.raw, ovs, s.bump, all)
	s.bump = bump
	s.setViewLocked(v)
	if msgs := diffMsgs(old, s.view, all, nil); msgs[0] != nil || msgs[1] != nil {
		s.broadcastLocked(msgs)
	}
}

// reconcile (overrideHost): file mode never drops an override for a file
// change (the file has no versions).
func (s *server) reconcile() []string { return nil }

func (s *server) sourceVersions(key string) (map[string]uint64, map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var v uint64
	if s.raw != nil {
		if it, ok := s.raw.Flags[key]; ok {
			v = it.Version
		}
	}
	return map[string]uint64{"file": v}, map[string]bool{"file": v == 0}
}

func (s *server) adminEnvs() []adminEnv {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []adminEnv{{hash: "file", raw: s.raw, view: s.view}}
}

func (s *server) authOK(r *http.Request) bool {
	if s.sdkKey == "" {
		return true
	}
	return r.Header.Get("Authorization") == s.sdkKey
}

// GET /all ({flags, segments}) and GET /flags (legacy: flags only) —
// server-side streaming (SSE). Emits a "put" with the full data set, then a
// fresh "put" whenever the file changes and a "patch"/"delete" when a local
// override changes; keepalive comments in between.
func (s *server) handleAll(w http.ResponseWriter, r *http.Request) {
	if !s.authOK(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	proto := protoAll
	if r.URL.Path == "/flags" {
		proto = protoFlags
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sub := &fileSub{proto: proto, ch: make(chan []byte, subscriberQueue), kick: make(chan struct{})}
	s.mu.Lock()
	s.subs[sub] = struct{}{}
	put := putMsgsFor(s.payload, s.view)[proto]
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subs, sub)
		s.mu.Unlock()
	}()

	w.Write(put)
	fl.Flush()

	ka := time.NewTicker(25 * time.Second)
	defer ka.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-sub.kick:
			return
		case b := <-sub.ch:
			w.Write(b)
			fl.Flush()
		case <-ka.C:
			fmt.Fprint(w, ":\n\n")
			fl.Flush()
		}
	}
}

// GET /sdk/latest-all — polling snapshot.
func (s *server) handleLatestAll(w http.ResponseWriter, r *http.Request) {
	if !s.authOK(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(s.current())
}

// POST /bulk — event ingestion. Offline: accept and drop.
func (s *server) handleBulk(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusAccepted)
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func (s *server) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/all", s.handleAll)
	mux.HandleFunc("/flags", s.handleAll)
	mux.HandleFunc("/sdk/latest-all", s.handleLatestAll)
	mux.HandleFunc("/sdk/poll", s.handleLatestAll)
	mux.HandleFunc("/bulk", s.handleBulk)
	mux.HandleFunc("/health", s.handleHealth)
	return mux
}

func newFileServer(path, sdkKey, overridesFile, token string, peers *peerSet) *server {
	m := newMetrics()
	s := &server{path: path, sdkKey: sdkKey, subs: map[*fileSub]struct{}{}}
	s.overrides = newOverrideStore(overridesFile, replicaName(), m)
	s.admin = newOverrideAdmin(s.overrides, s, token, peers, "", m)
	return s
}

func serve() error {
	cacheDir := os.Getenv("CACHE_DIR")
	if cacheDir != "" {
		if err := os.MkdirAll(cacheDir, 0o700); err != nil {
			return fmt.Errorf("CACHE_DIR: %w", err)
		}
	}
	peerPort := envOr("PEER_PORT", "8081")
	var peerAddrs []string
	for _, a := range strings.Split(os.Getenv("PEER_ADDRS"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			peerAddrs = append(peerAddrs, a)
		}
	}
	peers := newPeerSet(os.Getenv("PEER_DNS"), peerAddrs, peerPort, os.Getenv("POD_IP"))
	s := newFileServer(envOr("FLAGS_FILE", "flags.yaml"), os.Getenv("SDK_KEY"), overridesPath(cacheDir), adminToken(), peers)
	if err := s.rebuild(); err != nil {
		return fmt.Errorf("loading %s: %w", s.path, err)
	}
	// hot-reload: poll the file mtime (works for ConfigMap-mounted files too).
	go func() {
		var last time.Time
		for range time.Tick(5 * time.Second) {
			if fi, err := os.Stat(s.path); err == nil && fi.ModTime() != last {
				last = fi.ModTime()
				if err := s.rebuild(); err != nil {
					fmt.Fprintf(os.Stderr, "reload error: %v\n", err)
				}
			}
		}
	}()

	errc := make(chan error, 3)
	adminPort := envOr("ADMIN_PORT", "8082")
	var adminHandler http.Handler
	if s.admin.enabled() {
		adminHandler = s.admin.mux()
		if peers != nil {
			pmux := http.NewServeMux()
			pmux.HandleFunc("/relay/v1/overrides", s.admin.handlePeer)
			if peerPort == adminPort { // one listener serves both
				adminHandler = combinedMux(s.admin.mux(), pmux)
			} else {
				psrv := &http.Server{Addr: ":" + peerPort, Handler: pmux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}
				go func() { errc <- psrv.ListenAndServe() }()
			}
			s.admin.startupSync(context.Background(), envDur("PEER_SYNC_WAIT", 10*time.Second))
			go s.admin.syncLoop(context.Background(), envDur("OVERRIDE_SYNC_INTERVAL", 10*time.Second))
		}
		go s.overrides.retryLoop(nil, 5*time.Second)
		asrv := &http.Server{Addr: ":" + adminPort, Handler: adminHandler, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second}
		go func() { errc <- asrv.ListenAndServe() }()
		fmt.Printf("admin API on %s\n", asrv.Addr)
	}
	addr := ":" + envOr("PORT", "8080")
	srv := &http.Server{Addr: addr, Handler: s.mux(), ReadHeaderTimeout: 10 * time.Second}
	go func() { errc <- srv.ListenAndServe() }()
	fmt.Printf("flag-file-relay serving %s on %s (auth: %v, admin: %v)\n", s.path, addr, s.sdkKey != "", s.admin.enabled())
	return <-errc
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// ── flagctl (file management) ───────────────────────────────────────────────

func parseValue(s string) interface{} {
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func writeFlags(path string, ff *FlagsFile) error {
	b, err := yaml.Marshal(ff)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func cliList(path string) error {
	ff, err := loadFlags(path)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(ff.Flags))
	for k := range ff.Flags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sp := ff.Flags[k]
		extra := ""
		if len(sp.Tenants) > 0 {
			extra += fmt.Sprintf("  tenants=%v", sp.Tenants)
		}
		if len(sp.Users) > 0 {
			extra += fmt.Sprintf("  users=%v", sp.Users)
		}
		fmt.Printf("%-40s %v%s\n", k, sp.Value, extra)
	}
	return nil
}

func cliGet(path, flag string) error {
	ff, err := loadFlags(path)
	if err != nil {
		return err
	}
	sp, ok := ff.Flags[flag]
	if !ok {
		return fmt.Errorf("flag %q not found", flag)
	}
	b, _ := yaml.Marshal(map[string]FlagSpec{flag: sp})
	fmt.Print(string(b))
	return nil
}

func cliSet(path, flag, val string, tenant, user string) error {
	ff, err := loadFlags(path)
	if err != nil {
		if os.IsNotExist(err) {
			ff = &FlagsFile{Flags: map[string]FlagSpec{}}
		} else {
			return err
		}
	}
	sp := ff.Flags[flag]
	v := parseValue(val)
	switch {
	case tenant != "":
		if sp.Tenants == nil {
			sp.Tenants = map[string]interface{}{}
		}
		sp.Tenants[tenant] = v
	case user != "":
		if sp.Users == nil {
			sp.Users = map[string]interface{}{}
		}
		sp.Users[user] = v
	default:
		sp.Value = v
	}
	ff.Flags[flag] = sp
	if err := writeFlags(path, ff); err != nil {
		return err
	}
	fmt.Printf("set %s = %v", flag, val)
	if tenant != "" {
		fmt.Printf(" for tenant %s", tenant)
	}
	if user != "" {
		fmt.Printf(" for user %s", user)
	}
	fmt.Println()
	return nil
}

func main() {
	path := envOr("FLAGS_FILE", "flags.yaml")
	args := os.Args[1:]
	if len(args) == 0 {
		args = []string{"serve"}
	}
	var err error
	switch args[0] {
	case "serve":
		if upstreamModeConfigured() {
			err = serveUpstream()
		} else {
			err = serve()
		}
	case "seed":
		// flag-file-relay seed <latest-all.json>  (env SDK_KEY, CACHE_DIR)
		// Writes an upstream-mode cache entry so an appliance can start
		// offline before it ever reached upstream.
		if len(args) < 2 || os.Getenv("SDK_KEY") == "" || os.Getenv("CACHE_DIR") == "" {
			err = fmt.Errorf("usage: SDK_KEY=... CACHE_DIR=... flag-file-relay seed <latest-all.json>")
		} else {
			var b []byte
			if b, err = os.ReadFile(args[1]); err == nil {
				err = seedCache(os.Getenv("CACHE_DIR"), os.Getenv("SDK_KEY"), b)
			}
		}
	case "wait":
		// flag-file-relay wait <url> [timeout]: start-up gate for flag
		// consumers (initContainer). Polls <url> (the relay's /readyz) until
		// it answers 200, or the timeout passes; never fails, so a missing
		// relay delays a consumer but cannot block it.
		if len(args) < 2 {
			err = fmt.Errorf("usage: flag-file-relay wait <url> [timeout, default 120s]")
		} else {
			timeout := 120 * time.Second
			if len(args) > 2 {
				if d, perr := time.ParseDuration(args[2]); perr == nil {
					timeout = d
				}
			}
			waitReady(args[1], timeout)
		}
	case "list":
		err = cliList(path)
	case "get":
		if len(args) < 2 {
			err = fmt.Errorf("usage: flag-file-relay get <flag>")
		} else {
			err = cliGet(path, args[1])
		}
	case "set":
		var tenant, user string
		pos := []string{}
		for i := 1; i < len(args); i++ {
			switch args[i] {
			case "--tenant":
				i++
				if i < len(args) {
					tenant = args[i]
				}
			case "--user":
				i++
				if i < len(args) {
					user = args[i]
				}
			default:
				pos = append(pos, args[i])
			}
		}
		if len(pos) < 2 {
			err = fmt.Errorf("usage: flag-file-relay set <flag> <value> [--tenant T] [--user U]")
		} else {
			err = cliSet(path, pos[0], pos[1], tenant, user)
		}
	default:
		err = fmt.Errorf("unknown command %q (serve|seed|wait|list|get|set)", args[0])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func waitReady(url string, timeout time.Duration) {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for {
		if resp, err := client.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				fmt.Printf("flag relay ready: %s\n", url)
				return
			}
		}
		if time.Now().After(deadline) {
			fmt.Printf("flag relay not ready after %s (%s); starting anyway\n", timeout, url)
			return
		}
		time.Sleep(time.Second)
	}
}
