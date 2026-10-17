package main

// metrics.go: a minimal Prometheus text-format registry (counters and
// gauges with labels), so the binary stays dependency-free.

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

type metrics struct {
	mu     sync.Mutex
	values map[string]map[string]float64 // name -> labels -> value
	kinds  map[string]string             // name -> counter|gauge
	help   map[string]string
	gauges []func(m *metrics) // computed at scrape time
}

func newMetrics() *metrics {
	m := &metrics{values: map[string]map[string]float64{}, kinds: map[string]string{}, help: map[string]string{}}
	for name, h := range map[string][2]string{
		"flagrelay_upstream_connected":            {"gauge", "1 while the upstream stream for this environment is open"},
		"flagrelay_upstream_reconnects_total":     {"counter", "Upstream stream attempts that ended"},
		"flagrelay_upstream_errors_total":         {"counter", "Upstream errors by kind (stream, auth, poll, event)"},
		"flagrelay_data_age_seconds":              {"gauge", "Seconds since upstream last confirmed this environment's data"},
		"flagrelay_data_source":                   {"gauge", "1 for where the served data came from (upstream or disk)"},
		"flagrelay_environments":                  {"gauge", "SDK keys being relayed"},
		"flagrelay_downstream_streams":            {"gauge", "Open SDK streams"},
		"flagrelay_downstream_stream_opens_total": {"counter", "SDK streams opened"},
		"flagrelay_downstream_slow_closed_total":  {"counter", "SDK streams closed because they fell behind"},
		"flagrelay_requests_total":                {"counter", "SDK requests by endpoint and status code"},
		"flagrelay_events_forwarded_total":        {"counter", "Analytics events forwarded upstream"},
		"flagrelay_events_dropped_total":          {"counter", "Analytics events dropped (buffer full or upstream failure)"},
		"flagrelay_diagnostics_dropped_total":     {"counter", "Diagnostic events not forwarded (saturated or upstream error)"},
		"flagrelay_invalid_items_total":           {"counter", "Malformed flags/segments set aside (previous copy kept)"},
		"flagrelay_reconcile_changes_total":       {"counter", "Reconciliation polls that found changes the stream missed"},
		"flagrelay_startup_peer_sync_total":       {"counter", "Start-up peer sync outcomes"},
		"flagrelay_cache_write_errors_total":      {"counter", "Failed writes of the on-disk cache"},
		"flagrelay_draining":                      {"gauge", "1 while the relay is shutting down"},
	} {
		m.kinds[name], m.help[name] = h[0], h[1]
	}
	return m
}

func labelKey(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	parts := make([]string, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		v := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(kv[i+1])
		parts = append(parts, fmt.Sprintf(`%s="%s"`, kv[i], v))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (m *metrics) add(name string, delta float64, kv ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values[name] == nil {
		m.values[name] = map[string]float64{}
	}
	m.values[name][labelKey(kv)] += delta
}

func (m *metrics) inc(name string, kv ...string) { m.add(name, 1, kv...) }

func (m *metrics) set(name string, v float64, kv ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values[name] == nil {
		m.values[name] = map[string]float64{}
	}
	m.values[name][labelKey(kv)] = v
}

func (m *metrics) get(name string, kv ...string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.values[name][labelKey(kv)]
}

func (m *metrics) onScrape(f func(m *metrics)) { m.gauges = append(m.gauges, f) }

func (m *metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	for _, f := range m.gauges {
		f(m)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.values))
	for n := range m.values {
		names = append(names, n)
	}
	sort.Strings(names)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, n := range names {
		if h := m.help[n]; h != "" {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", n, h, n, m.kinds[n])
		}
		labels := make([]string, 0, len(m.values[n]))
		for l := range m.values[n] {
			labels = append(labels, l)
		}
		sort.Strings(labels)
		for _, l := range labels {
			fmt.Fprintf(w, "%s%s %g\n", n, l, m.values[n][l])
		}
	}
}
