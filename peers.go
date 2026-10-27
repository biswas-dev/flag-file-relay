package main

// peers.go: anti-entropy between relay replicas. A replica that has no
// upstream-confirmed data for an environment (it restarted while upstream is
// down) pulls the other replicas' copies and keeps the newest version of every
// flag and segment, so both replicas hand SDKs the same generation.
//
// The peer API listens on its own port (PEER_PORT), reachable through the
// headless peer Service only, and serves one environment by sha256(key):
// GET /relay/v1/snapshot/<hash>. There is no listing endpoint.

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type peerSet struct {
	static []string // fixed host:port list (tests); otherwise resolved from dns
	dns    string   // headless Service name resolving to every replica
	port   string
	selfIP string
	client *http.Client
}

func newPeerSet(dns string, static []string, port, selfIP string) *peerSet {
	if dns == "" && len(static) == 0 {
		return nil
	}
	return &peerSet{dns: dns, static: static, port: port, selfIP: selfIP, client: &http.Client{Timeout: 2 * time.Second}}
}

func (p *peerSet) addrs(ctx context.Context) []string {
	a, _ := p.resolve(ctx)
	return a
}

// resolve returns the peer addresses and whether discovery itself worked
// (a DNS failure is not the same as "no other replica").
func (p *peerSet) resolve(ctx context.Context) ([]string, bool) {
	if p.static != nil {
		return p.static, true
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupHost(ctx, p.dns)
	if err != nil {
		return nil, false
	}
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		if ip != p.selfIP {
			out = append(out, net.JoinHostPort(ip, p.port))
		}
	}
	return out, true
}

type peerCopy struct {
	data        *dataSet
	confirmedAt time.Time
}

// fetch asks every peer for the environment and returns the copies it got,
// plus how many peers answered at all (200 or 404).
func (p *peerSet) fetch(ctx context.Context, hash string) []peerCopy {
	copies, _ := p.fetchCounted(ctx, hash)
	return copies
}

func (p *peerSet) fetchCounted(ctx context.Context, hash string) ([]peerCopy, int) {
	addrs := p.addrs(ctx)
	var mu sync.Mutex
	var out []peerCopy
	answered := 0
	var wg sync.WaitGroup
	for _, a := range addrs {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+a+"/relay/v1/snapshot/"+hash, nil)
			if err != nil {
				return
			}
			resp, err := p.client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			mu.Lock()
			answered++
			mu.Unlock()
			if resp.StatusCode != http.StatusOK {
				return
			}
			var cf cacheFile
			if err := json.NewDecoder(io.LimitReader(resp.Body, maxDataSet)).Decode(&cf); err != nil {
				return
			}
			ds, err := parseDataSet(cf.Data)
			if err != nil {
				return
			}
			mu.Lock()
			out = append(out, peerCopy{data: ds, confirmedAt: cf.ConfirmedAt})
			mu.Unlock()
		}(a)
	}
	wg.Wait()
	return out, answered
}

// handlePeerSnapshot serves GET /relay/v1/snapshot/<hash> on the peer port:
// the data (with tombstones) and when the upstream chain last confirmed it.
func (r *relay) handlePeerSnapshot(w http.ResponseWriter, req *http.Request) {
	hash := strings.TrimPrefix(req.URL.Path, "/relay/v1/snapshot/")
	if req.Method != http.MethodGet || !validHash(hash) || r.denied(hash) {
		http.NotFound(w, req)
		return
	}
	var payload []byte
	var confirmed time.Time
	r.mu.Lock()
	e := r.byHash[hash]
	var dData *dataSet
	var dConf time.Time
	if d := r.disk[hash]; d != nil {
		dData, dConf = d.data, d.confirmedAt
	}
	r.mu.Unlock()
	if e != nil {
		payload, confirmed = e.peerSnapshot()
	}
	if payload == nil && dData != nil {
		payload, confirmed = dData.marshalWithTombstones(), dConf
	}
	if payload == nil {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cacheFile{Format: 1, ConfirmedAt: confirmed, Data: payload})
}

// syncDiskWithPeers runs at start, before the relay reports ready: every
// environment cached on disk is replaced by a peer's copy when that copy was
// confirmed by upstream later than ours (wholesale, never merged). It waits up
// to peerSyncWait for at least one peer to answer; if none does (all replicas
// restarting together), it proceeds with the disk copy and says so.
func (r *relay) syncDiskWithPeers(ctx context.Context) {
	if r.peers == nil || len(r.disk) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, r.cfg.peerSyncWait)
	defer cancel()
	r.mu.Lock()
	hashes := make([]string, 0, len(r.disk))
	for h := range r.disk {
		hashes = append(hashes, h)
	}
	r.mu.Unlock()
	for _, h := range hashes {
		for {
			copies, answered := r.peers.fetchCounted(ctx, h)
			r.mu.Lock()
			d := r.disk[h]
			adopted := false
			for _, c := range copies {
				if c.confirmedAt.After(d.confirmedAt) {
					d.data, d.confirmedAt, adopted = d.data.replaceFrom(c.data), c.confirmedAt, true
				}
			}
			r.mu.Unlock()
			if adopted {
				r.writeDiskEntry(h)
				log.Printf("env %s: cached copy replaced by a fresher peer copy at start", h[:8])
			}
			if a, resolved := r.peers.resolve(ctx); answered > 0 || (resolved && len(a) == 0) {
				r.metrics.inc("flagrelay_startup_peer_sync_total", "result", "ok")
				break
			}
			select {
			case <-ctx.Done():
				r.metrics.inc("flagrelay_startup_peer_sync_total", "result", "no_peer_answered")
				log.Printf("env %s: no peer answered within %s; serving the disk copy", h[:8], r.cfg.peerSyncWait)
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

// writeDiskEntry persists a start-up disk entry; on failure it stays dirty,
// is counted like any other cache write error, and is retried by
// diskRetryLoop. It holds the key's file lock across the ownership check and
// the write, and env writes take the same lock, so an env created meanwhile
// writes after us (newer data last); once an env owns the key only the env
// writes its file. r.mu is never held during disk I/O.
func (r *relay) writeDiskEntry(h string) {
	if r.cfg.cacheDir == "" {
		return
	}
	fl := r.fileLock(h)
	fl.Lock() // any env write for this key waits for us, and we for it
	defer fl.Unlock()
	r.mu.Lock()
	d := r.disk[h]
	owned := r.byHash[h] != nil
	var payload []byte
	var conf time.Time
	if d != nil && !owned {
		payload, conf = d.data.marshalWithTombstones(), d.confirmedAt
	}
	r.mu.Unlock()
	if payload == nil {
		return // an env owns the key now: only the env writes its file
	}
	err := writeCacheFile(r.cfg.cacheDir, h, payload, "peer", conf) // r.mu not held
	r.mu.Lock()
	d.dirty = err != nil
	r.mu.Unlock()
	if err != nil {
		r.metrics.inc("flagrelay_cache_write_errors_total")
		log.Printf("cache %s: write after peer sync failed, will retry: %v", h[:8], err)
	}
}

// diskRetryLoop rewrites dirty start-up entries every 5 s until they are on
// disk. Entries already taken over by an env are retried by that env.
func (r *relay) diskRetryLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.persistRetry)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r.mu.Lock()
		var dirty []string
		for h, d := range r.disk {
			if d.dirty {
				dirty = append(dirty, h)
			}
		}
		r.mu.Unlock()
		for _, h := range dirty {
			r.writeDiskEntry(h) // rechecks ownership under r.mu
		}
	}
}
