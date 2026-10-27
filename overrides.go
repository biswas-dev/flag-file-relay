package main

// overrides.go: the local override layer. An operator (through the admin
// API, admin.go) sets a flag to a value on this relay; every SDK endpoint
// then serves that value instead of the source's (upstream or file), and a
// flag the source does not define is synthesized. Overrides are relay-wide:
// they apply to every environment (SDK key) the relay serves.
//
// Records. One record per flag key, a last-writer-wins register: every change
// carries a revision from a hybrid logical clock (wall-clock ms, never moving
// backwards, always above any revision seen) and the replica that made it
// (origin). Removing an override leaves a tombstone, so replicas converge on
// the newest (rev, origin) per key whatever order they hear about changes in.
//
// Against the source (last write wins). An override remembers the version of
// the flag in every environment when it was set. When the source later
// delivers a higher version of that flag, the source change is newer than the
// override: the override is dropped (logged once) and the source served.
// local_only overrides are never dropped by the source.
//
// Durability. Every change is written to OVERRIDES_FILE (default
// $CACHE_DIR/overrides.json) with the same temp-file, fsync, rename, fsync-dir
// sequence as the cached data, before it is served. An admin write that cannot
// be persisted is refused; a change learnt from a peer or from the source is
// served anyway and retried in the background (like the data cache: an OFF
// must not be held back by a full disk).
//
// Serving. Overrides never touch the source data held in memory, on disk or
// handed to peers; SDKs get a "view" built from both (buildView). Changing an
// override can make the version of a flag served to SDKs go down (back to the
// source flag, or a synthesized flag removed), and the SDKs ignore a patch or
// delete that is not newer than what they hold. So every replica keeps a
// per-flag version offset ("bump", memory only) that grows whenever needed to
// keep the served version strictly increasing. It is reset whenever a full put
// goes to every SDK stream (and on restart, when every stream reconnects and
// gets a put), so served versions stay close to the source's.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Limits on what an override may hold.
const (
	maxOverrideValue = 256 << 10
	maxOverrideSetBy = 256
	maxOverrides     = 10000
	maxOverrideBytes = 16 << 20 // all live values together; fits a peer snapshot (maxPeerOverrides)
	maxPeerOverrides = 64 << 20 // bytes read from a peer's override state
	// maxRecords bounds the records (one per flag key ever overridden,
	// removed ones included as tombstones). Tombstones are kept for good, so
	// a replica that missed a deletion can never bring the override back;
	// the cap is enforced on admin writes of a new key, never on merges.
	maxRecords = 2 * maxOverrides
)

var flagKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)

var (
	errPrecondition = errors.New("override changed since it was read")
	errPersist      = errors.New("override could not be persisted")
	errTooMany      = errors.New("too many overrides")
)

type overrideRecord struct {
	Value     json.RawMessage `json:"value,omitempty"` // absent on a tombstone
	SetBy     string          `json:"set_by,omitempty"`
	SetAt     time.Time       `json:"set_at"`
	LocalOnly bool            `json:"local_only,omitempty"`
	Deleted   bool            `json:"deleted,omitempty"`
	Reason    string          `json:"reason,omitempty"` // tombstones: "admin" or "upstream"
	Rev       uint64          `json:"rev"`
	Origin    string          `json:"origin"`
	// Upstream holds, per environment (sha256 of the SDK key, or "file"), the
	// version of the source flag when the override was set (0: no such flag).
	Upstream map[string]uint64 `json:"upstream_versions,omitempty"`
	// Gone marks environments where the source had no live flag (absent, or
	// deleted at that version) when the override was set: a full snapshot
	// that removes a flag keeps its version, so liveness is compared too.
	Gone map[string]bool `json:"upstream_absent,omitempty"`
}

func (o *overrideRecord) live() bool { return o != nil && !o.Deleted }

// newerThan orders records of one key: higher revision, then origin.
func (o *overrideRecord) newerThan(b *overrideRecord) bool {
	if b == nil {
		return true
	}
	if o.Rev != b.Rev {
		return o.Rev > b.Rev
	}
	return o.Origin > b.Origin
}

func (o *overrideRecord) clone() *overrideRecord {
	c := *o
	if o.Upstream != nil {
		c.Upstream = make(map[string]uint64, len(o.Upstream))
		for k, v := range o.Upstream {
			c.Upstream[k] = v
		}
	}
	if o.Gone != nil {
		c.Gone = make(map[string]bool, len(o.Gone))
		for k, v := range o.Gone {
			c.Gone[k] = v
		}
	}
	return &c
}

// validate checks a record received from a peer or read from disk.
func (o *overrideRecord) validate(key string) error {
	if !flagKeyPattern.MatchString(key) {
		return fmt.Errorf("bad flag key")
	}
	if o.Origin == "" || o.Rev == 0 || len(o.SetBy) > maxOverrideSetBy {
		return fmt.Errorf("bad record")
	}
	if o.Deleted {
		return nil
	}
	return validOverrideValue(o.Value)
}

func validOverrideValue(v json.RawMessage) error {
	t := bytes.TrimSpace(v)
	if len(t) == 0 || string(t) == "null" {
		return fmt.Errorf("value is required (null is not a flag value)")
	}
	if len(t) > maxOverrideValue {
		return fmt.Errorf("value larger than %d bytes", maxOverrideValue)
	}
	if !json.Valid(t) {
		return fmt.Errorf("value is not valid JSON")
	}
	if d := jsonDepth(t); d > maxValueDepth {
		return fmt.Errorf("value nested %d levels deep (limit %d)", d, maxValueDepth)
	}
	return scanJSON(t)
}

// maxValueDepth leaves room, under the strictest SDK's 128-level limit, for
// the envelope a value is served in ({"data":{"flags":{key:{"variations":[v]}}}}).
const maxValueDepth = 112

func jsonDepth(b []byte) int {
	depth, max := 0, 0
	inStr, esc := false, false
	for _, c := range b {
		switch {
		case inStr:
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				inStr = false
			}
		case c == '"':
			inStr = true
		case c == '{' || c == '[':
			depth++
			if depth > max {
				max = depth
			}
		case c == '}' || c == ']':
			depth--
		}
	}
	return max
}

type overrideMap = map[string]*overrideRecord

type overrideFile struct {
	Format    int         `json:"format"`
	SavedAt   time.Time   `json:"saved_at"`
	Overrides overrideMap `json:"overrides"`
}

type overrideStore struct {
	path    string // "" = memory only
	origin  string
	metrics *metrics

	mu         sync.Mutex // serializes changes, held across the disk write
	clock      uint64     // last revision issued or seen (ms)
	persistErr string

	cur atomic.Pointer[overrideMap] // immutable snapshot, read without mu
}

// overridesPath resolves where overrides are kept: OVERRIDES_FILE, else
// overrides.json in CACHE_DIR, else nowhere (memory only).
func overridesPath(cacheDir string) string {
	if p := os.Getenv("OVERRIDES_FILE"); p != "" {
		return p
	}
	if cacheDir != "" {
		return filepath.Join(cacheDir, "overrides.json")
	}
	return ""
}

func replicaName() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	if ip := os.Getenv("POD_IP"); ip != "" {
		return ip
	}
	return "relay"
}

func newOverrideStore(path, origin string, m *metrics) *overrideStore {
	s := &overrideStore{path: path, origin: origin, metrics: m}
	empty := overrideMap{}
	s.cur.Store(&empty)
	if path == "" {
		log.Printf("overrides: no OVERRIDES_FILE or CACHE_DIR, overrides are kept in memory only")
		return s
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		log.Printf("overrides: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("overrides: %s unreadable, starting empty: %v", path, err)
		}
		return s
	}
	var f overrideFile
	if err := json.Unmarshal(b, &f); err != nil || f.Format != 1 {
		log.Printf("overrides: %s unreadable, starting empty", path)
		return s
	}
	loaded := overrideMap{}
	for k, o := range f.Overrides {
		if o == nil || o.validate(k) != nil {
			log.Printf("overrides: bad record for %q in %s ignored", k, path)
			continue
		}
		loaded[k] = o
		if o.Rev > s.clock {
			s.clock = o.Rev
		}
	}
	s.cur.Store(&loaded)
	if n := countLive(loaded); n > 0 {
		log.Printf("overrides: loaded %d override(s) from %s", n, path)
	}
	return s
}

func countLive(m overrideMap) int {
	n := 0
	for _, o := range m {
		if o.live() {
			n++
		}
	}
	return n
}

// snapshot returns the current records; callers must not modify them.
func (s *overrideStore) snapshot() overrideMap { return *s.cur.Load() }

func (s *overrideStore) get(key string) *overrideRecord { return s.snapshot()[key] }

// tick issues the next revision. Caller holds mu.
func (s *overrideStore) tick() uint64 {
	now := uint64(time.Now().UnixMilli())
	if now <= s.clock {
		now = s.clock + 1
	}
	s.clock = now
	return now
}

// publish persists next (when a path is set) and makes it current. Caller
// holds mu. With strict, a persist failure leaves the current state as is
// and returns errPersist; otherwise next is served anyway and the write is
// retried by retryLoop.
func (s *overrideStore) publish(next overrideMap, strict bool) error {
	if s.path != "" {
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if err = s.write(next); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			s.metrics.inc("flagrelay_override_write_errors_total")
			if strict {
				log.Printf("overrides: write failed, change refused: %v", err)
				return fmt.Errorf("%w: %v", errPersist, err)
			}
			s.persistErr = err.Error()
			log.Printf("overrides: write failed, serving the change anyway and retrying in the background: %v", err)
		} else {
			s.persistErr = ""
		}
	}
	s.cur.Store(&next)
	return nil
}

func (s *overrideStore) write(m overrideMap) error {
	b, err := json.Marshal(overrideFile{Format: 1, SavedAt: time.Now().UTC(), Overrides: m})
	if err != nil {
		return err
	}
	return writeFileDurable(filepath.Dir(s.path), filepath.Base(s.path), ".overrides-*.tmp", b, nil)
}

// retryLoop rewrites the overrides while the last write failed.
func (s *overrideStore) retryLoop(stop <-chan struct{}, every time.Duration) {
	if s.path == "" {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		s.mu.Lock()
		if s.persistErr != "" {
			if err := s.write(s.snapshot()); err == nil {
				s.persistErr = ""
				log.Printf("overrides: write recovered")
			}
		}
		s.mu.Unlock()
	}
}

func (s *overrideStore) persistError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistErr
}

func copyMap(m overrideMap) overrideMap {
	out := make(overrideMap, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

// matches reports whether the current record satisfies an If-Match revision:
// "*" = a live override exists, a number = the live override's revision
// ("0": there is no live override).
func matches(cur *overrideRecord, ifMatch string) bool {
	if ifMatch == "" {
		return true
	}
	if ifMatch == "*" {
		return cur.live()
	}
	return ifMatch == cur.etag()
}

// etag is the concurrency token of the current override: revision and
// replica (two replicas can issue the same revision), "0" for none.
func (o *overrideRecord) etag() string {
	if !o.live() {
		return "0"
	}
	return fmt.Sprintf("%d-%s", o.Rev, o.Origin)
}

// set creates or replaces the override of key. base returns the source flag
// version per environment; it runs under mu, and every commit of source data
// observes and publishes under mu too (observe), so the versions it reads are
// exactly the data served when the override takes effect.
func (s *overrideStore) set(key string, value json.RawMessage, setBy string, localOnly bool, ifMatch string, base func() (map[string]uint64, map[string]bool)) (*overrideRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.snapshot()
	if !matches(cur[key], ifMatch) {
		return cur[key], errPrecondition
	}
	if !cur[key].live() && countLive(cur) >= maxOverrides {
		return nil, errTooMany
	}
	if cur[key] == nil && len(cur) >= maxRecords {
		return nil, fmt.Errorf("%w: %d flag keys have had overrides (limit %d)", errTooMany, len(cur), maxRecords)
	}
	total := len(bytes.TrimSpace(value))
	for k, o := range cur {
		if o.live() && k != key {
			total += len(o.Value)
		}
	}
	if total > maxOverrideBytes {
		return nil, fmt.Errorf("%w: values would total %d bytes (limit %d)", errTooMany, total, maxOverrideBytes)
	}
	rec := &overrideRecord{
		Value: append(json.RawMessage(nil), bytes.TrimSpace(value)...), SetBy: setBy, SetAt: time.Now().UTC(),
		LocalOnly: localOnly, Rev: s.tick(), Origin: s.origin,
	}
	if base != nil {
		rec.Upstream, rec.Gone = base()
	}
	next := copyMap(cur)
	next[key] = rec
	if err := s.publish(next, true); err != nil {
		return nil, err
	}
	s.metrics.inc("flagrelay_override_changes_total", "source", "admin")
	log.Printf("override set: %s (%d bytes, local_only=%v, by %q, rev %d)", key, len(rec.Value), localOnly, setBy, rec.Rev)
	return rec, nil
}

// remove deletes the override of key (a tombstone). It reports whether a live
// override was removed.
func (s *overrideStore) remove(key, by, ifMatch string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.snapshot()
	if !matches(cur[key], ifMatch) {
		return false, errPrecondition
	}
	if !cur[key].live() {
		return false, nil
	}
	next := copyMap(cur)
	next[key] = &overrideRecord{SetBy: by, SetAt: time.Now().UTC(), Deleted: true, Reason: "admin", Rev: s.tick(), Origin: s.origin}
	if err := s.publish(next, true); err != nil {
		return false, err
	}
	s.metrics.inc("flagrelay_override_changes_total", "source", "admin")
	log.Printf("override removed: %s (by %q)", key, by)
	return true, nil
}

// merge adopts every record newer than ours (a peer's state); for the same
// revision it keeps the union of the recorded source versions. It returns the
// keys whose served state changed.
func (s *overrideStore) merge(in overrideMap) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.snapshot()
	var next overrideMap
	var changed []string
	dirty := false
	for k, o := range in {
		if o == nil || o.validate(k) != nil {
			continue
		}
		if o.Rev > s.clock {
			s.clock = o.Rev
		}
		mine := cur[k]
		switch {
		case o.newerThan(mine):
			if next == nil {
				next = copyMap(cur)
			}
			next[k] = o.clone()
			dirty = true
			if mine.live() || o.live() {
				changed = append(changed, k)
			}
		case mine != nil && mine.Rev == o.Rev && mine.Origin == o.Origin && !o.Deleted:
			// Same change: learn source versions we did not record.
			var add map[string]uint64
			for env, v := range o.Upstream {
				if _, ok := mine.Upstream[env]; !ok {
					if add == nil {
						add = map[string]uint64{}
					}
					add[env] = v
				}
			}
			if add != nil {
				if next == nil {
					next = copyMap(cur)
				}
				c := next[k].clone()
				if c.Upstream == nil {
					c.Upstream = map[string]uint64{}
				}
				for env, v := range add {
					c.Upstream[env] = v
					if o.Gone[env] {
						if c.Gone == nil {
							c.Gone = map[string]bool{}
						}
						c.Gone[env] = true
					}
				}
				next[k] = c
				dirty = true
			}
		}
	}
	if !dirty {
		return nil
	}
	_ = s.publish(next, false)
	if len(changed) > 0 {
		s.metrics.add("flagrelay_override_changes_total", float64(len(changed)), "source", "peer")
		sort.Strings(changed)
		log.Printf("overrides: %d change(s) adopted from a peer: %v", len(changed), changed)
	}
	return changed
}

// observe compares the source data of one environment with the overrides:
// it records the source version for overrides that have none for this
// environment yet, and drops (non local_only) overrides whose flag the source
// has changed since they were set. apply (optional) runs, with the dropped
// keys, before mu is released: it makes ds the served data, so no override can
// be set between this check and ds being served (set reads its baselines
// under mu), and "the source version is higher than the baseline" always
// means the source changed after the override. Returns the dropped keys.
func (s *overrideStore) observe(envID string, ds *dataSet, apply func(dropped []string)) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	dropped := s.observeLocked(envID, ds)
	if apply != nil {
		apply(dropped)
	}
	return dropped
}

func (s *overrideStore) observeLocked(envID string, ds *dataSet) []string {
	if ds == nil || countLive(s.snapshot()) == 0 {
		return nil
	}
	cur := s.snapshot()
	var next overrideMap
	var dropped []string
	for k, o := range cur {
		if !o.live() {
			continue
		}
		var v uint64
		live := false
		if it, ok := ds.Flags[k]; ok {
			v, live = it.Version, !it.Deleted
		}
		base, known := o.Upstream[envID]
		baseLive := !o.Gone[envID]                         // version 0 is a valid live version
		removed := known && v == base && baseLive && !live // deleted by a full snapshot, same version
		switch {
		case known && v == base && !removed:
			continue
		case (known && v > base || removed) && !o.LocalOnly:
			if next == nil {
				next = copyMap(cur)
			}
			next[k] = &overrideRecord{SetAt: time.Now().UTC(), Deleted: true, Reason: "upstream", Rev: s.tick(), Origin: s.origin}
			dropped = append(dropped, k)
			log.Printf("override dropped: %s changed at the source (version %d, override set against %d); serving the source value", k, v, base)
		case !known || v > base || removed:
			// First sight of this environment, or a local_only override: re-base.
			if next == nil {
				next = copyMap(cur)
			}
			c := o.clone()
			if c.Upstream == nil {
				c.Upstream = map[string]uint64{}
			}
			c.Upstream[envID] = v
			if !live {
				if c.Gone == nil {
					c.Gone = map[string]bool{}
				}
				c.Gone[envID] = true
			} else if c.Gone != nil {
				delete(c.Gone, envID)
			}
			next[k] = c
		}
	}
	if next == nil {
		return nil
	}
	_ = s.publish(next, false)
	if len(dropped) > 0 {
		s.metrics.add("flagrelay_override_changes_total", float64(len(dropped)), "source", "upstream")
		sort.Strings(dropped)
	}
	return dropped
}

// ── the view served to SDKs ──────────────────────────────────────────────────

// buildView returns ds with the overrides and version offsets applied, or ds
// itself when there is nothing to apply. ds may be nil (no source data): the
// view then holds only synthesized flags (nil if there are none).
func buildView(ds *dataSet, ovs overrideMap, bump map[string]uint64) *dataSet {
	if len(bump) == 0 && countLive(ovs) == 0 {
		return ds
	}
	var v *dataSet
	if ds == nil {
		v = newDataSet()
	} else {
		v = ds.clone()
	}
	keys := map[string]bool{}
	for k := range ovs {
		keys[k] = true
	}
	for k := range bump {
		keys[k] = true
	}
	for k := range keys {
		src, has := v.Flags[k]
		if ds == nil {
			has = false
		}
		if it, ok := viewItem(k, src, has, ovs[k], bump[k]); ok {
			v.Flags[k] = it
		} else {
			delete(v.Flags, k)
		}
	}
	if ds == nil && len(v.Flags) == 0 {
		return nil
	}
	return v
}

// viewItem is what SDKs are served for flag key: the source item (src, when
// has) or the override, with the version offset added.
func viewItem(key string, src item, has bool, o *overrideRecord, bump uint64) (item, bool) {
	var srcVer uint64
	if has {
		srcVer = src.Version
	}
	if o.live() {
		var base jsonObj
		if has && !src.Deleted {
			base, _ = asObj(src.Raw)
		}
		ver := srcVer + bump
		if ver == 0 {
			ver = 1
		}
		return overriddenFlag(key, base, o.Value, ver), true
	}
	if !has {
		if bump == 0 {
			return item{}, false
		}
		return tombstone(key, bump), true // a synthesized flag that was removed
	}
	if bump == 0 {
		return src, true
	}
	if src.Deleted {
		return tombstone(key, src.Version+bump), true
	}
	o2, ok := asObj(src.Raw)
	if !ok {
		return src, true
	}
	o2["version"] = json.RawMessage(fmt.Sprint(src.Version + bump))
	raw, _ := json.Marshal(o2)
	return item{Raw: raw, Version: src.Version + bump}, true
}

// overriddenFlag serves value to every context: the source flag (base, kept
// for its key, salt and any extra fields) with targeting replaced, or a
// synthesized flag when the source has none.
func overriddenFlag(key string, base jsonObj, value json.RawMessage, version uint64) item {
	f := jsonObj{}
	for k, v := range base {
		f[k] = v
	}
	mk := func(v interface{}) json.RawMessage { b, _ := json.Marshal(v); return b }
	f["key"] = mk(key)
	if _, ok := f["salt"]; !ok || !isStr(f["salt"]) {
		f["salt"] = mk(key)
	}
	f["version"] = json.RawMessage(fmt.Sprint(version))
	f["on"] = json.RawMessage("true")
	// Keep the source's variations (and so their indices: a prerequisite on
	// this flag names a variation index) and serve the one equal to value,
	// appending value when the source has no such variation.
	vars, _ := asArr(f["variations"])
	idx := -1
	for i, v := range vars {
		if jsonEqual(v, value) {
			idx = i
			break
		}
	}
	if idx < 0 {
		vars = append(vars, json.RawMessage(bytes.TrimSpace(value)))
		idx = len(vars) - 1
	}
	vb, _ := json.Marshal(vars)
	f["variations"] = vb
	f["offVariation"] = json.RawMessage(fmt.Sprint(idx))
	f["fallthrough"] = json.RawMessage(fmt.Sprintf(`{"variation":%d}`, idx))
	f["targets"] = json.RawMessage("[]")
	f["contextTargets"] = json.RawMessage("[]")
	f["rules"] = json.RawMessage("[]")
	f["prerequisites"] = json.RawMessage("[]")
	f["deleted"] = json.RawMessage("false")
	f["trackEvents"] = json.RawMessage("false")
	f["trackEventsFallthrough"] = json.RawMessage("false")
	if _, ok := f["clientSideAvailability"]; !ok {
		f["clientSideAvailability"] = json.RawMessage(`{"usingMobileKey":false,"usingEnvironmentId":false}`)
	}
	raw, _ := json.Marshal(f)
	return item{Raw: raw, Version: version}
}

// sameItem reports whether two view items are identical.
func sameItem(a item, aOK bool, b item, bOK bool) bool {
	if aOK != bOK {
		return false
	}
	return !aOK || (a.Version == b.Version && a.Deleted == b.Deleted && bytes.Equal(a.Raw, b.Raw))
}

// monotonic rebuilds the view after a change of keys so that every flag whose
// served form changed is served at a higher version than before (old is the
// view the SDKs hold). It raises bump as needed and returns the new view.
func monotonic(old, ds *dataSet, ovs overrideMap, bump map[string]uint64, keys []string) (*dataSet, map[string]uint64) {
	view := buildView(ds, ovs, bump)
	if old == nil {
		return view, bump
	}
	for pass := 0; pass < 2; pass++ {
		raised := false
		for _, k := range keys {
			o, oOK := old.Flags[k]
			var n item
			var nOK bool
			if view != nil {
				n, nOK = view.Flags[k]
			}
			if sameItem(o, oOK, n, nOK) || !oOK {
				continue
			}
			if nOK && n.Version > o.Version {
				continue
			}
			// The SDKs hold k at o.Version: serve the new form above it.
			cur := uint64(0)
			if nOK {
				cur = n.Version
			}
			if bump == nil {
				bump = map[string]uint64{}
			}
			bump[k] += o.Version - cur + 1
			raised = true
		}
		if !raised {
			break
		}
		view = buildView(ds, ovs, bump)
	}
	return view, bump
}

// itemMsgs renders the SDK event for one flag of a view: a patch, or a
// delete for a tombstone; nil when the view does not hold it.
func itemMsgs(view *dataSet, key string) [2][]byte {
	if view == nil {
		return [2][]byte{}
	}
	it, ok := view.Flags[key]
	if !ok {
		return [2][]byte{}
	}
	if it.Deleted {
		return deleteMsgs("flags", key, it.Version)
	}
	return patchMsgs("flags", key, it.Raw)
}

// concatMsgs joins per-protocol messages.
func concatMsgs(parts ...[2][]byte) [2][]byte {
	var out [2][]byte
	for _, p := range parts {
		for i := range out {
			out[i] = append(out[i], p[i]...)
		}
	}
	for i := range out {
		if len(out[i]) == 0 {
			out[i] = nil
		}
	}
	return out
}

// overlayKeys is must plus every flag the overlay may touch: every override
// record (tombstones included) and every version offset.
func overlayKeys(must []string, ovs overrideMap, bump map[string]uint64) []string {
	seen := make(map[string]bool, len(must)+len(ovs)+len(bump))
	out := make([]string, 0, len(must)+len(ovs)+len(bump))
	add := func(k string) {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for _, k := range must {
		add(k)
	}
	for k := range ovs {
		add(k)
	}
	for k := range bump {
		add(k)
	}
	return out
}

// diffMsgs renders the SDK events taking old to view for keys: one per flag
// whose served form changed, and always one for the keys in must.
func diffMsgs(old, view *dataSet, keys, must []string) [2][]byte {
	always := make(map[string]bool, len(must))
	for _, k := range must {
		always[k] = true
	}
	var parts [][2][]byte
	for _, k := range keys {
		var o, n item
		var oOK, nOK bool
		if old != nil {
			o, oOK = old.Flags[k]
		}
		if view != nil {
			n, nOK = view.Flags[k]
		}
		if always[k] || !sameItem(o, oOK, n, nOK) {
			parts = append(parts, itemMsgs(view, k))
		}
	}
	return concatMsgs(parts...)
}

// jsonEqual compares two JSON values semantically: key order, spacing, and
// number spelling (1.0 equals 1), with numbers compared exactly (no float64
// rounding: 9007199254740993 is not 9007199254740992).
func jsonEqual(a, b json.RawMessage) bool {
	var x, y interface{}
	da, db := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	da.UseNumber()
	db.UseNumber()
	if da.Decode(&x) != nil || db.Decode(&y) != nil {
		return false
	}
	return sameJSON(x, y)
}

func sameJSON(x, y interface{}) bool {
	switch xv := x.(type) {
	case json.Number:
		yv, ok := y.(json.Number)
		if !ok {
			return false
		}
		rx, okx := new(big.Rat).SetString(string(xv))
		ry, oky := new(big.Rat).SetString(string(yv))
		return okx && oky && rx.Cmp(ry) == 0
	case map[string]interface{}:
		yv, ok := y.(map[string]interface{})
		if !ok || len(xv) != len(yv) {
			return false
		}
		for k, v := range xv {
			w, ok := yv[k]
			if !ok || !sameJSON(v, w) {
				return false
			}
		}
		return true
	case []interface{}:
		yv, ok := y.([]interface{})
		if !ok || len(xv) != len(yv) {
			return false
		}
		for i := range xv {
			if !sameJSON(xv[i], yv[i]) {
				return false
			}
		}
		return true
	default:
		return x == y // string, bool, nil
	}
}
