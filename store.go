package main

// store.go: the in-memory flag data set for one SDK key, built from an
// upstream LaunchDarkly-protocol stream (put / patch / delete), with the same
// version rules the LaunchDarkly SDKs apply: a patch or delete only lands when
// its version is higher than the one held.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// item is one flag or segment exactly as upstream sent it.
type item struct {
	Raw     json.RawMessage
	Version uint64
	Deleted bool
}

// dataSet is {flags, segments}, keyed by flag/segment key.
type dataSet struct {
	Flags    map[string]item
	Segments map[string]item
	// invalid lists items ("flags/<key>", "segments/<key>") that were present
	// but malformed in the message this set was parsed from. They are left
	// out, and whoever applies the set keeps its previous good copy of them,
	// so one bad item can neither erase good data nor block every other
	// change (such as the PgDog kill switch).
	invalid []string
}

func newDataSet() *dataSet {
	return &dataSet{Flags: map[string]item{}, Segments: map[string]item{}}
}

// parseItem validates one flag ("flags") or segment ("segments"): a JSON
// object whose exact (case-sensitive) "version" is a u64 (flags may omit it:
// the SDKs then use 0; segments must have it) and, unless "deleted" is true,
// the fields the SDKs require (validate.go). The raw bytes must also be
// something serde_json accepts (scanJSON). Unknown fields are kept. Anything
// else is rejected, so a malformed message never replaces good data.
func parseItem(kind string, raw json.RawMessage) (item, error) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || t[0] != '{' {
		return item{}, fmt.Errorf("item is not a JSON object")
	}
	if err := scanJSON(t); err != nil {
		return item{}, err
	}
	o, ok := asObj(t)
	if !ok {
		return item{}, fmt.Errorf("item is not a JSON object")
	}
	var version uint64
	if v, ok := o["version"]; ok {
		n, err := parseU64(v)
		if err != nil {
			return item{}, fmt.Errorf("item has no valid version")
		}
		version = n
	} else if kind == "segments" {
		return item{}, fmt.Errorf("segment has no version")
	}
	deleted := false
	if d, ok := o["deleted"]; ok && !isNull(d) {
		if !isBool(d) {
			return item{}, fmt.Errorf("\"deleted\" has the wrong type")
		}
		deleted = string(bytes.TrimSpace(d)) == "true"
	}
	if !deleted {
		var err error
		if kind == "segments" {
			err = validSegment(o)
		} else {
			err = validFlag(o)
		}
		if err != nil {
			return item{}, err
		}
	}
	cp := make(json.RawMessage, len(t))
	copy(cp, t)
	return item{Raw: cp, Version: version, Deleted: deleted}, nil
}

// wireDataSet is the JSON shape on the wire: {"flags":{...},"segments":{...}}.
type wireDataSet struct {
	Flags    map[string]json.RawMessage `json:"flags"`
	Segments map[string]json.RawMessage `json:"segments"`
}

// parseDataSet accepts {"flags":{..},"segments":{..}}. "flags" must be
// present and an object (an empty object is a valid empty environment);
// "segments" may be absent. Anything else is rejected before it can touch
// the last good data.
func parseDataSet(b []byte) (*dataSet, error) {
	t := bytes.TrimSpace(b)
	if len(t) == 0 || t[0] != '{' {
		return nil, fmt.Errorf("data set is not a JSON object")
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(t, &probe); err != nil {
		return nil, err
	}
	fl, ok := probe["flags"]
	if !ok || len(bytes.TrimSpace(fl)) == 0 || bytes.TrimSpace(fl)[0] != '{' {
		return nil, fmt.Errorf("data set has no flags object")
	}
	if sg, ok := probe["segments"]; ok {
		if st := bytes.TrimSpace(sg); len(st) == 0 || st[0] != '{' {
			return nil, fmt.Errorf("segments is not an object")
		}
	}
	var w wireDataSet
	if err := json.Unmarshal(t, &w); err != nil {
		return nil, err
	}
	ds := newDataSet()
	for k, raw := range w.Flags {
		it, err := parseItem("flags", raw)
		if err != nil {
			ds.invalid = append(ds.invalid, "flags/"+k)
			continue
		}
		ds.Flags[k] = it
	}
	for k, raw := range w.Segments {
		it, err := parseItem("segments", raw)
		if err != nil {
			ds.invalid = append(ds.invalid, "segments/"+k)
			continue
		}
		ds.Segments[k] = it
	}
	return ds, nil
}

// marshal renders the data set as {"flags":..,"segments":..}. Deleted
// tombstones are kept out of the wire form (they only guard versions here).
// Keys are sorted so equal data sets give identical bytes (stable ETags).
func (ds *dataSet) marshal() []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"flags":`)
	writeItems(&buf, ds.Flags)
	buf.WriteString(`,"segments":`)
	writeItems(&buf, ds.Segments)
	buf.WriteString(`}`)
	return buf.Bytes()
}

func (ds *dataSet) marshalFlags() []byte {
	var buf bytes.Buffer
	writeItems(&buf, ds.Flags)
	return buf.Bytes()
}

func (ds *dataSet) marshalSegments() []byte {
	var buf bytes.Buffer
	writeItems(&buf, ds.Segments)
	return buf.Bytes()
}

func writeItems(buf *bytes.Buffer, m map[string]item) {
	keys := make([]string, 0, len(m))
	for k, it := range m {
		if !it.Deleted {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(m[k].Raw)
	}
	buf.WriteByte('}')
}

// marshalWithTombstones renders every item including deleted markers; used
// for the on-disk cache and the peer snapshot, so version guards survive a
// restart and peer merges cannot resurrect a deleted flag.
func (ds *dataSet) marshalWithTombstones() []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"flags":`)
	writeAllItems(&buf, ds.Flags)
	buf.WriteString(`,"segments":`)
	writeAllItems(&buf, ds.Segments)
	buf.WriteString(`}`)
	return buf.Bytes()
}

func writeAllItems(buf *bytes.Buffer, m map[string]item) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(m[k].Raw)
	}
	buf.WriteByte('}')
}

// replaceFrom applies an authoritative full data set (an upstream put or
// poll): it becomes the data, and every item it no longer contains becomes a
// tombstone at the version we held, so a peer that still has the old item at
// that version cannot bring it back.
func (ds *dataSet) replaceFrom(next *dataSet) *dataSet {
	out := next.clone()
	// Items that arrived malformed keep the copy we already had.
	for _, ref := range next.invalid {
		kind, key, _ := strings.Cut(ref, "/")
		if it, ok := ds.collection(kind)[key]; ok {
			out.collection(kind)[key] = it
		}
	}
	for _, kind := range []string{"flags", "segments"} {
		cur, nxt := ds.collection(kind), out.collection(kind)
		for k, it := range cur {
			if _, ok := nxt[k]; !ok {
				if it.Deleted {
					nxt[k] = it
				} else {
					nxt[k] = tombstone(k, it.Version)
				}
			}
		}
	}
	return out
}

// equal reports whether two data sets hold the same live items.
func (ds *dataSet) equal(o *dataSet) bool {
	if ds == nil || o == nil {
		return ds == o
	}
	return bytes.Equal(ds.marshal(), o.marshal())
}

func (ds *dataSet) clone() *dataSet {
	c := newDataSet()
	for k, v := range ds.Flags {
		c.Flags[k] = v
	}
	for k, v := range ds.Segments {
		c.Segments[k] = v
	}
	return c
}

// kindAndKey splits an LD stream path such as "/flags/my-flag".
func kindAndKey(path string) (kind, key string, ok bool) {
	switch {
	case strings.HasPrefix(path, "/flags/"):
		return "flags", strings.TrimPrefix(path, "/flags/"), true
	case strings.HasPrefix(path, "/segments/"):
		return "segments", strings.TrimPrefix(path, "/segments/"), true
	}
	return "", "", false
}

func (ds *dataSet) collection(kind string) map[string]item {
	if kind == "flags" {
		return ds.Flags
	}
	return ds.Segments
}

// upsert applies a patch or delete when it is newer than what is held.
// It returns true when the data changed.
func (ds *dataSet) upsert(kind, key string, it item) bool {
	coll := ds.collection(kind)
	if cur, ok := coll[key]; ok && cur.Version >= it.Version {
		return false // LD rule: a patch/delete must have a strictly higher version
	}
	coll[key] = it
	return true
}

// tombstone builds the deleted marker the LD SDKs keep for a delete event.
func tombstone(key string, version uint64) item {
	raw, _ := json.Marshal(map[string]interface{}{"key": key, "version": version, "deleted": true})
	return item{Raw: raw, Version: version, Deleted: true}
}
