package main

// persist.go: the last good data set per SDK key on disk, so a relay that
// restarts while upstream is down still serves. Files are named by
// sha256(key); no key material is written.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

type diskEntry struct {
	data        *dataSet
	savedAt     time.Time
	confirmedAt time.Time // when the upstream chain last confirmed this copy
	dirty       bool      // adopted from a peer at start but not yet written
}

// maxDataSet bounds one data set read from upstream, a peer or disk.
const maxDataSet = 64 << 20

type cacheFile struct {
	Format      int             `json:"format"`
	SavedAt     time.Time       `json:"saved_at"`
	Origin      string          `json:"origin"` // where this copy came from
	ConfirmedAt time.Time       `json:"confirmed_at"`
	Data        json.RawMessage `json:"data"` // includes deleted tombstones
}

func keyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func etagOf(payload []byte) string {
	sum := sha256.Sum256(payload)
	return `"` + hex.EncodeToString(sum[:12]) + `"`
}

func validHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// loadCache reads every <hash>.json in dir. Unreadable files are skipped and
// logged; they are overwritten on the next good upstream put.
func loadCache(dir string) map[string]*diskEntry {
	out := map[string]*diskEntry{}
	if dir == "" {
		return out
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("cache dir %s: %v", dir, err)
		}
		return out
	}
	for _, de := range entries {
		name := de.Name()
		hash := strings.TrimSuffix(name, ".json")
		if de.IsDir() || !strings.HasSuffix(name, ".json") || !validHash(hash) {
			continue
		}
		if fi, err := de.Info(); err != nil || fi.Size() > maxDataSet {
			log.Printf("cache %s: unreadable or too large, ignored", name)
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			log.Printf("cache %s: %v", name, err)
			continue
		}
		var cf cacheFile
		if err := json.Unmarshal(b, &cf); err != nil || cf.Format != 1 {
			log.Printf("cache %s: unreadable, ignored", name)
			continue
		}
		ds, err := parseDataSet(cf.Data)
		if err != nil {
			log.Printf("cache %s: bad data set, ignored: %v", name, err)
			continue
		}
		out[hash] = &diskEntry{data: ds, savedAt: cf.SavedAt, confirmedAt: cf.ConfirmedAt}
	}
	return out
}

// testWriteHook, when set (tests only), runs inside writeCacheFile before
// the rename, to simulate a slow disk.
var testWriteHook atomic.Pointer[func(hash string)]

// writeCacheFile writes atomically: temp file, fsync, rename, fsync the dir.
func writeCacheFile(dir, hash string, payload []byte, origin string, confirmedAt time.Time) error {
	if !validHash(hash) {
		return fmt.Errorf("bad hash")
	}
	b, err := json.Marshal(cacheFile{Format: 1, SavedAt: time.Now().UTC(), Origin: origin, ConfirmedAt: confirmedAt.UTC(), Data: payload})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+hash[:8]+"-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return err
	}
	if h := testWriteHook.Load(); h != nil {
		(*h)(hash)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, hash+".json")); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync() // the rename itself must survive a power loss
}

// removeCacheFile deletes the cached copy for a hash (revocation).
func removeCacheFile(dir, hash string) {
	if dir == "" || !validHash(hash) {
		return
	}
	if err := os.Remove(filepath.Join(dir, hash+".json")); err == nil {
		log.Printf("cache: removed %s (denied key)", hash[:8])
	}
}

// seedCache writes a /sdk/latest-all document as the cached copy for key, for
// an appliance that must start offline before it ever reached upstream. The
// copy is marked as never confirmed, so any copy from upstream or a peer wins.
func seedCache(dir, key string, latestAll []byte) error {
	ds, err := parseDataSet(latestAll)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return writeCacheFile(dir, keyHash(key), ds.marshalWithTombstones(), "seed", time.Time{})
}
