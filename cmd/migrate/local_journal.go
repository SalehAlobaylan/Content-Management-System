package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Historical self-owned transactions cannot make the ledger insert atomic.
// Persist intent before SQL so an interrupted application is never replayed
// just because its ledger entry is absent.
func journalLegacy(file migrationFile, phase string) error {
	dir := os.Getenv("WAHB_PREPARATION_LEGACY_JOURNAL")
	if dir == "" {
		return fmt.Errorf("managed historical bootstrap requires its durable legacy journal")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	path := filepath.Join(dir, file.Version+"-"+os.Getenv("WAHB_PREPARATION_OPERATION")+"-"+phase+".json")
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	b, e := json.Marshal(map[string]any{"file": file.Version, "checksum": file.Checksum, "phase": phase, "at": time.Now().UTC()})
	if e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
