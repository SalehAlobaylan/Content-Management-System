package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestListMigrationFilesOnlyIncludesTimestampedSQL(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"20260712000000_example.sql",
		"init.sql",
		"20260712000001_notes.txt",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("SELECT 1;"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	files, err := listMigrationFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Version != "20260712000000_example.sql" {
		t.Fatalf("unexpected migration files: %#v", files)
	}
}
