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

func TestListMigrationFilesRejectsEmptyFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "20260716000000_empty.sql"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := listMigrationFiles(dir); err == nil {
		t.Fatal("empty migration was accepted")
	}
}

func TestSelectMigrationsRejectsExplicitReplay(t *testing.T) {
	file := migrationFile{Version: "20260716000000_test.sql"}
	if _, err := selectMigrations([]migrationFile{file}, map[string]migrationRecord{file.Version: {}}, false, []string{file.Version}); err == nil {
		t.Fatal("applied migration replay was accepted")
	}
}

func TestVerifyAppliedChecksumsRejectsDrift(t *testing.T) {
	file := migrationFile{Version: "20260716000000_test.sql", Checksum: "current"}
	if err := verifyAppliedChecksums([]migrationFile{file}, map[string]migrationRecord{file.Version: {Checksum: "recorded"}}); err == nil {
		t.Fatal("edited applied migration was accepted")
	}
}

func TestRequireDestructiveApproval(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "20260719020000_drop_table.sql")
	if err := os.WriteFile(path, []byte("DROP TABLE legacy_event_topics;"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := []migrationFile{{Version: filepath.Base(path), Path: path}}
	if err := requireDestructiveApproval(files, false); err == nil {
		t.Fatal("destructive migration was allowed without approval")
	}
	if err := requireDestructiveApproval(files, true); err != nil {
		t.Fatalf("approved destructive migration was rejected: %v", err)
	}
}
