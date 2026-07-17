package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"content-management-system/src/utils"

	_ "github.com/joho/godotenv/autoload"
	"gorm.io/gorm"
)

const ledgerTableDDL = `
CREATE TABLE IF NOT EXISTS cms_schema_migrations (
	version varchar(255) PRIMARY KEY,
	applied_at timestamp NOT NULL DEFAULT now(),
	checksum_sha256 varchar(64),
	execution_mode varchar(32) NOT NULL DEFAULT 'legacy'
)`

type migrationFile struct {
	Version  string
	Path     string
	Checksum string
}

var timestampedMigrationName = regexp.MustCompile(`^\d{14}_.+\.sql$`)

// These immutable historical files own their own transactions. They execute
// once outside the runner transaction; a ledger-write failure must be repaired
// with the explicit audited baseline command, never replayed automatically.
var legacyTransactionalMigrations = map[string]struct{}{
	"20260711030000_embedding_space_provenance.sql": {},
	"20260711040000_embedding_lifecycle.sql":        {},
	"20260711050000_embedding_campaigns.sql":        {},
	"20260712010000_ai_spend_governor.sql":          {},
	"20260712050000_ops_command_center.sql":         {},
}

func main() {
	var (
		applyAll = flag.Bool("all", false, "apply every migration not recorded in schema_migrations")
		status   = flag.Bool("status", false, "print migration ledger status without applying files")
		baseline = flag.String("baseline-through", "", "record timestamped migrations through this version as already applied without executing them")
		dir      = flag.String("dir", "migrations", "directory containing CMS SQL migrations")
	)
	flag.Parse()

	if *status && (*applyAll || *baseline != "" || flag.NArg() > 0) {
		log.Fatal("--status cannot be combined with --all, --baseline-through, or explicit migration files")
	}
	if *baseline != "" && (*applyAll || flag.NArg() > 0) {
		log.Fatal("--baseline-through cannot be combined with --all or explicit migration files")
	}
	if !*status && !*applyAll && *baseline == "" && flag.NArg() == 0 {
		log.Fatal("no migrations selected. Use --status, --all, --baseline-through, or pass explicit migration filenames")
	}

	db, err := utils.ConnectDB()
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}

	if err := ensureLedger(db); err != nil {
		log.Fatalf("ensure migration ledger: %v", err)
	}

	files, err := listMigrationFiles(*dir)
	if err != nil {
		log.Fatalf("list migrations: %v", err)
	}

	applied, err := appliedVersions(db)
	if err != nil {
		log.Fatalf("read migration ledger: %v", err)
	}
	if err := verifyAppliedChecksums(files, applied); err != nil {
		log.Fatalf("migration ledger checksum verification failed: %v", err)
	}

	if *status {
		printStatus(files, applied)
		return
	}
	if *baseline != "" {
		if err := baselineThrough(db, files, applied, *baseline); err != nil {
			log.Fatalf("baseline migrations: %v", err)
		}
		return
	}

	selected, err := selectMigrations(files, applied, *applyAll, flag.Args())
	if err != nil {
		log.Fatal(err)
	}
	if len(selected) == 0 {
		log.Println("No migrations to apply.")
		return
	}

	for _, file := range selected {
		if record, exists := applied[file.Version]; exists {
			returnFatalApplied(file, record)
		}
		if err := applyMigration(db, file); err != nil {
			log.Fatalf("apply %s: %v", file.Version, err)
		}
		log.Printf("Applied %s", file.Version)
	}
}

func verifyAppliedChecksums(files []migrationFile, applied map[string]migrationRecord) error {
	byVersion := make(map[string]migrationFile, len(files))
	for _, file := range files {
		byVersion[file.Version] = file
	}
	for version, record := range applied {
		file, exists := byVersion[version]
		if !exists {
			return fmt.Errorf("ledger contains migration %q missing from migrations/", version)
		}
		if record.Checksum != "" && record.Checksum != file.Checksum {
			return fmt.Errorf("migration %q checksum differs from its immutable ledger record", version)
		}
	}
	return nil
}

func returnFatalApplied(file migrationFile, record migrationRecord) {
	log.Fatalf("refusing to replay applied migration %s (recorded checksum %s)", file.Version, record.Checksum)
}

func ensureLedger(db *gorm.DB) error {
	return db.Exec(ledgerTableDDL).Error
}

func listMigrationFiles(dir string) ([]migrationFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]migrationFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !timestampedMigrationName.MatchString(entry.Name()) {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(string(contents)) == "" {
			return nil, fmt.Errorf("migration %q is empty", entry.Name())
		}
		sum := sha256.Sum256(contents)
		files = append(files, migrationFile{
			Version:  entry.Name(),
			Path:     filepath.Join(dir, entry.Name()),
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Version < files[j].Version
	})
	return files, nil
}

type migrationRecord struct {
	AppliedAt time.Time
	Checksum  string
}

func baselineThrough(db *gorm.DB, files []migrationFile, applied map[string]migrationRecord, through string) error {
	through = strings.TrimSuffix(through, ".sql")
	matched := false
	selected := make([]migrationFile, 0)
	for _, file := range files {
		version := strings.TrimSuffix(file.Version, ".sql")
		if version > through {
			break
		}
		selected = append(selected, file)
		if version == through {
			matched = true
		}
	}
	if !matched {
		return fmt.Errorf("baseline version %q not found in migrations/", through)
	}

	return db.Transaction(func(tx *gorm.DB) error {
		for _, file := range selected {
			if _, ok := applied[file.Version]; ok {
				continue
			}
			if err := tx.Exec(
				"INSERT INTO cms_schema_migrations (version, applied_at, checksum_sha256, execution_mode) VALUES (?, now(), ?, 'adopted') ON CONFLICT (version) DO NOTHING",
				file.Version, file.Checksum,
			).Error; err != nil {
				return err
			}
			log.Printf("Baselined %s", file.Version)
		}
		return nil
	})
}

func appliedVersions(db *gorm.DB) (map[string]migrationRecord, error) {
	rows, err := db.Raw("SELECT version, applied_at, COALESCE(checksum_sha256, '') FROM cms_schema_migrations").Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]migrationRecord)
	for rows.Next() {
		var version string
		var appliedAt time.Time
		var checksum string
		if err := rows.Scan(&version, &appliedAt, &checksum); err != nil {
			return nil, err
		}
		applied[version] = migrationRecord{AppliedAt: appliedAt, Checksum: checksum}
	}
	return applied, rows.Err()
}

func printStatus(files []migrationFile, applied map[string]migrationRecord) {
	for _, file := range files {
		if record, ok := applied[file.Version]; ok {
			if record.Checksum != "" && record.Checksum != file.Checksum {
				fmt.Printf("drifted  %s\n", file.Version)
				continue
			}
			fmt.Printf("applied  %s  %s\n", file.Version, record.AppliedAt.Format(time.RFC3339))
			continue
		}
		fmt.Printf("pending  %s\n", file.Version)
	}
}

func selectMigrations(files []migrationFile, applied map[string]migrationRecord, applyAll bool, requested []string) ([]migrationFile, error) {
	if applyAll {
		selected := make([]migrationFile, 0)
		for _, file := range files {
			if _, ok := applied[file.Version]; !ok {
				selected = append(selected, file)
			}
		}
		return selected, nil
	}

	byVersion := make(map[string]migrationFile, len(files))
	for _, file := range files {
		byVersion[file.Version] = file
		byVersion[strings.TrimSuffix(file.Version, ".sql")] = file
	}

	selected := make([]migrationFile, 0, len(requested))
	for _, name := range requested {
		file, ok := byVersion[name]
		if !ok {
			return nil, fmt.Errorf("migration %q not found in migrations/", name)
		}
		if _, alreadyApplied := applied[file.Version]; alreadyApplied {
			return nil, fmt.Errorf("migration %q is already applied and cannot be replayed", name)
		}
		selected = append(selected, file)
	}
	sort.Slice(selected, func(i, j int) bool {
		return selected[i].Version < selected[j].Version
	})
	return selected, nil
}

func applyMigration(db *gorm.DB, file migrationFile) error {
	sqlBytes, err := os.ReadFile(file.Path)
	if err != nil {
		return err
	}
	sql := strings.TrimSpace(string(sqlBytes))
	if sql == "" {
		return errors.New("migration file is empty")
	}
	_, legacy := legacyTransactionalMigrations[file.Version]
	if regexp.MustCompile(`(?im)^\s*(BEGIN|COMMIT|ROLLBACK)\s*;`).MatchString(sql) && !legacy {
		return errors.New("migration contains top-level transaction control; legacy migrations must be adopted through the audited baseline command")
	}
	if legacy {
		if err := db.Exec(sql).Error; err != nil {
			return err
		}
		return db.Exec("INSERT INTO cms_schema_migrations (version, applied_at, checksum_sha256, execution_mode) VALUES (?, now(), ?, 'legacy')", file.Version, file.Checksum).Error
	}

	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(sql).Error; err != nil {
			return err
		}
		return tx.Exec(
			"INSERT INTO cms_schema_migrations (version, applied_at, checksum_sha256, execution_mode) VALUES (?, now(), ?, 'runner')",
			file.Version, file.Checksum,
		).Error
	})
}
