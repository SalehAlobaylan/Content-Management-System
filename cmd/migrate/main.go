package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
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
		applyAll         = flag.Bool("all", false, "apply every migration not recorded in schema_migrations")
		status           = flag.Bool("status", false, "print migration ledger status without applying files")
		check            = flag.Bool("check", false, "validate every pending migration without applying it")
		verify           = flag.Bool("verify", false, "verify checksums and require no pending migration")
		baseline         = flag.String("baseline-through", "", "record timestamped migrations through this version as already applied without executing them")
		allowDestructive = flag.Bool("allow-destructive", false, "allow migrations containing destructive SQL such as DROP TABLE, DROP COLUMN, TRUNCATE, or DELETE FROM")
		bootstrapEmpty   = flag.Bool("bootstrap-empty", false, "allow reviewed historical large-table migrations only on an explicitly acknowledged empty local disposable database")
		dir              = flag.String("dir", "migrations", "directory containing CMS SQL migrations")
	)
	flag.Parse()

	if (*status && *check) || (*status && *verify) || (*check && *verify) {
		log.Fatal("--status, --check, and --verify cannot be combined")
	}
	if (*status || *check || *verify) && (*applyAll || *baseline != "" || *allowDestructive || flag.NArg() > 0) {
		log.Fatal("--status, --check, and --verify cannot be combined with --all, --baseline-through, --allow-destructive, or explicit migration files")
	}
	if *baseline != "" && (*applyAll || flag.NArg() > 0) {
		log.Fatal("--baseline-through cannot be combined with --all or explicit migration files")
	}
	if !*status && !*check && !*verify && !*applyAll && *baseline == "" && flag.NArg() == 0 {
		log.Fatal("no migrations selected. Use --status, --check, --verify, --all, --baseline-through, or pass explicit migration filenames")
	}
	if *bootstrapEmpty && (!*applyAll || *status || *check || *baseline != "") {
		log.Fatal("--bootstrap-empty is only valid with --all and an apply operation")
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
	if *verify {
		pending, err := selectMigrations(files, applied, true, nil)
		if err != nil {
			log.Fatal(err)
		}
		if len(pending) != 0 {
			log.Fatalf("migration ledger is incomplete: %d pending migration(s)", len(pending))
		}
		log.Printf("Migration ledger verification passed for %d canonical migration(s).", len(files))
		return
	}
	if *check {
		pending, err := selectMigrations(files, applied, true, nil)
		if err != nil {
			log.Fatal(err)
		}
		if err := requireLargeTableMigrationSafety(pending); err != nil {
			log.Fatalf("migration safety check failed: %v", err)
		}
		_, blockedAt, err := migrationsBeforeDestructiveBoundary(pending)
		if err != nil {
			log.Fatal(err)
		}
		if blockedAt != "" {
			log.Printf("Destructive boundary: %s (normal apply will stop before it)", blockedAt)
		}
		log.Printf("Migration safety check passed for %d pending migration(s).", len(pending))
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
	blockedAt := ""
	if *applyAll && !*allowDestructive {
		selected, blockedAt, err = migrationsBeforeDestructiveBoundary(selected)
		if err != nil {
			log.Fatal(err)
		}
	}
	if len(selected) == 0 {
		if blockedAt != "" {
			log.Fatalf("next pending migration %s is destructive; review it and re-run with --allow-destructive when its own readiness guards are satisfied", blockedAt)
		}
		log.Println("No migrations to apply.")
		return
	}
	if *bootstrapEmpty {
		if err := requireEmptyDisposableBootstrap(db); err != nil {
			log.Fatal(err)
		}
	}
	if err := requireLargeTableMigrationSafety(selected); err != nil && !*bootstrapEmpty {
		log.Fatalf("migration safety check failed: %v", err)
	}
	if err := requireDestructiveApproval(selected, *allowDestructive); err != nil {
		log.Fatal(err)
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
	if blockedAt != "" {
		log.Printf("Stopped before destructive migration %s; safe preceding migrations were applied. Re-run with --allow-destructive only after reviewing that migration and satisfying its readiness guards.", blockedAt)
	}
}

const disposableBootstrapMarker = "I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE"

// requireEmptyDisposableBootstrap is deliberately stricter than the normal
// migration guard. It exists only to replay historical migrations into a new
// local fixture whose old files predate the large-table safety marker. It must
// never be usable against a managed provider or a non-test database.
func requireEmptyDisposableBootstrap(db *gorm.DB) error {
	if os.Getenv("CMS_MIGRATION_BOOTSTRAP_DISPOSABLE") != disposableBootstrapMarker {
		return fmt.Errorf("--bootstrap-empty requires CMS_MIGRATION_BOOTSTRAP_DISPOSABLE=%s", disposableBootstrapMarker)
	}
	raw := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return fmt.Errorf("--bootstrap-empty requires a PostgreSQL DATABASE_URL")
	}
	host := strings.ToLower(u.Hostname())
	if host != "localhost" && host != "127.0.0.1" && host != "::1" {
		return fmt.Errorf("--bootstrap-empty is restricted to localhost PostgreSQL targets")
	}
	database := strings.TrimPrefix(u.Path, "/")
	if !strings.HasPrefix(database, "wahb_cms_test_") {
		return fmt.Errorf("--bootstrap-empty requires a database named wahb_cms_test_<suffix>")
	}
	var tables []string
	if err := db.Raw("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename <> 'cms_schema_migrations' ORDER BY tablename").Scan(&tables).Error; err != nil {
		return fmt.Errorf("inspect bootstrap schema: %w", err)
	}
	if len(tables) > 0 {
		return fmt.Errorf("--bootstrap-empty refused: database already contains public tables (%s)", strings.Join(tables, ", "))
	}
	for _, table := range []string{"content_items", "stories"} {
		var exists bool
		if err := db.Raw("SELECT to_regclass(?) IS NOT NULL", "public."+table).Scan(&exists).Error; err != nil {
			return fmt.Errorf("inspect bootstrap table %s: %w", table, err)
		}
		if !exists {
			continue
		}
		var count int64
		if err := db.Table(table).Count(&count).Error; err != nil {
			return fmt.Errorf("count bootstrap table %s: %w", table, err)
		}
		if count != 0 {
			return fmt.Errorf("--bootstrap-empty refused: %s contains %d rows", table, count)
		}
	}
	return nil
}

var destructiveSQL = regexp.MustCompile(`(?im)^\s*(DROP\s+TABLE|ALTER\s+TABLE\b.*\bDROP\s+COLUMN|TRUNCATE\b|DELETE\s+FROM\b)`)
var largeTableUpdateSQL = regexp.MustCompile(`(?im)^\s*UPDATE\s+(?:ONLY\s+)?(?:(?:"?public"?)\.)?"?(content_items|stories)"?\b`)
var largeTableSafetyMarker = regexp.MustCompile(`(?im)^\s*--\s*wahb:large-table-backfill:\s*(bounded|operator-maintenance)\s*$`)

// content_items and stories are live, high-volume tables. Rewriting every row
// in either table can exceed hosted Postgres statement limits and amplify WAL
// and storage. A pending migration that updates one of these tables must carry
// an explicit safety classification after its predicate/batching strategy has
// been reviewed. Applied historical files are checksum-verified but not
// retroactively linted.
func requireLargeTableMigrationSafety(files []migrationFile) error {
	for _, file := range files {
		sql, err := os.ReadFile(file.Path)
		if err != nil {
			return err
		}
		matches := largeTableUpdateSQL.FindSubmatch(sql)
		if len(matches) == 0 || largeTableSafetyMarker.Match(sql) {
			continue
		}
		return fmt.Errorf(
			"%s updates large live table %s without a reviewed safety classification; avoid whole-table rewrites, use compatibility semantics or bounded indexed batches, then add exactly one of \"-- wahb:large-table-backfill: bounded\" or \"-- wahb:large-table-backfill: operator-maintenance\"",
			file.Version,
			string(matches[1]),
		)
	}
	return nil
}

func migrationIsDestructive(file migrationFile) (bool, error) {
	sql, err := os.ReadFile(file.Path)
	if err != nil {
		return false, err
	}
	return destructiveSQL.Match(sql), nil
}

// Normal --all execution advances only through the safe ordered prefix. It
// never skips over a destructive migration because later migrations may depend
// on the destructive schema transition. This lets operators use the wrapper
// script for ordinary progress without granting premature destructive approval.
func migrationsBeforeDestructiveBoundary(files []migrationFile) ([]migrationFile, string, error) {
	for i, file := range files {
		destructive, err := migrationIsDestructive(file)
		if err != nil {
			return nil, "", err
		}
		if destructive {
			return files[:i], file.Version, nil
		}
	}
	return files, "", nil
}

func requireDestructiveApproval(files []migrationFile, allowed bool) error {
	destructive := make([]string, 0)
	for _, file := range files {
		isDestructive, err := migrationIsDestructive(file)
		if err != nil {
			return err
		}
		if isDestructive {
			destructive = append(destructive, file.Version)
		}
	}
	if len(destructive) > 0 && !allowed {
		return fmt.Errorf("refusing destructive migration(s): %s; review them and re-run with --allow-destructive", strings.Join(destructive, ", "))
	}
	return nil
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
	if err := db.Exec(ledgerTableDDL).Error; err != nil {
		return err
	}
	return db.Exec(`
ALTER TABLE cms_schema_migrations
    ADD COLUMN IF NOT EXISTS checksum_sha256 varchar(64),
    ADD COLUMN IF NOT EXISTS execution_mode varchar(32) NOT NULL DEFAULT 'legacy'
`).Error
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
			return migrationExecutionError(err)
		}
		return db.Exec("INSERT INTO cms_schema_migrations (version, applied_at, checksum_sha256, execution_mode) VALUES (?, now(), ?, 'legacy')", file.Version, file.Checksum).Error
	}

	return db.Transaction(func(tx *gorm.DB) error {
		// Fail fast on DDL lock contention instead of consuming the provider's
		// full statement timeout while waiting for a busy live table.
		if err := tx.Exec("SET LOCAL lock_timeout = '10s'").Error; err != nil {
			return err
		}
		if err := tx.Exec(sql).Error; err != nil {
			return migrationExecutionError(err)
		}
		return tx.Exec(
			"INSERT INTO cms_schema_migrations (version, applied_at, checksum_sha256, execution_mode) VALUES (?, now(), ?, 'runner')",
			file.Version, file.Checksum,
		).Error
	})
}

func migrationExecutionError(err error) error {
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "statement timeout") || strings.Contains(message, "sqlstate 57014") {
		return fmt.Errorf("migration statement timed out; do not rewrite a large live table in one statement—use compatibility semantics or bounded indexed batches: %w", err)
	}
	if strings.Contains(message, "lock timeout") || strings.Contains(message, "sqlstate 55p03") {
		return fmt.Errorf("migration could not acquire a database lock within 10 seconds; retry during lower traffic or use an online-safe schema transition: %w", err)
	}
	return err
}
