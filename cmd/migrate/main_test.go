package main

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
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

func TestActivateEmptyBootstrapContentStagesPromotesBothLanes(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT count\(\*\) FROM "content_items"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT count\(\*\) FROM "content_sources"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`UPDATE content_stage_cutovers`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), "news").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE content_stage_cutovers`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), "pods").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := activateEmptyBootstrapContentStages(db); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestActivateEmptyBootstrapContentStagesRefusesPopulatedDatabase(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT count\(\*\) FROM "content_items"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectRollback()

	if err := activateEmptyBootstrapContentStages(db); err == nil || !strings.Contains(err.Error(), "content_items contains 1 row") {
		t.Fatalf("expected populated database refusal, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
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

func TestCanonicalMigrationsUseRunnerTransactionContract(t *testing.T) {
	files, err := listMigrationFiles(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	topLevelTransaction := regexp.MustCompile(`(?im)^\s*(BEGIN|COMMIT|ROLLBACK)\s*;`)
	for _, file := range files {
		sql, err := os.ReadFile(file.Path)
		if err != nil {
			t.Fatal(err)
		}
		_, historicalException := legacyTransactionalMigrations[file.Version]
		if topLevelTransaction.Match(sql) && !historicalException {
			t.Fatalf("%s contains top-level transaction control but is not an audited historical exception", file.Version)
		}
	}
}

func TestSourceTenantScopePrecedesSourceRunReliability(t *testing.T) {
	files, err := listMigrationFiles(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	positions := map[string]int{}
	for index, file := range files {
		positions[file.Version] = index
	}
	tenantScope := "20260808990000_content_sources_tenant_scope.sql"
	reliability := "20260809000000_source_run_reliability.sql"
	if _, ok := positions[tenantScope]; !ok {
		t.Fatalf("missing prerequisite migration %s", tenantScope)
	}
	if _, ok := positions[reliability]; !ok {
		t.Fatalf("missing dependent migration %s", reliability)
	}
	if positions[tenantScope] >= positions[reliability] {
		t.Fatalf("%s must precede %s", tenantScope, reliability)
	}
}

func TestContentIdempotencyPrecedesContentStageExecution(t *testing.T) {
	files, err := listMigrationFiles(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	positions := map[string]int{}
	for index, file := range files {
		positions[file.Version] = index
	}
	prerequisite := "20260817190000_content_items_idempotency.sql"
	dependent := "20260818100000_content_stage_execution.sql"
	if _, ok := positions[prerequisite]; !ok {
		t.Fatalf("missing prerequisite migration %s", prerequisite)
	}
	if _, ok := positions[dependent]; !ok {
		t.Fatalf("missing dependent migration %s", dependent)
	}
	if positions[prerequisite] >= positions[dependent] {
		t.Fatalf("%s must precede %s", prerequisite, dependent)
	}
}

func TestQualityProfilesPrecedeAudioFirstDelivery(t *testing.T) {
	files, err := listMigrationFiles(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	positions := map[string]int{}
	for index, file := range files {
		positions[file.Version] = index
	}
	prerequisite := "20260822180000_quality_profiles_foundation.sql"
	dependent := "20260823100000_audio_first_delivery_contract.sql"
	if _, ok := positions[prerequisite]; !ok {
		t.Fatalf("missing prerequisite migration %s", prerequisite)
	}
	if _, ok := positions[dependent]; !ok {
		t.Fatalf("missing dependent migration %s", dependent)
	}
	if positions[prerequisite] >= positions[dependent] {
		t.Fatalf("%s must precede %s", prerequisite, dependent)
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

func TestInspectAppliedChecksumDriftRejectsUnsupportedMigration(t *testing.T) {
	_, _, _, err := inspectAppliedChecksumDrift(nil, nil, nil, "20260908130000_other.sql")
	if err == nil || !strings.Contains(err.Error(), atomizationReliabilityMigration) {
		t.Fatalf("unsupported checksum repair target was accepted: %v", err)
	}
}

func TestRepairAppliedChecksumDriftRequiresExplicitAcknowledgement(t *testing.T) {
	file := migrationFile{Version: atomizationReliabilityMigration}
	err := repairAppliedChecksumDrift(nil, file, strings.Repeat("a", 64), "original migration unavailable", "")
	if err == nil || !strings.Contains(err.Error(), "--acknowledge-checksum-repair") {
		t.Fatalf("checksum repair without acknowledgement was accepted: %v", err)
	}
}

func TestRepairAtomizationReliabilitySchemaAppliesOnlySupportedMissingObjects(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(`ALTER TABLE transcription_segment_units`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DO \$\$`).WillReturnResult(sqlmock.NewResult(0, 0))

	issues := []string{
		"transcription_segment_units.effect_started_at",
		"constraint nested_manifest_fence_shape",
	}
	if err := repairAtomizationReliabilitySchema(db, issues); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRepairAtomizationReliabilitySchemaRefusesUnknownPostcondition(t *testing.T) {
	err := repairAtomizationReliabilitySchema(nil, []string{"unexpected.object"})
	if err == nil || !strings.Contains(err.Error(), "unsupported missing postcondition") {
		t.Fatalf("unknown postcondition was accepted: %v", err)
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

func TestMigrationsBeforeDestructiveBoundaryKeepsSafeOrderedPrefix(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"20260728030000_safe.sql",
		"20260729010000_destructive.sql",
		"20260729020000_later_safe.sql",
	}
	sql := []string{"CREATE TABLE safe_one(id bigint);", "ALTER TABLE content_items DROP COLUMN embedding_sparse;", "CREATE TABLE safe_two(id bigint);"}
	files := make([]migrationFile, 0, len(names))
	for i, name := range names {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(sql[i]), 0o600); err != nil {
			t.Fatal(err)
		}
		files = append(files, migrationFile{Version: name, Path: path})
	}
	selected, blockedAt, err := migrationsBeforeDestructiveBoundary(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Version != names[0] {
		t.Fatalf("safe prefix = %#v, want only %s", selected, names[0])
	}
	if blockedAt != names[1] {
		t.Fatalf("blocked at %q, want %q", blockedAt, names[1])
	}
}

func TestMigrationsBeforeDestructiveBoundaryBlocksImmediately(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "20260729010000_destructive.sql")
	if err := os.WriteFile(path, []byte("DELETE FROM legacy_rows;"), 0o600); err != nil {
		t.Fatal(err)
	}
	selected, blockedAt, err := migrationsBeforeDestructiveBoundary([]migrationFile{{Version: filepath.Base(path), Path: path}})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 0 || blockedAt != filepath.Base(path) {
		t.Fatalf("selected=%#v blockedAt=%q", selected, blockedAt)
	}
}

func TestRequireLargeTableMigrationSafetyRejectsUnclassifiedUpdate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "20260730000000_risky_backfill.sql")
	if err := os.WriteFile(path, []byte("UPDATE public.content_items SET retention_state = 'full';"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := requireLargeTableMigrationSafety([]migrationFile{{Version: filepath.Base(path), Path: path}})
	if err == nil {
		t.Fatal("unclassified large-table update was accepted")
	}
	if !strings.Contains(err.Error(), "content_items") {
		t.Fatalf("error does not identify the large table: %v", err)
	}
}

func TestRequireLargeTableMigrationSafetyAcceptsReviewedBoundedUpdate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "20260730000000_bounded_backfill.sql")
	sql := `-- wahb:large-table-backfill: bounded
UPDATE stories
SET retention_state = 'full'
WHERE id IN (SELECT id FROM stories WHERE retention_state IS NULL ORDER BY id LIMIT 1000);`
	if err := os.WriteFile(path, []byte(sql), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := requireLargeTableMigrationSafety([]migrationFile{{Version: filepath.Base(path), Path: path}}); err != nil {
		t.Fatalf("reviewed bounded update was rejected: %v", err)
	}
}

func TestRequireLargeTableMigrationSafetyAcceptsUnrelatedMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "20260730000000_safe.sql")
	if err := os.WriteFile(path, []byte("CREATE TABLE safe_table (id bigint PRIMARY KEY);"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := requireLargeTableMigrationSafety([]migrationFile{{Version: filepath.Base(path), Path: path}}); err != nil {
		t.Fatalf("unrelated migration was rejected: %v", err)
	}
}

func TestMigrationExecutionErrorExplainsStatementTimeout(t *testing.T) {
	err := migrationExecutionError(errors.New("ERROR: canceling statement due to statement timeout (SQLSTATE 57014)"))
	if !strings.Contains(err.Error(), "bounded indexed batches") {
		t.Fatalf("timeout error is not actionable: %v", err)
	}
}

func TestEmptyBootstrapRequiresExplicitDisposableAcknowledgement(t *testing.T) {
	t.Setenv("CMS_MIGRATION_BOOTSTRAP_DISPOSABLE", "")
	t.Setenv("DATABASE_URL", "postgresql://postgres:test@127.0.0.1:5432/wahb_cms_test_local123")
	if err := validateEmptyBootstrapTarget(); err == nil || !strings.Contains(err.Error(), "CMS_MIGRATION_BOOTSTRAP_DISPOSABLE") {
		t.Fatalf("missing bootstrap acknowledgement was accepted: %v", err)
	}
}

func TestEmptyBootstrapRejectsManagedDatabaseBeforeOpeningIt(t *testing.T) {
	t.Setenv("CMS_MIGRATION_BOOTSTRAP_DISPOSABLE", disposableBootstrapMarker)
	t.Setenv("DATABASE_URL", "postgresql://postgres:test@db.supabase.co:5432/wahb_cms_test_local123")
	if err := validateEmptyBootstrapTarget(); err == nil || !strings.Contains(err.Error(), "localhost or explicitly identified Neon") {
		t.Fatalf("managed bootstrap target was accepted: %v", err)
	}
}

func TestEmptyBootstrapAcceptsExactlyIdentifiedNeonTarget(t *testing.T) {
	t.Setenv("CMS_MIGRATION_BOOTSTRAP_NEON", neonBootstrapMarker)
	t.Setenv("CMS_MIGRATION_BOOTSTRAP_EXPECTED_HOST", "ep-staging-pooler.us-east-2.aws.neon.tech")
	t.Setenv("CMS_MIGRATION_BOOTSTRAP_EXPECTED_DATABASE", "neondb")
	t.Setenv("DATABASE_URL", "postgresql://user:test@ep-staging-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require")
	if err := validateEmptyBootstrapTarget(); err != nil {
		t.Fatalf("exactly identified Neon bootstrap target was rejected: %v", err)
	}
}

func TestEmptyBootstrapRejectsUnexpectedNeonTarget(t *testing.T) {
	t.Setenv("CMS_MIGRATION_BOOTSTRAP_NEON", neonBootstrapMarker)
	t.Setenv("CMS_MIGRATION_BOOTSTRAP_EXPECTED_HOST", "ep-other-pooler.us-east-2.aws.neon.tech")
	t.Setenv("CMS_MIGRATION_BOOTSTRAP_EXPECTED_DATABASE", "neondb")
	t.Setenv("DATABASE_URL", "postgresql://user:test@ep-staging-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require")
	if err := validateEmptyBootstrapTarget(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("unexpected Neon bootstrap target was accepted: %v", err)
	}
}

func TestAppliedVersionsTreatsMissingLedgerAsEmpty(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT to_regclass('public.cms_schema_migrations') IS NOT NULL")).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	applied, err := appliedVersions(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 0 {
		t.Fatalf("missing ledger returned applied migrations: %#v", applied)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
