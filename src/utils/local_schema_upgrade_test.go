package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestQualifiedSchemaManifestPinsExactCanonicalBytes(t *testing.T) {
	for version, policy := range localMigrationPolicies {
		checksum := policy.Checksum
		b, err := os.ReadFile(filepath.Join("..", "..", "migrations", version))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != checksum {
			t.Fatal("qualified SQL changed", version)
		}
		if QualifiedLocalSchemaMigration(version, checksum+"changed") {
			t.Fatal("changed SQL was qualified")
		}
	}
	if QualifiedLocalSchemaMigration("future.sql", "checksum") {
		t.Fatal("unreviewed SQL qualified")
	}
}

func TestQualifiedSchemaWorkReadsLiveClaimsWithoutSettlingHistory(t *testing.T) {
	for _, blocker := range []string{"", "content_stage_attempts", "pipeline_repair_requests", "pipeline_stage_leases", "source_run_attempts", "lifecycle_operation_claims", "unknown"} {
		t.Run(blocker, func(t *testing.T) {
			db, mock := migrationWorkDB(t)
			subjects := []string{"lifecycle_operation_claims", "pipeline_stage_leases", "source_run_attempts", "content_stage_attempts", "pipeline_repair_requests", "artifact_coverage_requests", "atomization_work_requests", "studio_clearance_requests", "media_supply_actions"}
			for _, subject := range subjects {
				mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				query := "SELECT count\\(\\*\\) FROM " + regexp.QuoteMeta(subject)
				if subject == "content_stage_attempts" {
					query += ".*lease_expires_at IS NULL OR lease_expires_at > now"
				}
				if subject == "pipeline_repair_requests" {
					query += ".*claim_expires_at > now"
				}
				if blocker == "unknown" {
					mock.ExpectQuery(query).WillReturnError(errors.New("not readable"))
					break
				}
				count := 0
				if subject == blocker {
					count = 1
				}
				mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
				if count > 0 {
					break
				}
			}
			err := CheckQualifiedLocalSchemaWork(db)
			if (err != nil) != (blocker != "") {
				t.Fatal("incorrect schema authority", err)
			}
			// A write to any processing row would fail the expectations.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The existing CMS CI runs this authoring check with go test ./....
func TestCanonicalMigrationsHaveReviewedLocalPolicies(t *testing.T) {
	files, err := filepath.Glob("../../migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	canonical := map[string]bool{}
	timestamped := regexp.MustCompile(`^\d{14}_.+\.sql$`)
	for _, path := range files {
		version := filepath.Base(path)
		canonical[version] = true
		if !timestamped.MatchString(version) {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if err := ValidateLocalMigrationPolicy(version, hex.EncodeToString(sum[:])); err != nil {
			t.Error(err)
		}
	}
	for version := range localMigrationPolicies {
		if !canonical[version] {
			t.Errorf("local migration policy %s has no canonical SQL file", version)
		}
	}
}

func TestFuturePoliciesRequireExplicitReviewAndPreserveConservativeFallback(t *testing.T) {
	const version = "20300101000000_future.sql"
	checksum := strings.Repeat("a", 64)
	if err := ValidateLocalMigrationPolicy(version, checksum); err == nil || !strings.Contains(err.Error(), "needs a tested local migration policy") {
		t.Fatalf("missing future policy: %v", err)
	}
	if err := ValidateLocalMigrationPolicy("20260101000000_historical.sql", checksum); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { delete(localMigrationPolicies, version) })
	for _, scope := range []string{PreservesExpiredProcessingHistory, RequiresOwnerReconciliation, "invalid"} {
		localMigrationPolicies[version] = LocalMigrationPolicy{checksum, scope}
		if err := ValidateLocalMigrationPolicy(version, checksum); (err == nil) != (scope != "invalid") {
			t.Fatalf("policy %s: %v", scope, err)
		}
		if QualifiedLocalSchemaMigration(version, checksum) != (scope == PreservesExpiredProcessingHistory) {
			t.Fatal("wrong processing compatibility", scope)
		}
		if QualifiedDeletionDefinitions(version, checksum) {
			t.Fatal("processing policy granted destructive approval")
		}
		if err := ValidateLocalMigrationPolicy(version, checksum+"changed"); err == nil {
			t.Fatal("changed SQL accepted")
		}
	}
}

func TestSharedWorkCheckRetainsExpiredHistoryButNamesExactBlocker(t *testing.T) {
	const version = "20300101000000_future.sql"
	checksum := strings.Repeat("a", 64)
	t.Cleanup(func() { delete(localMigrationPolicies, version) })
	for _, tc := range []struct {
		name, scope, subject, reason string
		count                        int
	}{
		{"compatible expired history", PreservesExpiredProcessingHistory, "content_stage_attempts", "", 0},
		{"compatible live attempt", PreservesExpiredProcessingHistory, "content_stage_attempts", "live_work", 1},
		{"owner reconciliation", RequiresOwnerReconciliation, "content_stage_attempts", "owner_reconciliation_required", 195},
		{"unreviewed SQL", "", "content_stage_attempts", "migration_not_reviewed", 195},
		{"live lease", PreservesExpiredProcessingHistory, "pipeline_stage_leases", "live_work", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delete(localMigrationPolicies, version)
			if tc.scope != "" {
				localMigrationPolicies[version] = LocalMigrationPolicy{checksum, tc.scope}
			}
			db, mock := migrationWorkDB(t)
			probes := append([]MigrationInflightProbe{{"lifecycle_operation_claims", ""}}, CMSMigrationInflightQueries...)
			for _, probe := range probes {
				exists := probe.Subject == tc.subject
				mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + probe.Subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
				if exists {
					query := "SELECT count\\(\\*\\) FROM " + regexp.QuoteMeta(probe.Subject)
					if tc.scope == PreservesExpiredProcessingHistory && probe.Subject == "content_stage_attempts" {
						query += ".*lease_expires_at IS NULL OR lease_expires_at > now"
					}
					mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tc.count))
					if tc.count > 0 {
						if tc.scope != PreservesExpiredProcessingHistory && tc.subject == "content_stage_attempts" {
							mock.ExpectQuery("SELECT count\\(\\*\\) FROM content_stage_attempts.*lease_expires_at IS NULL OR lease_expires_at > now").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
						}
						break
					}
				}
			}
			err := CheckLocalMigrationWork(db, []LocalMigration{{version, checksum}})
			if tc.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var blocked *MigrationWorkBlockedError
				if !errors.As(err, &blocked) || blocked.Blocker.Reason != tc.reason || blocked.Blocker.Subject != tc.subject {
					t.Fatalf("wrong blocker: %v", err)
				}
				if tc.reason != "live_work" && blocked.Blocker.Migration != version {
					t.Fatal("missing migration filename", blocked.Blocker)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func Test196UnresolvedRecordsPermitQualifiedSchemaWithoutSettlingThem(t *testing.T) {
	db, mock := migrationWorkDB(t)
	probes := append([]MigrationInflightProbe{{"lifecycle_operation_claims", "SELECT count(*) FROM lifecycle_operation_claims WHERE state='active'"}}, CMSMigrationInflightQueries...)
	for _, probe := range probes {
		exists := probe.Subject == "content_stage_attempts" || probe.Subject == "pipeline_repair_requests"
		mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + probe.Subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
		if !exists {
			continue
		}
		count := 1
		if probe.Subject == "content_stage_attempts" {
			count = 195
		}
		mock.ExpectQuery(regexp.QuoteMeta(probe.Query)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
		if probe.Subject == "content_stage_attempts" {
			mock.ExpectQuery(`SELECT count\(\*\) FROM content_stage_attempts WHERE .*lease_expires_at<=now`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(195))
			mock.ExpectQuery(`SELECT count\(\*\) FROM content_stage_attempts a`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(55))
		}
	}
	report, err := InspectCMSMigrationWork(db)
	if err != nil || report.Count != 196 || report.State != "blocked" {
		t.Fatal(report, err)
	}
	migrations := []LocalMigration{}
	for version, policy := range localMigrationPolicies {
		migrations = append(migrations, LocalMigration{version, policy.Checksum})
	}
	for _, probe := range probes {
		exists := probe.Subject == "content_stage_attempts" || probe.Subject == "pipeline_repair_requests"
		mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + probe.Subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
		if exists {
			mock.ExpectQuery(`SELECT count\(\*\) FROM ` + probe.Subject + `.*(lease_expires_at IS NULL OR lease_expires_at > now|claim_expires_at > now)`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		}
	}
	if err := CheckLocalMigrationWork(db, migrations); err != nil {
		t.Fatal(err)
	}
	// No UPDATE, DELETE or ledger writes are allowed by this SQL mock: attempts
	// and receipts stay untouched by qualification of the pending schema.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestQualifiedSchemaExecutionRechecksFenceAndWorkAfterPreview(t *testing.T) {
	t.Setenv("WAHB_LOCAL_PREPARATION", "1")
	t.Setenv("WAHB_PREPARATION_DATABASE_ID", "absent")
	const version = "20261002150000_content_reset_completion.sql"
	policy := localMigrationPolicies[version]
	for _, sealed := range []bool{false, true} {
		t.Run(fmt.Sprint(sealed), func(t *testing.T) {
			db, mock := migrationWorkDB(t)
			for _, subject := range []string{"content_stage_attempts", "pipeline_repair_requests", "pipeline_stage_leases"} {
				mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				mock.ExpectExec("LOCK TABLE " + subject + " IN SHARE MODE").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false)) // identity absent
			mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(sealed))
			if sealed {
				mock.ExpectQuery(`SELECT state,epoch FROM wahb_database_writer_fence`).WillReturnRows(sqlmock.NewRows([]string{"state", "epoch"}).AddRow("sealed", 0))
			} else {
				mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false)) // owner control
				mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false)) // identity's lifecycle check
				for _, subject := range []string{"lifecycle_operation_claims", "pipeline_stage_leases", "source_run_attempts", "content_stage_attempts"} {
					exists := subject == "content_stage_attempts"
					mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
					if exists {
						mock.ExpectQuery(`SELECT count\(\*\) FROM content_stage_attempts.*lease_expires_at`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
					}
				}
			}
			if err := CheckLocalSchemaMigrationAuthority(db, version, policy.Checksum); err == nil {
				t.Fatal("execution ignored new fence/live claim")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFutureCompatibleMigrationUsesExistingExecutionAuthority(t *testing.T) {
	const version = "20300101000000_future.sql"
	checksum := strings.Repeat("a", 64)
	localMigrationPolicies[version] = LocalMigrationPolicy{checksum, PreservesExpiredProcessingHistory}
	t.Cleanup(func() { delete(localMigrationPolicies, version) })
	t.Setenv("WAHB_LOCAL_PREPARATION", "1")
	t.Setenv("WAHB_PREPARATION_DATABASE_ID", "absent")
	db, mock := migrationWorkDB(t)
	for _, subject := range []string{"content_stage_attempts", "pipeline_repair_requests", "pipeline_stage_leases"} {
		mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectExec("LOCK TABLE " + subject + " IN SHARE MODE").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	for i := 0; i < 4; i++ {
		mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	}
	probes := append([]MigrationInflightProbe{{"lifecycle_operation_claims", ""}}, CMSMigrationInflightQueries...)
	for _, probe := range probes {
		retained := probe.Subject == "content_stage_attempts" || probe.Subject == "pipeline_repair_requests"
		mock.ExpectQuery(`SELECT to_regclass`).WithArgs("public." + probe.Subject).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(retained))
		if retained {
			mock.ExpectQuery(regexp.QuoteMeta(localSchemaWorkQuery(probe))).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		}
	}
	if err := CheckLocalSchemaMigrationAuthority(db, version, checksum); err != nil {
		t.Fatal(err)
	}
	if QualifiedDeletionDefinitions(version, checksum) {
		t.Fatal("future processing policy approved destructive SQL")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
