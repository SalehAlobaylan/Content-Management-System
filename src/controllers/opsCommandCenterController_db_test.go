package controllers

import (
	"content-management-system/src/tests/testdb"
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// PostgreSQL tests for the Operations Command Center pin the things unit tests cannot:
// (1) every hand-written registry/attention SQL string actually executes against
// the real schema — the adapter contract; (2) a pause value written to a
// `timestamp` policy column round-trips close enough to the `timestamptz`
// command ledger for the resume ownership check to recognize its own pause.
var opsFixtureOnce sync.Once
var opsFixtureMu sync.Mutex
var opsFixtureDB *gorm.DB
var opsFixtureClose func() error
var opsFixtureErr error

func opsTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("requires an explicitly guarded disposable CMS database")
	}
	opsFixtureMu.Lock()
	t.Cleanup(opsFixtureMu.Unlock)
	opsFixtureOnce.Do(func() {
		opsFixtureDB, opsFixtureClose, opsFixtureErr = testdb.OpenForMain()
		if opsFixtureErr == nil {
			opsFixtureErr = opsMigrateFixture(opsFixtureDB)
		}
	})
	if opsFixtureErr != nil {
		t.Fatal(opsFixtureErr)
	}
	// The factory owns this random database. Keep the canonical schema/ledger and
	// clear all business fixtures between serialized cases in one FK-safe reset.
	var names []string
	if err := opsFixtureDB.Raw("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('cms_schema_migrations','database_migration_owner_control') AND tablename NOT LIKE 'wahb_%' ORDER BY tablename").Scan(&names).Error; err != nil {
		t.Fatal(err)
	}
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, pq.QuoteIdentifier(name))
	}
	if len(quoted) > 0 {
		if err := opsFixtureDB.Exec("TRUNCATE TABLE " + strings.Join(quoted, ",") + " RESTART IDENTITY CASCADE").Error; err != nil {
			t.Fatal(err)
		}
	}
	opsWarmFixturePool(t, opsFixtureDB)
	return opsFixtureDB
}
func opsMigrateFixture(db *gorm.DB) error {
	dsn := db.Dialector.(*postgres.Dialector).Config.DSN
	target, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("invalid disposable target")
	}
	cmd := exec.Command("go", "run", "./cmd/migrate", "--all", "--allow-destructive", "--bootstrap-empty")
	cmd.Dir = "../.."
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "DATABASE_URL=") && !strings.HasPrefix(entry, "CMS_RUNTIME_DATABASE_URL=") && !strings.HasPrefix(entry, "CMS_MIGRATION_BOOTSTRAP_") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "DATABASE_URL="+dsn)
	if strings.HasSuffix(target.Hostname(), ".neon.tech") {
		cmd.Env = append(cmd.Env, "CMS_MIGRATION_BOOTSTRAP_NEON=I_UNDERSTAND_THIS_NEON_DATABASE_IS_EMPTY", "CMS_MIGRATION_BOOTSTRAP_EXPECTED_HOST="+target.Hostname(), "CMS_MIGRATION_BOOTSTRAP_EXPECTED_DATABASE="+strings.TrimPrefix(target.Path, "/"))
	} else {
		cmd.Env = append(cmd.Env, "CMS_MIGRATION_BOOTSTRAP_DISPOSABLE=I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE")
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		safe := strings.ReplaceAll(string(output), dsn, "[redacted disposable DSN]")
		if target.User != nil {
			if password, ok := target.User.Password(); ok && password != "" {
				safe = strings.ReplaceAll(safe, password, "[redacted]")
			}
		}
		if len(safe) > 5000 {
			safe = safe[len(safe)-5000:]
		}
		return fmt.Errorf("canonical operations fixture failed: %v\n%s", err, safe)
	}
	return nil
}

// The adapter contract: every lane's StatusSQL and every attention adapter SQL
// must execute without error against the live schema. Hand-written SQL strings
// only fail at runtime; this is the test that fails instead.
func TestOpsRegistryAdapterContract(t *testing.T) {
	db := opsTestDB(t)
	now := time.Now().UTC()
	for _, member := range opsFleetRegistry {
		for _, lane := range member.Lanes {
			if lane.RunTable != "" {
				var evidence []opsRunEvidence
				if err := db.Raw("SELECT * FROM (" + opsRunSelect(member, lane) + ") r LIMIT 0").Scan(&evidence).Error; err != nil {
					t.Errorf("%s/%s run evidence SQL: %v", member.Key, lane.Key, err)
				}
			}
			statuses := opsStatusForLane(db, member, lane, now)
			for _, status := range statuses {
				if status.State == "errored" {
					t.Errorf("%s/%s status adapter errored: %s", member.Key, lane.Key, status.Error)
				}
			}
			if lane.PauseColumn != "" && lane.Table == "" {
				t.Errorf("%s/%s declares a pause column without a table", member.Key, lane.Key)
			}
		}
	}
	if _, errs := opsOpenAttention(db, now); len(errs) > 0 {
		t.Errorf("attention adapters errored: %v", errs)
	}
}

// Pause → resume ownership round-trip through real column types. If the
// timestamp/timestamptz precision or timezone handling ever breaks
// opsPauseValuesMatch, this fails rather than every production resume
// silently reporting skipped: foreign_pause.
func TestOpsPauseValueRoundTripThroughPolicyColumns(t *testing.T) {
	db := opsTestDB(t)
	member, lane, ok := opsFindLane("pipeline", "autopilot")
	if !ok {
		t.Fatal("pipeline/autopilot lane missing from registry")
	}
	_ = member
	tenant := "ops-roundtrip-test"
	db.Where("tenant_id = ?", tenant).Delete(&models.PipelineAutopilotPolicy{})
	policy := models.DefaultPipelineAutopilotPolicy(tenant)
	if err := db.Create(&policy).Error; err != nil {
		t.Fatalf("seed policy: %v", err)
	}
	defer db.Where("tenant_id = ?", tenant).Delete(&models.PipelineAutopilotPolicy{})

	until := time.Now().UTC().Add(45 * time.Minute)
	update := fmt.Sprintf("UPDATE %s SET %s=?, updated_at=? WHERE %s=?", lane.Table, lane.PauseColumn, lane.TenantColumn)
	if err := db.Exec(update, until, time.Now().UTC(), tenant).Error; err != nil {
		t.Fatalf("write pause: %v", err)
	}
	query := fmt.Sprintf("SELECT %s AS paused_until FROM %s WHERE %s=?", lane.PauseColumn, lane.Table, lane.TenantColumn)
	var current struct {
		PausedUntil *time.Time `gorm:"column:paused_until"`
	}
	if err := db.Raw(query, tenant).Scan(&current).Error; err != nil {
		t.Fatalf("read pause back: %v", err)
	}
	canonical := current.PausedUntil
	if !opsPauseValuesMatch(current.PausedUntil, canonical) {
		t.Fatalf("resume ownership check does not recognize its own pause: wrote %v, read %v", until, current.PausedUntil)
	}
	foreign := until.Add(30 * time.Minute)
	if opsPauseValuesMatch(current.PausedUntil, &foreign) {
		t.Fatal("resume ownership check matched a foreign pause 30m away")
	}
}

// Connection establishment is provisioned test infrastructure, not a member's
// read budget. Hold four connections together so the measured request begins
// with the same warm pool that the supported running stack normally has.
func opsWarmFixturePool(t *testing.T, db *gorm.DB) {
	t.Helper()
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxIdleConns(4)
	pool.SetMaxOpenConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connections := []*sql.Conn{}
	defer func() {
		for _, c := range connections {
			_ = c.Close()
		}
	}()
	for i := 0; i < 4; i++ {
		c, e := pool.Conn(ctx)
		if e != nil {
			t.Fatalf("warm disposable read pool: %v", e)
		}
		connections = append(connections, c)
		if e = c.PingContext(ctx); e != nil {
			t.Fatalf("warm disposable read connection: %v", e)
		}
	}
}
