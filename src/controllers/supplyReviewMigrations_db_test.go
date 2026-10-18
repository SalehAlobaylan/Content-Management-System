package controllers

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"content-management-system/src/tests/testdb"
)

// This applies the actual new SQL against minimal pre-existing owner tables.
// It is forward-migration/constraint evidence, not a claim about the full Neon ledger.
func TestSupplyReviewForwardSQLAndConstraints(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := testdb.Open(t)
	for _, sql := range []string{
		`CREATE TABLE source_run_execution_units(public_id uuid PRIMARY KEY,unit_type text)`,
		`CREATE TABLE media_supply_action_requests(public_id uuid PRIMARY KEY,tenant_id text,execution_owner text,state text,updated_at timestamptz,created_at timestamptz)`,
		`CREATE TABLE artifact_coverage_requests(owner text,state text,updated_at timestamptz,created_at timestamptz,public_id uuid)`,
		`CREATE TABLE atomization_work_requests(state text,updated_at timestamptz,created_at timestamptz,public_id uuid)`,
		`CREATE TABLE studio_clearance_requests(state text,updated_at timestamptz,created_at timestamptz,public_id uuid)`,
		`CREATE TABLE consumer_feed_sessions(id uuid)`,
		`CREATE TABLE experience_events(id bigint)`,
		`CREATE TABLE pods_boundary_observations(tenant_id text,generation_id uuid,boundary text,observed_at timestamptz)`,
		`CREATE TABLE media_supply_episode_events(event_type text CONSTRAINT media_supply_episode_events_event_type_check CHECK(event_type IN ('opened','observed','recovering','resolved','resolution_clean')))`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"20261018010000_source_run_completion_evidence.sql", "20261018020000_supply_owner_recovery.sql", "20261018030000_supply_consumer_evidence.sql"} {
		sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Exec(string(sql)).Error; err != nil {
			t.Fatalf("canonical SQL %s: %v", name, err)
		}
	}
	if err := db.Exec(`INSERT INTO source_run_execution_units(public_id,unit_type,normalization_inputs,normalization_input_digest)VALUES('00000000-0000-4000-8000-000000000001','normalize_batch','[{"upstream_item_id":"exact-item"}]',?)`, strings.Repeat("a", 64)).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`UPDATE source_run_execution_units SET normalization_input_digest=?`, strings.Repeat("b", 64)).Error; err == nil {
		t.Fatal("canonical SQL permits cohort replacement")
	}
	if err := db.Exec(`INSERT INTO media_supply_action_requests(public_id,tenant_id,operator_plan_id,operator_step_id)VALUES('00000000-0000-4000-8000-000000000002','a','00000000-0000-4000-8000-000000000003',NULL)`).Error; err == nil {
		t.Fatal("half-bound Operator handoff accepted")
	}
	if err := db.Exec(`INSERT INTO media_supply_action_requests(public_id,tenant_id,operator_plan_id,operator_step_id)VALUES('00000000-0000-4000-8000-000000000002','a','00000000-0000-4000-8000-000000000003','00000000-0000-4000-8000-000000000004')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO media_supply_action_requests(public_id,tenant_id,operator_plan_id,operator_step_id)VALUES('00000000-0000-4000-8000-000000000005','a','00000000-0000-4000-8000-000000000003','00000000-0000-4000-8000-000000000004')`).Error; err == nil {
		t.Fatal("duplicate native Operator step accepted")
	}
	if err := db.Exec(`INSERT INTO media_supply_episode_events VALUES('resolution_not_clean')`).Error; err != nil {
		t.Fatal(err)
	}
}
