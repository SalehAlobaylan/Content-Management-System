package atomizationwork

import (
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"content-management-system/src/tests/testdb"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func TestAtomizationClaimDistinguishesUnreadableEvidenceFromChangedInput(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.AtomizationWorkRequest{}, &models.AtomizationWorkEvent{}, &models.MediaSupplyControl{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TABLE content_items (id bigserial PRIMARY KEY, public_id uuid UNIQUE, tenant_id text, parent_content_item_id uuid, type text, duration_sec integer, transcript_id uuid, status text, updated_at timestamptz)`).Error; err != nil {
		t.Fatal(err)
	}
	parent, transcript := uuid.New(), uuid.New()
	now := time.Now().UTC()
	if err := db.Exec("INSERT INTO content_items (public_id,tenant_id,parent_content_item_id,type,duration_sec,transcript_id,status,updated_at) VALUES (?, 'a', NULL, 'PODCAST', 3000, ?, 'READY', ?)", parent, transcript, now).Error; err != nil {
		t.Fatal(err)
	}
	request := models.AtomizationWorkRequest{PublicID: uuid.New(), TenantID: "a", ParentContentItemID: parent, State: "queued", Checkpoints: datatypes.JSON([]byte(`{}`))}
	if err := db.Create(&request).Error; err != nil {
		t.Fatal(err)
	}
	restore := supply.SetSupplyOwnerReadinessForTest(map[string]supply.SupplyOwnerReadiness{"aggregation_atomization": {State: "ready"}})
	defer restore()
	readFailure := errors.New("fixture transcript evidence unavailable")
	if err := db.Callback().Query().Before("gorm:query").Register("fixture_unreadable_transcript", func(tx *gorm.DB) {
		if tx.Statement.Table == "transcripts" {
			tx.AddError(readFailure)
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, found, err := ClaimNext(db, "fixture")
	if found || !errors.Is(err, readFailure) {
		t.Fatalf("unreadable evidence became a terminal preflight: found=%v err=%v", found, err)
	}
	if err := db.First(&request, request.ID).Error; err != nil || request.State != "queued" {
		t.Fatalf("unreadable evidence changed work: %s %v", request.State, err)
	}
	if err := db.Callback().Query().Remove("fixture_unreadable_transcript"); err != nil {
		t.Fatal(err)
	}
	// A definite product eligibility change can terminalize before acquiring a
	// processing slot; this minimal fixture deliberately has no slot tables.
	if err := db.Exec("UPDATE content_items SET duration_sec=2400 WHERE public_id=?", parent).Error; err != nil {
		t.Fatal(err)
	}
	if _, found, err := ClaimNext(db, "fixture"); err != nil || found {
		t.Fatalf("changed input did not terminalize safely: %v %v", found, err)
	}
	if err := db.First(&request, request.ID).Error; err != nil || request.State != "failed" {
		t.Fatalf("changed input remained queued: %s %v", request.State, err)
	}
}

func TestAtomizationCrashMatrixNeverBlindlyRestarts(t *testing.T) {
	tests := []struct {
		name, checkpoints, want string
		effectStarted           bool
	}{
		{"before effect", `{}`, "adopt_same_job", false},
		{"after plan", `{"plan_persisted":{"digest":"a"}}`, "verify_from_plan_persisted", true},
		{"after first cut", `{"first_cut":{"object":"one"}}`, "verify_from_first_cut", true},
		{"after uploads", `{"uploads_complete":{"count":2}}`, "verify_from_uploads_complete", true},
		{"after children", `{"children_persisted":{"count":2}}`, "verify_from_children_persisted", true},
		{"after embedding handoff", `{"embedding_handoff":{"count":2}}`, "verify_from_embedding_handoff", true},
		{"after final receipt", `{"owner_complete":{"child_ids":[]}}`, "verify_from_owner_complete", true},
		{"unknown post effect", `{}`, "verify_effect_unknown", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := AtomizationRecoveryDisposition(datatypes.JSON([]byte(test.checkpoints)), test.effectStarted)
			if err != nil || got != test.want {
				t.Fatalf("got %q, %v; want %q", got, err, test.want)
			}
			if test.effectStarted && got == "adopt_same_job" {
				t.Fatal("post-effect recovery cannot re-enter execution")
			}
		})
	}
}

func TestAtomizationMalformedCheckpointRequiresManualAttention(t *testing.T) {
	got, err := AtomizationRecoveryDisposition(datatypes.JSON([]byte(`{"broken"`)), true)
	if err == nil || got != "manual_attention" {
		t.Fatalf("got %q, %v", got, err)
	}
}
