package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"os"
	"testing"
)

func TestOperatorSupplyHandoffWaitsForExactVerifiedNativeTerminal(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	for _, outcome := range []string{"succeeded", "failed", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			db := testdb.Open(t)
			if err := db.AutoMigrate(&models.OperatorActionPlan{}, &models.OperatorActionStep{}, &models.OperatorActionJob{}, &models.OperatorPlanApproval{}, &models.OperatorPlanEvent{}, &models.MediaSupplyActionRequest{}); err != nil {
				t.Fatal(err)
			}
			plan := models.OperatorActionPlan{PublicID: uuid.New(), TenantID: "a", ActorID: "admin", ToolKey: "artifact.request_transcript", State: "running", Digest: "fixture"}
			if err := db.Create(&plan).Error; err != nil {
				t.Fatal(err)
			}
			step := models.OperatorActionStep{PublicID: uuid.New(), PlanID: plan.ID, TenantID: "a", Ordinal: 1, State: "running", ToolKey: plan.ToolKey}
			if err := db.Create(&step).Error; err != nil {
				t.Fatal(err)
			}
			job := models.OperatorActionJob{PublicID: uuid.New(), PlanID: plan.ID, TenantID: "a", State: "claimed"}
			if err := db.Create(&job).Error; err != nil {
				t.Fatal(err)
			}
			approval := models.OperatorPlanApproval{PublicID: uuid.New(), PlanID: plan.ID, TenantID: "a", ActorID: "admin", PlanDigest: plan.Digest}
			if err := db.Create(&approval).Error; err != nil {
				t.Fatal(err)
			}
			action := models.MediaSupplyActionRequest{PublicID: uuid.New(), TenantID: "a", PreviewID: uuid.New(), OperatorPlanID: &plan.PublicID, OperatorStepID: &step.PublicID, ActionKey: plan.ToolKey, ActionVersion: "v1", TargetType: "content_item", TargetID: uuid.New(), ExecutionOwner: "media", State: "queued", IdempotencyKey: uuid.NewString(), PlannedEffects: datatypes.JSON([]byte(`{}`)), AffectedSubjects: datatypes.JSON([]byte(`[]`)), DeepLinks: datatypes.JSON([]byte(`[]`))}
			if err := db.Create(&action).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Transaction(func(tx *gorm.DB) error { return waitOperatorSupplyHandoff(tx, plan, step, action) }); err != nil {
				t.Fatal(err)
			}
			if err := reconcileOperatorSupplyHandoffs(context.Background(), db); err != nil {
				t.Fatal(err)
			}
			var observed models.OperatorActionPlan
			if err := db.First(&observed, plan.ID).Error; err != nil || observed.State != "verifying" || observed.CompletedAt != nil {
				t.Fatalf("queue acceptance completed repair: %+v %v", observed, err)
			}
			// Restart readback does not authorize a new effect. Missing success proof
			// stays verifying; an exact verified terminal converges idempotently.
			if err := db.Model(&action).Update("state", outcome).Error; err != nil {
				t.Fatal(err)
			}
			if outcome == "succeeded" {
				if err := reconcileOperatorSupplyHandoffs(context.Background(), db); err != nil {
					t.Fatal(err)
				}
				db.First(&observed, plan.ID)
				if observed.State != "verifying" {
					t.Fatal("success without persisted owner proof escaped")
				}
				proof := datatypes.JSON([]byte(`{"schema_version":"media-supply-verified-effects/v1","proof":{"artifact":"transcript","evidence":"present"},"affected_domains":["media"],"affected_subjects":[],"deep_links":[]}`))
				if err := db.Model(&action).Update("verified_effects", proof).Error; err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				if err := reconcileOperatorSupplyHandoffs(context.Background(), db); err != nil {
					t.Fatal(err)
				}
			}
			db.First(&observed, plan.ID)
			if observed.State != outcome || observed.CompletedAt == nil {
				t.Fatalf("native outcome not propagated: %+v", observed)
			}
			db.First(&approval, approval.ID)
			if approval.ConsumedAt == nil {
				t.Fatal("approval not consumed after verified terminal")
			}
			var terminalEvents int64
			db.Model(&models.OperatorPlanEvent{}).Where("plan_id=? AND event_type=?", plan.ID, "native_action_"+outcome).Count(&terminalEvents)
			if terminalEvents != 1 {
				t.Fatalf("duplicate monitor terminal events: %d", terminalEvents)
			}
			db.First(&job, job.ID)
			if job.State != outcome {
				t.Fatalf("job not converged: %s", job.State)
			}
		})
	}
}

func TestStudioClearancePagesLargeExactChildSet(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := testdb.Open(t)
	if err := db.Exec(`CREATE TABLE atomization_work_requests (id bigserial PRIMARY KEY, public_id uuid UNIQUE, tenant_id text, state text, parent_content_item_id uuid); CREATE TABLE content_items (public_id uuid PRIMARY KEY, tenant_id text, parent_content_item_id uuid)`).Error; err != nil {
		t.Fatal(err)
	}
	parent, work := uuid.New(), uuid.New()
	if err := db.Exec("INSERT INTO atomization_work_requests (public_id, tenant_id, state, parent_content_item_id) VALUES (?, ?, ?, ?)", work, "a", "succeeded", parent).Error; err != nil {
		t.Fatal(err)
	}
	ids := make([]uuid.UUID, 205)
	raw := make([]string, 205)
	for i := range ids {
		ids[i] = uuid.New()
		raw[i] = ids[i].String()
		if err := db.Exec("INSERT INTO content_items VALUES (?, ?, ?)", ids[i], "a", parent).Error; err != nil {
			t.Fatal(err)
		}
	}
	bytes, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	request := models.StudioClearanceRequest{TenantID: "a", AtomizationRequestID: work, ChildIDs: datatypes.JSON(bytes)}
	got, err := exactStudioChildIDs(db, request)
	if err != nil || len(got) != len(ids) {
		t.Fatalf("205 legal children rejected: %d %v", len(got), err)
	}
	items, err := loadStudioChildItems(db, "a", got)
	if err != nil || len(items) != 205 {
		t.Fatalf("paged item read lost children: %d %v", len(items), err)
	}
	if err := db.Exec("UPDATE content_items SET tenant_id=? WHERE public_id=?", "b", ids[204]).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := exactStudioChildIDs(db, request); err == nil {
		t.Fatal("cross-tenant last-page child accepted")
	}
}
