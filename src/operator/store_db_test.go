package operator

import (
	"context"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func operatorStoreTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := testdb.Open(t)
	if err := db.AutoMigrate(
		&models.OperatorThread{}, &models.OperatorMessage{}, &models.OperatorInvestigation{}, &models.OperatorInvestigationEvent{}, &models.OperatorEvidence{},
		&models.OperatorCapabilityControl{},
		&models.OperatorActionPlan{}, &models.OperatorActionStep{}, &models.OperatorPlanApproval{}, &models.OperatorPlanEvent{}, &models.OperatorActionJob{},
	); err != nil {
		t.Fatalf("migrate operator execution test schema: %v", err)
	}
	clear := func() {
		_ = db.Exec("DELETE FROM operator_evidence").Error
		_ = db.Exec("DELETE FROM operator_investigation_events").Error
		_ = db.Exec("DELETE FROM operator_investigations").Error
		_ = db.Exec("DELETE FROM operator_messages").Error
		_ = db.Exec("DELETE FROM operator_threads").Error
		_ = db.Exec("DELETE FROM operator_plan_events").Error
		_ = db.Exec("DELETE FROM operator_plan_approvals").Error
		_ = db.Exec("DELETE FROM operator_action_steps").Error
		_ = db.Exec("DELETE FROM operator_action_jobs").Error
		_ = db.Exec("DELETE FROM operator_action_plans").Error
		_ = db.Exec("DELETE FROM operator_capability_controls").Error
	}
	clear()
	t.Cleanup(clear)
	return db
}

func newTestPlanStore(db *gorm.DB) *PlanStore {
	store := NewPlanStore(db, []byte("this-is-a-test-only-plan-signing-key-123"))
	store.now = func() time.Time { return time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC) }
	return store
}

func TestPlanStoreCreatesAndApprovesWithImmutableEvents(t *testing.T) {
	db := operatorStoreTestDB(t)
	store := newTestPlanStore(db)
	created, err := store.CreatePlan(context.Background(), samplePlan(), store.now().Add(time.Minute))
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if created.State != "awaiting_approval" || created.Signature == "" {
		t.Fatalf("unexpected created plan: %+v", created)
	}
	var steps []models.OperatorActionStep
	if err := db.Where("plan_id=?", created.ID).Find(&steps).Error; err != nil || len(steps) != 1 || steps[0].State != "pending" {
		t.Fatalf("expected one durable pending step, steps=%+v err=%v", steps, err)
	}
	snapshot := AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, IsAdmin: true, AccessVersion: "access-v1"}
	approved, err := store.ApprovePlan(context.Background(), created.ID, snapshot, "admin-a", "hash-only-proof")
	if err != nil {
		t.Fatalf("approve plan: %v", err)
	}
	if approved.State != "queued" {
		t.Fatalf("expected approval to queue durable work, got %q", approved.State)
	}
	var job models.OperatorActionJob
	if err := db.Where("plan_id=?", created.ID).First(&job).Error; err != nil || job.State != "queued" {
		t.Fatalf("expected durable queued job, job=%+v err=%v", job, err)
	}
	var events []models.OperatorPlanEvent
	if err := db.Where("plan_id=?", created.ID).Order("sequence ASC").Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].EventType != "plan_created" || events[1].EventType != "plan_approved" || events[2].EventType != "plan_queued" {
		t.Fatalf("expected immutable create/approve events, got %+v", events)
	}
}

func TestPlanStoreRefusesTamperedSignedPlanBeforeApproval(t *testing.T) {
	db := operatorStoreTestDB(t)
	store := newTestPlanStore(db)
	created, err := store.CreatePlan(context.Background(), samplePlan(), store.now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.OperatorActionPlan{}).Where("id=?", created.ID).Update("tool_key", "content.delete").Error; err != nil {
		t.Fatal(err)
	}
	snapshot := AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, IsAdmin: true, AccessVersion: "access-v1"}
	if _, err := store.ApprovePlan(context.Background(), created.ID, snapshot, "admin-a", "proof"); err == nil {
		t.Fatal("tampered plan metadata must not be approved")
	}
}

func TestPlanStoreFencesExecutionAndConsumesExactApproval(t *testing.T) {
	db := operatorStoreTestDB(t)
	store := newTestPlanStore(db)
	created, err := store.CreatePlan(context.Background(), samplePlan(), store.now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"feed:manage"}, AccessVersion: "access-v1"}
	if _, err := store.ApprovePlan(context.Background(), created.ID, snapshot, "admin-a", "proof"); err != nil {
		t.Fatal(err)
	}
	claimed, canonical, descriptor, err := store.ClaimPlan(context.Background(), created.ID, snapshot)
	if err != nil {
		t.Fatalf("claim plan: %v", err)
	}
	if claimed.ClaimToken == nil || canonical.PlanID != "plan-1" || descriptor.Key != "feed_integrity.refresh_snapshot" {
		t.Fatalf("unexpected claim: plan=%+v canonical=%+v descriptor=%+v", claimed, canonical, descriptor)
	}
	if _, _, err := store.BeginClaimedPlan(context.Background(), claimed.ID, "tenant-a", *claimed.ClaimToken); err != nil {
		t.Fatalf("begin claimed plan: %v", err)
	}
	if err := store.FinishClaimedPlan(context.Background(), claimed.ID, "tenant-a", uuid.New(), true, nil, nil, nil); err == nil {
		t.Fatal("stale claim token must not finish a plan")
	}
	if err := store.FinishClaimedPlan(context.Background(), claimed.ID, "tenant-a", *claimed.ClaimToken, true, map[string]any{"dirty": true}, map[string]any{"dirty": false}, map[string]any{"fresh": true}); err != nil {
		t.Fatalf("finish claimed plan: %v", err)
	}
	var stored models.OperatorActionPlan
	if err := db.First(&stored, claimed.ID).Error; err != nil || stored.State != "succeeded" || stored.ClaimToken != nil {
		t.Fatalf("expected succeeded unfenced plan, plan=%+v err=%v", stored, err)
	}
	var approval models.OperatorPlanApproval
	if err := db.Where("plan_id=?", claimed.ID).First(&approval).Error; err != nil || approval.ConsumedAt == nil {
		t.Fatalf("approval must be consumed at terminal execution, approval=%+v err=%v", approval, err)
	}
}

func TestPlanStoreQuarantinesInterruptedRunningWorkForVerification(t *testing.T) {
	db := operatorStoreTestDB(t)
	store := newTestPlanStore(db)
	created, err := store.CreatePlan(context.Background(), samplePlan(), store.now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"feed:manage"}, AccessVersion: "access-v1"}
	if _, err := store.ApprovePlan(context.Background(), created.ID, snapshot, "admin-a", "proof"); err != nil {
		t.Fatal(err)
	}
	claimed, _, _, err := store.ClaimPlan(context.Background(), created.ID, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.BeginClaimedPlan(context.Background(), claimed.ID, "tenant-a", *claimed.ClaimToken); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return claimed.ClaimExpiresAt.Add(time.Second) }
	if recovered, err := store.RecoverExpiredPlanClaims(context.Background()); err != nil || recovered != 1 {
		t.Fatalf("recover interrupted execution=%d err=%v", recovered, err)
	}
	var plan models.OperatorActionPlan
	var step models.OperatorActionStep
	if err := db.First(&plan, claimed.ID).Error; err != nil || plan.State != "verifying" || plan.ClaimToken != nil {
		t.Fatalf("interrupted plan must be verification-only, plan=%+v err=%v", plan, err)
	}
	if err := db.Where("plan_id=?", claimed.ID).First(&step).Error; err != nil || step.State != "verifying" || step.ClaimToken != nil {
		t.Fatalf("interrupted step must be verification-only, step=%+v err=%v", step, err)
	}
}

type staticAccessProvider struct{ snapshot AccessSnapshot }

func (provider staticAccessProvider) Snapshot(_ context.Context, _, _ string) (AccessSnapshot, error) {
	return provider.snapshot, nil
}

func TestPlanStoreClaimRequiresLiveMatchingAccess(t *testing.T) {
	db := operatorStoreTestDB(t)
	store := newTestPlanStore(db)
	created, err := store.CreatePlan(context.Background(), samplePlan(), store.now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"feed:manage"}, AccessVersion: "access-v1"}
	if _, err := store.ApprovePlan(context.Background(), created.ID, snapshot, "admin-a", "proof"); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNextPlan(context.Background(), staticAccessProvider{snapshot: snapshot}, "feed:manage")
	if err != nil {
		t.Fatalf("claim plan: %v", err)
	}
	if claimed.State != "claimed" || claimed.ClaimToken == nil || claimed.ClaimExpiresAt == nil {
		t.Fatalf("expected durable claim, got %+v", claimed)
	}
	store.now = func() time.Time { return claimed.ClaimExpiresAt.Add(time.Second) }
	recovered, err := store.RecoverExpiredPlanClaims(context.Background())
	if err != nil || recovered != 1 {
		t.Fatalf("recover expired claim=%d err=%v", recovered, err)
	}
	var recoveredPlan models.OperatorActionPlan
	if err := db.First(&recoveredPlan, claimed.ID).Error; err != nil {
		t.Fatal(err)
	}
	if recoveredPlan.State != "queued" || recoveredPlan.ClaimToken != nil || recoveredPlan.ClaimExpiresAt != nil {
		t.Fatalf("recovered plan=%+v", recoveredPlan)
	}
	inactive := snapshot
	inactive.Active = false
	if _, err := store.ClaimNextPlan(context.Background(), staticAccessProvider{snapshot: inactive}, "feed:manage"); err == nil {
		t.Fatal("inactive snapshot must fail closed")
	}
}

func TestPlanStoreHonorsDisableOnlyToolControlAtApprovalAndClaim(t *testing.T) {
	db := operatorStoreTestDB(t)
	store := newTestPlanStore(db)
	created, err := store.CreatePlan(context.Background(), samplePlan(), store.now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.OperatorCapabilityControl{TenantID: "tenant-a", CapabilityKind: "tool", CapabilityKey: "feed_integrity.refresh_snapshot", Disabled: true, Reason: "emergency stop", ActorID: "admin-a"}).Error; err != nil {
		t.Fatal(err)
	}
	snapshot := AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"feed:manage"}, AccessVersion: "access-v1"}
	if _, err := store.ApprovePlan(context.Background(), created.ID, snapshot, "admin-a", "proof"); err == nil {
		t.Fatal("disabled tool must not be approved")
	}
	if err := db.Where("tenant_id=?", "tenant-a").Delete(&models.OperatorCapabilityControl{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApprovePlan(context.Background(), created.ID, snapshot, "admin-a", "proof"); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.OperatorCapabilityControl{TenantID: "tenant-a", CapabilityKind: "tool", CapabilityKey: "feed_integrity.refresh_snapshot", Disabled: true, Reason: "emergency stop", ActorID: "admin-a"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.ClaimPlan(context.Background(), created.ID, snapshot); err == nil {
		t.Fatal("disabled tool must not be claimed after approval")
	}
}

func TestPlanStoreCancelsOnlyBeforeStartAndRecordsLifecycleEvent(t *testing.T) {
	db := operatorStoreTestDB(t)
	store := newTestPlanStore(db)
	created, err := store.CreatePlan(context.Background(), samplePlan(), store.now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := AccessSnapshot{UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"feed:manage"}, AccessVersion: "access-v1"}
	if _, err := store.ApprovePlan(context.Background(), created.ID, snapshot, "admin-a", "proof"); err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.CancelPlan(context.Background(), created.ID, snapshot)
	if err != nil || cancelled.State != "cancelled" {
		t.Fatalf("cancel plan=%+v err=%v", cancelled, err)
	}
	if _, _, _, err := store.ClaimPlan(context.Background(), created.ID, snapshot); err == nil {
		t.Fatal("cancelled plan must never be claimed")
	}
	var event models.OperatorPlanEvent
	if err := db.Where("plan_id=? AND event_type=?", created.ID, "plan_cancelled").First(&event).Error; err != nil {
		t.Fatalf("expected cancellation event: %v", err)
	}
}
