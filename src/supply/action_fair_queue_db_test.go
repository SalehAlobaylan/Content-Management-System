package supply

import (
	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"sync"
	"testing"
	"time"
)

func TestSupplyFairQueueProgressesPastSixteenDisabledTenantRequests(t *testing.T) {
	if !supplyDisposableDatabaseConfigured() {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.MediaSupplyActionRequest{}, &models.MediaSupplyActionAttempt{}, &models.MediaSupplyActionEvent{}, &models.MediaSupplyControl{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	key, _ := SupplyActionControlKey(SupplyActionCancelUnstarted)
	if err := db.Create(&models.MediaSupplyControl{TenantID: "blocked", ControlKey: key, ScopeType: "tenant", ScopeID: "all", DisabledAt: now, DisabledBy: "fixture", Reason: "fixture"}).Error; err != nil {
		t.Fatal(err)
	}
	var eligible uuid.UUID
	for i := 0; i < 17; i++ {
		tenant := "blocked"
		if i == 16 {
			tenant = "healthy"
		}
		request := models.MediaSupplyActionRequest{PublicID: uuid.New(), TenantID: tenant, PreviewID: uuid.New(), ActionKey: SupplyActionCancelUnstarted, ActionVersion: "v1", TargetType: "source_run_execution_unit", TargetID: uuid.New(), ExecutionOwner: "cms", ExecutionMode: SupplyExecutionApprovalRequired, IdempotencyKey: uuid.NewString(), State: "queued", PlannedEffects: datatypes.JSON([]byte(`{}`)), AffectedSubjects: datatypes.JSON([]byte(`[]`)), DeepLinks: datatypes.JSON([]byte(`[]`)), CreatedAt: now.Add(-time.Hour).Add(time.Duration(i) * time.Millisecond), UpdatedAt: now.Add(-time.Hour).Add(time.Duration(i) * time.Millisecond)}
		if err := db.Create(&request).Error; err != nil {
			t.Fatal(err)
		}
		if i == 16 {
			eligible = request.PublicID
		}
	}
	if _, found, err := claimNextCMSOwnedSupplyAction(db, "first"); err != nil || found {
		t.Fatalf("first bounded review found=%v err=%v", found, err)
	}
	// Concurrent next ticks may adopt different windows but can claim this exact
	// eligible request once only. A paused tenant cannot hide it indefinitely.
	var wg sync.WaitGroup
	var mu sync.Mutex
	claims := 0
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, found, err := claimNextCMSOwnedSupplyAction(db, "next")
			if err != nil {
				t.Error(err)
				return
			}
			if found {
				mu.Lock()
				defer mu.Unlock()
				claims++
				if lease.Request.PublicID != eligible {
					t.Errorf("wrong target %s", lease.Request.PublicID)
				}
			}
		}()
	}
	wg.Wait()
	if claims != 1 {
		t.Fatalf("expected exactly one eligible claim, got %d", claims)
	}
	var blocked int64
	if err := db.Model(&models.MediaSupplyActionRequest{}).Where("tenant_id=? AND state=?", "blocked", "queued").Count(&blocked).Error; err != nil || blocked != 16 {
		t.Fatalf("disabled requests must remain independently paused: %d %v", blocked, err)
	}
}

func TestSupplyExceptionalControlOnlyStopsAutonomousRecovery(t *testing.T) {
	if !supplyDisposableDatabaseConfigured() {
		t.Skip("guarded disposable PostgreSQL required")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.MediaSupplyControl{}); err != nil {
		t.Fatal(err)
	}
	stop := models.MediaSupplyControl{TenantID: "a", ControlKey: models.MediaSupplyControlExceptionalRecovery, ScopeType: "tenant", ScopeID: "all", DisabledAt: time.Now().UTC(), DisabledBy: "fixture", Reason: "fixture"}
	if err := db.Create(&stop).Error; err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := MayExecuteSupplyAction(db, "a", SupplyActionCancelUnstarted); err != nil || !allowed {
		t.Fatalf("manual recovery blocked: %v", err)
	}
	if allowed, control, err := MayExecuteAutonomousSupplyAction(db, "a", SupplyActionCancelUnstarted); err != nil || allowed || control != models.MediaSupplyControlExceptionalRecovery {
		t.Fatalf("autonomous recovery escaped stop: %v %s %v", allowed, control, err)
	}
	key, _ := SupplyActionControlKey(SupplyActionCancelUnstarted)
	stop.ID = 0
	stop.ControlKey = key
	if err := db.Create(&stop).Error; err != nil {
		t.Fatal(err)
	}
	if allowed, _, err := MayExecuteSupplyAction(db, "a", SupplyActionCancelUnstarted); err != nil || allowed {
		t.Fatal("exact action stop must also stop manual preview/claim")
	}
	if allowed, _, err := MayExecuteSupplyAction(db, "b", SupplyActionCancelUnstarted); err != nil || !allowed {
		t.Fatal("tenant A controls leaked to tenant B")
	}
}
