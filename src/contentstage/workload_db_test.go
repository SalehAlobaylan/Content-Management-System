package contentstage

import (
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/workload"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func configureStageGovernor(t *testing.T, db *gorm.DB, mode string) {
	t.Helper()
	if err := db.AutoMigrate(&models.WorkloadGovernorPolicy{}, &models.WorkloadPermit{}, &models.WorkloadDecision{}, &models.WorkloadCapacitySnapshot{}); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"workload_decisions", "workload_permits", "workload_capacity_snapshots", "workload_governor_policies"} {
		if err := db.Exec("DELETE FROM " + table).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, table := range []string{"workload_decisions", "workload_permits", "workload_capacity_snapshots", "workload_governor_policies"} {
			if db.Migrator().HasTable(table) {
				if err := db.Exec("DELETE FROM " + table).Error; err != nil {
					t.Errorf("clear governor fixture %s: %v", table, err)
				}
			}
		}
	})
	policy, err := workload.GetPolicy(db)
	if err != nil {
		t.Fatal(err)
	}
	policy.Mode, policy.EnforcedClasses = mode, jsonValue([]string{"text_embedding"})
	limits := map[string]workload.ResourceLimit{}
	for _, resource := range workload.Registry().Resources {
		limits[resource.Key] = resource.DefaultLimit
	}
	limits["enrichment_svc"] = workload.ResourceLimit{Capacity: 1}
	policy.ResourceLimits = jsonValue(limits)
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.WorkloadCapacitySnapshot{Resource: "enrichment_svc", State: "normal", Available: true, Healthy: true, ObservedAt: time.Now().UTC(), Evidence: jsonValue(map[string]any{})}).Error; err != nil {
		t.Fatal(err)
	}
}

func reserveOtherStage(t *testing.T, db *gorm.DB) workload.Decision {
	t.Helper()
	r := workload.Request{TenantID: "other-tenant", System: "content_stages", Class: "text_embedding", Units: 1, RunID: uuid.NewString(), Origin: workload.OriginAutomation, OwnerKind: workloadOwner, OwnerID: uuid.NewString()}
	d := workload.RequestPermit(db, r)
	if !d.Allowed() || d.Unavailable || d.PermitID == "" {
		t.Fatalf("fixture reservation failed: %#v", d)
	}
	if err := workload.BeginOwner(db, r.TenantID, r.OwnerKind, r.OwnerID); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestStageGovernorEnforceDefersBeforeAllocatingAttempt(t *testing.T) {
	db := openContentStageFixtureDB(t)
	configureStageGovernor(t, db, workload.ModeEnforce)
	reserveOtherStage(t, db)
	_, request := seedContentStageRequest(t, db, "governed-tenant", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, time.Now().Add(-time.Minute))
	if _, found, err := ClaimNext(db, request.TenantID, request.Lane, "governor-fixture"); err != nil || found {
		t.Fatalf("saturated capacity created a native claim: found=%v err=%v", found, err)
	}
	var actual models.ContentStageRequest
	if err := db.Where("public_id=?", request.PublicID).First(&actual).Error; err != nil {
		t.Fatal(err)
	}
	var attempts int64
	if err := db.Model(&models.ContentStageAttempt{}).Where("request_id=?", request.PublicID).Count(&attempts).Error; err != nil {
		t.Fatal(err)
	}
	if actual.State != models.ContentStageDeferred || actual.NotBeforeAt == nil || attempts != 0 || actual.ClaimEpoch != 0 {
		t.Fatalf("capacity wait burned native authority: state=%s attempts=%d epoch=%d retry=%v", actual.State, attempts, actual.ClaimEpoch, actual.NotBeforeAt)
	}
}

func TestStageGovernorConcurrentTenantClaimsCannotOverallocate(t *testing.T) {
	db := openContentStageFixtureDB(t)
	configureStageGovernor(t, db, workload.ModeEnforce)
	for _, tenant := range []string{"race-tenant-a", "race-tenant-b"} {
		seedContentStageRequest(t, db, tenant, models.ContentTypeNews, models.ContentStageNewsTextEmbedding, time.Now().Add(-time.Minute))
	}
	type result struct {
		found bool
		err   error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, tenant := range []string{"race-tenant-a", "race-tenant-b"} {
		workers.Add(1)
		go func(tenant string) {
			defer workers.Done()
			<-start
			_, found, err := ClaimNext(db, tenant, models.ContentStageLaneNews, "governor-race-fixture")
			results <- result{found, err}
		}(tenant)
	}
	close(start)
	workers.Wait()
	close(results)
	claimed := 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.found {
			claimed++
		}
	}
	var attempts int64
	if err := db.Model(&models.ContentStageAttempt{}).Where("tenant_id IN ?", []string{"race-tenant-a", "race-tenant-b"}).Count(&attempts).Error; err != nil {
		t.Fatal(err)
	}
	if claimed != 1 || attempts != 1 {
		t.Fatalf("one shared initiation slot admitted %d tenants and %d attempts", claimed, attempts)
	}
}

func TestStageGovernorObserveKeepsNativeAdmission(t *testing.T) {
	db := openContentStageFixtureDB(t)
	configureStageGovernor(t, db, workload.ModeObserve)
	reserveOtherStage(t, db)
	_, request := seedContentStageRequest(t, db, "observed-tenant", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, time.Now().Add(-time.Minute))
	envelope, found, err := ClaimNext(db, request.TenantID, request.Lane, "governor-fixture")
	if err != nil || !found {
		t.Fatalf("Observe changed native capacity admission: found=%v err=%v", found, err)
	}
	assertPersistedClaim(t, db, envelope, request)
	var permit models.WorkloadPermit
	if err := db.Where("tenant_id=? AND owner_id=?", request.TenantID, request.PublicID.String()).First(&permit).Error; err != nil {
		t.Fatal(err)
	}
	if permit.Verdict != "admit" || permit.WouldVerdict != "defer" {
		t.Fatalf("Observe lost counterfactual evidence: %#v", permit)
	}
}

func TestStageGovernorNeverReleasesUnprovenStartedEffect(t *testing.T) {
	db := openContentStageFixtureDB(t)
	configureStageGovernor(t, db, workload.ModeObserve)
	_, request := seedContentStageRequest(t, db, "uncertain-tenant", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, time.Now().Add(-time.Minute))
	decision := workload.RequestPermit(db, stageWorkloadRequest(request))
	if decision.PermitID == "" || !decision.Allowed() || decision.Unavailable {
		t.Fatalf("fixture permit unavailable: %#v", decision)
	}
	request.ClaimEpoch++ // the native claim now owns the acquired permit key
	now := time.Now().UTC()
	attempt := models.ContentStageAttempt{PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID, AttemptNumber: 1, Lane: request.Lane, Stage: request.Stage, Owner: request.Owner, State: models.ContentStageUncertain, ClaimToken: uuid.New(), FenceToken: uuid.New(), LeaseEpoch: 1, DeterministicJobID: uuid.NewString(), EffectStartedAt: &now, LeaseExpiresAt: now, HeartbeatAt: now}
	if err := db.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	if err := workload.BeginOwner(db, request.TenantID, workloadOwner, request.PublicID.String()); err != nil {
		t.Fatal(err)
	}
	settleUnstartedStageWorkload(db, request, "cancelled_without_effect")
	var permit models.WorkloadPermit
	if err := db.Where("id=?", decision.PermitID).First(&permit).Error; err != nil {
		t.Fatal(err)
	}
	if permit.ReleasedAt != nil || permit.State != workload.StateRunning {
		t.Fatalf("uncertain native effect released capacity: %#v", permit)
	}
}

func TestStageGovernorStorageFailureDoesNotAbortNativeClaim(t *testing.T) {
	db := openContentStageFixtureDB(t)
	configureStageGovernor(t, db, workload.ModeObserve)
	if err := db.Migrator().DropTable(&models.WorkloadPermit{}); err != nil {
		t.Fatal(err)
	}
	_, request := seedContentStageRequest(t, db, "failopen-tenant", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, time.Now().Add(-time.Minute))
	envelope, found, err := ClaimNext(db, request.TenantID, request.Lane, "governor-fixture")
	if err != nil || !found {
		t.Fatalf("advisory storage failure aborted native owner transaction: found=%v err=%v", found, err)
	}
	assertPersistedClaim(t, db, envelope, request)
}

func TestStageGovernorRetrySettlementPreservesEarlierUncertainty(t *testing.T) {
	db := openContentStageFixtureDB(t)
	configureStageGovernor(t, db, workload.ModeObserve)
	_, request := seedContentStageRequest(t, db, "retry-tenant", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, time.Now().Add(-time.Minute))
	old := workload.RequestPermit(db, stageWorkloadRequest(request))
	request.ClaimEpoch++
	if err := workload.BeginRequest(db, activeStageWorkloadRequest(request)); err != nil {
		t.Fatal(err)
	}
	if err := workload.MarkRequestUncertain(db, activeStageWorkloadRequest(request), "lost_remote_acknowledgement"); err != nil {
		t.Fatal(err)
	}
	current := workload.RequestPermit(db, stageWorkloadRequest(request))
	request.ClaimEpoch++
	if old.PermitID == "" || current.PermitID == "" || old.PermitID == current.PermitID || current.Unavailable {
		t.Fatalf("native retry did not receive a distinct permit: old=%#v current=%#v", old, current)
	}
	if err := workload.BeginRequest(db, activeStageWorkloadRequest(request)); err != nil {
		t.Fatalf("older uncertain attempt blocked an independently admitted retry: %v", err)
	}
	settleStageWorkload(db, request, 1, "exact_retry_artifact_receipt_verified")
	var oldRow, currentRow models.WorkloadPermit
	if err := db.Where("id=?", old.PermitID).First(&oldRow).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("id=?", current.PermitID).First(&currentRow).Error; err != nil {
		t.Fatal(err)
	}
	if oldRow.State != workload.StateUncertain || oldRow.ReleasedAt != nil || currentRow.State != "released" {
		t.Fatalf("retry settlement erased unrelated uncertainty: old=%s current=%s", oldRow.State, currentRow.State)
	}
}

func TestStageGovernorVerifiedRequestNeedsItsExactAttemptReceipt(t *testing.T) {
	db := openContentStageFixtureDB(t)
	configureStageGovernor(t, db, workload.ModeObserve)
	_, request := seedContentStageRequest(t, db, "receipt-tenant", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, time.Now().Add(-time.Minute))
	decision := workload.RequestPermit(db, stageWorkloadRequest(request))
	request.ClaimEpoch++
	if decision.PermitID == "" || decision.Unavailable {
		t.Fatalf("fixture permit unavailable: %#v", decision)
	}
	if err := workload.BeginRequest(db, activeStageWorkloadRequest(request)); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	attempt := models.ContentStageAttempt{PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID, AttemptNumber: 1, Lane: request.Lane, Stage: request.Stage, Owner: request.Owner, InputFingerprint: request.InputFingerprint, State: models.ContentStageVerified, ClaimToken: uuid.New(), FenceToken: uuid.New(), LeaseEpoch: 1, DeterministicJobID: uuid.NewString(), EffectStartedAt: &now, LeaseExpiresAt: now, HeartbeatAt: now}
	if err := db.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	request.State = models.ContentStageVerified
	settleVerifiedStageWorkload(db, request)
	var before models.WorkloadPermit
	if err := db.Where("id=?", decision.PermitID).First(&before).Error; err != nil {
		t.Fatal(err)
	}
	if before.ReleasedAt != nil {
		t.Fatal("request/artifact completion alone released an unproven attempted effect")
	}
	receipt := models.ContentStageReceipt{PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID, AttemptID: attempt.PublicID, ContentItemID: request.ContentItemID, ProcessingGeneration: request.ProcessingGeneration, Lane: request.Lane, Stage: request.Stage, Owner: models.ContentStageOwnerEnrichment, ProducerEventID: uuid.New(), FenceToken: attempt.FenceToken, InputFingerprint: request.InputFingerprint, Outcome: "persisted", PayloadDigest: "receipt-fixture", Payload: jsonValue(map[string]any{}), ObservedAt: now}
	if err := db.Create(&receipt).Error; err != nil {
		t.Fatal(err)
	}
	settleVerifiedStageWorkload(db, request)
	var after models.WorkloadPermit
	if err := db.Where("id=?", decision.PermitID).First(&after).Error; err != nil {
		t.Fatal(err)
	}
	if after.State != "released" || after.ActualUnits == nil || *after.ActualUnits != 1 {
		t.Fatalf("exact persisted attempt receipt did not release its capacity: %#v", after)
	}
}
