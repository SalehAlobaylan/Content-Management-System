package supply

import (
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestSourceRunDBConcurrentRequestIdempotencyAndTenantIsolation(t *testing.T) {
	db := openSupplyFixtureDB(t)
	sourceA := provisionSupplyFixture(t, db, "tenant-a", "media")
	sourceB := provisionSupplyFixture(t, db, "tenant-b", "media")
	identity := supplyFixtureIdentity(sourceA)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	ids := make(chan uuid.UUID, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request, _, err := CreateRequest(db, CreateRequestInput{Source: sourceA, Identity: identity, RequestedBy: "schedule", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))})
			if err == nil {
				ids <- request.PublicID
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	var first uuid.UUID
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for id := range ids {
		if first == uuid.Nil {
			first = id
		} else if id != first {
			t.Fatalf("idempotent requests diverged: %s != %s", id, first)
		}
	}
	var count int64
	if err := db.Model(&models.SourceRunRequest{}).Where("tenant_id=?", "tenant-a").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("tenant-a request count=%d err=%v", count, err)
	}
	foreign := supplyFixtureIdentity(sourceB)
	foreign.ContentSourceID = sourceA.PublicID.String()
	if _, _, err := CreateRequest(db, CreateRequestInput{Source: sourceB, Identity: foreign, RequestedBy: "schedule", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))}); err == nil {
		t.Fatal("cross-tenant source identity was admitted")
	}
}

func TestSourceRunDBClaimRecoveryKeepsFenceAndJobIdentity(t *testing.T) {
	db := openSupplyFixtureDB(t)
	source := provisionSupplyFixture(t, db, "tenant-a", "media")
	request, _, err := CreateRequest(db, CreateRequestInput{Source: source, Identity: supplyFixtureIdentity(source), RequestedBy: "schedule", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := CreateAttemptAndRootUnit(db, "tenant-a", request.PublicID.String())
	if err != nil {
		t.Fatal(err)
	}
	first, err := ClaimAttempt(db, "tenant-a", lease.Attempt.PublicID.String(), "dispatcher-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.SourceRunAttempt{}).Where("public_id=?", first.PublicID).Update("dispatcher_lease_expires_at", time.Now().UTC().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	recovered, err := ReclaimExpiredDispatchClaim(db, "tenant-a", first.PublicID.String())
	if err != nil {
		t.Fatal(err)
	}
	second, err := ClaimAttempt(db, "tenant-a", recovered.PublicID.String(), "dispatcher-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.FenceToken != first.FenceToken || lease.RootExecutionUnit.AttemptFenceToken != second.FenceToken || lease.RootExecutionUnit.JobID == "" {
		t.Fatalf("claim recovery changed immutable execution identity: first=%+v second=%+v root=%+v", first, second, lease.RootExecutionUnit)
	}
}

func TestSourceRunDBManifestSealAndStaleLeaseFailClosed(t *testing.T) {
	db := openSupplyFixtureDB(t)
	source := provisionSupplyFixture(t, db, "tenant-a", "media")
	request, _, err := CreateRequest(db, CreateRequestInput{Source: source, Identity: supplyFixtureIdentity(source), RequestedBy: "schedule", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := CreateAttemptAndRootUnit(db, "tenant-a", request.PublicID.String())
	if err != nil {
		t.Fatal(err)
	}
	unitLease, err := AcquireUnitExecution(db, "tenant-a", lease.RootExecutionUnit.PublicID.String(), "dispatcher", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BeginUnitEffect(db, UnitLeaseInput{TenantID: "tenant-a", UnitID: unitLease.Unit.PublicID.String(), Owner: "dispatcher", LeaseToken: unitLease.LeaseToken.String()}); err != nil {
		t.Fatal(err)
	}
	if _, err := BeginUnitEffect(db, UnitLeaseInput{TenantID: "tenant-a", UnitID: unitLease.Unit.PublicID.String(), Owner: "dispatcher", LeaseToken: uuid.NewString()}); err == nil {
		t.Fatal("stale lease token started an effect")
	}
	page, created, err := AuthorizeChildUnit(db, ChildUnitInput{TenantID: "tenant-a", RequestID: request.PublicID.String(), AttemptID: lease.Attempt.PublicID.String(), ParentUnitID: lease.RootExecutionUnit.PublicID.String(), UnitType: "fetch_page", UnitKey: "fetch:one", PageID: "one"})
	if err != nil || !created {
		t.Fatalf("authorize page: created=%v err=%v", created, err)
	}
	if _, err := SealManifest(db, "tenant-a", request.PublicID.String()); err == nil {
		t.Fatal("manifest sealed before page terminal evidence")
	}
	if err := db.Model(&models.SourceRunExecutionUnit{}).Where("public_id=?", page.PublicID).Updates(map[string]any{"state": string(UnitSucceeded), "declared_child_count": 0, "declared_child_digest": emptyManifestDigest(t)}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := SealManifest(db, "tenant-a", request.PublicID.String()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := AuthorizeChildUnit(db, ChildUnitInput{TenantID: "tenant-a", RequestID: request.PublicID.String(), AttemptID: lease.Attempt.PublicID.String(), ParentUnitID: page.PublicID.String(), UnitType: "normalize_batch", UnitKey: "normalize:one:one", PageID: "one", BatchID: "one"}); err == nil {
		t.Fatal("sealed manifest admitted a late child")
	}
}

func TestSourceRunDBReceiptIsIdempotentAndStaleEvidenceCannotProject(t *testing.T) {
	db := openSupplyFixtureDB(t)
	source := provisionSupplyFixture(t, db, "tenant-a", "media")
	request, _, err := CreateRequest(db, CreateRequestInput{Source: source, Identity: supplyFixtureIdentity(source), RequestedBy: "schedule", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1}`))})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := CreateAttemptAndRootUnit(db, "tenant-a", request.PublicID.String())
	if err != nil {
		t.Fatal(err)
	}
	unit, err := AcquireUnitExecution(db, "tenant-a", attempt.RootExecutionUnit.PublicID.String(), "dispatcher", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	input := ReceiptInput{
		TenantID: "tenant-a", ProducerEventKey: "fixture-receipt", SourceRunRequestID: request.PublicID, SourceRunAttemptID: attempt.Attempt.PublicID,
		ExecutionUnitID: unit.Unit.PublicID, ContentSourceID: source.PublicID, UnitJobID: unit.Unit.JobID, AttemptFenceToken: attempt.Attempt.FenceToken,
		ExecutionLeaseToken: unit.LeaseToken, SchemaVersion: ContractVersion, Producer: "aggregation", Stage: string(ReceiptStageDispatch), EventType: string(ReceiptEventAccepted),
		Outcome: string(OutcomeNoChange), Sequence: 0, Payload: datatypes.JSON([]byte(`{}`)), PayloadDigest: "fixture-digest", ProducedAt: time.Now().UTC(),
	}
	first, created, err := RecordReceipt(db, input)
	if err != nil || !created {
		t.Fatalf("record receipt created=%v err=%v", created, err)
	}
	second, created, err := RecordReceipt(db, input)
	if err != nil || created || second.PublicID != first.PublicID {
		t.Fatalf("duplicate receipt must converge: first=%+v second=%+v created=%v err=%v", first, second, created, err)
	}
	input.ProducerEventKey = "fixture-stale-receipt"
	input.ExecutionLeaseToken = uuid.New()
	if _, _, err := RecordReceipt(db, input); err == nil {
		t.Fatal("stale receipt lease advanced the evidence ledger")
	}
	var projectionCount int64
	if err := db.Model(&models.SourceRunProjectionWork{}).Where("tenant_id=?", "tenant-a").Count(&projectionCount).Error; err != nil || projectionCount != 1 {
		t.Fatalf("stale or duplicate receipt created projection work: count=%d err=%v", projectionCount, err)
	}
}

func emptyManifestDigest(t *testing.T) string {
	t.Helper()
	digest, err := ManifestChildDigest([]string{})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func TestSourceRunDBSchedulerFairnessAndCutoverRemainFailClosed(t *testing.T) {
	db := openSupplyFixtureDB(t)
	provisionSupplyFixture(t, db, "tenant-a", "media")
	provisionSupplyFixture(t, db, "tenant-b", "media")
	admitted, err := AdmitDueSourceRuns(db, time.Now().UTC(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted) != 2 {
		t.Fatalf("scheduler must admit each due explicit tenant once, got %d", len(admitted))
	}
	seen := map[string]bool{}
	for _, request := range admitted {
		seen[request.TenantID] = true
	}
	if !seen["tenant-a"] || !seen["tenant-b"] {
		t.Fatalf("scheduler fairness lost a tenant: %#v", seen)
	}
	if err := db.Where("tenant_id=? AND lane=?", "tenant-b", "media").Delete(&models.SourceRunAdmissionCutover{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := RequireDurableAdmission(db, "tenant-b", "media"); err == nil {
		t.Fatal("missing tenant/lane provisioned durable admission")
	}
	if err := RequireLegacyAdmission(db, "tenant-a", "media"); err == nil {
		t.Fatal("legacy writer remained admitted after durable cutover")
	}
}
