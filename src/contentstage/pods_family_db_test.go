package contentstage

import (
	"content-management-system/src/models"
	"content-management-system/src/podsflow"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestPodsFamilySlotSpansTenantsAndParksOnlyAfterEffectsSettle(t *testing.T) {
	db := openContentStageFixtureDB(t)
	first, request := seedContentStageRequest(t, db, "tenant-first", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now().Add(-time.Minute))
	second, _ := seedContentStageRequest(t, db, "tenant-second", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now())
	acquire := func(item models.ContentItem) bool {
		t.Helper()
		var accepted bool
		if err := db.Transaction(func(tx *gorm.DB) error { var err error; accepted, err = podsflow.Acquire(tx, item); return err }); err != nil {
			t.Fatal(err)
		}
		return accepted
	}
	if !acquire(first) {
		t.Fatal("first family not admitted")
	}
	if acquire(second) {
		t.Fatal("second family overlapped pending first family")
	}
	if err := db.Model(&request).Update("state", models.ContentStageUncertain).Error; err != nil {
		t.Fatal(err)
	}
	if acquire(second) {
		t.Fatal("unknown effects released the global slot")
	}
	if err := db.Model(&request).Update("state", models.ContentStageFailed).Error; err != nil {
		t.Fatal(err)
	}
	if !acquire(second) {
		t.Fatal("quiescent failed family did not park")
	}
	var disposition podsflow.Disposition
	if err := db.Where("tenant_id=? AND root_content_item_id=?", first.TenantID, first.PublicID).First(&disposition).Error; err != nil {
		t.Fatal(err)
	}
	if disposition.Disposition != "parked_failed" {
		t.Fatalf("disposition=%s", disposition.Disposition)
	}
	if err := db.Transaction(func(tx *gorm.DB) error { return podsflow.Resume(tx, first) }); err != nil {
		t.Fatal(err)
	}
	if acquire(first) {
		t.Fatal("operator resume preempted another active family")
	}
}

func TestMediaApprovalWaitReleasesQuiescentFamilySlot(t *testing.T) {
	db := openContentStageFixtureDB(t)
	first, request := seedContentStageRequest(t, db, "approval-first", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now())
	second, _ := seedContentStageRequest(t, db, "approval-second", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now())
	acquire := func(item models.ContentItem) bool {
		t.Helper()
		var admitted bool
		if err := db.Transaction(func(tx *gorm.DB) error { var err error; admitted, err = podsflow.Acquire(tx, item); return err }); err != nil {
			t.Fatal(err)
		}
		return admitted
	}
	if !acquire(first) {
		t.Fatal("first family was not admitted")
	}
	if err := db.Model(&request).Update("state", models.ContentStageUncertain).Error; err != nil {
		t.Fatal(err)
	}
	if acquire(second) {
		t.Fatal("uncertain effects released the slot")
	}
	if err := db.Model(&request).Update("state", models.ContentStageAwaitingApproval).Error; err != nil {
		t.Fatal(err)
	}
	if !acquire(second) {
		t.Fatal("media approval wait retained the slot")
	}
	var disposition podsflow.Disposition
	if err := db.Where("tenant_id=? AND root_content_item_id=?", first.TenantID, first.PublicID).First(&disposition).Error; err != nil {
		t.Fatal(err)
	}
	if disposition.Disposition != "parked_review" || disposition.Reason != "media acquisition approval required" {
		t.Fatalf("unexpected disposition: %+v", disposition)
	}
}

func TestGenerationActivationWaitsForAllChildrenWithoutAnotherEpisode(t *testing.T) {
	db := openContentStageFixtureDB(t)
	if err := db.AutoMigrate(&models.Chapter{}); err != nil {
		t.Fatal(err)
	}
	root, _ := seedContentStageRequest(t, db, "activation", models.ContentTypePodcast, models.ContentStagePodsAtomization, time.Now())
	old := models.AtomizationGeneration{PublicID: uuid.New(), TenantID: root.TenantID, ParentContentItemID: root.PublicID, ProcessingGeneration: 1, GenerationNumber: 1, State: "active"}
	next := models.AtomizationGeneration{PublicID: uuid.New(), TenantID: root.TenantID, ParentContentItemID: root.PublicID, ProcessingGeneration: 1, GenerationNumber: 2, State: "verifying", ExpectedUnits: 2, CompletedUnits: 2, TerminalProof: jsonValue(map[string]any{"artifacts_verified": true})}
	for _, gen := range []*models.AtomizationGeneration{&old, &next} {
		if err := db.Create(gen).Error; err != nil {
			t.Fatal(err)
		}
	}
	duration := 600
	makeChild := func(gen models.AtomizationGeneration, state models.ContentStatus, visibility string) models.ContentItem {
		t.Helper()
		child := models.ContentItem{PublicID: uuid.New(), TenantID: root.TenantID, ParentContentItemID: &root.PublicID, ProcessingGeneration: 1, Type: models.ContentTypePodcast, Source: models.SourceTypeRSS, Status: state, IsFeedUnit: true, FeedVisibility: visibility, DurationSec: &duration, Metadata: jsonValue(map[string]any{"atomization_generation_id": gen.PublicID.String()})}
		if err := db.Create(&child).Error; err != nil {
			t.Fatal(err)
		}
		return child
	}
	previous := makeChild(old, models.ContentStatusReady, "visible")
	first := makeChild(next, models.ContentStatusReady, "embedding_pending")
	second := makeChild(next, models.ContentStatusProcessing, "embedding_pending")
	for index, child := range []models.ContentItem{first, second} {
		unit := models.AtomizationChapterUnit{PublicID: uuid.New(), TenantID: root.TenantID, GenerationID: next.PublicID, UnitIndex: index, State: "verified", CandidateContentItemID: &child.PublicID}
		if err := db.Create(&unit).Error; err != nil {
			t.Fatal(err)
		}
	}
	assertVisibility := func(id uuid.UUID, want string) {
		t.Helper()
		var child models.ContentItem
		if err := db.Where("public_id=?", id).First(&child).Error; err != nil {
			t.Fatal(err)
		}
		if child.FeedVisibility != want {
			t.Fatalf("%s visibility=%s want=%s", id, child.FeedVisibility, want)
		}
	}
	if err := podsflow.ReconcileReadyGenerations(db); err != nil {
		t.Fatal(err)
	}
	assertVisibility(previous.PublicID, "visible")
	assertVisibility(first.PublicID, "embedding_pending")
	if err := db.Model(&second).Update("status", models.ContentStatusReady).Error; err != nil {
		t.Fatal(err)
	}
	if err := podsflow.ReconcileReadyGenerations(db); err != nil {
		t.Fatal(err)
	}
	assertVisibility(previous.PublicID, "hidden")
	assertVisibility(first.PublicID, "visible")
	assertVisibility(second.PublicID, "visible")
	var activated models.AtomizationGeneration
	if err := db.Where("public_id=?", next.PublicID).First(&activated).Error; err != nil {
		t.Fatal(err)
	}
	if activated.State != "active" {
		t.Fatalf("generation remained %s", activated.State)
	}
}

func TestActiveFamilyCandidatePrecedesOlderWaitingEpisode(t *testing.T) {
	db := openContentStageFixtureDB(t)
	_, _ = seedContentStageRequest(t, db, "tenant", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now().Add(-time.Hour))
	active, request := seedContentStageRequest(t, db, "tenant", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now())
	// This case starts with an existing owner, rather than bypassing the
	// global FIFO admission rule to create one.
	if err := db.Model(&podsflow.Slot{}).Where("singleton=true").Updates(map[string]any{"tenant_id": active.TenantID, "root_content_item_id": active.PublicID, "processing_generation": active.ProcessingGeneration}).Error; err != nil {
		t.Fatal(err)
	}
	filter := claimCandidateFilter{tenantID: active.TenantID, lane: request.Lane, expectedOwner: request.Owner, now: time.Now()}
	var candidates []models.ContentStageRequest
	if err := db.Transaction(func(tx *gorm.DB) error {
		return lockedClaimCandidateScope(tx, filter, 1).Find(&candidates).Error
	}); err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].PublicID != request.PublicID {
		t.Fatal("waiting episode hid the active family behind the candidate limit")
	}
}

func TestDownloadCannotOvertakeHigherPriorityAtomization(t *testing.T) {
	db := openContentStageFixtureDB(t)
	root, media := seedContentStageRequest(t, db, "atomization", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now().Add(-time.Hour))
	download, _ := seedContentStageRequest(t, db, "download", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now().Add(-2*time.Hour))
	// Legacy zero-dependency rows used JSON null, not an empty array.
	if err := db.Model(&models.ContentStageRequest{}).Where("content_item_id=?", download.PublicID).Update("dependency_manifest", jsonValue(nil)).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&media).Update("state", models.ContentStageVerified).Error; err != nil {
		t.Fatal(err)
	}
	atom := media
	atom.ID = 0
	atom.PublicID = uuid.New()
	atom.IdempotencyKey = uuid.NewString()
	atom.Stage = models.ContentStagePodsAtomization
	atom.State = models.ContentStageQueued
	atom.Priority = 100
	if err := db.Create(&atom).Error; err != nil {
		t.Fatal(err)
	}
	acquire := func(item models.ContentItem) bool {
		t.Helper()
		var admitted bool
		if err := db.Transaction(func(tx *gorm.DB) error { var err error; admitted, err = podsflow.Acquire(tx, item); return err }); err != nil {
			t.Fatal(err)
		}
		return admitted
	}
	if acquire(download) {
		t.Fatal("download dispatcher bypassed global priority")
	}
	if !acquire(root) {
		t.Fatal("atomization winner could not acquire slot")
	}
	if err := db.Model(&atom).Update("state", models.ContentStageUncertain).Error; err != nil {
		t.Fatal(err)
	}
	if acquire(download) {
		t.Fatal("uncertain current owner was preempted")
	}
	if err := db.Model(&atom).Updates(map[string]any{"state": models.ContentStageDeferred, "not_before_at": time.Now().Add(time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
	if !acquire(download) {
		t.Fatal("quiescent deferred owner monopolized the slot")
	}
}
