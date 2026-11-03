package contentstage

import (
	"content-management-system/src/models"
	"content-management-system/src/podscontrol"
	"errors"
	"testing"
	"time"
)

func TestPodsAutomationDBPauseHoldsClaimsAndResumePreservesOtherControls(t *testing.T) {
	db := openContentStageFixtureDB(t)
	if err := db.Exec("CREATE UNIQUE INDEX fixture_stage_control_identity ON content_stage_controls(tenant_id,lane)").Error; err != nil {
		t.Fatal(err)
	}
	_, request := seedContentStageRequest(t, db, "tenant-pods", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now().Add(-time.Minute))
	original := podscontrol.Default("tenant-pods")
	original.OptionalMetadataEnabled = false
	if err := db.Create(&original).Error; err != nil {
		t.Fatal(err)
	}
	stop := false
	paused, err := UpdateControl(db, original.TenantID, "pods", "operator", &stop, &stop, nil, nil, "pause", podscontrol.Version(original))
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := ClaimNext(db, original.TenantID, "pods", "worker"); err != nil || found {
		t.Fatalf("paused work claimed: %v %v", found, err)
	}
	start := true
	if _, err := UpdateControl(db, original.TenantID, "pods", "operator", &start, &start, nil, nil, "stale resume", podscontrol.Version(original)); !errors.Is(err, podscontrol.ErrChanged) {
		t.Fatalf("stale resume accepted: %v", err)
	}
	resumed, err := UpdateControl(db, original.TenantID, "pods", "operator", &start, &start, nil, nil, "resume", podscontrol.Version(paused))
	if err != nil {
		t.Fatal(err)
	}
	if resumed.OptionalMetadataEnabled {
		t.Fatal("resume overwrote unrelated metadata control")
	}
	envelope, found, err := ClaimNext(db, original.TenantID, "pods", "worker")
	if err != nil || !found || envelope.RequestID != request.PublicID {
		t.Fatalf("original queued work not resumed: %+v %v %v", envelope, found, err)
	}
}

func TestPodsAutomationDBPauseRetainsFinalProofUntilRawPublicationResumes(t *testing.T) {
	db := openContentStageFixtureDB(t)
	item, request := seedContentStageRequest(t, db, "tenant-pods", models.ContentTypePodcast, models.ContentStagePodsMediaArtifacts, time.Now().Add(-time.Minute))
	if err := db.Model(&request).Update("state", models.ContentStageVerified).Error; err != nil {
		t.Fatal(err)
	}
	control := podscontrol.Default(item.TenantID)
	control.ExecutionEnabled = false
	if err := db.Create(&control).Error; err != nil {
		t.Fatal(err)
	}
	if err := ReduceReadiness(db, item.TenantID, item.PublicID, item.ProcessingGeneration); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&item, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if item.Status == models.ContentStatusReady {
		t.Fatal("raw episode published during pause")
	}
	if err := db.Model(&control).Update("execution_enabled", true).Error; err != nil {
		t.Fatal(err)
	}
	if err := reconcileHeldPodsPublications(db); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&item, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if item.Status != models.ContentStatusReady {
		t.Fatal("verified raw episode did not resume without replaying its effect")
	}
}

func TestPodsAutomationDBPauseDoesNotHoldNewsReadiness(t *testing.T) {
	db := openContentStageFixtureDB(t)
	item, request := seedContentStageRequest(t, db, "tenant-a", models.ContentTypeNews, models.ContentStageNewsTextEmbedding, time.Now().Add(-time.Minute))
	if err := db.Model(&request).Update("state", models.ContentStageVerified).Error; err != nil {
		t.Fatal(err)
	}
	control := podscontrol.Default(item.TenantID)
	control.ExecutionEnabled = false
	if err := db.Create(&control).Error; err != nil {
		t.Fatal(err)
	}
	if err := ReduceReadiness(db, item.TenantID, item.PublicID, item.ProcessingGeneration); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&item, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if item.Status != models.ContentStatusReady {
		t.Fatal("Pods pause held News publication")
	}
}
