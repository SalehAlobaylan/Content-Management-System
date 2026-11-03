package supply

import (
	"content-management-system/src/podscontrol"
	"testing"
	"time"
)

func TestPodsAutomationDBPauseStopsSourcesWithoutStarvingNews(t *testing.T) {
	db := openSupplyFixtureDB(t)
	media := provisionSupplyFixture(t, db, "tenant-a", "media")
	news := provisionSupplyFixture(t, db, "tenant-a", "news")
	control := podscontrol.Default(media.TenantID)
	control.ExecutionEnabled = false
	if err := db.Create(&control).Error; err != nil {
		t.Fatal(err)
	}
	requests, err := AdmitDueSourceRuns(db, time.Now(), 1)
	if err != nil || len(requests) != 1 || requests[0].ContentSourceID != news.PublicID {
		t.Fatalf("Pods pause starved News: %+v %v", requests, err)
	}
	if err := db.Model(&control).Update("execution_enabled", true).Error; err != nil {
		t.Fatal(err)
	}
	requests, err = AdmitDueSourceRuns(db, time.Now(), 1)
	if err != nil || len(requests) != 1 || requests[0].ContentSourceID != media.PublicID {
		t.Fatalf("source selection did not survive pause/resume: %+v %v", requests, err)
	}
}
