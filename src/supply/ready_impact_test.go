package supply

import (
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
)

func TestInspectReadyOnlyImpactRequiresTenant(t *testing.T) {
	if _, err := InspectReadyOnlyImpact(nil, ""); err == nil {
		t.Fatal("inventory must not infer a tenant")
	}
}

func TestInspectReadyOnlyImpactIsReadOnlyAndTenantScoped(t *testing.T) {
	if !supplyDisposableDatabaseConfigured() {
		t.Skip("set guarded CMS_TEST_ADMIN_URL or CMS_TEST_DATABASE_URL to run supply DB tests")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.ContentItem{}); err != nil {
		t.Fatal(err)
	}
	clearSupplyTables(t, db, "content_items")
	media := "https://media.example.test/pod.mp3"
	rows := []models.ContentItem{
		{PublicID: uuid.New(), TenantID: "tenant-a", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusArchived, MediaURL: &media, IsFeedUnit: true, FeedVisibility: "visible"},
		{PublicID: uuid.New(), TenantID: "tenant-a", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusPending, MediaURL: &media, IsFeedUnit: true, FeedVisibility: "visible"},
		{PublicID: uuid.New(), TenantID: "tenant-a", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusReady, MediaURL: &media, IsFeedUnit: true, FeedVisibility: "visible"},
		{PublicID: uuid.New(), TenantID: "tenant-b", Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusArchived, MediaURL: &media, IsFeedUnit: true, FeedVisibility: "visible"},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := InspectReadyOnlyImpact(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, cohort := range report.Cohorts {
		counts[cohort.Key] = cohort.Count
	}
	if counts["archived_media"] != 1 || counts["held_pre_ready"] != 1 || counts["audio_only_ready"] != 1 {
		t.Fatalf("unexpected tenant-scoped impact report: %#v", counts)
	}
	var after int64
	if err := db.Model(&models.ContentItem{}).Where("tenant_id = ?", "tenant-a").Count(&after).Error; err != nil || after != 3 {
		t.Fatalf("inventory must not mutate content: count=%d err=%v", after, err)
	}
}
