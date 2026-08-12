package controllers

import (
	"os"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
)

func TestEnrichmentCoverageQueriesDoNotCrossTenantBoundary(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("set guarded CMS_TEST_ADMIN_URL or CMS_TEST_DATABASE_URL to run tenant DB tests")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.ContentItem{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DELETE FROM content_items").Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Exec("DELETE FROM content_items").Error })
	media := "https://media.example.test/item.mp3"
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		item := models.ContentItem{PublicID: uuid.New(), TenantID: tenant, Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusReady, MediaURL: &media, CreatedAt: time.Now().UTC().Add(-time.Hour)}
		if err := db.Create(&item).Error; err != nil {
			t.Fatal(err)
		}
	}
	var tenantACandidates int64
	if err := buildMissingQuery(db, "tenant-a", models.EnrichmentArtifactTranscript, "VIDEO,PODCAST", "READY").Count(&tenantACandidates).Error; err != nil {
		t.Fatal(err)
	}
	if tenantACandidates != 1 {
		t.Fatalf("tenant-a missing-artifact candidates=%d, want 1", tenantACandidates)
	}
	stats, err := computeEnrichmentStats(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalMedia != 1 {
		t.Fatalf("tenant-a enrichment stats leaked another tenant: %+v", stats)
	}
}

func TestTenantAutopilotAdvisoryLocksSerializeOnlySameTenant(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("set guarded CMS_TEST_ADMIN_URL or CMS_TEST_DATABASE_URL to run tenant DB tests")
	}
	db := testdb.Open(t)
	releaseA, acquiredA := tryAcquireTenantAutopilotLock(db, "enrichment-autopilot", "tenant-a")
	if !acquiredA {
		t.Fatal("first tenant lock was not acquired")
	}
	defer releaseA()
	if _, acquiredAgain := tryAcquireTenantAutopilotLock(db, "enrichment-autopilot", "tenant-a"); acquiredAgain {
		t.Fatal("same tenant/family lock was acquired twice")
	}
	releaseB, acquiredB := tryAcquireTenantAutopilotLock(db, "enrichment-autopilot", "tenant-b")
	if !acquiredB {
		t.Fatal("different tenant must remain independently eligible")
	}
	releaseB()
}
