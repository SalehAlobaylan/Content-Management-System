package supply

import (
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"testing"
	"time"
)

func TestNewsPauseLeavesPodsDispatchableAndResumesExistingRequest(t *testing.T) {
	db := openSupplyFixtureDB(t)
	news := provisionSupplyFixture(t, db, "tenant-a", "news")
	media := provisionSupplyFixture(t, db, "tenant-a", "media")
	// Queue enough paused News requests to fill the dispatcher's candidate scan.
	for i := 0; i < maxDispatchCandidateScan+1; i++ {
		clone := news
		clone.ID = 0
		clone.PublicID = uuid.New()
		if err := db.Create(&clone).Error; err != nil {
			t.Fatal(err)
		}
		if _, _, err := CreateRequest(db, CreateRequestInput{Source: clone, Identity: supplyFixtureIdentity(clone), RequestedBy: "schedule", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1,"max_provider_calls":1,"max_bytes":1024}`))}); err != nil {
			t.Fatal(err)
		}
	}
	original, _, err := CreateRequest(db, CreateRequestInput{Source: news, Identity: supplyFixtureIdentity(news), RequestedBy: "schedule", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1,"max_provider_calls":1,"max_bytes":1024}`))})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.NewsCollectionControl{TenantID: "tenant-a", Paused: true, Version: 1, UpdatedAt: time.Now(), UpdatedBy: "fixture"}).Error; err != nil {
		t.Fatal(err)
	}
	identity := supplyFixtureIdentity(news)
	identity.Purpose = "manual"
	if _, _, err := CreateRequest(db, CreateRequestInput{Source: news, Identity: identity, RequestedBy: "manual", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1,"max_provider_calls":1,"max_bytes":1024}`))}); err == nil {
		t.Fatal("paused News manually admitted")
	}
	request, _, err := CreateRequest(db, CreateRequestInput{Source: media, Identity: supplyFixtureIdentity(media), RequestedBy: "schedule", EvidenceFingerprint: "fixture", Metadata: datatypes.JSON([]byte(`{"max_results":1,"max_provider_calls":1,"max_bytes":1024}`))})
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := ClaimNextDispatchableRequest(db, "fixture", time.Minute, time.Minute)
	if err != nil || !ok || claim.Request.PublicID != request.PublicID {
		t.Fatalf("paused News blocked Pods: %v %v %+v", err, ok, claim.Request)
	}
	if err := db.Model(&models.NewsCollectionControl{}).Where("tenant_id=?", "tenant-a").Update("paused", false).Error; err != nil {
		t.Fatal(err)
	}
	claim, ok, err = ClaimNextDispatchableRequest(db, "fixture", time.Minute, time.Minute)
	if err != nil || !ok || claim.Request.Lane != "news" {
		t.Fatalf("News did not resume: %v %v", err, ok)
	}
	var preserved models.SourceRunRequest
	if err := db.Where("public_id=?", original.PublicID).First(&preserved).Error; err != nil {
		t.Fatal(err)
	}
	var count int64
	db.Model(&models.SourceRunRequest{}).Where("tenant_id=? AND content_source_id=?", "tenant-a", news.PublicID).Count(&count)
	if count != 1 {
		t.Fatal("resume duplicated request")
	}
}

func TestNewsMissingPauseSchemaDoesNotBlockPods(t *testing.T) {
	db := openSupplyFixtureDB(t)
	provisionSupplyFixture(t, db, "tenant-a", "news")
	media := provisionSupplyFixture(t, db, "tenant-a", "media")
	if err := db.Migrator().DropTable(&models.NewsCollectionControl{}); err != nil {
		t.Fatal(err)
	}
	requests, err := AdmitDueSourceRuns(db, time.Now().UTC(), 4)
	if err != nil || len(requests) != 1 || requests[0].ContentSourceID != media.PublicID {
		t.Fatalf("missing News schema blocked Pods: %v %+v", err, requests)
	}
}
