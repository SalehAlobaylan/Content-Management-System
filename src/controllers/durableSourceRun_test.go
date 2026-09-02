package controllers

import (
	"encoding/json"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestDurableSourceRunIdentityIsStableAndManual(t *testing.T) {
	now := time.Date(2026, time.August, 31, 12, 0, 0, 123, time.UTC)
	feedURL := "https://www.youtube.com/channel/example"
	source := models.ContentSource{
		PublicID: uuid.New(), TenantID: "default", Name: "Example", Type: models.SourceTypeYouTube,
		Category: models.SourceCategoryMedia, FeedURL: &feedURL, APIConfig: datatypes.JSON([]byte(`{"channel_id":"example"}`)),
		SourceConfigVersion: 3,
	}

	first, err := durableSourceRunIdentity(source, now)
	if err != nil {
		t.Fatalf("durableSourceRunIdentity: %v", err)
	}
	second, err := durableSourceRunIdentity(source, now)
	if err != nil {
		t.Fatalf("durableSourceRunIdentity repeat: %v", err)
	}
	firstKey, err := first.IdempotencyKey()
	if err != nil {
		t.Fatalf("first key: %v", err)
	}
	secondKey, err := second.IdempotencyKey()
	if err != nil {
		t.Fatalf("second key: %v", err)
	}
	if firstKey != secondKey || first.Purpose != "manual" || first.Lane != models.SourceCategoryMedia {
		t.Fatalf("unexpected manual identity: first=%+v second=%+v", first, second)
	}
}

func TestDurableSourceRunMetadataUsesBoundedLaneDefaults(t *testing.T) {
	media, err := durableSourceRunMetadata(models.ContentSource{Category: models.SourceCategoryMedia})
	if err != nil {
		t.Fatalf("media metadata: %v", err)
	}
	news, err := durableSourceRunMetadata(models.ContentSource{Category: models.SourceCategoryNews})
	if err != nil {
		t.Fatalf("news metadata: %v", err)
	}

	var mediaValue, newsValue map[string]any
	if err := json.Unmarshal(media, &mediaValue); err != nil {
		t.Fatalf("decode media metadata: %v", err)
	}
	if err := json.Unmarshal(news, &newsValue); err != nil {
		t.Fatalf("decode news metadata: %v", err)
	}
	if mediaValue["max_provider_calls"] != float64(8) || mediaValue["min_duration_minutes"] != 4.5 {
		t.Fatalf("unexpected media limits: %v", mediaValue)
	}
	if newsValue["max_provider_calls"] != float64(20) {
		t.Fatalf("unexpected news limits: %v", newsValue)
	}
}

func TestDispatchClaimResponseResolvesSourceURLFromAPIConfig(t *testing.T) {
	sourceID := uuid.New()
	response := dispatchClaimResponse(supply.DispatchClaim{
		Request: models.SourceRunRequest{PublicID: uuid.New(), TenantID: "default", ContentSourceID: sourceID},
		Source: models.ContentSource{
			PublicID: sourceID, TenantID: "default", Name: "YouTube source", Type: models.SourceTypeYouTube,
			Category: models.SourceCategoryMedia, APIConfig: datatypes.JSON([]byte(`{"channel_id":"UC-example"}`)),
		},
	})
	source, ok := response["source"].(gin.H)
	if !ok {
		t.Fatalf("dispatch source payload has type %T", response["source"])
	}
	if source["url"] != "UC-example" {
		t.Fatalf("dispatch source url = %v, want API-config channel id", source["url"])
	}
}
