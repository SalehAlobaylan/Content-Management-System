package integration

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

type characterizationForYouResponse struct {
	Cursor *string `json:"cursor"`
	Items  []struct {
		ID          string `json:"id"`
		Type        string `json:"type"`
		Title       string `json:"title"`
		PublishedAt string `json:"published_at"`
	} `json:"items"`
}

func TestForYouFeed_OnlyReadyItems(t *testing.T) {
	fmt.Println("📺 Characterizing ForYou READY filtering")
	clearWahbTables()

	readyOne := seedCharacterizationMediaItem(t, "Ready ForYou One", models.ContentStatusReady, 300, time.Now())
	readyTwo := seedCharacterizationMediaItem(t, "Ready ForYou Two", models.ContentStatusReady, 420, time.Now().Add(-time.Minute))
	processing := seedCharacterizationMediaItem(t, "Processing ForYou", models.ContentStatusProcessing, 360, time.Now().Add(-2*time.Minute))

	response := getCharacterizationForYouFeed(t, "/api/v1/feed/foryou?limit=10")
	ids := characterizationIDs(response.Items)

	if !ids[readyOne.PublicID.String()] || !ids[readyTwo.PublicID.String()] {
		t.Fatalf("expected both READY items in feed, got ids %#v", ids)
	}
	if ids[processing.PublicID.String()] {
		t.Fatalf("expected non-READY item %s to be absent", processing.PublicID)
	}
}

func TestForYouFeed_DurationBounds(t *testing.T) {
	fmt.Println("⏱️  Characterizing ForYou duration bounds")
	clearWahbTables()

	tooShort := seedCharacterizationMediaItem(t, "Too Short ForYou", models.ContentStatusReady, 200, time.Now())
	valid := seedCharacterizationMediaItem(t, "Valid ForYou", models.ContentStatusReady, 300, time.Now().Add(-time.Minute))
	tooLong := seedCharacterizationMediaItem(t, "Too Long ForYou", models.ContentStatusReady, 2500, time.Now().Add(-2*time.Minute))

	response := getCharacterizationForYouFeed(t, "/api/v1/feed/foryou?limit=10")
	ids := characterizationIDs(response.Items)

	if !ids[valid.PublicID.String()] {
		t.Fatalf("expected valid 300s item in feed, got ids %#v", ids)
	}
	if ids[tooShort.PublicID.String()] {
		t.Fatalf("expected 200s item to be excluded")
	}
	if ids[tooLong.PublicID.String()] {
		t.Fatalf("expected 2500s item to be excluded")
	}
}

func TestForYouFeed_CursorPaginationNoDuplicates(t *testing.T) {
	fmt.Println("📄 Characterizing ForYou cursor pagination")
	clearWahbTables()

	expected := make(map[string]bool)
	now := time.Now().Truncate(time.Second)
	for i := 0; i < 5; i++ {
		item := seedCharacterizationMediaItem(
			t,
			fmt.Sprintf("Paged ForYou %d", i+1),
			models.ContentStatusReady,
			300+i,
			now.Add(-time.Duration(i)*time.Minute),
		)
		expected[item.PublicID.String()] = true
	}

	seen := make(map[string]bool)
	var previousPublishedAt time.Time
	path := "/api/v1/feed/foryou?limit=2"
	for path != "" {
		response := getCharacterizationForYouFeed(t, path)
		for _, item := range response.Items {
			if seen[item.ID] {
				t.Fatalf("cursor pagination returned duplicate ID: %s", item.ID)
			}
			seen[item.ID] = true

			publishedAt, err := time.Parse(time.RFC3339, item.PublishedAt)
			if err != nil {
				t.Fatalf("parse published_at %q: %v", item.PublishedAt, err)
			}
			if !previousPublishedAt.IsZero() && publishedAt.After(previousPublishedAt) {
				t.Fatalf("items out of order: %s after %s", publishedAt, previousPublishedAt)
			}
			previousPublishedAt = publishedAt
		}

		if response.Cursor == nil {
			path = ""
		} else {
			path = "/api/v1/feed/foryou?limit=2&cursor=" + *response.Cursor
		}
	}

	if len(seen) != len(expected) {
		t.Fatalf("expected %d unique items, got %d: %#v", len(expected), len(seen), seen)
	}
	for id := range expected {
		if !seen[id] {
			t.Fatalf("expected seeded item %s to appear exactly once", id)
		}
	}
}

func TestNewsFeed_ReturnsSlides(t *testing.T) {
	fmt.Println("📰 Characterizing News story-slide response")
	clearWahbTables()
	seedCharacterizationNewsStory(t, "Characterization Story", 3, time.Now())

	req := httptest.NewRequest("GET", "/api/v1/feed/news?limit=3", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var response struct {
		Slides []struct {
			SlideID  string `json:"slide_id"`
			Featured struct {
				StoryID string `json:"story_id"`
				LeadID  string `json:"lead_id"`
				Title   string `json:"title"`
				Members []struct {
					ID string `json:"id"`
				} `json:"members"`
			} `json:"featured"`
		} `json:"slides"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(response.Slides) == 0 {
		t.Fatalf("expected at least one news slide")
	}
	if response.Slides[0].Featured.StoryID == "" || response.Slides[0].Featured.LeadID == "" {
		t.Fatalf("expected featured story to have story_id and lead_id")
	}
	if len(response.Slides[0].Featured.Members) == 0 {
		t.Fatalf("expected featured story to expose members")
	}
}

func seedCharacterizationMediaItem(t *testing.T, title string, status models.ContentStatus, duration int, publishedAt time.Time) models.ContentItem {
	t.Helper()

	mediaURL := fmt.Sprintf("https://test.cdn/%s.mp4", uuid.New().String())
	thumbnailURL := fmt.Sprintf("https://test.cdn/%s.jpg", uuid.New().String())
	playbackType := "mp4"
	hasVideo := true
	author := "Characterization Author"

	item := models.ContentItem{
		TenantID:       utils.GetDefaultTenantID(),
		Type:           models.ContentTypeVideo,
		Source:         models.SourceTypePodcast,
		Status:         status,
		Title:          &title,
		MediaURL:       &mediaURL,
		PlaybackURL:    &mediaURL,
		PlaybackType:   &playbackType,
		HasVideo:       &hasVideo,
		ThumbnailURL:   &thumbnailURL,
		Author:         &author,
		DurationSec:    &duration,
		IsFeedUnit:     true,
		FeedVisibility: "visible",
		PublishedAt:    &publishedAt,
	}
	if err := testDB.Create(&item).Error; err != nil {
		t.Fatalf("seed media item %q: %v", title, err)
	}
	return item
}

func seedCharacterizationNewsStory(t *testing.T, label string, count int, latest time.Time) {
	t.Helper()

	story := models.Topic{
		TenantID:     utils.GetDefaultTenantID(),
		Label:        label,
		ArticleCount: count,
		Labeled:      true,
		LastMemberAt: &latest,
	}
	if err := testDB.Create(&story).Error; err != nil {
		t.Fatalf("seed topic: %v", err)
	}

	format := string(models.ContentFormatArticle)
	sourceName := "Characterization News"
	for i := 0; i < count; i++ {
		title := fmt.Sprintf("%s Article %d", label, i+1)
		excerpt := fmt.Sprintf("Coverage item %d", i+1)
		author := fmt.Sprintf("Reporter %d", i+1)
		publishedAt := latest.Add(-time.Duration(i) * time.Minute)
		item := models.ContentItem{
			TenantID:    utils.GetDefaultTenantID(),
			Type:        models.ContentTypeNews,
			Format:      &format,
			Source:      models.SourceTypeRSS,
			Status:      models.ContentStatusReady,
			Title:       &title,
			Excerpt:     &excerpt,
			Author:      &author,
			SourceName:  &sourceName,
			TopicID:     &story.PublicID,
			PublishedAt: &publishedAt,
		}
		if err := testDB.Create(&item).Error; err != nil {
			t.Fatalf("seed news item %q: %v", title, err)
		}
	}
}

func getCharacterizationForYouFeed(t *testing.T, path string) characterizationForYouResponse {
	t.Helper()

	req := httptest.NewRequest("GET", path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var response characterizationForYouResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return response
}

func characterizationIDs(items []struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Title       string `json:"title"`
	PublishedAt string `json:"published_at"`
}) map[string]bool {
	ids := make(map[string]bool, len(items))
	for _, item := range items {
		ids[item.ID] = true
	}
	return ids
}
