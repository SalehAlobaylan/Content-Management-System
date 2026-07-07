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

func TestFeedIntegration(t *testing.T) {
	fmt.Println("📰 Starting Feed Integration Tests")
	clearWahbTables()
	seedTestContent()

	t.Run("Get ForYou Feed", func(t *testing.T) {
		fmt.Println("  📺 Testing ForYou feed...")
		req := httptest.NewRequest("GET", "/api/v1/feed/foryou?limit=5", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 ForYou feed response: %d\n", w.Code)
		if w.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var response struct {
			Cursor *string `json:"cursor"`
			Items  []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Title    string `json:"title"`
				MediaURL string `json:"media_url"`
			} `json:"items"`
		}

		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}

		fmt.Printf("  📊 Found %d items in ForYou feed\n", len(response.Items))
		if len(response.Items) == 0 {
			t.Fatalf("expected at least one item in ForYou feed")
		}

		// Verify items are VIDEO type
		for _, item := range response.Items {
			if item.Type != "VIDEO" && item.Type != "PODCAST" {
				t.Fatalf("expected VIDEO or PODCAST type, got %s", item.Type)
			}
		}
		fmt.Println("  ✅ ForYou feed test passed")
	})

	t.Run("Get News Feed", func(t *testing.T) {
		fmt.Println("  📰 Testing News feed...")
		req := httptest.NewRequest("GET", "/api/v1/feed/news?limit=3", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 News feed response: %d\n", w.Code)
		if w.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var response struct {
			Cursor *string `json:"cursor"`
			Slides []struct {
				SlideID  string `json:"slide_id"`
				Featured struct {
					StoryID string `json:"story_id"`
					LeadID  string `json:"lead_id"`
					Label   string `json:"label"`
					Members []struct {
						ID   string `json:"id"`
						Type string `json:"type"`
					} `json:"members"`
				} `json:"featured"`
				Related []struct {
					StoryID string `json:"story_id"`
					LeadID  string `json:"lead_id"`
				} `json:"related"`
			} `json:"slides"`
		}

		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}

		fmt.Printf("  📊 Found %d slides in News feed\n", len(response.Slides))
		if len(response.Slides) == 0 {
			t.Fatalf("expected at least one slide in News feed")
		}

		// Verify story-slide structure: a featured story with members.
		for _, slide := range response.Slides {
			if slide.Featured.StoryID == "" || slide.Featured.LeadID == "" {
				t.Fatalf("expected featured story to have story_id + lead_id")
			}
			if len(slide.Featured.Members) == 0 {
				t.Fatalf("expected featured story to have members")
			}
			if slide.Featured.Members[0].Type != "NEWS" {
				t.Fatalf("expected NEWS members, got %s", slide.Featured.Members[0].Type)
			}
		}
		fmt.Println("  ✅ News feed test passed")
	})

	t.Run("ForYou Cursor Pagination", func(t *testing.T) {
		fmt.Println("  📄 Testing cursor pagination...")

		// First request
		req1 := httptest.NewRequest("GET", "/api/v1/feed/foryou?limit=2", nil)
		w1 := httptest.NewRecorder()
		router.ServeHTTP(w1, req1)

		var resp1 struct {
			Cursor *string `json:"cursor"`
			Items  []struct {
				ID string `json:"id"`
			} `json:"items"`
		}
		json.Unmarshal(w1.Body.Bytes(), &resp1)

		if resp1.Cursor == nil {
			fmt.Println("  ⚠️  No next cursor (may have fewer items)")
			return
		}

		// Second request with cursor
		req2 := httptest.NewRequest("GET", "/api/v1/feed/foryou?limit=2&cursor="+*resp1.Cursor, nil)
		w2 := httptest.NewRecorder()
		router.ServeHTTP(w2, req2)

		if w2.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d", w2.Code)
		}

		var resp2 struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		}
		json.Unmarshal(w2.Body.Bytes(), &resp2)

		// Verify no overlap
		firstIDs := make(map[string]bool)
		for _, item := range resp1.Items {
			firstIDs[item.ID] = true
		}
		for _, item := range resp2.Items {
			if firstIDs[item.ID] {
				t.Fatalf("cursor pagination returned duplicate ID: %s", item.ID)
			}
		}
		fmt.Println("  ✅ Cursor pagination test passed")
	})
}

func TestContentIntegration(t *testing.T) {
	fmt.Println("📦 Starting Content Integration Tests")
	clearWahbTables()
	seedTestContent()

	t.Run("Get Content Item", func(t *testing.T) {
		fmt.Println("  📄 Testing get content item...")

		// Get an item first
		var item models.ContentItem
		if err := testDB.Where("type = ?", models.ContentTypeVideo).First(&item).Error; err != nil {
			t.Fatalf("failed to get test content: %v", err)
		}

		req := httptest.NewRequest("GET", "/api/v1/content/"+item.PublicID.String(), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		fmt.Printf("  📊 Get content response: %d\n", w.Code)
		if w.Code != http.StatusOK {
			t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var response utils.ResponseMessage
		json.Unmarshal(w.Body.Bytes(), &response)

		if response.Code != http.StatusOK {
			t.Fatalf("expected code 200, got %d", response.Code)
		}
		fmt.Println("  ✅ Get content item test passed")
	})

	t.Run("Get Content Item Not Found", func(t *testing.T) {
		fmt.Println("  ❓ Testing content not found...")
		req := httptest.NewRequest("GET", "/api/v1/content/"+uuid.New().String(), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("Expected 404, got %d", w.Code)
		}
		fmt.Println("  ✅ Content not found test passed")
	})
}

// Helper functions

func clearWahbTables() {
	fmt.Println("🗑️  Clearing Wahb test tables...")
	if testDB == nil {
		return
	}
	_ = testDB.Exec("DELETE FROM user_interactions").Error
	_ = testDB.Exec("DELETE FROM transcripts").Error
	_ = testDB.Exec("DELETE FROM content_items").Error
	_ = testDB.Exec("DELETE FROM topics").Error
	_ = testDB.Exec("DELETE FROM content_sources").Error
	fmt.Println("✅ Wahb tables cleared")
}

func seedTestContent() {
	fmt.Println("🌱 Seeding test content...")

	now := time.Now()

	// Create test videos
	for i := 0; i < 5; i++ {
		title := fmt.Sprintf("Test Video %d", i+1)
		mediaURL := fmt.Sprintf("https://test.cdn/video%d.mp4", i+1)
		thumbnailURL := fmt.Sprintf("https://test.cdn/video%d.jpg", i+1)
		author := "Test Author"
		duration := 300 + i*30
		playbackType := "mp4"
		hasVideo := true
		pubTime := now.Add(-time.Duration(i) * time.Hour)

		item := models.ContentItem{
			Type:           models.ContentTypeVideo,
			Source:         models.SourceTypePodcast,
			Status:         models.ContentStatusReady,
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
			PublishedAt:    &pubTime,
		}
		testDB.Create(&item)
	}

	// Create test articles
	for i := 0; i < 5; i++ {
		title := fmt.Sprintf("Test Article %d", i+1)
		excerpt := fmt.Sprintf("This is the excerpt for article %d", i+1)
		author := "Test Journalist"
		pubTime := now.Add(-time.Duration(i*2) * time.Hour)

		item := models.ContentItem{
			Type:        models.ContentTypeNews,
			Format:      func() *string { f := string(models.ContentFormatArticle); return &f }(),
			Source:      models.SourceTypeRSS,
			Status:      models.ContentStatusReady,
			Title:       &title,
			Excerpt:     &excerpt,
			Author:      &author,
			PublishedAt: &pubTime,
		}
		testDB.Create(&item)
	}

	// Create test tweets
	for i := 0; i < 10; i++ {
		text := fmt.Sprintf("Test tweet content %d #testing", i+1)
		author := fmt.Sprintf("@testuser%d", i+1)
		pubTime := now.Add(-time.Duration(i*30) * time.Minute)

		item := models.ContentItem{
			Type:        models.ContentTypeNews,
			Format:      func() *string { f := string(models.ContentFormatTweet); return &f }(),
			Source:      models.SourceTypeManual,
			Status:      models.ContentStatusReady,
			BodyText:    &text,
			Author:      &author,
			PublishedAt: &pubTime,
		}
		testDB.Create(&item)
	}

	// Phase 13: group the article fixtures under one story (topic) so the News
	// feed (story-slides) has a populated story to render.
	story := models.Topic{
		TenantID:     utils.GetDefaultTenantID(),
		Label:        "Test Story",
		ArticleCount: 5,
		Labeled:      true,
		LastMemberAt: &now,
	}
	testDB.Create(&story)
	testDB.Model(&models.ContentItem{}).
		Where("type = ? AND format = ?", models.ContentTypeNews, string(models.ContentFormatArticle)).
		Update("topic_id", story.PublicID)

	fmt.Println("✅ Test content seeded")
}
