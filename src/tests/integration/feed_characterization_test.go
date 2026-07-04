package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
)

// Characterization tests for the public feed endpoints (advisor plan 003).
// These pin the CURRENT observable behavior of the For You + News feeds so a
// later change to feed assembly, ranking, or eligibility fails loudly. They
// lock the hard constraints from docs/PRD.md:
//   - only READY items appear in public feeds (ARCHIVED is deliberately also
//     eligible for For You — cold-tier items stay playable; that is pinned
//     behavior, not a bug),
//   - For You feed units must be 270..2400 seconds,
//   - cursor pagination returns each item exactly once, in order.
//
// NOTE: with no ranking_configs row seeded, loadTenantConfig falls back to
// DefaultRankingConfig which has IsActive=true — so these tests exercise the
// RANKED path of GetForYouFeed (ScoreItems + score-ordered pagination), which
// is what production runs. Items are seeded identical except published_at and
// share one source so freshness dominates the score and the expected order is
// published_at DESC (the diversity pass is a no-op for a single source).

// seedForYouMediaItem creates one fully For You-eligible VIDEO feed unit:
// READY, valid duration, mp4 media URL, thumbnail, is_feed_unit/feed_visibility
// left at their model defaults (true/'visible').
func seedForYouMediaItem(t *testing.T, title string, durationSec int, status models.ContentStatus, publishedAt time.Time) models.ContentItem {
	t.Helper()
	mediaURL := fmt.Sprintf("https://test.cdn/%s.mp4", title)
	thumbURL := fmt.Sprintf("https://test.cdn/%s.jpg", title)
	author := "Characterization Author"
	sourceName := "characterization-source"
	item := models.ContentItem{
		Type:         models.ContentTypeVideo,
		Source:       models.SourceTypeYouTube,
		SourceName:   &sourceName,
		Status:       status,
		Title:        &title,
		MediaURL:     &mediaURL,
		ThumbnailURL: &thumbURL,
		Author:       &author,
		DurationSec:  &durationSec,
		PublishedAt:  &publishedAt,
	}
	if err := testDB.Create(&item).Error; err != nil {
		t.Fatalf("seed %q: %v", title, err)
	}
	return item
}

type forYouCharResponse struct {
	Cursor *string `json:"cursor"`
	Items  []struct {
		ID          string    `json:"id"`
		Type        string    `json:"type"`
		Title       string    `json:"title"`
		DurationSec int       `json:"duration_sec"`
		PublishedAt time.Time `json:"published_at"`
	} `json:"items"`
}

func getForYou(t *testing.T, query string) forYouCharResponse {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/v1/feed/foryou"+query, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /feed/foryou%s: expected 200, got %d: %s", query, w.Code, w.Body.String())
	}
	var resp forYouCharResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal foryou response: %v", err)
	}
	return resp
}

func TestForYouFeed_OnlyReadyItems(t *testing.T) {
	fmt.Println("🔒 Characterization: For You serves READY items only")
	clearWahbTables()

	now := time.Now()
	ready1 := seedForYouMediaItem(t, "char-ready-1", 300, models.ContentStatusReady, now.Add(-1*time.Hour))
	ready2 := seedForYouMediaItem(t, "char-ready-2", 600, models.ContentStatusReady, now.Add(-2*time.Hour))
	processing := seedForYouMediaItem(t, "char-processing", 300, models.ContentStatusProcessing, now.Add(-30*time.Minute))
	pending := seedForYouMediaItem(t, "char-pending", 300, models.ContentStatusPending, now.Add(-40*time.Minute))
	failed := seedForYouMediaItem(t, "char-failed", 300, models.ContentStatusFailed, now.Add(-50*time.Minute))

	resp := getForYou(t, "?limit=20")

	got := make(map[string]bool, len(resp.Items))
	for _, it := range resp.Items {
		got[it.ID] = true
	}
	if !got[ready1.PublicID.String()] || !got[ready2.PublicID.String()] {
		t.Fatalf("expected both READY items in feed, got %v", got)
	}
	for _, excluded := range []models.ContentItem{processing, pending, failed} {
		if got[excluded.PublicID.String()] {
			t.Fatalf("non-READY item (status=%s) leaked into For You feed: %s", excluded.Status, excluded.PublicID)
		}
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected exactly 2 items, got %d", len(resp.Items))
	}
	fmt.Println("  ✅ READY-only invariant holds")
}

func TestForYouFeed_DurationBounds(t *testing.T) {
	fmt.Println("🔒 Characterization: For You enforces the 270..2400s duration window")
	clearWahbTables()

	now := time.Now()
	tooShort := seedForYouMediaItem(t, "char-short-200s", 200, models.ContentStatusReady, now.Add(-1*time.Hour))
	valid := seedForYouMediaItem(t, "char-valid-300s", 300, models.ContentStatusReady, now.Add(-2*time.Hour))
	floorEdge := seedForYouMediaItem(t, "char-floor-270s", 270, models.ContentStatusReady, now.Add(-3*time.Hour))
	ceilEdge := seedForYouMediaItem(t, "char-ceil-2400s", 2400, models.ContentStatusReady, now.Add(-4*time.Hour))
	tooLong := seedForYouMediaItem(t, "char-long-2500s", 2500, models.ContentStatusReady, now.Add(-5*time.Hour))

	resp := getForYou(t, "?limit=20")

	got := make(map[string]bool, len(resp.Items))
	for _, it := range resp.Items {
		got[it.ID] = true
		if it.DurationSec < 270 || it.DurationSec > 2400 {
			t.Fatalf("feed returned item outside 270..2400s window: %s (%ds)", it.ID, it.DurationSec)
		}
	}
	if got[tooShort.PublicID.String()] {
		t.Fatalf("sub-270s item leaked into For You feed")
	}
	if got[tooLong.PublicID.String()] {
		t.Fatalf("over-2400s un-atomized parent leaked into For You feed")
	}
	for _, included := range []models.ContentItem{valid, floorEdge, ceilEdge} {
		if !got[included.PublicID.String()] {
			t.Fatalf("valid-duration item missing from feed: %s (%dsec)", *included.Title, *included.DurationSec)
		}
	}
	fmt.Println("  ✅ Duration-window invariant holds (270 and 2400 inclusive)")
}

func TestForYouFeed_CursorPaginationNoDuplicates(t *testing.T) {
	fmt.Println("🔒 Characterization: For You cursor pagination — no duplicates, no skips, stable order")
	clearWahbTables()

	now := time.Now()
	seeded := make(map[string]bool, 5)
	for i := 0; i < 5; i++ {
		item := seedForYouMediaItem(t,
			fmt.Sprintf("char-page-%d", i+1),
			300+i*60,
			models.ContentStatusReady,
			now.Add(-time.Duration(i+1)*time.Hour),
		)
		seeded[item.PublicID.String()] = true
	}

	var (
		collected []string
		seen      = make(map[string]bool)
		lastTS    time.Time
		cursor    *string
	)
	for page := 0; page < 10; page++ {
		query := "?limit=2"
		if cursor != nil {
			query += "&cursor=" + *cursor
		}
		resp := getForYou(t, query)
		for _, it := range resp.Items {
			if seen[it.ID] {
				t.Fatalf("duplicate item across pages: %s (page %d)", it.ID, page+1)
			}
			seen[it.ID] = true
			collected = append(collected, it.ID)
			if !lastTS.IsZero() && it.PublishedAt.After(lastTS) {
				t.Fatalf("published_at order violated: %s (%s) after previous (%s)", it.ID, it.PublishedAt, lastTS)
			}
			lastTS = it.PublishedAt
		}
		if resp.Cursor == nil {
			break
		}
		cursor = resp.Cursor
	}

	if len(collected) != 5 {
		t.Fatalf("expected exactly the 5 seeded items across pages, got %d: %v", len(collected), collected)
	}
	for id := range seeded {
		if !seen[id] {
			t.Fatalf("seeded item never returned by pagination: %s", id)
		}
	}
	fmt.Println("  ✅ Pagination invariant holds (5 items, once each, non-increasing published_at)")
}

func TestNewsFeed_ReturnsSlides(t *testing.T) {
	fmt.Println("🔒 Characterization: News feed returns story-slides with a featured story")
	clearWahbTables()

	// Seed a story: 4 NEWS articles grouped under one labeled topic (the same
	// shape seedTestContent uses — enough for at least one slide).
	now := time.Now()
	for i := 0; i < 4; i++ {
		title := fmt.Sprintf("Char News %d", i+1)
		excerpt := fmt.Sprintf("Characterization article %d body", i+1)
		author := "Char Journalist"
		format := string(models.ContentFormatArticle)
		pubTime := now.Add(-time.Duration(i) * time.Hour)
		item := models.ContentItem{
			Type:        models.ContentTypeNews,
			Format:      &format,
			Source:      models.SourceTypeRSS,
			Status:      models.ContentStatusReady,
			Title:       &title,
			Excerpt:     &excerpt,
			Author:      &author,
			PublishedAt: &pubTime,
		}
		if err := testDB.Create(&item).Error; err != nil {
			t.Fatalf("seed news item: %v", err)
		}
	}
	story := models.Topic{
		TenantID:     utils.GetDefaultTenantID(),
		Label:        "Char Test Story",
		ArticleCount: 4,
		Labeled:      true,
		LastMemberAt: &now,
	}
	if err := testDB.Create(&story).Error; err != nil {
		t.Fatalf("seed story topic: %v", err)
	}
	testDB.Model(&models.ContentItem{}).
		Where("type = ? AND title LIKE ?", models.ContentTypeNews, "Char News %").
		Update("topic_id", story.PublicID)

	req := httptest.NewRequest("GET", "/api/v1/feed/news?limit=3", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /feed/news: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Slides []struct {
			Featured struct {
				StoryID string `json:"story_id"`
				LeadID  string `json:"lead_id"`
				Members []struct {
					Type string `json:"type"`
				} `json:"members"`
			} `json:"featured"`
		} `json:"slides"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal news response: %v", err)
	}
	if len(resp.Slides) == 0 {
		t.Fatalf("expected at least one slide, got none: %s", w.Body.String())
	}
	first := resp.Slides[0].Featured
	if first.StoryID == "" || first.LeadID == "" {
		t.Fatalf("featured story missing story_id/lead_id: %+v", first)
	}
	if len(first.Members) == 0 {
		t.Fatalf("featured story has no members")
	}
	for _, m := range first.Members {
		if m.Type != "NEWS" {
			t.Fatalf("expected NEWS members in featured story, got %s", m.Type)
		}
	}
	fmt.Println("  ✅ News story-slide baseline holds")
}
