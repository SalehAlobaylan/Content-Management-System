package controllers

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ─── Phase 13: story-based News feed ───────────────────────────────────────
//
// A "story" is the existing first-class topic (a cluster of same-event posts
// from different sources/formats). Each News slide is one featured story plus
// up to 3 related stories. Stories are ranked by momentum — the max ranking
// score across their members (reusing the existing ranking engine) — and the
// featured headline + image come from the highest-engagement member.

const (
	storyMemberPoolLimit   = 1000 // members scored per request
	storyRelatedLimit      = 3    // related stories per slide
	storyRelatedPoolLimit  = 8    // candidate pool per slide (over-fetched, then filtered + deduped)
	storyRelatedWindowDays = 30   // related stories must have a member published within this window
	storyMaxMembers        = 8    // members rendered under a featured story
	newsSnapshotSlideCount = 30   // slides precomputed into the snapshot
)

// StoryMember is one post (NEWS item) inside a story.
type StoryMember struct {
	ID           uuid.UUID `json:"id"`
	Type         string    `json:"type"`
	Format       string    `json:"format,omitempty"`
	Title        string    `json:"title,omitempty"`
	Excerpt      string    `json:"excerpt,omitempty"`
	BodyText     string    `json:"body_text,omitempty"`
	Author       string    `json:"author,omitempty"`
	SourceName   string    `json:"source_name,omitempty"`
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
	PublishedAt  time.Time `json:"published_at"`
	LikeCount    int       `json:"like_count"`
	CommentCount int       `json:"comment_count"`
	ShareCount   int       `json:"share_count"`
	ViewCount    int       `json:"view_count"`
}

// StorySummary renders a story for a slide: headline + image from the
// highest-engagement member, plus the LLM label and member count. LeadID is
// that member's content id — clients open / like / bookmark against it (NOT the
// StoryID, which is a topic id with no content row).
type StorySummary struct {
	StoryID      uuid.UUID `json:"story_id"`
	LeadID       uuid.UUID `json:"lead_id"`
	Label        string    `json:"label"`
	Title        string    `json:"title,omitempty"`
	Excerpt      string    `json:"excerpt,omitempty"`
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
	SourceName   string    `json:"source_name,omitempty"`
	PublishedAt  time.Time `json:"published_at"`
	MemberCount  int       `json:"member_count"`
	LikeCount    int       `json:"like_count"`
	CommentCount int       `json:"comment_count"`
	ShareCount   int       `json:"share_count"`
	ViewCount    int       `json:"view_count"`
}

// StoryFeatured is the featured story of a slide: its summary plus its members.
type StoryFeatured struct {
	StorySummary
	Members []StoryMember `json:"members"`
}

// StorySlide = 1 featured story + up to 3 related stories.
type StorySlide struct {
	SlideID  uuid.UUID      `json:"slide_id"`
	Featured StoryFeatured  `json:"featured"`
	Related  []StorySummary `json:"related"`
}

// StoryNewsResponse is the Phase-13 News feed response (story-slides).
type StoryNewsResponse struct {
	Cursor *string      `json:"cursor"`
	Slides []StorySlide `json:"slides"`
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func itemTime(it models.ContentItem) time.Time {
	if it.PublishedAt != nil {
		return *it.PublishedAt
	}
	return it.CreatedAt
}

// engagementScore is a weighted sum used to pick a story's "top" member (the
// one that supplies the slide headline + image).
func engagementScore(it models.ContentItem) int {
	return it.LikeCount*3 + it.ShareCount*5 + it.CommentCount*2 + it.ViewCount/100
}

// storyAgg accumulates a story's scored members during assembly.
type storyAgg struct {
	storyID  uuid.UUID
	score    float64
	newest   time.Time
	newestID uuid.UUID // id of the newest member — what the client reports as "seen"
	members  []models.ContentItem
}

// assembleStoryNewsFeed builds the News feed as story-slides. Self-contained in
// CMS: stories + centroids live in the topics table, so no Enrichment round-trip
// is needed (the reranker re-orders related stories only in on_demand mode —
// wired in Slice 5). seenIDs (optional) drops slides the session already viewed;
// the client tracks views against the slide's lead member id.
func assembleStoryNewsFeed(
	db *gorm.DB,
	tenantID string,
	config models.RankingConfig,
	lastTimestamp time.Time,
	lastID uuid.UUID,
	slideLimit int,
	seenIDs []uuid.UUID,
) ([]StorySlide, *string) {
	// 1. Pull a recent pool of classified NEWS members and score them with the
	//    existing ranking engine (freshness/engagement/velocity/trending).
	windowDays := config.FreshnessDecayHours / 24
	if windowDays < 30 {
		windowDays = 30
	}
	var members []models.ContentItem
	db.Where("tenant_id = ? AND type = ? AND status = ? AND topic_id IS NOT NULL",
		tenantID, models.ContentTypeNews, models.ContentStatusReady).
		Where("COALESCE(published_at, created_at) > ?", time.Now().AddDate(0, 0, -windowDays)).
		Order("COALESCE(published_at, created_at) DESC").
		Limit(storyMemberPoolLimit).
		Find(&members)

	if len(members) == 0 {
		return []StorySlide{}, nil
	}

	contentIDs := extractPublicIDs(members)
	flagMap := LoadContentFlags(db, tenantID, contentIDs)
	velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, time.Now())
	scored := ScoreItems(members, config, flagMap, velocityData, time.Now())

	// 2. Aggregate scored members into stories. Story momentum = max member
	//    score; we also track the newest-member time for the cursor.
	aggByStory := make(map[uuid.UUID]*storyAgg)
	order := make([]*storyAgg, 0)
	for _, s := range scored {
		if s.Item.TopicID == nil {
			continue
		}
		sid := *s.Item.TopicID
		a := aggByStory[sid]
		if a == nil {
			a = &storyAgg{storyID: sid}
			aggByStory[sid] = a
			order = append(order, a)
		}
		a.members = append(a.members, s.Item)
		if s.FinalScore > a.score {
			a.score = s.FinalScore
		}
		if t := itemTime(s.Item); t.After(a.newest) {
			a.newest = t
			a.newestID = s.Item.PublicID
		}
	}

	// 3. Rank stories by momentum (score desc, then recency).
	sort.SliceStable(order, func(i, j int) bool {
		if order[i].score != order[j].score {
			return order[i].score > order[j].score
		}
		return order[i].newest.After(order[j].newest)
	})

	// 3b. Drop already-seen slides (the client reports views against the slide's
	//     lead member id = the story's newest member).
	if len(seenIDs) > 0 {
		seenSet := make(map[uuid.UUID]bool, len(seenIDs))
		for _, id := range seenIDs {
			seenSet[id] = true
		}
		filtered := order[:0]
		for _, a := range order {
			if !seenSet[a.newestID] {
				filtered = append(filtered, a)
			}
		}
		order = filtered
	}

	// 4. Cursor pagination over the ranked story list.
	startIdx := 0
	if !lastTimestamp.IsZero() && lastID != uuid.Nil {
		found := false
		for i, a := range order {
			if a.storyID == lastID {
				startIdx = i + 1
				found = true
				break
			}
		}
		if !found {
			// The cursor story dropped out (re-scored, reclassified, or expired
			// between pages). Resume by the cursor timestamp instead of silently
			// restarting at page 1 and re-serving the same slides.
			for i, a := range order {
				if !a.newest.After(lastTimestamp) {
					startIdx = i
					break
				}
			}
		}
	}
	endIdx := startIdx + slideLimit
	if endIdx > len(order) {
		endIdx = len(order)
	}
	if startIdx >= len(order) {
		return []StorySlide{}, nil
	}
	page := order[startIdx:endIdx]

	// 5. Load topic metadata (label + centroid) once for labelling + related kNN.
	var topics []models.Topic
	db.Where("tenant_id = ?", tenantID).Find(&topics)
	topicByID := make(map[uuid.UUID]models.Topic, len(topics))
	for _, t := range topics {
		topicByID[t.PublicID] = t
	}

	// 6. Build each slide. In on_demand mode the cross-encoder reranker reorders
	//    each slide's related-story candidates (config.NewsRerankEnabled).
	// Build slides in parallel — each slide does its own related-story kNN
	// (and, in on_demand mode, a cross-encoder rerank HTTP call), so serial
	// assembly would stack N rerank round-trips on the request path. gorm.DB is
	// goroutine-safe and topicByID is read-only here.
	rerank := config.NewsRerankEnabled
	// A related card must never duplicate a featured slide on the same page.
	pageIDs := make(map[uuid.UUID]bool, len(page))
	for _, a := range page {
		pageIDs[a.storyID] = true
	}
	slides := make([]StorySlide, len(page))
	relCandidates := make([][]StorySummary, len(page))
	var wg sync.WaitGroup
	for i, a := range page {
		wg.Add(1)
		go func(idx int, ag *storyAgg) {
			defer wg.Done()
			topic := topicByID[ag.storyID]
			featured := buildStoryFeatured(topic, ag.storyID, ag.members)
			slides[idx] = StorySlide{SlideID: uuid.New(), Featured: featured}
			relCandidates[idx] = buildRelatedStories(db, tenantID, topic, ag.storyID, topicByID, rerank, featured.Title, pageIDs)
		}(i, a)
	}
	wg.Wait()

	// Cross-slide dedup: a related story appears on at most one slide. Walk
	// slides in page order, picking each slide's first still-unused candidates
	// (the candidate lists are over-fetched, so this backfills rather than
	// just dropping).
	usedRelated := make(map[uuid.UUID]bool)
	for i := range slides {
		picked := make([]StorySummary, 0, storyRelatedLimit)
		for _, s := range relCandidates[i] {
			if usedRelated[s.StoryID] {
				continue
			}
			picked = append(picked, s)
			usedRelated[s.StoryID] = true
			if len(picked) == storyRelatedLimit {
				break
			}
		}
		slides[i].Related = picked
	}

	// 7. Next cursor — the last story on the page, when a full page was served.
	var nextCursor *string
	if len(page) == slideLimit && endIdx < len(order) {
		last := page[len(page)-1]
		cursor := utils.EncodeCursor(last.newest, last.storyID)
		nextCursor = &cursor
	}

	return slides, nextCursor
}

// topMember returns the highest-engagement item from a story's members.
func topMember(members []models.ContentItem) models.ContentItem {
	best := members[0]
	bestScore := engagementScore(best)
	for _, m := range members[1:] {
		if s := engagementScore(m); s > bestScore {
			best, bestScore = m, s
		}
	}
	return best
}

func buildStoryFeatured(topic models.Topic, storyID uuid.UUID, members []models.ContentItem) StoryFeatured {
	// Newest-first members for display.
	sort.SliceStable(members, func(i, j int) bool {
		return itemTime(members[i]).After(itemTime(members[j]))
	})

	top := topMember(members)
	// Story size = the topic's all-time member count (matches the related-story
	// cards); fall back to the recent members on hand when the centroid count
	// lags a fresh burst.
	memberCount := topic.ArticleCount
	if memberCount < len(members) {
		memberCount = len(members)
	}
	summary := StorySummary{
		StoryID:      storyID,
		LeadID:       top.PublicID,
		Label:        topic.Label,
		Title:        derefStr(top.Title),
		Excerpt:      derefStr(top.Excerpt),
		ThumbnailURL: derefStr(top.ThumbnailURL),
		SourceName:   derefStr(top.SourceName),
		PublishedAt:  itemTime(top),
		MemberCount:  memberCount,
		LikeCount:    top.LikeCount,
		CommentCount: top.CommentCount,
		ShareCount:   top.ShareCount,
		ViewCount:    top.ViewCount,
	}

	limit := len(members)
	if limit > storyMaxMembers {
		limit = storyMaxMembers
	}
	out := make([]StoryMember, 0, limit)
	for _, m := range members[:limit] {
		out = append(out, mapStoryMember(m))
	}
	return StoryFeatured{StorySummary: summary, Members: out}
}

func mapStoryMember(m models.ContentItem) StoryMember {
	return StoryMember{
		ID:           m.PublicID,
		Type:         string(m.Type),
		Format:       derefStr(m.Format),
		Title:        derefStr(m.Title),
		Excerpt:      derefStr(m.Excerpt),
		BodyText:     derefStr(m.BodyText),
		Author:       derefStr(m.Author),
		SourceName:   derefStr(m.SourceName),
		ThumbnailURL: derefStr(m.ThumbnailURL),
		PublishedAt:  itemTime(m),
		LikeCount:    m.LikeCount,
		CommentCount: m.CommentCount,
		ShareCount:   m.ShareCount,
		ViewCount:    m.ViewCount,
	}
}

// buildRelatedStories returns ranked related-story CANDIDATES (not truncated to
// the final 3 — the caller dedups across slides and truncates). It finds the
// nearest stories by centroid cosine, excludes the page's featured stories,
// requires recent activity, and drops stories with no renderable lead member
// (which would show as a blank card). In on_demand mode the candidates are
// cross-encoder reranked against the featured headline.
func buildRelatedStories(
	db *gorm.DB,
	tenantID string,
	topic models.Topic,
	storyID uuid.UUID,
	topicByID map[uuid.UUID]models.Topic,
	rerank bool,
	queryText string,
	excludeIDs map[uuid.UUID]bool,
) []StorySummary {
	if topic.Embedding == nil {
		return []StorySummary{}
	}
	lit := utils.PgvectorToLiteral(topic.Embedding.Slice())

	// Over-fetch: page-featured stories, blanks, and already-used (cross-slide)
	// candidates get filtered out, so pull a generous pool to still yield 3.
	poolLimit := storyRelatedPoolLimit
	if rerank {
		poolLimit = storyRelatedPoolLimit * 2
	}
	relatedFloor := time.Now().AddDate(0, 0, -storyRelatedWindowDays)
	var relIDs []uuid.UUID
	db.Model(&models.Topic{}).
		Where("tenant_id = ? AND public_id != ? AND embedding IS NOT NULL AND article_count > 0", tenantID, storyID).
		Where("last_member_at IS NULL OR last_member_at > ?", relatedFloor).
		Order("embedding <=> '" + lit + "'").
		Limit(poolLimit).
		Pluck("public_id", &relIDs)

	out := make([]StorySummary, 0, len(relIDs))
	for _, rid := range relIDs {
		if excludeIDs[rid] {
			continue // already shown as a featured slide on this page
		}
		s := storySummaryFromTopMember(db, tenantID, topicByID[rid], rid)
		if s.LeadID == uuid.Nil {
			continue // no READY member — would render a blank card
		}
		out = append(out, s)
	}

	// On_demand: rerank the candidate stories with the cross-encoder, keyed on
	// the featured story's headline. Degrades to centroid order on any error.
	if rerank && len(out) > 1 && strings.TrimSpace(queryText) != "" {
		out = rerankStorySummaries(queryText, out)
	}
	return out
}

// storySummaryFromTopMember renders a related story by fetching its highest-
// engagement member for the headline + image.
func storySummaryFromTopMember(db *gorm.DB, tenantID string, topic models.Topic, storyID uuid.UUID) StorySummary {
	summary := StorySummary{StoryID: storyID, Label: topic.Label, MemberCount: topic.ArticleCount}

	var members []models.ContentItem
	db.Where("tenant_id = ? AND topic_id = ? AND status = ?", tenantID, storyID, models.ContentStatusReady).
		Order("like_count*3 + share_count*5 + comment_count*2 DESC").
		Limit(1).
		Find(&members)
	if len(members) == 0 {
		return summary
	}
	top := members[0]
	summary.LeadID = top.PublicID
	summary.Title = derefStr(top.Title)
	summary.Excerpt = derefStr(top.Excerpt)
	summary.ThumbnailURL = derefStr(top.ThumbnailURL)
	summary.SourceName = derefStr(top.SourceName)
	summary.PublishedAt = itemTime(top)
	summary.LikeCount = top.LikeCount
	summary.CommentCount = top.CommentCount
	summary.ShareCount = top.ShareCount
	summary.ViewCount = top.ViewCount
	return summary
}

// ─── Precompute snapshot ───────────────────────────────────────────────────

// buildNewsSnapshot assembles the top story-slides live and upserts them into
// the per-tenant news_snapshots row. Called by the admin Refresh endpoint (or
// an external cron hitting it) and lazily on first read when the snapshot is
// empty.
func buildNewsSnapshot(db *gorm.DB, tenantID string) (int, error) {
	config := loadTenantConfig(db, tenantID)
	slides, _ := assembleStoryNewsFeed(db, tenantID, config, time.Time{}, uuid.Nil, newsSnapshotSlideCount, nil)
	if slides == nil {
		slides = []StorySlide{}
	}
	data, err := json.Marshal(slides)
	if err != nil {
		return 0, err
	}
	// Atomic upsert on the unique tenant_id — avoids the SELECT-then-INSERT race
	// (lazy build on read vs admin Refresh) that a FirstOrCreate would hit.
	snap := models.NewsSnapshot{
		TenantID:   tenantID,
		Slides:     datatypes.JSON(data),
		SlideCount: len(slides),
		BuiltAt:    time.Now(),
	}
	err = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"slides", "slide_count", "built_at"}),
	}).Create(&snap).Error
	if err != nil {
		return 0, err
	}
	return len(slides), nil
}

// newsSnapshotTTL is the stale-while-revalidate budget for precompute mode:
// reads always serve the stored snapshot instantly, and when it's older than
// this a background rebuild is kicked so the NEXT read is fresh. Keeps the
// news feed current without manual refreshes or a cron dependency.
const newsSnapshotTTL = 10 * time.Minute

// snapshotRebuildRunning guards the SWR background rebuild against stampedes.
var snapshotRebuildRunning atomic.Bool

// startSnapshotRebuild rebuilds the precompute snapshot in the background.
// No-ops when a rebuild is already in flight.
func startSnapshotRebuild(db *gorm.DB, tenantID string) {
	if !snapshotRebuildRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer snapshotRebuildRunning.Store(false)
		if _, err := buildNewsSnapshot(db, tenantID); err != nil {
			log.Printf("[news-snapshot] background rebuild failed: %v", err)
		}
	}()
}

// serveNewsSnapshot returns a page of precomputed story-slides for precompute
// mode. Lazily builds the snapshot on first read if it's missing/empty, and
// kicks a background rebuild when it's stale (stale-while-revalidate). seenIDs
// drops slides whose lead member the session already viewed.
func serveNewsSnapshot(db *gorm.DB, tenantID string, lastTimestamp time.Time, lastID uuid.UUID, slideLimit int, seenIDs []uuid.UUID) ([]StorySlide, *string) {
	var snap models.NewsSnapshot
	err := db.Where("tenant_id = ?", tenantID).First(&snap).Error
	if err != nil || len(snap.Slides) == 0 {
		_, _ = buildNewsSnapshot(db, tenantID)
		if err := db.Where("tenant_id = ?", tenantID).First(&snap).Error; err != nil {
			return []StorySlide{}, nil
		}
	} else if time.Since(snap.BuiltAt) > newsSnapshotTTL {
		startSnapshotRebuild(db, tenantID)
	}

	var all []StorySlide
	if len(snap.Slides) == 0 || json.Unmarshal(snap.Slides, &all) != nil {
		return []StorySlide{}, nil
	}

	// Drop already-seen slides (client tracks views against the lead member id,
	// which is the first member of the featured story).
	if len(seenIDs) > 0 {
		seenSet := make(map[uuid.UUID]bool, len(seenIDs))
		for _, id := range seenIDs {
			seenSet[id] = true
		}
		filtered := all[:0]
		for _, s := range all {
			if len(s.Featured.Members) > 0 && seenSet[s.Featured.Members[0].ID] {
				continue
			}
			filtered = append(filtered, s)
		}
		all = filtered
	}

	start := 0
	if lastID != uuid.Nil {
		found := false
		for i, s := range all {
			if s.Featured.StoryID == lastID {
				start = i + 1
				found = true
				break
			}
		}
		if !found && !lastTimestamp.IsZero() {
			// Snapshot was rebuilt between pages (SWR) and the cursor story
			// moved/dropped — resume by timestamp instead of restarting page 1.
			for i, s := range all {
				if !s.Featured.PublishedAt.After(lastTimestamp) {
					start = i
					break
				}
			}
		}
	}
	if start >= len(all) {
		return []StorySlide{}, nil
	}
	end := start + slideLimit
	if end > len(all) {
		end = len(all)
	}
	page := all[start:end]

	var nextCursor *string
	if end < len(all) && len(page) == slideLimit {
		last := page[len(page)-1]
		cursor := utils.EncodeCursor(last.Featured.PublishedAt, last.Featured.StoryID)
		nextCursor = &cursor
	}
	return page, nextCursor
}

// ─── On-demand reranking of related stories ────────────────────────────────

type storyRerankResponse struct {
	Scores []float64 `json:"scores"`
}

// rerankStorySummaries reorders related-story candidates with the Enrichment
// cross-encoder, keyed on the featured story's headline. On any failure it
// returns the input unchanged (centroid order) — rerank is enrichment, not a
// hard requirement, so the feed never fails on a reranker hiccup.
func rerankStorySummaries(query string, summaries []StorySummary) []StorySummary {
	baseURL := enrichmentBaseURL()
	token := enrichmentServiceToken()
	if baseURL == "" || token == "" || len(summaries) == 0 {
		return summaries
	}

	candidates := make([]string, len(summaries))
	for i, s := range summaries {
		text := strings.TrimSpace(s.Title + " " + s.Excerpt)
		if text == "" {
			text = s.Label
		}
		candidates[i] = text
	}

	body, err := json.Marshal(map[string]interface{}{"query": query, "candidates": candidates})
	if err != nil {
		return summaries
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/rerank", bytes.NewReader(body))
	if err != nil {
		return summaries
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return summaries
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return summaries
	}

	var decoded storyRerankResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil || len(decoded.Scores) != len(summaries) {
		return summaries
	}

	idx := make([]int, len(summaries))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		return decoded.Scores[idx[a]] > decoded.Scores[idx[b]]
	})
	out := make([]StorySummary, len(summaries))
	for i, j := range idx {
		out[i] = summaries[j]
	}
	return out
}
