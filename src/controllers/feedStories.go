package controllers

import (
	"bytes"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"net/url"
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
	// storyRelatedMinSimilarity floors related-story candidates. Calibrated on
	// the Qwen3 space: same-event ≈ ≥0.70, genuinely-related coverage ≈
	// 0.55–0.70, below 0.55 is just "other news". Better 0 honest related
	// cards than 3 fabricated ones.
	storyRelatedMinSimilarity = 0.55
	storyMaxMembers        = 8    // members rendered under a featured story
	newsSnapshotSlideCount = 60   // slides precomputed into the cache (≈6 pages before deep scroll goes live)
)

// storyFeedColumns omits the heavy content_items columns a feed read never
// renders: the embedding vectors serialize to ~12-17KB of text PER ROW on the
// wire, and metadata jsonb can be arbitrarily large. SELECT * over a remote
// DB (Neon, ~0.3-1s RTT) turned the live path into a multi-minute request —
// the 1000-member pool alone moved ~15MB. The ranking engine only checks
// Embedding != nil (+0.15 quality completeness), which classified members all
// satisfy, so omitting it shifts every item uniformly: no ranking change.
// body_text is capped in SQL: the feed renders at most ~400 chars of it
// (expandable preview), while raw Telegram/article bodies run multi-KB —
// across a 1000-row pool that's megabytes of wire transfer for nothing.
const storyFeedColumns = "id, public_id, tenant_id, type, format, source, status, " +
	"title, LEFT(body_text, 600) AS body_text, excerpt, author, source_name, " +
	"thumbnail_url, duration_sec, topic_tags, topic_id, transcript_id, source_feed_url, " +
	"like_count, comment_count, share_count, view_count, published_at, created_at"

// topicMetaColumns omits the topic centroid (vector(1024) ≈ 12KB wire text per
// row) — feed reads need labels/counts/related_ids only; the kNN fallback
// fetches the one centroid it needs on demand.
const topicMetaColumns = "id, public_id, tenant_id, label, article_count, " +
	"labeled, last_member_at, related_ids, created_at, updated_at"

// storyScoreColumns is the SCORING projection: the 1000-row candidate pool
// only feeds ScoreItems + story aggregation, so it carries no display text at
// all (title/excerpt/body dominated the pool's wire size — display fields are
// hydrated afterwards for just the page's stories). thumbnail_url/source_name
// stay: the ranking engine reads them as quality/diversity signals.
const storyScoreColumns = "public_id, tenant_id, type, source, status, topic_id, " +
	"topic_tags, transcript_id, duration_sec, source_name, thumbnail_url, " +
	"like_count, comment_count, share_count, view_count, published_at, created_at"

// StoryMember is one post (NEWS item) inside a story.
type StoryMember struct {
	ID             uuid.UUID `json:"id"`
	Type           string    `json:"type"`
	Format         string    `json:"format,omitempty"`
	Title          string    `json:"title,omitempty"`
	Excerpt        string    `json:"excerpt,omitempty"`
	BodyText       string    `json:"body_text,omitempty"`
	Author         string    `json:"author,omitempty"`
	SourceName     string    `json:"source_name,omitempty"`
	ThumbnailURL   string    `json:"thumbnail_url,omitempty"`
	SourceImageURL string    `json:"source_image_url,omitempty"`
	PublishedAt    time.Time `json:"published_at"`
	LikeCount      int       `json:"like_count"`
	CommentCount   int       `json:"comment_count"`
	ShareCount     int       `json:"share_count"`
	ViewCount      int       `json:"view_count"`
}

// StorySummary renders a story for a slide: headline + image from the
// highest-engagement member, plus the LLM label and member count. LeadID is
// that member's content id — clients open / like / bookmark against it (NOT the
// StoryID, which is a topic id with no content row).
type StorySummary struct {
	StoryID        uuid.UUID `json:"story_id"`
	LeadID         uuid.UUID `json:"lead_id"`
	Label          string    `json:"label"`
	Title          string    `json:"title,omitempty"`
	Excerpt        string    `json:"excerpt,omitempty"`
	ThumbnailURL   string    `json:"thumbnail_url,omitempty"`
	SourceName     string    `json:"source_name,omitempty"`
	SourceImageURL string    `json:"source_image_url,omitempty"`
	PublishedAt time.Time `json:"published_at"`
	MemberCount int       `json:"member_count"`
	// SourceCount is the number of DISTINCT sources among the story's hydrated
	// members — the Radar-style "covered by N sources" signal. Computed for
	// the featured story only (related cards show member_count); 0 = not
	// computed.
	SourceCount  int `json:"source_count,omitempty"`
	LikeCount    int `json:"like_count"`
	CommentCount   int       `json:"comment_count"`
	ShareCount     int       `json:"share_count"`
	ViewCount      int       `json:"view_count"`
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
	// Topic metadata is independent of the member pool — overlap the two WAN
	// round-trips (each is the better part of a second against a remote DB).
	topicsCh := make(chan map[uuid.UUID]models.Topic, 1)
	go func() {
		var topics []models.Topic
		db.Select(topicMetaColumns).Where("tenant_id = ?", tenantID).Find(&topics)
		byID := make(map[uuid.UUID]models.Topic, len(topics))
		for _, t := range topics {
			byID[t.PublicID] = t
		}
		topicsCh <- byID
	}()

	windowStart := time.Now().AddDate(0, 0, -windowDays)
	var members []models.ContentItem
	db.Select(storyScoreColumns).
		Where("tenant_id = ? AND type = ? AND status = ? AND topic_id IS NOT NULL",
			tenantID, models.ContentTypeNews, models.ContentStatusReady).
		Where("COALESCE(published_at, created_at) > ?", windowStart).
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

	// 3. Rank stories by COVERAGE-AWARE momentum. Aggregation is the product:
	//    a story many posts are covering IS the bigger story, so the per-item
	//    momentum (freshness/engagement/velocity) is lifted by ln(1 + recent
	//    members). At the 0.30 default a 24-post story gets ~2× over a
	//    singleton — the story of the day outranks fresher one-off posts
	//    without freezing out breaking singletons.
	coverageW := config.StoryCoverageWeight
	if coverageW < 0 {
		coverageW = 0
	}
	if coverageW > 0 {
		for _, a := range order {
			a.score *= 1 + coverageW*math.Log1p(float64(len(a.members)))
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		if order[i].score != order[j].score {
			return order[i].score > order[j].score
		}
		return order[i].newest.After(order[j].newest)
	})

	// 3b. Drop already-seen slides (the client reports views against the slide's
	//     lead member id = the story's newest member). When the session has
	//     seen EVERYTHING, recycle on a fresh load instead of serving an empty
	//     feed (mirrors the For You ShowWatchedWhenUnseenExhausted behavior) —
	//     a blank News tab is never the right answer.
	if len(seenIDs) > 0 {
		seenSet := make(map[uuid.UUID]bool, len(seenIDs))
		for _, id := range seenIDs {
			seenSet[id] = true
		}
		filtered := make([]*storyAgg, 0, len(order))
		for _, a := range order {
			if !seenSet[a.newestID] {
				filtered = append(filtered, a)
			}
		}
		// A fully-empty filter means literally every pooled story is seen
		// (a partially-seen corpus always leaves unseen entries) — recycle on
		// ANY page, not just the first, so a recycled feed can paginate past
		// slide 10 instead of dead-ending; unfiltered cursor pagination then
		// terminates naturally at the corpus end.
		exhausted := len(filtered) == 0
		if !exhausted || !config.ShowWatchedWhenUnseenExhausted {
			order = filtered
		}
	}

	// 3c. Inter-slide diversity: sibling stories (stories in each other's
	//     related sets — i.e. ≥storyRelatedMinSimilarity apart) must not stack
	//     adjacently. A burst (e.g. five Iran-conflict sub-stories) interleaves
	//     with other news instead of monopolizing consecutive slides. Greedy
	//     single-lookahead: pick the highest-ranked candidate that isn't a
	//     sibling of the previous slide; if everything left is a sibling, give
	//     up gracefully (don't bury content). Uses the stored related sets —
	//     no extra queries. topicByID arrives here (fetched concurrently with
	//     the pool).
	topicByID := <-topicsCh
	if len(order) > 2 {
		siblingSets := make(map[uuid.UUID]map[uuid.UUID]bool, len(order))
		siblings := func(id uuid.UUID) map[uuid.UUID]bool {
			if s, ok := siblingSets[id]; ok {
				return s
			}
			s := make(map[uuid.UUID]bool)
			if t, ok := topicByID[id]; ok {
				if ids, computed := storedRelatedIDs(t); computed {
					for _, rid := range ids {
						s[rid] = true
					}
				}
			}
			siblingSets[id] = s
			return s
		}
		reordered := make([]*storyAgg, 0, len(order))
		pool := append([]*storyAgg(nil), order...)
		var prev *storyAgg
		for len(pool) > 0 {
			pick := 0
			if prev != nil {
				prevSibs := siblings(prev.storyID)
				for i, c := range pool {
					if prevSibs[c.storyID] || siblings(c.storyID)[prev.storyID] {
						continue
					}
					pick = i
					break
				}
			}
			prev = pool[pick]
			reordered = append(reordered, prev)
			pool = append(pool[:pick], pool[pick+1:]...)
		}
		order = reordered
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

	// 6. Build each slide in parallel (per-slide related-story hydration is a
	//    handful of indexed queries; gorm.DB is goroutine-safe and topicByID is
	//    read-only here). Reranking does NOT happen here — related-story order
	//    is computed at WRITE time (refreshStoryRelated) and stored on the
	//    topic row, so the read path stays pure-Postgres and real-time.
	// A related card must never duplicate a featured slide on the same page.
	pageIDs := make(map[uuid.UUID]bool, len(page))
	pageStoryIDs := make([]uuid.UUID, 0, len(page))
	for _, a := range page {
		pageIDs[a.storyID] = true
		pageStoryIDs = append(pageStoryIDs, a.storyID)
	}

	// Hydrate display fields (title/body/excerpt/author) for the PAGE's
	// members only — the 1000-row scoring pool is deliberately text-free, and
	// a page renders at most ~storyMaxMembers per slide.
	maxHydrate := len(page) * storyMaxMembers * 3
	if maxHydrate > 500 {
		maxHydrate = 500
	}
	var pageMembers []models.ContentItem
	db.Select(storyFeedColumns).
		Where("tenant_id = ? AND type = ? AND status = ? AND topic_id IN ?",
			tenantID, models.ContentTypeNews, models.ContentStatusReady, pageStoryIDs).
		Where("COALESCE(published_at, created_at) > ?", windowStart).
		Order("COALESCE(published_at, created_at) DESC").
		Limit(maxHydrate).
		Find(&pageMembers)
	membersByStory := make(map[uuid.UUID][]models.ContentItem, len(page))
	for _, m := range pageMembers {
		if m.TopicID != nil {
			membersByStory[*m.TopicID] = append(membersByStory[*m.TopicID], m)
		}
	}
	sourceImageByFeedURL := loadSourceImagesByFeedURL(db, tenantID, pageMembers)

	slides := make([]StorySlide, len(page))
	relCandidates := make([][]StorySummary, len(page))
	var wg sync.WaitGroup
	for i, a := range page {
		wg.Add(1)
		go func(idx int, ag *storyAgg) {
			defer wg.Done()
			topic := topicByID[ag.storyID]
			storyMembers := membersByStory[ag.storyID]
			if len(storyMembers) == 0 {
				// Defensive — page stories always have pool members; slim rows
				// at worst render text-light rather than dropping the slide.
				storyMembers = ag.members
			}
			featured := buildStoryFeatured(topic, ag.storyID, storyMembers, sourceImageByFeedURL)
			slides[idx] = StorySlide{SlideID: uuid.New(), Featured: featured}
			relCandidates[idx] = buildRelatedStories(db, tenantID, topic, ag.storyID, topicByID, pageIDs)
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

func buildStoryFeatured(topic models.Topic, storyID uuid.UUID, members []models.ContentItem, sourceImageByFeedURL map[string]string) StoryFeatured {
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
	// Distinct sources among the visible members — the "covered by N sources"
	// aggregation signal. Free: members are already in hand.
	distinctSources := make(map[string]bool, len(members))
	for _, m := range members {
		if s := derefStr(m.SourceName); s != "" {
			distinctSources[s] = true
		}
	}
	summary := StorySummary{
		StoryID:        storyID,
		LeadID:         top.PublicID,
		Label:          topic.Label,
		Title:          derefStr(top.Title),
		Excerpt:        derefStr(top.Excerpt),
		ThumbnailURL:   derefStr(top.ThumbnailURL),
		SourceName:     derefStr(top.SourceName),
		SourceImageURL: sourceImageForItem(top, sourceImageByFeedURL),
		PublishedAt:    itemTime(top),
		MemberCount:    memberCount,
		SourceCount:    len(distinctSources),
		LikeCount:      top.LikeCount,
		CommentCount:   top.CommentCount,
		ShareCount:     top.ShareCount,
		ViewCount:      top.ViewCount,
	}

	limit := len(members)
	if limit > storyMaxMembers {
		limit = storyMaxMembers
	}
	out := make([]StoryMember, 0, limit)
	for _, m := range members[:limit] {
		out = append(out, mapStoryMember(m, sourceImageByFeedURL))
	}
	return StoryFeatured{StorySummary: summary, Members: out}
}

func mapStoryMember(m models.ContentItem, sourceImageByFeedURL map[string]string) StoryMember {
	return StoryMember{
		ID:             m.PublicID,
		Type:           string(m.Type),
		Format:         derefStr(m.Format),
		Title:          derefStr(m.Title),
		Excerpt:        derefStr(m.Excerpt),
		BodyText:       derefStr(m.BodyText),
		Author:         derefStr(m.Author),
		SourceName:     derefStr(m.SourceName),
		ThumbnailURL:   derefStr(m.ThumbnailURL),
		SourceImageURL: sourceImageForItem(m, sourceImageByFeedURL),
		PublishedAt:    itemTime(m),
		LikeCount:      m.LikeCount,
		CommentCount:   m.CommentCount,
		ShareCount:     m.ShareCount,
		ViewCount:      m.ViewCount,
	}
}

func loadSourceImagesByFeedURL(db *gorm.DB, tenantID string, items []models.ContentItem) map[string]string {
	lookupKeys := make([]string, 0)
	seen := make(map[string]bool)
	for _, it := range items {
		for _, key := range sourceLookupKeys(derefStr(it.SourceFeedURL), derefStr(it.SourceName)) {
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			lookupKeys = append(lookupKeys, key)
		}
	}
	if len(lookupKeys) == 0 {
		return nil
	}

	var sources []models.ContentSource
	db.Select("name, feed_url, image_url").
		Where("tenant_id = ? AND image_url IS NOT NULL AND image_url != ''", tenantID).
		Find(&sources)

	out := make(map[string]string, len(sources))
	for _, source := range sources {
		value := strings.TrimSpace(derefStr(source.ImageURL))
		if value == "" {
			continue
		}
		for _, key := range sourceLookupKeys(derefStr(source.FeedURL), source.Name) {
			if key != "" {
				out[key] = value
			}
		}
	}
	return out
}

func sourceLookupKeys(rawURL string, sourceName string) []string {
	keys := make([]string, 0, 4)
	add := func(value string) {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			return
		}
		for _, existing := range keys {
			if existing == value {
				return
			}
		}
		keys = append(keys, value)
	}

	add(rawURL)
	add(sourceName)
	if host := hostKey(rawURL); host != "" {
		add(host)
	}
	if host := hostKey(sourceName); host != "" {
		add(host)
	}
	return keys
}

func hostKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
}

func sourceImageForItem(it models.ContentItem, sourceImageByFeedURL map[string]string) string {
	if len(sourceImageByFeedURL) == 0 {
		return ""
	}
	for _, key := range sourceLookupKeys(derefStr(it.SourceFeedURL), derefStr(it.SourceName)) {
		if value := sourceImageByFeedURL[key]; value != "" {
			return value
		}
	}
	return ""
}

// storedRelatedIDs parses the write-time-computed related list. The second
// return distinguishes "computed" from "never computed": an EMPTY stored array
// is a real answer ("nothing genuinely related") and must NOT trigger a live
// kNN fallback on every read.
func storedRelatedIDs(topic models.Topic) ([]uuid.UUID, bool) {
	if len(topic.RelatedIDs) == 0 {
		return nil, false // NULL — never computed
	}
	var raw []string
	if json.Unmarshal(topic.RelatedIDs, &raw) != nil {
		return nil, false
	}
	ids := make([]uuid.UUID, 0, len(raw))
	for _, s := range raw {
		if u, err := uuid.Parse(s); err == nil {
			ids = append(ids, u)
		}
	}
	return ids, true
}

// relatedCandidateIDs returns the ordered related-story id candidates for a
// story. Prefers the WRITE-TIME-computed order stored on the topic row
// (refreshStoryRelated: floored centroid kNN + optional cross-encoder rerank);
// falls back to a live centroid kNN only when the stored list was never
// computed (story predates the feature or its refresh hasn't landed yet).
func relatedCandidateIDs(db *gorm.DB, tenantID string, topic models.Topic, storyID uuid.UUID) []uuid.UUID {
	if ids, computed := storedRelatedIDs(topic); computed {
		return ids // may be empty — an honest "no related stories"
	}
	return relatedKNNIDs(db, tenantID, storyID, storyRelatedPoolLimit)
}

// relatedKNNIDs is the raw centroid-cosine neighbor query: active stories with
// members, nearest first — FLOORED at storyRelatedMinSimilarity. Without the
// floor, kNN dutifully returns the 3 "nearest" stories even when nearest means
// cosine 0.35 (unrelated news), and the related section lies to the user.
// Self-contained — fetches the one centroid it needs (the bulk topic load
// deliberately skips centroids; see topicMetaColumns).
func relatedKNNIDs(db *gorm.DB, tenantID string, storyID uuid.UUID, limit int) []uuid.UUID {
	var lit string
	db.Model(&models.Topic{}).
		Where("public_id = ? AND embedding IS NOT NULL", storyID).
		Pluck("embedding::text", &lit)
	if lit == "" {
		return nil
	}
	relatedFloor := time.Now().AddDate(0, 0, -storyRelatedWindowDays)
	var relIDs []uuid.UUID
	db.Model(&models.Topic{}).
		Where("tenant_id = ? AND public_id != ? AND embedding IS NOT NULL AND article_count > 0", tenantID, storyID).
		Where("last_member_at IS NULL OR last_member_at > ?", relatedFloor).
		Where("embedding <=> '"+lit+"' <= ?", 1-storyRelatedMinSimilarity).
		Order("embedding <=> '"+lit+"'").
		Limit(limit).
		Pluck("public_id", &relIDs)
	return relIDs
}

// leadSummariesByStory hydrates summaries for a SET of stories in one query:
// DISTINCT ON picks each story's highest-engagement READY member. Replaces the
// per-story lookups that fanned out N×RTT against the remote DB (the main
// culprit in multi-minute feed responses). Stories with no READY member are
// simply absent from the result (callers drop them — blank cards).
func leadSummariesByStory(
	db *gorm.DB,
	tenantID string,
	storyIDs []uuid.UUID,
	topicByID map[uuid.UUID]models.Topic,
) map[uuid.UUID]StorySummary {
	out := make(map[uuid.UUID]StorySummary, len(storyIDs))
	if len(storyIDs) == 0 {
		return out
	}
	var leads []models.ContentItem
	db.Raw(
		"SELECT DISTINCT ON (topic_id) "+storyFeedColumns+
			" FROM content_items WHERE tenant_id = ? AND topic_id IN ? AND status = ?"+
			" ORDER BY topic_id, like_count*3 + share_count*5 + comment_count*2 DESC",
		tenantID, storyIDs, models.ContentStatusReady,
	).Scan(&leads)
	sourceImageByFeedURL := loadSourceImagesByFeedURL(db, tenantID, leads)
	for _, top := range leads {
		if top.TopicID == nil {
			continue
		}
		sid := *top.TopicID
		t := topicByID[sid]
		memberCount := t.ArticleCount
		out[sid] = StorySummary{
			StoryID:        sid,
			LeadID:         top.PublicID,
			Label:          t.Label,
			Title:          derefStr(top.Title),
			Excerpt:        derefStr(top.Excerpt),
			ThumbnailURL:   derefStr(top.ThumbnailURL),
			SourceName:     derefStr(top.SourceName),
			SourceImageURL: sourceImageForItem(top, sourceImageByFeedURL),
			PublishedAt:    itemTime(top),
			MemberCount:    memberCount,
			LikeCount:      top.LikeCount,
			CommentCount:   top.CommentCount,
			ShareCount:     top.ShareCount,
			ViewCount:      top.ViewCount,
		}
	}
	return out
}

// buildRelatedStories returns ranked related-story CANDIDATES (not truncated to
// the final 3 — the caller dedups across slides and truncates). Order comes
// from the write-time-computed list (or a kNN fallback); hydration is one
// batched query. Excludes the page's featured stories and stories with no
// renderable lead member.
func buildRelatedStories(
	db *gorm.DB,
	tenantID string,
	topic models.Topic,
	storyID uuid.UUID,
	topicByID map[uuid.UUID]models.Topic,
	excludeIDs map[uuid.UUID]bool,
) []StorySummary {
	relIDs := relatedCandidateIDs(db, tenantID, topic, storyID)

	cands := make([]uuid.UUID, 0, len(relIDs))
	for _, rid := range relIDs {
		if !excludeIDs[rid] {
			cands = append(cands, rid)
		}
	}
	leads := leadSummariesByStory(db, tenantID, cands, topicByID)

	out := make([]StorySummary, 0, len(cands))
	for _, rid := range cands {
		if s, ok := leads[rid]; ok && s.LeadID != uuid.Nil {
			out = append(out, s)
		}
	}
	return out
}

// ─── Read-through cache (freshness-bounded, never authoritative) ───────────

// buildNewsSnapshot assembles the top story-slides live and upserts them into
// the per-tenant news_snapshots cache row (clearing Dirty). Called by the SWR
// background refresh, the admin Refresh endpoint, the classification backfill,
// and lazily when the cache is empty.
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
	// built_at truncated to microseconds (Postgres timestamp precision) so the
	// in-memory copy's built_at compares Equal to what reads see from the DB.
	snap := models.NewsSnapshot{
		TenantID:   tenantID,
		Slides:     datatypes.JSON(data),
		SlideCount: len(slides),
		Dirty:      false,
		BuiltAt:    time.Now().Truncate(time.Microsecond),
	}
	err = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"slides", "slide_count", "dirty", "built_at"}),
	}).Create(&snap).Error
	if err != nil {
		return 0, err
	}
	// Seed process memory directly — the next read serves without re-pulling
	// the JSON it just wrote.
	newsSnapshotMem.Store(&memCachedSnapshot{tenantID: tenantID, slides: slides, builtAt: snap.BuiltAt})
	return len(slides), nil
}

// newsSnapshotTTL is the freshness SLO of the News-feed cache: a cached page
// is served only while younger than this AND not marked dirty by an ingest
// event. Past it, requests assemble LIVE (the product is real-time — PRD:
// "write-time intelligence, read-time freshness") while the cache refreshes
// in the background.
const newsSnapshotTTL = 60 * time.Second

// newsSnapshotMaxStale is the hard ceiling for serve-stale-while-revalidate:
// a user request is NEVER blocked on inline assembly while a cache younger
// than this exists — it serves instantly and the rebuild runs behind it.
// Inline assembly only happens truly cold (no cache, or idle longer than
// this). Bounded staleness, flat latency.
const newsSnapshotMaxStale = 15 * time.Minute

// memCachedSnapshot is the in-process copy of the news cache. The DB row stays
// the source of truth (and the cross-instance invalidation signal); memory
// only avoids re-pulling and re-parsing the multi-MB slides JSON over the WAN
// on every request. The hot path validates with one tiny (dirty, built_at)
// header query and serves from memory when built_at matches.
type memCachedSnapshot struct {
	tenantID string
	slides   []StorySlide
	builtAt  time.Time
}

var newsSnapshotMem atomic.Pointer[memCachedSnapshot]

// loadCachedSnapshot returns the cached slides plus their freshness header,
// preferring process memory (one tiny header query on the hot path). ok=false
// → no usable cache row at all.
func loadCachedSnapshot(db *gorm.DB, tenantID string) (slides []StorySlide, builtAt time.Time, dirty bool, ok bool) {
	var head struct {
		Dirty   bool
		BuiltAt time.Time
	}
	if err := db.Model(&models.NewsSnapshot{}).
		Select("dirty, built_at").
		Where("tenant_id = ?", tenantID).
		Scan(&head).Error; err != nil || head.BuiltAt.IsZero() {
		return nil, time.Time{}, false, false
	}
	if mem := newsSnapshotMem.Load(); mem != nil && mem.tenantID == tenantID && mem.builtAt.Equal(head.BuiltAt) {
		return mem.slides, head.BuiltAt, head.Dirty, true
	}
	var snap models.NewsSnapshot
	if err := db.Where("tenant_id = ?", tenantID).First(&snap).Error; err != nil || len(snap.Slides) == 0 {
		return nil, time.Time{}, false, false
	}
	var all []StorySlide
	if json.Unmarshal(snap.Slides, &all) != nil || len(all) == 0 {
		return nil, time.Time{}, false, false
	}
	newsSnapshotMem.Store(&memCachedSnapshot{tenantID: tenantID, slides: all, builtAt: snap.BuiltAt})
	return all, snap.BuiltAt, snap.Dirty, true
}

// markNewsSnapshotDirty invalidates the cache ahead of its TTL — called when
// classification adds a member to a story (a news event happened, the feed
// must reflect it now). Cheap single-row UPDATE; missing row is a no-op.
func markNewsSnapshotDirty(db *gorm.DB, tenantID string) {
	db.Model(&models.NewsSnapshot{}).
		Where("tenant_id = ?", tenantID).
		UpdateColumn("dirty", true)
}

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

// paginateStorySlides applies session seen-filtering + cursor pagination to a
// cached slide list. Shared by the read-through fresh-cache path and the
// cached_only emergency mode.
func paginateStorySlides(all []StorySlide, lastTimestamp time.Time, lastID uuid.UUID, slideLimit int, seenIDs []uuid.UUID) ([]StorySlide, *string) {
	// Drop already-seen slides (client tracks views against the lead member id,
	// which is the first member of the featured story).
	if len(seenIDs) > 0 {
		seenSet := make(map[uuid.UUID]bool, len(seenIDs))
		for _, id := range seenIDs {
			seenSet[id] = true
		}
		filtered := make([]StorySlide, 0, len(all))
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
			// Cache was rebuilt between pages and the cursor story moved or
			// dropped — resume by timestamp instead of restarting page 1.
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

	// A full page always carries a cursor — even when it drained the cache.
	// The cache is a bounded window over a larger corpus, so IT must never
	// declare end-of-feed: the follow-up request's cursor lands on the cache's
	// last slide, paginates to an empty page, and falls through to live
	// assembly, which continues past the cached depth. Only live assembly
	// (full corpus) may terminate the feed.
	var nextCursor *string
	if len(page) == slideLimit {
		last := page[len(page)-1]
		cursor := utils.EncodeCursor(last.Featured.PublishedAt, last.Featured.StoryID)
		nextCursor = &cursor
	}
	return page, nextCursor
}

// serveNewsSnapshot serves strictly from the cache (cached_only emergency mode).
// Lazily builds the cache on first read if it's missing/empty, and kicks a
// background rebuild when stale — even the escape hatch never fossilizes.
func serveNewsSnapshot(db *gorm.DB, tenantID string, lastTimestamp time.Time, lastID uuid.UUID, slideLimit int, seenIDs []uuid.UUID) ([]StorySlide, *string) {
	var snap models.NewsSnapshot
	err := db.Where("tenant_id = ?", tenantID).First(&snap).Error
	if err != nil || len(snap.Slides) == 0 {
		_, _ = buildNewsSnapshot(db, tenantID)
		if err := db.Where("tenant_id = ?", tenantID).First(&snap).Error; err != nil {
			return []StorySlide{}, nil
		}
	} else if snap.Dirty || time.Since(snap.BuiltAt) > newsSnapshotTTL {
		startSnapshotRebuild(db, tenantID)
	}

	var all []StorySlide
	if len(snap.Slides) == 0 || json.Unmarshal(snap.Slides, &all) != nil {
		return []StorySlide{}, nil
	}
	return paginateStorySlides(all, lastTimestamp, lastID, slideLimit, seenIDs)
}

// serveStoryNewsFeed is the News-feed serving orchestrator — the dynamic
// combination of live + cached. The PRODUCT is live assembly ("write-time
// intelligence, read-time freshness"): every request reflects current story
// state. The cache is a freshness-bounded shortcut, never authoritative:
//
//   - cache fresh (≤TTL) and clean (no story gained a member since build) →
//     serve it, saving the ~100-300ms live query;
//   - cache stale, dirty, or missing → assemble LIVE for this request and
//     refresh the cache in the background for the next reader;
//   - NewsFeedMode="cached_only" → emergency escape hatch, cache always
//     (admin-disable switch for the live path).
func serveStoryNewsFeed(
	db *gorm.DB,
	tenantID string,
	config models.RankingConfig,
	lastTimestamp time.Time,
	lastID uuid.UUID,
	slideLimit int,
	waitSeen func() []uuid.UUID,
) ([]StorySlide, *string) {
	if config.NewsFeedMode == "cached_only" {
		return serveNewsSnapshot(db, tenantID, lastTimestamp, lastID, slideLimit, waitSeen())
	}

	// Live mode (default; legacy "precompute"/"on_demand" values fold in here).
	// Serve-stale-while-revalidate: a request is never blocked on inline
	// assembly while ANY cache younger than newsSnapshotMaxStale exists — it
	// serves instantly; dirty/expired only decide whether a background rebuild
	// is kicked. Freshness stays bounded (rebuild lands within seconds of a
	// news event), latency stays flat. The seen query (kicked by the caller)
	// runs concurrently with the cache lookup.
	//
	// The cache can only answer requests it actually COVERS: its cursor must
	// be one of its own slides, and the page it produces must be non-empty.
	// Deep scrolls past the cached slides — or a session that has already
	// seen all of them — fall through to live assembly over the full corpus
	// instead of dead-ending the feed with an empty page.
	if all, builtAt, dirty, ok := loadCachedSnapshot(db, tenantID); ok {
		age := time.Since(builtAt)
		if age <= newsSnapshotMaxStale {
			if dirty || age > newsSnapshotTTL {
				startSnapshotRebuild(db, tenantID)
			}
			cursorCovered := lastID == uuid.Nil
			if !cursorCovered {
				for _, s := range all {
					if s.Featured.StoryID == lastID {
						cursorCovered = true
						break
					}
				}
			}
			if cursorCovered {
				slides, nextCursor := paginateStorySlides(all, lastTimestamp, lastID, slideLimit, waitSeen())
				if len(slides) > 0 {
					return slides, nextCursor
				}
			}
		}
	}

	// Cache can't serve this request (cold, idle past the stale ceiling, deep
	// cursor, or seen-exhausted page) — assemble inline from the full corpus.
	slides, nextCursor := assembleStoryNewsFeed(
		db, tenantID, config, lastTimestamp, lastID, slideLimit, waitSeen(),
	)
	startSnapshotRebuild(db, tenantID)
	return slides, nextCursor
}

// ─── Write-time related-story computation ──────────────────────────────────
//
// Related-story ORDER is a quality concern, not a freshness concern — so it is
// computed when a story CHANGES (gains a member), not when a user reads. The
// ordered ids land on topics.related_ids; the read path hydrates them fresh.
// This keeps cross-encoder quality entirely off the request path.

// storyRelatedRefreshInFlight debounces per-story recomputation (a burst of
// same-story ingests collapses to one refresh).
var storyRelatedRefreshInFlight sync.Map

// storyRelatedWorkers bounds concurrent write-time refreshes. Bulk
// classification (startup backfill / admin recluster) fires one refresh per
// story; unbounded, hundreds of goroutines each holding DB queries + a rerank
// HTTP call would starve the read path.
var storyRelatedWorkers = make(chan struct{}, 2)

// refreshStoryRelated recomputes one story's ordered related-story list:
// centroid kNN candidates, cross-encoder reranked when NewsRerankEnabled
// (pure quality knob — disabled means centroid order, still precomputed).
// Best-effort: any failure leaves the previous list in place.
func refreshStoryRelated(db *gorm.DB, tenantID string, storyID uuid.UUID) {
	if _, busy := storyRelatedRefreshInFlight.LoadOrStore(storyID, true); busy {
		return
	}
	defer storyRelatedRefreshInFlight.Delete(storyID)

	storyRelatedWorkers <- struct{}{}
	defer func() { <-storyRelatedWorkers }()

	// NOTE: an empty result is STORED (as []) — "computed, nothing genuinely
	// related" is a real answer that stops the read path from re-running a
	// live kNN for this story on every request.
	ids := relatedKNNIDs(db, tenantID, storyID, storyRelatedPoolLimit*2)

	if loadTenantConfig(db, tenantID).NewsRerankEnabled && len(ids) > 1 {
		// Hydrate candidate + self summaries in ONE batched query, rerank
		// against this story's lead headline, keep the reranked id order.
		all := make([]uuid.UUID, 0, len(ids)+1)
		all = append(all, ids...)
		all = append(all, storyID)
		var metas []models.Topic
		db.Select(topicMetaColumns).
			Where("tenant_id = ? AND public_id IN ?", tenantID, all).
			Find(&metas)
		metaByID := make(map[uuid.UUID]models.Topic, len(metas))
		for _, t := range metas {
			metaByID[t.PublicID] = t
		}
		leads := leadSummariesByStory(db, tenantID, all, metaByID)

		self := leads[storyID]
		query := strings.TrimSpace(self.Title + " " + self.Excerpt)
		if query == "" {
			query = metaByID[storyID].Label
		}
		summaries := make([]StorySummary, 0, len(ids))
		for _, rid := range ids {
			if s, ok := leads[rid]; ok && s.LeadID != uuid.Nil {
				summaries = append(summaries, s)
			}
		}
		if len(summaries) > 1 && query != "" {
			reranked := rerankStorySummaries(query, summaries)
			ids = ids[:0]
			for _, s := range reranked {
				ids = append(ids, s.StoryID)
			}
		}
	}

	raw := make([]string, len(ids))
	for i, id := range ids {
		raw[i] = id.String()
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return
	}
	db.Model(&models.Topic{}).
		Where("public_id = ?", storyID).
		UpdateColumn("related_ids", datatypes.JSON(data))
}

type storyRerankResponse struct {
	Scores []float64 `json:"scores"`
}

// rerankStorySummaries reorders related-story candidates with the Enrichment
// cross-encoder, keyed on the story's lead headline. Called ONLY from
// refreshStoryRelated (write time) — never on the read path. On any failure it
// returns the input unchanged (centroid order); rerank is enrichment, not a
// hard requirement.
func rerankStorySummaries(query string, summaries []StorySummary) []StorySummary {
	baseURL := enrichmentBaseURL()
	token := enrichmentServiceToken()
	if baseURL == "" || token == "" || len(summaries) == 0 {
		return summaries
	}

	// Cap pair texts — long excerpts balloon cross-encoder latency for no
	// ordering gain (the headline + first sentences carry the signal).
	capText := func(s string) string {
		if r := []rune(s); len(r) > 300 {
			return string(r[:300])
		}
		return s
	}
	query = capText(query)
	candidates := make([]string, len(summaries))
	for i, s := range summaries {
		text := strings.TrimSpace(s.Title + " " + s.Excerpt)
		if text == "" {
			text = s.Label
		}
		candidates[i] = capText(text)
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
