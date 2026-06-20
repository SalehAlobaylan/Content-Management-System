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
	storyMaxMembers           = 8  // members rendered under a featured story
	newsSnapshotSlideCount    = 60 // slides precomputed into the cache (≈6 pages before deep scroll goes live)
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
	"labeled, last_member_at, related_ids, summary, bullets, summary_built_at, category, created_at, updated_at"

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
	// Source (RSS/TELEGRAM/...) lets the UI distinguish a Telegram channel post
	// (which ingests as format=ARTICLE) from an RSS article for honest badging.
	Source         string    `json:"source,omitempty"`
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
	StoryID      uuid.UUID `json:"story_id"`
	LeadID       uuid.UUID `json:"lead_id"`
	Label        string    `json:"label"`
	LastMemberAt time.Time `json:"last_member_at"`
	Lifecycle    string    `json:"lifecycle"`
	IsCarryover  bool      `json:"is_carryover,omitempty"`
	Reason       string    `json:"reason,omitempty"`
	// Summary + Bullets are the source-grounded AI digest of the WHOLE story
	// (Slice 8). Populated on the FEATURED story only; empty when not yet
	// digested — the slide then falls back to the lead member's excerpt.
	Summary        string    `json:"summary,omitempty"`
	Bullets        []string  `json:"bullets,omitempty"`
	// Category is the story's news-taxonomy slug (politics/economy/...), shown as
	// the topic chip. Present on featured + related; empty/general → no chip.
	Category       string    `json:"category,omitempty"`
	Title          string    `json:"title,omitempty"`
	Excerpt        string    `json:"excerpt,omitempty"`
	ThumbnailURL   string    `json:"thumbnail_url,omitempty"`
	SourceName     string    `json:"source_name,omitempty"`
	SourceImageURL string    `json:"source_image_url,omitempty"`
	// Format + Source of the LEAD member, so a related-story card badges by its
	// lead post's real type (article/tweet/comment) and detects Telegram leads.
	Format         string    `json:"format,omitempty"`
	Source         string    `json:"source,omitempty"`
	PublishedAt    time.Time `json:"published_at"`
	MemberCount    int       `json:"member_count"`
	// SourceCount is the number of DISTINCT sources among the story's hydrated
	// members — the Radar-style "covered by N sources" signal. Computed for
	// the featured story only (related cards show member_count); 0 = not
	// computed.
	SourceCount  int `json:"source_count,omitempty"`
	LikeCount    int `json:"like_count"`
	CommentCount int `json:"comment_count"`
	ShareCount   int `json:"share_count"`
	ViewCount    int `json:"view_count"`
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
	storyID   uuid.UUID
	score     float64
	newest    time.Time
	newestID  uuid.UUID // id of the newest member — what the client reports as "seen"
	lifecycle string
	carryover bool
	reason    string
	members   []models.ContentItem
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
	circ circulationContext,
	lastTimestamp time.Time,
	lastID uuid.UUID,
	slideLimit int,
	seenIDs []uuid.UUID,
) ([]StorySlide, *string) {
	config = applyLatestPlusPolicy(config, circ.Policy)
	now := circ.Window.Now
	// 1. Pull a recent pool of classified NEWS members and score them with the
	//    existing ranking engine (freshness/engagement/velocity/trending).
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

	windowStart := circ.Window.QueryStart
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
	velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, now)
	scored := ScoreItems(members, config, flagMap, velocityData, now)

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

	storyIDsForOverrides := make([]uuid.UUID, 0, len(order))
	for _, a := range order {
		storyIDsForOverrides = append(storyIDsForOverrides, a.storyID)
	}
	overrides := activeStoryOverrides(db, tenantID, storyIDsForOverrides, now)
	filteredByOverride := order[:0]
	for _, a := range order {
		if ov, ok := overrides[a.storyID]; ok {
			if ov.ExcludeFromFeed {
				continue
			}
			if ov.PinToTop {
				a.score = math.MaxFloat64
				a.reason = "Editor priority"
			} else {
				if ov.ImportanceBoost > 0 && ov.ImportanceBoost != 1 {
					a.score *= ov.ImportanceBoost
					if ov.ImportanceBoost > 1 {
						a.reason = "Editor priority"
					}
				}
				if ov.Suppress {
					a.score *= 0.1
					if a.reason == "" {
						a.reason = "Suppressed"
					}
				}
			}
		}
		filteredByOverride = append(filteredByOverride, a)
	}
	order = filteredByOverride

	sort.SliceStable(order, func(i, j int) bool {
		if order[i].score != order[j].score {
			return order[i].score > order[j].score
		}
		return order[i].newest.After(order[j].newest)
	})

	if circ.Window.Name == models.NewsWindowToday {
		primary := make([]*storyAgg, 0, len(order))
		carryovers := make([]*storyAgg, 0)
		for _, a := range order {
			if !a.newest.Before(circ.Window.PrimaryStart) {
				if a.reason == "" {
					a.reason = "Updated today"
				}
				primary = append(primary, a)
				continue
			}
			if a.score >= circ.Policy.CarryoverMinScore {
				a.carryover = true
				if a.reason == "" {
					a.reason = "Carryover fill"
				}
				carryovers = append(carryovers, a)
			}
		}
		order = primary
		if len(order) < circ.Policy.MinTodayStories {
			need := circ.Policy.MinTodayStories - len(order)
			if need > len(carryovers) {
				need = len(carryovers)
			}
			order = append(order, carryovers[:need]...)
		}
	} else {
		filtered := order[:0]
		for _, a := range order {
			if !a.newest.Before(circ.Window.PrimaryStart) {
				if a.reason == "" {
					a.reason = "Inside selected window"
				}
				filtered = append(filtered, a)
			}
		}
		order = filtered
	}

	for _, a := range order {
		a.lifecycle = storyLifecycle(circ.Policy, circ.Window, a.newest, len(a.members), a.carryover)
	}

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
			featured := buildStoryFeatured(topic, ag, storyMembers, sourceImageByFeedURL)
			slides[idx] = StorySlide{SlideID: uuid.New(), Featured: featured}
			relCandidates[idx] = buildRelatedStories(db, tenantID, topic, ag.storyID, topicByID, pageIDs, circ)
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

func buildStoryFeatured(topic models.Topic, ag *storyAgg, members []models.ContentItem, sourceImageByFeedURL map[string]string) StoryFeatured {
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
	// Story image: prefer the lead member's own post image; if the lead has none,
	// borrow a real image from ANY member that carries one (stories with an image
	// are preferable, and the headline often differs from the member that shipped
	// the photo). Empty when no member has an image — the UI then shows just a
	// small source logo.
	storyImage := derefStr(top.ThumbnailURL)
	if storyImage == "" {
		for _, m := range members {
			if mt := derefStr(m.ThumbnailURL); mt != "" {
				storyImage = mt
				break
			}
		}
	}
	summary := StorySummary{
		StoryID:        ag.storyID,
		LeadID:         top.PublicID,
		Label:          topic.Label,
		LastMemberAt:   ag.newest,
		Lifecycle:      ag.lifecycle,
		IsCarryover:    ag.carryover,
		Reason:         ag.reason,
		Summary:        derefStr(topic.Summary),
		Bullets:        parseStoryBullets(topic.Bullets),
		Category:       derefStr(topic.Category),
		Title:          derefStr(top.Title),
		Excerpt:        derefStr(top.Excerpt),
		ThumbnailURL:   storyImage,
		SourceName:     derefStr(top.SourceName),
		SourceImageURL: sourceImageForItem(top, sourceImageByFeedURL),
		Format:         derefStr(top.Format),
		Source:         string(top.Source),
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
		Source:         string(m.Source),
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
	for _, key := range sourceLookupKeys(derefStr(it.SourceFeedURL), derefStr(it.SourceName)) {
		if value := sourceImageByFeedURL[key]; value != "" {
			return value
		}
	}
	// Telegram channels carry no stored image_url, but their public avatar is
	// derivable from the t.me handle — give them a source image like RSS sources.
	if it.Source == models.SourceTypeTelegram {
		if avatar := telegramAvatarURL(derefStr(it.SourceFeedURL)); avatar != "" {
			return avatar
		}
	}
	return ""
}

// telegramAvatarURL turns a Telegram channel's t.me handle (t.me/<user> or
// t.me/s/<user>, with or without scheme) into its public avatar URL. Telegram's
// userpic endpoint 302-redirects to the channel photo on its CDN, so browsers
// load it directly. Empty when no username can be parsed.
func telegramAvatarURL(feedURL string) string {
	u := strings.TrimSpace(feedURL)
	if u == "" {
		return ""
	}
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.TrimPrefix(u, "www.")
	u = strings.TrimPrefix(u, "t.me/")
	u = strings.TrimPrefix(u, "telegram.me/")
	u = strings.TrimPrefix(u, "s/") // t.me/s/<user>
	u = strings.TrimPrefix(u, "@")
	// Keep only the first path segment (channel username), drop message ids etc.
	if i := strings.IndexAny(u, "/?#"); i >= 0 {
		u = u[:i]
	}
	u = strings.TrimSpace(u)
	if u == "" {
		return ""
	}
	return "https://t.me/i/userpic/320/" + u + ".jpg"
}

// parseStoryBullets decodes the topic's jsonb bullets array (Slice 8). Returns
// nil for a null/invalid column so the slide cleanly falls back to the excerpt.
func parseStoryBullets(raw datatypes.JSON) []string {
	if len(raw) == 0 {
		return nil
	}
	var bullets []string
	if json.Unmarshal(raw, &bullets) != nil {
		return nil
	}
	return bullets
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
	circ circulationContext,
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
		lastMemberAt := itemTime(top)
		if t.LastMemberAt != nil {
			lastMemberAt = *t.LastMemberAt
		}
		out[sid] = StorySummary{
			StoryID:        sid,
			LeadID:         top.PublicID,
			Label:          t.Label,
			LastMemberAt:   lastMemberAt,
			Lifecycle:      storyLifecycle(circ.Policy, circ.Window, lastMemberAt, memberCount, false),
			Category:       derefStr(t.Category),
			Title:          derefStr(top.Title),
			Excerpt:        derefStr(top.Excerpt),
			ThumbnailURL:   derefStr(top.ThumbnailURL),
			SourceName:     derefStr(top.SourceName),
			SourceImageURL: sourceImageForItem(top, sourceImageByFeedURL),
			Format:         derefStr(top.Format),
			Source:         string(top.Source),
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
	circ circulationContext,
) []StorySummary {
	relIDs := relatedCandidateIDs(db, tenantID, topic, storyID)

	cands := make([]uuid.UUID, 0, len(relIDs))
	for _, rid := range relIDs {
		if !excludeIDs[rid] {
			cands = append(cands, rid)
		}
	}
	leads := leadSummariesByStory(db, tenantID, cands, topicByID, circ)

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
// the per-tenant/window news_snapshots cache row (clearing Dirty). Called by the SWR
// background refresh, the admin Refresh endpoint, the classification backfill,
// and lazily when the cache is empty.
func buildNewsSnapshot(db *gorm.DB, tenantID string, window string) (int, error) {
	config := loadTenantConfig(db, tenantID)
	circ := circulationContextFor(db, tenantID, window, time.Now())
	slides, _ := assembleStoryNewsFeed(db, tenantID, config, circ, time.Time{}, uuid.Nil, newsSnapshotSlideCount, nil)
	if slides == nil {
		slides = []StorySlide{}
	}
	data, err := json.Marshal(slides)
	if err != nil {
		return 0, err
	}
	// Atomic upsert on tenant + window avoids the SELECT-then-INSERT race
	// (lazy build on read vs admin Refresh) that a FirstOrCreate would hit.
	// built_at truncated to microseconds (Postgres timestamp precision) so the
	// in-memory copy's built_at compares Equal to what reads see from the DB.
	snap := models.NewsSnapshot{
		TenantID:   tenantID,
		Window:     circ.Window.Name,
		Slides:     datatypes.JSON(data),
		SlideCount: len(slides),
		Dirty:      false,
		BuiltAt:    time.Now().Truncate(time.Microsecond),
	}
	err = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "window"}},
		DoUpdates: clause.AssignmentColumns([]string{"slides", "slide_count", "dirty", "built_at"}),
	}).Create(&snap).Error
	if err != nil {
		return 0, err
	}
	// Seed process memory directly — the next read serves without re-pulling
	// the JSON it just wrote.
	newsSnapshotMem.Store(snapshotMemKey(tenantID, circ.Window.Name), &memCachedSnapshot{tenantID: tenantID, window: circ.Window.Name, slides: slides, builtAt: snap.BuiltAt})
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
	window   string
	slides   []StorySlide
	builtAt  time.Time
}

var newsSnapshotMem sync.Map

func snapshotMemKey(tenantID, window string) string {
	return tenantID + ":" + normalizeNewsWindow(window)
}

// loadCachedSnapshot returns the cached slides plus their freshness header,
// preferring process memory (one tiny header query on the hot path). ok=false
// → no usable cache row at all.
func loadCachedSnapshot(db *gorm.DB, tenantID string, window string) (slides []StorySlide, builtAt time.Time, dirty bool, ok bool) {
	window = normalizeNewsWindow(window)
	var head struct {
		Dirty   bool
		BuiltAt time.Time
	}
	if err := db.Model(&models.NewsSnapshot{}).
		Select("dirty, built_at").
		Where("tenant_id = ? AND window = ?", tenantID, window).
		Scan(&head).Error; err != nil || head.BuiltAt.IsZero() {
		return nil, time.Time{}, false, false
	}
	if raw, exists := newsSnapshotMem.Load(snapshotMemKey(tenantID, window)); exists {
		if mem, ok := raw.(*memCachedSnapshot); ok && mem.tenantID == tenantID && mem.window == window && mem.builtAt.Equal(head.BuiltAt) {
			return mem.slides, head.BuiltAt, head.Dirty, true
		}
	}
	var snap models.NewsSnapshot
	if err := db.Where("tenant_id = ? AND window = ?", tenantID, window).First(&snap).Error; err != nil || len(snap.Slides) == 0 {
		return nil, time.Time{}, false, false
	}
	var all []StorySlide
	if json.Unmarshal(snap.Slides, &all) != nil || len(all) == 0 {
		return nil, time.Time{}, false, false
	}
	newsSnapshotMem.Store(snapshotMemKey(tenantID, window), &memCachedSnapshot{tenantID: tenantID, window: window, slides: all, builtAt: snap.BuiltAt})
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

// snapshotRebuildRunning guards SWR background rebuilds against stampedes per
// tenant+window. Today, week, and month snapshots should not block each other.
var snapshotRebuildRunning sync.Map

// startSnapshotRebuild rebuilds the precompute snapshot in the background.
// No-ops when a rebuild is already in flight.
func startSnapshotRebuild(db *gorm.DB, tenantID string, window string) {
	window = normalizeNewsWindow(window)
	key := snapshotMemKey(tenantID, window)
	if _, loaded := snapshotRebuildRunning.LoadOrStore(key, true); loaded {
		return
	}
	go func() {
		defer snapshotRebuildRunning.Delete(key)
		if _, err := buildNewsSnapshot(db, tenantID, window); err != nil {
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
				if !s.Featured.LastMemberAt.After(lastTimestamp) {
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
		cursor := utils.EncodeCursor(last.Featured.LastMemberAt, last.Featured.StoryID)
		nextCursor = &cursor
	}
	return page, nextCursor
}

// serveNewsSnapshot serves strictly from the cache (cached_only emergency mode).
// Lazily builds the cache on first read if it's missing/empty, and kicks a
// background rebuild when stale — even the escape hatch never fossilizes.
func serveNewsSnapshot(db *gorm.DB, tenantID string, circ circulationContext, lastTimestamp time.Time, lastID uuid.UUID, slideLimit int, seenIDs []uuid.UUID) ([]StorySlide, *string) {
	var snap models.NewsSnapshot
	err := db.Where("tenant_id = ? AND window = ?", tenantID, circ.Window.Name).First(&snap).Error
	if err != nil || len(snap.Slides) == 0 {
		_, _ = buildNewsSnapshot(db, tenantID, circ.Window.Name)
		if err := db.Where("tenant_id = ? AND window = ?", tenantID, circ.Window.Name).First(&snap).Error; err != nil {
			return []StorySlide{}, nil
		}
	} else if snap.Dirty || time.Since(snap.BuiltAt) > newsSnapshotTTL {
		startSnapshotRebuild(db, tenantID, circ.Window.Name)
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
	circ circulationContext,
	lastTimestamp time.Time,
	lastID uuid.UUID,
	slideLimit int,
	waitSeen func() []uuid.UUID,
) ([]StorySlide, *string) {
	if config.NewsFeedMode == "cached_only" {
		return serveNewsSnapshot(db, tenantID, circ, lastTimestamp, lastID, slideLimit, waitSeen())
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
	if all, builtAt, dirty, ok := loadCachedSnapshot(db, tenantID, circ.Window.Name); ok {
		age := time.Since(builtAt)
		if age <= newsSnapshotMaxStale {
			if dirty || age > newsSnapshotTTL {
				startSnapshotRebuild(db, tenantID, circ.Window.Name)
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
		db, tenantID, config, circ, lastTimestamp, lastID, slideLimit, waitSeen(),
	)
	startSnapshotRebuild(db, tenantID, circ.Window.Name)
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

// storySummaryRefreshInFlight + storySummaryWorkers mirror the related-refresh
// debounce/bound for the LLM digest. A separate channel so digest generation
// (a slower LLM call) never starves related-story refresh.
var storySummaryRefreshInFlight sync.Map
var storySummaryWorkers = make(chan struct{}, 2)

// storyDigestMemberLimit caps how many member posts seed a digest — the LLM
// signal saturates well before the full story; the lead dozen carry it.
const storyDigestMemberLimit = 12

// storyDigestMemberTexts pulls the recent READY member posts of a story as the
// LLM digest input (title+excerpt, body fallback). READY-only so the digest is
// grounded in exactly what the feed shows — used by both the write-time refresh
// and the admin backfill so they stay consistent.
func storyDigestMemberTexts(db *gorm.DB, tenantID string, storyID uuid.UUID) []string {
	windowStart := time.Now().AddDate(0, 0, -storyRelatedWindowDays)
	var members []models.ContentItem
	db.Select("title, excerpt, LEFT(body_text, 600) AS body_text, published_at, created_at").
		Where("tenant_id = ? AND type = ? AND status = ? AND topic_id = ?",
			tenantID, models.ContentTypeNews, models.ContentStatusReady, storyID).
		Where("COALESCE(published_at, created_at) > ?", windowStart).
		Order("COALESCE(published_at, created_at) DESC").
		Limit(storyDigestMemberLimit).
		Find(&members)

	texts := make([]string, 0, len(members))
	for _, m := range members {
		t := strings.TrimSpace(derefStr(m.Title) + " " + derefStr(m.Excerpt))
		if t == "" {
			t = strings.TrimSpace(derefStr(m.BodyText))
		}
		if t != "" {
			texts = append(texts, t)
		}
	}
	return texts
}

// refreshStorySummary recomputes one story's source-grounded LLM digest
// (headline lede + bullets) at WRITE time. Harnessed: debounced per story,
// worker-bounded, and GATED — skipped when disabled, when the story is too
// small to be worth a digest (singletons keep their lead excerpt), or when it
// was digested within the min-interval (cost cap on hot stories). Best-effort:
// any failure leaves the previous digest in place; the feed falls back to the
// headline + lead excerpt when there is none.
func refreshStorySummary(db *gorm.DB, tenantID string, storyID uuid.UUID) {
	cfg := loadTenantConfig(db, tenantID)
	if !cfg.StorySummaryEnabled {
		return
	}
	if _, busy := storySummaryRefreshInFlight.LoadOrStore(storyID, true); busy {
		return
	}
	defer storySummaryRefreshInFlight.Delete(storyID)

	storySummaryWorkers <- struct{}{}
	defer func() { <-storySummaryWorkers }()

	var topic models.Topic
	if err := db.Select(topicMetaColumns).
		Where("tenant_id = ? AND public_id = ?", tenantID, storyID).
		First(&topic).Error; err != nil {
		return
	}
	minMembers := cfg.StorySummaryMinMembers
	if minMembers < 1 {
		minMembers = 1
	}
	if topic.ArticleCount < minMembers {
		return // singleton / tiny story — the lead excerpt already suffices
	}
	// Rate-cap regeneration: a hot story re-digests at most once per interval,
	// unless it has no digest yet (first time always runs).
	if topic.SummaryBuiltAt != nil && len(topic.Bullets) > 0 {
		interval := time.Duration(cfg.StorySummaryMinIntervalMinutes) * time.Minute
		if interval > 0 && time.Since(*topic.SummaryBuiltAt) < interval {
			return
		}
	}

	texts := storyDigestMemberTexts(db, tenantID, storyID)
	if len(texts) < minMembers {
		return
	}

	summary, bullets, category, err := generateStorySummaryViaEnrichment(texts)
	if err != nil || len(bullets) == 0 {
		return // best-effort — keep the previous digest (or none)
	}

	bulletsJSON, err := json.Marshal(bullets)
	if err != nil {
		return
	}
	now := time.Now()
	db.Model(&models.Topic{}).
		Where("public_id = ?", storyID).
		Updates(map[string]interface{}{
			"summary":          summary,
			"bullets":          datatypes.JSON(bulletsJSON),
			"summary_built_at": now,
			"category":         normalizeStoryCategory(category),
		})
}

// normalizeStoryCategory keeps the stored slug non-empty so the backfill's
// "category IS NULL" selection drains (an attempted-but-uncategorized story
// lands on "general", which the UI renders as no chip).
func normalizeStoryCategory(category string) string {
	cat := strings.ToLower(strings.TrimSpace(category))
	if cat == "" {
		return "general"
	}
	return cat
}

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
		circ := circulationContextFor(db, tenantID, models.NewsWindowMonth, time.Now())
		leads := leadSummariesByStory(db, tenantID, all, metaByID, circ)

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
