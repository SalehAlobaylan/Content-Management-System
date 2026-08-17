package controllers

import (
	"content-management-system/src/feedcontract"
	"content-management-system/src/intelligence"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// PodsResponse is the API response for the Pods feed
type PodsResponse struct {
	Cursor   *string               `json:"cursor"`
	Items    []PodsItem            `json:"items"`
	CaughtUp bool                  `json:"caught_up"`
	Meta     *feedAvailabilityMeta `json:"meta,omitempty"`
}

// PodsItem represents a single item in the Pods feed
type PodsItem struct {
	ID                   uuid.UUID  `json:"id"`
	Type                 string     `json:"type"`
	Title                string     `json:"title"`
	MediaURL             string     `json:"media_url"`
	ThumbnailURL         string     `json:"thumbnail_url,omitempty"`
	DurationSec          int        `json:"duration_sec,omitempty"`
	ParentID             *string    `json:"parent_id,omitempty"`
	ChapterIndex         *int       `json:"chapter_index,omitempty"`
	ChapterStartMs       *int       `json:"chapter_start_ms,omitempty"`
	ChapterEndMs         *int       `json:"chapter_end_ms,omitempty"`
	DurationBucket       *string    `json:"duration_bucket,omitempty"`
	PlaybackURL          *string    `json:"playback_url,omitempty"`
	PlaybackType         *string    `json:"playback_type,omitempty"`
	FallbackPlaybackURL  *string    `json:"fallback_playback_url,omitempty"`
	FallbackPlaybackType *string    `json:"fallback_playback_type,omitempty"`
	FallbackHasVideo     *bool      `json:"fallback_has_video,omitempty"`
	HasVideo             *bool      `json:"has_video,omitempty"`
	MediaRenditions      any        `json:"media_renditions,omitempty"`
	Author               string     `json:"author,omitempty"`
	SourceName           string     `json:"source_name,omitempty"`
	LikeCount            int        `json:"like_count"`
	CommentCount         int        `json:"comment_count"`
	ShareCount           int        `json:"share_count"`
	PublishedAt          time.Time  `json:"published_at"`
	BookmarkedAt         *time.Time `json:"bookmarked_at,omitempty"`
	IsLiked              bool       `json:"is_liked"`
	IsBookmarked         bool       `json:"is_bookmarked"`
	IsArchived           bool       `json:"is_archived"`
	TranscriptID         *string    `json:"transcript_id,omitempty"`
}

const (
	podsMinDurationSec     = feedcontract.PodsMinDurationSec
	podsSoftMaxDurationSec = 30 * 60
	podsHardMaxDurationSec = feedcontract.PodsHardMaxDuration
)

func hasCursor(pagination *utils.CursorPagination) bool {
	return pagination != nil && pagination.Cursor != ""
}

// GetPodsFeed returns the Pods feed with cursor-based pagination
// GET /api/v1/feed/pods?cursor=xxx&limit=20
func GetPodsFeed(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID, tenantErr := trustedPublicFeedTenant(c)
	if tenantErr != nil {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Public feed tenant is unavailable"})
		return
	}
	availability := currentFeedAvailability(db, tenantID, "media")
	if availability != nil && availability.RetryAfterSeconds != nil {
		c.Header("Retry-After", strconv.Itoa(*availability.RetryAfterSeconds))
	}
	deliveryLanguage, ok := parseDeliveryLanguage(c.Query("content_language"))
	if !ok {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "content_language must be ar, en, or both"})
		return
	}

	// Parse cursor pagination
	pagination, err := utils.ParseCursorParams(c.Query("cursor"), c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid cursor: " + err.Error(),
		})
		return
	}

	// Identity for interaction status / seen-filtering. Authenticated callers
	// are scoped to their verified user id only; anonymous callers to their own
	// session_id. A client-supplied ?user_id is never trusted, and an
	// authenticated caller cannot pass ?session_id to read someone else's state.
	userIDStr, sessionID := readIdentity(c)
	// Repetition suppression is applied while an identity is present. Frozen
	// sessions may deliberately place soft-suppressed items after unseen items
	// when the corpus is exhausted; explicit hides are never recycled.
	var seenIDs []uuid.UUID
	if sessionID != "" || userIDStr != "" {
		seenIDs = fetchPodsSuppressedIDs(db, sessionID, userIDStr, loadTenantConfig(db, tenantID), time.Now().UTC())
	}
	recycleSuppressed, _ := c.Get(podsRecycleSuppressedContextKey)
	allowSuppressedRecycle, _ := recycleSuppressed.(bool)

	config := loadTenantConfig(db, tenantID)
	durationTargetMinutes := parseDurationPreference(c.Query("duration"))
	atomizedFeedSchema := supportsAtomizedPodsSchema(db)

	// ------ Ranked path (when intelligence is active) ------
	if config.IsActive {
		// Fetch items for ranking — try time window first, then fall back to all
		var allItems []models.ContentItem
		baseQuery := podsEligibleMediaQuery(db, tenantID, atomizedFeedSchema)
		baseQuery = applyDeliveryLanguage(baseQuery, deliveryLanguage)
		baseQuery = applyDurationPreference(baseQuery, durationTargetMinutes)

		// First try: items from the configured freshness window (minimum 30 days)
		windowDays := config.FreshnessDecayHours / 24
		if windowDays < 30 {
			windowDays = 30
		}
		broadQuery := baseQuery.Session(&gorm.Session{}).
			Where("COALESCE(published_at, created_at) > ?", time.Now().AddDate(0, 0, -windowDays)).
			Order("COALESCE(published_at, created_at) DESC").Limit(200)
		if err := broadQuery.Find(&allItems).Error; err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to fetch feed: " + err.Error()})
			return
		}

		// Fallback: if not enough items to fill multiple pages, fetch all READY items
		if len(allItems) < 200 {
			baseQuery.Session(&gorm.Session{}).Order("COALESCE(published_at, created_at) DESC").Limit(200).Find(&allItems)
		}
		allItems = excludeCollapsedRedundancyMembers(db, tenantID, allItems)

		// Score items
		contentIDs := extractPublicIDs(allItems)
		flagMap := LoadContentFlags(db, tenantID, contentIDs)
		velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, time.Now())
		scored := ScoreItems(allItems, config, flagMap, velocityData, time.Now())
		scored, preferenceEligible := applyPreferenceFeedHook(db, tenantID, userIDStr, scored)
		scored = applyIntelligenceFeedHooks(db, tenantID, scored)
		scored = spaceScoredSiblingChapters(scored)
		// Filter out already-seen items
		if len(seenIDs) > 0 {
			if allowSuppressedRecycle && !hasCursor(pagination) {
				scored = prioritizeScoredPodsForSession(scored, seenIDs, fetchPodsHardHiddenIDs(db, sessionID, userIDStr))
			} else {
				scored = filterScoredPodsByIDs(scored, seenIDs)
			}
		}
		// Apply cursor-based pagination over scored results
		startIdx := 0
		if !pagination.Timestamp.IsZero() {
			found := false
			for i, s := range scored {
				if s.Item.PublicID == pagination.LastID {
					startIdx = i + 1
					found = true
					break
				}
			}
			// Fallback: if the cursor item wasn't found (scores shifted between requests),
			// find the closest position by timestamp to avoid restarting from page 1
			if !found {
				for i, s := range scored {
					var itemTs time.Time
					if s.Item.PublishedAt != nil {
						itemTs = *s.Item.PublishedAt
					} else {
						itemTs = s.Item.CreatedAt
					}
					if !itemTs.After(pagination.Timestamp) {
						startIdx = i
						break
					}
				}
			}
		}

		endIdx := startIdx + pagination.Limit
		var nextCursor *string
		hasMore := endIdx < len(scored)
		if endIdx > len(scored) {
			endIdx = len(scored)
		}

		pageItems := scored[startIdx:endIdx]
		if hasMore && len(pageItems) > 0 {
			lastItem := pageItems[len(pageItems)-1].Item
			var ts time.Time
			if lastItem.PublishedAt != nil {
				ts = *lastItem.PublishedAt
			} else {
				ts = lastItem.CreatedAt
			}
			cursor := utils.EncodeCursor(ts, lastItem.PublicID)
			nextCursor = &cursor
		}

		// Extract items for interaction lookup
		items := make([]models.ContentItem, len(pageItems))
		for i, s := range pageItems {
			items[i] = s.Item
		}

		likedMap := make(map[uuid.UUID]bool)
		bookmarkedMap := make(map[uuid.UUID]bool)
		if sessionID != "" || userIDStr != "" {
			likedMap, bookmarkedMap = getInteractionStatus(db, items, sessionID, userIDStr)
		}

		responseItems := make([]PodsItem, len(items))
		for i, item := range items {
			responseItems[i] = mapToPodsItem(item, likedMap[item.PublicID], bookmarkedMap[item.PublicID])
		}

		c.JSON(http.StatusOK, PodsResponse{Cursor: nextCursor, Items: responseItems, CaughtUp: len(responseItems) == 0 && !hasCursor(pagination), Meta: availability})
		if !isFeedIntegritySynthetic(c) {
			recordPodsServe(db, tenantID, items, pagination.Limit, durationTargetMinutes)
		}
		boosted := int64(0)
		for _, item := range pageItems {
			if item.ScoreBreakdown.Preference > 0 {
				boosted++
			}
		}
		if !isFeedIntegritySynthetic(c) {
			recordPreferenceServes(db, tenantID, preferenceEligible, boosted, int64(len(items)))
		}
		return
	}

	// ------ Chronological path (default) ------

	// Query for VIDEO and PODCAST content with a valid media URL.
	// Use COALESCE(published_at, created_at) so items with NULL published_at
	// are still ordered and reachable by cursor pagination.
	query := applyDeliveryLanguage(podsEligibleMediaQuery(db, tenantID, atomizedFeedSchema), deliveryLanguage).
		Order("COALESCE(published_at, created_at) DESC, public_id DESC")
	query = applyDurationPreference(query, durationTargetMinutes)

	// Apply cursor if provided
	if !pagination.Timestamp.IsZero() {
		query = query.Where(
			"(COALESCE(published_at, created_at) < ? OR (COALESCE(published_at, created_at) = ? AND public_id < ?))",
			pagination.Timestamp, pagination.Timestamp, pagination.LastID,
		)
	}

	// Exclude already-seen items
	if len(seenIDs) > 0 && !(allowSuppressedRecycle && !hasCursor(pagination)) {
		query = query.Where("public_id NOT IN ?", seenIDs)
	}

	// Fetch items + 1 to check for next page
	var items []models.ContentItem
	if err := query.Limit((pagination.Limit * 3) + 1).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{
			Code:    http.StatusInternalServerError,
			Message: "Failed to fetch feed: " + err.Error(),
		})
		return
	}
	items = excludeCollapsedRedundancyMembers(db, tenantID, items)
	if allowSuppressedRecycle && !hasCursor(pagination) && len(seenIDs) > 0 {
		items = prioritizePodsForSession(items, seenIDs, fetchPodsHardHiddenIDs(db, sessionID, userIDStr))
	}

	// Keep the cursor boundary chronological even when preferences reorder the
	// returned page. That makes the cursor stable while allowing a deliberately
	// bounded preference boost within the current chronological window.
	items = spaceSiblingChapters(items)
	var nextCursor *string
	hasMore := len(items) > pagination.Limit
	var cursorItem *models.ContentItem
	if hasMore {
		boundary := items[pagination.Limit-1]
		cursorItem = &boundary
		items = items[:pagination.Limit] // trim to limit
	}

	// Get last item for cursor
	if cursorItem != nil {
		lastItem := *cursorItem
		var ts time.Time
		if lastItem.PublishedAt != nil {
			ts = *lastItem.PublishedAt
		} else {
			ts = lastItem.CreatedAt
		}
		cursor := utils.EncodeCursor(ts, lastItem.PublicID)
		nextCursor = &cursor
	}

	items, boosted, preferenceEligible := applyChronologicalPreferenceOrder(db, tenantID, userIDStr, items)

	// Get interaction status if session/user provided
	likedMap := make(map[uuid.UUID]bool)
	bookmarkedMap := make(map[uuid.UUID]bool)
	if sessionID != "" || userIDStr != "" {
		likedMap, bookmarkedMap = getInteractionStatus(db, items, sessionID, userIDStr)
	}

	// Map to response
	responseItems := make([]PodsItem, len(items))
	for i, item := range items {
		responseItems[i] = mapToPodsItem(item, likedMap[item.PublicID], bookmarkedMap[item.PublicID])
	}

	c.JSON(http.StatusOK, PodsResponse{
		Cursor:   nextCursor,
		Items:    responseItems,
		CaughtUp: len(responseItems) == 0 && !hasCursor(pagination),
		Meta:     availability,
	})
	if !isFeedIntegritySynthetic(c) {
		recordPodsServe(db, tenantID, items, pagination.Limit, durationTargetMinutes)
		recordPreferenceServes(db, tenantID, preferenceEligible, int64(boosted), int64(len(items)))
	}
}

func uuidMembership(ids []uuid.UUID) map[uuid.UUID]struct{} {
	set := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func filterScoredPodsByIDs(items []ScoredItem, excludedIDs []uuid.UUID) []ScoredItem {
	excluded := uuidMembership(excludedIDs)
	filtered := make([]ScoredItem, 0, len(items))
	for _, item := range items {
		if _, skip := excluded[item.Item.PublicID]; !skip {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// prioritizePodsForSession preserves unseen-first delivery while allowing a
// frozen session to fill from softly suppressed inventory after the current
// corpus is exhausted. Explicit hides are always removed.
func prioritizePodsForSession(items []models.ContentItem, suppressedIDs, hardHiddenIDs []uuid.UUID) []models.ContentItem {
	suppressed := uuidMembership(suppressedIDs)
	hardHidden := uuidMembership(hardHiddenIDs)
	ordered := make([]models.ContentItem, 0, len(items))
	deferred := make([]models.ContentItem, 0, len(items))
	for _, item := range items {
		if _, hidden := hardHidden[item.PublicID]; hidden {
			continue
		}
		if _, seen := suppressed[item.PublicID]; seen {
			deferred = append(deferred, item)
		} else {
			ordered = append(ordered, item)
		}
	}
	return append(ordered, deferred...)
}

func prioritizeScoredPodsForSession(items []ScoredItem, suppressedIDs, hardHiddenIDs []uuid.UUID) []ScoredItem {
	suppressed := uuidMembership(suppressedIDs)
	hardHidden := uuidMembership(hardHiddenIDs)
	ordered := make([]ScoredItem, 0, len(items))
	deferred := make([]ScoredItem, 0, len(items))
	for _, item := range items {
		if _, hidden := hardHidden[item.Item.PublicID]; hidden {
			continue
		}
		if _, seen := suppressed[item.Item.PublicID]; seen {
			deferred = append(deferred, item)
		} else {
			ordered = append(ordered, item)
		}
	}
	return append(ordered, deferred...)
}

// excludeCollapsedRedundancyMembers is deliberately an inventory filter, not
// cursor/session state: once a human confirms a family, only its canonical
// member may enter Pods until the family is dissolved or collapse is off.
func excludeCollapsedRedundancyMembers(db *gorm.DB, tenantID string, items []models.ContentItem) []models.ContentItem {
	if len(items) == 0 {
		return items
	}
	var policy models.RedundancyPolicy
	if db.Where("tenant_id = ?", tenantID).First(&policy).Error != nil || !policy.Enabled || !policy.CollapseEnabled {
		return items
	}
	ids := extractPublicIDs(items)
	var hidden []uuid.UUID
	db.Table("redundancy_family_members m").
		Joins("JOIN redundancy_families f ON f.id = m.family_id").
		Where("m.tenant_id = ? AND m.role = ? AND m.ended_at IS NULL AND f.status = ? AND m.content_item_id IN ?", tenantID, "redundant", "active", ids).
		Pluck("m.content_item_id", &hidden)
	if len(hidden) == 0 {
		return items
	}
	hiddenSet := make(map[uuid.UUID]struct{}, len(hidden))
	for _, id := range hidden {
		hiddenSet[id] = struct{}{}
	}
	filtered := items[:0]
	for _, item := range items {
		if _, ok := hiddenSet[item.PublicID]; !ok {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// recordPodsServe fires the Ranking/Intelligence serve-side telemetry
// (impressions + demand stats) for one Pods response. Runs after the
// response is written and in its own goroutine — the serve path never waits
// on telemetry.
func recordPodsServe(db *gorm.DB, tenantID string, items []models.ContentItem, requestedLimit, durationTargetMinutes int) {
	served := make([]models.ContentItem, len(items))
	copy(served, items)
	durationBucket := ""
	if durationTargetMinutes > 0 {
		durationBucket = intelligence.BucketLabelForDuration(durationTargetMinutes * 60)
	}
	go intelligence.RecordServe(db, intelligence.ServeRecord{
		TenantID:       tenantID,
		Items:          served,
		RequestedLimit: requestedLimit,
		DurationBucket: durationBucket,
	})
}

// GetNewsFeed returns the News feed with cursor-based pagination
// GET /api/v1/feed/news?window=today|week|month&cursor=xxx&limit=10
func GetNewsFeed(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID, tenantErr := trustedPublicFeedTenant(c)
	if tenantErr != nil {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Public feed tenant is unavailable"})
		return
	}
	availability := currentFeedAvailability(db, tenantID, "news")
	if availability != nil && availability.RetryAfterSeconds != nil {
		c.Header("Retry-After", strconv.Itoa(*availability.RetryAfterSeconds))
	}

	// Parse cursor pagination
	pagination, err := utils.ParseCursorParams(c.Query("cursor"), c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid cursor: " + err.Error(),
		})
		return
	}

	// For news, we want slides (1 featured + 3 related each)
	// Limit means number of slides
	slideLimit := pagination.Limit
	if slideLimit > 20 {
		slideLimit = 20
	}
	if slideLimit <= 0 {
		slideLimit = 10
	}

	// Session view-tracking — exclude_seen drops slides this session already saw
	// (the client reports views against the slide's lead member id). Fetched
	// concurrently with the cache lookup: both are WAN round-trips and
	// independent, so serial execution doubles the floor latency.
	userIDStr, sessionID := readIdentity(c)
	var seenIDs []uuid.UUID
	seenDone := make(chan struct{})
	if c.Query("exclude_seen") == "true" && (sessionID != "" || userIDStr != "") {
		go func() {
			defer close(seenDone)
			seenIDs = fetchSeenIDs(db, sessionID, userIDStr)
		}()
	} else {
		close(seenDone)
	}

	// Load ranking config (in-process cached; also carries the Phase-13 story
	// + feed-mode knobs).
	config := loadTenantConfig(db, tenantID)
	circ := circulationContextFor(db, tenantID, c.Query("window"), time.Now())

	// News feed = story-slides, assembled LIVE by default ("write-time
	// intelligence, read-time freshness") behind a freshness-bounded
	// read-through cache. See serveStoryNewsFeed for the full policy. The seen
	// resolver lets the cache lookup run concurrently with the seen query.
	waitSeen := func() []uuid.UUID {
		<-seenDone
		return seenIDs
	}
	slides, nextCursor, serveMeta, err := serveStoryNewsFeed(
		db, tenantID, config, circ, pagination.Timestamp, pagination.LastID, slideLimit, waitSeen, userIDStr, !isFeedIntegritySynthetic(c),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "News feed is temporarily unavailable"})
		return
	}
	// A story snapshot is shared across viewers, while like/bookmark state is
	// identity-specific. Hydrate only the lead content IDs after assembly so a
	// cached News slide never leaks another viewer's interaction state.
	if sessionID != "" || userIDStr != "" {
		hydrateStoryInteractionStatus(db, slides, sessionID, userIDStr)
	}
	if isFeedIntegritySynthetic(c) {
		c.Header("X-Wahb-Feed-Source", serveMeta.Source)
		c.Header("X-Wahb-Snapshot-Age-Ms", strconv.FormatInt(serveMeta.SnapshotAge.Milliseconds(), 10))
		c.Header("X-Wahb-Snapshot-Window", serveMeta.Window)
		c.Header("X-Wahb-Snapshot-Built-At", serveMeta.SnapshotBuiltAt.UTC().Format(time.RFC3339Nano))
		c.Header("X-Wahb-Snapshot-Dirty", strconv.FormatBool(serveMeta.SnapshotDirty))
	}

	c.JSON(http.StatusOK, StoryNewsResponse{
		Cursor: nextCursor,
		Slides: slides,
		Meta:   availability,
	})
}

func hydrateStoryInteractionStatus(db *gorm.DB, slides []StorySlide, sessionID, userIDStr string) {
	ids := make([]uuid.UUID, 0, len(slides)*4)
	for _, slide := range slides {
		ids = append(ids, slide.Featured.LeadID)
		for _, related := range slide.Related {
			ids = append(ids, related.LeadID)
		}
	}
	if len(ids) == 0 {
		return
	}
	var items []models.ContentItem
	db.Select("public_id").Where("public_id IN ?", ids).Find(&items)
	liked, bookmarked := getInteractionStatus(db, items, sessionID, userIDStr)
	apply := func(summary *StorySummary) {
		summary.IsLiked = liked[summary.LeadID]
		summary.IsBookmarked = bookmarked[summary.LeadID]
	}
	for index := range slides {
		apply(&slides[index].Featured.StorySummary)
		for relatedIndex := range slides[index].Related {
			apply(&slides[index].Related[relatedIndex])
		}
	}
}

// Helper functions

func getInteractionStatus(db *gorm.DB, items []models.ContentItem, sessionID, userIDStr string) (map[uuid.UUID]bool, map[uuid.UUID]bool) {
	likedMap := make(map[uuid.UUID]bool)
	bookmarkedMap := make(map[uuid.UUID]bool)

	if len(items) == 0 {
		return likedMap, bookmarkedMap
	}

	// Collect content IDs
	contentIDs := make([]uuid.UUID, len(items))
	for i, item := range items {
		contentIDs[i] = item.PublicID
	}

	// Query interactions
	var interactions []models.UserInteraction
	query := db.Model(&models.UserInteraction{}).
		Where("content_item_id IN ?", contentIDs).
		Where("type IN ?", []models.InteractionType{models.InteractionTypeLike, models.InteractionTypeBookmark})

	// Build identity condition: match session_id OR user_id (both scoped to content_item_id IN above)
	if sessionID != "" && userIDStr != "" {
		if userID, err := uuid.Parse(userIDStr); err == nil {
			query = query.Where("session_id = ? OR user_id = ?", sessionID, userID)
		} else {
			query = query.Where("session_id = ?", sessionID)
		}
	} else if sessionID != "" {
		query = query.Where("session_id = ?", sessionID)
	} else if userIDStr != "" {
		if userID, err := uuid.Parse(userIDStr); err == nil {
			query = query.Where("user_id = ?", userID)
		}
	}

	query.Find(&interactions)

	for _, interaction := range interactions {
		if interaction.Type == models.InteractionTypeLike {
			likedMap[interaction.ContentItemID] = true
		}
		if interaction.Type == models.InteractionTypeBookmark {
			bookmarkedMap[interaction.ContentItemID] = true
		}
	}

	return likedMap, bookmarkedMap
}

func parseDurationPreference(raw string) int {
	if raw == "" {
		return 0
	}
	minutes, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	switch minutes {
	case 5, 10, 15, 20, 30, 40:
		return minutes
	default:
		return 0
	}
}

func applyDurationPreference(query *gorm.DB, targetMinutes int) *gorm.DB {
	if targetMinutes <= 0 {
		return query
	}
	targetSec := targetMinutes * 60
	minSec := int(float64(targetSec) * 0.6)
	maxSec := int(float64(targetSec) * 1.6)
	if minSec < podsMinDurationSec {
		minSec = podsMinDurationSec
	}
	if maxSec > podsHardMaxDurationSec {
		maxSec = podsHardMaxDurationSec
	}
	return query.Where("duration_sec IS NOT NULL AND duration_sec BETWEEN ? AND ?", minSec, maxSec)
}

func supportsAtomizedPodsSchema(db *gorm.DB) bool {
	return feedcontract.SupportsAtomizedPodsSchema(db)
}

func supportsStorageStateSchema(db *gorm.DB) bool {
	return feedcontract.SupportsStorageStateSchema(db)
}

func podsEligibleMediaQuery(db *gorm.DB, tenantID string, atomizedFeedSchema bool) *gorm.DB {
	q := feedcontract.PodsEligibleMediaQuery(db, tenantID, atomizedFeedSchema)
	return applyActiveGenerationMembership(db, q, tenantID, "media", "feed_unit", "content_items.public_id")
}

func chapterSiblingKey(item models.ContentItem) string {
	if item.ParentContentItemID != nil {
		return item.ParentContentItemID.String()
	}
	return item.PublicID.String()
}

func spaceSiblingChapters(items []models.ContentItem) []models.ContentItem {
	if len(items) < 2 {
		return items
	}
	out := make([]models.ContentItem, 0, len(items))
	remaining := append([]models.ContentItem(nil), items...)
	for len(remaining) > 0 {
		pick := 0
		if len(out) > 0 {
			lastKey := chapterSiblingKey(out[len(out)-1])
			for i, item := range remaining {
				if chapterSiblingKey(item) != lastKey {
					pick = i
					break
				}
			}
		}
		out = append(out, remaining[pick])
		remaining = append(remaining[:pick], remaining[pick+1:]...)
	}
	return out
}

func spaceScoredSiblingChapters(items []ScoredItem) []ScoredItem {
	if len(items) < 2 {
		return items
	}
	out := make([]ScoredItem, 0, len(items))
	remaining := append([]ScoredItem(nil), items...)
	for len(remaining) > 0 {
		pick := 0
		if len(out) > 0 {
			lastKey := chapterSiblingKey(out[len(out)-1].Item)
			for i, item := range remaining {
				if chapterSiblingKey(item.Item) != lastKey {
					pick = i
					break
				}
			}
		}
		out = append(out, remaining[pick])
		remaining = append(remaining[:pick], remaining[pick+1:]...)
	}
	return out
}

func mapToPodsItem(item models.ContentItem, isLiked, isBookmarked bool) PodsItem {
	result := PodsItem{
		ID:           item.PublicID,
		Type:         string(item.Type),
		LikeCount:    item.LikeCount,
		CommentCount: item.CommentCount,
		ShareCount:   item.ShareCount,
		IsLiked:      isLiked,
		IsBookmarked: isBookmarked,
		IsArchived:   item.Status == models.ContentStatusArchived,
	}

	if item.Title != nil {
		result.Title = *item.Title
	}
	if item.MediaURL != nil {
		result.MediaURL = *item.MediaURL
	}
	if item.ParentContentItemID != nil {
		parentID := item.ParentContentItemID.String()
		result.ParentID = &parentID
	}
	result.ChapterIndex = item.ChapterIndex
	result.ChapterStartMs = item.ChapterStartMs
	result.ChapterEndMs = item.ChapterEndMs
	result.DurationBucket = item.DurationBucket
	if item.PlaybackURL != nil {
		result.PlaybackURL = item.PlaybackURL
	} else if item.MediaURL != nil {
		result.PlaybackURL = item.MediaURL
	}
	result.PlaybackType = item.PlaybackType
	result.FallbackPlaybackURL = item.FallbackPlaybackURL
	result.HasVideo = item.HasVideo
	if len(item.MediaRenditions) > 0 {
		var renditions any
		if json.Unmarshal(item.MediaRenditions, &renditions) == nil {
			result.MediaRenditions = renditions
		}
	}
	result.FallbackPlaybackType, result.FallbackHasVideo = typedFallbackMetadata(item)
	if item.ThumbnailURL != nil {
		result.ThumbnailURL = *item.ThumbnailURL
	}
	if item.DurationSec != nil {
		result.DurationSec = *item.DurationSec
	}
	if item.Author != nil {
		result.Author = *item.Author
	}
	if item.SourceName != nil {
		result.SourceName = *item.SourceName
	}
	if item.PublishedAt != nil {
		result.PublishedAt = *item.PublishedAt
	}
	if item.TranscriptID != nil {
		tid := item.TranscriptID.String()
		result.TranscriptID = &tid
	}

	return result
}

type storedPlaybackRendition struct {
	Type     string `json:"type"`
	URL      string `json:"url"`
	HasVideo *bool  `json:"has_video"`
}

// typedFallbackMetadata only returns a fallback when the stored rendition
// explicitly records both its media type and video capability. Older rows that
// have only a URL remain playable through their primary source but are never
// guessed into an unsafe player transition.
func typedFallbackMetadata(item models.ContentItem) (*string, *bool) {
	if item.FallbackPlaybackURL == nil || *item.FallbackPlaybackURL == "" || len(item.MediaRenditions) == 0 {
		return nil, nil
	}
	var renditions []storedPlaybackRendition
	if json.Unmarshal(item.MediaRenditions, &renditions) != nil {
		return nil, nil
	}
	for _, rendition := range renditions {
		if rendition.URL != *item.FallbackPlaybackURL || rendition.HasVideo == nil {
			continue
		}
		typeName := rendition.Type
		switch typeName {
		case "hls", "mp4", "audio":
			return &typeName, rendition.HasVideo
		}
	}
	return nil, nil
}
