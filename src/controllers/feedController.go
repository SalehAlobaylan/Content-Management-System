package controllers

import (
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

// ForYouResponse is the API response for the For You feed
type ForYouResponse struct {
	Cursor *string      `json:"cursor"`
	Items  []ForYouItem `json:"items"`
}

// ForYouItem represents a single item in the For You feed
type ForYouItem struct {
	ID                  uuid.UUID  `json:"id"`
	Type                string     `json:"type"`
	Title               string     `json:"title"`
	MediaURL            string     `json:"media_url"`
	ThumbnailURL        string     `json:"thumbnail_url,omitempty"`
	DurationSec         int        `json:"duration_sec,omitempty"`
	ParentID            *string    `json:"parent_id,omitempty"`
	ChapterIndex        *int       `json:"chapter_index,omitempty"`
	ChapterStartMs      *int       `json:"chapter_start_ms,omitempty"`
	ChapterEndMs        *int       `json:"chapter_end_ms,omitempty"`
	DurationBucket      *string    `json:"duration_bucket,omitempty"`
	PlaybackURL         *string    `json:"playback_url,omitempty"`
	PlaybackType        *string    `json:"playback_type,omitempty"`
	FallbackPlaybackURL *string    `json:"fallback_playback_url,omitempty"`
	HasVideo            *bool      `json:"has_video,omitempty"`
	MediaRenditions     any        `json:"media_renditions,omitempty"`
	Author              string     `json:"author,omitempty"`
	SourceName          string     `json:"source_name,omitempty"`
	LikeCount           int        `json:"like_count"`
	CommentCount        int        `json:"comment_count"`
	ShareCount          int        `json:"share_count"`
	PublishedAt         time.Time  `json:"published_at"`
	BookmarkedAt        *time.Time `json:"bookmarked_at,omitempty"`
	IsLiked             bool       `json:"is_liked"`
	IsBookmarked        bool       `json:"is_bookmarked"`
	IsArchived          bool       `json:"is_archived"`
	TranscriptID        *string    `json:"transcript_id,omitempty"`
}

const (
	forYouMinDurationSec     = 4*60 + 30
	forYouSoftMaxDurationSec = 30 * 60
	forYouHardMaxDurationSec = 40 * 60
)

func hasCursor(pagination *utils.CursorPagination) bool {
	return pagination != nil && pagination.Cursor != ""
}

// GetForYouFeed returns the For You feed with cursor-based pagination
// GET /api/v1/feed/foryou?cursor=xxx&limit=20
func GetForYouFeed(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

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
	excludeSeen := c.Query("exclude_seen") == "true"

	// Pre-fetch IDs the user has already viewed (used by both paths below)
	var seenIDs []uuid.UUID
	if excludeSeen && (sessionID != "" || userIDStr != "") {
		seenIDs = fetchSeenIDs(db, sessionID, userIDStr)
	}

	// Load ranking config (uses "default" tenant for public feeds)
	config := loadTenantConfig(db, "default")
	durationTargetMinutes := parseDurationPreference(c.Query("duration"))
	atomizedFeedSchema := supportsAtomizedForYouSchema(db)

	// ------ Ranked path (when intelligence is active) ------
	if config.IsActive {
		// Fetch items for ranking — try time window first, then fall back to all
		var allItems []models.ContentItem
		baseQuery := forYouEligibleMediaQuery(db, atomizedFeedSchema)
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
		allItems = excludeCollapsedRedundancyMembers(db, "default", allItems)

		// Score items
		contentIDs := extractPublicIDs(allItems)
		flagMap := LoadContentFlags(db, "default", contentIDs)
		velocityData := LoadVelocityData(db, contentIDs, config.VelocityWindowHours, time.Now())
		scored := ScoreItems(allItems, config, flagMap, velocityData, time.Now())
		scored = applyPreferenceFeedHook(db, "default", userIDStr, scored)
		scored = applyIntelligenceFeedHooks(db, "default", scored)
		scored = spaceScoredSiblingChapters(scored)
		unfilteredScored := append([]ScoredItem(nil), scored...)

		// Filter out already-seen items
		if len(seenIDs) > 0 {
			seenSet := make(map[uuid.UUID]bool, len(seenIDs))
			for _, id := range seenIDs {
				seenSet[id] = true
			}
			filtered := scored[:0]
			for _, s := range scored {
				if !seenSet[s.Item.PublicID] {
					filtered = append(filtered, s)
				}
			}
			scored = filtered
		}
		if config.ShowWatchedWhenUnseenExhausted && len(scored) == 0 && len(unfilteredScored) > 0 && !hasCursor(pagination) {
			scored = unfilteredScored
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

		responseItems := make([]ForYouItem, len(items))
		for i, item := range items {
			responseItems[i] = mapToForYouItem(item, likedMap[item.PublicID], bookmarkedMap[item.PublicID])
		}

		c.JSON(http.StatusOK, ForYouResponse{Cursor: nextCursor, Items: responseItems})
		if !isFeedIntegritySynthetic(c) {
			recordForYouServe(db, items, pagination.Limit, durationTargetMinutes)
		}
		boosted := int64(0)
		for _, item := range pageItems {
			if item.ScoreBreakdown.Preference > 0 {
				boosted++
			}
		}
		if !isFeedIntegritySynthetic(c) {
			recordPreferenceServes(db, "default", boosted, int64(len(items)))
		}
		return
	}

	// ------ Chronological path (default) ------

	// Query for VIDEO and PODCAST content with a valid media URL.
	// Use COALESCE(published_at, created_at) so items with NULL published_at
	// are still ordered and reachable by cursor pagination.
	query := forYouEligibleMediaQuery(db, atomizedFeedSchema).
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
	if len(seenIDs) > 0 {
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
	items = excludeCollapsedRedundancyMembers(db, "default", items)
	if config.ShowWatchedWhenUnseenExhausted && len(items) == 0 && len(seenIDs) > 0 && !hasCursor(pagination) {
		query = forYouEligibleMediaQuery(db, atomizedFeedSchema).
			Order("COALESCE(published_at, created_at) DESC, public_id DESC")
		query = applyDurationPreference(query, durationTargetMinutes)
		if err := query.Limit(pagination.Limit + 1).Find(&items).Error; err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{
				Code:    http.StatusInternalServerError,
				Message: "Failed to fetch feed fallback: " + err.Error(),
			})
			return
		}
		items = excludeCollapsedRedundancyMembers(db, "default", items)
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

	items, boosted := applyChronologicalPreferenceOrder(db, "default", userIDStr, items)

	// Get interaction status if session/user provided
	likedMap := make(map[uuid.UUID]bool)
	bookmarkedMap := make(map[uuid.UUID]bool)
	if sessionID != "" || userIDStr != "" {
		likedMap, bookmarkedMap = getInteractionStatus(db, items, sessionID, userIDStr)
	}

	// Map to response
	responseItems := make([]ForYouItem, len(items))
	for i, item := range items {
		responseItems[i] = mapToForYouItem(item, likedMap[item.PublicID], bookmarkedMap[item.PublicID])
	}

	c.JSON(http.StatusOK, ForYouResponse{
		Cursor: nextCursor,
		Items:  responseItems,
	})
	if !isFeedIntegritySynthetic(c) {
		recordForYouServe(db, items, pagination.Limit, durationTargetMinutes)
		recordPreferenceServes(db, "default", int64(boosted), int64(len(items)))
	}
}

// excludeCollapsedRedundancyMembers is deliberately an inventory filter, not
// cursor/session state: once a human confirms a family, only its canonical
// member may enter For You until the family is dissolved or collapse is off.
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

// recordForYouServe fires the Ranking/Intelligence serve-side telemetry
// (impressions + demand stats) for one For You response. Runs after the
// response is written and in its own goroutine — the serve path never waits
// on telemetry.
func recordForYouServe(db *gorm.DB, items []models.ContentItem, requestedLimit, durationTargetMinutes int) {
	served := make([]models.ContentItem, len(items))
	copy(served, items)
	durationBucket := ""
	if durationTargetMinutes > 0 {
		durationBucket = intelligence.BucketLabelForDuration(durationTargetMinutes * 60)
	}
	go intelligence.RecordServe(db, intelligence.ServeRecord{
		TenantID:       "default",
		Items:          served,
		RequestedLimit: requestedLimit,
		DurationBucket: durationBucket,
	})
}

// GetNewsFeed returns the News feed with cursor-based pagination
// GET /api/v1/feed/news?window=today|week|month&cursor=xxx&limit=10
func GetNewsFeed(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

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
	config := loadTenantConfig(db, "default")
	circ := circulationContextFor(db, "default", c.Query("window"), time.Now())

	// News feed = story-slides, assembled LIVE by default ("write-time
	// intelligence, read-time freshness") behind a freshness-bounded
	// read-through cache. See serveStoryNewsFeed for the full policy. The seen
	// resolver lets the cache lookup run concurrently with the seen query.
	waitSeen := func() []uuid.UUID {
		<-seenDone
		return seenIDs
	}
	slides, nextCursor, serveMeta := serveStoryNewsFeed(
		db, "default", config, circ, pagination.Timestamp, pagination.LastID, slideLimit, waitSeen, userIDStr,
	)
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
	})
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
	if minSec < forYouMinDurationSec {
		minSec = forYouMinDurationSec
	}
	if maxSec > forYouHardMaxDurationSec {
		maxSec = forYouHardMaxDurationSec
	}
	return query.Where("duration_sec IS NOT NULL AND duration_sec BETWEEN ? AND ?", minSec, maxSec)
}

func supportsAtomizedForYouSchema(db *gorm.DB) bool {
	return db.Migrator().HasColumn(&models.ContentItem{}, "is_feed_unit") &&
		db.Migrator().HasColumn(&models.ContentItem{}, "feed_visibility") &&
		db.Migrator().HasColumn(&models.ContentItem{}, "playback_url")
}

func supportsStorageStateSchema(db *gorm.DB) bool {
	return db.Migrator().HasColumn(&models.ContentItem{}, "storage_state")
}

func forYouEligibleMediaQuery(db *gorm.DB, atomizedFeedSchema bool) *gorm.DB {
	storageUnavailableStates := []string{
		models.StorageStateRecoverableDeleted,
		models.StorageStateMissing,
		models.StorageStateRecoveryPending,
		models.StorageStateUnrecoverable,
	}
	if !atomizedFeedSchema {
		q := db.Model(&models.ContentItem{}).
			Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
			Where("status IN ?", []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).
			Where("duration_sec IS NOT NULL AND duration_sec BETWEEN ? AND ?", forYouMinDurationSec, forYouHardMaxDurationSec).
			Where("media_url IS NOT NULL AND media_url != '' AND (LOWER(media_url) LIKE '%.mp4' OR LOWER(media_url) LIKE '%.mp4?%') AND thumbnail_url IS NOT NULL AND thumbnail_url != ''")
		if supportsStorageStateSchema(db) {
			q = q.Where("(storage_state IS NULL OR storage_state NOT IN ?)", storageUnavailableStates)
		}
		return q
	}

	q := db.Model(&models.ContentItem{}).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("status IN ?", []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).
		Where("duration_sec IS NOT NULL AND duration_sec BETWEEN ? AND ?", forYouMinDurationSec, forYouHardMaxDurationSec).
		Where("is_feed_unit = TRUE AND feed_visibility = ?", feedVisibilityVisible).
		Where("COALESCE(playback_url, media_url) IS NOT NULL AND COALESCE(playback_url, media_url) != '' AND thumbnail_url IS NOT NULL AND thumbnail_url != ''")
	if supportsStorageStateSchema(db) {
		q = q.Where("(storage_state IS NULL OR storage_state NOT IN ?)", storageUnavailableStates)
	}
	return q
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

func mapToForYouItem(item models.ContentItem, isLiked, isBookmarked bool) ForYouItem {
	result := ForYouItem{
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
