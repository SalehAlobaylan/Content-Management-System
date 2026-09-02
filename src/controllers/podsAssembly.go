package controllers

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const podsAssemblyCandidateLimit = 200

type podsAssemblyMode string

const (
	podsAssemblyRanked        podsAssemblyMode = "ranked"
	podsAssemblyChronological podsAssemblyMode = "chronological"
)

type podsAssemblyRequest struct {
	TenantID          string
	Mode              podsAssemblyMode
	AsOf              time.Time
	DeliveryLanguage  deliveryLanguage
	DurationMinutes   int
	UserID            string
	SuppressedIDs     []uuid.UUID
	HardHiddenIDs     []uuid.UUID
	RecycleSuppressed bool
	RankingConfig     *models.RankingConfig
}

type podsAssemblyResult struct {
	Raw                     []ScoredItem
	BeforeSourceConstraints []ScoredItem
	Final                   []ScoredItem
	PreferenceEligible      bool
	PreferenceBoosted       int64
	FilterDigest            string
	SourceCapApplied        bool
	ConstraintRelaxation    string
}

func getPodsFeedCanonical(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID, tenantErr := trustedPublicFeedTenant(c)
	if tenantErr != nil {
		c.JSON(503, utils.HTTPError{Code: 503, Message: "Public feed tenant is unavailable"})
		return
	}
	availability := currentFeedAvailability(db, tenantID, "media")
	if availability != nil && availability.RetryAfterSeconds != nil {
		c.Header("Retry-After", strconv.Itoa(*availability.RetryAfterSeconds))
	}
	language, ok := parseDeliveryLanguage(c.Query("content_language"))
	if !ok {
		c.JSON(400, utils.HTTPError{Code: 400, Message: "content_language must be ar, en, or both"})
		return
	}
	userID, sessionID := readIdentity(c)
	config := loadTenantConfig(db, tenantID)
	recycle, _ := c.Get(podsRecycleSuppressedContextKey)
	duration := parseDurationPreference(c.Query("duration"))
	mode := podsAssemblyChronological
	if config.IsActive {
		mode = podsAssemblyRanked
	}
	seed := podsAssemblyRequest{TenantID: tenantID, Mode: mode, DeliveryLanguage: language, DurationMinutes: duration, UserID: userID}
	if sessionID != "" || userID != "" {
		seed.SuppressedIDs = fetchPodsSuppressedIDs(db, sessionID, userID, loadTenantConfig(db, tenantID), time.Now().UTC())
	}
	seed.RecycleSuppressed, _ = recycle.(bool)
	if seed.RecycleSuppressed {
		seed.HardHiddenIDs = fetchPodsHardHiddenIDs(db, sessionID, userID)
	}
	filterDigest := podsAssemblyFilterDigest(seed)
	limit, asOf, lastID, legacyTimestamp, hasCursor, err := parsePodsCursor(c.Query("cursor"), c.Query("limit"), mode, filterDigest, time.Now().UTC())
	if err != nil {
		c.JSON(400, utils.HTTPError{Code: 400, Message: "Invalid cursor: " + err.Error()})
		return
	}
	seed.AsOf = asOf
	assembled, err := assemblePods(db, seed)
	if err != nil {
		c.JSON(500, utils.HTTPError{Code: 500, Message: "Failed to assemble feed: " + err.Error()})
		return
	}
	page, hasMore := paginatePodsAssemblyWithBoundary(assembled.Final, lastID, legacyTimestamp, limit)
	var nextCursor *string
	if hasMore && len(page) > 0 {
		cursor := encodePodsCursorV2(podsCursorV2{AssemblyTime: seed.AsOf, LastID: page[len(page)-1].Item.PublicID, Mode: mode, FilterDigest: assembled.FilterDigest})
		nextCursor = &cursor
	}
	items := make([]models.ContentItem, len(page))
	for index, item := range page {
		items[index] = item.Item
	}
	liked, bookmarked := map[uuid.UUID]bool{}, map[uuid.UUID]bool{}
	if sessionID != "" || userID != "" {
		liked, bookmarked = getInteractionStatus(db, items, sessionID, userID)
	}
	response := make([]PodsItem, len(items))
	for index, item := range items {
		response[index] = mapToPodsItem(item, liked[item.PublicID], bookmarked[item.PublicID])
	}
	c.JSON(200, PodsResponse{Cursor: nextCursor, Items: response, CaughtUp: len(response) == 0 && !hasCursorParam(hasCursor), Meta: availability})
	if !isFeedIntegritySynthetic(c) {
		recordPodsServe(db, tenantID, items, limit, duration)
		recordPreferenceServes(db, tenantID, assembled.PreferenceEligible, assembled.PreferenceBoosted, int64(len(items)))
	}
}

func hasCursorParam(hasCursor bool) bool { return hasCursor }

type podsCursorV2 struct {
	Version      int              `json:"v"`
	AssemblyTime time.Time        `json:"as_of"`
	LastID       uuid.UUID        `json:"last_id"`
	Mode         podsAssemblyMode `json:"mode"`
	FilterDigest string           `json:"filter_digest"`
}

func podsAssemblyFilterDigest(req podsAssemblyRequest) string {
	ids := make([]string, 0, len(req.SuppressedIDs)+len(req.HardHiddenIDs))
	for _, id := range req.SuppressedIDs {
		ids = append(ids, "s:"+id.String())
	}
	for _, id := range req.HardHiddenIDs {
		ids = append(ids, "h:"+id.String())
	}
	sort.Strings(ids)
	raw, _ := json.Marshal(map[string]any{
		"schema": "pods-assembly-filter/v2", "tenant": req.TenantID, "mode": req.Mode,
		"language": req.DeliveryLanguage, "duration": req.DurationMinutes,
		"identity_filter": ids, "recycle": req.RecycleSuppressed,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func encodePodsCursorV2(cursor podsCursorV2) string {
	cursor.Version = 2
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func parsePodsCursor(raw, limitRaw string, mode podsAssemblyMode, expectedFilter string, now time.Time) (int, time.Time, uuid.UUID, time.Time, bool, error) {
	limit := 20
	if limitRaw != "" {
		parsed, err := strconv.Atoi(limitRaw)
		if err == nil && parsed > 0 {
			limit = parsed
		}
		if limit > 50 {
			limit = 50
		}
	}
	if raw == "" {
		return limit, now.UTC(), uuid.Nil, time.Time{}, false, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err == nil && len(decoded) > 0 && decoded[0] == '{' {
		var cursor podsCursorV2
		if json.Unmarshal(decoded, &cursor) != nil || cursor.Version != 2 || cursor.AssemblyTime.IsZero() || cursor.LastID == uuid.Nil {
			return 0, time.Time{}, uuid.Nil, time.Time{}, false, fmt.Errorf("invalid Pods cursor v2")
		}
		if cursor.Mode != mode || cursor.FilterDigest != expectedFilter {
			return 0, time.Time{}, uuid.Nil, time.Time{}, false, fmt.Errorf("Pods cursor filters changed")
		}
		if cursor.AssemblyTime.After(now.Add(15 * time.Minute)) {
			return 0, time.Time{}, uuid.Nil, time.Time{}, false, fmt.Errorf("Pods cursor assembly time is invalid")
		}
		return limit, cursor.AssemblyTime.UTC(), cursor.LastID, time.Time{}, true, nil
	}
	// Legacy timestamp/UUID cursors remain readable. They cannot freeze a prior
	// assembly time, so they receive a one-time reconstruction at request time.
	legacyTimestamp, id, legacyErr := utils.DecodeCursor(raw)
	if legacyErr != nil {
		return 0, time.Time{}, uuid.Nil, time.Time{}, false, legacyErr
	}
	return limit, now.UTC(), id, legacyTimestamp.UTC(), true, nil
}

func assemblePods(db *gorm.DB, req podsAssemblyRequest) (podsAssemblyResult, error) {
	result := podsAssemblyResult{FilterDigest: podsAssemblyFilterDigest(req)}
	if db == nil || strings.TrimSpace(req.TenantID) == "" {
		return result, gorm.ErrInvalidDB
	}
	if req.AsOf.IsZero() {
		req.AsOf = time.Now().UTC()
	}
	query := applyDeliveryLanguage(podsEligibleMediaQuery(db, req.TenantID, supportsAtomizedPodsSchema(db)), req.DeliveryLanguage)
	query = applyDurationPreference(query, req.DurationMinutes).
		Where("created_at <= ?", req.AsOf.UTC()).
		Order("COALESCE(published_at, created_at) DESC, public_id DESC").Limit(podsAssemblyCandidateLimit)
	var candidates []models.ContentItem
	if err := query.Find(&candidates).Error; err != nil {
		return result, err
	}
	candidates = excludeCollapsedRedundancyMembers(db, req.TenantID, candidates)
	if len(req.SuppressedIDs) > 0 || len(req.HardHiddenIDs) > 0 {
		if req.RecycleSuppressed {
			candidates = prioritizePodsForSession(candidates, req.SuppressedIDs, req.HardHiddenIDs)
		} else {
			excluded := uuidMembership(req.SuppressedIDs)
			for id := range uuidMembership(req.HardHiddenIDs) {
				excluded[id] = struct{}{}
			}
			filtered := candidates[:0]
			for _, candidate := range candidates {
				if _, found := excluded[candidate.PublicID]; !found {
					filtered = append(filtered, candidate)
				}
			}
			candidates = filtered
		}
	}
	config := loadTenantConfig(db, req.TenantID)
	if req.RankingConfig != nil {
		config = *req.RankingConfig
	}
	if req.Mode == podsAssemblyRanked && (config.IsActive || req.RankingConfig != nil) {
		ids := extractPublicIDs(candidates)
		result.Final = ScoreItems(candidates, config, LoadContentFlags(db, req.TenantID, ids), LoadVelocityData(db, ids, config.VelocityWindowHours, req.AsOf), req.AsOf)
		result.Final, result.PreferenceEligible = applyPreferenceFeedHook(db, req.TenantID, req.UserID, result.Final)
		result.Final = applyIntelligenceFeedHooks(db, req.TenantID, result.Final)
	} else {
		result.Final = make([]ScoredItem, len(candidates))
		for index, candidate := range candidates {
			result.Final[index] = ScoredItem{Item: candidate, FinalScore: float64(len(candidates) - index)}
		}
		result.Final, result.PreferenceEligible = applyPreferenceFeedHook(db, req.TenantID, req.UserID, result.Final)
	}
	result.Final = spaceScoredSiblingChapters(result.Final)
	result.Raw = append([]ScoredItem(nil), result.Final...)
	for _, item := range result.Final {
		if item.ScoreBreakdown.Preference > 0 {
			result.PreferenceBoosted++
		}
	}
	result.BeforeSourceConstraints = reserveFreshPodsFirstPageUnconstrained(result.Final, req.AsOf)
	result.Final = enforcePodsFirstPageConstraints(result.BeforeSourceConstraints)
	result.SourceCapApplied, result.ConstraintRelaxation = podsFirstPageConstraintStatus(result.Final)
	return result, nil
}

func paginatePodsAssembly(items []ScoredItem, lastID uuid.UUID, limit int) ([]ScoredItem, bool) {
	return paginatePodsAssemblyWithBoundary(items, lastID, time.Time{}, limit)
}

func paginatePodsAssemblyWithBoundary(items []ScoredItem, lastID uuid.UUID, legacyTimestamp time.Time, limit int) ([]ScoredItem, bool) {
	start := 0
	if lastID != uuid.Nil {
		found := false
		for index, item := range items {
			if item.Item.PublicID == lastID {
				start, found = index+1, true
				break
			}
		}
		if !found && !legacyTimestamp.IsZero() {
			// Legacy cursors were timestamp/UUID boundaries. Prefer the exact
			// identity when it is still in the reconstructed set; otherwise
			// continue after the nearest older boundary rather than restarting
			// page one and duplicating the feed.
			for index, item := range items {
				published := podsPublicationTime(item.Item)
				if published.Before(legacyTimestamp) || (published.Equal(legacyTimestamp) && item.Item.PublicID.String() < lastID.String()) {
					start = index
					found = true
					break
				}
			}
		}
		if !found {
			return []ScoredItem{}, false
		}
	}
	if start >= len(items) {
		return []ScoredItem{}, false
	}
	end := start + limit
	hasMore := end < len(items)
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], hasMore
}

func podsRankingTrace(db *gorm.DB, tenantID string, item models.ContentItem) map[string]any {
	config := loadTenantConfig(db, tenantID)
	mode := podsAssemblyChronological
	if config.IsActive {
		mode = podsAssemblyRanked
	}
	assembled, err := assemblePods(db, podsAssemblyRequest{TenantID: tenantID, Mode: mode, AsOf: time.Now().UTC()})
	if err != nil {
		return map[string]any{"raw_rank": nil, "final_rank": nil, "freshness_reserved": false, "source_spacing_movement": 0, "assembly_mode": mode, "error": err.Error()}
	}
	result := rankingTraceForItem(assembled.Raw, assembled.BeforeSourceConstraints, assembled.Final, item.PublicID)
	result["assembly_mode"] = mode
	result["filter_digest"] = assembled.FilterDigest
	result["source_cap_applied"] = assembled.SourceCapApplied
	if assembled.ConstraintRelaxation != "" {
		result["constraint_relaxation"] = assembled.ConstraintRelaxation
	}
	return result
}

func buildPodsSupplyReturnProbe(db *gorm.DB, tenantID string, limit int) ([]supply.PodsReturnedItem, error) {
	if db == nil || tenantID == "" || limit < 1 || limit > 100 {
		return nil, gorm.ErrInvalidDB
	}
	config := loadTenantConfig(db, tenantID)
	mode := podsAssemblyChronological
	if config.IsActive {
		mode = podsAssemblyRanked
	}
	assembled, err := assemblePods(db, podsAssemblyRequest{TenantID: tenantID, Mode: mode, AsOf: time.Now().UTC()})
	if err != nil {
		return nil, err
	}
	page, _ := paginatePodsAssembly(assembled.Final, uuid.Nil, limit)
	returned := make([]supply.PodsReturnedItem, 0, len(page))
	for _, scored := range page {
		item := scored.Item
		at := podsPublicationTime(item)
		returned = append(returned, supply.PodsReturnedItem{ID: item.PublicID, PublishedAt: at, SourceRunRequestID: item.SourceRunRequestID})
	}
	return returned, nil
}
