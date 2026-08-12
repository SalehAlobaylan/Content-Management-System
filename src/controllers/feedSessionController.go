package controllers

import (
	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const consumerFeedSessionLifetime = 6 * time.Hour
const consumerFeedSnapshotLimit = 50

type frozenPodsSessionResponse struct {
	SessionID string     `json:"session_id"`
	ExpiresAt time.Time  `json:"expires_at"`
	Cursor    *string    `json:"cursor"`
	Items     []PodsItem `json:"items"`
	CaughtUp  bool       `json:"caught_up"`
}

type frozenPodsSessionFreshnessResponse struct {
	HasNewContent bool `json:"has_new_content"`
}

func consumerFeedIdentityScope(c *gin.Context) (string, bool) {
	if uid, ok := authedUserID(c); ok {
		return "user:" + uid.String(), true
	}
	sessionID := strings.TrimSpace(c.Query("session_id"))
	if sessionID == "" || len(sessionID) > 255 {
		return "", false
	}
	return "session:" + sessionID, true
}

func frozenSessionCursor(offset, total int) *string {
	if offset >= total {
		return nil
	}
	value := base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
	return &value
}

func parseFrozenSessionCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	offset, err := strconv.Atoi(string(decoded))
	if err != nil || offset < 0 {
		return 0, strconv.ErrSyntax
	}
	return offset, nil
}

func frozenSessionLimit(c *gin.Context) int {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
	if err != nil || limit < 1 {
		return 10
	}
	if limit > 50 {
		return 50
	}
	return limit
}

func activeFeedGeneration(db *gorm.DB, tenantID, lane string) int64 {
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", tenantID, lane).First(&head).Error; err != nil || head.Generation < 1 {
		return 0
	}
	return head.Generation
}

// snapshotCurrentPodsFeed deliberately routes through the same controller
// contract as the public feed. This keeps session creation aligned with active
// ranking, preference, repetition, and playback eligibility policy while the
// feed assembly code is progressively extracted into a dedicated service.
func snapshotCurrentPodsFeed(c *gin.Context, db *gorm.DB) ([]PodsItem, error) {
	recorder := httptest.NewRecorder()
	snapshotContext, _ := gin.CreateTestContext(recorder)
	request := c.Request.Clone(c.Request.Context())
	request.URL = cloneURL(c.Request.URL)
	query := request.URL.Query()
	query.Del("cursor")
	query.Set("limit", strconv.Itoa(consumerFeedSnapshotLimit))
	request.URL.RawQuery = query.Encode()
	request.RemoteAddr = "127.0.0.1:0"
	request.Header = request.Header.Clone()
	request.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
	snapshotContext.Request = request
	snapshotContext.Set("db", db)
	if userID, ok := c.Get("user_id"); ok {
		snapshotContext.Set("user_id", userID)
	}
	if tenantID, ok := c.Get("tenant_id"); ok {
		snapshotContext.Set("tenant_id", tenantID)
	}

	GetPodsFeed(snapshotContext)
	if recorder.Code != http.StatusOK {
		return nil, strconv.ErrSyntax
	}
	var response PodsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func cloneURL(source *url.URL) *url.URL {
	copy := *source
	return &copy
}

func visibleFrozenPodsPage(db *gorm.DB, tenantID string, items []PodsItem, offset, limit int) ([]PodsItem, int) {
	if offset >= len(items) {
		return []PodsItem{}, len(items)
	}
	ids := make([]uuid.UUID, 0, len(items)-offset)
	for _, item := range items[offset:] {
		ids = append(ids, item.ID)
	}
	var visibleIDs []uuid.UUID
	// Preserve the frozen membership across later generation rotations while
	// still removing items that become unsafe or canonically ineligible.
	query := feedcontract.PodsEligibleMediaQuery(db, tenantID, supportsAtomizedPodsSchema(db))
	_ = query.
		Where("content_items.public_id IN ?", ids).
		Pluck("content_items.public_id", &visibleIDs).Error
	visible := make(map[uuid.UUID]struct{}, len(visibleIDs))
	for _, id := range visibleIDs {
		visible[id] = struct{}{}
	}

	page := make([]PodsItem, 0, limit)
	index := offset
	for ; index < len(items) && len(page) < limit; index += 1 {
		if _, ok := visible[items[index].ID]; ok {
			page = append(page, items[index])
		}
	}
	return page, index
}

// CreatePodsFeedSession freezes the current CMS-ranked response for the
// caller's six-hour active session.
func CreatePodsFeedSession(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID, tenantErr := trustedPublicFeedTenant(c)
	if tenantErr != nil {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Public feed tenant is unavailable"})
		return
	}
	identityScope, ok := consumerFeedIdentityScope(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication or session_id required"})
		return
	}
	if _, supported, active := feedcontract.ActiveGeneration(db, tenantID, "media"); supported && !active {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Pods generation authority is unavailable"})
		return
	}

	items, err := snapshotCurrentPodsFeed(c, db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Unable to create a stable Pods session"})
		return
	}
	snapshot, err := json.Marshal(items)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Unable to store a stable Pods session"})
		return
	}
	now := time.Now().UTC()
	session := models.ConsumerFeedSession{
		ID:            uuid.New(),
		TenantID:      tenantID,
		IdentityScope: identityScope,
		FeedType:      "pods",
		Snapshot:      datatypes.JSON(snapshot),
		Generation:    activeFeedGeneration(db, tenantID, "media"),
		ExpiresAt:     now.Add(consumerFeedSessionLifetime),
	}
	if err := db.Create(&session).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Unable to create a stable Pods session"})
		return
	}

	page, nextOffset := visibleFrozenPodsPage(db, tenantID, items, 0, frozenSessionLimit(c))
	c.JSON(http.StatusCreated, frozenPodsSessionResponse{SessionID: session.ID.String(), ExpiresAt: session.ExpiresAt, Cursor: frozenSessionCursor(nextOffset, len(items)), Items: page, CaughtUp: len(items) == 0})
}

// GetPodsFeedSessionPage serves only the persisted ordering for the session.
func GetPodsFeedSessionPage(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID, tenantErr := trustedPublicFeedTenant(c)
	if tenantErr != nil {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Public feed tenant is unavailable"})
		return
	}
	identityScope, ok := consumerFeedIdentityScope(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication or session_id required"})
		return
	}
	sessionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Pods session not found"})
		return
	}
	var session models.ConsumerFeedSession
	if err := db.Where("id = ? AND tenant_id = ? AND identity_scope = ? AND feed_type = ?", sessionID, tenantID, identityScope, "pods").First(&session).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Pods session not found"})
		return
	}
	if !session.ExpiresAt.After(time.Now().UTC()) {
		c.JSON(http.StatusGone, utils.HTTPError{Code: http.StatusGone, Message: "Pods session has expired"})
		return
	}
	offset, err := parseFrozenSessionCursor(c.Query("cursor"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid Pods session cursor"})
		return
	}
	var items []PodsItem
	if err := json.Unmarshal(session.Snapshot, &items); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Stored Pods session is invalid"})
		return
	}
	page, nextOffset := visibleFrozenPodsPage(db, tenantID, items, offset, frozenSessionLimit(c))
	c.JSON(http.StatusOK, frozenPodsSessionResponse{SessionID: session.ID.String(), ExpiresAt: session.ExpiresAt, Cursor: frozenSessionCursor(nextOffset, len(items)), Items: page, CaughtUp: offset >= len(items)})
}

// GetPodsFeedSessionFreshness reports whether the current policy-selected
// candidate set contains content that was not in this frozen session. It never
// alters the persisted order or returns ranked inventory; replacement remains a
// deliberate client action through session creation.
func GetPodsFeedSessionFreshness(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID, tenantErr := trustedPublicFeedTenant(c)
	if tenantErr != nil {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Public feed tenant is unavailable"})
		return
	}
	identityScope, ok := consumerFeedIdentityScope(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication or session_id required"})
		return
	}
	sessionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Pods session not found"})
		return
	}
	var session models.ConsumerFeedSession
	if err := db.Where("id = ? AND tenant_id = ? AND identity_scope = ? AND feed_type = ?", sessionID, tenantID, identityScope, "pods").First(&session).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Pods session not found"})
		return
	}
	if !session.ExpiresAt.After(time.Now().UTC()) {
		c.JSON(http.StatusGone, utils.HTTPError{Code: http.StatusGone, Message: "Pods session has expired"})
		return
	}
	var snapshot []PodsItem
	if err := json.Unmarshal(session.Snapshot, &snapshot); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Stored Pods session is invalid"})
		return
	}
	candidates, err := snapshotCurrentPodsFeed(c, db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Unable to check Pods freshness"})
		return
	}
	c.JSON(http.StatusOK, frozenPodsSessionFreshnessResponse{
		HasNewContent: hasNewFrozenPodsCandidate(snapshot, candidates),
	})
}

func hasNewFrozenPodsCandidate(snapshot, candidates []PodsItem) bool {
	known := make(map[uuid.UUID]struct{}, len(snapshot))
	for _, item := range snapshot {
		known[item.ID] = struct{}{}
	}
	for _, candidate := range candidates {
		if _, exists := known[candidate.ID]; !exists {
			return true
		}
	}
	return false
}
