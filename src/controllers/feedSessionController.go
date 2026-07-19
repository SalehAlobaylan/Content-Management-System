package controllers

import (
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

type frozenForYouSessionResponse struct {
	SessionID string       `json:"session_id"`
	ExpiresAt time.Time    `json:"expires_at"`
	Cursor    *string      `json:"cursor"`
	Items     []ForYouItem `json:"items"`
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

// snapshotCurrentForYouFeed deliberately routes through the same controller
// contract as the public feed. This keeps session creation aligned with active
// ranking, preference, repetition, and playback eligibility policy while the
// feed assembly code is progressively extracted into a dedicated service.
func snapshotCurrentForYouFeed(c *gin.Context, db *gorm.DB) ([]ForYouItem, error) {
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

	GetForYouFeed(snapshotContext)
	if recorder.Code != http.StatusOK {
		return nil, strconv.ErrSyntax
	}
	var response ForYouResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func cloneURL(source *url.URL) *url.URL {
	copy := *source
	return &copy
}

func visibleFrozenForYouPage(db *gorm.DB, items []ForYouItem, offset, limit int) ([]ForYouItem, int) {
	if offset >= len(items) {
		return []ForYouItem{}, len(items)
	}
	ids := make([]uuid.UUID, 0, len(items)-offset)
	for _, item := range items[offset:] {
		ids = append(ids, item.ID)
	}
	var visibleIDs []uuid.UUID
	_ = publicContentQuery(db).Where("content_items.public_id IN ?", ids).Pluck("content_items.public_id", &visibleIDs).Error
	visible := make(map[uuid.UUID]struct{}, len(visibleIDs))
	for _, id := range visibleIDs {
		visible[id] = struct{}{}
	}

	page := make([]ForYouItem, 0, limit)
	index := offset
	for ; index < len(items) && len(page) < limit; index += 1 {
		if _, ok := visible[items[index].ID]; ok {
			page = append(page, items[index])
		}
	}
	return page, index
}

// CreateForYouFeedSession freezes the current CMS-ranked response for the
// caller's six-hour active session.
func CreateForYouFeedSession(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	identityScope, ok := consumerFeedIdentityScope(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication or session_id required"})
		return
	}

	items, err := snapshotCurrentForYouFeed(c, db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, utils.HTTPError{Code: http.StatusServiceUnavailable, Message: "Unable to create a stable For You session"})
		return
	}
	snapshot, err := json.Marshal(items)
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Unable to store a stable For You session"})
		return
	}
	now := time.Now().UTC()
	session := models.ConsumerFeedSession{
		ID:            uuid.New(),
		IdentityScope: identityScope,
		FeedType:      "foryou",
		Snapshot:      datatypes.JSON(snapshot),
		ExpiresAt:     now.Add(consumerFeedSessionLifetime),
	}
	if err := db.Create(&session).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Unable to create a stable For You session"})
		return
	}

	page, nextOffset := visibleFrozenForYouPage(db, items, 0, frozenSessionLimit(c))
	c.JSON(http.StatusCreated, frozenForYouSessionResponse{SessionID: session.ID.String(), ExpiresAt: session.ExpiresAt, Cursor: frozenSessionCursor(nextOffset, len(items)), Items: page})
}

// GetForYouFeedSessionPage serves only the persisted ordering for the session.
func GetForYouFeedSessionPage(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	identityScope, ok := consumerFeedIdentityScope(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication or session_id required"})
		return
	}
	sessionID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "For You session not found"})
		return
	}
	var session models.ConsumerFeedSession
	if err := db.Where("id = ? AND identity_scope = ? AND feed_type = ?", sessionID, identityScope, "foryou").First(&session).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "For You session not found"})
		return
	}
	if !session.ExpiresAt.After(time.Now().UTC()) {
		c.JSON(http.StatusGone, utils.HTTPError{Code: http.StatusGone, Message: "For You session has expired"})
		return
	}
	offset, err := parseFrozenSessionCursor(c.Query("cursor"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid For You session cursor"})
		return
	}
	var items []ForYouItem
	if err := json.Unmarshal(session.Snapshot, &items); err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Stored For You session is invalid"})
		return
	}
	page, nextOffset := visibleFrozenForYouPage(db, items, offset, frozenSessionLimit(c))
	c.JSON(http.StatusOK, frozenForYouSessionResponse{SessionID: session.ID.String(), ExpiresAt: session.ExpiresAt, Cursor: frozenSessionCursor(nextOffset, len(items)), Items: page})
}
