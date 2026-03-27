package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ── Rate limiter ────────────────────────────────────────────

const (
	maxTriggersPerHour = 5
	rateLimitWindow    = 1 * time.Hour
	cleanupInterval    = 10 * time.Minute
)

type userRateEntry struct {
	timestamps []time.Time
}

var (
	rateLimitMap = make(map[string]*userRateEntry)
	rateLimitMu  sync.Mutex
)

func init() {
	// Periodic cleanup goroutine to prevent memory leak
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			pruneRateLimitMap()
		}
	}()
}

// pruneRateLimitMap removes entries older than the rate limit window.
func pruneRateLimitMap() {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()

	cutoff := time.Now().Add(-rateLimitWindow)
	for userID, entry := range rateLimitMap {
		valid := make([]time.Time, 0, len(entry.timestamps))
		for _, ts := range entry.timestamps {
			if ts.After(cutoff) {
				valid = append(valid, ts)
			}
		}
		if len(valid) == 0 {
			delete(rateLimitMap, userID)
		} else {
			entry.timestamps = valid
		}
	}
}

// checkRateLimit returns true if the user is within rate limits.
func checkRateLimit(userID string) bool {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rateLimitWindow)

	entry, exists := rateLimitMap[userID]
	if !exists {
		rateLimitMap[userID] = &userRateEntry{
			timestamps: []time.Time{now},
		}
		return true
	}

	// Prune old entries
	valid := make([]time.Time, 0, len(entry.timestamps))
	for _, ts := range entry.timestamps {
		if ts.After(cutoff) {
			valid = append(valid, ts)
		}
	}

	if len(valid) >= maxTriggersPerHour {
		entry.timestamps = valid
		return false
	}

	entry.timestamps = append(valid, now)
	return true
}

// ── POST /api/v1/content/:id/transcribe ─────────────────────

// RequestTranscription allows a logged-in user to trigger transcript generation
// for a content item that doesn't have one yet.
// Requires JWT auth — user_id is extracted from the token, not the request body.
func RequestTranscription(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	// Extract user_id from JWT (set by UserAuthMiddleware)
	userID, exists := c.Get("user_id")
	if !exists || userID == "" {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Authentication required",
		})
		return
	}
	userIDStr, ok := userID.(string)
	if !ok || userIDStr == "" {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{
			Code:    http.StatusUnauthorized,
			Message: "Invalid authentication token",
		})
		return
	}

	// Parse content item ID
	contentIDStr := c.Param("id")
	contentID, err := uuid.Parse(contentIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Invalid content ID",
		})
		return
	}

	// Rate limit check using JWT-verified user_id
	if !checkRateLimit(userIDStr) {
		c.JSON(http.StatusTooManyRequests, utils.HTTPError{
			Code:    http.StatusTooManyRequests,
			Message: "Rate limit exceeded. You can request up to 5 transcriptions per hour.",
		})
		return
	}

	// Look up content item
	var item models.ContentItem
	if err := db.Where("public_id = ?", contentID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{
			Code:    http.StatusNotFound,
			Message: "Content item not found",
		})
		return
	}

	// Verify it's a media type that can be transcribed
	if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "Transcription is only available for VIDEO and PODCAST content",
		})
		return
	}

	// Transcript already exists — return it directly, nothing to do
	if item.TranscriptID != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":        "exists",
			"message":       "Transcript already exists for this content",
			"transcript_id": item.TranscriptID.String(),
		})
		return
	}

	// Verify media URL exists
	if item.MediaURL == nil || *item.MediaURL == "" {
		c.JSON(http.StatusBadRequest, utils.HTTPError{
			Code:    http.StatusBadRequest,
			Message: "No media URL available for this content",
		})
		return
	}

	// Fire-and-forget: trigger transcription asynchronously so we return 202 immediately
	mediaURL := *item.MediaURL
	publicID := item.PublicID.String()
	go func() {
		if err := triggerTranscription(mediaURL, publicID); err != nil {
			log.Printf("[CMS] transcription trigger failed for %s: %v", publicID, err)
		}
	}()

	c.JSON(http.StatusAccepted, utils.ResponseMessage{
		Code:    http.StatusAccepted,
		Message: "Transcript generation started. It will be available shortly.",
		Data: gin.H{
			"status":     "processing",
			"content_id": contentIDStr,
		},
	})
}

// ── UserAuthMiddleware ──────────────────────────────────────

// UserAuthMiddleware validates JWT tokens for user-facing authenticated routes.
// It extracts user_id from the token and sets it in the Gin context.
// Lighter than AdminAuthMiddleware — only requires a valid JWT with user_id.
func UserAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, utils.HTTPError{
				Code:    http.StatusUnauthorized,
				Message: "Authentication required",
			})
			c.Abort()
			return
		}

		if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
			c.JSON(http.StatusUnauthorized, utils.HTTPError{
				Code:    http.StatusUnauthorized,
				Message: "Invalid authentication token",
			})
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		tokenString := ""
		if len(parts) == 2 {
			tokenString = strings.TrimSpace(parts[1])
		}
		if tokenString == "" {
			c.JSON(http.StatusUnauthorized, utils.HTTPError{
				Code:    http.StatusUnauthorized,
				Message: "Invalid authentication token",
			})
			c.Abort()
			return
		}

		secret, err := utils.GetJWTSecret()
		if err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{
				Code:    http.StatusInternalServerError,
				Message: "Internal server error",
			})
			c.Abort()
			return
		}

		claims, err := utils.ParseJWT(tokenString, secret)
		if err != nil {
			c.JSON(http.StatusUnauthorized, utils.HTTPError{
				Code:    http.StatusUnauthorized,
				Message: "Invalid or expired token",
			})
			c.Abort()
			return
		}

		if claims.UserID == "" {
			c.JSON(http.StatusUnauthorized, utils.HTTPError{
				Code:    http.StatusUnauthorized,
				Message: "Token missing user identity",
			})
			c.Abort()
			return
		}

		c.Set("user_id", claims.UserID)
		c.Next()
	}
}
