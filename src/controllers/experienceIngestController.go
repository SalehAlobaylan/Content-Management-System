package controllers

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Real User Experience (RUX) — public batch ingestion.
//
// Trust model (plan §12): the browser beacons to the Wahb-Platform BFF, which
// attaches RUX_INGEST_TOKEN and forwards here. CMS rejects direct unauthenticated
// telemetry. Raw client IP is NEVER persisted; rate limiting keys on the
// BFF-supplied authenticated key (X-RUX-Rate-Key) plus session, never a raw IP
// or a spoofable forwarding header.

const (
	ruxDefaultTenant     = "default"
	ruxMaxBatchEvents    = 50
	ruxRateWindow        = time.Minute
	ruxRatePerWindow     = 600 // events/min per rate-key; batches of ~20 → ~30 flushes/min
	ruxRateMaxKeys       = 50000
	ruxMaxRequestBytes   = 64 * 1024
	ruxIngestTokenHeader = "X-RUX-Ingest-Token"
	ruxRateKeyHeader     = "X-RUX-Rate-Key" // HMAC/opaque key computed by the trusted BFF
)

// ruxTelemetryLimiter is the bounded, self-cleaning limiter for the ingest path.
var ruxTelemetryLimiter = utils.NewTelemetryRateLimiter(ruxRatePerWindow, ruxRateWindow, ruxRateMaxKeys)

// RuxIngestAuthMiddleware enforces the dedicated RUX ingest token. Fail-closed:
// if RUX_INGEST_TOKEN is unset the endpoint refuses all traffic, so a
// misconfiguration can never silently accept anonymous public writes.
func RuxIngestAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		expected := strings.TrimSpace(os.Getenv("RUX_INGEST_TOKEN"))
		if expected == "" {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "RUX ingestion not configured"})
			return
		}
		got := strings.TrimSpace(c.GetHeader(ruxIngestTokenHeader))
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid RUX ingest token"})
			return
		}
		c.Next()
	}
}

// incomingRuxEvent mirrors src/lib/experience/types.ts. Arbitrary keys are
// dropped by the JSON decoder (only these fields are read); unknown enum values
// are rejected per-event during validation.
type incomingRuxEvent struct {
	EventID       string  `json:"event_id"`
	SchemaVersion int     `json:"schema_version"`
	EventType     string  `json:"event_type"`
	OccurredAt    string  `json:"occurred_at"`
	SessionID     string  `json:"session_id"`
	PageLoadID    string  `json:"page_load_id"`
	Sequence      int     `json:"sequence"`
	Release       string  `json:"release"`
	Surface       string  `json:"surface"`
	JourneyID     *string `json:"journey_id"`
	ContentID     *string `json:"content_id"`
	StoryID       *string `json:"story_id"`
	PlaybackType  *string `json:"playback_type"`
	Locale        *string `json:"locale"`
	Client        struct {
		BrowserFamily string `json:"browser_family"`
		BrowserMajor  int    `json:"browser_major"`
		DeviceClass   string `json:"device_class"`
		NetworkClass  string `json:"network_class"`
		InstalledPWA  bool   `json:"installed_pwa"`
	} `json:"client"`
	Measurements *struct {
		DurationMS      *int    `json:"duration_ms"`
		MediaErrorCode  *int    `json:"media_error_code"`
		StallDurationMS *int    `json:"stall_duration_ms"`
		FailureClass    *string `json:"failure_class"`
		Visible         *bool   `json:"visible"`
	} `json:"measurements"`
}

type incomingRuxBatch struct {
	SchemaVersion int                `json:"schema_version"`
	Events        []incomingRuxEvent `json:"events"`
}

// IngestExperienceEvents accepts a bounded, BFF-authenticated batch. It never
// computes verdicts inline; it validates, isolates bad events, and inserts.
func IngestExperienceEvents(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	// Kill-switch: checked before any parsing so an abuse flood or bad client
	// release can be shut off from the Console instantly.
	policy := getOrCreateExperiencePolicy(db, ruxDefaultTenant)
	if !policy.IngestEnabled {
		// 200 with disabled flag so the collector goes quiet (server-off backoff)
		// rather than retry-storming a switched-off system.
		c.JSON(http.StatusOK, gin.H{"disabled": true, "accepted": 0, "duplicate": 0, "rejected": 0})
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, ruxMaxRequestBytes)
	var batch incomingRuxBatch
	if err := c.ShouldBindJSON(&batch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid batch body"})
		return
	}
	if batch.SchemaVersion != models.RuxSchemaVersion {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported schema_version"})
		return
	}
	if len(batch.Events) == 0 {
		c.JSON(http.StatusOK, gin.H{"accepted": 0, "duplicate": 0, "rejected": 0})
		return
	}
	if len(batch.Events) > ruxMaxBatchEvents {
		c.JSON(http.StatusBadRequest, gin.H{"error": "batch too large"})
		return
	}
	rateKey := strings.TrimSpace(c.GetHeader(ruxRateKeyHeader))
	if allowed, _ := ruxTelemetryLimiter.AllowN(rateKey, len(batch.Events)); !allowed {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limited"})
		return
	}
	sessionID := strings.TrimSpace(batch.Events[0].SessionID)
	if sessionID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing session_id"})
		return
	}
	for i := 1; i < len(batch.Events); i++ {
		if strings.TrimSpace(batch.Events[i].SessionID) != sessionID {
			c.JSON(http.StatusBadRequest, gin.H{"error": "mixed session batch"})
			return
		}
	}

	now := time.Now()
	rows := make([]models.ExperienceEvent, 0, len(batch.Events))
	rejected := 0
	for i := range batch.Events {
		row, ok := validateAndNormalizeRuxEvent(&batch.Events[i], now)
		if !ok {
			rejected++
			continue
		}
		row.TenantID = ruxDefaultTenant
		row.ReceivedAt = now
		rows = append(rows, row)
	}

	accepted := 0
	duplicate := 0
	if len(rows) > 0 {
		// Idempotent insert: duplicate event_id is silently skipped. We insert
		// per-row (bounded batch ≤50) to count duplicates precisely without a
		// pre-select round-trip.
		for i := range rows {
			res := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "event_id"}}, DoNothing: true}).Create(&rows[i])
			if res.Error != nil {
				rejected++
				continue
			}
			if res.RowsAffected == 0 {
				duplicate++
			} else {
				accepted++
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"accepted": accepted, "duplicate": duplicate, "rejected": rejected})
}

// validateAndNormalizeRuxEvent enforces the allowlist + bounded ranges. Any
// violation rejects the single event; siblings in the batch are unaffected.
func validateAndNormalizeRuxEvent(in *incomingRuxEvent, now time.Time) (models.ExperienceEvent, bool) {
	var out models.ExperienceEvent

	if in.SchemaVersion != models.RuxSchemaVersion {
		return out, false
	}
	eventID, err := uuid.Parse(strings.TrimSpace(in.EventID))
	if err != nil {
		return out, false
	}
	if !models.RuxEventTypes[in.EventType] {
		return out, false
	}
	if !models.RuxSurfaces[in.Surface] {
		return out, false
	}
	if strings.TrimSpace(in.SessionID) == "" || len(in.SessionID) > 64 {
		return out, false
	}
	pageLoadID, err := uuid.Parse(strings.TrimSpace(in.PageLoadID))
	if err != nil {
		return out, false
	}
	if strings.TrimSpace(in.Release) == "" || len(in.Release) > 80 {
		return out, false
	}
	occurredAt, err := time.Parse(time.RFC3339, strings.TrimSpace(in.OccurredAt))
	if err != nil {
		// Client clock is evidence, not authority — but an unparseable timestamp
		// is a malformed event.
		return out, false
	}

	out.EventID = eventID
	out.SchemaVersion = in.SchemaVersion
	out.EventType = in.EventType
	out.Surface = in.Surface
	out.OccurredAt = occurredAt
	out.SessionID = in.SessionID
	out.PageLoadID = pageLoadID
	out.Sequence = in.Sequence
	out.Release = in.Release

	if in.JourneyID != nil {
		if jid, e := uuid.Parse(strings.TrimSpace(*in.JourneyID)); e == nil {
			out.JourneyID = &jid
		}
	}
	if in.ContentID != nil {
		if cid, e := uuid.Parse(strings.TrimSpace(*in.ContentID)); e == nil {
			out.ContentID = &cid
		}
	}
	if in.StoryID != nil {
		if sid, e := uuid.Parse(strings.TrimSpace(*in.StoryID)); e == nil {
			out.StoryID = &sid
		}
	}
	if in.PlaybackType != nil && models.RuxPlaybackTypes[*in.PlaybackType] {
		pt := *in.PlaybackType
		out.PlaybackType = &pt
	}
	if in.Locale != nil {
		loc := strings.TrimSpace(*in.Locale)
		if loc != "" && len(loc) <= 16 {
			out.Locale = &loc
		}
	}

	// Cohort dimensions: unknown enum values normalize to the safe bucket rather
	// than rejecting the whole event (a new browser shouldn't drop telemetry).
	out.BrowserFamily = "other"
	if models.RuxBrowserFamilies[in.Client.BrowserFamily] {
		out.BrowserFamily = in.Client.BrowserFamily
	}
	if in.Client.BrowserMajor >= 0 && in.Client.BrowserMajor <= 999 {
		out.BrowserMajor = in.Client.BrowserMajor
	}
	out.DeviceClass = "mobile"
	if models.RuxDeviceClasses[in.Client.DeviceClass] {
		out.DeviceClass = in.Client.DeviceClass
	}
	out.NetworkClass = "unknown"
	if models.RuxNetworkClasses[in.Client.NetworkClass] {
		out.NetworkClass = in.Client.NetworkClass
	}
	out.InstalledPWA = in.Client.InstalledPWA

	if in.Measurements != nil {
		m := in.Measurements
		if m.DurationMS != nil && *m.DurationMS >= 0 && *m.DurationMS <= models.RuxMaxDurationMS {
			v := *m.DurationMS
			out.DurationMS = &v
		}
		if m.MediaErrorCode != nil && *m.MediaErrorCode >= models.RuxMinErrorCode && *m.MediaErrorCode <= models.RuxMaxErrorCode {
			v := *m.MediaErrorCode
			out.MediaErrorCode = &v
		}
		if m.StallDurationMS != nil && *m.StallDurationMS >= 0 && *m.StallDurationMS <= models.RuxMaxStallMS {
			v := *m.StallDurationMS
			out.StallDurationMS = &v
		}
		if m.FailureClass != nil && models.RuxFailureClasses[*m.FailureClass] {
			fc := *m.FailureClass
			out.FailureClass = &fc
		}
		if m.Visible != nil {
			v := *m.Visible
			out.Visible = &v
		}
		// Persist the sanitized measurements block too (bounded, known keys only).
		if b, e := json.Marshal(map[string]any{
			"duration_ms": out.DurationMS, "media_error_code": out.MediaErrorCode,
			"stall_duration_ms": out.StallDurationMS, "failure_class": out.FailureClass,
			"visible": out.Visible,
		}); e == nil {
			out.Measurements = b
		}
	}

	return out, true
}

// getOrCreateExperiencePolicy returns the singleton Observe policy, creating the
// default row on first use so a missing seed never breaks ingestion.
func getOrCreateExperiencePolicy(db *gorm.DB, tenantID string) models.ExperiencePolicy {
	var p models.ExperiencePolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&p).Error; err != nil {
		p = models.DefaultExperiencePolicy(tenantID)
		db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}}, DoNothing: true}).Create(&p)
		db.Where("tenant_id = ?", tenantID).First(&p)
	}
	return p
}
