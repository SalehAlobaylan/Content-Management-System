package controllers

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"content-management-system/src/feedcontract"
	"content-management-system/src/models"
	"content-management-system/src/supply"
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
	Phase         string  `json:"phase"`
	Outcome       string  `json:"outcome"`
	JourneyID     *string `json:"journey_id"`
	ContentID     *string `json:"content_id"`
	StoryID       *string `json:"story_id"`
	PlaybackType  *string `json:"playback_type"`
	Locale        *string `json:"locale"`
	Client        struct {
		Platform      string `json:"platform"`
		OSMajor       int    `json:"os_major"`
		BrowserFamily string `json:"browser_family"`
		BrowserMajor  int    `json:"browser_major"`
		DeviceClass   string `json:"device_class"`
		NetworkClass  string `json:"network_class"`
		InstalledPWA  bool   `json:"installed_pwa"`
	} `json:"client"`
	Measurements *struct {
		DurationMS         *int    `json:"duration_ms"`
		MediaErrorCode     *int    `json:"media_error_code"`
		StallDurationMS    *int    `json:"stall_duration_ms"`
		FailureClass       *string `json:"failure_class"`
		ObservedPlaybackMS *int    `json:"observed_playback_ms"`
		StallCount         *int    `json:"stall_count"`
		PositionDriftMS    *int    `json:"position_drift_ms"`
		FallbackUsed       *bool   `json:"fallback_used"`
		BoundaryStarved    *bool   `json:"boundary_starved"`
		Visible            *bool   `json:"visible"`
	} `json:"measurements"`
}

type incomingRuxBatch struct {
	SchemaVersion int               `json:"schema_version"`
	Events        []json.RawMessage `json:"events"`
}

// IngestExperienceEvents accepts a bounded Web/BFF or native-ticket batch. It never
// computes verdicts inline; it validates, isolates bad events, and inserts.
func IngestExperienceEvents(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	// Kill-switch: checked before any parsing so an abuse flood or bad client
	// release can be shut off from the Console instantly.
	policy, policyErr := loadExperiencePolicy(db, ruxDefaultTenant)
	if policyErr != nil {
		c.JSON(503, gin.H{"error": "telemetry policy unavailable"})
		return
	}
	if !policy.IngestEnabled {
		// 200 with disabled flag so the collector goes quiet (server-off backoff)
		// rather than retry-storming a switched-off system.
		c.JSON(http.StatusOK, gin.H{"disabled": true, "accepted": 0, "duplicate": 0, "rejected": 0})
		return
	}

	client := "web"
	if native, ok := c.Get("rux_native_ticket"); ok {
		client = native.(nativeExperienceTicket).Platform
	}
	if !experienceClientEnabled(policy, client) {
		c.JSON(http.StatusOK, gin.H{"disabled": true, "accepted": 0, "duplicate": 0, "rejected": 0})
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, ruxMaxRequestBytes)
	var batch incomingRuxBatch
	if err := c.ShouldBindJSON(&batch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid batch body"})
		return
	}
	if batch.SchemaVersion != 1 && batch.SchemaVersion != models.RuxSchemaVersion && batch.SchemaVersion != 3 {
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

	// Decode identity separately from measurements. A valid identity with a
	// mistyped measurement still participates in the mixed-session guard, while
	// the malformed event is rejected individually and cannot discard siblings.
	sessionID := ""
	rows := make([]models.ExperienceEvent, 0, len(batch.Events))
	rejected := 0
	now := time.Now().UTC()
	for _, raw := range batch.Events {
		var identity struct {
			SessionID string `json:"session_id"`
		}
		if err := json.Unmarshal(raw, &identity); err != nil || strings.TrimSpace(identity.SessionID) == "" || len(identity.SessionID) > 64 {
			rejected++
			continue
		}
		normalizedSession := strings.TrimSpace(identity.SessionID)
		if sessionID != "" && normalizedSession != sessionID {
			c.JSON(http.StatusBadRequest, gin.H{"error": "mixed session batch"})
			return
		}
		sessionID = normalizedSession
		var incoming incomingRuxEvent
		if err := json.Unmarshal(raw, &incoming); err != nil {
			rejected++
			continue
		}
		row, valid := validateAndNormalizeRuxEvent(&incoming, now)
		if !valid {
			rejected++
			continue
		}
		native, isNative := c.Get("rux_native_ticket")
		if isNative {
			ticket := native.(nativeExperienceTicket)
			if row.SchemaVersion != 3 || row.ClientPlatform != ticket.Platform || row.SessionID != ticket.Session || row.PageLoadID.String() != ticket.Page || row.Release != ticket.Release {
				rejected++
				continue
			}
		} else if row.SchemaVersion == 3 {
			rejected++
			continue
		}
		if !experienceClientEnabled(policy, row.ClientPlatform) {
			rejected++
			continue
		}
		row.TenantID = ruxDefaultTenant
		row.ReceivedAt = now
		rows = append(rows, row)
	}

	// Public content IDs are investigation identifiers, never a tenant selector.
	// One bounded lookup supplies scope proof and the existing boundary owner's
	// metadata for every ID in the batch.
	scopedItems, err := experienceContentScope(db, rows)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "diagnostic scope unavailable"})
		return
	}
	admitted := rows[:0]
	for _, row := range rows {
		if row.ContentID != nil {
			if _, exists := scopedItems[*row.ContentID]; !exists {
				rejected++
				continue
			}
		}
		admitted = append(admitted, row)
	}
	rows = admitted
	dispositions, err := persistExperienceEvents(db, rows)
	if err != nil {
		// Persistence is all-or-nothing. Never report part of a failed batch as
		// accepted, and leave the same event IDs safe for the bounded retry.
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "telemetry persistence unavailable", "accepted": 0, "duplicate": 0, "rejected": rejected})
		return
	}
	accepted, duplicate := 0, 0
	rendered := make([]models.ExperienceEvent, 0, len(rows))
	for index, disposition := range dispositions {
		switch disposition {
		case "duplicate":
			duplicate++
		case "conflict":
			rejected++
		default:
			accepted++
			event := rows[index]
			feedRendered := event.EventType == "feed_rendered" || event.EventType == "journey_terminal" && event.Phase == "feed" && event.Outcome == "success"
			if event.Surface == "pods" && feedRendered && event.ContentID != nil {
				rendered = append(rendered, event)
			}
		}
	}
	recordExperienceRenderBoundaries(db, rendered, scopedItems)

	c.JSON(http.StatusOK, gin.H{"accepted": accepted, "duplicate": duplicate, "rejected": rejected})
}

// validateAndNormalizeRuxEvent enforces the allowlist + bounded ranges. Any
// violation rejects the single event; siblings in the batch are unaffected.
func validateAndNormalizeRuxEvent(in *incomingRuxEvent, now time.Time) (models.ExperienceEvent, bool) {
	var out models.ExperienceEvent

	if in.SchemaVersion != 1 && in.SchemaVersion != models.RuxSchemaVersion && in.SchemaVersion != 3 {
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
	if !regexp.MustCompile(`^[A-Za-z0-9._-]{1,80}$`).MatchString(in.Release) {
		return out, false
	}
	occurredAt, err := time.Parse(time.RFC3339, strings.TrimSpace(in.OccurredAt))
	if err != nil {
		// Client clock is evidence, not authority — but an unparseable timestamp
		// is a malformed event.
		return out, false
	}

	if occurredAt.UTC().Year() < 1 || occurredAt.UTC().Year() > 9999 {
		return out, false
	}
	out.ClientPlatform = "web"
	if in.SchemaVersion == 3 {
		if in.Client.Platform != "ios" && in.Client.Platform != "android" || in.Client.OSMajor < 0 || in.Client.OSMajor > 999 {
			return out, false
		}
		out.ClientPlatform, out.OSMajor = in.Client.Platform, in.Client.OSMajor
	}
	out.EventID = eventID
	out.SchemaVersion = in.SchemaVersion
	out.EventType = in.EventType
	if in.SchemaVersion >= 2 {
		if in.Phase != "" && !models.RuxPhases[in.Phase] || in.Outcome != "" && !models.RuxOutcomes[in.Outcome] {
			return out, false
		}
		out.Phase = in.Phase
		out.Outcome = in.Outcome
	}
	out.Surface = in.Surface
	out.OccurredAt = occurredAt.UTC()
	out.SessionID = strings.TrimSpace(in.SessionID)
	out.PageLoadID = pageLoadID
	out.Sequence = in.Sequence
	out.Release = in.Release

	if in.JourneyID != nil {
		jid, e := uuid.Parse(strings.TrimSpace(*in.JourneyID))
		if e != nil {
			return out, false
		}
		out.JourneyID = &jid
	}
	if in.ContentID != nil {
		if cid, e := uuid.Parse(strings.TrimSpace(*in.ContentID)); e == nil {
			out.ContentID = &cid
		} else if in.SchemaVersion >= 2 {
			return out, false
		}
	}
	if in.StoryID != nil {
		if sid, e := uuid.Parse(strings.TrimSpace(*in.StoryID)); e == nil {
			out.StoryID = &sid
		} else if in.SchemaVersion >= 2 {
			return out, false
		}
	}
	if in.SchemaVersion >= 2 && in.PlaybackType != nil && !models.RuxPlaybackTypes[*in.PlaybackType] {
		return out, false
	}
	if in.PlaybackType != nil && models.RuxPlaybackTypes[*in.PlaybackType] {
		pt := *in.PlaybackType
		out.PlaybackType = &pt
	}
	if in.Locale != nil {
		loc := strings.TrimSpace(*in.Locale)
		if loc == "ar" || loc == "en" || loc == "unknown" {
			out.Locale = &loc
		} else if in.SchemaVersion >= 2 {
			return out, false
		}
	}

	if in.Sequence < 0 {
		return out, false
	}
	if in.EventType == "journey_terminal" {
		if in.SchemaVersion < 2 || out.JourneyID == nil || !models.RuxPhases[in.Phase] || !models.RuxOutcomes[in.Outcome] || in.Measurements == nil {
			return out, false
		}
		if in.Measurements.DurationMS == nil || in.Measurements.Visible == nil {
			return out, false
		}
		if in.Outcome == "empty" && in.Phase != "feed" || in.Outcome == "end_of_feed" && in.Phase != "pagination" || in.Outcome == "autoplay_blocked" && in.Phase != "startup" {
			return out, false
		}
		if in.Outcome == "success" && in.Phase == "continuation" && (in.Measurements.ObservedPlaybackMS == nil || in.Measurements.StallCount == nil || in.Measurements.StallDurationMS == nil) {
			return out, false
		}
		if in.Phase == "handoff" && in.Outcome == "success" && in.Measurements.PositionDriftMS == nil {
			return out, false
		}
		out.TerminalStatus = "accepted"
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
		for _, bound := range []struct {
			v   *int
			max int
		}{{m.DurationMS, models.RuxMaxDurationMS}, {m.StallDurationMS, models.RuxMaxStallMS}, {m.ObservedPlaybackMS, models.RuxMaxDurationMS}, {m.StallCount, 10000}, {m.PositionDriftMS, models.RuxMaxDurationMS}} {
			if bound.v != nil && (*bound.v < 0 || *bound.v > bound.max) {
				return out, false
			}
		}
		if m.FailureClass != nil && !models.RuxFailureClasses[*m.FailureClass] {
			return out, false
		}
		if m.MediaErrorCode != nil && (*m.MediaErrorCode < 1 || *m.MediaErrorCode > 4) {
			return out, false
		}
		out.ObservedPlaybackMS = m.ObservedPlaybackMS
		out.StallCount = m.StallCount
		out.PositionDriftMS = m.PositionDriftMS
		out.FallbackUsed = m.FallbackUsed
		out.BoundaryStarved = m.BoundaryStarved
		if m.StallDurationMS != nil && m.ObservedPlaybackMS != nil && *m.StallDurationMS > *m.ObservedPlaybackMS {
			return out, false
		}
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
			"visible": out.Visible, "observed_playback_ms": out.ObservedPlaybackMS, "stall_count": out.StallCount, "position_drift_ms": out.PositionDriftMS, "fallback_used": out.FallbackUsed, "boundary_starved": out.BoundaryStarved,
		}); e == nil {
			out.Measurements = b
		}
	}

	return out, true
}

// getOrCreateExperiencePolicy returns the singleton Observe policy, creating the
// default row on first use so a missing seed never breaks ingestion.
func loadExperiencePolicy(db *gorm.DB, tenantID string) (models.ExperiencePolicy, error) {
	var p models.ExperiencePolicy
	err := db.Where("tenant_id = ?", tenantID).First(&p).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		p = models.DefaultExperiencePolicy(tenantID)
		if err = db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}}, DoNothing: true}).Create(&p).Error; err != nil {
			return p, err
		}
		err = db.Where("tenant_id = ?", tenantID).First(&p).Error
	}
	return p, err
}

// Existing owner integrations use this compatibility helper. RUX HTTP/runner
// paths use loadExperiencePolicy and propagate errors.
func getOrCreateExperiencePolicy(db *gorm.DB, tenantID string) models.ExperiencePolicy {
	p, _ := loadExperiencePolicy(db, tenantID)
	return p
}

func terminalReceipt(ev *models.ExperienceEvent) models.ExperienceTerminalReceipt {
	identity := fmt.Sprintf("%s\n%s\n%s\n%s\n%s", ev.TenantID, ev.SessionID, ev.PageLoadID, ev.JourneyID, ev.Phase)
	if models.RuxPlatform(ev.ClientPlatform) != "web" {
		identity = ev.ClientPlatform + "\n" + identity
	}
	dimensions := []any{ev.Surface, ev.Phase, ev.Outcome, ev.Release, ev.BrowserFamily, ev.BrowserMajor, ev.DeviceClass, ev.NetworkClass, ev.InstalledPWA, ev.Locale, ev.ContentID, ev.StoryID, ev.PlaybackType, ev.Measurements}
	if models.RuxPlatform(ev.ClientPlatform) != "web" {
		dimensions = append(dimensions, ev.OSMajor)
	}
	payload, _ := json.Marshal(dimensions)
	return models.ExperienceTerminalReceipt{Identity: fmt.Sprintf("%x", sha256.Sum256([]byte(identity))), Digest: fmt.Sprintf("%x", sha256.Sum256(payload))}
}

// experienceContentScope returns only the locked public-feed tenant's items.
func experienceContentScope(db *gorm.DB, events []models.ExperienceEvent) (map[uuid.UUID]models.ContentItem, error) {
	ids := make([]uuid.UUID, 0, len(events))
	seen := make(map[uuid.UUID]bool, len(events))
	for _, event := range events {
		if event.ContentID != nil && !seen[*event.ContentID] {
			seen[*event.ContentID] = true
			ids = append(ids, *event.ContentID)
		}
	}
	items := make(map[uuid.UUID]models.ContentItem, len(ids))
	if len(ids) == 0 {
		return items, nil
	}
	var rows []models.ContentItem
	if err := db.Select("public_id", "tenant_id", "source_run_request_id").Where("tenant_id = ? AND public_id IN ?", ruxDefaultTenant, ids).Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		items[row.PublicID] = row
	}
	return items, nil
}

func recordExperienceRenderBoundaries(db *gorm.DB, events []models.ExperienceEvent, items map[uuid.UUID]models.ContentItem) {
	if len(events) == 0 {
		return
	}
	generationID, _, active := feedcontract.ActiveGeneration(db, ruxDefaultTenant, "media")
	var generation *uuid.UUID
	if active {
		generation = &generationID
	}
	observations := make([]models.PodsBoundaryObservation, 0, len(events))
	for _, event := range events {
		item := items[*event.ContentID]
		observations = append(observations, models.PodsBoundaryObservation{
			TenantID: ruxDefaultTenant, ContentItemID: item.PublicID, GenerationID: generation,
			Boundary: "page_render", ProbeKind: "frozen_session", ProbeID: event.PageLoadID.String(),
			SourceRunRequestID: item.SourceRunRequestID, Verdict: string(supply.VerdictPresent), ObservedAt: event.OccurredAt.UTC(),
		})
	}
	// Supply validates/proves and writes its own observations and yield effects
	// atomically in one bounded batch. RUX does not gain ledger authority.
	_ = supply.RecordPodsBoundaryObservations(db, observations)
}

// The fixed list carries validated envelope columns only. The numeric ID stays
// database-owned and created_at is stamped by the server, never by client JSON.
const experienceEventInsertColumns = `event_id,tenant_id,schema_version,event_type,surface,client_platform,os_major,occurred_at,received_at,session_id,page_load_id,sequence,journey_id,release,content_id,story_id,playback_type,locale,browser_family,browser_major,device_class,network_class,installed_pwa,duration_ms,media_error_code,stall_duration_ms,failure_class,visible,phase,outcome,terminal_status,observed_playback_ms,stall_count,position_drift_ms,fallback_used,boundary_starved,measurements`

type experienceEventInsertResult struct {
	ID      uint
	EventID uuid.UUID
}

// persistExperienceEvents atomically admits one bounded batch. Event insertion
// determines which transport IDs are new before receipts are reserved, so a
// retried/reused event ID cannot create an orphan logical-terminal receipt.
// Unique receipt insertion serializes concurrent terminals; the following read
// sees the winner under PostgreSQL READ COMMITTED, without per-event queries.
func persistExperienceEvents(db *gorm.DB, events []models.ExperienceEvent) ([]string, error) {
	if len(events) > ruxMaxBatchEvents {
		return nil, errors.New("experience batch exceeds persistence bound")
	}
	dispositions := make([]string, len(events))
	if len(events) == 0 {
		return dispositions, nil
	}
	originalIndexes := make(map[uuid.UUID]int, len(events))
	uniqueEvents := make([]models.ExperienceEvent, 0, len(events))
	for index := range events {
		dispositions[index] = "duplicate"
		if _, exists := originalIndexes[events[index].EventID]; exists {
			continue
		}
		originalIndexes[events[index].EventID] = index
		event := events[index]
		event.ClientPlatform = models.RuxPlatform(event.ClientPlatform)
		event.OccurredAt = event.OccurredAt.UTC()
		event.ReceivedAt = event.ReceivedAt.UTC()
		if event.EventType == "journey_terminal" {
			event.TerminalStatus = ""
		}
		uniqueEvents = append(uniqueEvents, event)
	}
	// Every batch acquires event/receipt unique-key locks in the same order,
	// preventing reversed overlapping batches from deadlocking on those keys.
	sort.Slice(uniqueEvents, func(i, j int) bool { return uniqueEvents[i].EventID.String() < uniqueEvents[j].EventID.String() })
	payload, err := json.Marshal(uniqueEvents)
	if err != nil {
		return nil, err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		var inserted []experienceEventInsertResult
		statement := "INSERT INTO experience_events (" + experienceEventInsertColumns + ",created_at) SELECT " + experienceEventInsertColumns + ",CURRENT_TIMESTAMP FROM jsonb_populate_recordset(NULL::experience_events, ?::jsonb) ORDER BY event_id ON CONFLICT (event_id) DO NOTHING RETURNING id,event_id"
		if err := tx.Raw(statement, string(payload)).Scan(&inserted).Error; err != nil {
			return err
		}
		insertedIDs := make(map[uuid.UUID]uint, len(inserted))
		for _, row := range inserted {
			insertedIDs[row.EventID] = row.ID
		}
		receipts := make(map[string]models.ExperienceTerminalReceipt)
		firstTerminals := make(map[string]uuid.UUID)
		for index := range events {
			event := &events[index]
			if originalIndexes[event.EventID] != index {
				continue
			}
			id, exists := insertedIDs[event.EventID]
			if !exists {
				continue
			}
			event.ID = id
			event.OccurredAt = event.OccurredAt.UTC()
			event.ReceivedAt = event.ReceivedAt.UTC()
			dispositions[index] = "accepted"
			if event.EventType != "journey_terminal" {
				continue
			}
			receipt := terminalReceipt(event)
			if _, exists := receipts[receipt.Identity]; !exists {
				receipts[receipt.Identity] = receipt
				firstTerminals[receipt.Identity] = event.EventID
			}
		}
		if len(receipts) == 0 {
			return nil
		}
		identities := make([]string, 0, len(receipts))
		for identity := range receipts {
			identities = append(identities, identity)
		}
		sort.Strings(identities)
		receiptRows := make([]models.ExperienceTerminalReceipt, 0, len(identities))
		for _, identity := range identities {
			receiptRows = append(receiptRows, receipts[identity])
		}
		receiptJSON, err := json.Marshal(receiptRows)
		if err != nil {
			return err
		}
		// Receipt JSON uses model field names; the explicit record shape keeps
		// server creation time and the identity/digest columns unambiguous.
		var created []models.ExperienceTerminalReceipt
		if err := tx.Raw(`INSERT INTO experience_terminal_receipts(identity,digest,created_at)
            SELECT "Identity","Digest",CURRENT_TIMESTAMP FROM jsonb_to_recordset(?::jsonb) AS receipt("Identity" text,"Digest" text)
            ORDER BY "Identity" ON CONFLICT (identity) DO NOTHING RETURNING identity,digest`, string(receiptJSON)).Scan(&created).Error; err != nil {
			return err
		}
		createdIDs := make(map[string]bool, len(created))
		for _, receipt := range created {
			createdIDs[receipt.Identity] = true
		}
		var saved []models.ExperienceTerminalReceipt
		if err := tx.Where("identity IN ?", identities).Find(&saved).Error; err != nil {
			return err
		}
		savedDigests := make(map[string]string, len(saved))
		for _, receipt := range saved {
			savedDigests[receipt.Identity] = receipt.Digest
		}
		statuses := make([]map[string]interface{}, 0, len(inserted))
		for index := range events {
			event := &events[index]
			if originalIndexes[event.EventID] != index || insertedIDs[event.EventID] == 0 || event.EventType != "journey_terminal" {
				continue
			}
			receipt := terminalReceipt(event)
			digest, found := savedDigests[receipt.Identity]
			if !found {
				return errors.New("experience receipt missing after admission")
			}
			disposition := "duplicate"
			if createdIDs[receipt.Identity] && firstTerminals[receipt.Identity] == event.EventID {
				disposition = "accepted"
			} else if receipt.Digest != digest {
				disposition = "conflict"
			}
			dispositions[index] = disposition
			event.TerminalStatus = disposition
			statuses = append(statuses, map[string]interface{}{"event_id": event.EventID, "terminal_status": disposition})
		}
		statusJSON, err := json.Marshal(statuses)
		if err != nil {
			return err
		}
		result := tx.Exec(`UPDATE experience_events e SET terminal_status=terminal.terminal_status
            FROM jsonb_to_recordset(?::jsonb) AS terminal(event_id uuid,terminal_status varchar(16))
            WHERE e.event_id=terminal.event_id`, string(statusJSON))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != int64(len(statuses)) {
			return errors.New("experience terminal admission incomplete")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dispositions, nil
}

func persistExperienceEvent(db *gorm.DB, event *models.ExperienceEvent) (string, error) {
	rows := []models.ExperienceEvent{*event}
	dispositions, err := persistExperienceEvents(db, rows)
	if err != nil {
		return "", err
	}
	*event = rows[0]
	return dispositions[0], nil
}
