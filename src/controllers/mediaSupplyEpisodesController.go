package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const mediaSupplyEpisodePageLimit = 50

// EvaluateMediaSupplyNow records CMS-derived attention only. It neither
// creates source work nor grants a repair capability; its durable episode and
// append-only event make an observed handoff problem survivable across restarts.
func EvaluateMediaSupplyNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	status := buildMediaSupplyStatus(db, principal.TenantID)
	// The same explicit evaluation may safely close a prior episode when the
	// recovery verifier has fresh proof. This remains available during a
	// recording stop because it only converges existing durable state.
	if err := reconcileMediaSupplyEpisodes(db, principal.TenantID, status); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Could not reconcile Media Supply episode recovery evidence"})
		return
	}
	enabled, err := mediaSupplyEvaluationRecordingEnabled(db, principal.TenantID)
	if err != nil {
		recordMediaSupplyEvaluationCheckpointFailure(db, principal.TenantID, models.MediaSupplyEvaluationTriggerManual, models.MediaSupplyEvaluationOutcomeControlUnavailable)
		// Fail closed for the evidence-writing route while keeping the status
		// endpoint available. A control-ledger outage cannot become a bypass.
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Media Supply evaluation controls are unavailable"})
		return
	}
	if !enabled {
		recordMediaSupplyEvaluationCheckpointFailure(db, principal.TenantID, models.MediaSupplyEvaluationTriggerManual, models.MediaSupplyEvaluationOutcomeDisabled)
		c.JSON(http.StatusLocked, gin.H{"message": "Media Supply evaluation recording is disabled for this tenant"})
		return
	}
	episode, created, err := recordMediaSupplyEpisode(db, principal.TenantID, status.SupplyEvaluation)
	if err != nil {
		recordMediaSupplyEvaluationCheckpointFailure(db, principal.TenantID, models.MediaSupplyEvaluationTriggerManual, models.MediaSupplyEvaluationOutcomeRecordFailed)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Could not persist Media Supply evaluation evidence"})
		return
	}
	if err := recordMediaSupplyEvaluationCheckpoint(db, principal.TenantID, models.MediaSupplyEvaluationTriggerManual, status.SupplyEvaluation); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Could not persist Media Supply evaluation checkpoint"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"schema_version": supply.SupplyEvaluationSchemaVersion,
		"evaluation":     status.SupplyEvaluation,
		"episode":        episode,
		"episode_opened": created,
	})
}

func recordMediaSupplyEpisode(db *gorm.DB, tenantID string, evaluation supply.SupplyEvaluation) (*models.MediaSupplyEpisode, bool, error) {
	if !supply.IsEpisodeWorthy(evaluation.Verdict) {
		return nil, false, nil
	}
	fingerprint, err := supply.EpisodeFingerprint(tenantID, evaluation)
	if err != nil {
		return nil, false, err
	}
	evidenceDigest, err := supply.EvaluationEvidenceDigest(evaluation)
	if err != nil {
		return nil, false, err
	}
	evidence, err := json.Marshal(evaluation)
	if err != nil {
		return nil, false, err
	}
	subjects, err := mediaSupplyAffectedSubjects(evaluation)
	if err != nil {
		return nil, false, err
	}
	now := evaluation.EvaluatedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	var result models.MediaSupplyEpisode
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		var episode models.MediaSupplyEpisode
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ? AND fingerprint = ? AND state IN ?", tenantID, fingerprint, []string{models.MediaSupplyEpisodeOpen, models.MediaSupplyEpisodeRecovering}).
			First(&episode).Error
		if err == nil {
			if err := tx.Model(&episode).Updates(map[string]interface{}{
				"first_failed_boundary": evaluation.HeadlineBoundary,
				"verdict":               string(evaluation.Verdict),
				"severity":              supply.EpisodeSeverity(evaluation.Verdict),
				"owner":                 evaluation.Owner,
				"summary":               evaluation.Summary,
				"affected_subjects":     datatypes.JSON(subjects),
				"evidence_digest":       evidenceDigest,
				"evidence_completeness": evaluation.EvidenceCompleteness,
				"evidence":              datatypes.JSON(evidence),
				"last_seen_at":          now,
				"updated_at":            now,
			}).Error; err != nil {
				return err
			}
			if err := tx.Where("id = ?", episode.ID).First(&episode).Error; err != nil {
				return err
			}
			result = episode
			return appendMediaSupplyEpisodeEvent(tx, episode, "observed", evidenceDigest, evidence, now)
		}
		if err != gorm.ErrRecordNotFound {
			return err
		}

		episode = models.MediaSupplyEpisode{
			PublicID: uuid.New(), TenantID: tenantID, Fingerprint: fingerprint,
			FirstFailedBoundary: evaluation.HeadlineBoundary, Verdict: string(evaluation.Verdict),
			Severity: supply.EpisodeSeverity(evaluation.Verdict), Owner: evaluation.Owner,
			State: models.MediaSupplyEpisodeOpen, Summary: evaluation.Summary,
			AffectedSubjects: datatypes.JSON(subjects), EvidenceDigest: evidenceDigest,
			EvidenceCompleteness: evaluation.EvidenceCompleteness, Evidence: datatypes.JSON(evidence),
			FirstSeenAt: now, LastSeenAt: now,
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&episode).Error; err != nil {
			return err
		}
		if episode.ID == 0 {
			// A concurrent evaluator opened the same deterministic fingerprint.
			// Re-enter through the locked path rather than creating a second active
			// incident or changing the evidence identity.
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("tenant_id = ? AND fingerprint = ? AND state IN ?", tenantID, fingerprint, []string{models.MediaSupplyEpisodeOpen, models.MediaSupplyEpisodeRecovering}).
				First(&episode).Error; err != nil {
				return err
			}
			if err := tx.Model(&episode).Updates(map[string]interface{}{
				"last_seen_at": now, "evidence_digest": evidenceDigest, "evidence": datatypes.JSON(evidence),
				"evidence_completeness": evaluation.EvidenceCompleteness, "updated_at": now,
			}).Error; err != nil {
				return err
			}
			result = episode
			return appendMediaSupplyEpisodeEvent(tx, episode, "observed", evidenceDigest, evidence, now)
		}
		created = true
		result = episode
		return appendMediaSupplyEpisodeEvent(tx, episode, "opened", evidenceDigest, evidence, now)
	})
	if err != nil {
		return nil, false, err
	}
	return &result, created, nil
}

func appendMediaSupplyEpisodeEvent(db *gorm.DB, episode models.MediaSupplyEpisode, eventType, evidenceDigest string, evaluation []byte, occurredAt time.Time) error {
	return appendMediaSupplyEpisodeEventPayload(db, episode, eventType, evidenceDigest, evaluation, occurredAt)
}

// appendMediaSupplyEpisodeEventPayload keeps the append-only event ledger
// generic enough for both observation snapshots and independently verified
// recovery proof. The event type remains static at each call site.
func appendMediaSupplyEpisodeEventPayload(db *gorm.DB, episode models.MediaSupplyEpisode, eventType, evidenceDigest string, payload []byte, occurredAt time.Time) error {
	event := models.MediaSupplyEpisodeEvent{
		PublicID: uuid.New(), TenantID: episode.TenantID, EpisodeID: episode.PublicID,
		EventKey: fmt.Sprintf("%s:%s", episode.PublicID.String(), evidenceDigest), EventType: eventType,
		EvidenceDigest: evidenceDigest, Evaluation: datatypes.JSON(payload), OccurredAt: occurredAt,
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(&event).Error
}

func mediaSupplyAffectedSubjects(evaluation supply.SupplyEvaluation) ([]byte, error) {
	subjects := make([]map[string]string, 0, len(evaluation.AffectedSourceIDs)+len(evaluation.AffectedRequestIDs))
	for _, id := range evaluation.AffectedSourceIDs {
		subjects = append(subjects, map[string]string{"type": "content_source", "id": id})
	}
	for _, id := range evaluation.AffectedRequestIDs {
		subjects = append(subjects, map[string]string{"type": "source_run_request", "id": id})
	}
	return json.Marshal(subjects)
}

type mediaSupplyCursor struct {
	At time.Time `json:"at"`
	ID uuid.UUID `json:"id"`
}

func encodeMediaSupplyCursor(at time.Time, id uuid.UUID) string {
	payload, _ := json.Marshal(mediaSupplyCursor{At: at.UTC(), ID: id})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeMediaSupplyCursor(raw string) (mediaSupplyCursor, error) {
	bytes, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return mediaSupplyCursor{}, err
	}
	var cursor mediaSupplyCursor
	if err := json.Unmarshal(bytes, &cursor); err != nil || cursor.At.IsZero() || cursor.ID == uuid.Nil {
		return mediaSupplyCursor{}, fmt.Errorf("invalid media supply cursor")
	}
	return cursor, nil
}

func mediaSupplyPageLimit(c *gin.Context) (int, error) {
	raw := strings.TrimSpace(c.Query("limit"))
	if raw == "" {
		return 25, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > mediaSupplyEpisodePageLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", mediaSupplyEpisodePageLimit)
	}
	return limit, nil
}

func ListMediaSupplyEpisodes(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	limit, err := mediaSupplyPageLimit(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}
	query := c.MustGet("db").(*gorm.DB).Where("tenant_id = ?", principal.TenantID)
	if raw := c.Query("cursor"); raw != "" {
		cursor, err := decodeMediaSupplyCursor(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Media Supply episode cursor"})
			return
		}
		query = query.Where("(last_seen_at, public_id) < (?, ?)", cursor.At, cursor.ID)
	}
	var rows []models.MediaSupplyEpisode
	if err := query.Order("last_seen_at DESC, public_id DESC").Limit(limit + 1).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Could not load Media Supply episodes"})
		return
	}
	next := ""
	if len(rows) > limit {
		last := rows[limit-1]
		next = encodeMediaSupplyCursor(last.LastSeenAt, last.PublicID)
		rows = rows[:limit]
	}
	c.JSON(http.StatusOK, gin.H{"schema_version": supply.SupplyEvaluationSchemaVersion, "items": rows, "next_cursor": next})
}

func GetMediaSupplyEpisode(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Media Supply episode ID"})
		return
	}
	var episode models.MediaSupplyEpisode
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&episode).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"message": "Media Supply episode not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Could not load Media Supply episode"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"schema_version": supply.SupplyEvaluationSchemaVersion, "item": episode})
}

func ListMediaSupplyEpisodeEvents(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	limit, err := mediaSupplyPageLimit(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}
	query := c.MustGet("db").(*gorm.DB).Where("tenant_id = ?", principal.TenantID)
	if raw := c.Query("after"); raw != "" {
		cursor, err := decodeMediaSupplyCursor(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid Media Supply event cursor"})
			return
		}
		query = query.Where("(occurred_at, public_id) < (?, ?)", cursor.At, cursor.ID)
	}
	var rows []models.MediaSupplyEpisodeEvent
	if err := query.Order("occurred_at DESC, public_id DESC").Limit(limit + 1).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Could not load Media Supply events"})
		return
	}
	next := ""
	if len(rows) > limit {
		last := rows[limit-1]
		next = encodeMediaSupplyCursor(last.OccurredAt, last.PublicID)
		rows = rows[:limit]
	}
	c.JSON(http.StatusOK, gin.H{"schema_version": supply.SupplyEvaluationSchemaVersion, "items": rows, "next_after": next})
}
