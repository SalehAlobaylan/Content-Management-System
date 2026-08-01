package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const investigationRetention = 30 * 24 * time.Hour

const investigationClaimLease = 90 * time.Second

var (
	operatorBearerPattern = regexp.MustCompile(`(?i)\bbearer\s+[a-z0-9._~+\-/=]{8,}`)
	operatorSecretPattern = regexp.MustCompile(`(?i)\b(?:api[_ -]?key|token|secret|password|authorization|cookie)\s*[:=]\s*[^\s,;]+`)
)

// InvestigationStore persists short-lived, replayable investigation progress.
// It has no authority to create plans, approve, or execute actions.
type InvestigationStore struct {
	db  *gorm.DB
	now func() time.Time
}

type ResponseBlock struct {
	Kind        string   `json:"kind"`
	Text        string   `json:"text"`
	EvidenceIDs []string `json:"evidence_ids,omitempty"`
}

// InvestigationRequest is the minimal redacted envelope required to resume a
// backgrounded investigation. It intentionally contains no user JWT, plan,
// evidence, or permission decision.
type InvestigationRequest struct {
	Intent                   Intent `json:"intent"`
	Message                  string `json:"message"`
	Tier                     string `json:"tier"`
	CredentialRedactionCount int    `json:"credential_redaction_count"`
}

func NewInvestigationRequest(intent Intent, message, tier string) (InvestigationRequest, error) {
	if _, ok := validIntents[intent]; !ok || strings.TrimSpace(message) == "" || len(message) > 8000 || (tier != "fast" && tier != "reasoning") {
		return InvestigationRequest{}, fmt.Errorf("%w: invalid persisted investigation request", ErrInvalidContract)
	}
	redacted, redactionCount := redactOperatorRequestText(message)
	return InvestigationRequest{Intent: intent, Message: redacted, Tier: tier, CredentialRedactionCount: redactionCount}, nil
}

func redactOperatorRequestText(value string) (string, int) {
	count := 0
	value = operatorBearerPattern.ReplaceAllStringFunc(value, func(match string) string {
		count++
		return "[REDACTED_BEARER]"
	})
	value = operatorSecretPattern.ReplaceAllStringFunc(value, func(match string) string {
		if strings.Contains(match, "[REDACTED_SECRET]") {
			return match
		}
		count++
		return "[REDACTED_SECRET]"
	})
	return value, count
}

func (request InvestigationRequest) Validate() error {
	normalized, err := NewInvestigationRequest(request.Intent, request.Message, request.Tier)
	if err != nil {
		return err
	}
	markerCount := strings.Count(request.Message, "[REDACTED_BEARER]") + strings.Count(request.Message, "[REDACTED_SECRET]")
	if normalized.Message != request.Message || request.CredentialRedactionCount != markerCount {
		return fmt.Errorf("%w: persisted request must already be redacted", ErrInvalidContract)
	}
	return nil
}

func (block ResponseBlock) Validate() error {
	if strings.TrimSpace(block.Text) == "" || (block.Kind != "fact" && block.Kind != "interpretation" && block.Kind != "unknown" && block.Kind != "recommendation" && block.Kind != "degraded") {
		return fmt.Errorf("%w: invalid response block", ErrInvalidContract)
	}
	if (block.Kind == "fact" || block.Kind == "interpretation") && len(block.EvidenceIDs) == 0 {
		return fmt.Errorf("%w: factual response blocks require evidence", ErrInvalidContract)
	}
	return nil
}

func NewInvestigationStore(db *gorm.DB) *InvestigationStore {
	return &InvestigationStore{db: db, now: func() time.Time { return time.Now().UTC() }}
}

func (store *InvestigationStore) Create(ctx context.Context, tenantID, actorID, locale string, visible VisibleContext, request InvestigationRequest, threadID *uint) (models.OperatorInvestigation, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(actorID) == "" || (locale != "ar" && locale != "en") {
		return models.OperatorInvestigation{}, fmt.Errorf("%w: invalid investigation identity", ErrInvalidContract)
	}
	if err := visible.Validate(); err != nil {
		return models.OperatorInvestigation{}, err
	}
	if err := request.Validate(); err != nil {
		return models.OperatorInvestigation{}, err
	}
	raw, err := json.Marshal(visible)
	if err != nil {
		return models.OperatorInvestigation{}, fmt.Errorf("%w: encode visible context", ErrInvalidContract)
	}
	requestRaw, err := json.Marshal(request)
	if err != nil {
		return models.OperatorInvestigation{}, fmt.Errorf("%w: encode investigation request", ErrInvalidContract)
	}
	now := store.now()
	investigation := models.OperatorInvestigation{TenantID: tenantID, ThreadID: threadID, ActorID: actorID, State: "accepted", VisibleContext: datatypes.JSON(raw), Request: datatypes.JSON(requestRaw), Locale: locale, StartedAt: now, ExpiresAt: now.Add(investigationRetention)}
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if threadID != nil {
			var thread models.OperatorThread
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=? AND creator_id=?", *threadID, tenantID, actorID).First(&thread).Error; err != nil {
				return err
			}
			if !now.Before(thread.ExpiresAt) {
				return fmt.Errorf("%w: conversation thread has expired", ErrInvalidContract)
			}
		}
		if err := tx.Create(&investigation).Error; err != nil {
			return err
		}
		return appendInvestigationEvent(tx, investigation.ID, tenantID, "accepted", map[string]any{"domain": visible.Domain, "view": visible.View, "credential_redactions": request.CredentialRedactionCount})
	})
	return investigation, err
}

// QueueBackground records that the request has detached from the browser. The
// investigation is still read-only, but this durable state makes a disconnect
// harmless and prevents a later worker from silently treating it as a fresh
// synchronous request.
func (store *InvestigationStore) QueueBackground(ctx context.Context, investigationID uint, tenantID string) error {
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var investigation models.OperatorInvestigation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", investigationID, tenantID).First(&investigation).Error; err != nil {
			return err
		}
		if investigation.State != "accepted" {
			return fmt.Errorf("%w: investigation cannot be backgrounded", ErrInvalidContract)
		}
		if err := tx.Model(&investigation).Update("state", "backgrounded").Error; err != nil {
			return err
		}
		return appendInvestigationEvent(tx, investigation.ID, tenantID, "backgrounded", map[string]any{"state": "backgrounded"})
	})
	return err
}

func (store *InvestigationStore) Begin(ctx context.Context, investigationID uint, tenantID string) (uuid.UUID, error) {
	claimToken := uuid.New()
	claimExpiresAt := store.now().Add(investigationClaimLease)
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var investigation models.OperatorInvestigation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", investigationID, tenantID).First(&investigation).Error; err != nil {
			return err
		}
		if investigation.State != "accepted" && investigation.State != "backgrounded" {
			return fmt.Errorf("%w: investigation is not runnable", ErrInvalidContract)
		}
		if err := tx.Model(&investigation).Updates(map[string]any{"state": "running", "claim_token": claimToken, "claim_expires_at": claimExpiresAt}).Error; err != nil {
			return err
		}
		return appendInvestigationEvent(tx, investigation.ID, tenantID, "context_collecting", map[string]any{"state": "running", "claim_expires_at": claimExpiresAt})
	})
	return claimToken, err
}

// PersistPacket writes the complete CMS packet and its evidence before a
// response can be emitted. Packet validation prevents a foreign tenant or an
// orphan evidence ID from becoming durable history.
func (store *InvestigationStore) PersistPacket(ctx context.Context, investigationID uint, claimToken uuid.UUID, packet DecisionPacket) error {
	if err := packet.Validate(); err != nil {
		return err
	}
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var investigation models.OperatorInvestigation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&investigation, investigationID).Error; err != nil {
			return err
		}
		if investigation.TenantID != packet.TenantID || investigation.ActorID != packet.ActorID || investigation.State != "running" || investigation.ClaimToken == nil || *investigation.ClaimToken != claimToken || investigation.ClaimExpiresAt == nil || !store.now().Before(*investigation.ClaimExpiresAt) {
			return fmt.Errorf("%w: investigation context changed", ErrInvalidContract)
		}
		if err := tx.Model(&investigation).Updates(map[string]any{"state": "running", "packet_fingerprint": packet.Fingerprint}).Error; err != nil {
			return err
		}
		rows := make([]models.OperatorEvidence, 0, len(packet.Evidence))
		for _, evidence := range packet.Evidence {
			refs, err := json.Marshal(evidence.RecordRefs)
			if err != nil {
				return err
			}
			rows = append(rows, models.OperatorEvidence{InvestigationID: investigation.ID, TenantID: packet.TenantID, EvidenceID: evidence.EvidenceID, Authority: string(evidence.Authority), Domain: evidence.Domain, AdapterKey: evidence.AdapterKey, AdapterVersion: evidence.AdapterVersion, RequiredPermission: evidence.RequiredPermission, RecordRefs: datatypes.JSON(refs), DeepLink: evidence.DeepLink, ObservedAt: evidence.ObservedAt, FetchedAt: evidence.FetchedAt, MaxAgeSeconds: evidence.MaxAgeSeconds, ExpiresAt: evidence.ExpiresAt, ContentHash: evidence.ContentHash, SourceVersion: evidence.SourceVersion, Availability: string(evidence.Availability)})
		}
		if len(rows) > 0 {
			if err := tx.Create(&rows).Error; err != nil {
				return err
			}
		}
		// Recommendation state is intentionally persisted separately from the
		// packet event so routine snooze/dismiss feedback cannot rewrite the
		// immutable evidence packet or manufacture an action.
		recommendations := make([]models.OperatorRecommendation, 0, len(packet.Recommendations))
		for index, recommendation := range packet.Recommendations {
			evidenceIDs, err := json.Marshal(recommendation.EvidenceIDs)
			if err != nil {
				return err
			}
			payload, err := json.Marshal(map[string]any{"kind": recommendation.Kind, "title": recommendation.Title, "summary": recommendation.Summary, "deep_link": recommendation.DeepLink, "manual_only": recommendation.ManualOnly})
			if err != nil {
				return err
			}
			subjectType, subjectID := "tenant", packet.TenantID
			if len(packet.VisibleContext.Subjects) > 0 {
				subjectType, subjectID = packet.VisibleContext.Subjects[0].Type, packet.VisibleContext.Subjects[0].ID
			}
			rank := 0
			if index < 4 {
				rank = index + 1
			}
			recommendations = append(recommendations, models.OperatorRecommendation{InvestigationID: &investigation.ID, TenantID: packet.TenantID, RecommendationKey: recommendation.ID, SubjectType: subjectType, SubjectID: subjectID, Rank: rank, State: "eligible", EvidenceIDs: datatypes.JSON(evidenceIDs), Payload: datatypes.JSON(payload), ExpiresAt: investigation.ExpiresAt})
		}
		if len(recommendations) > 0 {
			if err := tx.Create(&recommendations).Error; err != nil {
				return err
			}
		}
		return appendInvestigationEvent(tx, investigation.ID, packet.TenantID, "packet_ready", map[string]any{"packet_fingerprint": packet.Fingerprint, "evidence_count": len(rows), "unknown_count": len(packet.Unknowns), "conflict_count": len(packet.Conflicts), "recommendations": packet.Recommendations})
	})
}

// RankRecommendations persists the validated LLM hierarchy independently from
// packet order. It never creates recommendations: it can only rank CMS-owned,
// evidence-bound records already persisted with the packet.
func (store *InvestigationStore) RankRecommendations(ctx context.Context, investigationID uint, tenantID string, primary *string, secondary []string) error {
	if len(secondary) > 3 {
		return fmt.Errorf("%w: too many secondary recommendations", ErrInvalidContract)
	}
	ranked := make([]string, 0, 4)
	if primary != nil && strings.TrimSpace(*primary) != "" {
		ranked = append(ranked, *primary)
	}
	for _, key := range secondary {
		if strings.TrimSpace(key) != "" {
			ranked = append(ranked, key)
		}
	}
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.OperatorRecommendation{}).Where("investigation_id=? AND tenant_id=?", investigationID, tenantID).Update("rank", 0).Error; err != nil {
			return err
		}
		for index, key := range ranked {
			result := tx.Model(&models.OperatorRecommendation{}).Where("investigation_id=? AND tenant_id=? AND recommendation_key=?", investigationID, tenantID, key).Update("rank", index+1)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("%w: ranked recommendation is not persisted", ErrInvalidContract)
			}
		}
		return appendInvestigationEvent(tx, investigationID, tenantID, "recommendations_ranked", map[string]any{"primary_recommendation_id": primary, "secondary_recommendation_ids": secondary})
	})
}

// Complete records only validated CMS response blocks. Response validation and
// authorization happen before this method; it just provides ordered durable
// delivery for reconnecting Console clients.
func (store *InvestigationStore) Complete(ctx context.Context, investigationID uint, tenantID string, claimToken uuid.UUID, blocks []ResponseBlock) error {
	now := store.now()
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var investigation models.OperatorInvestigation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", investigationID, tenantID).First(&investigation).Error; err != nil {
			return err
		}
		if investigation.State != "running" || investigation.ClaimToken == nil || *investigation.ClaimToken != claimToken || investigation.ClaimExpiresAt == nil || !now.Before(*investigation.ClaimExpiresAt) {
			return fmt.Errorf("%w: investigation is not running", ErrInvalidContract)
		}
		var evidence []models.OperatorEvidence
		if err := tx.Where("investigation_id=? AND tenant_id=?", investigation.ID, tenantID).Find(&evidence).Error; err != nil {
			return err
		}
		knownEvidence := make(map[string]struct{}, len(evidence))
		for _, row := range evidence {
			knownEvidence[row.EvidenceID] = struct{}{}
		}
		for _, block := range blocks {
			if err := block.Validate(); err != nil {
				return err
			}
			for _, evidenceID := range block.EvidenceIDs {
				if _, ok := knownEvidence[evidenceID]; !ok {
					return fmt.Errorf("%w: response cites unknown evidence", ErrInvalidContract)
				}
			}
			if err := appendInvestigationEvent(tx, investigation.ID, tenantID, "response_block", map[string]any{"kind": block.Kind, "text": block.Text, "evidence_ids": block.EvidenceIDs}); err != nil {
				return err
			}
		}
		if err := tx.Model(&investigation).Updates(map[string]any{"state": "completed", "finished_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		return appendInvestigationEvent(tx, investigation.ID, tenantID, "done", map[string]any{"state": "completed"})
	})
}

func (store *InvestigationStore) Fail(ctx context.Context, investigationID uint, tenantID, errorClass string) error {
	if strings.TrimSpace(errorClass) == "" {
		errorClass = "unknown"
	}
	now := store.now()
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// This unclaimed failure path is only for work that never obtained a
		// worker lease. A stale worker must not be able to fail the successor
		// that reclaimed an expired investigation.
		result := tx.Model(&models.OperatorInvestigation{}).Where("id=? AND tenant_id=? AND state IN ?", investigationID, tenantID, []string{"accepted", "backgrounded"}).Updates(map[string]any{"state": "failed", "error_class": errorClass, "finished_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: investigation is not active", ErrInvalidContract)
		}
		return appendInvestigationEvent(tx, investigationID, tenantID, "error", map[string]any{"class": errorClass})
	})
}

// FailClaim records a terminal worker error only while the caller still owns
// the live lease. This is the failure counterpart to PersistPacket and
// Complete: an expired or superseded worker has no authority to alter the
// investigation lifecycle.
func (store *InvestigationStore) FailClaim(ctx context.Context, investigationID uint, tenantID string, claimToken uuid.UUID, errorClass string) error {
	if strings.TrimSpace(errorClass) == "" {
		errorClass = "unknown"
	}
	now := store.now()
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var investigation models.OperatorInvestigation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", investigationID, tenantID).First(&investigation).Error; err != nil {
			return err
		}
		if investigation.State != "running" || investigation.ClaimToken == nil || *investigation.ClaimToken != claimToken || investigation.ClaimExpiresAt == nil || !now.Before(*investigation.ClaimExpiresAt) {
			return fmt.Errorf("%w: investigation claim is no longer current", ErrInvalidContract)
		}
		if err := tx.Model(&investigation).Updates(map[string]any{"state": "failed", "error_class": errorClass, "finished_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		return appendInvestigationEvent(tx, investigationID, tenantID, "error", map[string]any{"class": errorClass})
	})
}

// RecoverExpiredClaims makes an interrupted worker's work eligible for a
// fresh IAM-authorized retry. It never continues a claim itself and never
// marks a response completed; the next worker must call Begin and receive a
// new lease token.
func (store *InvestigationStore) RecoverExpiredClaims(ctx context.Context) (int, error) {
	now := store.now()
	var candidates []models.OperatorInvestigation
	if err := store.db.WithContext(ctx).Where("state=? AND claim_expires_at<?", "running", now).Order("updated_at ASC").Limit(50).Find(&candidates).Error; err != nil {
		return 0, err
	}
	recovered := 0
	for _, candidate := range candidates {
		err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var investigation models.OperatorInvestigation
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&investigation, candidate.ID).Error; err != nil {
				return err
			}
			if investigation.State != "running" || investigation.ClaimExpiresAt == nil || !investigation.ClaimExpiresAt.Before(now) {
				return nil
			}
			if err := tx.Model(&investigation).Updates(map[string]any{"state": "backgrounded", "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := appendInvestigationEvent(tx, investigation.ID, investigation.TenantID, "claim_recovered", map[string]any{"reason": "lease_expired"}); err != nil {
				return err
			}
			recovered++
			return nil
		})
		if err != nil {
			return recovered, err
		}
	}
	return recovered, nil
}

// LoadBackgrounded returns only persisted, resumable work. The caller must
// re-check IAM and runtime policy before Process; this store does neither.
func (store *InvestigationStore) LoadBackgrounded(ctx context.Context, limit int) ([]models.OperatorInvestigation, error) {
	if limit < 1 || limit > 20 {
		return nil, fmt.Errorf("%w: invalid investigation worker limit", ErrInvalidContract)
	}
	var investigations []models.OperatorInvestigation
	if err := store.db.WithContext(ctx).Where("state=? AND expires_at>?", "backgrounded", store.now()).Order("created_at ASC").Limit(limit).Find(&investigations).Error; err != nil {
		return nil, err
	}
	return investigations, nil
}

func DecodeStoredInvestigationInput(investigation models.OperatorInvestigation) (InvestigationInput, error) {
	var visible VisibleContext
	var request InvestigationRequest
	if err := json.Unmarshal(investigation.VisibleContext, &visible); err != nil {
		return InvestigationInput{}, fmt.Errorf("%w: decode persisted visible context", ErrInvalidContract)
	}
	if err := json.Unmarshal(investigation.Request, &request); err != nil {
		return InvestigationInput{}, fmt.Errorf("%w: decode persisted investigation request", ErrInvalidContract)
	}
	if err := visible.Validate(); err != nil {
		return InvestigationInput{}, err
	}
	if err := request.Validate(); err != nil || !visibleIntentAllowed(visible, request.Intent) {
		return InvestigationInput{}, fmt.Errorf("%w: persisted investigation request is invalid", ErrInvalidContract)
	}
	return InvestigationInput{VisibleContext: visible, Intent: request.Intent, Locale: investigation.Locale, Message: request.Message, Tier: request.Tier, ThreadID: investigation.ThreadID}, nil
}

// LoadEvents returns a bounded, ordered replay for the investigation creator.
// The sequence cursor is stable across browser reconnects and cannot cross a
// tenant or creator boundary.
func (store *InvestigationStore) LoadEvents(ctx context.Context, publicID string, tenantID, actorID string, afterSequence int64, limit int) (models.OperatorInvestigation, []models.OperatorInvestigationEvent, error) {
	if afterSequence < 0 || limit < 1 || limit > 200 || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(actorID) == "" {
		return models.OperatorInvestigation{}, nil, fmt.Errorf("%w: invalid investigation event cursor", ErrInvalidContract)
	}
	var investigation models.OperatorInvestigation
	if err := store.db.WithContext(ctx).Where("public_id=? AND tenant_id=? AND actor_id=?", publicID, tenantID, actorID).First(&investigation).Error; err != nil {
		return models.OperatorInvestigation{}, nil, err
	}
	var events []models.OperatorInvestigationEvent
	if err := store.db.WithContext(ctx).Where("investigation_id=? AND tenant_id=? AND sequence>?", investigation.ID, tenantID, afterSequence).Order("sequence ASC").Limit(limit).Find(&events).Error; err != nil {
		return models.OperatorInvestigation{}, nil, err
	}
	return investigation, events, nil
}

// ListInbox returns only the caller's still-retained investigation work. It is
// intentionally an index over durable lifecycle state, not a second source of
// facts, plans, or approvals.
func (store *InvestigationStore) ListInbox(ctx context.Context, tenantID string, actorID string, limit int) ([]models.OperatorInvestigation, int64, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(actorID) == "" || limit < 1 || limit > 100 {
		return nil, 0, fmt.Errorf("%w: invalid operator inbox query", ErrInvalidContract)
	}
	now := store.now()
	states := []string{"backgrounded", "running", "completed", "failed"}
	query := store.db.WithContext(ctx).Model(&models.OperatorInvestigation{}).Where("tenant_id=? AND actor_id=? AND expires_at>? AND state IN ?", tenantID, actorID, now, states)
	var unread int64
	if err := query.Session(&gorm.Session{}).Where("read_at IS NULL").Count(&unread).Error; err != nil {
		return nil, 0, err
	}
	var investigations []models.OperatorInvestigation
	if err := query.Order("updated_at DESC").Limit(limit).Find(&investigations).Error; err != nil {
		return nil, 0, err
	}
	return investigations, unread, nil
}

// MarkInboxRead records a creator's acknowledgement without weakening the
// immutable evidence/event trail. It is idempotent for reconnecting clients.
func (store *InvestigationStore) MarkInboxRead(ctx context.Context, publicID string, tenantID string, actorID string) (models.OperatorInvestigation, error) {
	if strings.TrimSpace(publicID) == "" || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(actorID) == "" {
		return models.OperatorInvestigation{}, fmt.Errorf("%w: invalid operator inbox identity", ErrInvalidContract)
	}
	now := store.now()
	var investigation models.OperatorInvestigation
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=? AND actor_id=?", publicID, tenantID, actorID).First(&investigation).Error; err != nil {
			return err
		}
		if investigation.ReadAt != nil {
			return nil
		}
		if err := tx.Model(&investigation).Update("read_at", now).Error; err != nil {
			return err
		}
		investigation.ReadAt = &now
		return appendInvestigationEvent(tx, investigation.ID, tenantID, "inbox_read", map[string]any{"read_at": now})
	})
	return investigation, err
}

// Cancel stops only unstarted/buffered investigation work. It cannot rewrite a
// completed answer and emits an immutable event so reconnecting clients see
// the cancellation reason instead of a disappeared task.
func (store *InvestigationStore) Cancel(ctx context.Context, publicID string, tenantID string, actorID string) (models.OperatorInvestigation, error) {
	if publicID == "" || tenantID == "" || actorID == "" {
		return models.OperatorInvestigation{}, fmt.Errorf("%w: investigation cancellation identity is required", ErrInvalidContract)
	}
	now := store.now()
	var investigation models.OperatorInvestigation
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=? AND actor_id=?", publicID, tenantID, actorID).First(&investigation).Error; err != nil {
			return err
		}
		if investigation.State != "accepted" && investigation.State != "backgrounded" {
			return fmt.Errorf("%w: investigation is already collecting or completed", ErrInvalidContract)
		}
		if err := tx.Model(&investigation).Updates(map[string]any{"state": "cancelled", "finished_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		investigation.State, investigation.FinishedAt, investigation.ClaimToken, investigation.ClaimExpiresAt = "cancelled", &now, nil, nil
		return appendInvestigationEvent(tx, investigation.ID, tenantID, "cancelled", map[string]any{"actor_id": actorID})
	})
	return investigation, err
}

func appendInvestigationEvent(tx *gorm.DB, investigationID uint, tenantID, eventType string, payload map[string]any) error {
	var sequence int64
	if err := tx.Model(&models.OperatorInvestigationEvent{}).Where("investigation_id=?", investigationID).Select("COALESCE(MAX(sequence), 0)").Scan(&sequence).Error; err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return tx.Create(&models.OperatorInvestigationEvent{InvestigationID: investigationID, TenantID: tenantID, Sequence: sequence + 1, EventType: eventType, Payload: datatypes.JSON(raw)}).Error
}
