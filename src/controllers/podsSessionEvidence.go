package controllers

import (
	"encoding/json"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// A client session reference is a lookup key, never generation authority. Only
// the CMS snapshot can prove which item and generation were actually served.
func podsSessionBoundary(db *gorm.DB, item models.ContentItem, sessionID uuid.UUID, identityScope, boundary, probeID string, at time.Time) (models.PodsBoundaryObservation, bool) {
	if sessionID == uuid.Nil || item.TenantID == "" || at.IsZero() || time.Since(at) > 15*time.Minute || at.After(time.Now().UTC().Add(time.Minute)) {
		return models.PodsBoundaryObservation{}, false
	}
	var session models.ConsumerFeedSession
	query := db.Where("id=? AND tenant_id=? AND feed_type=?", sessionID, item.TenantID, "pods")
	if identityScope != "" {
		query = query.Where("identity_scope=?", identityScope)
	}
	if query.First(&session).Error != nil || session.ActiveGenerationID == nil || at.Before(session.CreatedAt) || !at.Before(session.ExpiresAt) {
		return models.PodsBoundaryObservation{}, false
	}
	var snapshot []struct {
		ID uuid.UUID `json:"id"`
	}
	if json.Unmarshal(session.Snapshot, &snapshot) != nil || len(snapshot) > consumerFeedSnapshotLimit {
		return models.PodsBoundaryObservation{}, false
	}
	for _, member := range snapshot {
		if member.ID == item.PublicID {
			return models.PodsBoundaryObservation{TenantID: item.TenantID, ContentItemID: item.PublicID, GenerationID: session.ActiveGenerationID, FeedSessionID: &session.ID, Boundary: boundary, ProbeKind: "frozen_session", ProbeID: probeID, SourceRunRequestID: item.SourceRunRequestID, Verdict: string(supply.VerdictPresent), ObservedAt: at}, true
		}
	}
	return models.PodsBoundaryObservation{}, false
}

func recordPodsViewBoundary(tx *gorm.DB, item models.ContentItem, interaction models.UserInteraction) {
	if interaction.Type != models.InteractionTypeView {
		return
	}
	var metadata struct {
		FeedSessionID uuid.UUID `json:"feed_session_id"`
	}
	if json.Unmarshal(interaction.Metadata, &metadata) != nil || metadata.FeedSessionID == uuid.Nil {
		return
	}
	// A broken evidence store must not abort the authoritative interaction.
	_ = tx.Transaction(func(evidenceTx *gorm.DB) error {
		observation, valid := podsSessionBoundary(evidenceTx, item, metadata.FeedSessionID, interactionIdentityScope(interaction), "exact_view", interaction.PublicID.String(), interaction.CreatedAt.UTC())
		if !valid {
			return nil
		}
		return supply.RecordPodsBoundaryObservation(evidenceTx, observation)
	})
}
