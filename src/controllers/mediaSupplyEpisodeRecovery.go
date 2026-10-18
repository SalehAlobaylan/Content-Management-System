package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	mediaSupplyResolutionSchemaVersion = "media-supply-resolution/v1"
	mediaSupplyResolutionEpisodeLimit  = 50
)

// mediaSupplyEpisodeResolutionProof is the durable closure proof for one
// episode. It contains only CMS-owned identifiers and checkpoints; it never
// contains provider details, queue state, a repair command, or a model claim.
type mediaSupplyEpisodeResolutionProof struct {
	SchemaVersion       string                      `json:"schema_version"`
	EpisodeID           uuid.UUID                   `json:"episode_id"`
	EpisodeVerdict      string                      `json:"episode_verdict"`
	ResolvedAt          time.Time                   `json:"resolved_at"`
	ResolutionKind      string                      `json:"resolution_kind"`
	Summary             string                      `json:"summary"`
	EvaluationDigest    string                      `json:"evaluation_digest,omitempty"`
	ConsumerEvidenceIDs []string                    `json:"consumer_evidence_ids,omitempty"`
	ResolutionEvidence  []mediaSupplyResolutionFact `json:"resolution_evidence"`
}

type mediaSupplyResolutionFact struct {
	SourceID               string `json:"source_id,omitempty"`
	ScheduleState          string `json:"schedule_state,omitempty"`
	LastDeliveryVerifiedAt string `json:"last_delivery_verified_at,omitempty"`
	Active                 *bool  `json:"active,omitempty"`
}

// reconcileMediaSupplyEpisodes is exceptional recovery: it terminalizes an
// already-open attention record only after a fresh, boundary-specific CMS
// proof. It does not create or replay source work. A disabled evidence-record
// control intentionally does not block this safe terminalization path.
func reconcileMediaSupplyEpisodes(db *gorm.DB, tenantID string, status mediaSupplyStatusResponse) error {
	var episodes []models.MediaSupplyEpisode
	if err := db.Where("tenant_id = ? AND state IN ?", tenantID, []string{models.MediaSupplyEpisodeOpen, models.MediaSupplyEpisodeRecovering}).
		Order("first_seen_at ASC, public_id ASC").Limit(mediaSupplyResolutionEpisodeLimit).Find(&episodes).Error; err != nil {
		return err
	}
	for _, episode := range episodes {
		if err := db.Transaction(func(tx *gorm.DB) error {
			var current models.MediaSupplyEpisode
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state IN ?", tenantID, episode.PublicID, []string{models.MediaSupplyEpisodeOpen, models.MediaSupplyEpisodeRecovering}).First(&current).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil
				}
				return err
			}
			var resolution supply.EpisodeResolution
			var proof mediaSupplyEpisodeResolutionProof
			assessmentErr := tx.Transaction(func(readTx *gorm.DB) error {
				var err error
				resolution, proof, err = assessMediaSupplyEpisodeResolution(readTx, current, status)
				return err
			})
			if assessmentErr != nil {
				// Restore the transaction after an unreadable observation, then
				// persist unknown as a streak break rather than retaining old trust.
				resolution.State = supply.EpisodeResolutionUnknown
				proof.ResolvedAt = time.Now().UTC()
			}
			if resolution.State != supply.EpisodeResolutionResolved {
				payload, _ := json.Marshal(map[string]any{"state": resolution.State})
				digest := fmt.Sprintf("%x", sha256.Sum256([]byte(proof.ResolvedAt.Format(time.RFC3339Nano)+string(payload))))
				return appendMediaSupplyEpisodeEventPayload(tx, current, "resolution_not_clean", digest, payload, proof.ResolvedAt)
			}
			clean, err := recordMediaSupplyCleanResolutionObservation(tx, current, proof)
			if err != nil || !clean {
				return err
			}
			return resolveMediaSupplyEpisode(tx, current, proof)
		}); err != nil {
			return err
		}
	}
	return nil
}

func assessMediaSupplyEpisodeResolution(db *gorm.DB, episode models.MediaSupplyEpisode, status mediaSupplyStatusResponse) (supply.EpisodeResolution, mediaSupplyEpisodeResolutionProof, error) {
	verdict := supply.SupplyVerdict(episode.Verdict)
	sourceIDs, err := mediaSupplyEpisodeSourceIDs(episode)
	if err != nil {
		return supply.EpisodeResolution{}, mediaSupplyEpisodeResolutionProof{}, err
	}

	input := supply.EpisodeResolutionInput{Verdict: verdict}
	proof := mediaSupplyEpisodeResolutionProof{
		SchemaVersion: mediaSupplyResolutionSchemaVersion, EpisodeID: episode.PublicID,
		EpisodeVerdict: episode.Verdict, ResolvedAt: time.Now().UTC(), ResolutionEvidence: []mediaSupplyResolutionFact{},
	}
	switch verdict {
	case supply.SupplyVerdictSourceDueNotAdmitted:
		input.SourceAdmissionRecovered, proof.ResolutionEvidence = mediaSupplySourceAdmissionRecovery(db, episode.TenantID, sourceIDs)
	case supply.SupplyVerdictSourceRunWithoutIngest, supply.SupplyVerdictPodsDeliveryDegraded:
		input.SourceDeliveryRecovered, proof.ResolutionEvidence = mediaSupplySourceDeliveryRecovery(db, episode.TenantID, sourceIDs, episode.FirstSeenAt)
	case supply.SupplyVerdictNoActiveMediaSources:
		input.ActiveSourceConfigured, proof.ResolutionEvidence = mediaSupplyActiveSourceRecovery(db, episode.TenantID)
	case supply.SupplyVerdictEvidenceUnavailable:
		input.FreshEvidenceAvailable = mediaSupplyEvidenceBoundaryRecovery(status.SupplyEvaluation)
		digest, err := supply.EvaluationEvidenceDigest(status.SupplyEvaluation)
		if err != nil {
			return supply.EpisodeResolution{}, mediaSupplyEpisodeResolutionProof{}, err
		}
		proof.EvaluationDigest = digest
	case supply.SupplyVerdictNoEligibleInventory, supply.SupplyVerdictGenerationOmission,
		supply.SupplyVerdictServingOmission, supply.SupplyVerdictPodsInventoryStale:
		input.ConsumerBoundaryRecovered, proof.ConsumerEvidenceIDs = mediaSupplyConsumerEvidence(status.Exposure, episode.FirstSeenAt, proof.ResolvedAt)
	}

	resolution := supply.AssessEpisodeResolution(input)
	proof.ResolutionKind, proof.Summary = resolution.Kind, resolution.Summary
	if proof.EvaluationDigest == "" {
		digest, err := supply.EvaluationEvidenceDigest(status.SupplyEvaluation)
		if err != nil {
			return supply.EpisodeResolution{}, mediaSupplyEpisodeResolutionProof{}, err
		}
		proof.EvaluationDigest = digest
	}
	return resolution, proof, nil
}

func mediaSupplyConsumerBoundaryRecovery(exposure supply.PodsExposureProof) supply.ResolutionObservation {
	verdict, _ := mediaSupplyConsumerEvidence(exposure, time.Time{}, time.Now().UTC())
	return verdict
}

func mediaSupplyConsumerEvidence(exposure supply.PodsExposureProof, since, now time.Time) (supply.ResolutionObservation, []string) {
	if exposure.EvidenceCompleteness == "unavailable" || exposure.Verdict == "unknown" {
		return supply.ResolutionObservationUnknown, nil
	}
	if exposure.Verdict != "return_path_observed" {
		return supply.ResolutionObservationAbsent, nil
	}
	returned := map[uuid.UUID]bool{}
	for _, id := range exposure.ReturnedIDs {
		if parsed, err := uuid.Parse(id); err == nil {
			returned[parsed] = true
		}
	}
	for _, view := range exposure.ConsumerObservations {
		if view.Boundary != "exact_view" || !freshConsumerObservation(view, since, now) || !returned[view.ContentItemID] {
			continue
		}
		for _, render := range exposure.ConsumerObservations {
			if render.Boundary == "page_render" && freshConsumerObservation(render, since, now) && render.ContentItemID == view.ContentItemID && render.FeedSessionID != nil && view.FeedSessionID != nil && *render.FeedSessionID == *view.FeedSessionID && render.GenerationID != nil && view.GenerationID != nil && *render.GenerationID == *view.GenerationID && exposure.ActiveGenerationID != nil && *view.GenerationID == *exposure.ActiveGenerationID {
				return supply.ResolutionObservationPresent, []string{render.PublicID.String(), view.PublicID.String()}
			}
		}
	}
	return supply.ResolutionObservationAbsent, nil
}

func freshConsumerObservation(row models.PodsBoundaryObservation, since, now time.Time) bool {
	return row.PublicID != uuid.Nil && row.Verdict == string(supply.VerdictPresent) && row.ObservedAt.After(since) && !row.ObservedAt.Before(now.Add(-15*time.Minute)) && !row.ObservedAt.After(now)
}

// The ledger records every failed/unknown check too. Evidence identity is stable
// across reevaluations, so rereading the same consumer event cannot earn trust.
func recordMediaSupplyCleanResolutionObservation(db *gorm.DB, episode models.MediaSupplyEpisode, proof mediaSupplyEpisodeResolutionProof) (bool, error) {
	identity, _ := json.Marshal(proof.ConsumerEvidenceIDs)
	if len(proof.ConsumerEvidenceIDs) == 0 {
		identity = []byte(proof.EvaluationDigest + proof.ResolvedAt.UTC().Truncate(time.Minute).Format(time.RFC3339))
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(identity))
	payload, err := json.Marshal(proof)
	if err != nil {
		return false, err
	}
	var latest []models.MediaSupplyEpisodeEvent
	if err := db.Where("tenant_id=? AND episode_id=? AND event_type IN ? AND occurred_at>=?", episode.TenantID, episode.PublicID, []string{"resolution_clean", "resolution_not_clean"}, proof.ResolvedAt.Add(-15*time.Minute)).Order("occurred_at DESC, id DESC").Limit(2).Find(&latest).Error; err != nil {
		return false, err
	}
	if len(latest) > 0 && latest[0].EventType == "resolution_clean" && latest[0].EvidenceDigest == digest {
		return false, nil
	}
	if err := appendMediaSupplyEpisodeEventPayload(db, episode, "resolution_clean", digest, payload, proof.ResolvedAt); err != nil {
		return false, err
	}
	return len(latest) > 0 && latest[0].EventType == "resolution_clean" && latest[0].EvidenceDigest != digest && !latest[0].OccurredAt.After(proof.ResolvedAt.Add(-time.Minute)) && !latest[0].OccurredAt.Before(proof.ResolvedAt.Add(-2*mediaSupplyEvaluationInterval)), nil
}

func mediaSupplyEpisodeSourceIDs(episode models.MediaSupplyEpisode) ([]uuid.UUID, error) {
	var subjects []struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal(episode.AffectedSubjects, &subjects); err != nil {
		return nil, err
	}
	seen := map[uuid.UUID]struct{}{}
	result := make([]uuid.UUID, 0, len(subjects))
	for _, subject := range subjects {
		if subject.Type != "content_source" {
			continue
		}
		id, err := uuid.Parse(subject.ID)
		if err != nil {
			return nil, fmt.Errorf("invalid media supply episode source subject: %w", err)
		}
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result, nil
}

func mediaSupplySourceAdmissionRecovery(db *gorm.DB, tenantID string, sourceIDs []uuid.UUID) (supply.ResolutionObservation, []mediaSupplyResolutionFact) {
	if len(sourceIDs) == 0 {
		return supply.ResolutionObservationUnknown, []mediaSupplyResolutionFact{}
	}
	var sources []models.ContentSource
	if err := db.Where("tenant_id = ? AND public_id IN ? AND category = ?", tenantID, sourceIDs, models.SourceCategoryMedia).Find(&sources).Error; err != nil || len(sources) != len(sourceIDs) {
		return supply.ResolutionObservationUnknown, []mediaSupplyResolutionFact{}
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].PublicID.String() < sources[j].PublicID.String() })
	facts := make([]mediaSupplyResolutionFact, 0, len(sources))
	for _, source := range sources {
		var latest models.SourceRunRequest
		err := db.Where("tenant_id = ? AND content_source_id = ? AND lane = ?", tenantID, source.PublicID, models.SourceCategoryMedia).
			Order("requested_at DESC, public_id DESC").First(&latest).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return supply.ResolutionObservationUnknown, facts
		}
		state, _ := mediaSourceScheduleState(source, err == nil, latest, time.Now().UTC())
		facts = append(facts, mediaSupplyResolutionFact{SourceID: source.PublicID.String(), ScheduleState: state})
		if state != "scheduled" && state != "in_flight" {
			return supply.ResolutionObservationAbsent, facts
		}
	}
	return supply.ResolutionObservationPresent, facts
}

func mediaSupplySourceDeliveryRecovery(db *gorm.DB, tenantID string, sourceIDs []uuid.UUID, episodeOpenedAt time.Time) (supply.ResolutionObservation, []mediaSupplyResolutionFact) {
	if len(sourceIDs) == 0 || episodeOpenedAt.IsZero() {
		return supply.ResolutionObservationUnknown, []mediaSupplyResolutionFact{}
	}
	var sources []models.ContentSource
	if err := db.Where("tenant_id = ? AND public_id IN ? AND category = ?", tenantID, sourceIDs, models.SourceCategoryMedia).Find(&sources).Error; err != nil || len(sources) != len(sourceIDs) {
		return supply.ResolutionObservationUnknown, []mediaSupplyResolutionFact{}
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].PublicID.String() < sources[j].PublicID.String() })
	facts := make([]mediaSupplyResolutionFact, 0, len(sources))
	for _, source := range sources {
		fact := mediaSupplyResolutionFact{SourceID: source.PublicID.String()}
		if source.LastDeliveryVerifiedAt != nil {
			fact.LastDeliveryVerifiedAt = source.LastDeliveryVerifiedAt.UTC().Format(time.RFC3339Nano)
		}
		facts = append(facts, fact)
		if source.LastDeliveryVerifiedAt == nil || !source.LastDeliveryVerifiedAt.After(episodeOpenedAt) {
			return supply.ResolutionObservationAbsent, facts
		}
	}
	return supply.ResolutionObservationPresent, facts
}

func mediaSupplyActiveSourceRecovery(db *gorm.DB, tenantID string) (supply.ResolutionObservation, []mediaSupplyResolutionFact) {
	var source models.ContentSource
	err := db.Where("tenant_id = ? AND category = ? AND is_active = ? AND next_due_at IS NOT NULL", tenantID, models.SourceCategoryMedia, true).
		Order("next_due_at ASC, public_id ASC").First(&source).Error
	if err == gorm.ErrRecordNotFound {
		return supply.ResolutionObservationAbsent, []mediaSupplyResolutionFact{}
	}
	if err != nil {
		return supply.ResolutionObservationUnknown, []mediaSupplyResolutionFact{}
	}
	active := true
	return supply.ResolutionObservationPresent, []mediaSupplyResolutionFact{{SourceID: source.PublicID.String(), Active: &active}}
}

func mediaSupplyEvidenceBoundaryRecovery(evaluation supply.SupplyEvaluation) supply.ResolutionObservation {
	if evaluation.EvidenceCompleteness == "complete" && evaluation.Verdict != supply.SupplyVerdictEvidenceUnavailable {
		return supply.ResolutionObservationPresent
	}
	if evaluation.EvidenceCompleteness == "unavailable" || evaluation.Verdict == supply.SupplyVerdictEvidenceUnavailable {
		return supply.ResolutionObservationUnknown
	}
	return supply.ResolutionObservationUnknown
}

func resolveMediaSupplyEpisode(db *gorm.DB, episode models.MediaSupplyEpisode, proof mediaSupplyEpisodeResolutionProof) error {
	proofBytes, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	digestHash := sha256.Sum256(proofBytes)
	digest := hex.EncodeToString(digestHash[:])
	return db.Transaction(func(tx *gorm.DB) error {
		var current models.MediaSupplyEpisode
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ? AND public_id = ? AND state IN ?", episode.TenantID, episode.PublicID, []string{models.MediaSupplyEpisodeOpen, models.MediaSupplyEpisodeRecovering}).
			First(&current).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}
		if err := tx.Model(&current).Updates(map[string]interface{}{
			"state":            models.MediaSupplyEpisodeResolved,
			"resolved_at":      proof.ResolvedAt,
			"resolution_proof": datatypes.JSON(proofBytes),
			"updated_at":       proof.ResolvedAt,
		}).Error; err != nil {
			return err
		}
		return appendMediaSupplyEpisodeEventPayload(tx, current, "resolved", digest, proofBytes, proof.ResolvedAt)
	})
}
