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
	SchemaVersion      string                      `json:"schema_version"`
	EpisodeID          uuid.UUID                   `json:"episode_id"`
	EpisodeVerdict     string                      `json:"episode_verdict"`
	ResolvedAt         time.Time                   `json:"resolved_at"`
	ResolutionKind     string                      `json:"resolution_kind"`
	Summary            string                      `json:"summary"`
	EvaluationDigest   string                      `json:"evaluation_digest,omitempty"`
	ResolutionEvidence []mediaSupplyResolutionFact `json:"resolution_evidence"`
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
		resolution, proof, err := assessMediaSupplyEpisodeResolution(db, episode, status)
		if err != nil || resolution.State != supply.EpisodeResolutionResolved {
			continue
		}
		clean, err := recordMediaSupplyCleanResolutionObservation(db, episode, proof)
		if err != nil {
			return err
		}
		if !clean {
			continue
		}
		if err := resolveMediaSupplyEpisode(db, episode, proof); err != nil {
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
		input.ConsumerBoundaryRecovered = mediaSupplyConsumerBoundaryRecovery(status.Exposure)
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
	if exposure.EvidenceCompleteness == "unavailable" || exposure.Verdict == "unknown" {
		return supply.ResolutionObservationUnknown
	}
	if exposure.Verdict != "return_path_observed" || len(exposure.ReturnedIDs) == 0 || len(exposure.RenderedIDs) == 0 || len(exposure.ViewedIDs) == 0 {
		return supply.ResolutionObservationAbsent
	}
	return supply.ResolutionObservationPresent
}

// Two fresh independent observations are required before closure. The first
// moves no state and is durable evidence; a later clean evaluation supplies
// the second observation. Owner completion alone can therefore never close a
// consumer-boundary episode.
func recordMediaSupplyCleanResolutionObservation(db *gorm.DB, episode models.MediaSupplyEpisode, proof mediaSupplyEpisodeResolutionProof) (bool, error) {
	bytes, err := json.Marshal(proof)
	if err != nil {
		return false, err
	}
	hash := sha256.Sum256(bytes)
	digest := hex.EncodeToString(hash[:])
	if err := appendMediaSupplyEpisodeEventPayload(db, episode, "resolution_clean", digest, bytes, proof.ResolvedAt); err != nil {
		return false, err
	}
	var count int64
	err = db.Model(&models.MediaSupplyEpisodeEvent{}).
		Where("tenant_id=? AND episode_id=? AND event_type=? AND occurred_at>=?", episode.TenantID, episode.PublicID, "resolution_clean", proof.ResolvedAt.Add(-15*time.Minute)).
		Count(&count).Error
	return count >= 2, err
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
