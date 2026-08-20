// Package contentstage owns durable normal-operation stage intent, claims,
// fencing, verification, and lifecycle reduction. It is deliberately separate
// from approval-required artifact and pipeline recovery.
package contentstage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ProtocolVersion    = "content-stage/v1"
	leaseDuration      = 45 * time.Second
	verificationWindow = 10 * time.Minute
	maxEffectAttempts  = 2
	textElapsedBudget  = 30 * time.Minute
	mediaElapsedBudget = 60 * time.Minute
)

type Descriptor struct {
	Lane          string
	Stage         string
	Owner         string
	BlockingScope string
	Dependencies  []string
	ModelRecipe   string
}

var descriptors = map[string]Descriptor{
	models.ContentStageNewsTextEmbedding:       {models.ContentStageLaneNews, models.ContentStageNewsTextEmbedding, models.ContentStageOwnerAggregationNews, models.ContentStageBlockingContentReady, nil, "qwen-content-text/v1"},
	models.ContentStageNewsStoryClassification: {models.ContentStageLaneNews, models.ContentStageNewsStoryClassification, models.ContentStageOwnerCMS, models.ContentStageBlockingContentReady, []string{models.ContentStageNewsTextEmbedding}, "story-classification/v1"},
	models.ContentStageNewsLLMMetadata:         {models.ContentStageLaneNews, models.ContentStageNewsLLMMetadata, models.ContentStageOwnerAggregationNews, models.ContentStageBlockingOptional, []string{models.ContentStageNewsTextEmbedding}, "news-metadata/v1"},
	models.ContentStagePodsMediaArtifacts:      {models.ContentStageLanePods, models.ContentStagePodsMediaArtifacts, models.ContentStageOwnerAggregationPods, models.ContentStageBlockingContentReady, nil, "pods-rendition/v1"},
	models.ContentStagePodsTextEmbedding:       {models.ContentStageLanePods, models.ContentStagePodsTextEmbedding, models.ContentStageOwnerAggregationPods, models.ContentStageBlockingContentReady, nil, "qwen-content-text/v1"},
	models.ContentStagePodsTranscript:          {models.ContentStageLanePods, models.ContentStagePodsTranscript, models.ContentStageOwnerMedia, models.ContentStageBlockingFeedDelivery, []string{models.ContentStagePodsMediaArtifacts}, "pods-transcript/v1"},
	models.ContentStagePodsAtomization:         {models.ContentStageLanePods, models.ContentStagePodsAtomization, models.ContentStageOwnerAggregationPods, models.ContentStageBlockingFeedDelivery, []string{models.ContentStagePodsTranscript}, "pods-atomization/v1"},
	models.ContentStagePodsCaptionReembedding:  {models.ContentStageLanePods, models.ContentStagePodsCaptionReembedding, models.ContentStageOwnerAggregationPods, models.ContentStageBlockingOptional, []string{models.ContentStagePodsTranscript}, "qwen-caption-text/v1"},
	models.ContentStagePodsImageEmbedding:      {models.ContentStageLanePods, models.ContentStagePodsImageEmbedding, models.ContentStageOwnerMedia, models.ContentStageBlockingOptional, []string{models.ContentStagePodsMediaArtifacts}, "clip-image/v1"},
	models.ContentStagePodsLLMMetadata:         {models.ContentStageLanePods, models.ContentStagePodsLLMMetadata, models.ContentStageOwnerAggregationPods, models.ContentStageBlockingOptional, []string{models.ContentStagePodsTextEmbedding}, "pods-metadata/v1"},
}

func DescriptorFor(stage string) (Descriptor, bool) {
	d, ok := descriptors[strings.TrimSpace(stage)]
	return d, ok
}

func StagesForContentType(kind models.ContentType) []Descriptor {
	var keys []string
	switch kind {
	case models.ContentTypeNews:
		keys = []string{models.ContentStageNewsTextEmbedding, models.ContentStageNewsStoryClassification, models.ContentStageNewsLLMMetadata}
	case models.ContentTypeVideo, models.ContentTypePodcast:
		keys = []string{
			models.ContentStagePodsMediaArtifacts, models.ContentStagePodsTextEmbedding,
			models.ContentStagePodsTranscript, models.ContentStagePodsAtomization,
			models.ContentStagePodsCaptionReembedding, models.ContentStagePodsImageEmbedding,
			models.ContentStagePodsLLMMetadata,
		}
	}
	out := make([]Descriptor, 0, len(keys))
	for _, key := range keys {
		out = append(out, descriptors[key])
	}
	return out
}

func digest(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

func normalized(value *string) string {
	if value == nil {
		return ""
	}
	return strings.Join(strings.Fields(strings.TrimSpace(*value)), " ")
}

func stageFingerprint(item models.ContentItem, d Descriptor) string {
	// Processing generation belongs to request identity, not effect identity.
	// Keeping it out of this fingerprint lets a changed text input preserve a
	// still-valid media artifact (and vice versa) in the replacement manifest.
	common := []string{ProtocolVersion, item.TenantID, item.PublicID.String(), d.Stage, d.ModelRecipe}
	switch d.Stage {
	case models.ContentStageNewsTextEmbedding, models.ContentStagePodsTextEmbedding, models.ContentStageNewsLLMMetadata, models.ContentStagePodsLLMMetadata:
		common = append(common, normalized(item.Title), normalized(item.Excerpt), normalized(item.BodyText), normalized(item.ContentLanguage))
	case models.ContentStagePodsMediaArtifacts:
		common = append(common, normalized(item.OriginalURL), normalized(item.SourceFeedURL), string(item.Source))
	case models.ContentStagePodsTranscript:
		// The transcript command depends on the verified media-artifact stage,
		// but its identity remains stable while that predecessor fills playback
		// metadata inside the same processing generation.
		common = append(common, normalized(item.OriginalURL), string(item.Source))
	case models.ContentStagePodsAtomization:
		common = append(common, normalized(item.OriginalURL), "atomization-policy/v1")
	case models.ContentStageNewsStoryClassification:
		common = append(common, normalized(item.Title), normalized(item.Excerpt), normalized(item.BodyText), normalized(item.ContentLanguage))
	case models.ContentStagePodsCaptionReembedding:
		common = append(common, normalized(item.OriginalURL), normalized(item.SourceFeedURL), string(item.Source))
	case models.ContentStagePodsImageEmbedding:
		common = append(common, normalized(item.OriginalURL), normalized(item.SourceFeedURL), string(item.Source))
	}
	return digest(common...)
}

func ItemInputDigest(item models.ContentItem) string {
	return digest(
		"content-input/v1", item.TenantID, string(item.Type), normalized(item.Title),
		normalized(item.Excerpt), normalized(item.BodyText), normalized(item.ContentLanguage),
		normalized(item.OriginalURL), normalized(item.SourceFeedURL),
	)
}

func jsonValue(value any) datatypes.JSON {
	raw, _ := json.Marshal(value)
	return datatypes.JSON(raw)
}

// EnsureManifest persists normal-stage intent in the caller's content-item
// transaction. It is idempotent for one processing generation.
func EnsureManifest(tx *gorm.DB, item *models.ContentItem) ([]models.ContentStageRequest, error) {
	if tx == nil || item == nil || item.PublicID == uuid.Nil || strings.TrimSpace(item.TenantID) == "" {
		return nil, fmt.Errorf("content stage manifest requires a persisted tenant-scoped item")
	}
	if item.ProcessingGeneration <= 0 {
		item.ProcessingGeneration = 1
	}
	inputDigest := ItemInputDigest(*item)
	item.ProcessingInputDigest = &inputDigest
	if err := tx.Model(item).Updates(map[string]any{
		"processing_generation":   item.ProcessingGeneration,
		"processing_input_digest": inputDigest,
	}).Error; err != nil {
		return nil, err
	}

	descriptors := StagesForContentType(item.Type)
	requests := make([]models.ContentStageRequest, 0, len(descriptors))
	for _, d := range descriptors {
		fingerprint := stageFingerprint(*item, d)
		idem := digest("content-stage-request/v1", item.TenantID, item.PublicID.String(), fmt.Sprint(item.ProcessingGeneration), d.Stage, fingerprint)
		deadline := item.CreatedAt.UTC().Add(textElapsedBudget)
		if d.Stage == models.ContentStagePodsMediaArtifacts {
			deadline = item.CreatedAt.UTC().Add(mediaElapsedBudget)
		}
		request := models.ContentStageRequest{
			PublicID: uuid.New(), TenantID: item.TenantID, ContentItemID: item.PublicID,
			ProcessingGeneration: item.ProcessingGeneration, Lane: d.Lane, Stage: d.Stage,
			Owner: d.Owner, BlockingScope: d.BlockingScope, State: models.ContentStageQueued,
			InputFingerprint: fingerprint, PolicyVersion: "v1", ModelRecipe: d.ModelRecipe,
			IdempotencyKey: idem, DependencyManifest: jsonValue(d.Dependencies),
			WorkloadEstimate: workloadEstimate(*item, d), DeadlineAt: &deadline,
			TerminalProof: jsonValue(map[string]any{}),
		}
		result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{
			{Name: "tenant_id"}, {Name: "content_item_id"}, {Name: "processing_generation"}, {Name: "stage"},
		}, DoNothing: true}).Create(&request)
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected == 0 {
			if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage=?", item.TenantID, item.PublicID, item.ProcessingGeneration, d.Stage).First(&request).Error; err != nil {
				return nil, err
			}
		} else if err := appendEvent(tx, request, nil, "queued", map[string]any{"lane": d.Lane, "stage": d.Stage, "input_fingerprint": fingerprint}); err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	if err := reduceReadiness(tx, item.TenantID, item.PublicID, item.ProcessingGeneration); err != nil {
		return nil, err
	}
	return requests, nil
}

// ReconcileManifest advances only when stage-relevant normalized inputs
// changed. It supersedes old active intent and creates the replacement
// generation in the caller's item-update transaction.
func ReconcileManifest(tx *gorm.DB, item *models.ContentItem, previousDigest string) ([]models.ContentStageRequest, bool, error) {
	if item == nil {
		return nil, false, fmt.Errorf("content item is required")
	}
	nextDigest := ItemInputDigest(*item)
	changed := strings.TrimSpace(previousDigest) != "" && previousDigest != nextDigest
	var previousByStage map[string]models.ContentStageRequest
	invalidated := map[string]bool{}
	var previousGeneration int64
	if changed {
		previousGeneration = item.ProcessingGeneration
		if previousGeneration <= 0 {
			previousGeneration = 1
		}
		var previous []models.ContentStageRequest
		if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=?", item.TenantID, item.PublicID, previousGeneration).Find(&previous).Error; err != nil {
			return nil, false, err
		}
		previousByStage = make(map[string]models.ContentStageRequest, len(previous))
		for _, request := range previous {
			previousByStage[request.Stage] = request
			descriptor, ok := DescriptorFor(request.Stage)
			if !ok || stageFingerprint(*item, descriptor) != request.InputFingerprint {
				invalidated[request.Stage] = true
			}
		}
		for advanced := true; advanced; {
			advanced = false
			for _, descriptor := range StagesForContentType(item.Type) {
				if invalidated[descriptor.Stage] {
					continue
				}
				for _, dependency := range descriptor.Dependencies {
					if invalidated[dependency] {
						invalidated[descriptor.Stage], advanced = true, true
						break
					}
				}
			}
		}
		for _, request := range previous {
			if request.State == models.ContentStageVerified || request.State == models.ContentStageFailed || request.State == models.ContentStageCancelled || request.State == models.ContentStageSuperseded {
				continue
			}
			if err := supersede(tx, request, "input_generation_replaced"); err != nil {
				return nil, false, err
			}
		}
		item.ProcessingGeneration = previousGeneration + 1
	}
	item.ProcessingInputDigest = &nextDigest
	requests, err := EnsureManifest(tx, item)
	if err != nil || !changed {
		return requests, changed, err
	}
	// Carry only proven, input-identical effects into the new generation. Any
	// directly changed stage and every transitive dependant remains queued.
	now := time.Now().UTC()
	for index := range requests {
		request := &requests[index]
		previous, ok := previousByStage[request.Stage]
		if !ok || invalidated[request.Stage] || previous.State != models.ContentStageVerified {
			continue
		}
		if err := tx.Model(request).Updates(map[string]any{"state": models.ContentStageVerified, "verified_at": now, "finished_at": now, "terminal_proof": previous.TerminalProof, "updated_at": now}).Error; err != nil {
			return nil, false, err
		}
		request.State, request.VerifiedAt, request.FinishedAt, request.TerminalProof = models.ContentStageVerified, &now, &now, previous.TerminalProof
		if err := appendEvent(tx, *request, nil, "verified_from_predecessor", map[string]any{"previous_request_id": previous.PublicID, "previous_generation": previousGeneration}); err != nil {
			return nil, false, err
		}
	}
	if err := reduceReadiness(tx, item.TenantID, item.PublicID, item.ProcessingGeneration); err != nil {
		return nil, changed, err
	}
	return requests, changed, nil
}

type ManifestDisposition struct {
	Disposition          string   `json:"disposition"`
	ProcessingGeneration int64    `json:"processing_generation"`
	RequiredStages       []string `json:"required_stages"`
	ActiveStages         []string `json:"active_stages"`
}

func SummarizeManifest(requests []models.ContentStageRequest, generation int64, disposition string) ManifestDisposition {
	required, active := make([]string, 0), make([]string, 0)
	for _, request := range requests {
		if request.BlockingScope != models.ContentStageBlockingOptional {
			required = append(required, request.Stage)
		}
		if request.State != models.ContentStageVerified && request.State != models.ContentStageCancelled && request.State != models.ContentStageSuperseded {
			active = append(active, request.Stage)
		}
	}
	return ManifestDisposition{Disposition: disposition, ProcessingGeneration: generation, RequiredStages: required, ActiveStages: active}
}

func workloadEstimate(item models.ContentItem, d Descriptor) datatypes.JSON {
	value := map[string]any{"units": 1}
	if d.Stage == models.ContentStagePodsMediaArtifacts || d.Stage == models.ContentStagePodsTranscript || d.Stage == models.ContentStagePodsAtomization {
		value["duration_sec"] = item.DurationSec
	}
	return jsonValue(value)
}

func appendEvent(tx *gorm.DB, request models.ContentStageRequest, attempt *models.ContentStageAttempt, eventType string, payload any) error {
	var sequence int64
	if err := tx.Model(&models.ContentStageEvent{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Select("COALESCE(MAX(sequence),0)").Scan(&sequence).Error; err != nil {
		return err
	}
	sequence++
	var attemptID *uuid.UUID
	if attempt != nil {
		id := attempt.PublicID
		attemptID = &id
	}
	return tx.Create(&models.ContentStageEvent{
		PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID,
		AttemptID: attemptID, Sequence: sequence, EventType: eventType,
		Payload: jsonValue(payload), OccurredAt: time.Now().UTC(),
	}).Error
}

func CutoverMode(db *gorm.DB, tenantID, lane string) (string, error) {
	var cutover models.ContentStageCutover
	err := db.Where("tenant_id=? AND lane=?", tenantID, lane).First(&cutover).Error
	if err == gorm.ErrRecordNotFound {
		return models.ContentStageCutoverLegacy, nil
	}
	if err != nil {
		return "", err
	}
	return cutover.Mode, nil
}

// DeliveryMode is returned by materialization so callers never infer queue
// ownership from process configuration or Redis state.
func DeliveryMode(db *gorm.DB, tenantID string, kind models.ContentType) (string, error) {
	lane := ""
	switch kind {
	case models.ContentTypeNews:
		lane = models.ContentStageLaneNews
	case models.ContentTypeVideo, models.ContentTypePodcast:
		lane = models.ContentStageLanePods
	default:
		return models.ContentStageCutoverLegacy, nil
	}
	return CutoverMode(db, tenantID, lane)
}

func executionAllowed(tx *gorm.DB, tenantID, lane, stage string) (bool, error) {
	mode, err := CutoverMode(tx, tenantID, lane)
	if err != nil || mode != models.ContentStageCutoverDurableRequired {
		return false, err
	}
	var control models.ContentStageControl
	err = tx.Where("tenant_id=? AND lane=?", tenantID, lane).First(&control).Error
	if err == gorm.ErrRecordNotFound {
		return true, nil
	}
	if err != nil || !control.ExecutionEnabled {
		return false, err
	}
	if strings.Contains(stage, "llm_metadata") && !control.OptionalMetadataEnabled {
		return false, nil
	}
	if stage == models.ContentStagePodsTranscript && !control.TranscriptExecutionEnabled {
		return false, nil
	}
	return true, nil
}

func schedulingAllowed(tx *gorm.DB, tenantID, lane string) (bool, error) {
	var control models.ContentStageControl
	err := tx.Where("tenant_id=? AND lane=?", tenantID, lane).First(&control).Error
	if err == gorm.ErrRecordNotFound {
		return true, nil
	}
	return err == nil && control.SchedulingEnabled, err
}

func dependenciesVerified(tx *gorm.DB, request models.ContentStageRequest) (bool, error) {
	var dependencies []string
	if len(request.DependencyManifest) == 0 || json.Unmarshal(request.DependencyManifest, &dependencies) != nil || len(dependencies) == 0 {
		return true, nil
	}
	var count int64
	err := tx.Model(&models.ContentStageRequest{}).Where(
		"tenant_id=? AND content_item_id=? AND processing_generation=? AND stage IN ? AND state=?",
		request.TenantID, request.ContentItemID, request.ProcessingGeneration, dependencies, models.ContentStageVerified,
	).Count(&count).Error
	return count == int64(len(dependencies)), err
}

type ClaimEnvelope struct {
	SchemaVersion        string                     `json:"schema_version"`
	RequestID            uuid.UUID                  `json:"request_id"`
	AttemptID            uuid.UUID                  `json:"attempt_id"`
	TenantID             string                     `json:"tenant_id"`
	ContentItemID        uuid.UUID                  `json:"content_item_id"`
	ProcessingGeneration int64                      `json:"processing_generation"`
	Lane                 string                     `json:"lane"`
	Stage                string                     `json:"stage"`
	InputFingerprint     string                     `json:"input_fingerprint"`
	ClaimToken           uuid.UUID                  `json:"claim_token"`
	FenceToken           uuid.UUID                  `json:"fence_token"`
	LeaseEpoch           int64                      `json:"lease_epoch"`
	LeaseExpiresAt       time.Time                  `json:"lease_expires_at"`
	DeterministicJobID   string                     `json:"deterministic_job_id"`
	BoundedInput         map[string]any             `json:"bounded_input"`
	Request              models.ContentStageRequest `json:"-"`
	Attempt              models.ContentStageAttempt `json:"-"`
}

func boundedInput(item models.ContentItem, stage string) map[string]any {
	input := map[string]any{
		"title": item.Title, "excerpt": item.Excerpt, "body_text": item.BodyText,
		"content_language": item.ContentLanguage, "content_type": item.Type,
	}
	switch stage {
	case models.ContentStagePodsMediaArtifacts:
		input["original_url"] = item.OriginalURL
		input["source"] = item.Source
		input["source_name"] = item.SourceName
	case models.ContentStagePodsTranscript, models.ContentStagePodsAtomization:
		input["playback_url"] = item.PlaybackURL
		input["media_url"] = item.MediaURL
		input["duration_sec"] = item.DurationSec
		input["transcript_id"] = item.TranscriptID
		if stage == models.ContentStagePodsTranscript && len(item.Metadata) > 0 {
			var metadata map[string]any
			if json.Unmarshal(item.Metadata, &metadata) == nil {
				if captionArtifact, ok := metadata["caption_artifact"].(map[string]any); ok {
					input["caption_artifact"] = captionArtifact
				}
			}
		}
	case models.ContentStagePodsImageEmbedding:
		input["thumbnail_url"] = item.ThumbnailURL
	}
	return input
}
