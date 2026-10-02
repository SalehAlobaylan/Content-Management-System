package controllers

import (
	"content-management-system/src/artifacts"
	"content-management-system/src/contentstage"
	"content-management-system/src/feedcontract"
	"content-management-system/src/feedstate"
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/pipeline"
	"content-management-system/src/sourceidentity"
	"content-management-system/src/spaceid"
	"content-management-system/src/supply"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type internalCreateContentItemRequest struct {
	IdempotencyKey            string                        `json:"idempotency_key"`
	Type                      string                        `json:"type"`
	Format                    *string                       `json:"format"`
	Source                    string                        `json:"source"`
	Status                    string                        `json:"status"`
	Title                     string                        `json:"title"`
	BodyText                  *string                       `json:"body_text"`
	Excerpt                   *string                       `json:"excerpt"`
	ContentLanguage           *string                       `json:"content_language"`
	Author                    *string                       `json:"author"`
	SourceName                string                        `json:"source_name"`
	SourceFeedURL             *string                       `json:"source_feed_url"`
	TenantID                  string                        `json:"tenant_id"`
	ContentSourceID           string                        `json:"content_source_id"`
	SourceRunRequestID        string                        `json:"source_run_request_id"`
	SourceUpstreamItemID      string                        `json:"source_upstream_item_id,omitempty"`
	SourceUpstreamFingerprint string                        `json:"source_upstream_fingerprint,omitempty"`
	SourceObservationID       string                        `json:"source_observation_id,omitempty"`
	ReconstructionGrant       string                        `json:"content_reset_reconstruction_grant,omitempty"`
	SourceRunAttribution      *internalSourceRunAttribution `json:"source_run_attribution,omitempty"`
	OriginalURL               string                        `json:"original_url"`
	MediaURL                  *string                       `json:"media_url"`
	ThumbnailURL              *string                       `json:"thumbnail_url"`
	DurationSec               *int                          `json:"duration_sec"`
	TopicTags                 []string                      `json:"topic_tags"`
	Metadata                  map[string]interface{}        `json:"metadata"`
	PublishedAt               *string                       `json:"published_at"`
	RecoveryRunID             *string                       `json:"recovery_run_id"`
	RecoveryManifestHash      string                        `json:"recovery_manifest_hash"`
}

type internalSourceRunAttribution struct {
	RequestID           string `json:"request_id"`
	AttemptID           string `json:"attempt_id"`
	ExecutionUnitID     string `json:"execution_unit_id"`
	UnitJobID           string `json:"unit_job_id"`
	AttemptFenceToken   string `json:"attempt_fence_token"`
	ExecutionLeaseToken string `json:"execution_lease_token"`
	PageID              string `json:"page_id"`
	BatchID             string `json:"batch_id"`
}

type internalCreateContentItemResponse struct {
	ID                  string `json:"id"`
	TenantID            string `json:"tenant_id"`
	Status              string `json:"status"`
	Created             bool   `json:"created"`
	Retired             bool   `json:"retired,omitempty"`
	SourceRunAttributed bool   `json:"source_run_attributed,omitempty"`
	CreatedAt           string `json:"created_at"`
	DeliveryMode        string `json:"delivery_mode"`
	contentstage.ManifestDisposition
}

var sourceRunAttributionMetadataKeys = []string{
	"source_run_execution_unit_id",
	"source_run_attempt_id",
	"source_run_page_id",
	"source_run_batch_id",
}

var errInvalidSourceRunAttribution = errors.New("invalid source-run content attribution")

func mergeSourceRunAttribution(existing datatypes.JSON, attribution map[string]string) (datatypes.JSON, bool) {
	if len(attribution) != len(sourceRunAttributionMetadataKeys) {
		return existing, false
	}
	metadata := map[string]interface{}{}
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, &metadata); err != nil || metadata == nil {
			return existing, false
		}
	}
	for key, value := range attribution {
		metadata[key] = value
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return existing, false
	}
	return datatypes.JSON(encoded), true
}

func validateSourceRunAttribution(tx *gorm.DB, tenantID string, contentSourceID *uuid.UUID, sourceRunRequestID string, input *internalSourceRunAttribution) (map[string]string, error) {
	if input == nil {
		return nil, nil
	}
	if tx == nil || contentSourceID == nil || *contentSourceID == uuid.Nil || strings.TrimSpace(sourceRunRequestID) == "" ||
		strings.TrimSpace(input.RequestID) != strings.TrimSpace(sourceRunRequestID) ||
		!validSourceRunAttributionToken(input.PageID) || !validSourceRunAttributionToken(input.BatchID) {
		return nil, fmt.Errorf("%w: envelope is incomplete", errInvalidSourceRunAttribution)
	}
	leaseToken, err := uuid.Parse(strings.TrimSpace(input.ExecutionLeaseToken))
	if err != nil {
		return nil, fmt.Errorf("%w: execution lease token is invalid", errInvalidSourceRunAttribution)
	}
	unit, err := supply.VerifyExecutionEnvelope(tx, tenantID, input.RequestID, input.AttemptID, input.ExecutionUnitID, input.UnitJobID, input.AttemptFenceToken)
	if err != nil {
		return nil, fmt.Errorf("%w: execution envelope is invalid: %v", errInvalidSourceRunAttribution, err)
	}
	now := time.Now().UTC()
	if unit.ContentSourceID != *contentSourceID || unit.UnitType != "normalize_batch" || unit.PageID != input.PageID || unit.BatchID != input.BatchID ||
		unit.State != string(supply.UnitRunning) || unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != leaseToken ||
		unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(now) {
		return nil, fmt.Errorf("%w: normalization unit is not current for this content write", errInvalidSourceRunAttribution)
	}
	return map[string]string{
		"source_run_execution_unit_id": unit.PublicID.String(),
		"source_run_attempt_id":        unit.SourceRunAttemptID.String(),
		"source_run_page_id":           unit.PageID,
		"source_run_batch_id":          unit.BatchID,
	}, nil
}

func stripSourceRunAttributionMetadata(metadata map[string]interface{}) map[string]interface{} {
	clean := make(map[string]interface{}, len(metadata))
	reserved := make(map[string]bool, len(sourceRunAttributionMetadataKeys))
	for _, key := range sourceRunAttributionMetadataKeys {
		reserved[key] = true
	}
	for key, value := range metadata {
		if !reserved[key] {
			clean[key] = value
		}
	}
	return clean
}

func validSourceRunAttributionToken(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && len(value) <= 128
}

type internalUpdateContentItemRequest struct {
	Title           *string                `json:"title"`
	BodyText        *string                `json:"body_text"`
	Excerpt         *string                `json:"excerpt"`
	ContentLanguage *string                `json:"content_language"`
	Author          *string                `json:"author"`
	SourceName      *string                `json:"source_name"`
	SourceFeed      *string                `json:"source_feed_url"`
	OriginalURL     *string                `json:"original_url"`
	PublishedAt     *string                `json:"published_at"`
	Metadata        map[string]interface{} `json:"metadata"`
}

// internalEnrichmentMetadataRequest is intentionally narrow: Enrichment owns
// only generated summaries, key points, and language-specific translations.
// It must not receive a general metadata replacement capability.
type internalEnrichmentMetadataRequest struct {
	Fields           map[string]interface{}              `json:"fields"`
	ArtifactRecovery *artifactRecoveryCorrelationRequest `json:"artifact_recovery,omitempty"`
	ContentStage     *contentStageCorrelationRequest     `json:"content_stage,omitempty"`
}

type artifactRecoveryCorrelationRequest struct {
	RequestID       string `json:"request_id"`
	AttemptID       string `json:"attempt_id"`
	ClaimToken      string `json:"claim_token"`
	FenceToken      string `json:"fence_token"`
	InputDigest     string `json:"input_digest"`
	ProducerEventID string `json:"producer_event_id"`
}

func (value *artifactRecoveryCorrelationRequest) correlation() artifacts.Correlation {
	if value == nil {
		return artifacts.Correlation{}
	}
	return artifacts.Correlation{RequestID: value.RequestID, AttemptID: value.AttemptID, ClaimToken: value.ClaimToken, FenceToken: value.FenceToken, InputDigest: value.InputDigest, ProducerEventID: value.ProducerEventID}
}

func normalizeContentLanguage(raw *string) *string {
	if raw == nil {
		return nil
	}
	value := strings.ToLower(strings.TrimSpace(*raw))
	if value != "ar" && value != "en" {
		return nil
	}
	return &value
}

func validEnrichmentMetadataField(key string, value interface{}) bool {
	if key == "summary" {
		_, ok := value.(string)
		return ok
	}
	if key == "key_points" {
		_, ok := value.([]interface{})
		return ok
	}
	if strings.HasPrefix(key, "translation_") && len(key) > len("translation_") {
		_, ok := value.(string)
		return ok
	}
	return false
}

// InternalMergeEnrichmentMetadata atomically merges Enrichment-owned fields
// into metadata. The JSONB operation preserves ingest/artifact keys even when
// the two services write concurrently.
func InternalMergeEnrichmentMetadata(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}
	var req internalEnrichmentMetadataRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.Fields) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid enrichment metadata"})
		return
	}
	for key, value := range req.Fields {
		if !validEnrichmentMetadataField(key, value) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Unsupported enrichment metadata field"})
			return
		}
	}
	var recoveryRequest models.ArtifactCoverageRequest
	var recoveryAttempt models.ArtifactCoverageAttempt
	if req.ArtifactRecovery != nil {
		recoveryRequest, recoveryAttempt, err = artifacts.AuthorizeWriteback(db, id, artifacts.EnrichmentOwner, artifacts.ArtifactLLMMetadata, req.ArtifactRecovery.correlation())
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Artifact recovery correlation is stale"})
			return
		}
	}
	raw, err := json.Marshal(req.Fields)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid enrichment metadata"})
		return
	}
	var rows int64
	err = db.Transaction(func(tx *gorm.DB) error {
		var item models.ContentItem
		claimsAvailable := lifecycle.SchemaAvailable(tx)
		if claimsAvailable || req.ContentStage != nil || req.ArtifactRecovery != nil {
			if err := tx.Where("public_id = ?", id).First(&item).Error; err != nil {
				return err
			}
			if claimsAvailable {
				if err := checkContentLifecycleMutation(tx, item); err != nil {
					return err
				}
			}
		}
		// The legacy enrichment endpoint remains a compatibility path when no
		// durable stage correlation is supplied. The row read above supplies the
		// resource identity needed to honor lifecycle campaign claims.
		if req.ContentStage == nil && req.ArtifactRecovery == nil {
			result := tx.Model(&models.ContentItem{}).Where("public_id = ?", id).UpdateColumn("metadata", gorm.Expr("COALESCE(metadata, '{}'::jsonb) || ?::jsonb", string(raw)))
			rows = result.RowsAffected
			return result.Error
		}
		var stageRequest models.ContentStageRequest
		var stageAttempt models.ContentStageAttempt
		stage := models.ContentStagePodsLLMMetadata
		if item.Type == models.ContentTypeNews {
			stage = models.ContentStageNewsLLMMetadata
		}
		if err := requireNormalStageCorrelation(tx, item, stage, req.ContentStage, req.ArtifactRecovery != nil); err != nil {
			return err
		}
		if req.ContentStage != nil {
			var authErr error
			stageRequest, stageAttempt, authErr = contentstage.AuthorizeWriteback(tx, id, req.ContentStage.correlation(), stage)
			if authErr != nil {
				return authErr
			}
		}
		result := tx.Model(&models.ContentItem{}).Where("public_id = ?", id).UpdateColumn("metadata", gorm.Expr("COALESCE(metadata, '{}'::jsonb) || ?::jsonb", string(raw)))
		rows = result.RowsAffected
		if result.Error != nil {
			return result.Error
		}
		if req.ArtifactRecovery != nil {
			return artifacts.RecordPersistence(tx, recoveryRequest, recoveryAttempt, req.ArtifactRecovery.correlation(), map[string]any{"fields": len(req.Fields)})
		}
		if req.ContentStage != nil {
			return contentstage.RecordPersistence(tx, stageRequest, stageAttempt, req.ContentStage.correlation(), models.ContentStageOwnerEnrichment, "llm-metadata", map[string]any{"fields": len(req.Fields)})
		}
		return nil
	})
	if err != nil {
		if lifecycle.IsConflict(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "content mutation conflicts with an active lifecycle campaign", "code": "OPERATION_CONFLICT"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to merge enrichment metadata"})
		return
	}
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "fields": req.Fields})
}

type internalUpdateStatusRequest struct {
	Status           string  `json:"status"`
	FailureReason    *string `json:"failure_reason"`
	FeedVisibility   *string `json:"feed_visibility"`
	ChapteringStatus *string `json:"chaptering_status"`
}

type internalUpdateArtifactsRequest struct {
	DeferStageCompletion  bool                     `json:"defer_stage_completion,omitempty"`
	MediaURL              *string                  `json:"media_url"`
	ThumbnailURL          *string                  `json:"thumbnail_url"`
	DurationSec           *int                     `json:"duration_sec"`
	FileSizeBytes         *int64                   `json:"file_size_bytes"`
	StorageTier           *string                  `json:"storage_tier"`
	PlaybackURL           *string                  `json:"playback_url"`
	PlaybackType          *string                  `json:"playback_type"`
	FallbackPlaybackURL   *string                  `json:"fallback_playback_url"`
	HasVideo              *bool                    `json:"has_video"`
	VisualAvailable       *bool                    `json:"visual_available"`
	RenditionSetVersion   *int                     `json:"rendition_set_version"`
	MediaRenditions       []map[string]interface{} `json:"media_renditions"`
	MediaSuitability      *string                  `json:"media_suitability"`
	SuitabilityConfidence *float64                 `json:"media_suitability_confidence"`
	SuitabilityReasons    []string                 `json:"media_suitability_reasons"`

	// Quality bookkeeping. These are recorded once per item at first ingest;
	// the controller writes them only if the existing column is NULL.
	OriginalSizeBytes       *int64 `json:"original_size_bytes"`
	OriginalBitrateKbps     *int   `json:"original_bitrate_kbps"`
	CurrentBitrateKbps      *int   `json:"current_bitrate_kbps"`
	CurrentQualityProfileID *uint  `json:"current_quality_profile_id"`

	// Download-time signals harvested from the yt-dlp info-json (heatmap,
	// sponsor_segments, categories). MERGED into the existing metadata jsonb so
	// fetcher-set keys (videoId, tags, categoryId, …) are preserved.
	Metadata map[string]interface{} `json:"metadata"`
	// ExpectedItemUpdatedAt is an optional owner-issued fence. Normal ingest
	// leaves it empty; exact Pipeline repair writes must use it so a stale
	// worker cannot attach an artifact to a newer item version.
	ExpectedItemUpdatedAt *string                         `json:"expected_item_updated_at"`
	ContentStage          *contentStageCorrelationRequest `json:"content_stage,omitempty"`
}

type internalUpdateEmbeddingRequest struct {
	Embedding []float32 `json:"embedding"`
	TopicTags []string  `json:"topic_tags"`
	// Model is the embedder that produced this vector (provenance). Optional
	// for back-compat — when absent the row's embedding_model is cleared, which
	// flags the vector for re-embedding by the reconcile sweep.
	Model string `json:"model"`
	// SpaceID / ProducerID are the immutable vector-space identities (stage 10).
	// SpaceID = "may this be compared?"; ProducerID = "must this surface be
	// recomputed?". When absent both are cleared so the row is visibly unstamped
	// debt rather than silently inheriting a stale identity. The comparability
	// guards exclude NULL-space rows from similarity.
	SpaceID          string                              `json:"space_id"`
	ProducerID       string                              `json:"producer_id"`
	ArtifactRecovery *artifactRecoveryCorrelationRequest `json:"artifact_recovery,omitempty"`
	PipelineRepair   *pipelineRepairCorrelationRequest   `json:"pipeline_repair,omitempty"`
	ContentStage     *contentStageCorrelationRequest     `json:"content_stage,omitempty"`
}

// Topic tags are optional enrichment metadata. They deliberately have a
// separate write contract from embeddings so a slow/failing LLM call cannot
// keep the required embedding stage claimed or require a second correlated
// stage writeback.
type internalUpdateTopicTagsRequest struct {
	TopicTags []string `json:"topic_tags"`
}

type contentStageCorrelationRequest struct {
	RequestID        string `json:"request_id"`
	AttemptID        string `json:"attempt_id"`
	ClaimToken       string `json:"claim_token"`
	FenceToken       string `json:"fence_token"`
	InputFingerprint string `json:"input_fingerprint"`
	ProducerEventID  string `json:"producer_event_id"`
}

func (value *contentStageCorrelationRequest) correlation() contentstage.Correlation {
	if value == nil {
		return contentstage.Correlation{}
	}
	return contentstage.Correlation{
		RequestID: value.RequestID, AttemptID: value.AttemptID, ClaimToken: value.ClaimToken,
		FenceToken: value.FenceToken, InputFingerprint: value.InputFingerprint,
		ProducerEventID: value.ProducerEventID,
	}
}

type pipelineRepairCorrelationRequest struct {
	RepairID            string `json:"repair_id"`
	AttemptID           string `json:"attempt_id"`
	ClaimToken          string `json:"claim_token"`
	FenceToken          string `json:"fence_token"`
	ExpectedItemVersion string `json:"expected_item_version"`
	InputDigest         string `json:"input_digest"`
}

func (value *pipelineRepairCorrelationRequest) correlation() pipeline.TextEmbeddingWriteback {
	if value == nil {
		return pipeline.TextEmbeddingWriteback{}
	}
	return pipeline.TextEmbeddingWriteback{RepairID: value.RepairID, AttemptID: value.AttemptID, ClaimToken: value.ClaimToken, FenceToken: value.FenceToken, ExpectedItemVersion: value.ExpectedItemVersion, InputDigest: value.InputDigest}
}

// textEmbeddingDim is the dense embedding length Qwen3-Embedding-0.6B produces.
// Mirrors the strict-dimension check on image embeddings (CLIP at 512).
const textEmbeddingDim = 1024

type internalUpdateImageEmbeddingRequest struct {
	Embedding []float32 `json:"embedding"`
	// Vector-space provenance for the CLIP space (stage 10), mirroring the text
	// write-back. Media supplies these on write-back; absent ⇒ cleared.
	Model            string                              `json:"model"`
	SpaceID          string                              `json:"space_id"`
	ProducerID       string                              `json:"producer_id"`
	ArtifactRecovery *artifactRecoveryCorrelationRequest `json:"artifact_recovery,omitempty"`
	ContentStage     *contentStageCorrelationRequest     `json:"content_stage,omitempty"`
}

type internalLinkTranscriptRequest struct {
	TranscriptID     string                              `json:"transcript_id"`
	ArtifactRecovery *artifactRecoveryCorrelationRequest `json:"artifact_recovery,omitempty"`
	ContentStage     *contentStageCorrelationRequest     `json:"content_stage,omitempty"`
}

func requireNormalStageCorrelation(db *gorm.DB, item models.ContentItem, stage string, correlation *contentStageCorrelationRequest, alternativeAuthorizedPath bool) error {
	lane := models.ContentStageLaneNews
	if item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast {
		lane = models.ContentStageLanePods
	}
	mode, err := contentstage.CutoverMode(db, item.TenantID, lane)
	if err != nil {
		return err
	}
	if mode == models.ContentStageCutoverDurableRequired && correlation == nil && !alternativeAuthorizedPath {
		return fmt.Errorf("durable content-stage correlation is required")
	}
	return nil
}

const maxIdempotencyKeyLength = 512

func lifecycleSourceID(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}

func checkContentLifecycleMutation(tx *gorm.DB, item models.ContentItem) error {
	lane := "news"
	if item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast {
		lane = "pods"
	}
	return lifecycle.Check(tx, lifecycle.Scope{
		TenantID: item.TenantID,
		Lane:     lane,
		SourceID: lifecycleSourceID(item.ContentSourceID),
		ItemID:   item.PublicID.String(),
	}, lifecycle.PhaseContentWrite)
}

// lockContentForLifecycleMutation takes the shared lifecycle boundary before
// locking the content row, then verifies that the resource identity did not
// change between observation and lock acquisition. Owner commits use this in
// their existing short transaction so a campaign claim and a late write are
// ordered by the same advisory lock.
func lockContentForLifecycleMutation(tx *gorm.DB, tenantID string, contentID uuid.UUID) (models.ContentItem, error) {
	if tx == nil || strings.TrimSpace(tenantID) == "" || contentID == uuid.Nil {
		return models.ContentItem{}, errors.New("content lifecycle mutation requires tenant and content identity")
	}
	var observed models.ContentItem
	if err := tx.Where("tenant_id = ? AND public_id = ?", tenantID, contentID).First(&observed).Error; err != nil {
		return models.ContentItem{}, err
	}
	if err := checkContentLifecycleMutation(tx, observed); err != nil {
		return models.ContentItem{}, err
	}
	var current models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", tenantID, contentID).First(&current).Error; err != nil {
		return models.ContentItem{}, err
	}
	if !current.UpdatedAt.Equal(observed.UpdatedAt) || current.Type != observed.Type ||
		current.ProcessingGeneration != observed.ProcessingGeneration ||
		(current.ContentSourceID == nil) != (observed.ContentSourceID == nil) ||
		(current.ContentSourceID != nil && *current.ContentSourceID != *observed.ContentSourceID) {
		return models.ContentItem{}, errors.New("content lifecycle identity changed during owner admission")
	}
	return current, nil
}

func writeLifecycleConflict(c *gin.Context, err error) bool {
	if lifecycle.IsConflict(err) {
		c.JSON(http.StatusConflict, gin.H{"error": "content mutation conflicts with an active lifecycle campaign", "code": "OPERATION_CONFLICT"})
		return true
	}
	if lifecycle.IsIntakePaused(err) {
		c.JSON(http.StatusConflict, gin.H{"error": "source intake is paused by an active Content Reset operation", "code": "SOURCE_ADMISSION_PAUSED"})
		return true
	}
	return false
}

func normalizeMediaSuitability(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case models.MediaSuitabilityAudioFirstTalkingHead:
		return models.MediaSuitabilityAudioFirstTalkingHead
	case models.MediaSuitabilityAudioFirstShow:
		return models.MediaSuitabilityAudioFirstShow
	case models.MediaSuitabilityVisualDependent:
		return models.MediaSuitabilityVisualDependent
	case models.MediaSuitabilityUnsuitable:
		return models.MediaSuitabilityUnsuitable
	default:
		return models.MediaSuitabilityUnknown
	}
}

func normalizeIdempotencyKey(key string) string {
	normalized := strings.TrimSpace(key)
	if utf8.RuneCountInString(normalized) <= maxIdempotencyKeyLength {
		return normalized
	}

	// Keep deterministic de-duplication for very long URLs/keys without DB length errors.
	sum := sha256.Sum256([]byte(normalized))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func setFeedUnitDurationBucket(item *models.ContentItem) {
	if item == nil || item.DurationSec == nil || *item.DurationSec <= 0 {
		return
	}
	if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
		return
	}
	// Ingested parents begin hidden until Media writes the authoritative
	// duration. Raw media inside the Pods duration contract does not need
	// atomization and must become a feed unit; long parents and undersized clips
	// remain hidden. Child visibility is owned by the atomization workflow.
	if item.ParentContentItemID == nil && item.ChapteringStatus != nil && *item.ChapteringStatus == "waiting_media" {
		if *item.DurationSec >= podsMinDurationSec && *item.DurationSec <= podsHardMaxDurationSec {
			item.IsFeedUnit = true
			item.FeedVisibility = "visible"
		} else {
			item.IsFeedUnit = false
			item.FeedVisibility = "hidden"
		}
	}
	if !item.IsFeedUnit {
		item.DurationBucket = nil
		return
	}
	bucket := durationBucketLabel(*item.DurationSec * 1000)
	item.DurationBucket = &bucket
}

func mediaDurationBelowAdmission(kind models.ContentType, duration *int) bool {
	return (kind == models.ContentTypeVideo || kind == models.ContentTypePodcast) && duration != nil && *duration < podsMinDurationSec
}

func mediaArtifactDurationInvalid(kind models.ContentType, duration *int) bool {
	return (kind == models.ContentTypeVideo || kind == models.ContentTypePodcast) && (duration == nil || *duration < podsMinDurationSec)
}

func durationVerificationMatches(value interface{}, duration int) bool {
	verification, ok := value.(map[string]interface{})
	if !ok || verification["source"] != "ffprobe" {
		return false
	}
	switch verified := verification["duration_sec"].(type) {
	case float64:
		return verified == float64(duration)
	case int:
		return verified == duration
	case int64:
		return verified == int64(duration)
	default:
		return false
	}
}

func mediaArtifactDurationVerified(item models.ContentItem, incoming map[string]interface{}, duration int) bool {
	if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
		return true
	}
	if value, exists := incoming["duration_verification"]; exists {
		return durationVerificationMatches(value, duration)
	}
	existing := map[string]interface{}{}
	if len(item.Metadata) > 0 && json.Unmarshal(item.Metadata, &existing) == nil {
		return durationVerificationMatches(existing["duration_verification"], duration)
	}
	return false
}

// InternalCreateContentItem handles POST /internal/content-items
func InternalCreateContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalCreateContentItemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if strings.TrimSpace(req.IdempotencyKey) == "" || req.Type == "" || req.Source == "" || req.Status == "" || req.Title == "" || req.OriginalURL == "" || req.SourceName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing required fields"})
		return
	}

	idempotencyKey := normalizeIdempotencyKey(req.IdempotencyKey)
	lineageTenantID := strings.TrimSpace(req.TenantID)
	if lineageTenantID == "" {
		lineageTenantID = defaultCirculationTenant
	}

	var contentSourceID *uuid.UUID
	var sourceRunRequestID *uint
	if strings.TrimSpace(req.ContentSourceID) != "" || strings.TrimSpace(req.SourceRunRequestID) != "" {
		sourcePublicID, err := uuid.Parse(strings.TrimSpace(req.ContentSourceID))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content_source_id"})
			return
		}
		var source models.ContentSource
		if err := db.Where("public_id=? AND tenant_id=?", sourcePublicID, lineageTenantID).First(&source).Error; err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Unknown content source lineage"})
			return
		}
		contentSourceID = &sourcePublicID
		if strings.TrimSpace(req.SourceRunRequestID) != "" {
			runRequest, err := sourceRunRequestByPublicID(db, lineageTenantID, req.SourceRunRequestID)
			if err != nil || runRequest.ContentSourceID != sourcePublicID {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid source-run lineage"})
				return
			}
			sourceRunRequestID = &runRequest.ID
			// Durable replay purpose requires a scoped reconstruction grant even
			// when a stale worker omits its optional observation/replay context.
			if err := validateContentResetReplayIngest(runRequest.Purpose, req); err != nil {
				c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "code": "CONTENT_RESET_RECONSTRUCTION_GRANT_INVALID"})
				return
			}
		}
	}
	var sourceIDString string
	if contentSourceID != nil {
		sourceIDString = contentSourceID.String()
	}
	identityInput, identityErr := sourceidentity.ParseInput(
		lineageTenantID, sourceIDString, req.SourceRunRequestID,
		req.SourceObservationID, req.SourceUpstreamItemID,
	)
	if identityErr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid source item observation", "code": "SOURCE_OBSERVATION_INVALID"})
		return
	}
	if identityInput != nil {
		if req.SourceRunAttribution == nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Source item identity requires a current normalization unit", "code": "SOURCE_OBSERVATION_INVALID"})
			return
		}
		identityInput.SourceRunAttemptID, identityErr = uuid.Parse(strings.TrimSpace(req.SourceRunAttribution.AttemptID))
		if identityErr == nil {
			identityInput.ExecutionUnitID, identityErr = uuid.Parse(strings.TrimSpace(req.SourceRunAttribution.ExecutionUnitID))
		}
		if identityErr == nil {
			identityInput.ExecutionFenceToken, identityErr = uuid.Parse(strings.TrimSpace(req.SourceRunAttribution.AttemptFenceToken))
		}
		if identityErr == nil {
			identityInput.ExecutionLeaseToken, identityErr = uuid.Parse(strings.TrimSpace(req.SourceRunAttribution.ExecutionLeaseToken))
		}
		if identityErr != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Source item execution identity is invalid", "code": "SOURCE_OBSERVATION_INVALID"})
			return
		}
		identityInput.UnitJobID = strings.TrimSpace(req.SourceRunAttribution.UnitJobID)
		identityInput.PageID = strings.TrimSpace(req.SourceRunAttribution.PageID)
		identityInput.BatchID = strings.TrimSpace(req.SourceRunAttribution.BatchID)
		identityInput.ExpectedFingerprint = strings.ToLower(strings.TrimSpace(req.SourceUpstreamFingerprint))
		if _, attributionErr := validateSourceRunAttribution(db, lineageTenantID, contentSourceID, req.SourceRunRequestID, req.SourceRunAttribution); attributionErr != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Source item identity is outside the current fenced normalization unit", "code": "SOURCE_OBSERVATION_INVALID"})
			return
		}
	}
	if strings.TrimSpace(req.ReconstructionGrant) != "" {
		if identityInput == nil || req.SourceRunAttribution == nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Content Reset reconstruction grant requires source identity and a current normalization unit", "code": "CONTENT_RESET_RECONSTRUCTION_GRANT_INVALID"})
			return
		}
		if len(identityInput.ExpectedFingerprint) != 64 {
			c.JSON(http.StatusConflict, gin.H{"error": "Content Reset replay fingerprint is invalid", "code": "CONTENT_RESET_RECONSTRUCTION_GRANT_INVALID"})
			return
		}
		if _, fingerprintErr := hex.DecodeString(identityInput.ExpectedFingerprint); fingerprintErr != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Content Reset replay fingerprint is invalid", "code": "CONTENT_RESET_RECONSTRUCTION_GRANT_INVALID"})
			return
		}
	}
	var identityBinding *sourceidentity.Binding
	if strings.TrimSpace(req.ReconstructionGrant) != "" {
		identityBinding, identityErr = sourceidentity.ResolveReconstructionGrant(db, identityInput, req.ReconstructionGrant)
	} else {
		identityBinding, identityErr = sourceidentity.Resolve(db, identityInput)
	}
	if identityErr != nil {
		code := "SOURCE_OBSERVATION_INVALID"
		message := "Source item observation is not authorized for this materialization"
		if strings.TrimSpace(req.ReconstructionGrant) != "" {
			code = "CONTENT_RESET_RECONSTRUCTION_GRANT_INVALID"
			message = "Content Reset reconstruction grant is invalid or outside its approved scope"
		}
		c.JSON(http.StatusConflict, gin.H{"error": message, "code": code})
		return
	}
	if identityBinding != nil && identityBinding.ReconstructionGrant != nil {
		grant := identityBinding.ReconstructionGrant
		idempotencyKey = fmt.Sprintf("content-reset-instance:%s:%d", identityBinding.IdentityPublicID, grant.ReplacementInstanceGeneration)
	}

	// Identity resolution is tenant-scoped. A matching provider key in another
	// tenant must never expose or mutate that tenant's item or stage evidence.
	var existing models.ContentItem
	var lookupErr error
	if identityBinding != nil && identityBinding.HasIdentity {
		if identityBinding.CurrentContentItemID == nil {
			grantAllowsNewIdentity := identityBinding.ReconstructionGrant != nil &&
				identityBinding.ReconstructionGrant.State == "issued" &&
				identityBinding.ReconstructionGrant.GrantKind == "new_identity" &&
				identityBinding.CurrentInstance == 0
			if !grantAllowsNewIdentity {
				c.JSON(http.StatusConflict, gin.H{"error": "Source item identity registry is inconsistent", "code": "SOURCE_ITEM_IDENTITY_CONFLICT"})
				return
			}
			lookupErr = gorm.ErrRecordNotFound
		} else if identityBinding.ReconstructionGrant != nil && identityBinding.ReconstructionGrant.State == "issued" {
			// Fresh Start must create a separate content instance. Reusing the old
			// row would keep its retired identity and idempotency key in place.
			lookupErr = gorm.ErrRecordNotFound
		} else {
			lookupErr = db.Where("tenant_id = ? AND public_id = ?", lineageTenantID, *identityBinding.CurrentContentItemID).First(&existing).Error
		}
		if errors.Is(lookupErr, gorm.ErrRecordNotFound) && identityBinding.ReconstructionGrant == nil {
			c.JSON(http.StatusOK, internalCreateContentItemResponse{
				ID: identityBinding.CurrentContentItemID.String(), Status: string(models.ContentStatusArchived), Created: false, Retired: true,
			})
			return
		}
		if lookupErr == nil && (existing.ContentSourceID == nil || *existing.ContentSourceID != identityBinding.Input.ContentSourceID) {
			c.JSON(http.StatusConflict, gin.H{"error": "Source item identity points to content owned by another source", "code": "SOURCE_ITEM_IDENTITY_CONFLICT"})
			return
		}
	} else {
		lookupErr = db.Where("tenant_id = ? AND idempotency_key = ?", lineageTenantID, idempotencyKey).First(&existing).Error
	}
	if errors.Is(lookupErr, gorm.ErrRecordNotFound) && identityBinding != nil && identityBinding.ReconstructionGrant != nil && identityBinding.ReconstructionGrant.State == "consumed" {
		c.JSON(http.StatusConflict, gin.H{"error": "Consumed Content Reset replacement is missing from the content store", "code": "CONTENT_RESET_REPLACEMENT_MISSING"})
		return
	}
	if lookupErr == nil {
		if identityBinding != nil && identityBinding.ReconstructionGrant != nil && identityBinding.ReconstructionGrant.State == "consumed" {
			grant := identityBinding.ReconstructionGrant
			if grant.ReplacementContentItemID == nil || existing.PublicID != *grant.ReplacementContentItemID {
				c.JSON(http.StatusConflict, gin.H{"error": "Content Reset reconstruction grant no longer points to its replacement", "code": "CONTENT_RESET_RECONSTRUCTION_GRANT_INVALID"})
				return
			}
			deliveryMode, modeErr := contentstage.DeliveryMode(db, existing.TenantID, existing.Type)
			if modeErr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to resolve content delivery mode"})
				return
			}
			c.JSON(http.StatusOK, internalCreateContentItemResponse{
				ID: existing.PublicID.String(), TenantID: existing.TenantID, Status: string(existing.Status),
				Created: false, SourceRunAttributed: true, CreatedAt: existing.CreatedAt.UTC().Format(time.RFC3339), DeliveryMode: deliveryMode,
			})
			return
		}
		var requests []models.ContentStageRequest
		disposition := "no_change"
		sourceRunAttributed := false
		if manifestErr := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=?", existing.PublicID).First(&existing).Error; err != nil {
				return err
			}
			lane := "news"
			if existing.Type == models.ContentTypeVideo || existing.Type == models.ContentTypePodcast {
				lane = "pods"
			}
			if err := lifecycle.Check(tx, lifecycle.Scope{
				TenantID: existing.TenantID,
				Lane:     lane,
				SourceID: lifecycleSourceID(existing.ContentSourceID),
				ItemID:   existing.PublicID.String(),
			}, lifecycle.PhaseContentWrite); err != nil {
				return err
			}
			previousDigest := ""
			if existing.ProcessingInputDigest != nil {
				previousDigest = *existing.ProcessingInputDigest
			}
			// The idempotency identity is stable, but source observations may carry
			// corrected text or a replaced media reference. Compare those inputs in
			// CMS; Redis and Aggregation must not guess whether work is current.
			sameSource := contentSourceID != nil && existing.ContentSourceID != nil && *contentSourceID == *existing.ContentSourceID
			if contentSourceID != nil && (existing.ContentSourceID == nil || *contentSourceID != *existing.ContentSourceID) {
				return errors.New("idempotency identity is already owned by another content source")
			}
			sameSourceRun := sourceRunRequestID != nil && existing.SourceRunRequestID != nil && *sourceRunRequestID == *existing.SourceRunRequestID
			if (contentSourceID == nil && existing.ContentSourceID == nil) || sameSource {
				existing.Title = &req.Title
				existing.BodyText = req.BodyText
				existing.Excerpt = req.Excerpt
				existing.ContentLanguage = normalizeContentLanguage(req.ContentLanguage)
				existing.SourceFeedURL = req.SourceFeedURL
				existing.OriginalURL = &req.OriginalURL
			}
			if sameSource && sameSourceRun && req.SourceRunAttribution != nil {
				attribution, attributionErr := validateSourceRunAttribution(tx, existing.TenantID, contentSourceID, req.SourceRunRequestID, req.SourceRunAttribution)
				if attributionErr != nil {
					return attributionErr
				}
				mergedMetadata, attributed := mergeSourceRunAttribution(existing.Metadata, attribution)
				if !attributed {
					return errInvalidSourceRunAttribution
				}
				existing.Metadata = mergedMetadata
				sourceRunAttributed = true
			}
			// media_url, thumbnail_url, and duration_sec are produced artifacts
			// after materialization; duplicate intake must never overwrite them
			// with the original provider reference.
			if err := tx.Save(&existing).Error; err != nil {
				return err
			}
			if identityBinding != nil && sameSource {
				if err := sourceidentity.RegisterMaterialized(tx, identityBinding, existing.PublicID); err != nil {
					return err
				}
			}
			var changed bool
			var reconcileErr error
			requests, changed, reconcileErr = contentstage.ReconcileManifest(tx, &existing, previousDigest)
			if changed {
				disposition = "changed"
			}
			return reconcileErr
		}); manifestErr != nil {
			if errors.Is(manifestErr, errInvalidSourceRunAttribution) {
				c.JSON(http.StatusConflict, gin.H{"error": "Source-run content attribution is no longer current", "code": "SOURCE_RUN_ATTRIBUTION_INVALID"})
				return
			}
			if strings.Contains(manifestErr.Error(), "idempotency identity is already owned by another content source") {
				c.JSON(http.StatusConflict, gin.H{"error": "Idempotency identity belongs to another content source", "code": "CONTENT_IDENTITY_SOURCE_CONFLICT"})
				return
			}
			if lifecycle.IsConflict(manifestErr) {
				c.JSON(http.StatusConflict, gin.H{"error": "content mutation conflicts with an active lifecycle campaign", "code": "OPERATION_CONFLICT"})
				return
			}
			if errors.Is(manifestErr, sourceidentity.ErrIdentityConflict) || errors.Is(manifestErr, sourceidentity.ErrIdentityCorrupt) || errors.Is(manifestErr, sourceidentity.ErrInvalidObservation) {
				c.JSON(http.StatusConflict, gin.H{"error": manifestErr.Error(), "code": "SOURCE_ITEM_IDENTITY_CONFLICT"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to reconcile content stage manifest"})
			return
		}
		deliveryMode, modeErr := contentstage.DeliveryMode(db, existing.TenantID, existing.Type)
		if modeErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to resolve content delivery mode"})
			return
		}
		c.JSON(http.StatusOK, internalCreateContentItemResponse{
			ID:                  existing.PublicID.String(),
			TenantID:            existing.TenantID,
			Status:              string(existing.Status),
			Created:             false,
			SourceRunAttributed: sourceRunAttributed,
			CreatedAt:           existing.CreatedAt.UTC().Format(time.RFC3339),
			DeliveryMode:        deliveryMode,
			ManifestDisposition: contentstage.SummarizeForItem(db, existing, requests, disposition),
		})
		return
	} else if lookupErr != nil && !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check idempotency"})
		return
	}

	var publishedAt *time.Time
	if req.PublishedAt != nil && *req.PublishedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, *req.PublishedAt); err == nil {
			publishedAt = &parsed
		}
	}

	metadataJSON, _ := json.Marshal(stripSourceRunAttributionMetadata(req.Metadata))

	// Normalize kind + format. New callers send type='NEWS' with an explicit
	// format. Back-compat: legacy callers may still send type=ARTICLE/TWEET/
	// COMMENT — fold those into the NEWS kind with a format sub-classification.
	kind := models.ContentType(strings.ToUpper(req.Type))
	var format *string
	if req.Format != nil && strings.TrimSpace(*req.Format) != "" {
		f := strings.ToUpper(strings.TrimSpace(*req.Format))
		format = &f
	}
	switch kind {
	case models.ContentTypeArticle, models.ContentTypeTweet, models.ContentTypeComment:
		if format == nil {
			f := string(kind)
			format = &f
		}
		kind = models.ContentTypeNews
	}
	if mediaDurationBelowAdmission(kind, req.DurationSec) {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Media duration is below the Pods minimum", "code": "PODS_DURATION_BELOW_MINIMUM"})
		return
	}
	if kind == models.ContentTypeNews {
		identity, identityErr := retentionTombstoneIdentityForIngest(retentionV1Tenant, idempotencyKey, req.OriginalURL)
		if identityErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid News ingest identity"})
			return
		}
		var tombstone models.NewsIngestTombstone
		if err := db.Where("tenant_id = ? AND identity_hash = ?", retentionV1Tenant, identity).First(&tombstone).Error; err == nil {
			if req.RecoveryRunID != nil && strings.TrimSpace(req.RecoveryManifestHash) != "" && tombstone.RecoveryRunPublicID != nil && tombstone.RecoveryRunPublicID.String() == strings.TrimSpace(*req.RecoveryRunID) && tombstone.ManifestHash == strings.TrimSpace(req.RecoveryManifestHash) && tombstone.ReplayConsumedAt == nil {
				consumed := time.Now().UTC()
				if err := db.Model(&tombstone).Where("replay_consumed_at IS NULL").Update("replay_consumed_at", consumed).Error; err != nil {
					c.JSON(http.StatusConflict, gin.H{"error": "Recovery tombstone replay could not be consumed"})
					return
				}
			} else {
				c.JSON(http.StatusOK, internalCreateContentItemResponse{
					ID: tombstone.OriginalContentID.String(), Status: string(models.ContentStatusArchived), Created: false, Retired: true,
					CreatedAt: tombstone.CreatedAt.UTC().Format(time.RFC3339),
				})
				return
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check retired News identity"})
			return
		}
	}

	item := models.ContentItem{
		PublicID:           uuid.New(),
		TenantID:           lineageTenantID,
		Type:               kind,
		Format:             format,
		Source:             models.SourceType(strings.ToUpper(req.Source)),
		Status:             models.ContentStatus(strings.ToUpper(req.Status)),
		IdempotencyKey:     &idempotencyKey,
		Title:              &req.Title,
		BodyText:           req.BodyText,
		Excerpt:            req.Excerpt,
		ContentLanguage:    normalizeContentLanguage(req.ContentLanguage),
		Author:             req.Author,
		SourceName:         &req.SourceName,
		SourceFeedURL:      req.SourceFeedURL,
		ContentSourceID:    contentSourceID,
		SourceRunRequestID: sourceRunRequestID,
		MediaURL:           req.MediaURL,
		ThumbnailURL:       req.ThumbnailURL,
		OriginalURL:        &req.OriginalURL,
		DurationSec:        req.DurationSec,
		TopicTags:          req.TopicTags,
		Metadata:           datatypes.JSON(metadataJSON),
		PublishedAt:        publishedAt,
	}
	if kind == models.ContentTypeVideo || kind == models.ContentTypePodcast {
		waiting := "waiting_media"
		item.IsFeedUnit = false
		item.FeedVisibility = "hidden"
		item.ChapteringStatus = &waiting
	}

	sourceRunAttributed := false
	if err := db.Transaction(func(tx *gorm.DB) error {
		lane := "news"
		if kind == models.ContentTypeVideo || kind == models.ContentTypePodcast {
			lane = "pods"
		}
		if err := lifecycle.Check(tx, lifecycle.Scope{
			TenantID: lineageTenantID,
			Lane:     lane,
			SourceID: lifecycleSourceID(contentSourceID),
			ItemID:   item.PublicID.String(),
		}, lifecycle.PhaseContentCreate); err != nil {
			return err
		}
		if req.SourceRunAttribution != nil {
			attribution, attributionErr := validateSourceRunAttribution(tx, lineageTenantID, contentSourceID, req.SourceRunRequestID, req.SourceRunAttribution)
			if attributionErr != nil {
				return attributionErr
			}
			mergedMetadata, attributed := mergeSourceRunAttribution(item.Metadata, attribution)
			if !attributed {
				return errInvalidSourceRunAttribution
			}
			item.Metadata = mergedMetadata
			sourceRunAttributed = true
		}
		if err := tx.Create(&item).Error; err != nil {
			return err
		}
		if identityBinding != nil && identityBinding.ReconstructionGrant != nil {
			if err := sourceidentity.ConsumeReconstructionGrant(tx, identityInput, req.ReconstructionGrant, item.PublicID); err != nil {
				return err
			}
		} else if err := sourceidentity.RegisterMaterialized(tx, identityBinding, item.PublicID); err != nil {
			return err
		}
		if _, err := contentstage.EnsureManifest(tx, &item); err != nil {
			return err
		}
		if err := feedstate.SyncMediaMembership(tx, item); err != nil {
			return err
		}
		if contentSourceID == nil {
			return nil
		}
		return appendContentProcessingEvent(tx, models.ContentProcessingEvent{
			TenantID: lineageTenantID, ContentSourceID: contentSourceID, SourceRunRequestID: sourceRunRequestID, ContentItemID: &item.PublicID,
			Stage: lineageStageIngest, State: "completed", Producer: "cms", IdempotencyKey: idempotencyKey, EventClass: "content_item_created",
			Payload: lineagePayload(map[string]interface{}{"content_type": string(item.Type), "status": string(item.Status)}), OccurredAt: time.Now().UTC(),
		})
	}); err != nil {
		if writeLifecycleConflict(c, err) {
			return
		}
		if errors.Is(err, errInvalidSourceRunAttribution) {
			c.JSON(http.StatusConflict, gin.H{"error": "Source-run content attribution is no longer current", "code": "SOURCE_RUN_ATTRIBUTION_INVALID"})
			return
		}
		if errors.Is(err, sourceidentity.ErrInvalidReconstructionGrant) || errors.Is(err, sourceidentity.ErrReconstructionGrantScope) || errors.Is(err, sourceidentity.ErrReconstructionGrantUsed) {
			c.JSON(http.StatusConflict, gin.H{"error": "Content Reset reconstruction grant is no longer current for this item", "code": "CONTENT_RESET_RECONSTRUCTION_GRANT_INVALID"})
			return
		}
		if strings.Contains(err.Error(), "idempotency identity is already owned by another content source") {
			c.JSON(http.StatusConflict, gin.H{"error": "Idempotency identity belongs to another content source", "code": "CONTENT_IDENTITY_SOURCE_CONFLICT"})
			return
		}
		if errors.Is(err, sourceidentity.ErrIdentityConflict) || errors.Is(err, sourceidentity.ErrIdentityCorrupt) || errors.Is(err, sourceidentity.ErrInvalidObservation) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "code": "SOURCE_ITEM_IDENTITY_CONFLICT"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create content item"})
		return
	}

	var stageRequests []models.ContentStageRequest
	if contentstage.SchemaAvailable(db) {
		if err := db.Where("tenant_id=? AND content_item_id=? AND processing_generation=?", item.TenantID, item.PublicID, item.ProcessingGeneration).Order("created_at").Find(&stageRequests).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "content-stage manifest unavailable"})
			return
		}
	}
	deliveryMode, modeErr := contentstage.DeliveryMode(db, item.TenantID, item.Type)
	if modeErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to resolve content delivery mode"})
		return
	}
	c.JSON(http.StatusOK, internalCreateContentItemResponse{
		ID:                  item.PublicID.String(),
		TenantID:            item.TenantID,
		Status:              string(item.Status),
		Created:             true,
		SourceRunAttributed: sourceRunAttributed,
		CreatedAt:           item.CreatedAt.UTC().Format(time.RFC3339),
		DeliveryMode:        deliveryMode,
		ManifestDisposition: contentstage.SummarizeForItem(db, item, stageRequests, "created"),
	})
}

func validateContentResetReplayIngest(purpose string, req internalCreateContentItemRequest) error {
	if purpose == "content_reset_replay" && (strings.TrimSpace(req.ReconstructionGrant) == "" ||
		strings.TrimSpace(req.SourceObservationID) == "" || strings.TrimSpace(req.SourceUpstreamItemID) == "" || req.SourceRunAttribution == nil) {
		return errors.New("Content Reset replay requires a reconstruction grant, observation and fenced normalization unit")
	}
	return nil
}

// InternalUpdateContentItem handles PUT /internal/content-items/:id
func InternalUpdateContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateContentItemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	previousDigest := ""
	if item.ProcessingInputDigest != nil {
		previousDigest = *item.ProcessingInputDigest
	}
	if req.Title != nil {
		item.Title = req.Title
	}
	if req.BodyText != nil {
		item.BodyText = req.BodyText
	}
	if req.Excerpt != nil {
		item.Excerpt = req.Excerpt
	}
	if req.ContentLanguage != nil {
		item.ContentLanguage = normalizeContentLanguage(req.ContentLanguage)
	}
	if req.Author != nil {
		item.Author = req.Author
	}
	if req.SourceName != nil {
		item.SourceName = req.SourceName
	}
	if req.SourceFeed != nil {
		item.SourceFeedURL = req.SourceFeed
	}
	if req.OriginalURL != nil {
		item.OriginalURL = req.OriginalURL
	}
	if req.PublishedAt != nil && *req.PublishedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, *req.PublishedAt); err == nil {
			item.PublishedAt = &parsed
		}
	}
	if req.Metadata != nil {
		if raw, err := json.Marshal(req.Metadata); err == nil {
			item.Metadata = datatypes.JSON(raw)
		}
	}

	changed := false
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := checkContentLifecycleMutation(tx, item); err != nil {
			return err
		}
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		_, manifestChanged, err := contentstage.ReconcileManifest(tx, &item, previousDigest)
		changed = manifestChanged
		return err
	}); err != nil {
		if writeLifecycleConflict(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update content item"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "disposition": map[bool]string{true: "changed", false: "no_change"}[changed], "processing_generation": item.ProcessingGeneration})
}

// InternalUpdateContentStatus handles PATCH /internal/content-items/:id/status
func InternalUpdateContentStatus(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if req.Status == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Status is required"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	deliveryMode, modeErr := contentstage.DeliveryMode(db, item.TenantID, item.Type)
	if modeErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to resolve lifecycle ownership"})
		return
	}
	if deliveryMode == models.ContentStageCutoverDurableRequired {
		c.JSON(http.StatusConflict, gin.H{"error": "CMS content-stage reducer owns lifecycle in durable mode"})
		return
	}
	requestedStatus := models.ContentStatus(strings.ToUpper(req.Status))
	// A late legacy worker can report a timeout after the owning service already
	// persisted the required artifact. Never downgrade published content, and
	// never turn an artifact-complete failed row into another failed attempt.
	// The reconciliation worker promotes the latter without invoking Enrichment.
	if requestedStatus == models.ContentStatusFailed {
		if item.Status == models.ContentStatusReady || item.Status == models.ContentStatusArchived {
			c.JSON(http.StatusOK, gin.H{"success": true, "status": string(item.Status), "lifecycle_reconciled": false, "reason": "published_lifecycle_is_monotonic"})
			return
		}
		if contentItemHasRequiredArtifact(&item) {
			if item.Status == models.ContentStatusFailed {
				item.Status = models.ContentStatusReady
				setFeedUnitDurationBucket(&item)
				if err := db.Transaction(func(tx *gorm.DB) error {
					if err := checkContentLifecycleMutation(tx, item); err != nil {
						return err
					}
					if err := tx.Save(&item).Error; err != nil {
						return err
					}
					if err := feedstate.AttachReadyNewsStory(tx, item); err != nil {
						return err
					}
					if err := feedstate.SyncMediaMembership(tx, item); err != nil {
						return err
					}
					return appendItemProcessingEvent(tx, item, "content_status", "completed", "cms", "artifact_complete_reconciled", map[string]interface{}{"status": string(item.Status)})
				}); err != nil {
					if writeLifecycleConflict(c, err) {
						return
					}
					c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to reconcile artifact-complete status"})
					return
				}
				c.JSON(http.StatusOK, gin.H{"success": true, "status": string(item.Status), "lifecycle_reconciled": true, "reason": "required_artifact_present"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"success": true, "status": string(item.Status), "lifecycle_reconciled": false, "reason": "required_artifact_present"})
			return
		}
	}
	item.Status = requestedStatus
	if req.FeedVisibility != nil && strings.TrimSpace(*req.FeedVisibility) != "" {
		item.FeedVisibility = strings.TrimSpace(*req.FeedVisibility)
	}
	if req.ChapteringStatus != nil && strings.TrimSpace(*req.ChapteringStatus) != "" {
		status := strings.TrimSpace(*req.ChapteringStatus)
		item.ChapteringStatus = &status
	}
	setFeedUnitDurationBucket(&item)

	if req.FailureReason != nil {
		metadata := map[string]interface{}{}
		if len(item.Metadata) > 0 {
			_ = json.Unmarshal(item.Metadata, &metadata)
		}
		metadata["failure_reason"] = *req.FailureReason
		if raw, err := json.Marshal(metadata); err == nil {
			item.Metadata = datatypes.JSON(raw)
		}
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := checkContentLifecycleMutation(tx, item); err != nil {
			return err
		}
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		if err := feedstate.AttachReadyNewsStory(tx, item); err != nil {
			return err
		}
		if err := feedstate.SyncMediaMembership(tx, item); err != nil {
			return err
		}
		return appendItemProcessingEvent(tx, item, "content_status", "completed", "aggregation", "content_status_updated", map[string]interface{}{"status": string(item.Status)})
	}); err != nil {
		if writeLifecycleConflict(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update status"})
		return
	}
	if shouldPublishLinkedChapter(item) {
		_ = db.Model(&models.Chapter{}).
			Where("tenant_id = ? AND child_content_item_id = ?", item.TenantID, item.PublicID).
			Update("status", chapterStatusPublished).Error
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// contentItemHasRequiredArtifact is deliberately conservative. It prevents a
// stale compatibility failure from hiding a durable downstream effect; it is
// not a general READY eligibility shortcut.
func contentItemHasRequiredArtifact(item *models.ContentItem) bool {
	if item == nil {
		return false
	}
	if item.Type == models.ContentTypeNews {
		return item.Embedding != nil && item.StoryID != nil
	}
	if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
		return false
	}
	if item.Embedding == nil || item.PlaybackURL == nil || strings.TrimSpace(*item.PlaybackURL) == "" || item.DurationSec == nil || *item.DurationSec < podsMinDurationSec {
		return false
	}
	metadata := map[string]interface{}{}
	if len(item.Metadata) > 0 {
		_ = json.Unmarshal(item.Metadata, &metadata)
	}
	return mediaArtifactDurationVerified(*item, metadata, *item.DurationSec)
}

// InternalUpdateContentArtifacts handles PATCH /internal/content-items/:id/artifacts
func InternalUpdateContentArtifacts(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateArtifactsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	hasServingMediaWrite := req.MediaURL != nil || req.PlaybackURL != nil || req.FallbackPlaybackURL != nil || len(req.MediaRenditions) > 0
	if hasServingMediaWrite {
		effectiveDuration := req.DurationSec
		if effectiveDuration == nil {
			effectiveDuration = item.DurationSec
		}
		if mediaArtifactDurationInvalid(item.Type, effectiveDuration) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Media artifact duration is missing or below the Pods minimum", "code": "PODS_DURATION_NOT_ADMISSIBLE"})
			return
		}
		if effectiveDuration != nil && !mediaArtifactDurationVerified(item, req.Metadata, *effectiveDuration) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Media artifact duration is not authoritatively verified", "code": "PODS_DURATION_NOT_VERIFIED"})
			return
		}
	}
	var expectedItemUpdatedAt *time.Time
	if req.ExpectedItemUpdatedAt != nil {
		parsed, parseErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(*req.ExpectedItemUpdatedAt))
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid expected item version"})
			return
		}
		expectedItemUpdatedAt = &parsed
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		var stageRequest models.ContentStageRequest
		var stageAttempt models.ContentStageAttempt
		if err := requireNormalStageCorrelation(tx, item, models.ContentStagePodsMediaArtifacts, req.ContentStage, false); err != nil {
			return err
		}
		if req.ContentStage != nil {
			var err error
			stageRequest, stageAttempt, err = contentstage.AuthorizeWriteback(tx, id, req.ContentStage.correlation(), models.ContentStagePodsMediaArtifacts)
			if err != nil {
				return err
			}
		}
		query := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=?", item.PublicID, item.TenantID)
		if expectedItemUpdatedAt != nil {
			query = query.Where("updated_at=?", *expectedItemUpdatedAt)
		}
		if err := query.First(&item).Error; err != nil {
			if expectedItemUpdatedAt != nil {
				return fmt.Errorf("artifact target version is stale: %w", err)
			}
			return err
		}
		if err := checkContentLifecycleMutation(tx, item); err != nil {
			return err
		}
		applyArtifactRequest(&item, req)
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		if err := feedstate.SyncMediaMembership(tx, item); err != nil {
			return err
		}
		if req.ContentStage != nil && !req.DeferStageCompletion {
			artifactDigest := contentstage.ItemInputDigest(item)
			if err := contentstage.RecordPersistence(tx, stageRequest, stageAttempt, req.ContentStage.correlation(), models.ContentStageOwnerAggregationPods, artifactDigest, map[string]any{"playback_ready": item.PlaybackURL != nil, "duration_sec": item.DurationSec}); err != nil {
				return err
			}
		}
		if req.ContentStage != nil && req.DeferStageCompletion {
			return nil // Preparation is not complete until fenced activation finishes.
		}
		return appendItemProcessingEvent(tx, item, "media_artifacts", "completed", "aggregation", "media_artifacts_persisted", map[string]interface{}{"playback_ready": item.PlaybackURL != nil, "has_thumbnail": item.ThumbnailURL != nil})
	}); err != nil {
		if writeLifecycleConflict(c, err) {
			return
		}
		if strings.Contains(err.Error(), "artifact target version is stale") {
			c.JSON(http.StatusConflict, gin.H{"error": "Artifact target version is stale"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update artifacts"})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

func applyArtifactRequest(item *models.ContentItem, req internalUpdateArtifactsRequest) {
	if req.MediaURL != nil {
		item.MediaURL = req.MediaURL
	}
	if req.PlaybackURL != nil {
		item.PlaybackURL = req.PlaybackURL
	}
	if req.PlaybackType != nil {
		item.PlaybackType = req.PlaybackType
	}
	if req.FallbackPlaybackURL != nil {
		item.FallbackPlaybackURL = req.FallbackPlaybackURL
	}
	if req.HasVideo != nil {
		item.HasVideo = req.HasVideo
	}
	if req.VisualAvailable != nil {
		item.VisualAvailable = *req.VisualAvailable
	}
	if req.RenditionSetVersion != nil && *req.RenditionSetVersion > 0 {
		item.RenditionSetVersion = *req.RenditionSetVersion
	}
	if req.MediaRenditions != nil {
		if raw, err := json.Marshal(req.MediaRenditions); err == nil {
			item.MediaRenditions = datatypes.JSON(raw)
		}
	}
	if req.MediaSuitability != nil {
		item.MediaSuitability = normalizeMediaSuitability(*req.MediaSuitability)
		if req.SuitabilityConfidence != nil {
			conf := *req.SuitabilityConfidence
			if conf < 0 {
				conf = 0
			}
			if conf > 1 {
				conf = 1
			}
			item.MediaSuitabilityConfidence = &conf
		}
		if req.SuitabilityReasons != nil {
			if raw, err := json.Marshal(req.SuitabilityReasons); err == nil {
				item.MediaSuitabilityReasons = datatypes.JSON(raw)
			}
		}
	}
	if req.ThumbnailURL != nil {
		item.ThumbnailURL = req.ThumbnailURL
	}
	if req.DurationSec != nil {
		item.DurationSec = req.DurationSec
	}
	setFeedUnitDurationBucket(item)
	if req.FileSizeBytes != nil {
		item.FileSizeBytes = *req.FileSizeBytes
	}
	if req.StorageTier != nil {
		value := strings.ToLower(strings.TrimSpace(*req.StorageTier))
		if value == "" || value == "primary" {
			item.StorageTier = nil
		} else {
			item.StorageTier = &value
		}
	}
	if req.OriginalSizeBytes != nil && item.OriginalSizeBytes == nil {
		value := *req.OriginalSizeBytes
		item.OriginalSizeBytes = &value
	}
	if req.OriginalBitrateKbps != nil && item.OriginalBitrateKbps == nil {
		value := *req.OriginalBitrateKbps
		item.OriginalBitrateKbps = &value
	}
	if req.CurrentBitrateKbps != nil {
		value := *req.CurrentBitrateKbps
		item.CurrentBitrateKbps = &value
	}
	if req.CurrentQualityProfileID != nil {
		value := *req.CurrentQualityProfileID
		item.CurrentQualityProfileID = &value
	}
	if len(req.Metadata) > 0 {
		metadata := map[string]interface{}{}
		if len(item.Metadata) > 0 {
			_ = json.Unmarshal(item.Metadata, &metadata)
		}
		for key, value := range req.Metadata {
			metadata[key] = value
		}
		if raw, err := json.Marshal(metadata); err == nil {
			item.Metadata = datatypes.JSON(raw)
		}
	}
}

// InternalUpdateContentEmbedding handles PATCH /internal/content-items/:id/embedding
func InternalUpdateContentEmbedding(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateEmbeddingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if len(req.Embedding) != textEmbeddingDim {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Text embedding must be " + strconv.Itoa(textEmbeddingDim) +
				"-dim (got " + strconv.Itoa(len(req.Embedding)) + ")",
		})
		return
	}

	// Write fence (stage 10 §7): while a text campaign is running, every write
	// must carry the target identity. A write stamped with a different (old)
	// producer — a rolling old model instance overwriting a migrated row — is
	// rejected as writer_regression; a missing stamp is rejected too.
	if reason, blocked := fenceEmbeddingWrite(db, EmbeddingSpaceText, spaceid.RecipeContentText, req.SpaceID, req.ProducerID); blocked {
		c.JSON(http.StatusConflict, gin.H{"error": reason, "code": "writer_regression"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	var recoveryRequest models.ArtifactCoverageRequest
	var recoveryAttempt models.ArtifactCoverageAttempt
	if req.ArtifactRecovery != nil {
		recoveryRequest, recoveryAttempt, err = artifacts.AuthorizeWriteback(db, id, artifacts.EnrichmentOwner, artifacts.ArtifactTextEmbedding, req.ArtifactRecovery.correlation())
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Artifact recovery correlation is stale"})
			return
		}
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		var stageRequest models.ContentStageRequest
		var stageAttempt models.ContentStageAttempt
		stageKey := models.ContentStageNewsTextEmbedding
		if item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast {
			stageKey = models.ContentStagePodsTextEmbedding
			if req.ContentStage != nil {
				requestID, parseErr := uuid.Parse(req.ContentStage.RequestID)
				if parseErr != nil {
					return parseErr
				}
				var requestedStage models.ContentStageRequest
				if err := tx.Where("public_id=? AND content_item_id=?", requestID, id).First(&requestedStage).Error; err != nil {
					return err
				}
				if requestedStage.Stage == models.ContentStagePodsCaptionReembedding {
					stageKey = models.ContentStagePodsCaptionReembedding
				}
			}
		}
		if err := requireNormalStageCorrelation(tx, item, stageKey, req.ContentStage, req.ArtifactRecovery != nil || req.PipelineRepair != nil); err != nil {
			return err
		}
		if req.ContentStage != nil {
			var err error
			stageRequest, stageAttempt, err = contentstage.AuthorizeWriteback(tx, id, req.ContentStage.correlation(), stageKey)
			if err != nil {
				return err
			}
		}
		if req.PipelineRepair != nil {
			if err := pipeline.AuthorizeTextEmbeddingWriteback(tx, id, req.PipelineRepair.correlation()); err != nil {
				return err
			}
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", item.TenantID, item.PublicID).First(&item).Error; err != nil {
			return err
		}
		if err := checkContentLifecycleMutation(tx, item); err != nil {
			return err
		}
		vec := pgvector.NewVector(req.Embedding)
		item.Embedding = &vec
		item.EmbeddingModel = stampOrNil(req.Model)
		item.EmbeddingSpaceID = stampOrNil(req.SpaceID)
		item.EmbeddingProducerID = stampOrNil(req.ProducerID)
		if len(req.TopicTags) > 0 {
			item.TopicTags = req.TopicTags
		}
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		if err := appendItemProcessingEvent(tx, item, "text_embedding", "completed", "enrichment", "text_embedding_persisted", map[string]interface{}{"model": req.Model}); err != nil {
			return err
		}
		if req.ArtifactRecovery != nil {
			return artifacts.RecordPersistence(tx, recoveryRequest, recoveryAttempt, req.ArtifactRecovery.correlation(), map[string]any{"model": req.Model, "space_id": req.SpaceID})
		}
		if req.ContentStage != nil {
			return contentstage.RecordPersistence(tx, stageRequest, stageAttempt, req.ContentStage.correlation(), models.ContentStageOwnerEnrichment, req.ProducerID, map[string]any{"model": req.Model, "space_id": req.SpaceID, "producer_id": req.ProducerID})
		}
		return nil
	}); err != nil {
		if writeLifecycleConflict(c, err) {
			return
		}
		if req.PipelineRepair != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Pipeline repair embedding writeback is stale"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update embedding"})
		}
		return
	}

	// Now that the dense embedding exists, classify the article into a
	// first-class topic. Fire-and-forget — it calls Enrichment's LLM for new
	// topic labels and must not block the embedding write-back.
	laneMode, _ := contentstage.CutoverMode(db, item.TenantID, models.ContentStageLaneNews)
	if item.Type == models.ContentTypeNews && laneMode != models.ContentStageCutoverDurableRequired {
		go classifyContentTopic(db, item.PublicID)
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// InternalUpdateContentTopicTags handles PATCH
// /internal/content-items/:id/topic-tags. This is an optional, idempotent
// metadata write issued by Enrichment after the required embedding writeback
// has completed. It intentionally has no content-stage correlation: the
// embedding endpoint owns the required stage receipt, while tags are an
// eventual best-effort enrichment effect.
func InternalUpdateContentTopicTags(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateTopicTagsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	if len(req.TopicTags) > 5 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "At most 5 topic tags are allowed"})
		return
	}

	cleaned := make([]string, 0, len(req.TopicTags))
	seen := make(map[string]struct{}, len(req.TopicTags))
	for _, raw := range req.TopicTags {
		tag := strings.ToLower(strings.TrimSpace(raw))
		if tag == "" {
			continue
		}
		if len(tag) > 96 || strings.ContainsAny(tag, "\r\n") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Topic tags must be at most 96 characters and single-line"})
			return
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		cleaned = append(cleaned, tag)
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load content"})
		return
	}

	if len(cleaned) == 0 {
		// An empty result means the optional classifier found nothing (or was
		// skipped). Preserve existing tags instead of erasing useful metadata.
		c.JSON(http.StatusOK, gin.H{"success": true, "updated": false})
		return
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id=? AND public_id=?", item.TenantID, item.PublicID).
			First(&item).Error; err != nil {
			return err
		}
		if err := checkContentLifecycleMutation(tx, item); err != nil {
			return err
		}
		item.TopicTags = cleaned
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		return appendItemProcessingEvent(tx, item, "topic_tags", "completed", "enrichment", "topic_tags_persisted", map[string]interface{}{
			"tag_count": len(cleaned),
		})
	}); err != nil {
		if writeLifecycleConflict(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update topic tags"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "updated": true})
}

// InternalUpdateContentImageEmbedding handles PATCH /internal/content-items/:id/image-embedding.
// Stores a CLIP-ViT-B-32 image embedding (512-dim) on the content item.
// Independent from the text Embedding (1024-dim Qwen3) — both can coexist.
func InternalUpdateContentImageEmbedding(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalUpdateImageEmbeddingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if len(req.Embedding) != 512 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Image embedding must be 512-dim (got " +
				strconv.Itoa(len(req.Embedding)) + ")",
		})
		return
	}

	// Write fence (stage 10 §7) for the image space.
	if reason, blocked := fenceEmbeddingWrite(db, EmbeddingSpaceImage, spaceid.RecipeContentImage, req.SpaceID, req.ProducerID); blocked {
		c.JSON(http.StatusConflict, gin.H{"error": reason, "code": "writer_regression"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	var recoveryRequest models.ArtifactCoverageRequest
	var recoveryAttempt models.ArtifactCoverageAttempt
	if req.ArtifactRecovery != nil {
		recoveryRequest, recoveryAttempt, err = artifacts.AuthorizeWriteback(db, id, artifacts.MediaOwner, artifacts.ArtifactImageEmbedding, req.ArtifactRecovery.correlation())
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Artifact recovery correlation is stale"})
			return
		}
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		var stageRequest models.ContentStageRequest
		var stageAttempt models.ContentStageAttempt
		if err := requireNormalStageCorrelation(tx, item, models.ContentStagePodsImageEmbedding, req.ContentStage, req.ArtifactRecovery != nil); err != nil {
			return err
		}
		if req.ContentStage != nil {
			var err error
			stageRequest, stageAttempt, err = contentstage.AuthorizeWriteback(tx, id, req.ContentStage.correlation(), models.ContentStagePodsImageEmbedding)
			if err != nil {
				return err
			}
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", item.TenantID, item.PublicID).First(&item).Error; err != nil {
			return err
		}
		if err := checkContentLifecycleMutation(tx, item); err != nil {
			return err
		}
		vec := pgvector.NewVector(req.Embedding)
		item.ImageEmbedding = &vec
		item.ImageEmbeddingModel = stampOrNil(req.Model)
		item.ImageEmbeddingSpaceID = stampOrNil(req.SpaceID)
		item.ImageEmbeddingProducerID = stampOrNil(req.ProducerID)
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		if err := appendItemProcessingEvent(tx, item, "image_embedding", "completed", "media", "image_embedding_persisted", map[string]interface{}{"model": req.Model}); err != nil {
			return err
		}
		if req.ArtifactRecovery != nil {
			return artifacts.RecordPersistence(tx, recoveryRequest, recoveryAttempt, req.ArtifactRecovery.correlation(), map[string]any{"model": req.Model, "space_id": req.SpaceID})
		}
		if req.ContentStage != nil {
			return contentstage.RecordPersistence(tx, stageRequest, stageAttempt, req.ContentStage.correlation(), models.ContentStageOwnerMedia, req.ProducerID, map[string]any{"model": req.Model, "space_id": req.SpaceID})
		}
		return nil
	}); err != nil {
		if writeLifecycleConflict(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update image embedding"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// ─── Dense related-content retrieval endpoints ──────────────────────
//
// Two active internal endpoints support Enrichment-Service's /v1/related:
//   1. InternalGetContentEmbeddings — fetch the dense vector for
//      an anchor content_id so /v1/related can run dense kNN without
//      re-embedding what's already stored.
//   2. InternalKNNDense  — pgvector cosine kNN against `embedding`.
// InternalKNNSparse remains as a migration-compatibility endpoint only and
// is not part of the active retrieval path.
//
// All three are POST (kNN payloads carry 1024-dim or larger vectors that
// don't belong in query strings) except the embeddings fetch, which is GET.
// Filtering by canonical content kind, NEWS format, and excluded ids is built in.

type internalKNNDenseRequest struct {
	Embedding         []float32 `json:"embedding"`
	SpaceID           string    `json:"space_id"`
	TenantID          string    `json:"tenant_id"`
	NewsGenerationID  string    `json:"news_generation_id"`
	MediaGenerationID string    `json:"media_generation_id"`
	Types             []string  `json:"types"`       // optional — when empty, no type filter
	Formats           []string  `json:"formats"`     // optional NEWS format filter, independent of type
	K                 int       `json:"k"`           // required, >0
	ExcludeIDs        []string  `json:"exclude_ids"` // optional public_ids to skip
}

type internalKNNHit struct {
	ID     string  `json:"id"`   // public_id (UUID string)
	Type   string  `json:"type"` // canonical ContentType (NEWS, VIDEO, PODCAST)
	Format *string `json:"format,omitempty"`
	Score  float64 `json:"score"`
	// SourceName + PublishedAt let downstream ranking rules (source
	// diversity, freshness decay) run on the kNN results directly,
	// without a second round-trip to /internal/content-items/batch-text.
	// Critical for the rerank-disabled path where batch-text is skipped.
	SourceName  *string `json:"source_name,omitempty"`
	PublishedAt *string `json:"published_at,omitempty"`
}

type internalKNNResponse struct {
	Hits []internalKNNHit `json:"hits"`
}

type internalEmbeddingsResponse struct {
	Embedding         []float32 `json:"embedding"` // 1024 dense, null if missing
	EmbeddingSpaceID  string    `json:"embedding_space_id,omitempty"`
	TenantID          string    `json:"tenant_id"`
	NewsGenerationID  string    `json:"news_generation_id"`
	MediaGenerationID string    `json:"media_generation_id"`
}

// InternalGetContentEmbeddings handles GET /internal/content-items/:id/embeddings.
// Returns the dense vector for one content item so Enrichment /v1/related can
// skip re-embedding for an anchor.
func InternalGetContentEmbeddings(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var item models.ContentItem
	var tenantID string
	if err := db.Model(&models.ContentItem{}).Where("public_id = ?", id).Pluck("tenant_id", &tenantID).Error; err != nil || strings.TrimSpace(tenantID) == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	requestedTenant := strings.TrimSpace(c.Query("tenant_id"))
	requestedNews := strings.TrimSpace(c.Query("news_generation_id"))
	requestedMedia := strings.TrimSpace(c.Query("media_generation_id"))
	viewProvided := requestedTenant != "" || requestedNews != "" || requestedMedia != ""
	var view feedcontract.ServingView
	if viewProvided {
		view, err = parseInternalServingView(requestedTenant, requestedNews, requestedMedia)
		if err != nil || view.TenantID != tenantID {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid serving view for content tenant"})
			return
		}
	} else {
		var supported, complete bool
		view, supported, complete = feedcontract.LoadActiveServingView(db, tenantID)
		if !supported || !complete {
			c.JSON(http.StatusNotFound, gin.H{"error": "Content is not available in a complete active feed view"})
			return
		}
	}
	if err := feedcontract.ApplyServingViewMembership(db, db.Model(&models.ContentItem{}).
		Scopes(publicContentBaseQuery).
		Where("public_id = ?", id).
		Select("embedding", "embedding_space_id"), view).
		First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	resp := internalEmbeddingsResponse{TenantID: tenantID, NewsGenerationID: view.NewsGenerationID.String(), MediaGenerationID: view.MediaGenerationID.String()}
	if item.Embedding != nil {
		resp.Embedding = item.Embedding.Slice()
		if item.EmbeddingSpaceID != nil {
			resp.EmbeddingSpaceID = *item.EmbeddingSpaceID
		}
	}
	c.JSON(http.StatusOK, resp)
}

// InternalKNNDense handles POST /internal/content-items/knn.
// Runs cosine-similarity kNN against the `embedding` HNSW index added in
// migration 20260522000000_bge_m3_retrieval.sql.
func InternalKNNDense(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalKNNDenseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	if len(req.Embedding) != textEmbeddingDim {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Embedding must be " + strconv.Itoa(textEmbeddingDim) +
				"-dim (got " + strconv.Itoa(len(req.Embedding)) + ")",
		})
		return
	}
	if req.K <= 0 || req.K > 200 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "k must be in [1, 200]"})
		return
	}
	if strings.TrimSpace(req.SpaceID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "space_id is required for dense kNN"})
		return
	}
	view, err := parseInternalServingView(req.TenantID, req.NewsGenerationID, req.MediaGenerationID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	hits := runKNNQuery(db, "embedding", utils.PgvectorToLiteral(req.Embedding),
		req.SpaceID, req.Types, req.Formats, req.K, req.ExcludeIDs, view)
	c.JSON(http.StatusOK, internalKNNResponse{Hits: hits})
}

// InternalKNNSparse is a compatibility response for clients that have not yet
// moved to the Qwen dense-only retrieval endpoint.
func InternalKNNSparse(c *gin.Context) {
	c.JSON(http.StatusGone, gin.H{"error": "sparse retrieval was removed; use /content-items/knn"})
}

// ─── Slice B: batch text fetch for the reranker stage ────────────────
//
// Reranker needs candidate text. kNN handlers return only {id, type, score}
// to keep the search payload lean; this endpoint fans the resulting id list
// back out into the full (title, excerpt, body_text, source_name, published_at)
// tuple for the small post-RRF candidate set (typically top-30).

type internalBatchTextRequest struct {
	IDs               []string `json:"ids"`
	TenantID          string   `json:"tenant_id"`
	NewsGenerationID  string   `json:"news_generation_id"`
	MediaGenerationID string   `json:"media_generation_id"`
}

type internalBatchTextItem struct {
	ID          string  `json:"id"` // public_id (UUID string)
	Type        string  `json:"type"`
	Title       *string `json:"title"`
	Excerpt     *string `json:"excerpt"`
	BodyText    *string `json:"body_text"`
	SourceName  *string `json:"source_name"`
	PublishedAt *string `json:"published_at"` // ISO-8601, nil if missing
}

type internalBatchTextResponse struct {
	Items []internalBatchTextItem `json:"items"`
}

// Cap on a single batch — high enough to cover post-RRF candidate pools
// (RERANK_INPUT_K=30 by default) but low enough to bound payload size.
const batchTextMaxIDs = 200

// InternalBatchText handles POST /internal/content-items/batch-text.
// Returns text + metadata for the requested ids, used by Enrichment's
// reranker stage (Slice B). Order of items in the response is unspecified;
// caller looks them up by id.
func InternalBatchText(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var req internalBatchTextRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	if len(req.IDs) == 0 {
		c.JSON(http.StatusOK, internalBatchTextResponse{Items: []internalBatchTextItem{}})
		return
	}
	if len(req.IDs) > batchTextMaxIDs {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "ids exceeds maximum batch size of " + strconv.Itoa(batchTextMaxIDs),
		})
		return
	}
	view, err := parseInternalServingView(req.TenantID, req.NewsGenerationID, req.MediaGenerationID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Parse UUIDs; skip malformed ones silently. Caller may interleave
	// invalid ids without us bailing on the whole batch.
	parsed := make([]uuid.UUID, 0, len(req.IDs))
	for _, s := range req.IDs {
		if u, err := uuid.Parse(s); err == nil {
			parsed = append(parsed, u)
		}
	}
	if len(parsed) == 0 {
		c.JSON(http.StatusOK, internalBatchTextResponse{Items: []internalBatchTextItem{}})
		return
	}

	type row struct {
		PublicID    uuid.UUID
		Type        string
		Title       *string
		Excerpt     *string
		BodyText    *string
		SourceName  *string
		PublishedAt *time.Time
	}
	var rows []row
	if err := feedcontract.ApplyServingViewMembership(db, db.Model(&models.ContentItem{}).
		Scopes(publicContentBaseQuery).
		Where("public_id IN ?", parsed).
		Where(`type <> 'NEWS' OR COALESCE(news_retention_state, 'full') = 'full' OR news_feed_role IN ('lead', 'representative')`), view).
		Select("public_id, type, title, excerpt, body_text, source_name, published_at").
		Scan(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch batch text"})
		return
	}

	items := make([]internalBatchTextItem, 0, len(rows))
	for _, r := range rows {
		var publishedAtStr *string
		if r.PublishedAt != nil {
			s := r.PublishedAt.UTC().Format(time.RFC3339)
			publishedAtStr = &s
		}
		items = append(items, internalBatchTextItem{
			ID:          r.PublicID.String(),
			Type:        r.Type,
			Title:       r.Title,
			Excerpt:     r.Excerpt,
			BodyText:    r.BodyText,
			SourceName:  r.SourceName,
			PublishedAt: publishedAtStr,
		})
	}
	c.JSON(http.StatusOK, internalBatchTextResponse{Items: items})
}

// ── GET /internal/content-items/missing-embedding ───────────
//
// Returns READY items that have no dense embedding yet (oldest first), so the
// Aggregation reconciliation sweep can re-enqueue embedding-only AI jobs. Same
// query shape as the admin GetMissingEnrichments, but on the service-token
// internal API and returning the text fields needed to rebuild the embedding
// input. Reuses internalBatchTextItem/Response (same field set).
func InternalListMissingEmbedding(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	type row struct {
		PublicID    uuid.UUID
		Type        string
		Title       *string
		Excerpt     *string
		BodyText    *string
		SourceName  *string
		PublishedAt *time.Time
	}
	var rows []row
	// "Missing" = no vector at all, OR a vector without model provenance
	// (written by a pre-provenance / wrong-deployment service) — both get
	// re-embedded so the corpus converges on the current embedder.
	if err := db.Model(&models.ContentItem{}).
		Where("status = ?", models.ContentStatusReady).
		Where("type <> ? OR COALESCE(news_retention_state, 'full') = 'full'", models.ContentTypeNews).
		Where("embedding IS NULL OR embedding_model IS NULL").
		Order("created_at ASC").
		Limit(limit).
		Select("public_id, type, title, excerpt, body_text, source_name, published_at").
		Scan(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list missing-embedding items"})
		return
	}

	items := make([]internalBatchTextItem, 0, len(rows))
	for _, r := range rows {
		var publishedAtStr *string
		if r.PublishedAt != nil {
			s := r.PublishedAt.UTC().Format(time.RFC3339)
			publishedAtStr = &s
		}
		items = append(items, internalBatchTextItem{
			ID:          r.PublicID.String(),
			Type:        r.Type,
			Title:       r.Title,
			Excerpt:     r.Excerpt,
			BodyText:    r.BodyText,
			SourceName:  r.SourceName,
			PublishedAt: publishedAtStr,
		})
	}
	c.JSON(http.StatusOK, internalBatchTextResponse{Items: items})
}

// InternalReconcileArtifactCompleteStatuses repairs compatibility-mode rows
// that are marked FAILED even though the artifact owner already persisted the
// required result. It is intentionally CMS-local: no model call, queue replay,
// or provider access is performed by this endpoint.
func InternalReconcileArtifactCompleteStatuses(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var candidates []models.ContentItem
	if err := db.Where("status = ?", models.ContentStatusFailed).
		Where("(type = ? AND embedding IS NOT NULL) OR (type IN ? AND embedding IS NOT NULL AND playback_url IS NOT NULL AND duration_sec IS NOT NULL)", models.ContentTypeNews, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Order("updated_at ASC").Limit(limit).Find(&candidates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list artifact-complete failed items"})
		return
	}
	reconciled := 0
	for index := range candidates {
		itemID := candidates[index].PublicID
		if err := db.Transaction(func(tx *gorm.DB) error {
			var item models.ContentItem
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND status=?", itemID, models.ContentStatusFailed).First(&item).Error; err != nil {
				return nil
			}
			if !contentItemHasRequiredArtifact(&item) {
				return nil
			}
			if err := checkContentLifecycleMutation(tx, item); err != nil {
				return err
			}
			item.Status = models.ContentStatusReady
			metadata := map[string]interface{}{}
			if len(item.Metadata) > 0 {
				_ = json.Unmarshal(item.Metadata, &metadata)
			}
			metadata["lifecycle_reconciliation"] = map[string]interface{}{
				"reason":        "required_artifact_present",
				"reconciled_at": time.Now().UTC().Format(time.RFC3339),
			}
			if raw, err := json.Marshal(metadata); err == nil {
				item.Metadata = datatypes.JSON(raw)
			}
			setFeedUnitDurationBucket(&item)
			if err := tx.Save(&item).Error; err != nil {
				return err
			}
			if err := feedstate.AttachReadyNewsStory(tx, item); err != nil {
				return err
			}
			if err := feedstate.SyncMediaMembership(tx, item); err != nil {
				return err
			}
			if err := appendItemProcessingEvent(tx, item, "content_status", "completed", "cms", "artifact_complete_reconciled", map[string]interface{}{"status": string(item.Status)}); err != nil {
				return err
			}
			reconciled++
			return nil
		}); err != nil {
			if writeLifecycleConflict(c, err) {
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to reconcile artifact-complete status"})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"reconciled": reconciled, "scanned": len(candidates)})
}

// runKNNQuery is the shared dense-vector kNN body. The RRF fusion in
// Enrichment only uses rank, not the raw cosine score.
func runKNNQuery(db *gorm.DB, column, vecLiteral, spaceID string, types, formats []string, k int, excludeIDs []string, view feedcontract.ServingView) []internalKNNHit {
	q := feedcontract.ApplyServingViewMembership(db, db.Model(&models.ContentItem{}).
		Scopes(publicContentBaseQuery).
		Where(`type <> 'NEWS' OR COALESCE(news_retention_state, 'full') = 'full' OR news_feed_role IN ('lead', 'representative')`).
		Where(column+" IS NOT NULL"), view)
	if column == "embedding" {
		q = q.Where("embedding_space_id = ?", spaceID)
	}

	if len(types) > 0 {
		q = q.Where("type IN ?", types)
	}
	if len(formats) > 0 {
		q = q.Where("format IN ?", formats)
	}
	if len(excludeIDs) > 0 {
		// Parse UUIDs once; skip invalid ones silently.
		parsed := make([]uuid.UUID, 0, len(excludeIDs))
		for _, s := range excludeIDs {
			if u, err := uuid.Parse(s); err == nil {
				parsed = append(parsed, u)
			}
		}
		if len(parsed) > 0 {
			q = q.Where("public_id NOT IN ?", parsed)
		}
	}

	type row struct {
		PublicID    uuid.UUID
		Type        string
		Format      *string
		Distance    float64
		SourceName  *string
		PublishedAt *time.Time
	}
	var rows []row

	// Distance via the cosine operator; convert to score = 1 - distance so
	// higher is better. Both columns + their HNSW indexes are guarded by
	// `<column> IS NOT NULL`, so the planner uses the index. source_name
	// + published_at are pulled so callers can run freshness + diversity
	// rules without a second round-trip.
	err := q.Select("public_id, type, format, source_name, published_at, (" + column + " <=> '" + vecLiteral + "') AS distance").
		Order(column + " <=> '" + vecLiteral + "'").
		Limit(k).
		Scan(&rows).Error
	if err != nil {
		return nil
	}

	hits := make([]internalKNNHit, 0, len(rows))
	for _, r := range rows {
		var publishedAt *string
		if r.PublishedAt != nil {
			s := r.PublishedAt.UTC().Format(time.RFC3339)
			publishedAt = &s
		}
		hits = append(hits, internalKNNHit{
			ID:          r.PublicID.String(),
			Type:        r.Type,
			Format:      r.Format,
			Score:       1.0 - r.Distance,
			SourceName:  r.SourceName,
			PublishedAt: publishedAt,
		})
	}
	return hits
}

func parseInternalServingView(tenantID, newsGenerationID, mediaGenerationID string) (feedcontract.ServingView, error) {
	if strings.TrimSpace(tenantID) == "" {
		return feedcontract.ServingView{}, errors.New("tenant_id is required")
	}
	newsID, err := uuid.Parse(strings.TrimSpace(newsGenerationID))
	if err != nil {
		return feedcontract.ServingView{}, errors.New("news_generation_id must be a UUID")
	}
	mediaID, err := uuid.Parse(strings.TrimSpace(mediaGenerationID))
	if err != nil {
		return feedcontract.ServingView{}, errors.New("media_generation_id must be a UUID")
	}
	view := feedcontract.ServingView{TenantID: tenantID, NewsGenerationID: newsID, MediaGenerationID: mediaID}
	return view, feedcontract.ValidateServingView(view)
}

// InternalLinkTranscript handles PATCH /internal/content-items/:id/transcript
func InternalLinkTranscript(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var req internalLinkTranscriptRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.TranscriptID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "transcript_id is required"})
		return
	}

	transcriptUUID, err := uuid.Parse(req.TranscriptID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid transcript ID"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	var recoveryRequest models.ArtifactCoverageRequest
	var recoveryAttempt models.ArtifactCoverageAttempt
	if req.ArtifactRecovery != nil {
		recoveryRequest, recoveryAttempt, err = artifacts.AuthorizeWriteback(db, id, artifacts.MediaOwner, artifacts.ArtifactTranscript, req.ArtifactRecovery.correlation())
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "Artifact recovery correlation is stale"})
			return
		}
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		var stageRequest models.ContentStageRequest
		var stageAttempt models.ContentStageAttempt
		if err := requireNormalStageCorrelation(tx, item, models.ContentStagePodsTranscript, req.ContentStage, req.ArtifactRecovery != nil); err != nil {
			return err
		}
		if req.ContentStage != nil {
			var err error
			stageRequest, stageAttempt, err = contentstage.AuthorizeWriteback(tx, id, req.ContentStage.correlation(), models.ContentStagePodsTranscript)
			if err != nil {
				return err
			}
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", item.TenantID, item.PublicID).First(&item).Error; err != nil {
			return err
		}
		if err := checkContentLifecycleMutation(tx, item); err != nil {
			return err
		}
		item.TranscriptID = &transcriptUUID
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		if err := appendItemProcessingEvent(tx, item, "transcript", "completed", "media", "transcript_linked", map[string]interface{}{"transcript_id": transcriptUUID.String()}); err != nil {
			return err
		}
		if req.ArtifactRecovery != nil {
			return artifacts.RecordPersistence(tx, recoveryRequest, recoveryAttempt, req.ArtifactRecovery.correlation(), map[string]any{"transcript_id": transcriptUUID.String()})
		}
		if req.ContentStage != nil {
			return contentstage.RecordPersistence(tx, stageRequest, stageAttempt, req.ContentStage.correlation(), models.ContentStageOwnerMedia, transcriptUUID.String(), map[string]any{"transcript_id": transcriptUUID.String()})
		}
		return nil
	}); err != nil {
		if writeLifecycleConflict(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to link transcript"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

type internalListContentItemResponse struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Source      string                 `json:"source"`
	Status      string                 `json:"status"`
	OriginalURL string                 `json:"original_url"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// InternalListContentItems handles GET /internal/content-items
// Supports ?status=FAILED&source=TELEGRAM&ids=a,b&limit=100&page=1
func InternalListContentItems(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	status := strings.ToUpper(strings.TrimSpace(c.Query("status")))
	source := strings.ToUpper(strings.TrimSpace(c.Query("source")))
	rawIDs := strings.TrimSpace(c.Query("ids"))

	limit := 100
	if l, err := strconv.Atoi(c.Query("limit")); err == nil && l > 0 && l <= 500 {
		limit = l
	}
	page := 1
	if p, err := strconv.Atoi(c.Query("page")); err == nil && p > 0 {
		page = p
	}
	offset := (page - 1) * limit

	query := db.Model(&models.ContentItem{})
	if status != "" {
		query = query.Where("status = ?", status)
	}
	if source != "" {
		query = query.Where("source = ?", source)
	}
	if rawIDs != "" {
		ids := []uuid.UUID{}
		for _, raw := range strings.Split(rawIDs, ",") {
			id, err := uuid.Parse(strings.TrimSpace(raw))
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ids parameter"})
				return
			}
			ids = append(ids, id)
		}
		if len(ids) > 0 {
			query = query.Where("public_id IN ?", ids)
		}
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count content items"})
		return
	}

	var items []models.ContentItem
	if err := query.Offset(offset).Limit(limit).Order("created_at DESC").Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list content items"})
		return
	}

	data := make([]internalListContentItemResponse, 0, len(items))
	for _, item := range items {
		var meta map[string]interface{}
		if item.Metadata != nil {
			_ = json.Unmarshal(item.Metadata, &meta)
		}
		originalURL := ""
		if item.OriginalURL != nil {
			originalURL = *item.OriginalURL
		}
		data = append(data, internalListContentItemResponse{
			ID:          item.PublicID.String(),
			Type:        string(item.Type),
			Source:      string(item.Source),
			Status:      string(item.Status),
			OriginalURL: originalURL,
			Metadata:    meta,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  data,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}

// InternalGetContentItem handles GET /internal/content-items/:id
// Returns the fields the Aggregation quality worker needs to drive a
// re-encode: tier, current media URL, version, active profile id (for
// idempotency), current bitrate and duration. Auth: InternalAuthMiddleware.
func InternalGetContentItem(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return
	}
	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	// Serialize published_at as RFC3339 UTC so Enrichment's ISO parser
	// (datetime.fromisoformat after a Z→+00:00 swap) gets an aware datetime.
	var publishedAt *string
	if item.PublishedAt != nil {
		s := item.PublishedAt.UTC().Format(time.RFC3339)
		publishedAt = &s
	}
	var servingView any
	var publicID uuid.UUID
	if view, supported, complete := feedcontract.LoadActiveServingView(db, item.TenantID); supported && complete {
		query := publicContentBaseQuery(db).Model(&models.ContentItem{}).
			Where("content_items.tenant_id = ? AND content_items.public_id = ?", item.TenantID, item.PublicID).
			Select("content_items.public_id")
		if err := feedcontract.ApplyServingViewMembership(db, query, view).Take(&publicID).Error; err == nil {
			servingView = gin.H{
				"tenant_id":           view.TenantID,
				"news_generation_id":  view.NewsGenerationID.String(),
				"media_generation_id": view.MediaGenerationID.String(),
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"id":        item.PublicID.String(),
		"tenant_id": item.TenantID,
		"status":    string(item.Status),
		// Content type (TWEET/ARTICLE/…) — distinct from source_type below.
		// FeedNewsService anchors read this; without it the slide anchor's
		// type field is the empty string.
		"type": string(item.Type),
		// source_type is required by the quality re-encode auto-resolve path
		// — without it the resolver can never pick a source-scoped ingest
		// profile (e.g. "YouTube items use mobile-720p"). Stringified so
		// callers can match against the string values in QualityProfile.SourceType.
		"source_type":                  string(item.Source),
		"title":                        item.Title,
		"excerpt":                      item.Excerpt,
		"content_language":             item.ContentLanguage,
		"source_name":                  item.SourceName,
		"published_at":                 publishedAt,
		"serving_view":                 servingView,
		"media_url":                    item.MediaURL,
		"thumbnail_url":                item.ThumbnailURL,
		"storage_tier":                 item.StorageTier, // nil = primary
		"media_version":                item.MediaVersion,
		"file_size_bytes":              item.FileSizeBytes,
		"current_quality_profile_id":   item.CurrentQualityProfileID,
		"current_bitrate_kbps":         item.CurrentBitrateKbps,
		"duration_sec":                 item.DurationSec,
		"transcript_id":                item.TranscriptID,
		"source_feed_url":              item.SourceFeedURL,
		"parent_content_item_id":       item.ParentContentItemID,
		"is_feed_unit":                 item.IsFeedUnit,
		"feed_visibility":              item.FeedVisibility,
		"chaptering_status":            item.ChapteringStatus,
		"media_suitability":            item.MediaSuitability,
		"media_suitability_confidence": item.MediaSuitabilityConfidence,
		"media_suitability_reasons":    item.MediaSuitabilityReasons,
		"has_embedding":                item.Embedding != nil,
		"metadata":                     item.Metadata,
	})
}
