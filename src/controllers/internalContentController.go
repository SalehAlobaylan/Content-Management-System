package controllers

import (
	"content-management-system/src/artifacts"
	"content-management-system/src/contentstage"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"content-management-system/src/pipeline"
	"content-management-system/src/spaceid"
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
	IdempotencyKey       string                 `json:"idempotency_key"`
	Type                 string                 `json:"type"`
	Format               *string                `json:"format"`
	Source               string                 `json:"source"`
	Status               string                 `json:"status"`
	Title                string                 `json:"title"`
	BodyText             *string                `json:"body_text"`
	Excerpt              *string                `json:"excerpt"`
	ContentLanguage      *string                `json:"content_language"`
	Author               *string                `json:"author"`
	SourceName           string                 `json:"source_name"`
	SourceFeedURL        *string                `json:"source_feed_url"`
	TenantID             string                 `json:"tenant_id"`
	ContentSourceID      string                 `json:"content_source_id"`
	SourceRunRequestID   string                 `json:"source_run_request_id"`
	OriginalURL          string                 `json:"original_url"`
	MediaURL             *string                `json:"media_url"`
	ThumbnailURL         *string                `json:"thumbnail_url"`
	DurationSec          *int                   `json:"duration_sec"`
	TopicTags            []string               `json:"topic_tags"`
	Metadata             map[string]interface{} `json:"metadata"`
	PublishedAt          *string                `json:"published_at"`
	RecoveryRunID        *string                `json:"recovery_run_id"`
	RecoveryManifestHash string                 `json:"recovery_manifest_hash"`
}

type internalCreateContentItemResponse struct {
	ID           string `json:"id"`
	TenantID     string `json:"tenant_id"`
	Status       string `json:"status"`
	Created      bool   `json:"created"`
	Retired      bool   `json:"retired,omitempty"`
	CreatedAt    string `json:"created_at"`
	DeliveryMode string `json:"delivery_mode"`
	contentstage.ManifestDisposition
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
		var stageRequest models.ContentStageRequest
		var stageAttempt models.ContentStageAttempt
		var item models.ContentItem
		if err := tx.Where("public_id=?", id).First(&item).Error; err != nil {
			return err
		}
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
	MediaURL              *string                  `json:"media_url"`
	ThumbnailURL          *string                  `json:"thumbnail_url"`
	DurationSec           *int                     `json:"duration_sec"`
	FileSizeBytes         *int64                   `json:"file_size_bytes"`
	StorageTier           *string                  `json:"storage_tier"`
	PlaybackURL           *string                  `json:"playback_url"`
	PlaybackType          *string                  `json:"playback_type"`
	FallbackPlaybackURL   *string                  `json:"fallback_playback_url"`
	HasVideo              *bool                    `json:"has_video"`
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

	// Identity resolution is tenant-scoped. A matching provider key in another
	// tenant must never expose or mutate that tenant's item or stage evidence.
	var existing models.ContentItem
	if err := db.Where("tenant_id = ? AND idempotency_key = ?", lineageTenantID, idempotencyKey).First(&existing).Error; err == nil {
		var requests []models.ContentStageRequest
		disposition := "no_change"
		if manifestErr := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=?", existing.PublicID).First(&existing).Error; err != nil {
				return err
			}
			previousDigest := ""
			if existing.ProcessingInputDigest != nil {
				previousDigest = *existing.ProcessingInputDigest
			}
			// The idempotency identity is stable, but source observations may carry
			// corrected text or a replaced media reference. Compare those inputs in
			// CMS; Redis and Aggregation must not guess whether work is current.
			existing.Title = &req.Title
			existing.BodyText = req.BodyText
			existing.Excerpt = req.Excerpt
			existing.ContentLanguage = normalizeContentLanguage(req.ContentLanguage)
			existing.SourceFeedURL = req.SourceFeedURL
			existing.OriginalURL = &req.OriginalURL
			// media_url, thumbnail_url, and duration_sec are produced artifacts
			// after materialization; duplicate intake must never overwrite them
			// with the original provider reference.
			if err := tx.Save(&existing).Error; err != nil {
				return err
			}
			var changed bool
			var reconcileErr error
			requests, changed, reconcileErr = contentstage.ReconcileManifest(tx, &existing, previousDigest)
			if changed {
				disposition = "changed"
			}
			return reconcileErr
		}); manifestErr != nil {
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
			CreatedAt:           existing.CreatedAt.UTC().Format(time.RFC3339),
			DeliveryMode:        deliveryMode,
			ManifestDisposition: contentstage.SummarizeManifest(requests, existing.ProcessingGeneration, disposition),
		})
		return
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check idempotency"})
		return
	}

	var publishedAt *time.Time
	if req.PublishedAt != nil && *req.PublishedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, *req.PublishedAt); err == nil {
			publishedAt = &parsed
		}
	}

	metadataJSON, _ := json.Marshal(req.Metadata)
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
		}
	}

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

	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&item).Error; err != nil {
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create content item"})
		return
	}

	var stageRequests []models.ContentStageRequest
	_ = db.Where("tenant_id=? AND content_item_id=? AND processing_generation=?", item.TenantID, item.PublicID, item.ProcessingGeneration).Order("created_at").Find(&stageRequests).Error
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
		CreatedAt:           item.CreatedAt.UTC().Format(time.RFC3339),
		DeliveryMode:        deliveryMode,
		ManifestDisposition: contentstage.SummarizeManifest(stageRequests, item.ProcessingGeneration, "created"),
	})
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
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		_, manifestChanged, err := contentstage.ReconcileManifest(tx, &item, previousDigest)
		changed = manifestChanged
		return err
	}); err != nil {
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
	item.Status = models.ContentStatus(strings.ToUpper(req.Status))
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
		applyArtifactRequest(&item, req)
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		if err := feedstate.SyncMediaMembership(tx, item); err != nil {
			return err
		}
		if req.ContentStage != nil {
			artifactDigest := contentstage.ItemInputDigest(item)
			if err := contentstage.RecordPersistence(tx, stageRequest, stageAttempt, req.ContentStage.correlation(), models.ContentStageOwnerAggregationPods, artifactDigest, map[string]any{"playback_ready": item.PlaybackURL != nil, "duration_sec": item.DurationSec}); err != nil {
				return err
			}
		}
		return appendItemProcessingEvent(tx, item, "media_artifacts", "completed", "aggregation", "media_artifacts_persisted", map[string]interface{}{"playback_ready": item.PlaybackURL != nil, "has_thumbnail": item.ThumbnailURL != nil})
	}); err != nil {
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
	Embedding  []float32 `json:"embedding"`
	SpaceID    string    `json:"space_id"`
	Types      []string  `json:"types"`       // optional — when empty, no type filter
	Formats    []string  `json:"formats"`     // optional NEWS format filter, independent of type
	K          int       `json:"k"`           // required, >0
	ExcludeIDs []string  `json:"exclude_ids"` // optional public_ids to skip
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
	Embedding        []float32 `json:"embedding"` // 1024 dense, null if missing
	EmbeddingSpaceID string    `json:"embedding_space_id,omitempty"`
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
	if err := db.Where("public_id = ?", id).
		Select("embedding", "embedding_space_id").
		First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}
	resp := internalEmbeddingsResponse{}
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

	hits := runKNNQuery(db, "embedding", utils.PgvectorToLiteral(req.Embedding),
		req.SpaceID, req.Types, req.Formats, req.K, req.ExcludeIDs)
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
	IDs []string `json:"ids"`
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
	if err := db.Model(&models.ContentItem{}).
		Where("public_id IN ?", parsed).
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

// runKNNQuery is the shared dense-vector kNN body. The RRF fusion in
// Enrichment only uses rank, not the raw cosine score.
func runKNNQuery(db *gorm.DB, column, vecLiteral, spaceID string, types, formats []string, k int, excludeIDs []string) []internalKNNHit {
	q := db.Model(&models.ContentItem{}).
		Where("status = ?", models.ContentStatusReady).
		Where("type <> ? OR COALESCE(news_retention_state, 'full') = 'full'", models.ContentTypeNews).
		Where(column + " IS NOT NULL")
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
		"metadata":                     item.Metadata,
	})
}
