package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	manifestStateUploading       = "uploading"
	manifestStateUploaded        = "uploaded"
	manifestStateVerified        = "verified"
	manifestStateActive          = "active"
	manifestStateCleanupEligible = "cleanup_eligible"
	manifestStateDeleted         = "deleted"
	manifestStateUncertain       = "uncertain"
	manifestStateFailed          = "failed"
	unitStateQueued              = "queued"
	unitStateClaimed             = "claimed"
	unitStateRunning             = "running"
	unitStateVerifying           = "verifying"
	unitStateVerified            = "verified"
	unitStateDeferred            = "deferred"
	unitStateUncertain           = "uncertain"
	unitStateFailed              = "failed"
	unitStateSuperseded          = "superseded"
)

func longFormUUID(raw string, required bool) (*uuid.UUID, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		if required {
			return nil, errors.New("uuid is required")
		}
		return nil, nil
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return nil, errors.New("invalid uuid")
	}
	return &id, nil
}

func longFormJSON(value any) datatypes.JSON {
	if value == nil {
		return datatypes.JSON([]byte(`{}`))
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return datatypes.JSON([]byte(`{}`))
	}
	return datatypes.JSON(raw)
}

func longFormDigest(value any) string {
	raw, _ := json.Marshal(value)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

type artifactManifestRequest struct {
	TenantID                   string         `json:"tenant_id"`
	ContentItemID              string         `json:"content_item_id"`
	ParentContentItemID        string         `json:"parent_content_item_id"`
	AtomizationGenerationID    string         `json:"atomization_generation_id"`
	AtomizationChapterUnitID   string         `json:"atomization_chapter_unit_id"`
	TranscriptionGenerationID  string         `json:"transcription_generation_id"`
	TranscriptionSegmentUnitID string         `json:"transcription_segment_unit_id"`
	AttemptID                  string         `json:"attempt_id"`
	ArtifactRole               string         `json:"artifact_role"`
	PackageManifestID          string         `json:"package_manifest_id"`
	StorageTier                string         `json:"storage_tier"`
	Bucket                     string         `json:"bucket"`
	ObjectKey                  string         `json:"object_key"`
	PublicURL                  string         `json:"public_url"`
	ContentType                string         `json:"content_type"`
	CacheControl               string         `json:"cache_control"`
	SizeBytes                  int64          `json:"size_bytes"`
	ETag                       string         `json:"etag"`
	SHA256                     string         `json:"sha256"`
	DurationMs                 *int64         `json:"duration_ms"`
	CreatorRole                string         `json:"creator_role"`
	ProducerEventID            string         `json:"producer_event_id"`
	FenceToken                 string         `json:"fence_token"`
	InputDigest                string         `json:"input_digest"`
	RecoveryClass              string         `json:"recovery_class"`
	VerificationEvidence       map[string]any `json:"verification_evidence"`
	TerminalProof              map[string]any `json:"terminal_proof"`
}

func InternalCreateArtifactManifest(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req artifactManifestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid manifest"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	role := strings.TrimSpace(req.ArtifactRole)
	allowedRoles := map[string]bool{"source": true, "analysis_audio": true, "chapter_media": true, "chapter_hls": true, "thumbnail": true, "transcript_segment": true, "playback_audio": true, "playback_mp4": true, "delivery_audio": true, "delivery_progressive": true, "hls_master": true, "hls_playlist": true, "hls_init": true, "hls_segment": true}
	if !allowedRoles[role] || strings.TrimSpace(req.Bucket) == "" || strings.TrimSpace(req.ObjectKey) == "" || strings.TrimSpace(req.CreatorRole) == "" || strings.TrimSpace(req.InputDigest) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "artifact role, bucket, object_key, creator_role, and input_digest are required"})
		return
	}
	producer, err := longFormUUID(req.ProducerEventID, true)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	contentID, err := longFormUUID(req.ContentItemID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content_item_id"})
		return
	}
	parentID, err := longFormUUID(req.ParentContentItemID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid parent_content_item_id"})
		return
	}
	atomGen, err := longFormUUID(req.AtomizationGenerationID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid atomization_generation_id"})
		return
	}
	atomUnit, err := longFormUUID(req.AtomizationChapterUnitID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid atomization_chapter_unit_id"})
		return
	}
	transGen, err := longFormUUID(req.TranscriptionGenerationID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transcription_generation_id"})
		return
	}
	transUnit, err := longFormUUID(req.TranscriptionSegmentUnitID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transcription_segment_unit_id"})
		return
	}
	attempt, err := longFormUUID(req.AttemptID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid attempt_id"})
		return
	}
	fence, err := longFormUUID(req.FenceToken, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid fence_token"})
		return
	}
	packageManifest, err := longFormUUID(req.PackageManifestID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid package_manifest_id"})
		return
	}
	manifest := models.MediaArtifactManifest{PublicID: uuid.New(), TenantID: tenant, ContentItemID: contentID, ParentContentItemID: parentID, AtomizationGenerationID: atomGen, AtomizationChapterUnitID: atomUnit, TranscriptionGenerationID: transGen, TranscriptionSegmentUnitID: transUnit, AttemptID: attempt, ArtifactRole: role, PackageManifestID: packageManifest, StorageTier: strings.TrimSpace(req.StorageTier), Bucket: strings.TrimSpace(req.Bucket), ObjectKey: strings.TrimSpace(req.ObjectKey), PublicURL: strings.TrimSpace(req.PublicURL), ContentType: strings.TrimSpace(req.ContentType), CacheControl: strings.TrimSpace(req.CacheControl), SizeBytes: req.SizeBytes, ETag: strings.TrimSpace(req.ETag), SHA256: strings.TrimSpace(req.SHA256), DurationMs: req.DurationMs, CreatorRole: strings.TrimSpace(req.CreatorRole), ProducerEventID: *producer, FenceToken: fence, InputDigest: strings.TrimSpace(req.InputDigest), State: manifestStateUploading, RecoveryClass: strings.TrimSpace(req.RecoveryClass), VerificationEvidence: longFormJSON(req.VerificationEvidence), TerminalProof: longFormJSON(req.TerminalProof)}
	if manifest.StorageTier == "" {
		manifest.StorageTier = "primary"
	}
	if manifest.RecoveryClass == "" {
		manifest.RecoveryClass = "recoverable"
	}
	if err := validateArtifactManifestOwnership(db, &manifest); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "manifest ownership validation failed", "reason": err.Error()})
		return
	}
	candidate := manifest
	result := db.Where("tenant_id=? AND storage_tier=? AND bucket=? AND object_key=?", tenant, candidate.StorageTier, candidate.Bucket, candidate.ObjectKey).First(&manifest)
	if result.Error == nil {
		if manifest.State == manifestStateDeleted {
			c.JSON(http.StatusConflict, gin.H{"error": "immutable artifact identity was already deleted"})
			return
		}
		if !artifactManifestMatchesImmutableIntent(&manifest, &candidate) {
			c.JSON(http.StatusConflict, gin.H{"error": "immutable artifact identity ownership or input mismatch"})
			return
		}
		c.JSON(http.StatusOK, manifest)
		return
	}
	if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "manifest lookup failed"})
		return
	}
	if err := db.Create(&candidate).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "manifest creation rejected"})
		return
	}
	c.JSON(http.StatusOK, candidate)
}

func sameLongFormUUID(left, right *uuid.UUID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// An object key is immutable ownership, not a reusable output slot. Retries
// may carry a new producer-event ID, but every identity that could change the
// effect (owner, generation/unit, attempt/fence, role, or input) must match.
func artifactManifestMatchesImmutableIntent(existing, candidate *models.MediaArtifactManifest) bool {
	if existing.TenantID != candidate.TenantID ||
		existing.StorageTier != candidate.StorageTier ||
		existing.Bucket != candidate.Bucket ||
		existing.ObjectKey != candidate.ObjectKey ||
		existing.ArtifactRole != candidate.ArtifactRole ||
		existing.CreatorRole != candidate.CreatorRole ||
		existing.InputDigest != candidate.InputDigest ||
		!sameLongFormUUID(existing.ContentItemID, candidate.ContentItemID) ||
		!sameLongFormUUID(existing.ParentContentItemID, candidate.ParentContentItemID) ||
		!sameLongFormUUID(existing.AtomizationGenerationID, candidate.AtomizationGenerationID) ||
		!sameLongFormUUID(existing.AtomizationChapterUnitID, candidate.AtomizationChapterUnitID) ||
		!sameLongFormUUID(existing.TranscriptionGenerationID, candidate.TranscriptionGenerationID) ||
		!sameLongFormUUID(existing.TranscriptionSegmentUnitID, candidate.TranscriptionSegmentUnitID) ||
		!sameLongFormUUID(existing.AttemptID, candidate.AttemptID) ||
		!sameLongFormUUID(existing.FenceToken, candidate.FenceToken) {
		return false
	}
	if existing.SizeBytes > 0 && candidate.SizeBytes > 0 && existing.SizeBytes != candidate.SizeBytes {
		return false
	}
	return existing.SHA256 == "" || candidate.SHA256 == "" || existing.SHA256 == candidate.SHA256
}

func validateArtifactManifestOwnership(db *gorm.DB, manifest *models.MediaArtifactManifest) error {
	checkItem := func(id *uuid.UUID) error {
		if id == nil {
			return nil
		}
		var item models.ContentItem
		if err := db.Where("public_id=?", *id).First(&item).Error; err != nil {
			return fmt.Errorf("content owner not found")
		}
		if item.TenantID != manifest.TenantID {
			return fmt.Errorf("content owner tenant mismatch")
		}
		return nil
	}
	if err := checkItem(manifest.ContentItemID); err != nil {
		return err
	}
	if err := checkItem(manifest.ParentContentItemID); err != nil {
		return err
	}
	if manifest.AtomizationGenerationID != nil {
		var generation models.AtomizationGeneration
		if err := db.Where("public_id=? AND tenant_id=?", *manifest.AtomizationGenerationID, manifest.TenantID).First(&generation).Error; err != nil {
			return fmt.Errorf("atomization generation owner not found")
		}
		if manifest.ParentContentItemID != nil && generation.ParentContentItemID != *manifest.ParentContentItemID {
			return fmt.Errorf("atomization generation parent mismatch")
		}
	}
	if manifest.AtomizationChapterUnitID != nil {
		var unit models.AtomizationChapterUnit
		if err := db.Where("public_id=? AND tenant_id=?", *manifest.AtomizationChapterUnitID, manifest.TenantID).First(&unit).Error; err != nil {
			return fmt.Errorf("atomization unit owner not found")
		}
		if manifest.AtomizationGenerationID == nil || unit.GenerationID != *manifest.AtomizationGenerationID {
			return fmt.Errorf("atomization unit generation mismatch")
		}
		if manifest.FenceToken != nil && (unit.FenceToken == nil || *unit.FenceToken != *manifest.FenceToken) {
			return fmt.Errorf("atomization unit fence mismatch")
		}
	}
	if manifest.TranscriptionGenerationID != nil {
		var generation models.TranscriptionGeneration
		if err := db.Where("public_id=? AND tenant_id=?", *manifest.TranscriptionGenerationID, manifest.TenantID).First(&generation).Error; err != nil {
			return fmt.Errorf("transcription generation owner not found")
		}
		if manifest.ContentItemID != nil && generation.ContentItemID != *manifest.ContentItemID {
			return fmt.Errorf("transcription generation content mismatch")
		}
	}
	if manifest.TranscriptionSegmentUnitID != nil {
		var unit models.TranscriptionSegmentUnit
		if err := db.Where("public_id=? AND tenant_id=?", *manifest.TranscriptionSegmentUnitID, manifest.TenantID).First(&unit).Error; err != nil {
			return fmt.Errorf("transcription segment owner not found")
		}
		if manifest.TranscriptionGenerationID == nil || unit.GenerationID != *manifest.TranscriptionGenerationID {
			return fmt.Errorf("transcription segment generation mismatch")
		}
		if manifest.FenceToken != nil && (unit.FenceToken == nil || *unit.FenceToken != *manifest.FenceToken) {
			return fmt.Errorf("transcription segment fence mismatch")
		}
	}
	if manifest.AttemptID != nil {
		var attempt models.ContentStageAttempt
		contentStageResult := db.Where("public_id=? AND tenant_id=?", *manifest.AttemptID, manifest.TenantID).First(&attempt)
		if contentStageResult.Error == nil {
			if manifest.FenceToken != nil && attempt.FenceToken != *manifest.FenceToken {
				return fmt.Errorf("content-stage attempt fence mismatch")
			}
		} else if !errors.Is(contentStageResult.Error, gorm.ErrRecordNotFound) {
			return fmt.Errorf("content-stage attempt lookup failed")
		} else {
			// Atomization and pipeline-repair attempts also materialize artifacts.
			// AttemptID is intentionally polymorphic so a manifest can carry the
			// durable attempt that owns it without inventing a duplicate
			// correlation ID.
			var atomizationAttempt models.AtomizationWorkAttempt
			atomizationResult := db.Where("public_id=? AND tenant_id=?", *manifest.AttemptID, manifest.TenantID).First(&atomizationAttempt)
			if atomizationResult.Error == nil {
				if manifest.FenceToken != nil && atomizationAttempt.FenceToken != *manifest.FenceToken {
					return fmt.Errorf("atomization attempt fence mismatch")
				}
				return nil
			}
			if !errors.Is(atomizationResult.Error, gorm.ErrRecordNotFound) {
				return fmt.Errorf("atomization attempt lookup failed")
			}

			var repairAttempt models.PipelineRepairAttempt
			if err := db.Where("public_id=? AND tenant_id=?", *manifest.AttemptID, manifest.TenantID).First(&repairAttempt).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("artifact attempt owner not found")
				}
				return fmt.Errorf("pipeline-repair attempt lookup failed")
			}
			if manifest.FenceToken != nil && repairAttempt.FenceToken != *manifest.FenceToken {
				return fmt.Errorf("pipeline-repair attempt fence mismatch")
			}
		}
	}
	return nil
}

type artifactManifestTransitionRequest struct {
	TenantID             string         `json:"tenant_id"`
	ProducerEventID      string         `json:"producer_event_id"`
	FenceToken           string         `json:"fence_token"`
	State                string         `json:"state"`
	ETag                 string         `json:"etag"`
	SHA256               string         `json:"sha256"`
	SizeBytes            *int64         `json:"size_bytes"`
	ContentType          string         `json:"content_type"`
	PublicURL            string         `json:"public_url"`
	VerificationEvidence map[string]any `json:"verification_evidence"`
	TerminalProof        map[string]any `json:"terminal_proof"`
	CleanupAfterSec      int            `json:"cleanup_after_sec"`
}

func manifestTransitionAllowed(from, to string) bool {
	if to == manifestStateUncertain || to == manifestStateFailed {
		return from != manifestStateDeleted
	}
	switch from {
	case manifestStateUploading:
		return to == manifestStateUploaded || to == manifestStateCleanupEligible
	case manifestStateUploaded:
		return to == manifestStateVerified || to == manifestStateUncertain || to == manifestStateFailed || to == manifestStateCleanupEligible
	case manifestStateVerified:
		return to == manifestStateActive || to == manifestStateCleanupEligible
	case manifestStateActive:
		return to == manifestStateCleanupEligible
	case manifestStateCleanupEligible:
		return to == manifestStateDeleted
	case manifestStateUncertain:
		return to == manifestStateUploaded || to == manifestStateVerified || to == manifestStateFailed || to == manifestStateCleanupEligible
	case manifestStateFailed:
		return to == manifestStateUploaded || to == manifestStateUncertain
	default:
		return false
	}
}

func InternalTransitionArtifactManifest(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid manifest id"})
		return
	}
	var req artifactManifestTransitionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transition"})
		return
	}
	state := strings.TrimSpace(req.State)
	var manifest models.MediaArtifactManifest
	if err := db.Where("public_id=?", id).First(&manifest).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "manifest not found"})
		return
	}
	if err := validateArtifactManifestOwnership(db, &manifest); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "manifest ownership validation failed", "reason": err.Error()})
		return
	}
	if strings.TrimSpace(req.TenantID) != "" && strings.TrimSpace(req.TenantID) != manifest.TenantID {
		c.JSON(http.StatusConflict, gin.H{"error": "manifest tenant mismatch"})
		return
	}
	if strings.TrimSpace(req.ProducerEventID) != "" {
		producer, parseErr := uuid.Parse(strings.TrimSpace(req.ProducerEventID))
		if parseErr != nil || producer != manifest.ProducerEventID {
			c.JSON(http.StatusConflict, gin.H{"error": "manifest producer event mismatch"})
			return
		}
	}
	if manifest.FenceToken != nil {
		fence, parseErr := uuid.Parse(strings.TrimSpace(req.FenceToken))
		if parseErr != nil || fence != *manifest.FenceToken {
			c.JSON(http.StatusConflict, gin.H{"error": "manifest fence mismatch"})
			return
		}
	}
	if !manifestTransitionAllowed(manifest.State, state) {
		if manifest.State == state {
			c.JSON(http.StatusOK, manifest)
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": "invalid manifest state transition"})
		return
	}
	updates := map[string]any{"state": state, "updated_at": time.Now().UTC()}
	if req.ETag != "" {
		updates["etag"] = req.ETag
	}
	if req.SHA256 != "" {
		updates["sha256"] = req.SHA256
	}
	if req.SizeBytes != nil {
		updates["size_bytes"] = *req.SizeBytes
	}
	if req.ContentType != "" {
		updates["content_type"] = req.ContentType
	}
	if req.PublicURL != "" {
		updates["public_url"] = req.PublicURL
	}
	if req.VerificationEvidence != nil {
		updates["verification_evidence"] = longFormJSON(req.VerificationEvidence)
	}
	if req.TerminalProof != nil {
		updates["terminal_proof"] = longFormJSON(req.TerminalProof)
	}
	if state == manifestStateVerified {
		now := time.Now().UTC()
		updates["verified_at"] = &now
	}
	if state == manifestStateCleanupEligible {
		now := time.Now().UTC().Add(time.Duration(req.CleanupAfterSec) * time.Second)
		updates["cleanup_eligible_at"] = &now
	}
	if state == manifestStateDeleted {
		now := time.Now().UTC()
		updates["deleted_at"] = &now
	}
	if err := db.Model(&manifest).Updates(updates).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "manifest transition failed"})
		return
	}
	if err := db.Where("public_id=?", id).First(&manifest).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "manifest readback failed"})
		return
	}
	c.JSON(http.StatusOK, manifest)
}

func InternalGetArtifactManifest(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var manifest models.MediaArtifactManifest
	query := db.Model(&models.MediaArtifactManifest{})
	if tenant := strings.TrimSpace(c.Query("tenant_id")); tenant != "" {
		query = query.Where("tenant_id=?", tenant)
	}
	if id := strings.TrimSpace(c.Param("id")); id != "" {
		query = query.Where("public_id=?", id)
	}
	if key := strings.TrimSpace(c.Query("object_key")); key != "" {
		query = query.Where("object_key=?", key)
	}
	if bucket := strings.TrimSpace(c.Query("bucket")); bucket != "" {
		query = query.Where("bucket=?", bucket)
	}
	if tier := strings.TrimSpace(c.Query("storage_tier")); tier != "" {
		query = query.Where("storage_tier=?", tier)
	}
	if state := strings.TrimSpace(c.Query("state")); state != "" {
		query = query.Where("state IN ?", strings.Split(state, ","))
	}
	if strings.EqualFold(strings.TrimSpace(c.Query("stale")), "true") {
		query = query.Where("state IN ? AND updated_at < ?", []string{manifestStateUploading, manifestStateUploaded, manifestStateUncertain}, time.Now().UTC().Add(-15*time.Minute))
	}
	if id := strings.TrimSpace(c.Param("id")); id != "" {
		if err := query.First(&manifest).Error; err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "manifest not found"})
			return
		}
		c.JSON(http.StatusOK, manifest)
		return
	}
	var manifests []models.MediaArtifactManifest
	if err := query.Order("created_at ASC").Limit(200).Find(&manifests).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "manifest query failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"manifests": manifests})
}

type transcriptionGenerationRequest struct {
	TenantID                string                          `json:"tenant_id"`
	ContentItemID           string                          `json:"content_item_id"`
	TranscriptionJobID      string                          `json:"transcription_job_id"`
	InputDigest             string                          `json:"input_digest"`
	AnalysisAudioManifestID string                          `json:"analysis_audio_manifest_id"`
	Provider                string                          `json:"provider"`
	Model                   string                          `json:"model"`
	Language                string                          `json:"language"`
	ContentStage            *contentStageCorrelationRequest `json:"content_stage,omitempty"`
	Segments                []struct {
		StartMs            int64  `json:"start_ms"`
		EndMs              int64  `json:"end_ms"`
		OverlapMs          int64  `json:"overlap_ms"`
		SourceDigest       string `json:"source_digest"`
		SegmentDigest      string `json:"segment_digest"`
		ArtifactManifestID string `json:"artifact_manifest_id"`
	} `json:"segments"`
}

func InternalCreateTranscriptionGeneration(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req transcriptionGenerationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transcription generation"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	contentID, err := uuid.Parse(req.ContentItemID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content_item_id"})
		return
	}
	if strings.TrimSpace(req.InputDigest) == "" || len(req.Segments) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "input_digest and segments are required"})
		return
	}
	var existing models.TranscriptionGeneration
	if db.Where("tenant_id=? AND content_item_id=? AND input_digest=?", tenant, contentID, req.InputDigest).First(&existing).Error == nil {
		c.JSON(http.StatusOK, gin.H{"generation": existing})
		return
	}
	jobID, err := longFormUUID(req.TranscriptionJobID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transcription_job_id"})
		return
	}
	audioID, err := longFormUUID(req.AnalysisAudioManifestID, false)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid analysis_audio_manifest_id"})
		return
	}
	if audioID == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "analysis_audio_manifest_id is required"})
		return
	}
	var contentItem models.ContentItem
	if err := db.Where("public_id=?", contentID).First(&contentItem).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "transcription content owner not found"})
		return
	}
	if contentItem.TenantID != tenant {
		c.JSON(http.StatusConflict, gin.H{"error": "transcription content tenant mismatch"})
		return
	}
	var audioManifest models.MediaArtifactManifest
	if err := db.Where("public_id=? AND tenant_id=?", *audioID, tenant).First(&audioManifest).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "analysis audio manifest owner not found"})
		return
	}
	if audioManifest.ContentItemID != nil && *audioManifest.ContentItemID != contentID {
		c.JSON(http.StatusConflict, gin.H{"error": "analysis audio manifest content mismatch"})
		return
	}
	if audioManifest.State != manifestStateVerified && audioManifest.State != manifestStateActive {
		c.JSON(http.StatusConflict, gin.H{"error": "analysis audio manifest is not verified"})
		return
	}
	if audioManifest.ArtifactRole != "analysis_audio" && audioManifest.ArtifactRole != "source" {
		c.JSON(http.StatusConflict, gin.H{"error": "manifest is not an audio artifact"})
		return
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(audioManifest.ContentType)), "audio/") {
		c.JSON(http.StatusConflict, gin.H{"error": "analysis audio manifest must have an audio content type"})
		return
	}
	proof := map[string]any{}
	if req.ContentStage != nil {
		proof["content_stage"] = req.ContentStage
	}
	generation := models.TranscriptionGeneration{PublicID: uuid.New(), TenantID: tenant, ContentItemID: contentID, TranscriptionJobID: jobID, InputDigest: req.InputDigest, AnalysisAudioManifestID: audioID, Provider: req.Provider, Model: req.Model, Language: req.Language, State: unitStateQueued, TotalSegments: len(req.Segments), TerminalProof: longFormJSON(proof)}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&generation).Error; err != nil {
			return err
		}
		var previousEnd int64
		for i, segment := range req.Segments {
			if segment.EndMs <= segment.StartMs || segment.StartMs < 0 || (i == 0 && segment.StartMs != 0) || (i > 0 && segment.StartMs > previousEnd) {
				return errors.New("invalid transcription segment bounds")
			}
			overlap := previousEnd - segment.StartMs
			if i == 0 {
				overlap = 0
			}
			if segment.OverlapMs < 0 || segment.OverlapMs != overlap || segment.OverlapMs > segment.EndMs-segment.StartMs {
				return errors.New("invalid transcription segment overlap")
			}
			artifactID, parseErr := longFormUUID(segment.ArtifactManifestID, false)
			if parseErr != nil {
				return parseErr
			}
			if artifactID == nil || *artifactID != *audioID || strings.TrimSpace(segment.SourceDigest) == "" || segment.SourceDigest != req.InputDigest || strings.TrimSpace(segment.SegmentDigest) == "" {
				return errors.New("invalid transcription segment correlation")
			}
			unit := models.TranscriptionSegmentUnit{PublicID: uuid.New(), TenantID: tenant, GenerationID: generation.PublicID, SegmentIndex: i, StartMs: segment.StartMs, EndMs: segment.EndMs, OverlapMs: segment.OverlapMs, SourceDigest: segment.SourceDigest, SegmentDigest: segment.SegmentDigest, ArtifactManifestID: artifactID, State: unitStateQueued, TerminalProof: longFormJSON(map[string]any{})}
			if err := tx.Create(&unit).Error; err != nil {
				return err
			}
			previousEnd = segment.EndMs
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "transcription generation creation failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"generation": generation})
}

type claimedLongFormUnit struct {
	Unit       any `json:"unit"`
	Generation any `json:"generation"`
}

type longFormClaimRequest struct {
	GenerationID string `json:"generation_id"`
}

func claimTranscriptionUnit(db *gorm.DB, owner string) (*models.TranscriptionSegmentUnit, *models.TranscriptionGeneration, error) {
	now := time.Now().UTC()
	var unit models.TranscriptionSegmentUnit
	var generation models.TranscriptionGeneration
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("(state IN ? AND (not_before_at IS NULL OR not_before_at <= ?)) OR (state IN ? AND lease_expires_at < ?)", []string{unitStateQueued, unitStateDeferred, unitStateUncertain}, now, []string{unitStateClaimed, unitStateRunning, unitStateVerifying}, now).Order("created_at ASC, segment_index ASC").First(&unit).Error; err != nil {
			return err
		}
		if err := tx.Where("public_id=?", unit.GenerationID).First(&generation).Error; err != nil {
			return err
		}
		claim := uuid.New()
		fence := uuid.New()
		expires := now.Add(2 * time.Minute)
		if err := tx.Model(&unit).Updates(map[string]any{"state": unitStateClaimed, "claim_owner": owner, "claim_token": claim, "fence_token": fence, "lease_expires_at": expires, "attempt_count": gorm.Expr("attempt_count + 1"), "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&generation).Updates(map[string]any{"state": unitStateRunning, "updated_at": now}).Error; err != nil {
			return err
		}
		unit.State, unit.ClaimOwner, unit.ClaimToken, unit.FenceToken, unit.LeaseExpiresAt = unitStateClaimed, owner, &claim, &fence, &expires
		return nil
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, nil
	}
	return &unit, &generation, err
}

func InternalClaimTranscriptionSegment(c *gin.Context) {
	unit, generation, err := claimTranscriptionUnit(c.MustGet("db").(*gorm.DB), strings.TrimSpace(c.GetHeader("X-Worker-Role")))
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "transcription segment claim unavailable"})
		return
	}
	if unit == nil {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, gin.H{"unit": unit, "generation": generation})
}

type longFormUnitStepRequest struct {
	ClaimToken          string           `json:"claim_token"`
	RetryAfterSec       int              `json:"retry_after_sec"`
	FailureClass        string           `json:"failure_class"`
	Summary             string           `json:"summary"`
	TranscriptText      string           `json:"transcript_text"`
	TranscriptSegments  []map[string]any `json:"transcript_segments"`
	Result              map[string]any   `json:"result"`
	ArtifactManifestIDs []string         `json:"artifact_manifest_ids"`
}

func InternalTransitionTranscriptionSegment(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid segment id"})
		return
	}
	var body longFormUnitStepRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid segment transition"})
		return
	}
	var unit models.TranscriptionSegmentUnit
	if err := db.Where("public_id=?", id).First(&unit).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "segment not found"})
		return
	}
	if body.ClaimToken != "" {
		token, parseErr := uuid.Parse(body.ClaimToken)
		if parseErr != nil || unit.ClaimToken == nil || *unit.ClaimToken != token {
			c.JSON(http.StatusConflict, gin.H{"error": "stale segment claim"})
			return
		}
	}
	target := strings.TrimSpace(c.Param("state"))
	if target == "" {
		target = strings.TrimSpace(c.Query("state"))
	}
	if target == "" {
		target = unitStateRunning
	}
	if !longFormUnitTransitionAllowed(unit.State, target) {
		c.JSON(http.StatusConflict, gin.H{"error": "invalid transcription segment state transition"})
		return
	}
	updates := map[string]any{"state": target, "updated_at": time.Now().UTC()}
	if target == unitStateDeferred {
		updates["not_before_at"] = time.Now().UTC().Add(time.Duration(body.RetryAfterSec) * time.Second)
		updates["failure_class"] = "capacity_deferred"
	}
	if body.FailureClass != "" {
		updates["failure_class"] = body.FailureClass
	}
	if body.Summary != "" {
		updates["terminal_proof"] = longFormJSON(map[string]any{"summary": body.Summary})
	}
	if body.TranscriptText != "" {
		updates["transcript_text"] = body.TranscriptText
	}
	if body.TranscriptSegments != nil {
		updates["transcript_segments"] = longFormJSON(body.TranscriptSegments)
	}
	if target == unitStateVerified {
		updates["result_digest"] = longFormDigest(map[string]any{"text": body.TranscriptText, "segments": body.TranscriptSegments})
		updates["terminal_proof"] = longFormJSON(map[string]any{"verified": true, "summary": body.Summary})
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&unit).Where("claim_token=?", unit.ClaimToken).Updates(updates).Error; err != nil {
			return err
		}
		if target == unitStateVerified {
			var completed int64
			if err := tx.Model(&models.TranscriptionSegmentUnit{}).Where("generation_id=? AND state=?", unit.GenerationID, unitStateVerified).Count(&completed).Error; err != nil {
				return err
			}
			return tx.Model(&models.TranscriptionGeneration{}).Where("public_id=?", unit.GenerationID).Updates(map[string]any{"state": unitStateRunning, "completed_segments": completed, "updated_at": time.Now().UTC()}).Error
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "segment transition rejected"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"unit": unit, "state": target})
}

func InternalHeartbeatTranscriptionSegment(c *gin.Context) { transitionTranscriptionHeartbeat(c) }
func transitionTranscriptionHeartbeat(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid segment id"})
		return
	}
	var body struct {
		ClaimToken string `json:"claim_token"`
	}
	if c.ShouldBindJSON(&body) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid heartbeat"})
		return
	}
	token, err := uuid.Parse(body.ClaimToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid claim token"})
		return
	}
	expires := time.Now().UTC().Add(2 * time.Minute)
	result := db.Model(&models.TranscriptionSegmentUnit{}).Where("public_id=? AND claim_token=? AND state IN ?", id, token, []string{unitStateClaimed, unitStateRunning, unitStateVerifying}).Updates(map[string]any{"state": unitStateRunning, "lease_expires_at": expires, "updated_at": time.Now().UTC()})
	if result.Error != nil || result.RowsAffected != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "heartbeat rejected"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"lease_expires_at": expires})
}

func InternalFinalizeTranscriptionGeneration(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var request struct {
		ContentStage *contentStageCorrelationRequest `json:"content_stage,omitempty"`
	}
	if err := c.ShouldBindJSON(&request); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transcription finalization request"})
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid generation id"})
		return
	}
	var generation models.TranscriptionGeneration
	if err := db.Where("public_id=?", id).First(&generation).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "generation not found"})
		return
	}
	if generation.MergedTranscriptID != nil {
		c.JSON(http.StatusOK, gin.H{"transcript_id": generation.MergedTranscriptID})
		return
	}
	var units []models.TranscriptionSegmentUnit
	if err := db.Where("generation_id=?", id).Order("segment_index ASC").Find(&units).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "segment query failed"})
		return
	}
	if len(units) != generation.TotalSegments || len(units) == 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "transcription segments are incomplete", "code": "TRANSCRIPTION_INCOMPLETE"})
		return
	}
	for _, unit := range units {
		if unit.State != unitStateVerified {
			c.JSON(http.StatusConflict, gin.H{"error": "transcription segment is not verified", "code": "TRANSCRIPTION_INCOMPLETE"})
			return
		}
	}
	var item models.ContentItem
	if err := db.Where("public_id=?", generation.ContentItemID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "content item not found"})
		return
	}
	allSegments := make([]map[string]any, 0)
	for _, unit := range units {
		var segments []map[string]any
		_ = json.Unmarshal(unit.TranscriptSegments, &segments)
		for _, segment := range segments {
			if start, ok := segment["start"].(float64); ok {
				segment["start"] = start + float64(unit.StartMs)/1000
			}
			if end, ok := segment["end"].(float64); ok {
				segment["end"] = end + float64(unit.StartMs)/1000
			}
			allSegments = append(allSegments, segment)
		}
	}
	allSegments = dedupeLongFormTranscriptSegments(allSegments)
	texts := make([]string, 0, len(allSegments))
	for _, segment := range allSegments {
		if text, ok := segment["text"].(string); ok && strings.TrimSpace(text) != "" {
			texts = append(texts, strings.TrimSpace(text))
		}
	}
	if len(texts) == 0 {
		for _, unit := range units {
			if strings.TrimSpace(unit.TranscriptText) != "" {
				texts = append(texts, strings.TrimSpace(unit.TranscriptText))
			}
		}
	}
	var transcript models.Transcript
	stageCorrelation := request.ContentStage
	if stageCorrelation == nil && len(generation.TerminalProof) > 0 {
		var proof struct {
			ContentStage *contentStageCorrelationRequest `json:"content_stage,omitempty"`
		}
		if json.Unmarshal(generation.TerminalProof, &proof) == nil {
			stageCorrelation = proof.ContentStage
		}
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		var stageRequest models.ContentStageRequest
		var stageAttempt models.ContentStageAttempt
		if stageCorrelation != nil {
			var err error
			stageRequest, stageAttempt, err = contentstage.AuthorizeWriteback(tx, item.PublicID, stageCorrelation.correlation(), models.ContentStagePodsTranscript)
			if err != nil {
				return fmt.Errorf("content-stage writeback rejected: %w", err)
			}
		}
		transcript = models.Transcript{PublicID: uuid.New(), ContentItemID: item.PublicID, FullText: strings.Join(texts, " "), Segments: longFormJSON(allSegments), Source: ptrString("stt_deepgram"), Provider: ptrString(generation.Provider), Language: ptrString(generation.Language)}
		if err := tx.Create(&transcript).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.ContentItem{}).Where("public_id=?", item.PublicID).Updates(map[string]any{"transcript_id": transcript.PublicID, "caption_state": models.CaptionStateSTTDone, "transcript_source": models.TranscriptSourceSTTDeepgram}).Error; err != nil {
			return err
		}
		if err := tx.Model(&generation).Updates(map[string]any{"state": unitStateVerified, "merged_transcript_id": transcript.PublicID, "completed_segments": len(units), "terminal_proof": longFormJSON(map[string]any{"verified": true, "segment_count": len(units)})}).Error; err != nil {
			return err
		}
		if generation.TranscriptionJobID != nil {
			var job models.TranscriptionJob
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND content_item_id=?", *generation.TranscriptionJobID, item.PublicID).First(&job).Error; err != nil && err != gorm.ErrRecordNotFound {
				return err
			} else if err == nil {
				status, writebackStatus := models.TranscriptionJobStatusSucceeded, "ok"
				duration := 0.0
				if item.DurationSec != nil {
					duration = float64(*item.DurationSec)
				}
				jobUpdate := internalUpdateTranscriptionJobRequest{
					Status: &status, TranscriptID: ptrString(transcript.PublicID.String()),
					Provider: ptrString(generation.Provider), Model: ptrString(generation.Model),
					Language: ptrString(generation.Language), DurationSec: &duration,
					WritebackStatus: &writebackStatus,
					Metadata:        map[string]interface{}{"write_back_status": "ok", "segment_count": len(units)},
				}
				wasTerminal := terminalTranscriptionStatus(job.Status) && job.CompletedAt != nil
				updateTranscriptionJobFromRequest(tx, &job, jobUpdate)
				if err := tx.Save(&job).Error; err != nil {
					return err
				}
				if !wasTerminal {
					actual := job.ActualCostUsd
					if actual == 0 {
						actual = job.EstimatedCostUsd
					}
					settleTranscriptionBudget(tx, job.TenantID, job.ReservedCostUsd, actual)
					updateBatchItemForJob(tx, &job)
				}
			}
		}
		if stageCorrelation != nil {
			if err := contentstage.RecordPersistence(tx, stageRequest, stageAttempt, stageCorrelation.correlation(), models.ContentStageOwnerMedia, transcript.PublicID.String(), map[string]any{"transcript_id": transcript.PublicID.String(), "segment_count": len(units)}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "transcription finalization failed", "code": "TRANSCRIPTION_FINALIZATION_FAILED", "reason": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"transcript_id": transcript.PublicID, "segment_count": len(units)})
}

func dedupeLongFormTranscriptSegments(input []map[string]any) []map[string]any {
	output := make([]map[string]any, 0, len(input))
	lastStart := make(map[string]float64)
	for _, segment := range input {
		text, _ := segment["text"].(string)
		normalized := strings.Join(strings.Fields(strings.ToLower(text)), " ")
		start, _ := segment["start"].(float64)
		if normalized != "" {
			if previous, exists := lastStart[normalized]; exists && start-previous <= 45 {
				continue
			}
			lastStart[normalized] = start
		}
		output = append(output, segment)
	}
	return output
}

type atomizationGenerationRequest struct {
	TenantID            string           `json:"tenant_id"`
	ParentContentItemID string           `json:"parent_content_item_id"`
	WorkRequestID       string           `json:"work_request_id"`
	TranscriptDigest    string           `json:"transcript_digest"`
	PolicyDigest        string           `json:"policy_digest"`
	InputDigest         string           `json:"input_digest"`
	PlanDigest          string           `json:"plan_digest"`
	CoverageDigest      string           `json:"coverage_digest"`
	Chapters            []map[string]any `json:"chapters"`
}

func InternalCreateAtomizationGeneration(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req atomizationGenerationRequest
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid atomization generation"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	parentID, err := uuid.Parse(req.ParentContentItemID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid parent_content_item_id"})
		return
	}
	workID, err := uuid.Parse(req.WorkRequestID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid work_request_id"})
		return
	}
	if len(req.Chapters) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "chapters are required"})
		return
	}
	var parent models.ContentItem
	if err := db.Where("public_id=?", parentID).First(&parent).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization parent not found"})
		return
	}
	if parent.TenantID != tenant {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization parent tenant mismatch"})
		return
	}
	if parent.DurationSec == nil || *parent.DurationSec <= 2400 {
		c.JSON(http.StatusConflict, gin.H{"error": "parent duration is not atomization-eligible"})
		return
	}
	var workRequest models.AtomizationWorkRequest
	workResult := db.Where("public_id=?", workID).First(&workRequest)
	if workResult.Error != nil && !errors.Is(workResult.Error, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization work request lookup failed"})
		return
	}
	if workResult.Error == nil && (workRequest.TenantID != tenant || workRequest.ParentContentItemID != parentID) {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization work request owner not found"})
		return
	}
	var previous models.AtomizationGeneration
	generationNumber := 1
	var samePlan models.AtomizationGeneration
	if db.Where("tenant_id=? AND work_request_id=? AND plan_digest=?", tenant, workID, req.PlanDigest).First(&samePlan).Error == nil {
		c.JSON(http.StatusOK, gin.H{"generation": samePlan})
		return
	}
	if db.Where("tenant_id=? AND work_request_id=?", tenant, workID).Order("generation_number DESC").First(&previous).Error == nil {
		generationNumber = previous.GenerationNumber + 1
	}
	initialProof := map[string]any{}
	if errors.Is(workResult.Error, gorm.ErrRecordNotFound) {
		initialProof["compatibility_work_request"] = true
	}
	generation := models.AtomizationGeneration{PublicID: uuid.New(), TenantID: tenant, ParentContentItemID: parentID, WorkRequestID: workID, GenerationNumber: generationNumber, TranscriptDigest: req.TranscriptDigest, PolicyDigest: req.PolicyDigest, InputDigest: req.InputDigest, PlanDigest: req.PlanDigest, CoverageDigest: req.CoverageDigest, ExpectedUnits: len(req.Chapters), State: "running", TerminalProof: longFormJSON(initialProof)}
	parentDurationMs := int64(*parent.DurationSec) * 1000
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&generation).Error; err != nil {
			return err
		}
		var previousEnd int64
		for i, ch := range req.Chapters {
			start, ok1 := numberFromMap(ch, "start_ms")
			end, ok2 := numberFromMap(ch, "end_ms")
			if !ok1 || !ok2 || start < 0 || end <= start || int64(start) != previousEnd || end-start < 270000 || end-start > 2400000 {
				return errors.New("invalid chapter bounds")
			}
			if i == len(req.Chapters)-1 && int64(end) != parentDurationMs {
				return errors.New("chapter plan does not cover the parent")
			}
			unit := models.AtomizationChapterUnit{PublicID: uuid.New(), TenantID: tenant, GenerationID: generation.PublicID, UnitIndex: i, StartMs: int64(start), EndMs: int64(end), PlanDigest: req.PlanDigest, TranscriptSliceDigest: longFormDigest(ch), State: unitStateQueued, Result: longFormJSON(ch), TerminalProof: longFormJSON(map[string]any{})}
			if err := tx.Create(&unit).Error; err != nil {
				return err
			}
			previousEnd = int64(end)
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization generation creation failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"generation": generation})
}

func numberFromMap(value map[string]any, key string) (float64, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	switch n := raw.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

func claimAtomizationUnit(db *gorm.DB, owner string, generationID string) (*models.AtomizationChapterUnit, *models.AtomizationGeneration, error) {
	now := time.Now().UTC()
	var unit models.AtomizationChapterUnit
	var gen models.AtomizationGeneration
	err := db.Transaction(func(tx *gorm.DB) error {
		query := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("((state IN ? AND (not_before_at IS NULL OR not_before_at<=?)) OR (state IN ? AND lease_expires_at<?))", []string{unitStateQueued, unitStateDeferred, unitStateUncertain}, now, []string{unitStateClaimed, unitStateRunning, unitStateVerifying}, now)
		if strings.TrimSpace(generationID) != "" {
			query = query.Where("generation_id=?", generationID)
		}
		if err := query.Order("created_at ASC, unit_index ASC").First(&unit).Error; err != nil {
			return err
		}
		if err := tx.Where("public_id=?", unit.GenerationID).First(&gen).Error; err != nil {
			return err
		}
		claim := uuid.New()
		fence := uuid.New()
		expires := now.Add(2 * time.Minute)
		if err := tx.Model(&unit).Updates(map[string]any{"state": unitStateClaimed, "claim_owner": owner, "claim_token": claim, "fence_token": fence, "lease_expires_at": expires, "attempt_count": gorm.Expr("attempt_count+1"), "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&gen).Updates(map[string]any{"state": "running", "updated_at": now}).Error; err != nil {
			return err
		}
		unit.State, unit.ClaimOwner, unit.ClaimToken, unit.FenceToken, unit.LeaseExpiresAt = unitStateClaimed, owner, &claim, &fence, &expires
		return nil
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, nil
	}
	return &unit, &gen, err
}

func InternalClaimAtomizationChapterUnit(c *gin.Context) {
	var body longFormClaimRequest
	_ = c.ShouldBindJSON(&body)
	unit, gen, err := claimAtomizationUnit(c.MustGet("db").(*gorm.DB), strings.TrimSpace(c.GetHeader("X-Worker-Role")), strings.TrimSpace(body.GenerationID))
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chapter unit claim unavailable"})
		return
	}
	if unit == nil {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, gin.H{"unit": unit, "generation": gen})
}

func InternalTransitionAtomizationChapterUnit(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid chapter unit id"})
		return
	}
	var body longFormUnitStepRequest
	if c.ShouldBindJSON(&body) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid chapter transition"})
		return
	}
	var unit models.AtomizationChapterUnit
	if db.Where("public_id=?", id).First(&unit).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "chapter unit not found"})
		return
	}
	token, err := uuid.Parse(body.ClaimToken)
	if err != nil || unit.ClaimToken == nil || *unit.ClaimToken != token {
		c.JSON(http.StatusConflict, gin.H{"error": "stale chapter claim"})
		return
	}
	state := strings.TrimSpace(c.Param("state"))
	if state == "" {
		state = strings.TrimSpace(c.Query("state"))
	}
	if state == "" {
		state = unitStateRunning
	}
	if !longFormUnitTransitionAllowed(unit.State, state) {
		c.JSON(http.StatusConflict, gin.H{"error": "invalid chapter unit state transition"})
		return
	}
	updates := map[string]any{"state": state, "updated_at": time.Now().UTC()}
	if state == unitStateDeferred {
		updates["not_before_at"] = time.Now().UTC().Add(time.Duration(body.RetryAfterSec) * time.Second)
		updates["failure_class"] = "capacity_deferred"
	}
	if body.FailureClass != "" {
		updates["failure_class"] = body.FailureClass
	}
	if body.Result != nil {
		updates["result"] = longFormJSON(body.Result)
	}
	if body.ArtifactManifestIDs != nil {
		updates["artifact_manifest_ids"] = longFormJSON(body.ArtifactManifestIDs)
	}
	if state == unitStateVerified {
		updates["terminal_proof"] = longFormJSON(map[string]any{"verified": true})
	}
	if db.Model(&unit).Where("claim_token=?", token).Updates(updates).Error != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "chapter transition rejected"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"unit": unit, "state": state})
}

func longFormUnitTransitionAllowed(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case unitStateClaimed:
		return to == unitStateRunning || to == unitStateVerifying || to == unitStateVerified || to == unitStateDeferred || to == unitStateUncertain || to == unitStateFailed
	case unitStateRunning, unitStateVerifying:
		return to == unitStateRunning || to == unitStateVerifying || to == unitStateVerified || to == unitStateDeferred || to == unitStateUncertain || to == unitStateFailed
	case unitStateUncertain:
		return to == unitStateRunning || to == unitStateFailed || to == unitStateDeferred
	case unitStateDeferred:
		return to == unitStateFailed
	default:
		return false
	}
}

func InternalHeartbeatAtomizationChapterUnit(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid chapter unit id"})
		return
	}
	var body struct {
		ClaimToken string `json:"claim_token"`
	}
	if c.ShouldBindJSON(&body) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid heartbeat"})
		return
	}
	token, err := uuid.Parse(body.ClaimToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid claim token"})
		return
	}
	expires := time.Now().UTC().Add(2 * time.Minute)
	if db.Model(&models.AtomizationChapterUnit{}).Where("public_id=? AND claim_token=? AND state IN ?", id, token, []string{unitStateClaimed, unitStateRunning, unitStateVerifying}).Updates(map[string]any{"state": unitStateRunning, "lease_expires_at": expires, "updated_at": time.Now().UTC()}).RowsAffected != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "heartbeat rejected"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"lease_expires_at": expires})
}

func InternalListAtomizationChapterUnits(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid generation id"})
		return
	}
	var units []models.AtomizationChapterUnit
	if db.Where("generation_id=?", id).Order("unit_index ASC").Find(&units).Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "chapter unit query failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"units": units})
}

func InternalFinalizeAtomizationGeneration(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var request struct {
		ContentStage *contentStageCorrelationRequest `json:"content_stage,omitempty"`
	}
	if err := c.ShouldBindJSON(&request); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid finalization request"})
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid generation id"})
		return
	}
	var gen models.AtomizationGeneration
	if db.Where("public_id=?", id).First(&gen).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "generation not found"})
		return
	}
	var parent models.ContentItem
	var transcript models.Transcript
	if db.Where("public_id=?", gen.ParentContentItemID).First(&parent).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "parent not found"})
		return
	}
	if parent.TranscriptID == nil || db.Where("public_id=?", *parent.TranscriptID).First(&transcript).Error != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "parent transcript not found"})
		return
	}
	var units []models.AtomizationChapterUnit
	if db.Where("generation_id=?", gen.PublicID).Order("unit_index ASC").Find(&units).Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "unit query failed"})
		return
	}
	if len(units) != gen.ExpectedUnits {
		c.JSON(http.StatusConflict, gin.H{"error": "chapter units incomplete"})
		return
	}
	policy := atomizationPolicyForItem(db, &parent)
	if err := validateAtomizationGenerationUnits(db, &parent, &gen, units); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "chapter unit validation failed", "reason": err.Error()})
		return
	}
	if request.ContentStage != nil {
		if err := requireNormalStageCorrelation(db, parent, models.ContentStagePodsAtomization, request.ContentStage, false); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
	}
	children := make([]map[string]any, 0, len(units))
	err = db.Transaction(func(tx *gorm.DB) error {
		var stageRequest models.ContentStageRequest
		var stageAttempt models.ContentStageAttempt
		if request.ContentStage != nil {
			var err error
			stageRequest, stageAttempt, err = contentstage.AuthorizeWriteback(tx, parent.PublicID, request.ContentStage.correlation(), models.ContentStagePodsAtomization)
			if err != nil {
				return err
			}
		}
		for _, unit := range units {
			if unit.State != unitStateVerified {
				return errors.New("chapter unit is not verified")
			}
			var ch atomizationChapterRequest
			if err := json.Unmarshal(unit.Result, &ch); err != nil {
				return err
			}
			child, err := upsertAtomizedChild(tx, &parent, &transcript, ch, unit.UnitIndex, policy, gen.PublicID.String())
			if err != nil {
				return err
			}
			delivery, err := contentstage.DeliveryMode(tx, child.TenantID, child.Type)
			if err != nil {
				return err
			}
			children = append(children, map[string]any{"id": child.PublicID.String(), "status": child.Status, "feed_visibility": child.FeedVisibility, "delivery_mode": delivery})
			_ = tx.Model(&unit).Updates(map[string]any{"candidate_content_item_id": child.PublicID, "updated_at": time.Now().UTC()}).Error
		}
		manifestIDs := make([]string, 0)
		for _, unit := range units {
			manifestIDs = append(manifestIDs, atomizationUnitManifestIDs(unit)...)
		}
		if len(manifestIDs) > 0 {
			if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND public_id IN ? AND state=?", gen.TenantID, manifestIDs, manifestStateVerified).Updates(map[string]any{"state": manifestStateActive, "updated_at": time.Now().UTC()}).Error; err != nil {
				return err
			}
		}
		now := time.Now().UTC()
		var previousGenerations []models.AtomizationGeneration
		if err := tx.Where("tenant_id=? AND parent_content_item_id=? AND state=? AND public_id<>?", gen.TenantID, gen.ParentContentItemID, "active", gen.PublicID).Find(&previousGenerations).Error; err != nil {
			return err
		}
		if len(previousGenerations) > 0 {
			previousIDs := make([]uuid.UUID, 0, len(previousGenerations))
			for _, previous := range previousGenerations {
				previousIDs = append(previousIDs, previous.PublicID)
			}
			// Eligibility records the replacement proof but deliberately does not
			// delete cloud objects. The cleanup executor remains rollout-gated.
			if err := tx.Model(&models.MediaArtifactManifest{}).
				Where("tenant_id=? AND atomization_generation_id IN ? AND state IN ?", gen.TenantID, previousIDs, []string{manifestStateVerified, manifestStateActive}).
				Updates(map[string]any{
					"state":               manifestStateCleanupEligible,
					"cleanup_eligible_at": now.Add(24 * time.Hour),
					"terminal_proof":      longFormJSON(map[string]any{"replacement_generation_id": gen.PublicID.String(), "activated_at": now}),
					"updated_at":          now,
				}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND parent_content_item_id=? AND state=?", gen.TenantID, gen.ParentContentItemID, "active").Updates(map[string]any{"state": "superseded", "updated_at": now}).Error; err != nil {
			return err
		}
		childIDs := make([]string, 0, len(children))
		for _, child := range children {
			if value, ok := child["id"].(string); ok {
				childIDs = append(childIDs, value)
			}
		}
		if len(childIDs) > 0 {
			if err := tx.Model(&models.ContentItem{}).
				Where("tenant_id=? AND parent_content_item_id=? AND public_id NOT IN ?", parent.TenantID, parent.PublicID, childIDs).
				Updates(map[string]any{"status": models.ContentStatusArchived, "feed_visibility": feedVisibilityHidden, "is_feed_unit": false, "chaptering_status": "superseded"}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=?", parent.TenantID, parent.PublicID).Updates(map[string]any{"is_feed_unit": false, "feed_visibility": feedVisibilityHidden, "chaptering_status": "completed"}).Error; err != nil {
			return err
		}
		if err := tx.Model(&gen).Updates(map[string]any{"state": "active", "completed_units": len(units), "activation_at": now, "terminal_proof": longFormJSON(map[string]any{"verified": true, "unit_count": len(units)})}).Error; err != nil {
			return err
		}
		if request.ContentStage != nil {
			ids := make([]string, 0, len(children))
			for _, child := range children {
				if value, ok := child["id"].(string); ok {
					ids = append(ids, value)
				}
			}
			return contentstage.RecordPersistence(tx, stageRequest, stageAttempt, request.ContentStage.correlation(), models.ContentStageOwnerAggregationPods, strings.Join(ids, ","), map[string]any{"generation_id": gen.PublicID.String(), "child_ids": ids})
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "atomization generation finalization failed", "reason": err.Error()})
		return
	}
	_ = db.Where("public_id=?", gen.PublicID).First(&gen)
	c.JSON(http.StatusOK, gin.H{"generation": gen, "children": children})
}

func atomizationUnitManifestIDs(unit models.AtomizationChapterUnit) []string {
	var ids []string
	if len(unit.ArtifactManifestIDs) == 0 || json.Unmarshal(unit.ArtifactManifestIDs, &ids) != nil {
		return nil
	}
	return ids
}

func validateAtomizationGenerationUnits(db *gorm.DB, parent *models.ContentItem, generation *models.AtomizationGeneration, units []models.AtomizationChapterUnit) error {
	if parent.DurationSec == nil || *parent.DurationSec <= 2400 {
		return errors.New("parent duration is not atomization-eligible")
	}
	cursor := int64(0)
	manifestIDs := make([]string, 0)
	for index, unit := range units {
		if unit.UnitIndex != index || unit.StartMs != cursor || unit.EndMs <= unit.StartMs {
			return fmt.Errorf("coverage gap or ordering error at unit %d", index)
		}
		duration := unit.EndMs - unit.StartMs
		if duration < 270000 || duration > 2400000 {
			return fmt.Errorf("unit %d has illegal duration %dms", index, duration)
		}
		var result atomizationChapterRequest
		if err := json.Unmarshal(unit.Result, &result); err != nil || result.PlaybackURL == nil || strings.TrimSpace(*result.PlaybackURL) == "" || result.MediaURL == nil || strings.TrimSpace(*result.MediaURL) == "" {
			return fmt.Errorf("unit %d has no verified playback result", index)
		}
		if err := validateTypedAudioTierSet(result.MediaRenditions); err != nil {
			return fmt.Errorf("unit %d has an invalid native-audio tier set", index)
		}
		manifestIDs = append(manifestIDs, atomizationUnitManifestIDs(unit)...)
		cursor = unit.EndMs
	}
	parentDurationMs := int64(*parent.DurationSec) * 1000
	if cursor != parentDurationMs {
		return fmt.Errorf("coverage ends at %dms, parent is %dms", cursor, parentDurationMs)
	}
	type coveragePoint struct {
		StartMs int64 `json:"start_ms"`
		EndMs   int64 `json:"end_ms"`
	}
	coverage := make([]coveragePoint, 0, len(units))
	for _, unit := range units {
		coverage = append(coverage, coveragePoint{StartMs: unit.StartMs, EndMs: unit.EndMs})
	}
	if generation.CoverageDigest != "" && generation.CoverageDigest != longFormDigest(coverage) {
		return errors.New("coverage digest does not match chapter units")
	}
	if len(manifestIDs) == 0 {
		return errors.New("no chapter artifact manifests recorded")
	}
	var verified int64
	if err := db.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND public_id IN ? AND state IN ? AND atomization_generation_id=?", generation.TenantID, manifestIDs, []string{manifestStateVerified, manifestStateActive}, generation.PublicID).Count(&verified).Error; err != nil {
		return err
	}
	if verified != int64(len(manifestIDs)) {
		return fmt.Errorf("only %d of %d chapter manifests are verified", verified, len(manifestIDs))
	}
	return nil
}
