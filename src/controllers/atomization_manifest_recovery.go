package controllers

import (
	"content-management-system/src/models"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"time"
)

// A nested atomization unit owns its effects through a short renewable lease.
// Reconciliation waits only for this lease plus a small clock/transport safety
// margin; the generic source-artifact path intentionally remains conservative.
const atomizationObservationSafetyWindow = 30 * time.Second

// Recovery observes the immutable attempt-owned key after its execution lease
// has expired; it never grants fresh execution authority to that attempt.
func validateSourceArtifactObservation(db *gorm.DB, manifest models.MediaArtifactManifest, req artifactManifestTransitionRequest) error {
	if manifest.CreatorRole == "aggregation_quality_worker" {
		return validateQualityArtifactObservation(db, manifest, req)
	}
	if manifest.AttemptID == nil || manifest.FenceToken == nil || manifest.ContentItemID == nil || manifest.AtomizationGenerationID != nil || manifest.TranscriptionGenerationID != nil || manifest.TranscriptionSegmentUnitID != nil {
		return fmt.Errorf("source observation requires unambiguous content-stage ownership")
	}
	if manifest.State != "uploading" && manifest.State != "uploaded" && manifest.State != "uncertain" {
		return fmt.Errorf("artifact is not awaiting reconciliation")
	}
	var attempt models.ContentStageAttempt
	if err := db.Where("tenant_id=? AND public_id=? AND fence_token=? AND lease_expires_at<?", manifest.TenantID, manifest.AttemptID, manifest.FenceToken, time.Now().UTC().Add(-15*time.Minute)).First(&attempt).Error; err != nil {
		return err
	}
	if attempt.EffectStartedAt == nil {
		return fmt.Errorf("attempt has no effect-start evidence")
	}
	var request models.ContentStageRequest
	if err := db.Where("tenant_id=? AND public_id=? AND content_item_id=? AND stage=?", manifest.TenantID, attempt.RequestID, manifest.ContentItemID, models.ContentStagePodsMediaArtifacts).First(&request).Error; err != nil {
		return err
	}
	if req.State == "failed" && req.TerminalProof["object_present"] == false {
		return nil
	}
	if req.State != "verified" || req.SizeBytes == nil || *req.SizeBytes != manifest.SizeBytes || manifest.SizeBytes <= 0 || req.ContentType != manifest.ContentType {
		return fmt.Errorf("source observation requires confirmed absence or matching immutable bytes")
	}
	observed, _ := req.VerificationEvidence["provider_checksum_sha256"].(string)
	expected, _ := hex.DecodeString(manifest.SHA256)
	if len(expected) != 32 || (observed != manifest.SHA256 && observed != base64.StdEncoding.EncodeToString(expected)) {
		return fmt.Errorf("source observation requires matching SHA256")
	}
	return nil
}

func validateQualityArtifactObservation(db *gorm.DB, manifest models.MediaArtifactManifest, req artifactManifestTransitionRequest) error {
	if req.State != manifestStateVerified || manifest.ArtifactRole != "playback_mp4" || manifest.ContentItemID == nil || manifest.ParentContentItemID != nil || manifest.AttemptID != nil || manifest.AtomizationGenerationID != nil || manifest.AtomizationChapterUnitID != nil || manifest.TranscriptionGenerationID != nil || manifest.TranscriptionSegmentUnitID != nil || manifest.FenceToken == nil {
		return fmt.Errorf("quality observation requires an item-owned playback artifact and stable fence")
	}
	if manifest.State != manifestStateUploaded && manifest.State != manifestStateUncertain {
		return fmt.Errorf("quality artifact is not awaiting reconciliation")
	}
	producer, err := uuid.Parse(strings.TrimSpace(req.ProducerEventID))
	if err != nil || producer != manifest.ProducerEventID {
		return fmt.Errorf("quality producer identity mismatch")
	}
	fence, err := uuid.Parse(strings.TrimSpace(req.FenceToken))
	if err != nil || fence != *manifest.FenceToken {
		return fmt.Errorf("quality artifact fence mismatch")
	}
	if req.SizeBytes == nil || *req.SizeBytes != manifest.SizeBytes || manifest.SizeBytes <= 0 || req.ContentType != manifest.ContentType || manifest.ContentType != "video/mp4" || len(manifest.SHA256) != 64 {
		return fmt.Errorf("quality observation requires exact immutable size, type, and digest")
	}
	observedChecksum, _ := req.VerificationEvidence["provider_checksum_sha256"].(string)
	if observedChecksum != manifest.SHA256 || req.VerificationEvidence["provider_head_verified"] != true {
		return fmt.Errorf("quality observation requires provider HEAD and exact streamed SHA256 evidence")
	}
	if req.ETag == "" {
		return fmt.Errorf("quality observation requires provider ETag evidence")
	}
	if manifest.ETag != "" && !strings.EqualFold(strings.Trim(manifest.ETag, `"`), strings.Trim(req.ETag, `"`)) {
		return fmt.Errorf("quality observation ETag differs from the recorded provider receipt")
	}
	prefix := "content/" + manifest.ContentItemID.String() + "/processed"
	version := 0
	switch {
	case manifest.ObjectKey == prefix+".mp4":
		version = 1
	case strings.HasPrefix(manifest.ObjectKey, prefix+".v") && strings.HasSuffix(manifest.ObjectKey, ".mp4"):
		raw := strings.TrimSuffix(strings.TrimPrefix(manifest.ObjectKey, prefix+".v"), ".mp4")
		version, err = strconv.Atoi(raw)
		if err != nil || version < 2 {
			return fmt.Errorf("quality object key has an invalid media version")
		}
	default:
		return fmt.Errorf("quality object key is outside its canonical item version namespace")
	}
	var item models.ContentItem
	if err := db.Where("tenant_id=? AND public_id=?", manifest.TenantID, *manifest.ContentItemID).First(&item).Error; err != nil {
		return fmt.Errorf("quality content owner is unavailable")
	}
	if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast || item.Status != models.ContentStatusReady || item.RetiredPayloadAt != nil {
		return fmt.Errorf("quality artifact owner is not an active Pods item")
	}
	var retirementCount int64
	if err := db.Model(&models.PodsResetRetirement{}).Where("tenant_id=? AND content_item_id=? AND state IN ?", item.TenantID, item.PublicID, []string{"retiring", "retired"}).Count(&retirementCount).Error; err != nil || retirementCount != 0 {
		return fmt.Errorf("quality artifact owner is or may be permanently retired")
	}
	if version != item.MediaVersion+1 && version != item.MediaVersion {
		return fmt.Errorf("quality object version is not the current or next item media version")
	}
	if version == item.MediaVersion && (item.MediaURL == nil || *item.MediaURL != manifest.PublicURL) {
		return fmt.Errorf("current item media URL does not identify the observed quality artifact")
	}
	return nil
}

// This permits observation only, never another upload or rotating a unit's
// execution fence. Unknown/legacy ownership remains an operator decision.
func validateChapterArtifactObservation(db *gorm.DB, manifest models.MediaArtifactManifest, req artifactManifestTransitionRequest) error {
	if manifest.AtomizationChapterUnitID == nil || manifest.AtomizationGenerationID == nil || manifest.UnitFenceToken == nil || manifest.OuterFenceToken == nil || manifest.AttemptID == nil || manifest.ParentContentItemID == nil {
		return fmt.Errorf("unambiguous nested authority is required")
	}
	if req.State != "verified" && req.State != "failed" {
		return fmt.Errorf("reconciliation may only record verified bytes or confirmed absence")
	}
	var unit models.AtomizationChapterUnit
	if err := db.Where("tenant_id=? AND public_id=? AND generation_id=? AND fence_token=?", manifest.TenantID, manifest.AtomizationChapterUnitID, manifest.AtomizationGenerationID, manifest.UnitFenceToken).First(&unit).Error; err != nil {
		return err
	}
	if unit.EffectStartedAt == nil || unit.LeaseExpiresAt == nil || unit.LeaseExpiresAt.After(time.Now().UTC().Add(-atomizationObservationSafetyWindow)) {
		return fmt.Errorf("effect is live or has no quiescence proof")
	}
	if unit.State != "uncertain" && unit.State != "running" && unit.State != "verifying" && unit.State != "verified" {
		return fmt.Errorf("unit is not awaiting effect reconciliation")
	}
	var generation models.AtomizationGeneration
	if err := db.Where("tenant_id=? AND public_id=? AND parent_content_item_id=? AND state NOT IN ?", manifest.TenantID, manifest.AtomizationGenerationID, manifest.ParentContentItemID, []string{"superseded", "active"}).First(&generation).Error; err != nil {
		return err
	}
	var outer models.ContentStageAttempt
	if generation.ContentStageRequestID != nil {
		if err := db.Where("tenant_id=? AND public_id=? AND request_id=? AND fence_token=? AND lease_expires_at<?", manifest.TenantID, manifest.AttemptID, generation.ContentStageRequestID, manifest.OuterFenceToken, time.Now().UTC()).First(&outer).Error; err != nil {
			return err
		}
	} else {
		var governed models.AtomizationWorkAttempt
		if err := db.Where("tenant_id=? AND public_id=? AND request_id=? AND fence_token=? AND lease_expires_at<?", manifest.TenantID, manifest.AttemptID, generation.WorkRequestID, manifest.OuterFenceToken, time.Now().UTC()).First(&governed).Error; err != nil {
			return err
		}
	}
	if req.State == "failed" {
		if req.TerminalProof["object_present"] != false {
			return fmt.Errorf("provider absence observation required")
		}
		return nil
	}
	if req.SizeBytes == nil || *req.SizeBytes != manifest.SizeBytes || manifest.SizeBytes <= 0 || req.ContentType != manifest.ContentType {
		return fmt.Errorf("provider metadata differs from immutable upload intent")
	}
	observed, _ := req.VerificationEvidence["provider_checksum_sha256"].(string)
	expectedBytes, _ := hex.DecodeString(manifest.SHA256)
	checksumMatches := len(expectedBytes) == 32 && (observed == manifest.SHA256 || observed == base64.StdEncoding.EncodeToString(expectedBytes))
	// An ETag already persisted from this upload identifies the exact object
	// receipt. Never learn a new ETag from an uncertain object by size alone.
	receiptMatches := manifest.ETag != "" && strings.Trim(req.ETag, "\"") == strings.Trim(manifest.ETag, "\"") && manifest.SHA256 != ""
	if !checksumMatches && !receiptMatches {
		return fmt.Errorf("object checksum or original upload receipt required")
	}
	return nil
}
