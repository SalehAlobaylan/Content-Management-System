package controllers

import (
	"content-management-system/src/models"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"gorm.io/gorm"
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
