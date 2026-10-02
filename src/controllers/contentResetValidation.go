package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type contentResetValidationIssue struct {
	Code   string `json:"code"`
	Owner  string `json:"owner"`
	Count  int64  `json:"count"`
	Reason string `json:"reason"`
}

type contentResetValidationResult struct {
	CampaignID             uuid.UUID                     `json:"campaign_id"`
	Revision               int                           `json:"revision"`
	ManifestHash           string                        `json:"manifest_hash,omitempty"`
	CheckedAt              time.Time                     `json:"checked_at"`
	TargetCount            int64                         `json:"target_count"`
	VerifiedTargetCount    int64                         `json:"verified_target_count"`
	ManifestIntegrityValid bool                          `json:"manifest_integrity_valid"`
	PreviewCurrent         bool                          `json:"preview_current"`
	ApprovalEligible       bool                          `json:"approval_eligible"`
	ExecutionEnabled       bool                          `json:"execution_enabled"`
	Issues                 []contentResetValidationIssue `json:"issues"`
	Blockers               []contentResetBlocker         `json:"blockers"`
}

type contentResetCompletionEvidence struct {
	State                  string                `json:"state"`
	Revision               int                   `json:"revision"`
	ManifestHash           string                `json:"manifest_hash"`
	TargetCount            int64                 `json:"target_count"`
	InventoryHighwater     int64                 `json:"inventory_highwater"`
	DeletionsSinceBoundary int64                 `json:"deletions_since_boundary"`
	ProtectedCount         int64                 `json:"protected_count"`
	UnknownDateCount       int64                 `json:"unknown_date_count"`
	UnattributedCount      int64                 `json:"unattributed_count"`
	MissingExplicitCount   int                   `json:"missing_explicit_count"`
	Blockers               []contentResetBlocker `json:"blockers"`
}

func validateContentResetPreview(tx *gorm.DB, tenant string, campaignID uuid.UUID) (contentResetValidationResult, error) {
	result := contentResetValidationResult{
		CampaignID: campaignID, CheckedAt: time.Now().UTC(),
		ManifestIntegrityValid: true, PreviewCurrent: true,
		ExecutionEnabled: false, ApprovalEligible: false,
		Issues: []contentResetValidationIssue{}, Blockers: []contentResetBlocker{},
	}
	issueCounts := make(map[string]int64)
	issueMeta := map[string]contentResetValidationIssue{
		"preview_incomplete":             {Code: "preview_incomplete", Owner: "cms/content-reset", Reason: "The campaign has no completed, frozen preview to validate."},
		"preview_expired":                {Code: "preview_expired", Owner: "cms/content-reset", Reason: "The preview approval window has expired."},
		"campaign_cancelled":             {Code: "campaign_cancelled", Owner: "cms/content-reset", Reason: "The campaign was cancelled and cannot be approved or executed."},
		"manifest_evidence_invalid":      {Code: "manifest_evidence_invalid", Owner: "cms/content-reset", Reason: "The stored completion evidence or request does not match the frozen revision."},
		"target_snapshot_invalid":        {Code: "target_snapshot_invalid", Owner: "cms/content-reset", Reason: "One or more immutable target snapshots or protection records fail their stored integrity checks."},
		"manifest_chain_invalid":         {Code: "manifest_chain_invalid", Owner: "cms/content-reset", Reason: "The ordered target chain does not match the frozen manifest chain hash."},
		"target_changed":                 {Code: "target_changed", Owner: "cms/content", Reason: "One or more selected content records changed after the preview was frozen."},
		"target_missing":                 {Code: "target_missing", Owner: "cms/content", Reason: "One or more selected content records no longer exist."},
		"protection_changed":             {Code: "protection_changed", Owner: "cms/retention", Reason: "Protection evidence changed after the preview was frozen."},
		"selection_changed":              {Code: "selection_changed", Owner: "cms/content", Reason: "The current selection no longer matches the frozen target set."},
		"content_deleted_after_boundary": {Code: "content_deleted_after_boundary", Owner: "cms/content", Reason: "Content was deleted after the preview inventory boundary."},
		"replay_sources_changed":         {Code: "replay_sources_changed", Owner: "cms/source-run", Reason: "A frozen replay source is missing, inactive, moved, or has a different configuration version."},
	}
	addIssue := func(code string, count int64) {
		if count > 0 {
			issueCounts[code] += count
		}
	}

	var campaign models.ContentResetCampaign
	if err := tx.Where("tenant_id = ? AND public_id = ?", tenant, campaignID).First(&campaign).Error; err != nil {
		return result, err
	}
	result.CampaignID = campaign.PublicID
	if campaign.State == "cancelled" {
		addIssue("campaign_cancelled", 1)
		result.PreviewCurrent = false
	}
	var revision models.ContentResetRevision
	if err := tx.Where("tenant_id = ? AND campaign_id = ? AND revision = ?", tenant, campaign.ID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return result, err
	}
	result.Revision = revision.Revision
	result.TargetCount = revision.TargetCount
	result.ManifestHash = ""
	if revision.ManifestHash != nil {
		result.ManifestHash = *revision.ManifestHash
	}

	var req contentResetPlanRequest
	if err := json.Unmarshal(revision.Request, &req); err != nil {
		addIssue("manifest_evidence_invalid", 1)
		result.ManifestIntegrityValid = false
	} else if hashContentResetValue(req) != campaign.RequestHash || req.Operation != campaign.Operation || req.Lane != campaign.Lane {
		addIssue("manifest_evidence_invalid", 1)
		result.ManifestIntegrityValid = false
	}
	if revision.ManifestHash == nil || len(result.ManifestHash) != 64 || revision.PlanningCompletedAt == nil ||
		(revision.State != "blocked" && revision.State != "previewed") {
		addIssue("preview_incomplete", 1)
		result.PreviewCurrent = false
	}
	if revision.ExpiresAt == nil || !result.CheckedAt.Before(*revision.ExpiresAt) {
		addIssue("preview_expired", 1)
		result.PreviewCurrent = false
	}

	var blockers []contentResetBlocker
	if err := json.Unmarshal(revision.Blockers, &blockers); err != nil {
		addIssue("manifest_evidence_invalid", 1)
		result.ManifestIntegrityValid = false
		blockers = []contentResetBlocker{}
	}
	result.Blockers = blockers

	var completionRow models.ContentResetEvidence
	err := tx.Where("tenant_id = ? AND campaign_id = ? AND evidence_key = ? AND evidence_type = ?", tenant, campaign.ID, "planning-completed", "planning_completed").First(&completionRow).Error
	var completion contentResetCompletionEvidence
	var originalDeletionCount int64
	if err != nil {
		addIssue("manifest_evidence_invalid", 1)
		result.ManifestIntegrityValid = false
	} else {
		canonical, canonicalErr := contentResetCanonicalJSON(completionRow.Payload)
		if canonicalErr != nil || hashContentResetValue(json.RawMessage(canonical)) != completionRow.PayloadHash ||
			json.Unmarshal(canonical, &completion) != nil {
			addIssue("manifest_evidence_invalid", 1)
			result.ManifestIntegrityValid = false
		} else {
			originalDeletionCount = completion.DeletionsSinceBoundary
			if completion.State != revision.State || completion.Revision != revision.Revision || completion.ManifestHash != result.ManifestHash ||
				completion.TargetCount != revision.TargetCount || completion.InventoryHighwater != revision.InventoryHighwater ||
				completion.ProtectedCount != revision.ProtectedCount || completion.UnknownDateCount != revision.UnknownDateCount ||
				completion.UnattributedCount != revision.UnattributedCount || completion.MissingExplicitCount != revision.MissingExplicitCount ||
				hashContentResetValue(completion.Blockers) != hashContentResetValue(blockers) {
				addIssue("manifest_evidence_invalid", 1)
				result.ManifestIntegrityValid = false
			}
		}
	}

	replaySources := []contentResetReplaySourceSnapshot{}
	if err := json.Unmarshal(revision.ReplaySourceSnapshot, &replaySources); err != nil || hashContentResetValue(replaySources) != revision.ReplaySourceHash {
		addIssue("manifest_evidence_invalid", 1)
		result.ManifestIntegrityValid = false
	}

	chain := strings.Repeat("0", 64)
	var cursor int64
	var protectedCount int64
	var verifiedTargetCount int64
	for {
		var targets []models.ContentResetTarget
		if err := tx.Where("tenant_id = ? AND revision_id = ? AND item_ordinal > ?", tenant, revision.ID, cursor).
			Order("item_ordinal ASC").Limit(contentResetPlanPageSize).Find(&targets).Error; err != nil {
			return result, err
		}
		if len(targets) == 0 {
			break
		}
		verifiedTargetCount += int64(len(targets))
		pageIDs := make([]uuid.UUID, 0, len(targets))
		for _, target := range targets {
			pageIDs = append(pageIDs, target.ContentItemID)
			if target.Protected {
				protectedCount++
			}
			canonical, canonicalErr := contentResetCanonicalJSON(target.Snapshot)
			var reasonCodes []string
			reasonErr := json.Unmarshal(target.ProtectionEvidence, &reasonCodes)
			if reasonCodes == nil && reasonErr == nil {
				reasonCodes = []string{}
			}
			if canonicalErr != nil || hashContentResetValue(json.RawMessage(canonical)) != target.SnapshotHash || reasonErr != nil ||
				target.Protected != (len(reasonCodes) > 0) || target.ProtectionReason != contentResetProtectionReason(reasonCodes) {
				addIssue("target_snapshot_invalid", 1)
				result.ManifestIntegrityValid = false
			}
			chain = hashContentResetValue([]any{
				chain, target.ItemOrdinal, target.SnapshotHash, target.Disposition, target.Protected, reasonCodes,
			})
			cursor = target.ItemOrdinal
		}

		var current []contentResetCandidate
		if err := tx.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id IN ?", tenant, pageIDs).
			Select("id, public_id, tenant_id, type, source, status, processing_generation, content_source_id, idempotency_key, original_url, source_episode_id, metadata, updated_at, created_at, published_at, story_id, parent_content_item_id").
			Find(&current).Error; err != nil {
			return result, err
		}
		currentByID := make(map[uuid.UUID]contentResetCandidate, len(current))
		for _, item := range current {
			currentByID[item.PublicID] = item
		}
		registered, err := contentResetRegisteredIdentities(tx, tenant, current)
		if err != nil {
			return result, fmt.Errorf("read current source-item identity evidence: %w", err)
		}
		protectionMap, err := retentionProtectedContentIDs(tx, tenant, pageIDs)
		if err != nil {
			return result, err
		}
		protectionReasons, err := contentResetProtectionReasons(tx, tenant, pageIDs)
		if err != nil {
			return result, err
		}
		for _, target := range targets {
			item, found := currentByID[target.ContentItemID]
			if !found {
				addIssue("target_missing", 1)
				result.PreviewCurrent = false
				continue
			}
			snapshot := contentResetTargetSnapshot(item)
			if identity, exists := registered[item.PublicID]; exists {
				snapshot["source_identity_hash"] = identity.Hash
				snapshot["source_identity_quality"] = identity.Quality
			}
			if hashContentResetValue(snapshot) != target.SnapshotHash || item.ID != target.ItemOrdinal || contentResetLaneForType(item.Type) != target.Lane {
				addIssue("target_changed", 1)
				result.PreviewCurrent = false
			}
			currentReasons := protectionReasons[item.PublicID]
			if currentReasons == nil {
				currentReasons = []string{}
			}
			if protectionMap[item.PublicID] != target.Protected || hashContentResetValue(currentReasons) != hashContentResetValue(decodeContentResetProtectionEvidence(target.ProtectionEvidence)) {
				addIssue("protection_changed", 1)
				result.PreviewCurrent = false
			}
		}

	}
	result.VerifiedTargetCount = verifiedTargetCount
	if verifiedTargetCount != revision.TargetCount || protectedCount != revision.ProtectedCount {
		addIssue("target_snapshot_invalid", 1)
		result.ManifestIntegrityValid = false
	}
	if chain != revision.ManifestChainHash {
		addIssue("manifest_chain_invalid", 1)
		result.ManifestIntegrityValid = false
	}

	// Re-run the frozen selection count against the same sequence boundary. Any
	// target leaving a filter is already caught by its current snapshot check;
	// a newly eligible row then changes this count without a second full scan.
	var selectedCount int64
	selectionQuery := applyContentResetSelection(tx.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND id <= ? AND inventory_sequence <= ?", tenant, revision.SelectionHighwater, revision.InventoryHighwater), req, true)
	if err := selectionQuery.Count(&selectedCount).Error; err != nil {
		return result, err
	}
	if selectedCount != revision.TargetCount {
		addIssue("selection_changed", absContentResetCount(selectedCount-revision.TargetCount))
		result.PreviewCurrent = false
	}

	currentDeletionCount, err := countContentResetDeletionsSinceBoundary(tx, tenant, revision, req)
	if err != nil {
		return result, err
	}
	if currentDeletionCount != originalDeletionCount {
		addIssue("content_deleted_after_boundary", currentDeletionCount)
		result.PreviewCurrent = false
	}
	if campaign.Operation == "fresh_start" {
		changed, err := contentResetReplaySourcesChanged(tx, tenant, revision)
		if err != nil {
			return result, err
		}
		if changed {
			addIssue("replay_sources_changed", 1)
			result.PreviewCurrent = false
		}
	}

	blockersBytes, err := json.Marshal(blockers)
	if err != nil {
		return result, err
	}
	if revision.ManifestHash != nil && json.Unmarshal(revision.Request, &req) == nil {
		expectedManifestHash := hashContentResetValue(map[string]any{
			"request_hash": hashContentResetValue(req), "selection_highwater": revision.SelectionHighwater,
			"inventory_highwater": revision.InventoryHighwater,
			"target_count":        revision.TargetCount, "protected_count": revision.ProtectedCount,
			"unknown_date_count": revision.UnknownDateCount, "unattributed_count": revision.UnattributedCount,
			"deletions_since_boundary": originalDeletionCount,
			"replay_source_hash":       revision.ReplaySourceHash, "chain_hash": revision.ManifestChainHash,
			"blockers": json.RawMessage(blockersBytes),
		})
		if expectedManifestHash != *revision.ManifestHash {
			addIssue("manifest_evidence_invalid", 1)
			result.ManifestIntegrityValid = false
		}
	}

	// Approval eligibility is derived from the current durable qualification
	// registry. A preview frozen before a contract was qualified stays blocked
	// and requires a fresh preview, so approval can never run against a stale
	// owner set.
	if revision.ManifestHash != nil {
		_, contractBlockers := contentResetContractFor(req)
		result.ExecutionEnabled = len(contractBlockers) == 0
	}
	result.ApprovalEligible = result.ManifestIntegrityValid && result.PreviewCurrent && len(issueCounts) == 0 && len(blockers) == 0 && result.ExecutionEnabled
	issueCodes := make([]string, 0, len(issueCounts))
	for code := range issueCounts {
		issueCodes = append(issueCodes, code)
	}
	sort.Strings(issueCodes)
	for _, code := range issueCodes {
		issue := issueMeta[code]
		issue.Count = issueCounts[code]
		result.Issues = append(result.Issues, issue)
	}
	return result, nil
}

func contentResetProtectionReason(reasons []string) string {
	reason := strings.Join(reasons, ",")
	if len(reason) > 64 {
		return reason[:64]
	}
	return reason
}

func decodeContentResetProtectionEvidence(raw []byte) []string {
	var reasons []string
	if json.Unmarshal(raw, &reasons) != nil || reasons == nil {
		return []string{}
	}
	return reasons
}

func absContentResetCount(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func ValidateContentResetCampaign(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	campaignID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var validation contentResetValidationResult
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY").Error; err != nil {
			return err
		}
		var validateErr error
		validation, validateErr = validateContentResetPreview(tx, principal.TenantID, campaignID)
		return validateErr
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Content Reset preview validation could not complete"})
		return
	}
	c.JSON(http.StatusOK, validation)
}
