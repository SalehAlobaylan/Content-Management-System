package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/podsreset"
	"content-management-system/src/utils"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// podsResetExecutionError carries the HTTP-compatible failure classification of
// an in-process Pods reset execution. The standalone handler maps it to a
// response.
type podsResetExecutionError struct {
	Status  int
	Code    string
	Message string
}

func (e *podsResetExecutionError) Error() string { return e.Message }

type podsResetExecutionOutcome struct {
	Run      models.PodsResetRun
	Items    []models.PodsResetItem
	Totals   podsResetTotals
	State    string
	Phase    string
	Warning  string
	Rollback string
}

// executePodsResetRunInProcess is the single Pods retirement state machine.
// The standalone HTTP handler retains the Plan 119 owner semantics.
func executePodsResetRunInProcess(db *gorm.DB, principal utils.AdminPrincipal, run models.PodsResetRun) (podsResetExecutionOutcome, error) {
	outcome := podsResetExecutionOutcome{Run: run, State: "partial", Rollback: "unavailable"}
	if run.PauseRequested {
		return outcome, &podsResetExecutionError{Status: http.StatusConflict, Code: "RESET_PAUSED", Message: "reset is paused; explicitly resume it before execution"}
	}
	if run.State != "approved" && run.State != "executing" && run.State != "partial" {
		return outcome, &podsResetExecutionError{Status: http.StatusConflict, Message: "run is not approved or resumable"}
	}
	if run.ContentResetCampaignID != nil {
		return outcome, &podsResetExecutionError{Status: http.StatusGone, Code: "CONTENT_RESET_DISCONTINUED", Message: "campaign-delegated Pods reset is discontinued"}
	}
	if run.PolicyVersion != podsreset.PolicyVersion {
		return outcome, &podsResetExecutionError{Status: http.StatusConflict, Code: "POLICY_DRIFT", Message: "reset policy changed after preview; create a fresh preview"}
	}
	currentSchema, err := podsResetSchemaFingerprint(db)
	if err != nil || currentSchema != run.SchemaFingerprint {
		return outcome, &podsResetExecutionError{Status: http.StatusConflict, Code: "SCHEMA_DRIFT", Message: "content reference schema changed after preview; no further actions allowed"}
	}
	manifestHash, err := podsResetManifestHash(run.Manifest)
	if err != nil || manifestHash != run.ManifestHash {
		return outcome, &podsResetExecutionError{Status: http.StatusConflict, Message: "persisted manifest failed integrity validation"}
	}
	run, err = podsResetAcquireExecutionClaim(db, principal.TenantID, run.PublicID)
	if err != nil {
		if isLifecycleConflict(err) {
			return outcome, &podsResetExecutionError{Status: http.StatusConflict, Code: "OPERATION_CONFLICT", Message: "an active lifecycle campaign holds one or more Pods reset targets"}
		}
		return outcome, &podsResetExecutionError{Status: http.StatusConflict, Code: "RESET_EXECUTOR_BUSY", Message: err.Error()}
	}
	outcome.Run = run
	defer func() { _ = podsResetReleaseExecutionClaim(db, run) }()
	var items []models.PodsResetItem
	if err := db.Where("run_id=?", run.PublicID).Order("ordinal").Find(&items).Error; err != nil {
		return outcome, &podsResetExecutionError{Status: http.StatusInternalServerError, Message: "could not load reset state"}
	}
	processedItems := 0
	pauseAtBoundary := false
	verificationWait := false
	fencingWait := false
	// Fence the complete approved selection in bounded passes before any files
	// are removed. Selected chapters may still reference a selected parent folder.
	for _, fenceOnly := range []bool{true, false} {
		processedItems = 0
		if !fenceOnly {
			if err := db.Where("run_id=?", run.PublicID).Order("ordinal").Find(&items).Error; err != nil {
				return outcome, &podsResetExecutionError{Status: http.StatusInternalServerError, Message: "could not load fenced reset selection"}
			}
		}
		for _, item := range items {
			if item.State == "complete" {
				continue
			}
			if !fenceOnly && item.State == "verification_pending" && item.VerificationProbeCount == 1 && item.VerificationNotBefore != nil && time.Now().UTC().Before(*item.VerificationNotBefore) {
				verificationWait = true
				continue
			}
			if fenceOnly && item.State != "planned" {
				continue
			}
			if processedItems >= podsreset.MaxBatchItems {
				break
			}
			processedItems++
			var pauseRequested bool
			if err := db.Model(&models.PodsResetRun{}).Select("pause_requested").Where("public_id=?", run.PublicID).Scan(&pauseRequested).Error; err != nil {
				podsResetMarkPartial(db, &run, &item, "could not read pause control at item boundary")
				break
			}
			if pauseRequested {
				pauseAtBoundary = true
				break
			}
			if item.State == "blocked" {
				return outcome, &podsResetExecutionError{Status: http.StatusConflict, Message: "approved run contains a blocked target"}
			}
			if err := podsResetRenewExecutionClaim(db, &run); err != nil {
				return outcome, &podsResetExecutionError{Status: http.StatusConflict, Code: "RESET_EXECUTOR_FENCE_LOST", Message: "reset execution lease was lost"}
			}
			if item.State == "planned" {
				var content models.ContentItem
				if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, item.ContentItemID).First(&content).Error; err != nil {
					podsResetMarkPartial(db, &run, &item, "selected content identity disappeared before fence")
					break
				}
				hash, _ := podsreset.Hash(podsreset.SnapshotFor(content))
				if hash != item.SnapshotHash {
					podsResetMarkPartial(db, &run, &item, "selected content changed after preview")
					break
				}
				var allIDs []uuid.UUID
				for _, row := range items {
					allIDs = append(allIDs, row.ContentItemID)
				}
				blockers, _, checkErr := podsResetItemBlockers(db, principal.TenantID, content, uuidSet(allIDs))
				if checkErr == nil {
					blockers, checkErr = podsResetRevalidateProtectionOverrides(run, item.ContentItemID, blockers)
				}
				if checkErr != nil || len(blockers) > 0 {
					reason := "preflight protection or work state changed after approval"
					if checkErr != nil {
						reason = "preflight revalidation failed: " + checkErr.Error()
					}
					podsResetMarkPartial(db, &run, &item, reason)
					break
				}
				var frozenManifest struct {
					Targets []podsResetManifestTarget `json:"targets"`
				}
				if err := json.Unmarshal(run.Manifest, &frozenManifest); err != nil {
					podsResetMarkPartial(db, &run, &item, "approved object inventory could not be decoded")
					break
				}
				var frozenTarget *podsResetManifestTarget
				for index := range frozenManifest.Targets {
					if frozenManifest.Targets[index].ID == item.ContentItemID {
						frozenTarget = &frozenManifest.Targets[index]
						break
					}
				}
				if frozenTarget == nil {
					podsResetMarkPartial(db, &run, &item, "approved target inventory is missing")
					break
				}
				inventoryBody, inventoryStatus, inventoryErr := callAggregationInternal(http.MethodPost, "/internal/pods-reset/inventory", map[string]any{"tenant_id": run.TenantID, "content_ids": []string{item.ContentItemID.String()}})
				var latestInventory podsResetInventoryResponse
				if inventoryErr != nil || inventoryStatus < 200 || inventoryStatus >= 300 || json.Unmarshal(inventoryBody, &latestInventory) != nil || !latestInventory.Data.Complete || len(latestInventory.Data.Items) != 1 || latestInventory.Data.Items[0].ContentItemID != item.ContentItemID.String() {
					podsResetMarkPartial(db, &run, &item, "storage inventory could not be revalidated before retirement")
					break
				}
				if frozenTarget.StorageVersionModel != latestInventory.Data.VersionModel || frozenTarget.StorageVersionModel != "cloudflare-r2-current-key-delete-v1" || !podsResetSameStrings(frozenTarget.StorageTiers, latestInventory.Data.ConfiguredTiers) || podsreset.ValidateStorageBindings(frozenTarget.StorageTiers, latestInventory.Data.StorageBindings) != nil || !podsreset.SameStorageBindings(frozenTarget.StorageBindings, latestInventory.Data.StorageBindings) || !podsResetObjectsMatchStorageBindings(latestInventory.Data.Items[0].Objects, latestInventory.Data.StorageBindings) || podsResetInventoryChanged(frozenTarget.Objects, latestInventory.Data.Items[0].Objects) {
					podsResetMarkPartial(db, &run, &item, "storage tiers or object fingerprints changed after approval; create a fresh preview")
					break
				}
				if err := podsResetFenceItem(db, run, &item, &content, uuidSet(allIDs)); err != nil {
					podsResetMarkPartial(db, &run, &item, "retirement fence failed: "+err.Error())
					break
				}
				item.State = "fenced"
			}
			if fenceOnly {
				continue
			}
			if !podsResetItemNeedsProcessing(item.State) {
				continue
			}
			if item.State == "fenced" {
				var objectRows []models.PodsResetObject
				if err := db.Where("run_id=? AND content_item_id=?", run.PublicID, item.ContentItemID).Order("storage_tier,bucket,object_key").Find(&objectRows).Error; err != nil {
					podsResetMarkPartial(db, &run, &item, "could not load frozen object set")
					break
				}
				for _, row := range objectRows {
					if row.State == "blocked" {
						podsResetMarkPartial(db, &run, &item, "a frozen object is blocked")
						break
					}
				}
				if run.State == "partial" {
					break
				}
				if err := db.Model(&models.PodsResetObject{}).Where("run_id=? AND content_item_id=? AND state='planned'", run.PublicID, item.ContentItemID).Updates(map[string]interface{}{"state": "deleting", "attempt_count": gorm.Expr("attempt_count + 1"), "updated_at": time.Now().UTC()}).Error; err != nil {
					podsResetMarkPartial(db, &run, &item, "could not persist object deletion attempt")
					break
				}
				objects := make([]podsResetObjectWire, 0, len(objectRows))
				for _, row := range objectRows {
					objects = append(objects, podsResetObjectWire{StorageTier: row.StorageTier, Bucket: row.Bucket, ObjectKey: row.ObjectKey, ETag: row.ETag, SizeBytes: row.SizeBytes})
				}
				storageTarget := itemStorageTarget(run.Manifest, item.ContentItemID)
				if storageTarget == nil {
					podsResetMarkPartial(db, &run, &item, "approved storage binding is missing")
					break
				}
				payload := map[string]any{"run_id": run.PublicID.String(), "tenant_id": run.TenantID, "content_item_id": item.ContentItemID.String(), "manifest_hash": run.ManifestHash, "fencing_token": run.FencingToken.String(), "execution_token": run.ExecutionToken.String(), "storage_bindings": storageTarget.StorageBindings, "objects": objects}
				// The owner must call back into CMS before deleting and then verify
				// storage absence. Its authorization alone may take up to a minute.
				body, status, callErr := callAggregationInternalWithTimeout(http.MethodPost, "/internal/pods-reset/delete-media-item", payload, 2*time.Minute)
				if callErr != nil || status < 200 || status >= 300 {
					podsResetMarkPartial(db, &run, &item, podsResetDeletionFailureMessage(status, body, callErr))
					break
				}
				var response podsResetDeleteResponse
				if err := json.Unmarshal(body, &response); err != nil || !response.Data.ObjectsAbsent || response.Data.ContentItemID != item.ContentItemID.String() || response.Data.FencingToken != run.FencingToken.String() || !podsreset.SameStorageBindings(storageTarget.StorageBindings, response.Data.StorageBindings) {
					podsResetMarkPartial(db, &run, &item, "provider result did not prove exact object absence")
					break
				}
				actualDeleted := podsResetObjectSet(response.Data.DeletedObjects)
				absent := podsResetObjectSet(response.Data.AlreadyAbsentObjects)
				if outcomeErr := podsResetValidateObjectOutcomes(objectRows, response.Data.DeletedObjects, response.Data.AlreadyAbsentObjects, response.Data.DeletedCount); outcomeErr != nil {
					podsResetMarkPartial(db, &run, &item, outcomeErr.Error())
					break
				}
				var freedBytes int64
				for _, row := range objectRows {
					if row.State == "deleted" {
						continue
					}
					identity := podsResetObjectKey(row.StorageTier, row.Bucket, row.ObjectKey)
					if !actualDeleted[identity] && !absent[identity] {
						podsResetMarkPartial(db, &run, &item, "provider result omitted an approved object outcome")
						break
					}
					freed := int64(0)
					if actualDeleted[identity] {
						freed = row.SizeBytes
						freedBytes += freed
					}
					if err := db.Model(&models.PodsResetObject{}).Where("run_id=? AND storage_tier=? AND bucket=? AND object_key=?", run.PublicID, row.StorageTier, row.Bucket, row.ObjectKey).
						Updates(map[string]interface{}{"state": "deleted", "deleted_by_run": actualDeleted[identity], "freed_bytes": freed, "deleted_at": time.Now().UTC(), "error": "", "updated_at": time.Now().UTC()}).Error; err != nil {
						podsResetMarkPartial(db, &run, &item, "could not persist exact object outcome")
						break
					}
				}
				if run.State == "partial" {
					break
				}
				storageTarget = itemStorageTarget(run.Manifest, item.ContentItemID)
				if storageTarget == nil {
					podsResetMarkPartial(db, &run, &item, "approved storage target is missing after deletion")
					break
				}
				probeAt := time.Now().UTC()
				verificationEvidence, err := podsResetAppendVerificationProbe(item.VerificationEvidence, podsResetVerificationProbe{
					Number: 1, ObservedAt: probeAt, ObjectsAbsent: true, ObservedObjectCount: 0,
					StorageVersionModel: storageTarget.StorageVersionModel,
					StorageTiers:        append([]string(nil), storageTarget.StorageTiers...),
					StorageBindings:     append([]podsreset.StorageBinding(nil), storageTarget.StorageBindings...),
				})
				if err != nil {
					podsResetMarkPartial(db, &run, &item, "could not persist first origin-absence evidence")
					break
				}
				nextProbeAt := probeAt.Add(podsreset.VerificationProbeInterval)
				itemResult := db.Model(&models.PodsResetItem{}).Where("id=? AND state='fenced'", item.ID).Updates(map[string]interface{}{
					"state": "verification_pending", "verification_probe_count": 1,
					"verification_not_before": nextProbeAt, "verification_evidence": verificationEvidence,
					"last_error": "", "updated_at": probeAt,
				})
				if itemResult.Error != nil || itemResult.RowsAffected != 1 {
					podsResetMarkPartial(db, &run, &item, "could not persist object absence proof")
					break
				}
				item.State = "verification_pending"
				item.VerificationProbeCount = 1
				item.VerificationNotBefore = &nextProbeAt
				item.VerificationEvidence = verificationEvidence
				_ = freedBytes
			}
			if item.State == "verification_pending" {
				if item.VerificationProbeCount != 1 || item.VerificationNotBefore == nil {
					podsResetMarkPartial(db, &run, &item, "durable second-probe state is incomplete")
					break
				}
				now := time.Now().UTC()
				if now.Before(*item.VerificationNotBefore) {
					verificationWait = true
					_ = db.Model(&models.PodsResetItem{}).Where("id=? AND state='verification_pending'", item.ID).Update("last_error", "verification_probe_not_due").Error
					continue
				}
				inventoryBody, inventoryStatus, inventoryErr := callAggregationInternal(http.MethodPost, "/internal/pods-reset/inventory", map[string]any{"tenant_id": run.TenantID, "content_ids": []string{item.ContentItemID.String()}})
				var latestInventory podsResetInventoryResponse
				if inventoryErr != nil || inventoryStatus < 200 || inventoryStatus >= 300 || json.Unmarshal(inventoryBody, &latestInventory) != nil || !latestInventory.Data.Complete || len(latestInventory.Data.Items) != 1 || latestInventory.Data.Items[0].ContentItemID != item.ContentItemID.String() {
					podsResetMarkPartial(db, &run, &item, "second storage verification could not prove a complete inventory")
					break
				}
				frozenTarget := itemStorageTarget(run.Manifest, item.ContentItemID)
				if frozenTarget == nil || frozenTarget.StorageVersionModel != latestInventory.Data.VersionModel || frozenTarget.StorageVersionModel != "cloudflare-r2-current-key-delete-v1" || !podsResetSameStrings(frozenTarget.StorageTiers, latestInventory.Data.ConfiguredTiers) || podsreset.ValidateStorageBindings(frozenTarget.StorageTiers, latestInventory.Data.StorageBindings) != nil || !podsreset.SameStorageBindings(frozenTarget.StorageBindings, latestInventory.Data.StorageBindings) {
					podsResetMarkPartial(db, &run, &item, "storage tier or version model changed before the second verification")
					break
				}
				observedObjects := latestInventory.Data.Items[0].Objects
				if len(observedObjects) != 0 {
					podsResetMarkPartial(db, &run, &item, "second storage verification found a late or unapproved object; it was not deleted")
					break
				}
				probeCount, code := podsreset.AdvanceVerificationProbe(item.VerificationProbeCount, true, now, *item.VerificationNotBefore)
				if code != "" {
					podsResetMarkPartial(db, &run, &item, code)
					break
				}
				verificationEvidence, err := podsResetAppendVerificationProbe(item.VerificationEvidence, podsResetVerificationProbe{
					Number: 2, ObservedAt: now, ObjectsAbsent: true, ObservedObjectCount: len(observedObjects),
					StorageVersionModel: latestInventory.Data.VersionModel,
					StorageTiers:        append([]string(nil), latestInventory.Data.ConfiguredTiers...),
					StorageBindings:     append([]podsreset.StorageBinding(nil), latestInventory.Data.StorageBindings...),
				})
				if err != nil {
					podsResetMarkPartial(db, &run, &item, "could not persist second origin-absence evidence")
					break
				}
				itemResult := db.Model(&models.PodsResetItem{}).Where("id=? AND state='verification_pending' AND verification_probe_count=1", item.ID).Updates(map[string]interface{}{
					"state": "objects_deleted", "verification_probe_count": probeCount,
					"verification_not_before": nil, "verification_evidence": verificationEvidence,
					"last_error": "", "updated_at": now,
				})
				if itemResult.Error != nil || itemResult.RowsAffected != 1 {
					podsResetMarkPartial(db, &run, &item, "could not durably persist the second origin-absence proof")
					break
				}
				item.State = "objects_deleted"
				item.VerificationProbeCount = probeCount
			}
			if item.State == "objects_deleted" {
				if err := podsResetFinalizeItem(db, run, &item); err != nil {
					podsResetMarkPartial(db, &run, &item, "metadata finalization failed: "+err.Error())
					break
				}
				item.State = "complete"
			}
		}
		if fenceOnly {
			ready, err := podsResetSelectionFenced(db, run)
			if err != nil {
				return outcome, &podsResetExecutionError{Status: http.StatusConflict, Message: "could not verify the complete retirement fence"}
			}
			if !ready || pauseAtBoundary || run.State == "partial" {
				fencingWait = !ready
				break
			}
		}
	}
	if err := db.Where("run_id=?", run.PublicID).Order("ordinal").Find(&items).Error; err != nil {
		return outcome, &podsResetExecutionError{Status: http.StatusInternalServerError, Message: "could not read back reset progress"}
	}
	outcome.Items = items
	var persistedRun models.PodsResetRun
	if err := db.Select("public_id", "state", "phase", "error").Where("tenant_id=? AND public_id=?", principal.TenantID, run.PublicID).First(&persistedRun).Error; err != nil {
		return outcome, &podsResetExecutionError{Status: http.StatusInternalServerError, Message: "could not read back reset run state"}
	}
	outcome.Phase = persistedRun.Phase
	if persistedRun.State == "cancelled" {
		outcome.State = "cancelled"
		outcome.Rollback = "no irreversible effect began"
		return outcome, nil
	}
	var remaining int64
	if err := db.Model(&models.PodsResetItem{}).Where("run_id=? AND state <> 'complete'", run.PublicID).Count(&remaining).Error; err != nil {
		podsResetMarkPartial(db, &run, &models.PodsResetItem{RunID: run.PublicID}, "could not verify terminal item accounting")
		return outcome, &podsResetExecutionError{Status: http.StatusAccepted, Message: "terminal accounting is unavailable"}
	}
	if remaining > 0 && persistedRun.State == "partial" && persistedRun.Phase == "partial" {
		outcome.State = "partial"
		outcome.Warning = persistedRun.Error
		outcome.Totals, _ = podsResetResultTotals(db, run.PublicID)
		return outcome, nil
	}
	if remaining == 0 {
		totals, totalsErr := podsResetResultTotals(db, run.PublicID)
		if totalsErr != nil {
			podsResetMarkPartial(db, &run, &models.PodsResetItem{RunID: run.PublicID}, "completed rows have incomplete result accounting")
			_ = podsResetReleaseExecutionClaim(db, run)
			return outcome, &podsResetExecutionError{Status: http.StatusAccepted, Message: "result totals unavailable"}
		}
		completion := db.Model(&models.PodsResetRun{}).Where("tenant_id=? AND public_id=? AND state='executing' AND execution_token=?", principal.TenantID, run.PublicID, run.ExecutionToken).
			Updates(map[string]interface{}{"state": "complete", "phase": "purge_only_complete", "error": "", "execution_token": nil, "execution_lease_until": nil, "updated_at": time.Now().UTC()})
		if completion.Error != nil || completion.RowsAffected != 1 {
			podsResetMarkPartial(db, &run, &models.PodsResetItem{RunID: run.PublicID}, "could not durably record reset completion")
			return outcome, &podsResetExecutionError{Status: http.StatusAccepted, Message: "completion write did not persist; retry to verify"}
		}
		retentionAudit(db, principal, "pods_reset.complete", run.PublicID.String(), "success", map[string]interface{}{"manifest_hash": run.ManifestHash})
		if err := podsResetReleaseExecutionClaim(db, run); err != nil {
			outcome.Warning = "execution lease cleanup pending"
		}
		outcome.State = "complete"
		outcome.Totals = totals
		return outcome, nil
	}
	run.State = "partial"
	phase := "batch_complete"
	if pauseAtBoundary {
		phase = "paused"
	} else if fencingWait {
		phase = "fencing"
	} else if verificationWait {
		phase = "verification_wait"
	}
	_ = db.Model(&models.PodsResetRun{}).Where("tenant_id=? AND public_id=? AND state='executing' AND execution_token=?", principal.TenantID, run.PublicID, run.ExecutionToken).Updates(map[string]interface{}{"state": "partial", "phase": phase, "updated_at": time.Now().UTC()}).Error
	_ = podsResetReleaseExecutionClaim(db, run)
	retentionAudit(db, principal, "pods_reset.partial", run.PublicID.String(), "partial", map[string]interface{}{"manifest_hash": run.ManifestHash})
	totals, _ := podsResetResultTotals(db, run.PublicID)
	outcome.State = "partial"
	outcome.Phase = phase
	outcome.Totals = totals
	return outcome, nil
}

func podsResetDeletionFailureMessage(status int, body []byte, err error) string {
	if err != nil {
		if os.IsTimeout(err) {
			return "Aggregation file deletion timed out; the result is unknown. Saved progress is retained for continuation."
		}
		return "CMS could not contact Aggregation for file deletion. Saved progress is retained for continuation."
	}
	var failure struct {
		Code      string `json:"code"`
		CMSStatus int    `json:"cms_status"`
	}
	_ = json.Unmarshal(body, &failure)
	cmsStatus := ""
	if failure.CMSStatus >= 400 && failure.CMSStatus <= 599 {
		cmsStatus = fmt.Sprintf(" (HTTP %d)", failure.CMSStatus)
	}
	switch failure.Code {
	case "pods_reset_cms_authorization_failed":
		return "Aggregation could not complete CMS reset authorization" + cmsStatus + "; file deletion was not started. Continue this saved run after CMS is reachable."
	case "pods_reset_cms_retirement_read_failed":
		return "Aggregation could not read the retired content from CMS" + cmsStatus + "; file deletion was not started. Continue this saved run after CMS is reachable."
	case "pods_reset_storage_deletion_failed":
		return "Aggregation storage deletion or file-absence verification failed. Saved progress is retained for continuation; inspect the Aggregation log for the storage error."
	default:
		return fmt.Sprintf("Aggregation rejected file deletion (HTTP %d). Saved progress is retained for continuation; inspect the Aggregation log for the cause.", status)
	}
}

func isLifecycleConflict(err error) bool {
	return err != nil && lifecycle.IsConflict(err)
}
