package controllers

import (
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errStorageItemChanged = errors.New("storage candidate changed after owner selection")

// -----------------------------------------------------------------------------
// Aggregation reads candidates and policy through these endpoints.
// They run under /internal/* with the CMS_SERVICE_TOKEN gate.
// -----------------------------------------------------------------------------

type internalListPoliciesResponse struct {
	Global    *models.StoragePolicy  `json:"global"`
	Overrides []models.StoragePolicy `json:"overrides"`
	All       []models.StoragePolicy `json:"all"`
	// AutopilotTenants lists tenants where the Media Circulation Autopilot is
	// enabled (stage 5, G4 single-actor rule): the storage worker defers its
	// self-scheduled repeatable sweep tick for these tenants — Autopilot runs
	// trigger bounded sweeps instead. Manual sweeps are unaffected.
	AutopilotTenants []string `json:"autopilot_tenants"`
}

// InternalListStoragePolicies handles GET /internal/storage/policies
func InternalListStoragePolicies(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	var all []models.StoragePolicy
	if err := db.Find(&all).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list policies"})
		return
	}
	resp := internalListPoliciesResponse{All: all, AutopilotTenants: []string{}}
	for i := range all {
		if all[i].TenantID == nil {
			p := all[i]
			resp.Global = &p
			continue
		}
		resp.Overrides = append(resp.Overrides, all[i])
	}
	// The Autopilot-managed tenant list gates the sweep worker's single-actor
	// rule (G4). If this query fails we must NOT return an empty list — that
	// would read as "no tenant is autopilot-managed" and let the legacy sweep
	// run alongside Autopilot. Fail the request so the worker fails closed
	// (skips its tick) rather than double-acting on stale/incomplete data.
	var autopilotTenants []string
	if err := db.Model(&models.MediaCirculationPolicy{}).
		Where("enabled = ? AND autopilot_enabled = ?", true, true).
		Pluck("tenant_id", &autopilotTenants).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to resolve autopilot-managed tenants"})
		return
	}
	if autopilotTenants != nil {
		resp.AutopilotTenants = autopilotTenants
	}
	c.JSON(http.StatusOK, resp)
}

type internalCandidatesResponse struct {
	Data       []internalCandidate `json:"data"`
	Total      int64               `json:"total"`
	TotalBytes int64               `json:"total_bytes"`
}

type internalCandidate struct {
	ID                  string  `json:"id"`
	TenantID            string  `json:"tenant_id"`
	Type                string  `json:"type"`
	Status              string  `json:"status"`
	MediaURL            *string `json:"media_url,omitempty"`
	ThumbnailURL        *string `json:"thumbnail_url,omitempty"`
	FileSizeBytes       int64   `json:"file_size_bytes"`
	ViewCount           int     `json:"view_count"`
	CreatedAt           string  `json:"created_at"`
	UpdatedAt           string  `json:"updated_at"`
	ParentContentItemID *string `json:"parent_content_item_id,omitempty"`
	IsFeedUnit          bool    `json:"is_feed_unit"`
	FeedVisibility      string  `json:"feed_visibility"`
	DurationSec         *int    `json:"duration_sec,omitempty"`
	OriginalURL         *string `json:"original_url,omitempty"`
	SourceFeedURL       *string `json:"source_feed_url,omitempty"`
	SourceEpisodeID     *string `json:"source_episode_id,omitempty"`
	MediaSuitability    string  `json:"media_suitability"`
	ContentRole         string  `json:"content_role"`
	ProtectionReason    string  `json:"protection_reason,omitempty"`
}

// InternalListStorageCandidates handles GET /internal/storage/candidates
// Query params: tenant_id, min_age_days, max_view_count, limit,
// delete_failed_immediately (bool), max_bytes
func InternalListStorageCandidates(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id required"})
		return
	}

	minAgeDays := atoiDefault(c.Query("min_age_days"), defaultMinAgeDays)
	maxViewCount := atoiDefault(c.Query("max_view_count"), defaultMinViewCountForKeep)
	limit := atoiDefault(c.Query("limit"), 200)
	if limit > 1000 {
		limit = 1000
	}
	deleteFailed := strings.EqualFold(strings.TrimSpace(c.DefaultQuery("delete_failed_immediately", "true")), "true")
	protectTopN := atoiDefault(c.Query("protect_top_n_by_views"), 0)
	protectWindow := atoiDefault(c.Query("protect_top_n_window_days"), 30)
	includeAtomizedParents := strings.EqualFold(strings.TrimSpace(c.DefaultQuery("include_atomized_parents", "false")), "true")
	archiveAction := strings.ToLower(strings.TrimSpace(c.DefaultQuery("archive_action", "re_encode")))
	filterIDs := parseStorageCandidateIDs(c.Query("ids"))
	if strings.TrimSpace(c.Query("ids")) != "" && len(filterIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ids must contain at least one valid UUID"})
		return
	}

	q := buildCandidateQuery(db, candidateFilter{
		tenantID:                tenantID,
		minAgeDays:              minAgeDays,
		maxViewCount:            maxViewCount,
		deleteFailedImmediately: deleteFailed,
		protectTopNByViews:      protectTopN,
		protectTopNWindowDays:   protectWindow,
		excludeColdTier:         true,
		includeAtomizedParents:  includeAtomizedParents,
		archiveAction:           archiveAction,
	})
	if len(filterIDs) > 0 {
		q = q.Where("public_id IN ?", filterIDs)
		if limit > len(filterIDs) {
			limit = len(filterIDs)
		}
	}

	var total int64
	q.Model(&models.ContentItem{}).Count(&total)

	var totalBytes int64
	q.Model(&models.ContentItem{}).Select("COALESCE(SUM(file_size_bytes),0)").Scan(&totalBytes)

	var items []models.ContentItem
	if err := q.Order(storageValueOrderExpr).Limit(limit).Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list candidates"})
		return
	}

	out := make([]internalCandidate, 0, len(items))
	for _, it := range items {
		role, reason := storageRoleForContentItem(it)
		var parentID *string
		if it.ParentContentItemID != nil {
			s := it.ParentContentItemID.String()
			parentID = &s
		}
		out = append(out, internalCandidate{
			ID:                  it.PublicID.String(),
			TenantID:            it.TenantID,
			Type:                string(it.Type),
			Status:              string(it.Status),
			MediaURL:            it.MediaURL,
			ThumbnailURL:        it.ThumbnailURL,
			FileSizeBytes:       it.FileSizeBytes,
			ViewCount:           it.ViewCount,
			CreatedAt:           it.CreatedAt.UTC().Format(time.RFC3339),
			UpdatedAt:           it.UpdatedAt.UTC().Format(time.RFC3339Nano),
			ParentContentItemID: parentID,
			IsFeedUnit:          it.IsFeedUnit,
			FeedVisibility:      it.FeedVisibility,
			DurationSec:         it.DurationSec,
			OriginalURL:         it.OriginalURL,
			SourceFeedURL:       it.SourceFeedURL,
			SourceEpisodeID:     it.SourceEpisodeID,
			MediaSuitability:    it.MediaSuitability,
			ContentRole:         role,
			ProtectionReason:    reason,
		})
	}

	// Trim by max_bytes if set
	if raw := strings.TrimSpace(c.Query("max_bytes")); raw != "" {
		if maxBytes, err := strconv.ParseInt(raw, 10, 64); err == nil && maxBytes > 0 {
			var running int64
			cut := 0
			for i, it := range out {
				running += it.FileSizeBytes
				cut = i + 1
				if running >= maxBytes {
					break
				}
			}
			out = out[:cut]
		}
	}

	c.JSON(http.StatusOK, internalCandidatesResponse{
		Data:       out,
		Total:      total,
		TotalBytes: totalBytes,
	})
}

func parseStorageCandidateIDs(raw string) []uuid.UUID {
	parts := strings.Split(raw, ",")
	out := make([]uuid.UUID, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if id, err := uuid.Parse(part); err == nil {
			out = append(out, id)
		}
	}
	return out
}

// storageOwnedItems is the canonical direct-ID guard. Callers may narrow a
// Storage run, but they can never turn an arbitrary UUID into a storage target
// or cross a tenant boundary.
func storageOwnedItems(db *gorm.DB, tenant string, ids []uuid.UUID, action string) ([]models.ContentItem, error) {
	if len(ids) == 0 || strings.TrimSpace(tenant) == "" {
		return nil, fmt.Errorf("storage tenant and ids are required")
	}
	policy := loadEffectiveStoragePolicy(db, tenant)
	if !policy.Enabled {
		return nil, fmt.Errorf("storage policy is disabled")
	}
	archiveAction := action
	if archiveAction != "delete" && archiveAction != "move_to_cold" && archiveAction != "re_encode" {
		return nil, fmt.Errorf("unsupported storage operation")
	}
	var items []models.ContentItem
	query := buildCandidateQuery(db, candidateFilter{tenantID: tenant, minAgeDays: policy.MinAgeDays, maxViewCount: policy.MinViewCountForKeep, deleteFailedImmediately: policy.DeleteFailedImmediately, protectTopNByViews: policy.ProtectTopNByViews, protectTopNWindowDays: policy.ProtectTopNWindowDays, excludeColdTier: true, includeAtomizedParents: true, archiveAction: archiveAction}).Where("public_id IN ?", ids)
	if err := query.Find(&items).Error; err != nil {
		return nil, err
	}
	if len(items) != len(ids) {
		return nil, fmt.Errorf("one or more direct storage ids are not canonical owner candidates")
	}
	return items, nil
}

func storageSagaKey(idempotencyKey, operation string, itemID uuid.UUID) string {
	key := strings.TrimSpace(idempotencyKey)
	if key == "" {
		key = fmt.Sprintf("legacy-%s", operation)
	}
	return key + ":" + itemID.String()
}

func createPreparedStorageSaga(db *gorm.DB, tenant string, item models.ContentItem, operation, idempotencyKey, manifestHash, correlationID, ownerRequestID string, evidence map[string]interface{}) (*models.StorageOperationSaga, error) {
	key := storageSagaKey(idempotencyKey, operation, item.PublicID)
	var ownerID *uint
	if parsed, err := uuid.Parse(strings.TrimSpace(ownerRequestID)); err == nil {
		var owner models.RetentionOwnerRequest
		if err := db.Where("tenant_id=? AND public_id=?", tenant, parsed).First(&owner).Error; err == nil {
			ownerID = &owner.ID
		}
	}
	var correlation *uuid.UUID
	if parsed, err := uuid.Parse(strings.TrimSpace(correlationID)); err == nil {
		correlation = &parsed
	}
	var manifest *string
	if trimmed := strings.TrimSpace(manifestHash); trimmed != "" {
		manifest = &trimmed
	}
	saga := models.StorageOperationSaga{TenantID: tenant, ContentItemID: item.PublicID, OwnerRequestID: ownerID, Operation: operation, IdempotencyKey: key, ManifestHash: manifest, CorrelationID: correlation, State: "prepared", ObjectEvidence: storageJSON(evidence), CMSEvidence: datatypes.JSON([]byte(`{}`)), StartedAt: time.Now().UTC()}
	create := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "content_item_id"}, {Name: "operation"}, {Name: "idempotency_key"}}, DoNothing: true}).Create(&saga)
	if create.Error != nil {
		return nil, create.Error
	}
	saga.Created = create.RowsAffected == 1
	if !saga.Created {
		if err := db.Where("tenant_id=? AND content_item_id=? AND operation=? AND idempotency_key=?", tenant, item.PublicID, operation, key).First(&saga).Error; err != nil {
			return nil, err
		}
	}
	return &saga, nil
}

func prepareStorageSagaUnderLifecycle(db *gorm.DB, tenant string, selected models.ContentItem, operation, idempotencyKey, manifestHash, correlationID, ownerRequestID string, evidence map[string]interface{}) (*models.StorageOperationSaga, error) {
	var saga *models.StorageOperationSaga
	err := db.Transaction(func(tx *gorm.DB) error {
		var current models.ContentItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", tenant, selected.PublicID).First(&current).Error; err != nil {
			return err
		}
		if !current.UpdatedAt.Equal(selected.UpdatedAt) || current.FileSizeBytes != selected.FileSizeBytes || stringValue(current.MediaURL) != stringValue(selected.MediaURL) || stringValue(current.ThumbnailURL) != stringValue(selected.ThumbnailURL) {
			return errStorageItemChanged
		}
		if err := checkContentLifecycleMutation(tx, current); err != nil {
			return err
		}
		var err error
		saga, err = createPreparedStorageSaga(tx, tenant, current, operation, idempotencyKey, manifestHash, correlationID, ownerRequestID, evidence)
		return err
	})
	return saga, err
}

func requireObjectAppliedSaga(db *gorm.DB, tenant string, item models.ContentItem, operation, idempotencyKey string) (*models.StorageOperationSaga, error) {
	var saga models.StorageOperationSaga
	key := storageSagaKey(idempotencyKey, operation, item.PublicID)
	if err := db.Where("tenant_id=? AND content_item_id=? AND operation=? AND idempotency_key=?", tenant, item.PublicID, operation, key).First(&saga).Error; err != nil {
		return nil, fmt.Errorf("storage operation saga is missing")
	}
	if saga.State != "object_applied" {
		return nil, fmt.Errorf("storage operation saga has not confirmed object mutation")
	}
	return &saga, nil
}

func completeStorageSaga(db *gorm.DB, saga *models.StorageOperationSaga, evidence map[string]interface{}) error {
	if saga == nil {
		return errors.New("storage operation saga is required")
	}
	now := time.Now().UTC()
	result := db.Model(&models.StorageOperationSaga{}).Where("id=? AND state=?", saga.ID, "object_applied").Updates(map[string]interface{}{"state": "cms_committed", "cms_evidence": storageJSON(evidence), "completed_at": now, "error": ""})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("storage saga lost its object-applied state")
	}
	return nil
}

type storageSagaItemSnapshot struct {
	UpdatedAt       time.Time         `json:"updated_at"`
	OldSizeBytes    int64             `json:"old_size_bytes"`
	OldMediaURL     *string           `json:"old_media_url"`
	OldThumbnailURL *string           `json:"old_thumbnail_url"`
	RequestedAbsent *bool             `json:"requested_artifacts_absent"`
	ObjectsAbsent   *bool             `json:"objects_absent"`
	Errors          []string          `json:"errors"`
	Artifacts       []string          `json:"artifacts"`
	MovedCount      int               `json:"moved_count"`
	NewPrimaryURLs  map[string]string `json:"new_primary_urls"`
}

func storageSagaSnapshot(saga *models.StorageOperationSaga, item models.ContentItem) (storageSagaItemSnapshot, error) {
	var snapshot storageSagaItemSnapshot
	if saga == nil || len(saga.ObjectEvidence) == 0 || json.Unmarshal(saga.ObjectEvidence, &snapshot) != nil {
		return snapshot, errors.New("storage saga evidence is missing or invalid")
	}
	if snapshot.UpdatedAt.IsZero() || !snapshot.UpdatedAt.Equal(item.UpdatedAt) ||
		snapshot.OldSizeBytes != item.FileSizeBytes ||
		stringValue(snapshot.OldMediaURL) != stringValue(item.MediaURL) ||
		stringValue(snapshot.OldThumbnailURL) != stringValue(item.ThumbnailURL) {
		return snapshot, errStorageItemChanged
	}
	return snapshot, nil
}

type internalStartStorageSagaRequest struct {
	TenantID       string                 `json:"tenant_id"`
	ContentItemID  string                 `json:"content_item_id"`
	Operation      string                 `json:"operation"`
	IdempotencyKey string                 `json:"idempotency_key"`
	ManifestHash   string                 `json:"manifest_hash"`
	CorrelationID  string                 `json:"correlation_id"`
	OwnerRequestID string                 `json:"owner_request_id"`
	Evidence       map[string]interface{} `json:"evidence"`
}

// InternalStartStorageOperationSaga writes the durable intent before
// Aggregation mutates any object. A failed intent write must prevent mutation.
func InternalStartStorageOperationSaga(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req internalStartStorageSagaRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.IdempotencyKey) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id and idempotency_key required"})
		return
	}
	id, err := uuid.Parse(strings.TrimSpace(req.ContentItemID))
	if err != nil || (req.Operation != "recoverable_delete" && req.Operation != "move_to_cold") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid content_item_id and operation required"})
		return
	}
	action := "delete"
	if req.Operation == "move_to_cold" {
		action = "move_to_cold"
	}
	items, err := storageOwnedItems(db, req.TenantID, []uuid.UUID{id}, action)
	if err != nil || len(items) != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "content item is not a canonical storage candidate"})
		return
	}
	if req.Evidence == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "storage operation requires a versioned candidate snapshot", "code": "STORAGE_CANDIDATE_STALE"})
		return
	}
	evidenceBytes, err := json.Marshal(req.Evidence)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid storage evidence"})
		return
	}
	if _, err := storageSagaSnapshot(&models.StorageOperationSaga{ObjectEvidence: datatypes.JSON(evidenceBytes)}, items[0]); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "storage candidate changed or lacks a complete version snapshot", "code": "STORAGE_CANDIDATE_STALE"})
		return
	}
	var candidateEvidence storageSagaItemSnapshot
	if err := json.Unmarshal(evidenceBytes, &candidateEvidence); err != nil || len(candidateEvidence.Artifacts) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "storage operation requires an exact non-empty artifact set"})
		return
	}
	saga, err := prepareStorageSagaUnderLifecycle(db, req.TenantID, items[0], req.Operation, req.IdempotencyKey, req.ManifestHash, req.CorrelationID, req.OwnerRequestID, req.Evidence)
	if err != nil {
		if lifecycle.IsConflict(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "content item is held by an active lifecycle campaign", "code": "OPERATION_CONFLICT"})
			return
		}
		if errors.Is(err, errStorageItemChanged) {
			c.JSON(http.StatusConflict, gin.H{"error": "storage candidate changed; refresh the owner preview", "code": "STORAGE_CANDIDATE_STALE"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start storage operation saga"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": saga})
}

type internalMarkStorageSagaObjectAppliedRequest struct {
	Evidence map[string]interface{} `json:"evidence"`
}

// InternalMarkStorageSagaObjectApplied records the provider-side success before
// CMS references are changed. Prepared rows without this marker are explicit
// reconciliation work, never silently treated as committed.
func InternalMarkStorageSagaObjectApplied(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saga id"})
		return
	}
	var req internalMarkStorageSagaObjectAppliedRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	var saga models.StorageOperationSaga
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=?", id).First(&saga).Error; err != nil {
			return err
		}
		if saga.State != "prepared" && saga.State != "object_applied" {
			return errors.New("storage operation cannot be marked")
		}
		merged, err := mergeStorageObjectReceipt(saga.Operation, saga.ObjectEvidence, req.Evidence)
		if err != nil {
			return err
		}
		if saga.State == "object_applied" {
			var oldObject, newObject map[string]interface{}
			if json.Unmarshal(saga.ObjectEvidence, &oldObject) != nil || json.Unmarshal(merged, &newObject) != nil || !reflect.DeepEqual(oldObject, newObject) {
				return errors.New("storage object receipt changed after it was recorded")
			}
			return nil
		}
		result := tx.Model(&saga).Where("state=?", "prepared").Updates(map[string]interface{}{"state": "object_applied", "object_evidence": datatypes.JSON(merged)})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("storage saga lost its prepared state")
		}
		saga.State = "object_applied"
		saga.ObjectEvidence = datatypes.JSON(merged)
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "storage object receipt is invalid or conflicts with the prepared operation", "code": "STORAGE_RECEIPT_UNCERTAIN"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": saga.PublicID, "state": "object_applied"}})
}

func mergeStorageObjectReceipt(operation string, preparedJSON datatypes.JSON, receipt map[string]interface{}) ([]byte, error) {
	if len(preparedJSON) == 0 || len(receipt) == 0 {
		return nil, errors.New("prepared evidence and provider receipt are required")
	}
	var prepared map[string]interface{}
	if err := json.Unmarshal(preparedJSON, &prepared); err != nil || prepared == nil {
		return nil, errors.New("prepared storage evidence is invalid")
	}
	for key, value := range receipt {
		if existing, exists := prepared[key]; exists {
			if !reflect.DeepEqual(existing, value) {
				return nil, fmt.Errorf("provider receipt attempted to change immutable evidence field %q", key)
			}
			continue
		}
		prepared[key] = value
	}
	encoded, err := json.Marshal(prepared)
	if err != nil {
		return nil, errors.New("failed to encode merged storage receipt")
	}
	var merged storageSagaItemSnapshot
	if err := json.Unmarshal(encoded, &merged); err != nil {
		return nil, errors.New("merged storage receipt is invalid")
	}
	if merged.UpdatedAt.IsZero() || len(merged.Artifacts) == 0 || len(merged.Errors) != 0 {
		return nil, errors.New("provider receipt lacks the exact version, artifact set, or clean result")
	}
	switch operation {
	case "recoverable_delete":
		if merged.RequestedAbsent == nil || !*merged.RequestedAbsent {
			return nil, errors.New("delete receipt does not prove requested artifacts absent")
		}
	case "move_to_cold":
		if merged.MovedCount <= 0 || len(merged.NewPrimaryURLs) == 0 || (merged.NewPrimaryURLs["processed"] == "" && merged.NewPrimaryURLs["original"] == "") {
			return nil, errors.New("move receipt does not prove a verified destination")
		}
	default:
		return nil, errors.New("unsupported storage saga operation")
	}
	return encoded, nil
}

type internalArchiveItemsRequest struct {
	IDs                []string `json:"ids"`
	PreserveThumbnails bool     `json:"preserve_thumbnails"`
	TenantID           string   `json:"tenant_id"`
	IdempotencyKey     string   `json:"idempotency_key"`
	ManifestHash       string   `json:"manifest_hash"`
	CorrelationID      string   `json:"correlation_id"`
	OwnerRequestID     string   `json:"owner_request_id"`
}

type internalArchiveItemsResponse struct {
	UpdatedCount int   `json:"updated_count"`
	FreedBytes   int64 `json:"freed_bytes"`
}

// InternalArchiveItems handles POST /internal/storage/archive.
// It deletes artifact references and records storage_state without using
// content status as the artifact lifecycle marker.
func InternalArchiveItems(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req internalArchiveItemsRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.IDs) == 0 || strings.TrimSpace(req.TenantID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ids required"})
		return
	}

	ids := make([]uuid.UUID, 0, len(req.IDs))
	seen := make(map[uuid.UUID]bool, len(req.IDs))
	for _, raw := range req.IDs {
		id, err := uuid.Parse(raw)
		if err != nil || seen[id] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "ids must contain unique valid UUIDs"})
			return
		}
		seen[id] = true
		ids = append(ids, id)
	}

	items, err := storageOwnedItems(db, req.TenantID, ids, "delete")
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	now := time.Now().UTC()
	updates := map[string]interface{}{
		"file_size_bytes":         0,
		"media_url":               nil,
		"storage_state":           models.StorageStateRecoverableDeleted,
		"storage_state_reason":    "storage_archive",
		"storage_recovery_status": models.StorageRecoveryRecoverable,
		"storage_deleted_at":      &now,
	}
	if !req.PreserveThumbnails {
		updates["thumbnail_url"] = nil
	}
	var rowsAffected int64
	var freed int64
	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, item := range items {
			var current models.ContentItem
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", req.TenantID, item.PublicID).First(&current).Error; err != nil {
				return err
			}
			if !current.UpdatedAt.Equal(item.UpdatedAt) || current.FileSizeBytes != item.FileSizeBytes || stringValue(current.MediaURL) != stringValue(item.MediaURL) || stringValue(current.ThumbnailURL) != stringValue(item.ThumbnailURL) {
				return errStorageItemChanged
			}
			saga, err := requireObjectAppliedSaga(tx, req.TenantID, current, "recoverable_delete", req.IdempotencyKey)
			if err != nil {
				return err
			}
			snapshot, err := storageSagaSnapshot(saga, current)
			if err != nil {
				return err
			}
			if snapshot.RequestedAbsent == nil || !*snapshot.RequestedAbsent || len(snapshot.Errors) != 0 || len(snapshot.Artifacts) == 0 {
				return errors.New("storage delete saga has no exact artifact-absence proof")
			}
			if err := checkContentLifecycleMutation(tx, current); err != nil {
				return err
			}
			res := tx.Model(&models.ContentItem{}).Where("id=? AND tenant_id=? AND public_id=? AND updated_at=?", current.ID, req.TenantID, current.PublicID, current.UpdatedAt).Updates(updates)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected != 1 {
				return errStorageItemChanged
			}
			rowsAffected++
			freed += current.FileSizeBytes
			if _, err := createStorageArtifactEvent(tx, storageArtifactEventInput{
				TenantID: current.TenantID, ContentItemID: current.PublicID,
				ParentContentItemID: current.ParentContentItemID,
				EventType:           models.StorageArtifactEventRecoverableDeleted,
				Status:              models.StorageArtifactEventStatusSuccess,
				Reason:              "Archived by storage sweep", Trigger: "auto", Source: "aggregation",
				OldMediaURL: stringValue(current.MediaURL), OldSizeBytes: current.FileSizeBytes,
				DeletedBytes: current.FileSizeBytes, FreedBytes: current.FileSizeBytes,
				ArtifactKeys:          map[string]interface{}{"requested_artifacts": snapshot.Artifacts},
				RecoveryPayload:       storageRecoveryPayloadForItem(current),
				StorageState:          models.StorageStateRecoverableDeleted,
				StorageStateReason:    "storage_archive",
				StorageRecoveryStatus: models.StorageRecoveryRecoverable,
			}); err != nil {
				return err
			}
			if err := completeStorageSaga(tx, saga, map[string]interface{}{"storage_state": models.StorageStateRecoverableDeleted, "freed_bytes": current.FileSizeBytes, "requested_artifacts_absent": true}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		if lifecycle.IsConflict(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "content item is held by an active lifecycle campaign", "code": "OPERATION_CONFLICT"})
			return
		}
		if errors.Is(err, errStorageItemChanged) {
			c.JSON(http.StatusConflict, gin.H{"error": "storage candidate changed after object mutation; reconciliation is required", "code": "STORAGE_CANDIDATE_STALE"})
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": "storage archive did not commit atomically; reconciliation is required", "code": "STORAGE_RECONCILIATION_REQUIRED"})
		return
	}

	c.JSON(http.StatusOK, internalArchiveItemsResponse{
		UpdatedCount: int(rowsAffected),
		FreedBytes:   freed,
	})
}

type internalMoveToColdItem struct {
	ID                string  `json:"id"`
	ExpectedUpdatedAt string  `json:"expected_updated_at"`
	MediaURL          *string `json:"media_url"`
	ThumbnailURL      *string `json:"thumbnail_url"`
	NewSizeBytes      *int64  `json:"new_size_bytes"`
}

type internalMoveToColdRequest struct {
	Items          []internalMoveToColdItem `json:"items"`
	TenantID       string                   `json:"tenant_id"`
	IdempotencyKey string                   `json:"idempotency_key"`
	ManifestHash   string                   `json:"manifest_hash"`
	CorrelationID  string                   `json:"correlation_id"`
	OwnerRequestID string                   `json:"owner_request_id"`
}

type internalMoveToColdResponse struct {
	UpdatedCount int   `json:"updated_count"`
	FreedBytes   int64 `json:"freed_bytes"`
}

// InternalMoveItemsToCold handles POST /internal/storage/move-to-cold
// Bulk-updates items to storage_tier='cold' with new media URLs pointing at the
// cold bucket. Status stays READY so playback continues from the cold tier.
func InternalMoveItemsToCold(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req internalMoveToColdRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.Items) == 0 || strings.TrimSpace(req.TenantID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "items required"})
		return
	}
	cold := "cold"
	now := time.Now().UTC()
	seen := make(map[uuid.UUID]bool, len(req.Items))
	resolved := make([]struct {
		Input internalMoveToColdItem
		Item  models.ContentItem
	}, 0, len(req.Items))
	for _, input := range req.Items {
		id, err := uuid.Parse(input.ID)
		if err != nil || seen[id] || input.NewSizeBytes != nil && *input.NewSizeBytes < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "items must contain unique valid ids and non-negative sizes"})
			return
		}
		seen[id] = true
		expectedUpdatedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(input.ExpectedUpdatedAt))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "expected_updated_at must be an RFC3339 timestamp"})
			return
		}
		owned, err := storageOwnedItems(db, req.TenantID, []uuid.UUID{id}, "move_to_cold")
		if err != nil || len(owned) != 1 || !owned[0].UpdatedAt.Equal(expectedUpdatedAt) {
			c.JSON(http.StatusConflict, gin.H{"error": "storage candidate changed or is no longer eligible", "code": "STORAGE_CANDIDATE_STALE"})
			return
		}
		resolved = append(resolved, struct {
			Input internalMoveToColdItem
			Item  models.ContentItem
		}{Input: input, Item: owned[0]})
	}
	updated := 0
	var freed int64
	err := db.Transaction(func(tx *gorm.DB) error {
		for _, selection := range resolved {
			input, selected := selection.Input, selection.Item
			var item models.ContentItem
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", req.TenantID, selected.PublicID).First(&item).Error; err != nil {
				return err
			}
			expectedUpdatedAt, _ := time.Parse(time.RFC3339Nano, input.ExpectedUpdatedAt)
			if !item.UpdatedAt.Equal(expectedUpdatedAt) || !item.UpdatedAt.Equal(selected.UpdatedAt) {
				return errStorageItemChanged
			}
			saga, err := requireObjectAppliedSaga(tx, req.TenantID, item, "move_to_cold", req.IdempotencyKey)
			if err != nil {
				return err
			}
			snapshot, err := storageSagaSnapshot(saga, item)
			if err != nil {
				return err
			}
			if snapshot.MovedCount <= 0 || len(snapshot.NewPrimaryURLs) == 0 || len(snapshot.Errors) != 0 {
				return errors.New("storage move saga has no verified destination evidence")
			}
			expectedMediaURL := snapshot.NewPrimaryURLs["processed"]
			if expectedMediaURL == "" {
				expectedMediaURL = snapshot.NewPrimaryURLs["original"]
			}
			if expectedMediaURL == "" || input.MediaURL == nil || *input.MediaURL != expectedMediaURL {
				return errors.New("storage move URL does not match its object receipt")
			}
			if input.ThumbnailURL != nil && snapshot.NewPrimaryURLs["thumbnail"] != *input.ThumbnailURL {
				return errors.New("storage thumbnail URL does not match its object receipt")
			}
			if err := checkContentLifecycleMutation(tx, item); err != nil {
				return err
			}
			oldSize := item.FileSizeBytes
			newSize := int64Value(input.NewSizeBytes, oldSize)
			eventFreed := oldSize - newSize
			if input.NewSizeBytes == nil {
				eventFreed = oldSize
			}
			if eventFreed < 0 {
				eventFreed = 0
			}
			updates := map[string]interface{}{
				"storage_tier": &cold, "last_storage_check": &now,
				"storage_state": models.StorageStateCold, "storage_state_reason": "moved_to_cold",
				"storage_recovery_status": models.StorageRecoveryRecoverable, "storage_last_verified_at": &now,
				"media_url": input.MediaURL,
			}
			if input.ThumbnailURL != nil {
				updates["thumbnail_url"] = input.ThumbnailURL
			}
			if input.NewSizeBytes != nil {
				updates["file_size_bytes"] = *input.NewSizeBytes
			}
			result := tx.Model(&models.ContentItem{}).Where("id=? AND tenant_id=? AND public_id=? AND updated_at=?", item.ID, req.TenantID, item.PublicID, item.UpdatedAt).Updates(updates)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return errStorageItemChanged
			}
			if _, err := createStorageArtifactEvent(tx, storageArtifactEventInput{
				TenantID: item.TenantID, ContentItemID: item.PublicID, ParentContentItemID: item.ParentContentItemID,
				EventType: models.StorageArtifactEventMovedCold, Status: models.StorageArtifactEventStatusSuccess,
				Reason: "Moved to cold storage by storage sweep", Trigger: "auto", Source: "aggregation",
				StorageTier: "cold", OldStorageTier: tierFromItem(item), OldMediaURL: stringValue(item.MediaURL),
				NewMediaURL: stringValue(input.MediaURL), OldSizeBytes: oldSize, NewSizeBytes: newSize,
				FreedBytes: eventFreed, RecoveryPayload: storageRecoveryPayloadForItem(item),
				StorageState: models.StorageStateCold, StorageStateReason: "moved_to_cold",
				StorageRecoveryStatus: models.StorageRecoveryRecoverable,
			}); err != nil {
				return err
			}
			if err := completeStorageSaga(tx, saga, map[string]interface{}{"storage_state": models.StorageStateCold, "new_size_bytes": newSize}); err != nil {
				return err
			}
			updated++
			if input.NewSizeBytes == nil {
				freed += oldSize
			} else {
				freed += oldSize - *input.NewSizeBytes
			}
		}
		return nil
	})
	if err != nil {
		if lifecycle.IsConflict(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "content item is held by an active lifecycle campaign", "code": "OPERATION_CONFLICT"})
			return
		}
		if errors.Is(err, errStorageItemChanged) {
			c.JSON(http.StatusConflict, gin.H{"error": "storage candidate changed after object mutation; reconciliation is required", "code": "STORAGE_CANDIDATE_STALE"})
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": "storage move did not commit atomically; reconciliation is required", "code": "STORAGE_RECONCILIATION_REQUIRED"})
		return
	}

	c.JSON(http.StatusOK, internalMoveToColdResponse{
		UpdatedCount: updated,
		FreedBytes:   freed,
	})
}

type internalSweepRunRequest struct {
	TenantID         string  `json:"tenant_id"`
	StartedAt        string  `json:"started_at"`
	FinishedAt       *string `json:"finished_at"`
	DeletedCount     int     `json:"deleted_count"`
	MovedToColdCount int     `json:"moved_to_cold_count"`
	ReEncodedCount   int     `json:"re_encoded_count"`
	FreedBytes       int64   `json:"freed_bytes"`
	Trigger          string  `json:"trigger"`
	Error            string  `json:"error,omitempty"`
	CorrelationID    string  `json:"correlation_id"`
	OwnerRequestID   string  `json:"owner_request_id"`
	IdempotencyKey   string  `json:"idempotency_key"`
	ManifestHash     string  `json:"manifest_hash"`
}

// InternalCreateSweepRun handles POST /internal/storage/sweep-runs
func InternalCreateSweepRun(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req internalSweepRunRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.TenantID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	started, err := time.Parse(time.RFC3339, req.StartedAt)
	if err != nil {
		started = time.Now().UTC()
	}
	var finished *time.Time
	if req.FinishedAt != nil && *req.FinishedAt != "" {
		if f, err := time.Parse(time.RFC3339, *req.FinishedAt); err == nil {
			finished = &f
		}
	}

	trigger := strings.TrimSpace(req.Trigger)
	if trigger == "" {
		trigger = "auto"
	}

	var correlationID *uuid.UUID
	if parsed, parseErr := uuid.Parse(strings.TrimSpace(req.CorrelationID)); parseErr == nil {
		correlationID = &parsed
	}
	var ownerRequestID *uint
	if parsed, parseErr := uuid.Parse(strings.TrimSpace(req.OwnerRequestID)); parseErr == nil {
		var owner models.RetentionOwnerRequest
		if err := db.Where("tenant_id=? AND public_id=?", req.TenantID, parsed).First(&owner).Error; err == nil {
			ownerRequestID = &owner.ID
		}
	}
	var manifestHash *string
	if value := strings.TrimSpace(req.ManifestHash); value != "" {
		manifestHash = &value
	}
	var idempotencyKey *string
	if value := strings.TrimSpace(req.IdempotencyKey); value != "" {
		idempotencyKey = &value
	}
	run := models.StorageSweepRun{
		TenantID:         req.TenantID,
		StartedAt:        started,
		FinishedAt:       finished,
		DeletedCount:     req.DeletedCount,
		MovedToColdCount: req.MovedToColdCount,
		ReEncodedCount:   req.ReEncodedCount,
		FreedBytes:       req.FreedBytes,
		Trigger:          trigger,
		Error:            req.Error,
		CorrelationID:    correlationID,
		OwnerRequestID:   ownerRequestID,
		IdempotencyKey:   idempotencyKey,
		ManifestHash:     manifestHash,
	}
	if err := db.Create(&run).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to record sweep run"})
		return
	}

	// Touch the policy's last_sweep_at
	if req.Error == "" {
		_ = db.Model(&models.StoragePolicy{}).
			Where("tenant_id = ? OR tenant_id IS NULL", req.TenantID).
			Update("last_sweep_at", started).Error
	}

	c.JSON(http.StatusOK, run)
}
