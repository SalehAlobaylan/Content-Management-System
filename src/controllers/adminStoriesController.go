package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ─── Topics overview ────────────────────────────────────────

type topicStatusCounts struct {
	Total    int64 `json:"total"`
	Ready    int64 `json:"ready"`
	Pending  int64 `json:"pending"`
	Archived int64 `json:"archived"`
}

type topicSummary struct {
	ID       string  `json:"id"`
	Label    string  `json:"label"`
	Total    int64   `json:"total"`
	Ready    int64   `json:"ready"`
	Pending  int64   `json:"pending"`
	Archived int64   `json:"archived"`
	AvgViews float64 `json:"avg_views"`
}

type topicsListResponse struct {
	Data          []topicSummary    `json:"data"`
	Uncategorized topicStatusCounts `json:"uncategorized"`
	Total         int64             `json:"total"`
	Page          int               `json:"page"`
	Limit         int               `json:"limit"`
	TotalPages    int               `json:"total_pages"`
}

// ListContentTopics handles GET /admin/content/stories.
//
// Lists first-class (LLM-labeled) topics with live per-status counts via a
// LEFT JOIN onto content_items, plus an "uncategorized" bucket for
// not-yet-classified articles. Searchable by label + paginated. News = ARTICLE
// by default; pass ?type= to scope to another content type.
func ListContentTopics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	contentType := strings.ToUpper(strings.TrimSpace(c.DefaultQuery("type", "NEWS")))
	search := strings.TrimSpace(c.Query("search"))

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit < 1 || limit > 200 {
		limit = 50
	}
	offset := (page - 1) * limit

	// Topic count drives pagination.
	var total int64
	countQ := db.Model(&models.Story{}).Where("tenant_id = ?", principal.TenantID)
	if search != "" {
		countQ = countQ.Where("label ILIKE ?", "%"+search+"%")
	}
	countQ.Count(&total)

	// Per-topic live counts. LEFT JOIN keeps empty topics; the join predicate
	// scopes counts to the requested content type + tenant.
	listQ := db.Table("stories").
		Select("stories.public_id::text AS id, stories.label AS label, "+
			"COUNT(c.id) AS total, "+
			"COUNT(c.id) FILTER (WHERE c.status = 'READY') AS ready, "+
			"COUNT(c.id) FILTER (WHERE c.status = 'PENDING') AS pending, "+
			"COUNT(c.id) FILTER (WHERE c.status = 'ARCHIVED') AS archived, "+
			"COALESCE(AVG(c.view_count), 0) AS avg_views").
		Joins("LEFT JOIN content_items c ON c.story_id = stories.public_id AND c.tenant_id = stories.tenant_id AND c.type = ?", contentType).
		Where("stories.tenant_id = ?", principal.TenantID)
	if search != "" {
		listQ = listQ.Where("stories.label ILIKE ?", "%"+search+"%")
	}
	listQ = listQ.Group("stories.public_id, stories.label").
		Order("total DESC, stories.label ASC").
		Limit(limit).Offset(offset)

	var rows []topicSummary
	if err := listQ.Scan(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to list topics",
			Code:    "TOPICS_LIST_FAILED",
		})
		return
	}
	if rows == nil {
		rows = []topicSummary{}
	}

	// Uncategorized bucket — articles not yet classified.
	var unc topicStatusCounts
	db.Model(&models.ContentItem{}).
		Select("COUNT(*) AS total, "+
			"COUNT(*) FILTER (WHERE status = 'READY') AS ready, "+
			"COUNT(*) FILTER (WHERE status = 'PENDING') AS pending, "+
			"COUNT(*) FILTER (WHERE status = 'ARCHIVED') AS archived").
		Where("tenant_id = ? AND type = ? AND story_id IS NULL", principal.TenantID, contentType).
		Scan(&unc)

	totalPages := 0
	if limit > 0 {
		totalPages = int((total + int64(limit) - 1) / int64(limit))
	}

	c.JSON(http.StatusOK, topicsListResponse{
		Data:          rows,
		Uncategorized: unc,
		Total:         total,
		Page:          page,
		Limit:         limit,
		TotalPages:    totalPages,
	})
}

// ─── Bulk topic edit (re-tag / move / merge / rename) ───────

type bulkEditTagsRequest struct {
	// Selection — explicit ids OR a filter. ids take precedence.
	IDs           []string `json:"ids"`
	Status        string   `json:"status"`
	Type          string   `json:"type"`
	SourceName    string   `json:"source_name"`
	Topic         string   `json:"topic"`
	CreatedBefore string   `json:"created_before"`

	// Operations. set_tags replaces the whole array; otherwise add/remove are
	// applied together (add, then remove, then de-dupe).
	AddTags    []string  `json:"add_tags"`
	RemoveTags []string  `json:"remove_tags"`
	SetTags    *[]string `json:"set_tags"`

	DryRun bool `json:"dry_run"`
}

type bulkEditTagsResponse struct {
	UpdatedCount int64  `json:"updated_count"`
	Message      string `json:"message"`
}

// cleanTags trims whitespace and drops empty entries; order preserved.
func cleanTags(in []string) []string {
	out := make([]string, 0, len(in))
	for _, t := range in {
		if tt := strings.TrimSpace(t); tt != "" {
			out = append(out, tt)
		}
	}
	return out
}

// BulkEditTags handles POST /admin/content/bulk-tags.
//
// One set-based UPDATE retags a whole selection. Powers: assign (add), remove,
// move (add target + remove source over a selection), rename (filter topic=old,
// add new, remove old) and merge (rename repeated per source). Filter-mode is
// uncapped so a topic of thousands is retagged in a single statement.
func BulkEditTags(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	var req bulkEditTagsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	addTags := cleanTags(req.AddTags)
	removeTags := cleanTags(req.RemoveTags)
	var setTags []string
	if req.SetTags != nil {
		setTags = cleanTags(*req.SetTags)
	}

	hasSet := req.SetTags != nil
	if !hasSet && len(addTags) == 0 && len(removeTags) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Provide at least one of add_tags, remove_tags, or set_tags",
			Code:    "NO_TAG_OPERATION",
		})
		return
	}

	hasIDs := len(req.IDs) > 0
	if hasIDs && len(req.IDs) > bulkStatusIDsLimit {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Too many ids; maximum is 500",
			Code:    "TOO_MANY_IDS",
		})
		return
	}
	if !hasIDs && req.Status == "" && req.Type == "" && req.SourceName == "" && req.Topic == "" && req.CreatedBefore == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "A selection is required (ids, or one of status/type/source_name/topic/created_before)",
			Code:    "SELECTION_REQUIRED",
		})
		return
	}

	var createdBefore *time.Time
	if !hasIDs && strings.TrimSpace(req.CreatedBefore) != "" {
		parsed, err := time.Parse(time.RFC3339, req.CreatedBefore)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Invalid created_before format. Use RFC3339 (e.g., 2026-03-14T00:00:00Z)",
				Code:    "INVALID_DATE",
			})
			return
		}
		createdBefore = &parsed
	}

	applySelection := func(q *gorm.DB) *gorm.DB {
		q = q.Where("tenant_id = ?", principal.TenantID)
		if hasIDs {
			return q.Where("public_id IN ?", req.IDs)
		}
		if req.Status != "" {
			q = q.Where("status = ?", strings.ToUpper(req.Status))
		}
		if req.Type != "" {
			q = q.Where("type = ?", strings.ToUpper(req.Type))
		}
		if req.SourceName != "" {
			q = q.Where("source_name = ?", req.SourceName)
		}
		if req.Topic != "" {
			q = q.Where("? = ANY(topic_tags)", req.Topic)
		}
		if createdBefore != nil {
			q = q.Where("created_at < ?", *createdBefore)
		}
		return q
	}

	if req.DryRun {
		var count int64
		applySelection(db.Model(&models.ContentItem{})).Count(&count)
		c.JSON(http.StatusOK, bulkEditTagsResponse{
			UpdatedCount: count,
			Message:      "Dry run — no items updated",
		})
		return
	}

	var result *gorm.DB
	if hasSet {
		// Replace the entire tag array.
		result = applySelection(db.Model(&models.ContentItem{})).
			Update("topic_tags", pq.StringArray(setTags))
	} else {
		// Add then remove, de-duplicated, in one set-based expression evaluated
		// against each row's current topic_tags.
		expr := "(SELECT COALESCE(array_agg(DISTINCT e), '{}') " +
			"FROM unnest(COALESCE(topic_tags, '{}') || ?::text[]) AS e " +
			"WHERE e <> ALL(?::text[]))"
		result = applySelection(db.Model(&models.ContentItem{})).
			Update("topic_tags", gorm.Expr(expr, pq.StringArray(addTags), pq.StringArray(removeTags)))
	}

	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to update tags: " + result.Error.Error(),
			Code:    "TAGS_UPDATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, bulkEditTagsResponse{
		UpdatedCount: result.RowsAffected,
		Message:      "Updated tags on content items",
	})
}

// ─── First-class topic management ───────────────────────────

type renameTopicRequest struct {
	Label string `json:"label"`
}

// RenameTopic handles PATCH /admin/stories/:id.
func RenameTopic(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid topic id", Code: "INVALID_ID"})
		return
	}

	var req renameTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Label) == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "label is required", Code: "LABEL_REQUIRED"})
		return
	}
	label := strings.TrimSpace(req.Label)

	// A name collision is a merge, not a rename — steer the caller there.
	var existing models.Story
	if err := db.Where("tenant_id = ? AND label = ? AND public_id <> ?", principal.TenantID, label, id).
		First(&existing).Error; err == nil {
		c.JSON(http.StatusConflict, authErrorResponse{
			Message: "A topic with that name already exists — merge instead",
			Code:    "TOPIC_LABEL_CONFLICT",
		})
		return
	}

	res := db.Model(&models.Story{}).
		Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).
		Update("label", label)
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to rename topic", Code: "RENAME_FAILED"})
		return
	}
	if res.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Topic not found", Code: "NOT_FOUND"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "label": label})
}

type mergeTopicsRequest struct {
	SourceIDs []string `json:"source_ids"`
	TargetID  string   `json:"target_id"`
}

// MergeTopics handles POST /admin/stories/merge — repoints all content from the
// source topics onto the target, then deletes the empty sources.
func MergeTopics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req mergeTopicsRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.TargetID == "" || len(req.SourceIDs) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "target_id and source_ids are required", Code: "INVALID_REQUEST"})
		return
	}
	target, err := uuid.Parse(req.TargetID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid target_id", Code: "INVALID_ID"})
		return
	}
	sources := make([]uuid.UUID, 0, len(req.SourceIDs))
	for _, s := range req.SourceIDs {
		if u, e := uuid.Parse(s); e == nil && u != target {
			sources = append(sources, u)
		}
	}
	if len(sources) == 0 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "No valid source topics", Code: "NO_SOURCES"})
		return
	}

	var moved int64
	err = db.Transaction(func(tx *gorm.DB) error {
		// Lock the destination in the caller's tenant before re-pointing any
		// rows. A public UUID is globally unique, but it is not an authority to
		// attach this tenant's content to another tenant's story.
		var targetStory models.Story
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("public_id = ? AND tenant_id = ?", target, principal.TenantID).
			First(&targetStory).Error; err != nil {
			return err
		}
		res := tx.Model(&models.ContentItem{}).
			Where("tenant_id = ? AND story_id IN ?", principal.TenantID, sources).
			Update("story_id", target)
		if res.Error != nil {
			return res.Error
		}
		moved = res.RowsAffected

		var cnt int64
		if err := tx.Model(&models.ContentItem{}).Where("tenant_id = ? AND story_id = ?", principal.TenantID, target).Count(&cnt).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.Story{}).Where("tenant_id = ? AND public_id = ?", principal.TenantID, target).Update("article_count", cnt).Error; err != nil {
			return err
		}

		return tx.Where("tenant_id = ? AND public_id IN ?", principal.TenantID, sources).
			Delete(&models.Story{}).Error
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, authErrorResponse{Message: "Target topic not found", Code: "NOT_FOUND"})
			return
		}
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to merge topics: " + err.Error(), Code: "MERGE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"merged": len(sources), "moved": moved, "target_id": req.TargetID})
}

// DeleteTopic handles DELETE /admin/stories/:id. Content survives — its story_id
// is cleared (so the articles fall back into "uncategorized").
func DeleteTopic(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid topic id", Code: "INVALID_ID"})
		return
	}

	var deleted int64
	err = db.Transaction(func(tx *gorm.DB) error {
		if e := tx.Model(&models.ContentItem{}).
			Where("story_id = ? AND tenant_id = ?", id, principal.TenantID).
			Update("story_id", nil).Error; e != nil {
			return e
		}
		res := tx.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).Delete(&models.Story{})
		if res.Error != nil {
			return res.Error
		}
		deleted = res.RowsAffected
		return nil
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to delete topic", Code: "DELETE_FAILED"})
		return
	}
	if deleted == 0 {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Topic not found", Code: "NOT_FOUND"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

type bulkAssignTopicRequest struct {
	IDs           []string `json:"ids"`
	Status        string   `json:"status"`
	Type          string   `json:"type"`
	SourceName    string   `json:"source_name"`
	Topic         string   `json:"topic"`
	StoryID       string   `json:"story_id"` // current-topic filter (e.g. the active board topic)
	CreatedBefore string   `json:"created_before"`
	TargetStoryID string   `json:"target_story_id"` // destination; empty/"null" => uncategorize
	DryRun        bool     `json:"dry_run"`
}

// BulkAssignTopic handles POST /admin/content/bulk-topic — move a selection
// (ids or filter) to a target topic (or uncategorize). Uncapped (filter mode).
func BulkAssignTopic(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req bulkAssignTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}

	hasIDs := len(req.IDs) > 0
	if !hasIDs && req.Status == "" && req.Type == "" && req.SourceName == "" && req.Topic == "" && req.StoryID == "" && req.CreatedBefore == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "A selection is required", Code: "SELECTION_REQUIRED"})
		return
	}

	var createdBefore *time.Time
	if !hasIDs && strings.TrimSpace(req.CreatedBefore) != "" {
		parsed, err := time.Parse(time.RFC3339, req.CreatedBefore)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid created_before", Code: "INVALID_DATE"})
			return
		}
		createdBefore = &parsed
	}

	apply := func(q *gorm.DB) *gorm.DB {
		q = q.Where("tenant_id = ?", principal.TenantID)
		if hasIDs {
			return q.Where("public_id IN ?", req.IDs)
		}
		if req.Status != "" {
			q = q.Where("status = ?", strings.ToUpper(req.Status))
		}
		if req.Type != "" {
			q = q.Where("type = ?", strings.ToUpper(req.Type))
		}
		if req.SourceName != "" {
			q = q.Where("source_name = ?", req.SourceName)
		}
		if req.Topic != "" {
			q = q.Where("? = ANY(topic_tags)", req.Topic)
		}
		if req.StoryID != "" {
			if strings.EqualFold(req.StoryID, "none") {
				q = q.Where("story_id IS NULL")
			} else {
				q = q.Where("story_id = ?", req.StoryID)
			}
		}
		if createdBefore != nil {
			q = q.Where("created_at < ?", *createdBefore)
		}
		return q
	}

	if req.DryRun {
		var count int64
		apply(db.Model(&models.ContentItem{})).Count(&count)
		c.JSON(http.StatusOK, bulkEditTagsResponse{UpdatedCount: count, Message: "Dry run — no items updated"})
		return
	}

	var target interface{}
	var targetID *uuid.UUID
	if req.TargetStoryID == "" || strings.EqualFold(req.TargetStoryID, "null") {
		target = nil
	} else {
		tid, err := uuid.Parse(req.TargetStoryID)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid target_story_id", Code: "INVALID_ID"})
			return
		}
		target = tid
		targetID = &tid
	}

	var updated int64
	err := db.Transaction(func(tx *gorm.DB) error {
		if targetID != nil {
			var targetStory models.Story
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("public_id = ? AND tenant_id = ?", *targetID, principal.TenantID).
				First(&targetStory).Error; err != nil {
				return err
			}
		}
		res := apply(tx.Model(&models.ContentItem{})).Update("story_id", target)
		if res.Error != nil {
			return res.Error
		}
		updated = res.RowsAffected
		if targetID == nil {
			return nil
		}
		var cnt int64
		if err := tx.Model(&models.ContentItem{}).Where("tenant_id = ? AND story_id = ?", principal.TenantID, *targetID).Count(&cnt).Error; err != nil {
			return err
		}
		return tx.Model(&models.Story{}).Where("tenant_id = ? AND public_id = ?", principal.TenantID, *targetID).Update("article_count", cnt).Error
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, authErrorResponse{Message: "Target topic not found", Code: "NOT_FOUND"})
			return
		}
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to assign topic: " + err.Error(), Code: "ASSIGN_FAILED"})
		return
	}

	c.JSON(http.StatusOK, bulkEditTagsResponse{UpdatedCount: updated, Message: "Moved items to topic"})
}

type reclassifyRequest struct {
	Limit int    `json:"limit"`
	Type  string `json:"type"`
}

type reclassifyResponse struct {
	Processed int   `json:"processed"`
	Remaining int64 `json:"remaining"`
}

// ReclassifyTopics handles POST /admin/stories/reclassify — backfill: classify a
// batch of not-yet-classified articles. Synchronous (so it can report
// progress); the UI loops until remaining == 0. Capped per call.
func ReclassifyTopics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req reclassifyRequest
	_ = c.ShouldBindJSON(&req)
	contentType := strings.ToUpper(strings.TrimSpace(req.Type))
	if contentType == "" {
		contentType = "NEWS"
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 25
	}
	if limit > 50 {
		limit = 50
	}

	var ids []uuid.UUID
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type = ? AND story_id IS NULL AND embedding IS NOT NULL", principal.TenantID, contentType).
		Order("created_at ASC").
		Limit(limit).
		Pluck("public_id", &ids)

	for _, id := range ids {
		classifyContentTopic(db, id)
	}

	var remaining int64
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type = ? AND story_id IS NULL AND embedding IS NOT NULL", principal.TenantID, contentType).
		Count(&remaining)

	c.JSON(http.StatusOK, reclassifyResponse{Processed: len(ids), Remaining: remaining})
}

// ─── Full re-cluster pass ───────────────────────────────────

type reclusterRequest struct {
	K    int    `json:"k"`
	Type string `json:"type"`
}

type reclusterResponse struct {
	Clusters int    `json:"clusters"`
	Articles int    `json:"articles"`
	Message  string `json:"message"`
}

// ReclusterTopics handles POST /admin/stories/recluster — rebuilds the story
// taxonomy from scratch. Phase 13: replaced the old global k-means pass (which
// produced broad THEMATIC buckets) with the same threshold-based event
// clustering the live classifier uses: wipe assignments + topics, then replay
// every embedded item chronologically through classifyContentTopic (cosine ≥
// StoryMatchThreshold within the story activity window). Runs in the
// background via the classification backfill; the snapshot rebuilds when done.
// The legacy `k` parameter is accepted and ignored.
func ReclusterTopics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req reclusterRequest
	_ = c.ShouldBindJSON(&req)
	contentType := strings.ToUpper(strings.TrimSpace(req.Type))
	if contentType == "" {
		contentType = "NEWS"
	}

	var n int64
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND type = ? AND embedding IS NOT NULL", principal.TenantID, contentType).
		Count(&n)
	if n < 2 {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Not enough embedded articles to cluster",
			Code:    "INSUFFICIENT_DATA",
		})
		return
	}

	// Wipe assignments + taxonomy, then let the threshold backfill rebuild.
	err := db.Transaction(func(tx *gorm.DB) error {
		if e := tx.Model(&models.ContentItem{}).
			Where("tenant_id = ? AND type = ?", principal.TenantID, contentType).
			Update("story_id", nil).Error; e != nil {
			return e
		}
		return tx.Where("tenant_id = ?", principal.TenantID).Delete(&models.Story{}).Error
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Re-cluster failed: " + err.Error(), Code: "RECLUSTER_FAILED"})
		return
	}

	StartClassificationBackfill(db)

	c.JSON(http.StatusOK, reclusterResponse{
		Clusters: 0,
		Articles: int(n),
		Message:  "Rebuilding stories in the background (threshold clustering) — the snapshot refreshes automatically when done",
	})
}

type labelBatchRequest struct {
	Limit int `json:"limit"`
}

type labelBatchResponse struct {
	Processed int   `json:"processed"`
	Remaining int64 `json:"remaining"`
}

// LabelTopicsBatch handles POST /admin/stories/label-batch — names a batch of
// freshly-clustered (labeled=false) topics via the LLM, biggest first. The UI
// loops until remaining == 0. Capped per call (each topic = one LLM call).
func LabelTopicsBatch(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req labelBatchRequest
	_ = c.ShouldBindJSON(&req)
	limit := req.Limit
	if limit <= 0 {
		limit = 8
	}
	if limit > 20 {
		limit = 20
	}

	var topics []models.Story
	db.Where("tenant_id = ? AND labeled = ?", principal.TenantID, false).
		Order("article_count DESC").
		Limit(limit).
		Find(&topics)

	processed := 0
	for _, t := range topics {
		texts := topicRepresentativeTexts(db, principal.TenantID, t)
		if len(texts) == 0 {
			// No member text to name from — keep the placeholder but mark it
			// labeled so the loop terminates.
			db.Model(&models.Story{}).Where("public_id = ?", t.PublicID).Update("labeled", true)
			processed++
			continue
		}

		label, lerr := generateTopicLabelViaEnrichment(texts)
		if lerr != nil {
			// Enrichment/LLM is unreachable — surface it instead of silently
			// stamping "Cluster N" on every topic. The caller can retry.
			c.JSON(http.StatusBadGateway, authErrorResponse{
				Message: "Topic naming failed (Enrichment): " + lerr.Error(),
				Code:    "LABELING_FAILED",
			})
			return
		}

		label = strings.TrimSpace(label)
		if label == "" {
			db.Model(&models.Story{}).Where("public_id = ?", t.PublicID).Update("labeled", true)
			processed++
			continue
		}
		if err := db.Model(&models.Story{}).Where("public_id = ?", t.PublicID).
			Updates(map[string]interface{}{"label": label, "labeled": true}).Error; err != nil {
			// Unique (tenant,label) collision — disambiguate with a short suffix.
			db.Model(&models.Story{}).Where("public_id = ?", t.PublicID).
				Updates(map[string]interface{}{"label": label + " " + t.PublicID.String()[:4], "labeled": true})
		}
		processed++
	}

	var remaining int64
	db.Model(&models.Story{}).
		Where("tenant_id = ? AND labeled = ?", principal.TenantID, false).
		Count(&remaining)

	c.JSON(http.StatusOK, labelBatchResponse{Processed: processed, Remaining: remaining})
}

// DigestTopicsBatch handles POST /admin/stories/summary-batch — backfills the
// source-grounded AI digest (Slice 8) for multi-member stories that lack one,
// biggest first. The UI loops until remaining == 0. Capped per call (each story
// = one LLM call). Future stories self-digest at write time; this fills the
// pre-existing corpus. Best-effort per story, but surfaces an Enrichment outage
// so the loop can retry instead of silently no-op'ing.
func DigestTopicsBatch(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	cfg := loadTenantConfig(db, principal.TenantID)
	if !cfg.StorySummaryEnabled {
		c.JSON(http.StatusOK, labelBatchResponse{Processed: 0, Remaining: 0})
		return
	}
	minMembers := cfg.StorySummaryMinMembers
	if minMembers < 1 {
		minMembers = 1
	}

	var req labelBatchRequest
	_ = c.ShouldBindJSON(&req)
	limit := req.Limit
	if limit <= 0 {
		limit = 5
	}
	if limit > 12 {
		limit = 12
	}

	// "Needs work" = multi-member stories that either have no digest yet (no
	// bullets AND not already attempted — summary_built_at marks an attempt) OR
	// have no category yet (a story digested before the classifier existed).
	// Every attempt path below writes BOTH summary_built_at and category, so a
	// groundless story can't stay NULL on either axis — `remaining` always
	// drains and the UI loop never hammers Enrichment indefinitely.
	selectUndigested := func(q *gorm.DB) *gorm.DB {
		return q.Where("tenant_id = ? AND article_count >= ? AND ((bullets IS NULL AND summary_built_at IS NULL) OR category IS NULL)",
			principal.TenantID, minMembers)
	}

	var topics []models.Story
	selectUndigested(db.Model(&models.Story{})).
		Order("article_count DESC").
		Limit(limit).
		Find(&topics)

	processed := 0
	for _, t := range topics {
		texts := storyDigestMemberTexts(db, principal.TenantID, t.PublicID)
		now := time.Now()
		if len(texts) < minMembers {
			// Nothing groundable — mark attempted (both axes) so the loop drains;
			// write-time will retry it (its gate regenerates while bullets is NULL).
			db.Model(&models.Story{}).Where("public_id = ?", t.PublicID).
				Updates(map[string]interface{}{"summary_built_at": now, "category": "general"})
			continue
		}
		summary, bullets, category, derr := generateStorySummaryViaEnrichment(texts)
		if derr != nil {
			// Surface an Enrichment outage (don't mark attempted) so the caller
			// can retry — matches LabelTopicsBatch's behaviour.
			c.JSON(http.StatusBadGateway, authErrorResponse{
				Message: "Story digest failed (Enrichment): " + derr.Error(),
				Code:    "DIGEST_FAILED",
			})
			return
		}
		if len(bullets) == 0 {
			db.Model(&models.Story{}).Where("public_id = ?", t.PublicID).
				Updates(map[string]interface{}{"summary_built_at": now, "category": normalizeStoryCategory(category)})
			continue
		}
		bulletsJSON, _ := json.Marshal(bullets)
		db.Model(&models.Story{}).Where("public_id = ?", t.PublicID).
			Updates(map[string]interface{}{
				"summary":          summary,
				"bullets":          datatypes.JSON(bulletsJSON),
				"summary_built_at": now,
				"category":         normalizeStoryCategory(category),
			})
		processed++
	}

	var remaining int64
	selectUndigested(db.Model(&models.Story{})).Count(&remaining)

	c.JSON(http.StatusOK, labelBatchResponse{Processed: processed, Remaining: remaining})
}

// topicRepresentativeTexts returns title+excerpt snippets of the members
// closest to a topic's centroid — the LLM names the cluster from these.
func topicRepresentativeTexts(db *gorm.DB, tenant string, t models.Story) []string {
	order := "created_at DESC"
	if t.Embedding != nil && t.EmbeddingSpaceID != nil {
		lit := utils.PgvectorToLiteral(t.Embedding.Slice())
		order = "embedding <=> '" + lit + "'"
	}

	type snip struct {
		Title   *string
		Excerpt *string
	}
	var rows []snip
	q := db.Model(&models.ContentItem{}).
		Select("title, excerpt").
		Where("tenant_id = ? AND story_id = ?", tenant, t.PublicID)
	if t.Embedding != nil && t.EmbeddingSpaceID != nil {
		q = q.Where("embedding_space_id = ?", *t.EmbeddingSpaceID)
	}
	q.
		Order(order).
		Limit(5).
		Scan(&rows)

	texts := make([]string, 0, len(rows))
	for _, r := range rows {
		s := ""
		if r.Title != nil {
			s = strings.TrimSpace(*r.Title)
		}
		if r.Excerpt != nil && strings.TrimSpace(*r.Excerpt) != "" {
			ex := *r.Excerpt
			if runes := []rune(ex); len(runes) > 300 {
				ex = string(runes[:300])
			}
			if s != "" {
				s += " — "
			}
			s += ex
		}
		if strings.TrimSpace(s) != "" {
			texts = append(texts, s)
		}
	}
	return texts
}
