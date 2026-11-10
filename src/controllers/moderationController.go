package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const maxModerationDetailLength = 1_000
const maxAnonymousReportsPerHour = 5

var errModerationIdempotencyConflict = errors.New("moderation idempotency key reused")
var errAnonymousModerationRateLimit = errors.New("anonymous moderation report rate limit reached")

var moderationReasons = map[string]struct{}{
	"harmful_inappropriate": {}, "misinformation": {}, "copyright": {},
	"broken_media": {}, "incorrect_language_translation": {}, "other": {},
}

type createModerationReportRequest struct {
	TargetType string `json:"target_type" binding:"required"`
	TargetID   string `json:"target_id" binding:"required"`
	Reason     string `json:"reason" binding:"required"`
	Detail     string `json:"detail"`
}

type blockAuthorRequest struct {
	AuthorID string `json:"author_id" binding:"required"`
}

func moderationRequestDigest(request createModerationReportRequest) string {
	encoded, _ := json.Marshal(request)
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest[:])
}

func moderationReporter(c *gin.Context) (string, *uuid.UUID, bool) {
	if uid, ok := authedUserID(c); ok {
		return "user:" + uid.String(), &uid, true
	}
	installationID, err := uuid.Parse(strings.TrimSpace(c.Query("installation_id")))
	if err != nil {
		return "", nil, false
	}
	return "installation:" + installationID.String(), nil, true
}

// Blocks remain an account-only control; an installation cannot block an
// author globally because it has no durable account relationship.
func moderationUser(c *gin.Context) (uuid.UUID, bool) {
	return authedUserID(c)
}

// CreateModerationReport accepts either a verified account or an opaque,
// app-local installation UUID. It deliberately does not expose report status
// to users or leak whether a report changed moderator action.
func CreateModerationReport(c *gin.Context) {
	reporterScope, reporterID, ok := moderationReporter(c)
	if !ok {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "A valid installation_id is required when unauthenticated"})
		return
	}
	var req createModerationReportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid report body"})
		return
	}
	req.TargetType = strings.TrimSpace(req.TargetType)
	req.Reason = strings.TrimSpace(req.Reason)
	req.Detail = strings.TrimSpace(req.Detail)
	if req.TargetType != models.ModerationTargetContent && req.TargetType != models.ModerationTargetComment {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "target_type must be content or comment"})
		return
	}
	if _, valid := moderationReasons[req.Reason]; !valid {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid report reason"})
		return
	}
	if req.Reason == "other" && req.Detail == "" {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "detail is required for other"})
		return
	}
	if len([]rune(req.Detail)) > maxModerationDetailLength {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "detail is too long"})
		return
	}
	targetID, err := uuid.Parse(req.TargetID)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid target_id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if req.TargetType == models.ModerationTargetContent {
		var content models.ContentItem
		if err := publicContentQuery(db).Where("public_id = ?", targetID).First(&content).Error; err != nil {
			c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Target not found"})
			return
		}
	} else {
		var comment models.UserInteraction
		if err := db.Where("public_id = ? AND type = ?", targetID, models.InteractionTypeComment).First(&comment).Error; err != nil {
			c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Target not found"})
			return
		}
	}
	key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if len(key) > maxConsumerIdempotencyKeyLength {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Idempotency-Key is too long"})
		return
	}
	digest := moderationRequestDigest(req)
	report := models.ModerationReport{TenantID: "default", ReporterID: reporterID, ReporterScope: reporterScope, TargetType: req.TargetType, TargetID: targetID, Reason: req.Reason}
	if req.Detail != "" {
		report.Detail = &req.Detail
	}
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		// Installation identity is deliberately bounded. The authenticated scope
		// remains subject to normal account moderation controls instead.
		if reporterID == nil {
			var reportsInWindow int64
			if err := tx.Model(&models.ModerationReport{}).
				Where("tenant_id = ? AND reporter_scope = ? AND created_at >= ?", "default", reporterScope, time.Now().Add(-time.Hour)).
				Count(&reportsInWindow).Error; err != nil {
				return err
			}
			if reportsInWindow >= maxAnonymousReportsPerHour {
				return errAnonymousModerationRateLimit
			}
		}
		if key != "" {
			var existing models.ConsumerModerationIdempotency
			if err := tx.Where("reporter_scope = ? AND endpoint = ? AND idempotency_key = ?", reporterScope, "POST /api/v1/moderation/reports", key).First(&existing).Error; err == nil {
				if existing.RequestDigest != digest {
					return errModerationIdempotencyConflict
				}
				return tx.Where("public_id = ?", existing.ReportID).First(&report).Error
			} else if err != gorm.ErrRecordNotFound {
				return err
			}
		}
		if err := tx.Create(&report).Error; err != nil {
			return err
		}
		created = true
		if key != "" {
			return tx.Create(&models.ConsumerModerationIdempotency{ReporterScope: reporterScope, Endpoint: "POST /api/v1/moderation/reports", IdempotencyKey: key, RequestDigest: digest, ReportID: report.PublicID}).Error
		}
		return nil
	})
	if errors.Is(err, errModerationIdempotencyConflict) {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Idempotency-Key was reused with a different request"})
		return
	}
	if errors.Is(err, errAnonymousModerationRateLimit) {
		c.JSON(http.StatusTooManyRequests, utils.HTTPError{Code: http.StatusTooManyRequests, Message: "Too many reports; please try again later"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to create report"})
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	c.JSON(status, gin.H{"id": report.PublicID, "status": "received"})
}

func BlockAuthor(c *gin.Context) {
	uid, ok := moderationUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	var req blockAuthorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid block body"})
		return
	}
	authorID, err := uuid.Parse(req.AuthorID)
	if err != nil || authorID == uid {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid author_id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	block := models.UserBlock{TenantID: "default", UserID: uid, BlockedUserID: authorID}
	if err := db.Where("tenant_id = ? AND user_id = ? AND blocked_user_id = ?", block.TenantID, uid, authorID).FirstOrCreate(&block).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to block author"})
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Author blocked"})
}

func UnblockAuthor(c *gin.Context) {
	uid, ok := moderationUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	authorID, err := uuid.Parse(c.Param("authorID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid author id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if err := db.Where("tenant_id = ? AND user_id = ? AND blocked_user_id = ?", "default", uid, authorID).Delete(&models.UserBlock{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to unblock author"})
		return
	}
	c.JSON(http.StatusOK, utils.ResponseMessage{Code: http.StatusOK, Message: "Author unblocked"})
}

type moderationQueueResponse struct {
	Data       []adminModerationReport `json:"data"`
	Total      int64                   `json:"total"`
	Page       int                     `json:"page"`
	Limit      int                     `json:"limit"`
	TotalPages int                     `json:"total_pages"`
}

// adminModerationReport adds only the reported comment author's UUID. It is
// operator-only and intentionally never exposes the reporter's identity.
type adminModerationReport struct {
	models.ModerationReport
	AuthorID        *uuid.UUID `json:"author_id,omitempty"`
	AuthorSuspended bool       `json:"author_suspended"`
}

// adminCommentReview exposes only the policy evidence needed for a human
// decision. It deliberately omits reporter identity and unrelated metadata.
type adminCommentReview struct {
	ID        uuid.UUID  `json:"id"`
	ContentID uuid.UUID  `json:"content_id"`
	Text      string     `json:"text"`
	Reason    string     `json:"reason"`
	AuthorID  *uuid.UUID `json:"author_id,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

func AdminListCommentReviews(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var comments []models.UserInteraction
	if err := db.Model(&models.UserInteraction{}).
		Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
		Where("content_items.tenant_id = ? AND user_interactions.type = ? AND user_interactions.comment_moderation_status = ?", principal.TenantID, models.InteractionTypeComment, string(commentPolicyReview)).
		Order("created_at ASC").Limit(100).Find(&comments).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to load comment reviews"})
		return
	}
	rows := make([]adminCommentReview, 0, len(comments))
	for _, comment := range comments {
		var meta commentMetadata
		if json.Unmarshal(comment.Metadata, &meta) != nil || strings.TrimSpace(meta.Text) == "" {
			continue
		}
		reason := "policy_review"
		if comment.CommentModerationReason != nil {
			reason = *comment.CommentModerationReason
		}
		rows = append(rows, adminCommentReview{
			ID: comment.PublicID, ContentID: comment.ContentItemID, Text: meta.Text,
			Reason: reason, AuthorID: comment.UserID, CreatedAt: comment.CreatedAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"data": rows, "total": len(rows), "tenant_id": principal.TenantID})
}

func AdminResolveCommentReview(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	commentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid comment id"})
		return
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || (body.Status != string(commentPolicyAllow) && body.Status != "removed") {
		c.JSON(http.StatusBadRequest, gin.H{"message": "status must be allow or removed"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	err = db.Transaction(func(tx *gorm.DB) error {
		var comment models.UserInteraction
		if err := tx.Model(&models.UserInteraction{}).Clauses(clause.Locking{Strength: "UPDATE", Table: clause.Table{Name: "user_interactions"}}).
			Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
			Where("content_items.tenant_id = ? AND user_interactions.public_id = ? AND user_interactions.type = ? AND user_interactions.comment_moderation_status = ?", principal.TenantID, commentID, models.InteractionTypeComment, string(commentPolicyReview)).
			First(&comment).Error; err != nil {
			return err
		}
		if body.Status == "removed" {
			if err := tx.Delete(&comment).Error; err != nil {
				return err
			}
			return writeModerationAudit(tx, principal, "moderation.comment."+body.Status, commentID.String(), nil)
		}
		status := string(commentPolicyAllow)
		if err := tx.Model(&comment).Updates(map[string]any{
			"comment_moderation_status": status,
			"comment_moderation_reason": nil,
		}).Error; err != nil {
			return err
		}
		if err := updateEngagementCount(tx, comment.ContentItemID, models.InteractionTypeComment, 1); err != nil {
			return err
		}
		return writeModerationAudit(tx, principal, "moderation.comment."+body.Status, commentID.String(), nil)
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"message": "Comment review not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to resolve comment review"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Comment review resolved"})
}

// AdminListModerationReports is a tenant-scoped operational queue. The queue
// intentionally returns only the bounded report detail, not reporter identity.
func AdminListModerationReports(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "30"))
	if limit < 1 {
		limit = 30
	}
	if limit > 100 {
		limit = 100
	}
	query := db.Model(&models.ModerationReport{}).Where("tenant_id = ?", principal.TenantID)
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		if status != "open" && status != "resolved" && status != "dismissed" {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid status"})
			return
		}
		query = query.Where("status = ?", status)
	}
	var total int64
	if err := query.Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to load moderation queue"})
		return
	}
	var reports []models.ModerationReport
	if err := query.Order("created_at ASC, id ASC").Offset((page - 1) * limit).Limit(limit).Find(&reports).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to load moderation queue"})
		return
	}
	commentIDs := make([]uuid.UUID, 0, len(reports))
	for _, report := range reports {
		if report.TargetType == models.ModerationTargetComment {
			commentIDs = append(commentIDs, report.TargetID)
		}
	}
	authors := make(map[uuid.UUID]uuid.UUID, len(commentIDs))
	if len(commentIDs) > 0 {
		var comments []models.UserInteraction
		if err := db.Where("public_id IN ? AND type = ? AND user_id IS NOT NULL", commentIDs, models.InteractionTypeComment).Find(&comments).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to load reported comment authors"})
			return
		}
		for _, comment := range comments {
			if comment.UserID != nil {
				authors[comment.PublicID] = *comment.UserID
			}
		}
	}
	suspendedAuthors := make(map[uuid.UUID]struct{}, len(authors))
	if len(authors) > 0 {
		authorIDs := make([]uuid.UUID, 0, len(authors))
		for _, authorID := range authors {
			authorIDs = append(authorIDs, authorID)
		}
		var suspensions []models.AuthSuspension
		if err := db.Where("tenant_id = ? AND user_id IN ?", principal.TenantID, authorIDs).Find(&suspensions).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to load account suspension state"})
			return
		}
		for _, suspension := range suspensions {
			suspendedAuthors[suspension.UserID] = struct{}{}
		}
	}
	response := make([]adminModerationReport, 0, len(reports))
	for _, report := range reports {
		row := adminModerationReport{ModerationReport: report}
		if report.TargetType == models.ModerationTargetComment {
			authorID, ok := authors[report.TargetID]
			if ok {
				row.AuthorID = &authorID
				_, row.AuthorSuspended = suspendedAuthors[authorID]
			}
		}
		response = append(response, row)
	}
	c.JSON(http.StatusOK, moderationQueueResponse{Data: response, Total: total, Page: page, Limit: limit, TotalPages: int((total + int64(limit) - 1) / int64(limit))})
}

var errModerationAlreadyReviewed = errors.New("This report was already reviewed. Refresh the case.")

func AdminResolveModerationReport(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	reportID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid report id"})
		return
	}
	var body struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || (body.Status != "resolved" && body.Status != "dismissed") {
		c.JSON(http.StatusBadRequest, gin.H{"message": "status must be resolved or dismissed"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	body.Reason = strings.TrimSpace(body.Reason)
	if len(body.Reason) > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Review reason is too long"})
		return
	}
	var report models.ModerationReport
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", reportID, principal.TenantID).First(&report).Error; err != nil {
			return err
		}
		if report.Status != "open" {
			return errModerationAlreadyReviewed
		}
		report.Status = body.Status
		if err := tx.Save(&report).Error; err != nil {
			return err
		}
		return writeModerationAudit(tx, principal, "moderation.report."+body.Status, reportID.String(), map[string]any{"target_type": report.TargetType, "target_id": report.TargetID.String(), "review_reason": body.Reason})
	})
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.JSON(http.StatusNotFound, gin.H{"message": "Report not found"})
		return
	case errors.Is(err, errModerationAlreadyReviewed):
		c.JSON(http.StatusConflict, gin.H{"message": err.Error()})
		return
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to update report"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": report})
}

// AdminRemoveComment removes a comment after a human review. This is separate
// from an owner's self-delete and decrements engagement in the same mutation.
func AdminRemoveComment(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	commentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid comment id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	err = db.Transaction(func(tx *gorm.DB) error {
		var comment models.UserInteraction
		if err := tx.Model(&models.UserInteraction{}).
			Clauses(clause.Locking{Strength: "UPDATE", Table: clause.Table{Name: "user_interactions"}}).
			Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
			Where("content_items.tenant_id = ? AND user_interactions.public_id = ? AND user_interactions.type = ?", principal.TenantID, commentID, models.InteractionTypeComment).First(&comment).Error; err != nil {
			return err
		}
		if comment.CommentModerationStatus == nil || *comment.CommentModerationStatus == string(commentPolicyAllow) {
			if err := updateEngagementCount(tx, comment.ContentItemID, models.InteractionTypeComment, -1); err != nil {
				return err
			}
		}
		if err := tx.Delete(&comment).Error; err != nil {
			return err
		}
		return writeModerationAudit(tx, principal, "moderation.comment.remove", commentID.String(), nil)
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"message": "Comment not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to remove comment"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Comment removed"})
}

func writeModerationAudit(db *gorm.DB, principal utils.AdminPrincipal, action, target string, payload map[string]any) error {
	encoded, _ := json.Marshal(payload)
	return db.Create(&models.AuditLog{TenantID: principal.TenantID, UserID: principal.UserID, UserEmail: principal.Email, Action: action, TargetService: "cms", TargetResource: target, Status: "success", Payload: datatypes.JSON(encoded)}).Error
}

func AdminGetModerationReport(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(400, gin.H{"message": "Invalid report ID"})
		return
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	var report models.ModerationReport
	err = db.Where("public_id = ? AND tenant_id = ?", id, p.TenantID).First(&report).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(404, gin.H{"message": "Report not found"})
		return
	}
	if err != nil {
		c.JSON(503, gin.H{"message": "Case is unavailable"})
		return
	}
	result := gin.H{"report": report, "target_available": false}
	parentID := report.TargetID
	if report.TargetType == models.ModerationTargetComment {
		var comment models.UserInteraction
		err = db.Model(&models.UserInteraction{}).
			Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
			Where("content_items.tenant_id = ? AND user_interactions.public_id = ? AND user_interactions.type = ?", p.TenantID, report.TargetID, models.InteractionTypeComment).First(&comment).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(200, gin.H{"data": result})
			return
		}
		if err != nil {
			c.JSON(503, gin.H{"message": "Comment evidence is unavailable"})
			return
		}
		parentID = comment.ContentItemID
		var meta commentMetadata
		if json.Unmarshal(comment.Metadata, &meta) == nil {
			result["comment_text"] = meta.Text
		}
	}
	var content models.ContentItem
	err = db.Select("public_id", "title").Where("tenant_id = ? AND public_id = ?", p.TenantID, parentID).First(&content).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(200, gin.H{"data": result})
		return
	}
	if err != nil {
		c.JSON(503, gin.H{"message": "Parent evidence is unavailable"})
		return
	}
	result["target_available"] = true
	result["parent_id"] = content.PublicID
	result["parent_title"] = content.Title
	c.JSON(200, gin.H{"data": result})
}

func AdminNextModerationReport(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil || id == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid report ID"})
		return
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	var next struct{ ID *uuid.UUID }
	// A lateral read preserves the tenant-scoped anchor even after closure.
	result := db.Raw(`
		SELECT next.public_id AS id
		FROM moderation_reports anchor
		LEFT JOIN LATERAL (
			SELECT candidate.public_id
			FROM moderation_reports candidate
			WHERE candidate.tenant_id = anchor.tenant_id AND candidate.status = 'open'
				AND (candidate.created_at, candidate.id) > (anchor.created_at, anchor.id)
			ORDER BY candidate.created_at, candidate.id LIMIT 1
		) next ON TRUE
		WHERE anchor.tenant_id = ? AND anchor.public_id = ?
	`, p.TenantID, id).Scan(&next)
	if result.Error != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Could not check the next case"})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"message": "Report not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": next.ID}})
}
