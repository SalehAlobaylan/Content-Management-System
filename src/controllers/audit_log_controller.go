package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type createAuditLogRequest struct {
	Action         string                 `json:"action"`
	TargetService  string                 `json:"target_service"`
	TargetResource string                 `json:"target_resource,omitempty"`
	Status         string                 `json:"status"`
	ErrorMessage   string                 `json:"error_message,omitempty"`
	Payload        map[string]interface{} `json:"payload,omitempty"`
}

type auditLogResponse struct {
	ID             string                 `json:"id"`
	TenantID       string                 `json:"tenant_id"`
	UserID         string                 `json:"user_id"`
	UserEmail      string                 `json:"user_email"`
	Action         string                 `json:"action"`
	TargetService  string                 `json:"target_service"`
	TargetResource string                 `json:"target_resource,omitempty"`
	Status         string                 `json:"status"`
	ErrorMessage   string                 `json:"error_message,omitempty"`
	Payload        map[string]interface{} `json:"payload,omitempty"`
	CreatedAt      string                 `json:"created_at"`
}

type auditLogListResponse struct {
	Data       []auditLogResponse `json:"data"`
	Total      int64              `json:"total"`
	Page       int                `json:"page"`
	Limit      int                `json:"limit"`
	TotalPages int                `json:"total_pages"`
}

func validAuditStatus(s string) bool {
	return s == "success" || s == "failure"
}

func toAuditResponse(log models.AuditLog) auditLogResponse {
	var payload map[string]interface{}
	if len(log.Payload) > 0 {
		_ = json.Unmarshal(log.Payload, &payload)
	}
	return auditLogResponse{
		ID:             log.PublicID.String(),
		TenantID:       log.TenantID,
		UserID:         log.UserID,
		UserEmail:      log.UserEmail,
		Action:         log.Action,
		TargetService:  log.TargetService,
		TargetResource: log.TargetResource,
		Status:         log.Status,
		ErrorMessage:   log.ErrorMessage,
		Payload:        payload,
		CreatedAt:      log.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}

// CreateAuditLog records an admin-executed action. Caller (Platform-Console)
// posts one entry per action, success or failure.
func CreateAuditLog(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required", "code": "AUTH_REQUIRED"})
		return
	}

	var req createAuditLogRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid request body", "code": "INVALID_BODY"})
		return
	}

	req.Action = strings.TrimSpace(req.Action)
	req.TargetService = strings.TrimSpace(req.TargetService)
	req.Status = strings.TrimSpace(strings.ToLower(req.Status))

	if req.Action == "" || req.TargetService == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "action and target_service are required", "code": "INVALID_BODY"})
		return
	}
	if !validAuditStatus(req.Status) {
		c.JSON(http.StatusBadRequest, gin.H{"message": "status must be 'success' or 'failure'", "code": "INVALID_BODY"})
		return
	}

	entry := models.AuditLog{
		TenantID:       principal.TenantID,
		UserID:         principal.UserID,
		UserEmail:      principal.Email,
		Action:         req.Action,
		TargetService:  req.TargetService,
		TargetResource: req.TargetResource,
		Status:         req.Status,
		ErrorMessage:   req.ErrorMessage,
	}

	if req.Payload != nil {
		raw, err := json.Marshal(req.Payload)
		if err == nil {
			entry.Payload = datatypes.JSON(raw)
		}
	}

	if err := db.Create(&entry).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to record audit log", "code": "DB_ERROR"})
		return
	}

	c.JSON(http.StatusCreated, toAuditResponse(entry))
}

// ListAuditLogs returns recent audit entries, scoped to the caller's tenant.
// Query params: page, limit (max 100), service, action.
func ListAuditLogs(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required", "code": "AUTH_REQUIRED"})
		return
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "25"))
	if limit < 1 || limit > 100 {
		limit = 25
	}
	offset := (page - 1) * limit

	query := db.Model(&models.AuditLog{}).Where("tenant_id = ?", principal.TenantID)
	if svc := strings.TrimSpace(c.Query("service")); svc != "" {
		query = query.Where("target_service = ?", svc)
	}
	if act := strings.TrimSpace(c.Query("action")); act != "" {
		query = query.Where("action = ?", act)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to count audit logs", "code": "DB_ERROR"})
		return
	}

	var logs []models.AuditLog
	if err := query.Order("created_at DESC").Limit(limit).Offset(offset).Find(&logs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to load audit logs", "code": "DB_ERROR"})
		return
	}

	out := make([]auditLogResponse, 0, len(logs))
	for _, log := range logs {
		out = append(out, toAuditResponse(log))
	}

	totalPages := int((total + int64(limit) - 1) / int64(limit))
	c.JSON(http.StatusOK, auditLogListResponse{
		Data:       out,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
	})
}
