package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type adminSourceListResponse struct {
	Data       []contentSourceResponse `json:"data"`
	Total      int64                   `json:"total"`
	Page       int                     `json:"page"`
	Limit      int                     `json:"limit"`
	TotalPages int                     `json:"total_pages"`
}

type contentSourceResponse struct {
	ID                   string          `json:"id"`
	Name                 string          `json:"name"`
	Type                 string          `json:"type"`
	FeedURL              *string         `json:"feed_url,omitempty"`
	APIConfig            json.RawMessage `json:"api_config,omitempty"`
	IsActive             bool            `json:"is_active"`
	FetchIntervalMinutes int             `json:"fetch_interval_minutes"`
	LastFetchedAt        *string         `json:"last_fetched_at,omitempty"`
	Metadata             json.RawMessage `json:"metadata,omitempty"`
	CreatedAt            string          `json:"created_at"`
	UpdatedAt            string          `json:"updated_at"`
}

type createContentSourceRequest struct {
	Name                 string                 `json:"name"`
	Type                 string                 `json:"type"`
	FeedURL              *string                `json:"feed_url,omitempty"`
	APIConfig            map[string]interface{} `json:"api_config,omitempty"`
	IsActive             *bool                  `json:"is_active,omitempty"`
	FetchIntervalMinutes *int                   `json:"fetch_interval_minutes,omitempty"`
	Metadata             map[string]interface{} `json:"metadata,omitempty"`
}

type updateContentSourceRequest struct {
	Name                 *string                `json:"name,omitempty"`
	Type                 *string                `json:"type,omitempty"`
	FeedURL              *string                `json:"feed_url,omitempty"`
	APIConfig            map[string]interface{} `json:"api_config,omitempty"`
	IsActive             *bool                  `json:"is_active,omitempty"`
	FetchIntervalMinutes *int                   `json:"fetch_interval_minutes,omitempty"`
	Metadata             map[string]interface{} `json:"metadata,omitempty"`
}

type runSourceResponse struct {
	Message string `json:"message"`
	JobID   string `json:"job_id,omitempty"`
}

var contentSourceQueryConfig = utils.QueryConfig{
	DefaultLimit: 20,
	MaxLimit:     100,
	DefaultSort: []utils.SortParam{{
		Field:     "created_at",
		Direction: "desc",
	}},
	SortableFields: map[string]string{
		"created_at":      "content_sources.created_at",
		"updated_at":      "content_sources.updated_at",
		"name":            "content_sources.name",
		"type":            "content_sources.type",
		"last_fetched_at": "content_sources.last_fetched_at",
	},
	FilterableFields: map[string]string{
		"name":       "content_sources.name",
		"type":       "content_sources.type",
		"is_active":  "content_sources.is_active",
		"created_at": "content_sources.created_at",
		"updated_at": "content_sources.updated_at",
	},
	SearchableFields: map[string]string{
		"name":     "content_sources.name",
		"feed_url": "content_sources.feed_url",
	},
	DefaultSearchFields: []string{"name", "feed_url"},
	FieldDefaultOperators: map[string]string{
		"name":     "contains",
		"feed_url": "contains",
	},
}

// ListContentSources handles GET /admin/sources
func ListContentSources(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)

	params, err := utils.ParseQueryParams(c, contentSourceQueryConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: err.Error(),
			Code:    "INVALID_QUERY",
		})
		return
	}

	query := db.Model(&models.ContentSource{}).Where("tenant_id = ?", principal.TenantID)
	query = utils.ApplyQuery(query, params, contentSourceQueryConfig)

	var sources []models.ContentSource
	meta, err := utils.FetchWithPagination(query, params, &sources)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to fetch sources",
			Code:    "FETCH_FAILED",
		})
		return
	}

	data := make([]contentSourceResponse, 0, len(sources))
	for _, source := range sources {
		data = append(data, mapContentSourceResponse(source))
	}

	c.JSON(http.StatusOK, adminSourceListResponse{
		Data:       data,
		Total:      meta.Total,
		Page:       meta.Page,
		Limit:      meta.Limit,
		TotalPages: meta.TotalPages,
	})
}

// GetContentSource handles GET /admin/sources/:id
func GetContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid source ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Source not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	c.JSON(http.StatusOK, mapContentSourceResponse(source))
}

// CreateContentSource handles POST /admin/sources
func CreateContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	var req createContentSourceRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Name is required",
			Code:    "NAME_REQUIRED",
		})
		return
	}

	sourceType := strings.TrimSpace(req.Type)
	if sourceType == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Type is required",
			Code:    "TYPE_REQUIRED",
		})
		return
	}

	apiConfig, err := mapToJSON(req.APIConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid api_config",
			Code:    "INVALID_API_CONFIG",
		})
		return
	}
	metadata, err := mapToJSON(req.Metadata)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid metadata",
			Code:    "INVALID_METADATA",
		})
		return
	}

	isActive := true
	if req.IsActive != nil {
		isActive = *req.IsActive
	}

	fetchInterval := 60
	if req.FetchIntervalMinutes != nil {
		if *req.FetchIntervalMinutes <= 0 {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Fetch interval must be greater than zero",
				Code:    "INVALID_FETCH_INTERVAL",
			})
			return
		}
		fetchInterval = *req.FetchIntervalMinutes
	}

	source := models.ContentSource{
		TenantID:             principal.TenantID,
		Name:                 name,
		Type:                 models.SourceType(strings.ToUpper(sourceType)),
		FeedURL:              req.FeedURL,
		APIConfig:            apiConfig,
		IsActive:             isActive,
		FetchIntervalMinutes: fetchInterval,
		Metadata:             metadata,
	}

	if err := db.Create(&source).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to create source",
			Code:    "CREATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusCreated, mapContentSourceResponse(source))
}

// UpdateContentSource handles PUT /admin/sources/:id
func UpdateContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid source ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var req updateContentSourceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid request",
			Code:    "INVALID_REQUEST",
		})
		return
	}

	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Source not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Name cannot be empty",
				Code:    "NAME_REQUIRED",
			})
			return
		}
		source.Name = name
	}

	if req.Type != nil {
		sourceType := strings.TrimSpace(*req.Type)
		if sourceType == "" {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Type cannot be empty",
				Code:    "TYPE_REQUIRED",
			})
			return
		}
		source.Type = models.SourceType(strings.ToUpper(sourceType))
	}

	if req.FeedURL != nil {
		source.FeedURL = req.FeedURL
	}

	if req.APIConfig != nil {
		apiConfig, err := mapToJSON(req.APIConfig)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Invalid api_config",
				Code:    "INVALID_API_CONFIG",
			})
			return
		}
		source.APIConfig = apiConfig
	}

	if req.Metadata != nil {
		metadata, err := mapToJSON(req.Metadata)
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Invalid metadata",
				Code:    "INVALID_METADATA",
			})
			return
		}
		source.Metadata = metadata
	}

	if req.IsActive != nil {
		source.IsActive = *req.IsActive
	}

	if req.FetchIntervalMinutes != nil {
		if *req.FetchIntervalMinutes <= 0 {
			c.JSON(http.StatusBadRequest, authErrorResponse{
				Message: "Fetch interval must be greater than zero",
				Code:    "INVALID_FETCH_INTERVAL",
			})
			return
		}
		source.FetchIntervalMinutes = *req.FetchIntervalMinutes
	}

	if err := db.Save(&source).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to update source",
			Code:    "UPDATE_FAILED",
		})
		return
	}

	c.JSON(http.StatusOK, mapContentSourceResponse(source))
}

// DeleteContentSource handles DELETE /admin/sources/:id
func DeleteContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid source ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Source not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	if err := db.Delete(&source).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{
			Message: "Failed to delete source",
			Code:    "DELETE_FAILED",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

// RunContentSource handles POST /admin/sources/:id/run
func RunContentSource(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	publicID := c.Param("id")
	id, err := uuid.Parse(publicID)
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{
			Message: "Invalid source ID",
			Code:    "INVALID_ID",
		})
		return
	}

	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", id, principal.TenantID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{
			Message: "Source not found",
			Code:    "NOT_FOUND",
		})
		return
	}

	now := time.Now().UTC()
	source.LastFetchedAt = &now
	_ = db.Save(&source).Error

	c.JSON(http.StatusOK, runSourceResponse{
		Message: "Source run queued",
	})
}

func mapContentSourceResponse(source models.ContentSource) contentSourceResponse {
	var lastFetched *string
	if source.LastFetchedAt != nil {
		formatted := source.LastFetchedAt.UTC().Format(time.RFC3339)
		lastFetched = &formatted
	}

	return contentSourceResponse{
		ID:                   source.PublicID.String(),
		Name:                 source.Name,
		Type:                 string(source.Type),
		FeedURL:              source.FeedURL,
		APIConfig:            json.RawMessage(source.APIConfig),
		IsActive:             source.IsActive,
		FetchIntervalMinutes: source.FetchIntervalMinutes,
		LastFetchedAt:        lastFetched,
		Metadata:             json.RawMessage(source.Metadata),
		CreatedAt:            source.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:            source.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func mapToJSON(value map[string]interface{}) (datatypes.JSON, error) {
	if value == nil {
		return nil, nil
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return datatypes.JSON(bytes), nil
}
