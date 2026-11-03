package controllers

import (
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/newscollection"
	"content-management-system/src/supply"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func newsCapabilities(c *gin.Context) newscollection.Capabilities {
	caps := newscollection.Capabilities{SchedulerReady: supply.SourceRunSchedulerHealthy(time.Now())}
	base := strings.TrimRight(os.Getenv("AGGREGATION_BASE_URL"), "/")
	if base == "" {
		return caps
	}
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, base+"/internal/source-capabilities", nil)
	if err != nil {
		return caps
	}
	req.Header.Set("Authorization", "Bearer "+aggregationInternalServiceToken())
	response, err := (&http.Client{Timeout: 4 * time.Second}).Do(req)
	if err != nil {
		return caps
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return caps
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 16*1024+1))
	if err != nil || len(raw) > 16*1024 {
		return caps
	}
	var remote newscollection.Capabilities
	if json.Unmarshal(raw, &remote) != nil {
		return caps
	}
	remote.SchedulerReady = caps.SchedulerReady
	return remote
}

func GetNewsCollectionStatus(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	caps := newsCapabilities(c)
	state, err := newscollection.Ready(db, p.TenantID, caps)
	if err != nil {
		newsCollectionError(c, err)
		return
	}
	var sources []models.ContentSource
	if err := db.Where("tenant_id=? AND category='news'", p.TenantID).Find(&sources).Error; err != nil {
		newsCollectionError(c, err)
		return
	}
	enabled, needsConfig := 0, 0
	issues := []gin.H{}
	for _, s := range sources {
		if s.IsActive {
			enabled++
		}
		if reason := newscollection.SourceBlocker(s, caps); reason != "" {
			needsConfig++
			issues = append(issues, gin.H{"id": s.PublicID, "name": s.Name, "type": s.Type, "reason": reason})
		}
	}
	var operations []models.NewsSourceOperation
	if err := db.Where("tenant_id=? AND applied_at IS NOT NULL", p.TenantID).Order("applied_at DESC").Limit(10).Find(&operations).Error; err != nil {
		newsCollectionError(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(200, gin.H{"readiness": state, "total": len(sources), "enabled": enabled, "needs_configuration": needsConfig, "configuration_issues": issues, "environment": os.Getenv("WAHB_ENVIRONMENT_ID"), "tenant_id": p.TenantID, "operations": operations, "can_manage": p.HasPermission("source:write")})
}

func PreviewNewsSourceOperation(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req newscollection.Request
	decoder := json.NewDecoder(io.LimitReader(c.Request.Body, 64*1024))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&req) != nil {
		c.JSON(400, gin.H{"message": "Invalid News operation"})
		return
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		c.JSON(400, gin.H{"message": "Invalid News operation"})
		return
	}
	op, err := newscollection.Preview(c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context()), p.TenantID, p.UserID, req, newsCapabilities(c))
	if err != nil {
		newsCollectionError(c, err)
		return
	}
	c.JSON(200, op)
}

func ApplyNewsSourceOperation(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("operation"))
	if err != nil {
		c.JSON(400, gin.H{"message": "Invalid operation ID"})
		return
	}
	result, err := newscollection.Apply(c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context()), p.TenantID, p.UserID, id, newsCapabilities(c))
	if err != nil {
		newsCollectionError(c, err)
		return
	}
	c.JSON(200, result)
}

func newsCollectionError(c *gin.Context, err error) {
	if err == gorm.ErrRecordNotFound {
		c.JSON(404, gin.H{"message": "News operation not found"})
		return
	}
	var domain *newscollection.OperationError
	if errors.As(err, &domain) || errors.Is(err, newscollection.ErrConflict) || lifecycle.IsConflict(err) || lifecycle.IsIntakePaused(err) {
		c.JSON(409, gin.H{"message": err.Error(), "code": "NEWS_COLLECTION_CONFLICT"})
		return
	}
	c.JSON(500, gin.H{"message": "News collection is unavailable. Refresh or inspect CMS health.", "code": "NEWS_COLLECTION_UNAVAILABLE"})
}
