package controllers

import (
	"content-management-system/src/models"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func FindConsoleSources(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	q := strings.TrimSpace(c.Query("q"))
	if count := utf8.RuneCountInString(q); count < 2 || count > 160 {
		c.JSON(400, gin.H{"message": "Enter a source name between 2 and 160 characters"})
		return
	}
	type sourceHit struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		Type     string `json:"type"`
		Category string `json:"category"`
	}
	items := []sourceHit{}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	err := db.Model(&models.ContentSource{}).Select("public_id::text AS id, name, type, category").
		Where("tenant_id = ? AND name ILIKE ?", p.TenantID, "%"+q+"%").
		Order("name, public_id").Limit(6).Scan(&items).Error
	if err != nil {
		c.JSON(503, gin.H{"message": "Source search is unavailable"})
		return
	}
	c.JSON(200, gin.H{"items": items})
}

func GetConsoleNewsStory(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(400, gin.H{"message": "Invalid story ID"})
		return
	}
	var story models.Story
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	err = db.Select("public_id", "label").Where("tenant_id = ? AND public_id = ?", p.TenantID, id).First(&story).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(404, gin.H{"message": "Story not found"})
		return
	}
	if err != nil {
		c.JSON(503, gin.H{"message": "Story is unavailable"})
		return
	}
	c.JSON(200, gin.H{"id": story.PublicID, "label": story.Label})
}
