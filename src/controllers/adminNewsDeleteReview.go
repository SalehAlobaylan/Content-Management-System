package controllers

import (
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type newsDeleteReview struct {
	ID           uuid.UUID
	TenantID     string
	Actor        string
	Selection    datatypes.JSON
	Targets      datatypes.JSON
	ExpiresAt    time.Time
	DeletedCount *int64
}

type newsDeleteTarget struct {
	ID        uuid.UUID
	UpdatedAt time.Time
}

var errNewsDeleteReviewChanged = errors.New("review expired or its selected items changed; preview again")

func PreviewNewsContentDelete(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req bulkDeleteContentRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.IDs) != 0 || req.CreatedBefore != "" || req.Topic != "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid News delete filters"})
		return
	}
	switch req.Type {
	case "NEWS", "ARTICLE", "TWEET", "COMMENT":
	default:
		c.JSON(http.StatusBadRequest, gin.H{"message": "Select a News type before previewing deletion"})
		return
	}
	if req.StoryID != "" && req.StoryID != "none" {
		if _, err := uuid.Parse(req.StoryID); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid story ID"})
			return
		}
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	targets, err := loadBulkDeleteTargets(bulkContentDeleteQuery(db, p.TenantID, req))
	if err != nil {
		newsDeleteReviewError(c, err)
		return
	}
	if len(targets) == 0 {
		c.JSON(http.StatusOK, gin.H{"count": 0})
		return
	}
	snapshot := make([]newsDeleteTarget, 0, len(targets))
	for _, row := range targets {
		snapshot = append(snapshot, newsDeleteTarget{row.PublicID, row.UpdatedAt})
	}
	filters, _ := json.Marshal(req)
	rows, _ := json.Marshal(snapshot)
	review := newsDeleteReview{ID: uuid.New(), TenantID: p.TenantID, Actor: p.UserID, Selection: filters, Targets: rows, ExpiresAt: time.Now().UTC().Add(10 * time.Minute)}
	if err := db.Create(&review).Error; err != nil {
		newsDeleteReviewError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": review.ID, "count": len(targets), "expires_at": review.ExpiresAt})
}

func ApplyNewsContentDelete(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid review ID"})
		return
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	var deleted int64
	err = db.Transaction(func(tx *gorm.DB) error {
		var review newsDeleteReview
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ? AND actor = ?", id, p.TenantID, p.UserID).First(&review).Error; err != nil {
			return err
		}
		if review.DeletedCount != nil {
			deleted = *review.DeletedCount
			return nil
		}
		if !time.Now().Before(review.ExpiresAt) {
			return errNewsDeleteReviewChanged
		}
		var selection bulkDeleteContentRequest
		var expected []newsDeleteTarget
		if err := json.Unmarshal(review.Selection, &selection); err != nil {
			return err
		}
		if err := json.Unmarshal(review.Targets, &expected); err != nil {
			return err
		}
		query := bulkContentDeleteQuery(tx, p.TenantID, selection)
		targets, err := loadBulkDeleteTargets(query)
		if err != nil {
			return err
		}
		if !newsDeleteTargetsMatch(targets, expected) {
			return errNewsDeleteReviewChanged
		}
		if err := deleteBulkContentTargets(tx, p.TenantID, query, targets, &deleted); err != nil {
			return err
		}
		return tx.Model(&review).Update("deleted_count", deleted).Error
	})
	if err != nil {
		newsDeleteReviewError(c, err)
		return
	}
	c.JSON(http.StatusOK, bulkDeleteContentResponse{DeletedCount: deleted, Message: "Successfully deleted the reviewed items"})
}

func newsDeleteTargetsMatch(actual []models.ContentItem, expected []newsDeleteTarget) bool {
	if len(actual) == 0 || len(actual) != len(expected) {
		return false
	}
	for i, item := range actual {
		if item.PublicID != expected[i].ID || !item.UpdatedAt.Equal(expected[i].UpdatedAt) {
			return false
		}
	}
	return true
}

func newsDeleteReviewError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.JSON(http.StatusNotFound, gin.H{"message": "Delete review not found"})
	case errors.Is(err, errBulkDeleteScopeTooLarge):
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"message": err.Error()})
	case errors.Is(err, errNewsDeleteReviewChanged), errors.Is(err, errBulkDeleteScopeChanged), errors.Is(err, errBulkDeletePodsRequired), lifecycle.IsConflict(err):
		c.JSON(http.StatusConflict, gin.H{"message": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Unable to verify the News delete review"})
	}
}

func GetNewsContentDeleteReview(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid review ID"})
		return
	}
	var review newsDeleteReview
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	if err := db.Where("id=? AND tenant_id=? AND actor=?", id, p.TenantID, p.UserID).First(&review).Error; err != nil {
		newsDeleteReviewError(c, err)
		return
	}
	state := "pending"
	if review.DeletedCount != nil {
		state = "applied"
	} else if !time.Now().Before(review.ExpiresAt) {
		state = "expired"
	}
	c.JSON(http.StatusOK, gin.H{"id": review.ID, "state": state, "deleted_count": review.DeletedCount, "expires_at": review.ExpiresAt})
}
