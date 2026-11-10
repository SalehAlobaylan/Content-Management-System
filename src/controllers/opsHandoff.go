package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type opsHandoff struct {
	TenantID  string     `json:"-" gorm:"primaryKey"`
	EpisodeID uuid.UUID  `json:"episode_id" gorm:"primaryKey"`
	OwnerID   string     `json:"-"`
	OwnerName string     `json:"owner"`
	NextStep  string     `json:"next_step"`
	NextCheck *time.Time `json:"next_check,omitempty"`
	Revision  int64      `json:"revision"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type opsHandoffView struct {
	opsHandoff
	Mine bool `json:"mine"`
}

var errHandoffChanged = errors.New("Issue or ownership changed. Refresh Operations.")

func OpsUpdateHandoff(c *gin.Context) {
	p, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(401, gin.H{"error": "Unauthorized"})
		return
	}
	var req struct {
		EpisodeID   uuid.UUID  `json:"episode_id"`
		Fingerprint string     `json:"fingerprint"`
		Severity    string     `json:"severity"`
		Action      string     `json:"action"`
		Revision    int64      `json:"revision"`
		NextStep    string     `json:"next_step"`
		NextCheck   *time.Time `json:"next_check"`
	}
	if c.ShouldBindJSON(&req) != nil || req.EpisodeID == uuid.Nil || req.Revision < 0 {
		c.JSON(400, gin.H{"error": "Invalid handoff review"})
		return
	}
	now := time.Now().UTC()
	req.NextStep = strings.TrimSpace(req.NextStep)
	if req.Action != "release" && ((req.Action != "claim" && req.Action != "update") || req.NextStep == "" || len(req.NextStep) > 500 || req.NextCheck == nil || !req.NextCheck.After(now) || req.NextCheck.After(now.Add(30*24*time.Hour))) {
		c.JSON(400, gin.H{"error": "Enter a next step and a check time within 30 days"})
		return
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	row := opsHandoff{TenantID: p.TenantID, EpisodeID: req.EpisodeID}
	err := db.Transaction(func(tx *gorm.DB) error {
		var episode models.FeedIntegrityEpisode
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND status IN ('open','recovering')", p.TenantID, req.EpisodeID).First(&episode).Error
		if err != nil {
			return err
		}
		if req.Fingerprint != opsFingerprint("feed_integrity", "episode", p.TenantID, req.EpisodeID.String()) || req.Severity != opsNormalizeSeverity(episode.Severity) {
			return errHandoffChanged
		}
		err = tx.Where("tenant_id=? AND episode_id=?", p.TenantID, req.EpisodeID).First(&row).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if row.Revision != req.Revision {
			return errHandoffChanged
		}
		if req.Action == "claim" {
			if row.OwnerID != "" {
				return errHandoffChanged
			}
		} else if row.OwnerID != p.UserID {
			return errHandoffChanged
		}
		row.Revision++
		row.UpdatedAt = now
		if req.Action == "release" {
			row.OwnerID, row.OwnerName, row.NextStep, row.NextCheck = "", "", "", nil
		} else {
			row.OwnerID, row.OwnerName, row.NextStep, row.NextCheck = p.UserID, p.Email, req.NextStep, req.NextCheck
		}
		return tx.Save(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, errHandoffChanged) {
		c.JSON(409, gin.H{"error": errHandoffChanged.Error()})
		return
	}
	if err != nil {
		c.JSON(503, gin.H{"error": "Handoff is unavailable"})
		return
	}
	c.JSON(200, gin.H{"data": opsHandoffView{row, row.OwnerID == p.UserID}})
}

func readOpsHandoffs(db *gorm.DB, tenant, user string, ids []uuid.UUID) ([]opsHandoffView, error) {
	result := []opsHandoffView{}
	q := db.Where("ops_handoffs.tenant_id=?", tenant)
	if ids != nil {
		if len(ids) == 0 {
			return result, nil
		}
		q = q.Where("ops_handoffs.episode_id IN ?", ids)
	} else {
		q = q.Joins("JOIN feed_integrity_episodes e ON e.tenant_id=ops_handoffs.tenant_id AND e.public_id=ops_handoffs.episode_id").
			Where("e.status IN ('open','recovering') AND ops_handoffs.owner_id <> ''")
	}
	var rows []opsHandoff
	if err := q.Order("ops_handoffs.updated_at DESC").Limit(200).Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		result = append(result, opsHandoffView{row, row.OwnerID == user})
	}
	return result, nil
}
