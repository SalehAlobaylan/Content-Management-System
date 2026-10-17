package economics

import (
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"time"
)

type ContentRule struct {
	TenantID      string    `json:"-"`
	Kind          string    `json:"kind"`
	Version       int       `json:"version"`
	EffectiveFrom time.Time `json:"effective_from"`
	AllocableBPS  int       `json:"allocable_bps"`
	Observation   string    `json:"observation"`
	Evidence      string    `json:"evidence"`
	Actor         string    `json:"actor"`
}

func (ContentRule) TableName() string { return "economics_content_rules" }
func ContentRuleWrite(c *gin.Context) {
	s, ok := requestScope(c)
	if !ok {
		return
	}
	var rule ContentRule
	if c.ShouldBindJSON(&rule) != nil || rule.Kind != "pods" && rule.Kind != "news" || rule.AllocableBPS < 0 || rule.AllocableBPS > 10000 || rule.Observation != "mobile" && rule.Observation != "web_completion" || rule.Kind == "news" && rule.Observation != "mobile" || len(rule.Evidence) < 10 || len(rule.Evidence) > 4000 || rule.EffectiveFrom.Before(time.Now().UTC().Truncate(24*time.Hour)) {
		c.JSON(400, gin.H{"error": "A prospective fixed rule, measured covered portion and qualified observation source are required"})
		return
	}
	rule.TenantID = s.Tenant
	rule.Actor = s.Actor
	e := s.DB.Transaction(func(tx *gorm.DB) error {
		if e := Lock(tx); e != nil {
			return e
		}
		var version int
		if e := tx.Model(&ContentRule{}).Where("tenant_id=? AND kind=?", s.Tenant, rule.Kind).Select("COALESCE(max(version),0)").Scan(&version).Error; e != nil {
			return e
		}
		rule.Version = version + 1
		return tx.Create(&rule).Error
	})
	if fail(c, e) {
		return
	}
	c.JSON(201, rule)
}
