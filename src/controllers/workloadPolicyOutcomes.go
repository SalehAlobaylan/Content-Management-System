package controllers

import (
	"content-management-system/src/models"
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type workloadOutcomeWindow struct {
	Revision          int64     `json:"revision"`
	From              time.Time `json:"from"`
	Until             time.Time `json:"until"`
	Requests          int64     `json:"requests"`
	Deferred          int64     `json:"deferred"`
	ObserveWouldDefer int64     `json:"observe_would_defer"`
	Bypassed          int64     `json:"bypassed"`
	Available         bool      `json:"available"`
	Reason            string    `json:"reason,omitempty"`
}

func GetWorkloadPolicyOutcomes(c *gin.Context) {
	before, e1 := strconv.ParseInt(c.Query("before_revision"), 10, 64)
	after, e2 := strconv.ParseInt(c.Query("after_revision"), 10, 64)
	class := c.Query("class")
	if e1 != nil || e2 != nil || before <= 0 || after <= before || len(class) == 0 || len(class) > 48 {
		c.JSON(400, gin.H{"error": "Choose two ordered policy revisions and one workload class"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
	defer cancel()
	db := c.MustGet("db").(*gorm.DB).WithContext(ctx)
	windows := make([]workloadOutcomeWindow, 0, 2)
	for _, revision := range []int64{before, after} {
		window, err := readWorkloadOutcomeWindow(db, revision, class, time.Now().UTC())
		if err != nil {
			c.JSON(503, gin.H{"error": "Policy outcome evidence is unavailable"})
			return
		}
		windows = append(windows, window)
	}
	c.JSON(200, gin.H{"data": windows})
}

func readWorkloadOutcomeWindow(db *gorm.DB, revision int64, class string, now time.Time) (workloadOutcomeWindow, error) {
	w := workloadOutcomeWindow{Revision: revision}
	var change models.WorkloadDecision
	receipts := func() *gorm.DB {
		return db.Where("tenant_id=? AND system=? AND class=? AND event=?", "platform", "workload_governor", "policy", "policy_changed")
	}
	err := receipts().Where("policy_revision=?", revision).First(&change).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		w.Reason = "The saved policy receipt is unavailable."
		return w, nil
	}
	if err != nil {
		return w, err
	}
	w.From, w.Until = change.CreatedAt, change.CreatedAt.Add(24*time.Hour)
	if now.Before(w.Until) {
		w.Reason = "The first 24 hours of this revision are still open."
		return w, nil
	}
	var later int64
	if err := receipts().Model(&models.WorkloadDecision{}).Where("policy_revision > ? AND created_at < ?", revision, w.Until).Count(&later).Error; err != nil {
		return w, err
	}
	if later != 0 {
		w.Reason = "Another revision intervened during the first 24 hours."
		return w, nil
	}
	if err := db.Raw(`
		SELECT count(*) AS requests,
			count(*) FILTER (WHERE verdict='defer') AS deferred,
			count(*) FILTER (WHERE mode='observe' AND would_verdict='defer') AS observe_would_defer,
			count(*) FILTER (WHERE evidence->>'bypass'='true') AS bypassed
		FROM workload_decisions
		WHERE event='request' AND class=? AND policy_revision=? AND created_at>=? AND created_at<?
	`, class, revision, w.From, w.Until).Scan(&w).Error; err != nil {
		return w, err
	}
	w.Available = w.Requests >= 20
	if !w.Available {
		w.Reason = "Fewer than 20 recorded requests for this class."
	}
	return w, nil
}
