package controllers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func opsRequestDB(c *gin.Context) (*gorm.DB, context.CancelFunc, time.Time) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), opsReadDeadline)
	return c.MustGet("db").(*gorm.DB).WithContext(ctx), cancel, time.Now().UTC()
}
func opsRegistryView() []gin.H {
	out := []gin.H{}
	for _, m := range opsFleetRegistry {
		lanes := []gin.H{}
		for _, l := range m.Lanes {
			lanes = append(lanes, gin.H{"key": l.Key, "label": l.Label, "scheduled": l.Scheduled, "manual_only": l.ManualOnly, "pausable": l.PauseColumn != "", "cockpit_path": l.CockpitPath, "run_evidence": l.RunTable != ""})
		}
		out = append(out, gin.H{"key": m.Key, "label": m.Label, "family": m.Family, "kind": m.Kind, "cockpit_path": m.CockpitPath, "lanes": lanes})
	}
	return out
}

func prepareOpsSnapshot(c *gin.Context, db *gorm.DB, now time.Time) opsSnapshot {
	s := collectOpsSnapshot(db, now, true)
	principal, _ := utils.GetAdminPrincipal(c)
	var err error
	s.Attention, err = applyOpsAttentionStateRead(db, principal.TenantID, s.Attention, now)
	if err != nil {
		s.Errors = append(s.Errors, "attention_state: "+opsReadError(err))
	}
	if err = opsApplyContainment(db, s.Fleet); err != nil {
		s.Errors = append(s.Errors, "containment: "+opsReadError(err))
	}
	for i := range s.Fleet {
		for _, item := range s.Attention {
			if item.System == s.Fleet[i].MemberKey && (item.TenantID == s.Fleet[i].TenantID || s.Fleet[i].TenantID == "") {
				s.Fleet[i].OpenAttentionCount++
			}
		}
	}
	s.Complete = len(s.Errors) == 0
	return s
}
func GetOpsStatus(c *gin.Context) {
	db, cancel, now := opsRequestDB(c)
	defer cancel()
	s := prepareOpsSnapshot(c, db, now)
	headline, summary := opsHeadlineAt(s.Attention, s.Fleet, now)
	if !s.Complete && headline != "incident" {
		headline = "attention"
		summary = "Fleet evidence is incomplete; " + summary
	}
	counts, kinds := map[string]int{}, map[string]int{}
	suppressed := 0
	for _, item := range s.Attention {
		if item.Snoozed {
			suppressed++
			continue
		}
		counts[item.Severity]++
		kinds[item.Kind]++
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"as_of": s.AsOf, "headline": headline, "summary": summary, "fleet": s.Fleet, "attention_counts": counts, "attention_kind_counts": kinds, "suppressed_count": suppressed, "adapter_errors": s.Errors, "adapter_timings": s.Timings, "complete": s.Complete, "registry": opsRegistryView(), "pause_targets_readable": opsPauseTargetsReadable(s.Fleet)}})
}
func GetOpsFleet(c *gin.Context) {
	db, cancel, now := opsRequestDB(c)
	defer cancel()
	s := prepareOpsSnapshot(c, db, now)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"as_of": now, "items": s.Fleet, "members": opsRegistryView(), "adapter_errors": s.Errors, "adapter_timings": s.Timings, "complete": s.Complete}})
}
func ListOpsAttention(c *gin.Context) {
	db, cancel, now := opsRequestDB(c)
	defer cancel()
	s := prepareOpsSnapshot(c, db, now)
	items := opsFilterAttention(s.Attention, c.Query("system"), c.Query("kind"), c.Query("severity"), c.Query("state"), c.Query("include_acked") != "false", c.Query("include_snoozed") == "true")
	limit := opsPageInt(c.Query("limit"), 100, 1, 200)
	offset := opsPageInt(c.Query("offset"), 0, 0, 100000)
	total := len(items)
	start := offset
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}
	p, _ := utils.GetAdminPrincipal(c)
	ids := []uuid.UUID{}
	prefix := "feed_integrity:episode:" + p.TenantID + ":"
	for i := start; i < end; i++ {
		item := &items[i]
		item.IssueHref = opsExactIssueHref(*item, p)
		if suffix, match := strings.CutPrefix(item.Key, prefix); match {
			if id, err := uuid.Parse(suffix); err == nil {
				ids = append(ids, id)
			}
		}
	}
	handoffs, handoffErr := readOpsHandoffs(db, p.TenantID, p.UserID, ids)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"as_of": now, "items": items[start:end], "total": total, "offset": offset, "limit": limit, "has_more": end < total, "handoffs": handoffs, "handoffs_available": handoffErr == nil, "adapter_errors": s.Errors, "complete": s.Complete}})
}
func opsPageInt(value string, fallback, min, max int) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func applyOpsAttentionStateRead(db *gorm.DB, tenant string, items []opsAttentionItem, now time.Time) ([]opsAttentionItem, error) {
	if len(items) == 0 {
		return items, nil
	}
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	var states []models.OpsAttentionState
	if err := db.Where("tenant_id=? AND attention_key IN ? AND updated_at>=?", tenant, keys, now.Add(-opsAttentionRetention)).Find(&states).Error; err != nil {
		return items, err
	}
	byKey := map[string]models.OpsAttentionState{}
	for _, state := range states {
		byKey[state.AttentionKey] = state
	}
	for i := range items {
		if state, ok := byKey[items[i].Key]; ok {
			items[i] = opsApplyAttentionBaseline(items[i], state, now)
		}
	}
	opsSortAttention(items)
	return items, nil
}
func applyOpsAttentionState(db *gorm.DB, tenant string, items []opsAttentionItem, now time.Time) []opsAttentionItem {
	items, _ = applyOpsAttentionStateRead(db, tenant, items, now)
	return items
}
func opsApplyAttentionBaseline(item opsAttentionItem, state models.OpsAttentionState, now time.Time) opsAttentionItem {
	if now.Sub(state.UpdatedAt) > opsAttentionRetention || state.BaselineFingerprint != item.Fingerprint || opsSeverityRank(item.Severity) > opsSeverityRank(state.BaselineSeverity) || item.Count > state.BaselineCount {
		return item
	}
	if state.State == "snoozed" {
		if state.SnoozedUntil == nil || !state.SnoozedUntil.After(now) {
			return item
		}
		item.Snoozed = true
		item.SnoozedUntil = state.SnoozedUntil
	}
	item.State = state.State
	return item
}

type opsAttentionMutationRequest struct {
	Key         string `json:"key"`
	Fingerprint string `json:"fingerprint"`
	Severity    string `json:"severity"`
	Count       int    `json:"count"`
	TTLMinutes  int    `json:"ttl_minutes"`
}

func mutateOpsAttentionState(c *gin.Context, state string) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(401, gin.H{"error": "unauthorized"})
		return
	}
	var req opsAttentionMutationRequest
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.Key) == "" || len(req.Key) > 255 || req.Fingerprint == "" || len(req.Fingerprint) > 255 || req.Severity == "" || req.Count < 1 {
		c.JSON(400, gin.H{"error": "the displayed attention key, fingerprint, severity and count are required; reload the Operations page"})
		return
	}
	if state == "snoozed" && (req.TTLMinutes < 1 || req.TTLMinutes > opsCommandMaxTTLMinutes) {
		c.JSON(400, gin.H{"error": "ttl_minutes must be between 1 and 1440"})
		return
	}
	db, cancel, now := opsRequestDB(c)
	defer cancel()
	s := collectOpsSnapshot(db, now, true)
	var current *opsAttentionItem
	for i := range s.Attention {
		if s.Attention[i].Key == req.Key {
			current = &s.Attention[i]
			break
		}
	}
	if current == nil {
		status := 404
		if !s.Complete {
			status = 503
		}
		c.JSON(status, gin.H{"error": "attention evidence is unavailable or no longer open"})
		return
	}
	// Bind suppression to what the operator saw. A click on a cached row must
	// not acknowledge or snooze an escalation that arrived after that read.
	if current.Fingerprint != req.Fingerprint || current.Severity != req.Severity || current.Count != req.Count {
		c.JSON(http.StatusConflict, gin.H{"error": "attention changed after it was displayed; refresh the inbox before acknowledging or snoozing"})
		return
	}
	row := models.OpsAttentionState{TenantID: principal.TenantID, AttentionKey: req.Key, State: state, BaselineFingerprint: current.Fingerprint, BaselineSeverity: current.Severity, BaselineCount: current.Count, ActorID: principal.UserID, ActorEmail: principal.Email}
	if state == "snoozed" {
		until := now.Add(time.Duration(req.TTLMinutes) * time.Minute)
		row.SnoozedUntil = &until
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if e := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "attention_key"}}, DoUpdates: clause.AssignmentColumns([]string{"state", "snoozed_until", "baseline_fingerprint", "baseline_severity", "baseline_count", "actor_id", "actor_email", "updated_at"})}).Create(&row).Error; e != nil {
			return e
		}
		return tx.Where("updated_at<?", now.Add(-opsAttentionRetention)).Delete(&models.OpsAttentionState{}).Error
	})
	if err != nil {
		c.JSON(503, gin.H{"error": "attention state could not be saved"})
		return
	}
	c.JSON(200, gin.H{"data": row})
}
func AckOpsAttention(c *gin.Context)    { mutateOpsAttentionState(c, "acked") }
func SnoozeOpsAttention(c *gin.Context) { mutateOpsAttentionState(c, "snoozed") }
func ClearOpsAttentionState(c *gin.Context) {
	p, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(401, gin.H{"error": "unauthorized"})
		return
	}
	var req opsAttentionMutationRequest
	if c.ShouldBindJSON(&req) != nil || req.Key == "" {
		c.JSON(400, gin.H{"error": "attention key is required"})
		return
	}
	db, cancel, _ := opsRequestDB(c)
	defer cancel()
	if err := db.Where("tenant_id=? AND attention_key=?", p.TenantID, req.Key).Delete(&models.OpsAttentionState{}).Error; err != nil {
		c.JSON(503, gin.H{"error": "attention state could not be cleared"})
		return
	}
	c.Status(204)
}

func opsPauseTargetsReadable(statuses []opsMemberStatus) bool {
	for _, s := range statuses {
		if s.Pausable && (s.State == "unknown" || s.State == "errored") {
			return false
		}
	}
	return true
}
