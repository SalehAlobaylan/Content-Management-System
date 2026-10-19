package controllers

import (
	"errors"
	"math"
	"net/http"
	"sort"
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

func redundancyRequest(c *gin.Context) (*gorm.DB, utils.AdminPrincipal, bool) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return nil, p, false
	}
	return c.MustGet("db").(*gorm.DB), p, true
}
func redundancyFailure(c *gin.Context, err error) {
	status := http.StatusInternalServerError
	message := "Redundancy evidence is unavailable; no success is assumed"
	if errors.Is(err, gorm.ErrRecordNotFound) {
		status = 404
		message = "Record not found"
	} else if errors.Is(err, errRedundancyBusy) || errors.Is(err, errRedundancyPaused) || errors.Is(err, errRedundancyPolicyChanged) {
		status = 409
		message = err.Error()
	}
	c.JSON(status, gin.H{"error": message})
}
func redundancyID(c *gin.Context) (uuid.UUID, bool) {
	id, e := uuid.Parse(c.Param("id"))
	if e != nil {
		c.JSON(400, gin.H{"error": "invalid record ID"})
		return id, false
	}
	return id, true
}
func redundancyPage(c *gin.Context) (int, int) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "25"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if limit < 1 || limit > 100 {
		limit = 25
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
func GetRedundancyStatus(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	policy, err := redundancyPolicyFor(db, p.TenantID)
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	var proposals, families, library, redundant, bytes, coverage, pending int64
	var oldest struct {
		At *time.Time `gorm:"column:oldest_at"`
	}
	checks := []*gorm.DB{
		db.Model(&models.RedundancyPair{}).Where("tenant_id=? AND tombstoned=false AND verdict IN ?", p.TenantID, []string{models.RedundancyVerdictProbable, models.RedundancyVerdictHighConfidence}).Count(&proposals),
		db.Model(&models.RedundancyFamily{}).Where("tenant_id=? AND status='active'", p.TenantID).Count(&families),
		db.Model(&models.ContentItem{}).Where("tenant_id=? AND type IN ('VIDEO','PODCAST') AND parent_content_item_id IS NULL", p.TenantID).Count(&library),
		db.Table("redundancy_family_members m").Joins("JOIN redundancy_families f ON f.id=m.family_id").Where("m.tenant_id=? AND m.ended_at IS NULL AND m.role='redundant' AND f.status='active'", p.TenantID).Count(&redundant),
		db.Table("redundancy_family_members m").Joins("JOIN redundancy_families f ON f.id=m.family_id JOIN content_items ci ON ci.public_id=m.content_item_id").Where("m.tenant_id=? AND m.ended_at IS NULL AND m.role='redundant' AND f.status='active'", p.TenantID).Select("COALESCE(sum(ci.file_size_bytes),0)").Scan(&bytes),
		db.Model(&models.RedundancyFingerprint{}).Where("tenant_id=? AND jsonb_array_length(signature)=128", p.TenantID).Distinct("content_item_id").Count(&coverage),
		db.Model(&models.MediaCirculationRecommendation{}).Where("tenant_id=? AND status='pending' AND metrics->>'origin'='redundancy' AND verdict='recoverable_delete'", p.TenantID).Count(&pending),
		db.Model(&models.RedundancyPair{}).Where("tenant_id=? AND tombstoned=false AND verdict IN ?", p.TenantID, []string{models.RedundancyVerdictProbable, models.RedundancyVerdictHighConfidence}).Select("min(created_at) AS oldest_at").Scan(&oldest),
	}
	for _, q := range checks {
		if q.Error != nil {
			redundancyFailure(c, q.Error)
			return
		}
	}
	var run *models.RedundancyRun
	var latest models.RedundancyRun
	err = db.Where("tenant_id=?", p.TenantID).Order("started_at DESC").First(&latest).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		redundancyFailure(c, err)
		return
	}
	if err == nil {
		run = &latest
	}
	type familyAttention struct {
		ID                   uuid.UUID `json:"id"`
		CanonicalID          uuid.UUID `json:"canonical_id"`
		CanonicalUnavailable bool      `json:"canonical_unavailable"`
		EngagementConflict   bool      `json:"engagement_conflict"`
	}
	var attention []familyAttention
	attentionQuery := db.Table("redundancy_families f").Select(`f.public_id AS id,f.canonical_content_item_id AS canonical_id,
      (canonical.status<>'READY' OR COALESCE(canonical.playback_url,canonical.media_url,'')='') AS canonical_unavailable,
      EXISTS(SELECT 1 FROM redundancy_family_members m JOIN content_items sibling ON sibling.public_id=m.content_item_id
       WHERE m.family_id=f.id AND m.ended_at IS NULL AND m.role='redundant'
       AND sibling.view_count*3+sibling.like_count*10+sibling.share_count*15 > canonical.view_count*3+canonical.like_count*10+canonical.share_count*15) AS engagement_conflict`).
		Joins("JOIN content_items canonical ON canonical.public_id=f.canonical_content_item_id").Where("f.tenant_id=? AND f.status='active'", p.TenantID)
	err = db.Table("(?) AS family_attention", attentionQuery).Where("canonical_unavailable OR engagement_conflict").Order("id").Limit(25).Scan(&attention).Error
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	rate := 0.
	if library > 0 {
		rate = float64(redundant) / float64(library)
	}
	c.JSON(200, gin.H{"policy": policy, "latest_run": run, "library_items": library, "open_proposals": proposals, "active_families": families, "redundant_members": redundant, "redundant_bytes": bytes, "redundancy_rate": rate, "fingerprint_items": coverage, "oldest_proposal_at": oldest.At, "reclaim_awaiting_approval": pending, "lane_settings": redundancySettings(policy), "evaluator_version": redundancyEvaluator, "attention": attention, "generated_at": time.Now().UTC()})
}
func GetRedundancyPolicy(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	policy, err := redundancyPolicyFor(db, p.TenantID)
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, policy)
}
func UpdateRedundancyPolicy(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	var in struct {
		ExpectedUpdatedAt    *time.Time              `json:"expected_updated_at"`
		Enabled              *bool                   `json:"enabled"`
		CollapseEnabled      *bool                   `json:"collapse_enabled"`
		SweepIntervalMinutes *int                    `json:"sweep_interval_minutes"`
		MaxFrontierItems     *int                    `json:"max_frontier_items"`
		MaxPairsScored       *int                    `json:"max_pairs_scored"`
		ProposalFloor        *float64                `json:"proposal_floor"`
		EmitCirculationRecs  *bool                   `json:"emit_circulation_recs"`
		LaneSettings         *redundancyLaneSettings `json:"lane_settings"`
	}
	if c.ShouldBindJSON(&in) != nil {
		c.JSON(400, gin.H{"error": "invalid settings"})
		return
	}
	updates := map[string]interface{}{}
	if in.Enabled != nil {
		updates["enabled"] = *in.Enabled
	}
	if in.CollapseEnabled != nil {
		updates["collapse_enabled"] = *in.CollapseEnabled
	}
	if in.EmitCirculationRecs != nil {
		updates["emit_circulation_recs"] = *in.EmitCirculationRecs
	}
	for key, v := range map[string]*int{"sweep_interval_minutes": in.SweepIntervalMinutes, "max_frontier_items": in.MaxFrontierItems, "max_pairs_scored": in.MaxPairsScored} {
		if v == nil {
			continue
		}
		min, max := 1, 5000
		if key == "sweep_interval_minutes" {
			min, max = 5, 10080
		}
		if key == "max_pairs_scored" {
			max = 50000
		}
		if *v < min || *v > max {
			c.JSON(400, gin.H{"error": key + " is outside its supported range"})
			return
		}
		updates[key] = *v
	}
	if in.ProposalFloor != nil {
		if math.IsNaN(*in.ProposalFloor) || *in.ProposalFloor < .5 || *in.ProposalFloor > 1 {
			c.JSON(400, gin.H{"error": "proposal_floor must be 0.5..1"})
			return
		}
		updates["proposal_floor"] = *in.ProposalFloor
	}
	if in.LaneSettings != nil {
		s := *in.LaneSettings
		if s.TranscriptConfirm < .8 || s.TranscriptConfirm > 1 || s.EmbeddingConfirm < .93 || s.EmbeddingConfirm > 1 || s.TitleConfirm < .7 || s.TitleConfirm > 1 || s.TitleWeight < 0 || s.TranscriptWeight <= 0 || s.EmbeddingWeight < 0 || s.ThumbnailWeight < 0 || s.ThumbnailWeight > .05 || s.TitleWeight+s.TranscriptWeight+s.EmbeddingWeight+s.ThumbnailWeight > 1.01 {
			c.JSON(400, gin.H{"error": "invalid evidence thresholds or weights; confirmation safeguards cannot be weakened"})
			return
		}
		updates["lane_settings"] = redundancyJSON(s)
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if e := lockRedundancyFamilies(tx, p.TenantID); e != nil {
			return e
		}
		current, err := redundancyPolicyFor(tx, p.TenantID)
		if err != nil {
			return err
		}
		if in.ExpectedUpdatedAt != nil && !current.UpdatedAt.Equal(*in.ExpectedUpdatedAt) {
			return errRedundancyPolicyChanged
		}
		if err := tx.Model(&models.RedundancyPolicy{}).Where("tenant_id=?", p.TenantID).Updates(updates).Error; err != nil {
			return err
		}
		if in.EmitCirculationRecs != nil && !*in.EmitCirculationRecs {
			if err := tx.Model(&models.MediaCirculationRecommendation{}).Where("tenant_id=? AND status='pending' AND metrics->>'origin'='redundancy'", p.TenantID).Update("status", models.MediaCirculationRecStatusSuperseded).Error; err != nil {
				return err
			}
		}
		return redundancyAction(tx, p.TenantID, "policy.update", p.Email, "succeeded", nil, nil, nil, "Reviewed policy change", updates)
	})
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	GetRedundancyPolicy(c)
}
func PauseRedundancy(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	var in struct {
		Minutes int `json:"minutes"`
	}
	if c.ShouldBindJSON(&in) != nil || in.Minutes < 0 || in.Minutes > 10080 {
		c.JSON(400, gin.H{"error": "minutes must be 0..10080"})
		return
	}
	var until *time.Time
	if in.Minutes > 0 {
		t := time.Now().UTC().Add(time.Duration(in.Minutes) * time.Minute)
		until = &t
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if _, e := redundancyPolicyFor(tx, p.TenantID); e != nil {
			return e
		}
		if e := tx.Model(&models.RedundancyPolicy{}).Where("tenant_id=?", p.TenantID).Update("paused_until", until).Error; e != nil {
			return e
		}
		return redundancyAction(tx, p.TenantID, "pause", p.Email, "succeeded", nil, nil, nil, "Schedule pause updated", gin.H{"paused_until": until})
	})
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"paused_until": until})
}
func RunRedundancyNow(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	var in struct {
		Scope string `json:"scope"`
	}
	if c.ShouldBindJSON(&in) != nil || in.Scope != "" && in.Scope != "full" && in.Scope != "frontier" {
		c.JSON(400, gin.H{"error": "scope must be frontier or full"})
		return
	}
	if in.Scope == "" {
		in.Scope = "frontier"
	}
	run, err := runRedundancyTenant(db, p.TenantID, "manual", p.Email, in.Scope)
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"data": run})
}
func ListRedundancyPairs(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	q := db.Model(&models.RedundancyPair{}).Joins("JOIN content_items ra ON ra.public_id=redundancy_pairs.item_a_id JOIN content_items rb ON rb.public_id=redundancy_pairs.item_b_id").Where("redundancy_pairs.tenant_id=?", p.TenantID)
	switch v := c.DefaultQuery("verdict", "review"); v {
	case "review":
		q = q.Where("tombstoned=false AND verdict IN ?", []string{models.RedundancyVerdictProbable, models.RedundancyVerdictHighConfidence})
	case "all":
	default:
		q = q.Where("verdict=?", v)
	}
	if item := c.Query("item"); item != "" {
		q = q.Where("item_a_id=? OR item_b_id=?", item, item)
	}
	if run := c.Query("run"); run != "" {
		q = q.Where("EXISTS (SELECT 1 FROM redundancy_pair_evaluations e JOIN redundancy_runs r ON r.id=e.run_id WHERE e.pair_id=redundancy_pairs.id AND r.public_id::text=?)", run)
	}
	if min, err := strconv.ParseFloat(c.Query("min_confidence"), 64); err == nil {
		q = q.Where("confidence>=?", min)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	limit, offset := redundancyPage(c)
	var rows []models.RedundancyPair
	if err := q.Select("redundancy_pairs.*").Order("confidence * GREATEST(ra.file_size_bytes,rb.file_size_bytes,1) DESC, redundancy_pairs.id ASC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"data": rows, "total": total, "offset": offset, "limit": limit})
}
func GetRedundancyPair(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	id, ok := redundancyID(c)
	if !ok {
		return
	}
	var pair models.RedundancyPair
	if err := db.Where("tenant_id=? AND public_id=?", p.TenantID, id).First(&pair).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	var evaluations []models.RedundancyPairEvaluation
	var items []models.ContentItem
	if err := db.Where("pair_id=?", pair.ID).Order("id DESC").Limit(20).Find(&evaluations).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	if err := db.Where("tenant_id=? AND public_id IN ?", p.TenantID, []uuid.UUID{pair.ItemAID, pair.ItemBID}).Order("public_id").Find(&items).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	bodies := map[uuid.UUID]string{}
	for _, item := range items {
		transcript, err := activeRedundancyTranscript(db, p.TenantID, item.PublicID)
		if err != nil && err != gorm.ErrRecordNotFound {
			redundancyFailure(c, err)
			return
		}
		bodies[item.PublicID] = redundancyTranscriptBody(transcript)
	}
	snippets := map[uuid.UUID]string{}
	match := ""
	x, y := redundancyShingles(bodies[pair.ItemAID]), redundancyShingles(bodies[pair.ItemBID])
	keys := []string{}
	for shingle := range x {
		if y[shingle] {
			keys = append(keys, shingle)
		}
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		match = keys[0]
	}
	for item, body := range bodies {
		r := []rune(body)
		start := 0
		if match != "" {
			index := strings.Index(body, match)
			if index >= 0 {
				start = len([]rune(body[:index])) - 100
				if start < 0 {
					start = 0
				}
			}
		}
		end := start + 500
		if end > len(r) {
			end = len(r)
		}
		snippets[item] = string(r[start:end])
	}
	c.JSON(200, gin.H{"data": gin.H{"pair": pair, "evaluations": evaluations, "items": items, "transcript_snippets": snippets, "matching_shingle": match}})
}
func ConfirmRedundancyPair(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	id, ok := redundancyID(c)
	if !ok {
		return
	}
	var in struct {
		EvaluationID uint `json:"evaluation_id"`
	}
	if c.ShouldBindJSON(&in) != nil || in.EvaluationID == 0 {
		c.JSON(400, gin.H{"error": "review the current evidence before confirming"})
		return
	}
	err := confirmRedundancyPair(db, p.TenantID, p.Email, id, in.EvaluationID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			redundancyFailure(c, err)
		} else {
			c.JSON(409, gin.H{"error": err.Error()})
		}
		return
	}
	policy, err := redundancyPolicyFor(db, p.TenantID)
	if err == nil && policy.EmitCirculationRecs {
		err = reconcileRedundancyRecommendations(db, p.TenantID, p.Email, nil)
	}
	if err != nil {
		c.JSON(200, gin.H{"success": true, "warning": "Identity confirmed; Circulation handoff is pending retry"})
		return
	}
	c.JSON(200, gin.H{"success": true})
}
func redundancyReason(c *gin.Context) (string, bool) {
	var in struct {
		Reason string `json:"reason"`
	}
	if c.ShouldBindJSON(&in) != nil || len(strings.TrimSpace(in.Reason)) < 3 || len(in.Reason) > 2000 {
		c.JSON(400, gin.H{"error": "provide a reason between 3 and 2000 characters"})
		return "", false
	}
	return strings.TrimSpace(in.Reason), true
}
func RejectRedundancyPair(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	id, ok := redundancyID(c)
	if !ok {
		return
	}
	reason, ok := redundancyReason(c)
	if !ok {
		return
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		var pair models.RedundancyPair
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", p.TenantID, id).First(&pair).Error; e != nil {
			return e
		}
		if pair.Verdict == models.RedundancyVerdictConfirmed {
			return errors.New("dissolve the family to correct an existing confirmation")
		}
		if e := tx.Model(&pair).Updates(map[string]interface{}{"verdict": models.RedundancyVerdictRejected, "tombstoned": true, "reviewed_by": p.Email, "reviewed_at": time.Now().UTC(), "reject_reason": reason}).Error; e != nil {
			return e
		}
		return redundancyAction(tx, p.TenantID, "pair.reject", p.Email, "succeeded", nil, &pair.ID, nil, reason, nil)
	})
	if err != nil {
		c.JSON(409, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"success": true})
}
func ListRedundancyFamilies(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	q := db.Model(&models.RedundancyFamily{}).Where("tenant_id=?", p.TenantID)
	if c.DefaultQuery("status", "active") != "all" {
		q = q.Where("status=?", c.DefaultQuery("status", "active"))
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	limit, offset := redundancyPage(c)
	var rows []models.RedundancyFamily
	if err := q.Order("last_confirmed_at DESC,id DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"data": rows, "total": total, "offset": offset, "limit": limit})
}
func GetRedundancyFamily(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	id, ok := redundancyID(c)
	if !ok {
		return
	}
	var f models.RedundancyFamily
	if err := db.Where("tenant_id=? AND public_id=?", p.TenantID, id).First(&f).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	var members []models.RedundancyFamilyMember
	if err := db.Where("family_id=? AND ended_at IS NULL", f.ID).Order("role,id").Find(&members).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	ids := []uuid.UUID{}
	for _, m := range members {
		ids = append(ids, m.ContentItemID)
	}
	var items []models.ContentItem
	if err := db.Where("tenant_id=? AND public_id IN ?", p.TenantID, ids).Find(&items).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	var recs []models.MediaCirculationRecommendation
	if err := db.Where("tenant_id=? AND metrics->>'origin'='redundancy' AND metrics->>'family_id'=?", p.TenantID, strconv.FormatUint(uint64(f.ID), 10)).Order("id DESC").Limit(25).Find(&recs).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	policy, err := redundancyPolicyFor(db, p.TenantID)
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"data": gin.H{"family": f, "members": members, "items": items, "recommendations": recs, "collapse_active": policy.Enabled && policy.CollapseEnabled}})
}
func SetRedundancyCanonical(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	id, ok := redundancyID(c)
	if !ok {
		return
	}
	var in struct {
		ContentItemID uuid.UUID `json:"content_item_id"`
		Locked        *bool     `json:"locked"`
	}
	if c.ShouldBindJSON(&in) != nil || in.ContentItemID == uuid.Nil {
		c.JSON(400, gin.H{"error": "select an active family member"})
		return
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if e := lockRedundancyFamilies(tx, p.TenantID); e != nil {
			return e
		}
		var f models.RedundancyFamily
		if e := tx.Where("tenant_id=? AND public_id=? AND status='active'", p.TenantID, id).First(&f).Error; e != nil {
			return e
		}
		var m models.RedundancyFamilyMember
		if e := tx.Where("family_id=? AND ended_at IS NULL AND content_item_id=?", f.ID, in.ContentItemID).First(&m).Error; e != nil {
			return e
		}
		var item models.ContentItem
		if e := redundancyCanonicalLibrary(tx, p.TenantID).Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=?", in.ContentItemID).First(&item).Error; e != nil {
			return e
		}
		if e := tx.Model(&models.RedundancyFamilyMember{}).Where("family_id=? AND ended_at IS NULL", f.ID).Update("role", "redundant").Error; e != nil {
			return e
		}
		if e := tx.Model(&m).Update("role", "canonical").Error; e != nil {
			return e
		}
		actor := p.Email
		if in.Locked != nil && !*in.Locked {
			actor = ""
		}
		if e := tx.Model(&f).Updates(map[string]interface{}{"canonical_content_item_id": in.ContentItemID, "canonical_locked_by": actor, "canonical_reasons": redundancyJSON([]string{"Selected by human review"})}).Error; e != nil {
			return e
		}
		if e := invalidateRedundancyRecommendations(tx, p.TenantID, []uint{f.ID}); e != nil {
			return e
		}
		return redundancyAction(tx, p.TenantID, "family.canonical", p.Email, "succeeded", nil, nil, &f.ID, "Canonical changed", in)
	})
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"success": true})
}
func DissolveRedundancyFamily(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	id, ok := redundancyID(c)
	if !ok {
		return
	}
	reason, ok := redundancyReason(c)
	if !ok {
		return
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if e := lockRedundancyFamilies(tx, p.TenantID); e != nil {
			return e
		}
		var f models.RedundancyFamily
		if e := tx.Where("tenant_id=? AND public_id=? AND status='active'", p.TenantID, id).First(&f).Error; e != nil {
			return e
		}
		var memberIDs []uuid.UUID
		if e := tx.Model(&models.RedundancyFamilyMember{}).Where("family_id=? AND ended_at IS NULL", f.ID).Pluck("content_item_id", &memberIDs).Error; e != nil {
			return e
		}
		now := time.Now().UTC()
		if e := tx.Model(&models.RedundancyFamilyMember{}).Where("family_id=? AND ended_at IS NULL", f.ID).Update("ended_at", now).Error; e != nil {
			return e
		}
		if e := tx.Model(&f).Updates(map[string]interface{}{"status": "dissolved", "dissolved_at": now, "dissolved_by": p.Email, "dissolve_reason": reason}).Error; e != nil {
			return e
		}
		if e := tx.Model(&models.RedundancyPair{}).Where("tenant_id=? AND (family_id=? OR (item_a_id IN ? AND item_b_id IN ?))", p.TenantID, f.ID, memberIDs, memberIDs).Updates(map[string]interface{}{"tombstoned": true, "verdict": models.RedundancyVerdictRejected, "reject_reason": reason, "reviewed_by": p.Email, "reviewed_at": now}).Error; e != nil {
			return e
		}
		if e := invalidateRedundancyRecommendations(tx, p.TenantID, []uint{f.ID}); e != nil {
			return e
		}
		return redundancyAction(tx, p.TenantID, "family.dissolve", p.Email, "succeeded", nil, nil, &f.ID, reason, nil)
	})
	if err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"success": true})
}
func ListRedundancyRuns(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	limit, offset := redundancyPage(c)
	var rows []models.RedundancyRun
	if err := db.Where("tenant_id=?", p.TenantID).Order("id DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"data": rows})
}
func GetRedundancyRun(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	id, ok := redundancyID(c)
	if !ok {
		return
	}
	var run models.RedundancyRun
	if err := db.Where("tenant_id=? AND public_id=?", p.TenantID, id).First(&run).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	limit, offset := redundancyPage(c)
	var rows []models.RedundancyPairEvaluation
	var total int64
	if err := db.Model(&models.RedundancyPairEvaluation{}).Where("run_id=?", run.ID).Count(&total).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	if err := db.Where("run_id=?", run.ID).Order("id").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"data": gin.H{"run": run, "evaluations": rows, "total": total, "offset": offset, "limit": limit}})
}
func ListRedundancyActions(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	q := db.Model(&models.RedundancyAction{}).Where("redundancy_actions.tenant_id=?", p.TenantID)
	for _, filter := range []struct{ Key, Table, Column string }{{"family", "redundancy_families", "family_id"}, {"pair", "redundancy_pairs", "pair_id"}, {"run", "redundancy_runs", "run_id"}} {
		if value := c.Query(filter.Key); value != "" {
			q = q.Where("EXISTS (SELECT 1 FROM "+filter.Table+" t WHERE t.id=redundancy_actions."+filter.Column+" AND t.public_id::text=?)", value)
		}
	}
	if outcome := c.Query("outcome"); outcome != "" {
		q = q.Where("outcome=?", outcome)
	}
	limit, offset := redundancyPage(c)
	var total int64
	if err := q.Count(&total).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	var rows []models.RedundancyAction
	if err := q.Order("id DESC").Limit(limit).Offset(offset).Find(&rows).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"data": rows, "total": total, "offset": offset, "limit": limit})
}
func GetRedundancyAction(c *gin.Context) {
	db, p, ok := redundancyRequest(c)
	if !ok {
		return
	}
	id, ok := redundancyID(c)
	if !ok {
		return
	}
	var row models.RedundancyAction
	if err := db.Where("tenant_id=? AND public_id=?", p.TenantID, id).First(&row).Error; err != nil {
		redundancyFailure(c, err)
		return
	}
	c.JSON(200, gin.H{"data": row})
}

// Precheck is evidence only. CMS materialization remains authoritative for deciding
// whether a durable identity needs new stages, changed-input work, or no work.
func InternalRedundancyPrecheck(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var in struct {
		TenantID        string     `json:"tenant_id"`
		ContentSourceID *uuid.UUID `json:"content_source_id"`
		Candidates      []struct {
			Title       string `json:"title"`
			DurationSec *int   `json:"duration_sec"`
			SourceURL   string `json:"source_url"`
		} `json:"candidates"`
	}
	if c.ShouldBindJSON(&in) != nil || strings.TrimSpace(in.TenantID) == "" || len(in.Candidates) < 1 || len(in.Candidates) > 100 {
		c.JSON(400, gin.H{"error": "tenant_id and 1..100 candidates are required"})
		return
	}
	if in.ContentSourceID != nil {
		var n int64
		if err := db.Model(&models.ContentSource{}).Where("tenant_id=? AND public_id=?", in.TenantID, *in.ContentSourceID).Count(&n).Error; err != nil {
			redundancyFailure(c, err)
			return
		}
		if n != 1 {
			c.JSON(403, gin.H{"error": "source does not belong to this tenant"})
			return
		}
	}
	out := []gin.H{}
	for _, candidate := range in.Candidates {
		r := gin.H{"verdict": "clear", "confidence": 0, "reasons": []string{}}
		var exact models.ContentItem
		err := db.Where("tenant_id=? AND type IN ('VIDEO','PODCAST') AND original_url=?", in.TenantID, candidate.SourceURL).First(&exact).Error
		if candidate.SourceURL != "" && err == nil {
			r = gin.H{"verdict": "exact_identity", "existing_item_id": exact.PublicID, "confidence": 1, "reasons": []string{"Source URL exists; CMS lifecycle reconciliation determines required work"}}
		} else {
			if err != nil && err != gorm.ErrRecordNotFound {
				redundancyFailure(c, err)
				return
			}
			if candidate.DurationSec != nil && *candidate.DurationSec > 0 && candidate.Title != "" {
				tolerance := redundancyMaxInt(15, int(float64(*candidate.DurationSec)*.02))
				var matches []models.ContentItem
				err = redundancyLibrary(db, in.TenantID).Where("duration_sec BETWEEN ? AND ? AND redundancy_normalized_title(title) % redundancy_normalized_title(?) AND similarity(redundancy_normalized_title(title),redundancy_normalized_title(?))>=.55", *candidate.DurationSec-tolerance, *candidate.DurationSec+tolerance, candidate.Title, candidate.Title).Limit(8).Find(&matches).Error
				if err != nil {
					redundancyFailure(c, err)
					return
				}
				best := 0.
				for _, item := range matches {
					score := titleSimilarity(candidate.Title, contentTitle(item))
					if score >= .55 && score > best {
						best = score
						r = gin.H{"verdict": "likely_duplicate", "existing_item_id": item.PublicID, "confidence": score, "reasons": []string{"Similar title and duration; advisory only"}}
					}
				}
			}
		}
		out = append(out, r)
		if err := redundancyAction(db, in.TenantID, "precheck", "aggregation-service", "succeeded", nil, nil, nil, r["verdict"].(string), gin.H{"source_id": in.ContentSourceID, "source_url": candidate.SourceURL, "result": r}); err != nil {
			redundancyFailure(c, err)
			return
		}
	}
	c.JSON(200, gin.H{"candidates": out})
}
