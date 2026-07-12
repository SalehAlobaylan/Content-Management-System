package controllers

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const redundancyTenant = "default"

var redundancyRunMu sync.Mutex
var redundancyRunning bool

func tryStartRedundancyRun() bool {
	redundancyRunMu.Lock()
	defer redundancyRunMu.Unlock()
	if redundancyRunning {
		return false
	}
	redundancyRunning = true
	return true
}
func finishRedundancyRun() {
	redundancyRunMu.Lock()
	redundancyRunning = false
	redundancyRunMu.Unlock()
}

func redundancyPolicy(db *gorm.DB) models.RedundancyPolicy {
	var p models.RedundancyPolicy
	if db.Where("tenant_id = ?", redundancyTenant).First(&p).Error != nil {
		p = models.DefaultRedundancyPolicy(redundancyTenant)
		_ = db.Create(&p).Error
	}
	return p
}
func redundancyActor(c *gin.Context) string {
	if v, ok := c.Get("admin_email"); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return "admin"
}
func redundancyAction(db *gorm.DB, kind, actor, outcome string, pair *uint, family *uint, reason string) {
	_ = db.Create(&models.RedundancyAction{TenantID: redundancyTenant, PairID: pair, FamilyID: family, ActionKind: kind, Actor: actor, Outcome: outcome, Reason: reason}).Error
}

func GetRedundancyStatus(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	p := redundancyPolicy(db)
	var proposals, families int64
	db.Model(&models.RedundancyPair{}).Where("tenant_id = ? AND verdict IN ? AND tombstoned = false", redundancyTenant, []string{models.RedundancyVerdictProbable, models.RedundancyVerdictHighConfidence}).Count(&proposals)
	db.Model(&models.RedundancyFamily{}).Where("tenant_id = ? AND status = 'active'", redundancyTenant).Count(&families)
	var run models.RedundancyRun
	db.Where("tenant_id = ?", redundancyTenant).Order("started_at desc").First(&run)
	c.JSON(http.StatusOK, gin.H{"policy": p, "open_proposals": proposals, "active_families": families, "latest_run": run})
}
func GetRedundancyPolicy(c *gin.Context) {
	c.JSON(http.StatusOK, redundancyPolicy(c.MustGet("db").(*gorm.DB)))
}
func UpdateRedundancyPolicy(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	p := redundancyPolicy(db)
	var in struct {
		Enabled              *bool    `json:"enabled"`
		CollapseEnabled      *bool    `json:"collapse_enabled"`
		SweepIntervalMinutes *int     `json:"sweep_interval_minutes"`
		MaxFrontierItems     *int     `json:"max_frontier_items"`
		MaxPairsScored       *int     `json:"max_pairs_scored"`
		ProposalFloor        *float64 `json:"proposal_floor"`
		EmitCirculationRecs  *bool    `json:"emit_circulation_recs"`
	}
	if c.ShouldBindJSON(&in) != nil {
		c.JSON(400, gin.H{"error": "invalid policy"})
		return
	}
	// Bounds guard the tuning knobs: a 0 interval would make the heartbeat fire a
	// full scan every tick, and a 0 cap would silently disable detection.
	if in.SweepIntervalMinutes != nil && (*in.SweepIntervalMinutes < 5 || *in.SweepIntervalMinutes > 10080) {
		c.JSON(400, gin.H{"error": "sweep_interval_minutes must be 5..10080"})
		return
	}
	if in.MaxFrontierItems != nil && (*in.MaxFrontierItems < 1 || *in.MaxFrontierItems > 5000) {
		c.JSON(400, gin.H{"error": "max_frontier_items must be 1..5000"})
		return
	}
	if in.MaxPairsScored != nil && (*in.MaxPairsScored < 1 || *in.MaxPairsScored > 50000) {
		c.JSON(400, gin.H{"error": "max_pairs_scored must be 1..50000"})
		return
	}
	if in.ProposalFloor != nil && (*in.ProposalFloor < 0 || *in.ProposalFloor > 1) {
		c.JSON(400, gin.H{"error": "proposal_floor must be 0..1"})
		return
	}
	if in.Enabled != nil {
		p.Enabled = *in.Enabled
	}
	if in.CollapseEnabled != nil {
		p.CollapseEnabled = *in.CollapseEnabled
	}
	if in.SweepIntervalMinutes != nil {
		p.SweepIntervalMinutes = *in.SweepIntervalMinutes
	}
	if in.MaxFrontierItems != nil {
		p.MaxFrontierItems = *in.MaxFrontierItems
	}
	if in.MaxPairsScored != nil {
		p.MaxPairsScored = *in.MaxPairsScored
	}
	if in.ProposalFloor != nil {
		p.ProposalFloor = *in.ProposalFloor
	}
	if in.EmitCirculationRecs != nil {
		p.EmitCirculationRecs = *in.EmitCirculationRecs
	}
	db.Save(&p)
	redundancyAction(db, "policy.update", redundancyActor(c), "succeeded", nil, nil, "")
	c.JSON(200, p)
}

func PauseRedundancy(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	p := redundancyPolicy(db)
	var in struct {
		Minutes int `json:"minutes"`
	}
	if c.ShouldBindJSON(&in) != nil || in.Minutes < 0 || in.Minutes > 10080 {
		c.JSON(400, gin.H{"error": "minutes must be 0..10080"})
		return
	}
	if in.Minutes == 0 {
		p.PausedUntil = nil
	} else {
		t := time.Now().Add(time.Duration(in.Minutes) * time.Minute)
		p.PausedUntil = &t
	}
	db.Save(&p)
	redundancyAction(db, "pause", redundancyActor(c), "succeeded", nil, nil, "")
	c.JSON(200, gin.H{"paused_until": p.PausedUntil})
}

// RunRedundancyNow produces bounded deterministic proposals. It deliberately
// never activates families: only the confirm endpoint changes feed inventory.
func RunRedundancyNow(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var in struct {
		Scope string `json:"scope"`
	}
	_ = c.ShouldBindJSON(&in)
	if in.Scope != "full" {
		in.Scope = "frontier"
	}
	run, err := runRedundancyScanScope(db, "manual", redundancyActor(c), in.Scope)
	if err != nil {
		c.JSON(409, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"data": run})
}

func runRedundancyScan(db *gorm.DB, trigger, actor string) (models.RedundancyRun, error) {
	return runRedundancyScanScope(db, trigger, actor, "frontier")
}
func runRedundancyScanScope(db *gorm.DB, trigger, actor, scope string) (models.RedundancyRun, error) {
	if !tryStartRedundancyRun() {
		return models.RedundancyRun{}, fmt.Errorf("redundancy hygiene run already running")
	}
	defer finishRedundancyRun()
	p := redundancyPolicy(db)
	if (trigger == "scheduled" && !p.Enabled) || (p.PausedUntil != nil && p.PausedUntil.After(time.Now())) {
		return models.RedundancyRun{}, fmt.Errorf("redundancy hygiene is paused or disabled")
	}
	run := models.RedundancyRun{TenantID: redundancyTenant, Trigger: trigger, Status: "running", StartedAt: time.Now()}
	db.Create(&run)
	var items []models.ContentItem
	q := db.Where("tenant_id = ? AND status = ? AND type IN ? AND parent_content_item_id IS NULL AND duration_sec IS NOT NULL", redundancyTenant, models.ContentStatusReady, []string{"VIDEO", "PODCAST"})
	if scope != "full" && p.LastSweptAt != nil {
		q = q.Where("created_at >= ?", *p.LastSweptAt)
	}
	q.Order("created_at desc").Limit(p.MaxFrontierItems).Find(&items)
	proposed := 0
	for i := 0; i < len(items) && proposed < p.MaxPairsScored; i++ {
		for j := i + 1; j < len(items) && proposed < p.MaxPairsScored; j++ {
			a, b := items[i], items[j]
			if a.DurationSec == nil || b.DurationSec == nil {
				continue
			}
			max := 15
			if int(float64(*a.DurationSec)*.02) > max {
				max = int(float64(*a.DurationSec) * .02)
			}
			if redundancyAbs(*a.DurationSec-*b.DurationSec) > max {
				continue
			}
			titleScore := titleSimilarity(contentTitle(a), contentTitle(b))
			transcriptScore := transcriptSimilarity(db, a.PublicID, b.PublicID)
			embeddingScore, embeddingAvailable := textEmbeddingSimilarity(db, a, b)
			thumbnailScore, thumbnailAvailable := imageEmbeddingSimilarity(db, a, b)
			if titleScore < .55 && transcriptScore < .70 && (!embeddingAvailable || embeddingScore < .90) {
				continue
			}
			score := titleScore
			if transcriptScore > score {
				score = transcriptScore
			}
			if embeddingAvailable && embeddingScore > score {
				score = embeddingScore
			}
			if thumbnailAvailable && thumbnailScore > score {
				score = thumbnailScore
			}
			// The tunable proposal floor (cockpit-editable) is the final recording
			// gate — this is the lever §16.1's propose-only calibration walks.
			if score < p.ProposalFloor {
				continue
			}
			aid, bid := a.PublicID, b.PublicID
			if strings.Compare(aid.String(), bid.String()) > 0 {
				aid, bid = bid, aid
			}
			verdict := models.RedundancyVerdictProbable
			if transcriptScore >= .80 || (embeddingAvailable && embeddingScore >= .93 && titleScore >= .70) {
				verdict = models.RedundancyVerdictHighConfidence
			}
			pair := models.RedundancyPair{TenantID: redundancyTenant, ItemAID: aid, ItemBID: bid, Confidence: score, Verdict: verdict}
			db.Where(models.RedundancyPair{TenantID: redundancyTenant, ItemAID: aid, ItemBID: bid}).Assign(map[string]interface{}{"confidence": score, "verdict": verdict}).FirstOrCreate(&pair)
			lanes, _ := json.Marshal(map[string]interface{}{"duration": 1, "title": titleScore, "transcript": transcriptScore, "embedding": map[string]interface{}{"score": embeddingScore, "available": embeddingAvailable}, "thumbnail": map[string]interface{}{"score": thumbnailScore, "available": thumbnailAvailable}})
			e := models.RedundancyPairEvaluation{PairID: pair.ID, RunID: &run.ID, EvaluatorVersion: "v1", LaneScores: datatypes.JSON(lanes), Confidence: score, MachineVerdict: verdict}
			db.Create(&e)
			db.Model(&pair).Update("latest_evaluation_id", e.ID)
			proposed++
		}
	}
	now := time.Now()
	counts, _ := json.Marshal(map[string]int{"frontier_items": len(items), "proposals": proposed})
	run.Status = "completed"
	run.Counts = datatypes.JSON(counts)
	run.FinishedAt = &now
	run.Summary = "deterministic duration, title, and transcript-body proposal pass"
	db.Save(&run)
	p.LastSweptAt = &now
	db.Save(&p)
	redundancyAction(db, "run", actor, "succeeded", nil, nil, run.Summary)
	return run, nil
}

func StartRedundancyHygieneHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			runRedundancyDue(db)
		}
	}()
}
func runRedundancyDue(db *gorm.DB) {
	var policies []models.RedundancyPolicy
	if db.Where("enabled = ?", true).Find(&policies).Error != nil {
		return
	}
	now := time.Now()
	for _, p := range policies {
		if p.PausedUntil != nil && p.PausedUntil.After(now) {
			continue
		}
		if p.LastSweptAt != nil && now.Sub(*p.LastSweptAt) < time.Duration(p.SweepIntervalMinutes)*time.Minute {
			continue
		}
		_, _ = runRedundancyScan(db, "scheduled", "automation")
	}
	pruneRedundancyRetention(db, now)
}
func pruneRedundancyRetention(db *gorm.DB, now time.Time) {
	cutoff := now.AddDate(0, 0, -30)
	_ = db.Where("tenant_id = ? AND verdict = ? AND tombstoned = false AND family_id IS NULL AND updated_at < ?", redundancyTenant, models.RedundancyVerdictClear, cutoff).Delete(&models.RedundancyPair{}).Error
	_ = db.Where("tenant_id = ? AND created_at < ?", redundancyTenant, cutoff).Delete(&models.RedundancyFingerprint{}).Error
}

func textEmbeddingSimilarity(db *gorm.DB, a, b models.ContentItem) (float64, bool) {
	space := currentTextSpaceIDForSimilarity()
	if space == "" || a.Embedding == nil || b.Embedding == nil || a.EmbeddingSpaceID == nil || b.EmbeddingSpaceID == nil || *a.EmbeddingSpaceID != space || *b.EmbeddingSpaceID != space {
		return 0, false
	}
	var score float64
	if err := db.Raw("SELECT 1 - (?::vector <=> ?::vector)", a.Embedding, b.Embedding).Scan(&score).Error; err != nil {
		return 0, false
	}
	return score, true
}

func imageEmbeddingSimilarity(db *gorm.DB, a, b models.ContentItem) (float64, bool) {
	expected := currentExpectedSpace(EmbeddingSpaceImage)
	if !expected.Fresh(descriptorCacheTTL) || a.ImageEmbedding == nil || b.ImageEmbedding == nil || a.ImageEmbeddingSpaceID == nil || b.ImageEmbeddingSpaceID == nil || *a.ImageEmbeddingSpaceID != expected.SpaceID || *b.ImageEmbeddingSpaceID != expected.SpaceID {
		return 0, false
	}
	var score float64
	if err := db.Raw("SELECT 1 - (?::vector <=> ?::vector)", a.ImageEmbedding, b.ImageEmbedding).Scan(&score).Error; err != nil {
		return 0, false
	}
	return score, true
}
func titleSimilarity(a, b string) float64 {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	if a == "" || b == "" {
		return 0
	}
	if a == b {
		return 1
	}
	aw := strings.Fields(a)
	bw := strings.Fields(b)
	m := map[string]bool{}
	for _, w := range aw {
		m[w] = true
	}
	hit := 0
	for _, w := range bw {
		if m[w] {
			hit++
		}
	}
	d := len(aw) + len(bw) - hit
	if d == 0 {
		return 0
	}
	return float64(hit) / float64(d)
}

var redundancyWord = regexp.MustCompile(`[\p{L}\p{N}]+`)

func transcriptSimilarity(db *gorm.DB, a, b uuid.UUID) float64 {
	// content_item_id is NOT unique on transcripts (re-runs: youtube_auto caption
	// then a later Deepgram STT), so we must pick the latest row PER item — never
	// an IN(a,b) fetch that could return two transcripts of the SAME item and
	// score them against each other as a false ~1.0 duplicate.
	ta, oka := latestTranscript(db, a)
	tb, okb := latestTranscript(db, b)
	if !oka || !okb {
		return 0
	}
	x, y := cachedTranscriptBody(db, ta), cachedTranscriptBody(db, tb)
	if x == "" || y == "" {
		return 0
	}
	return shingleJaccard(x, y)
}

func latestTranscript(db *gorm.DB, itemID uuid.UUID) (models.Transcript, bool) {
	var t models.Transcript
	if db.Where("content_item_id = ?", itemID).Order("id desc").First(&t).Error != nil {
		return models.Transcript{}, false
	}
	return t, true
}

func cachedTranscriptBody(db *gorm.DB, transcript models.Transcript) string {
	body := transcriptBody(transcript.FullText)
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(transcript.FullText)))
	bodyHash := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	var fp models.RedundancyFingerprint
	if db.Where("tenant_id = ? AND content_item_id = ? AND transcript_checksum = ?", redundancyTenant, transcript.ContentItemID, checksum).First(&fp).Error != nil {
		_ = db.Create(&models.RedundancyFingerprint{TenantID: redundancyTenant, ContentItemID: transcript.ContentItemID, TranscriptChecksum: checksum, BodyHash: bodyHash, ShingleCount: redundancyMaxInt(0, len(strings.Fields(body))-7)}).Error
	}
	return body
}
func transcriptBody(text string) string {
	words := redundancyWord.FindAllString(strings.ToLower(text), -1)
	if len(words) < 20 {
		return strings.Join(words, " ")
	}
	trim := len(words) * 8 / 100
	if trim < 1 {
		trim = 1
	}
	return strings.Join(words[trim:len(words)-trim], " ")
}
func shingleJaccard(a, b string) float64 {
	wa, wb := strings.Fields(a), strings.Fields(b)
	if len(wa) < 8 || len(wb) < 8 {
		return 0
	}
	set := func(w []string) map[string]struct{} {
		m := map[string]struct{}{}
		for i := 0; i+8 <= len(w); i++ {
			m[strings.Join(w[i:i+8], " ")] = struct{}{}
		}
		return m
	}
	sa, sb := set(wa), set(wb)
	hit := 0
	for s := range sa {
		if _, ok := sb[s]; ok {
			hit++
		}
	}
	union := len(sa) + len(sb) - hit
	if union == 0 {
		return 0
	}
	return float64(hit) / float64(union)
}
func contentTitle(item models.ContentItem) string {
	if item.Title == nil {
		return ""
	}
	return *item.Title
}
func redundancyAbs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
func selectRedundancyCanonical(db *gorm.DB, a, b uuid.UUID) (uuid.UUID, []byte) {
	var items []models.ContentItem
	db.Where("public_id IN ?", []uuid.UUID{a, b}).Find(&items)
	if len(items) < 2 {
		raw, _ := json.Marshal([]string{"fallback: first item"})
		return a, raw
	}
	score := func(x models.ContentItem) int64 {
		v := int64(x.ViewCount*3 + x.LikeCount*10 + x.ShareCount*15)
		if x.PlaybackURL != nil {
			v += 100
		}
		if x.TranscriptID != nil {
			v += 25
		}
		return v
	}
	winner := items[0]
	if score(items[1]) > score(winner) || (score(items[1]) == score(winner) && items[1].CreatedAt.Before(winner.CreatedAt)) {
		winner = items[1]
	}
	raw, _ := json.Marshal([]string{"engagement gravity", "playback and transcript completeness", "earliest ingest tie-break"})
	return winner.PublicID, raw
}

func ListRedundancyPairs(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	q := db.Where("tenant_id = ?", redundancyTenant).Order("confidence desc")
	if v := c.Query("verdict"); v != "" {
		q = q.Where("verdict = ?", v)
	}
	var rows []models.RedundancyPair
	q.Limit(100).Find(&rows)
	c.JSON(200, gin.H{"data": rows})
}
func GetRedundancyPair(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, e := uuid.Parse(c.Param("id"))
	if e != nil {
		c.JSON(400, gin.H{"error": "invalid pair id"})
		return
	}
	var pair models.RedundancyPair
	if db.Where("public_id = ? AND tenant_id = ?", id, redundancyTenant).First(&pair).Error != nil {
		c.JSON(404, gin.H{"error": "pair not found"})
		return
	}
	var evaluations []models.RedundancyPairEvaluation
	db.Where("pair_id = ?", pair.ID).Order("created_at desc").Find(&evaluations)
	var items []models.ContentItem
	db.Where("public_id IN ?", []uuid.UUID{pair.ItemAID, pair.ItemBID}).Find(&items)
	var transcripts []models.Transcript
	db.Where("content_item_id IN ?", []uuid.UUID{pair.ItemAID, pair.ItemBID}).Find(&transcripts)
	snippets := map[uuid.UUID]string{}
	for _, t := range transcripts {
		body := transcriptBody(t.FullText)
		// Rune-safe truncation: byte-slicing would split multibyte UTF-8
		// (Arabic) characters and corrupt the snippet at the boundary.
		if r := []rune(body); len(r) > 500 {
			body = string(r[:500])
		}
		snippets[t.ContentItemID] = body
	}
	c.JSON(200, gin.H{"data": gin.H{"pair": pair, "evaluations": evaluations, "items": items, "transcript_snippets": snippets}})
}
func ConfirmRedundancyPair(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(400, gin.H{"error": "invalid pair id"})
		return
	}
	var pair models.RedundancyPair
	if db.Where("public_id = ? AND tenant_id = ?", id, redundancyTenant).First(&pair).Error != nil {
		c.JSON(404, gin.H{"error": "pair not found"})
		return
	}
	var confirmedFamilyID uint
	err = db.Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		var memberships []models.RedundancyFamilyMember
		if e := tx.Joins("JOIN redundancy_families f ON f.id = redundancy_family_members.family_id AND f.status = 'active'").Where("redundancy_family_members.tenant_id = ? AND redundancy_family_members.ended_at IS NULL AND redundancy_family_members.content_item_id IN ?", redundancyTenant, []uuid.UUID{pair.ItemAID, pair.ItemBID}).Find(&memberships).Error; e != nil {
			return e
		}
		var f models.RedundancyFamily
		if len(memberships) == 0 {
			canonical, reasons := selectRedundancyCanonical(tx, pair.ItemAID, pair.ItemBID)
			f = models.RedundancyFamily{TenantID: redundancyTenant, Status: "active", CanonicalContentItemID: canonical, CanonicalReasons: datatypes.JSON(reasons), FirstConfirmedAt: now, LastConfirmedAt: now}
			if e := tx.Create(&f).Error; e != nil {
				return e
			}
			roleA, roleB := "redundant", "redundant"
			if canonical == pair.ItemAID {
				roleA = "canonical"
			} else {
				roleB = "canonical"
			}
			if e := tx.Create(&[]models.RedundancyFamilyMember{{FamilyID: f.ID, TenantID: redundancyTenant, ContentItemID: pair.ItemAID, Role: roleA, Since: now}, {FamilyID: f.ID, TenantID: redundancyTenant, ContentItemID: pair.ItemBID, Role: roleB, Since: now}}).Error; e != nil {
				return e
			}
		} else {
			f.ID = memberships[0].FamilyID
			if e := tx.First(&f, f.ID).Error; e != nil {
				return e
			}
			for _, m := range memberships[1:] {
				if m.FamilyID != f.ID {
					if e := tx.Model(&models.RedundancyFamilyMember{}).Where("family_id = ? AND ended_at IS NULL", m.FamilyID).Update("family_id", f.ID).Error; e != nil {
						return e
					}
					if e := tx.Model(&models.RedundancyFamily{}).Where("id = ?", m.FamilyID).Updates(map[string]interface{}{"status": "dissolved", "dissolved_at": now, "dissolve_reason": "merged into confirmed redundancy family"}).Error; e != nil {
						return e
					}
				}
			}
			for _, itemID := range []uuid.UUID{pair.ItemAID, pair.ItemBID} {
				var n int64
				tx.Model(&models.RedundancyFamilyMember{}).Where("family_id = ? AND content_item_id = ? AND ended_at IS NULL", f.ID, itemID).Count(&n)
				if n == 0 {
					if e := tx.Create(&models.RedundancyFamilyMember{FamilyID: f.ID, TenantID: redundancyTenant, ContentItemID: itemID, Role: "redundant", Since: now}).Error; e != nil {
						return e
					}
				}
			}
			tx.Model(&f).Update("last_confirmed_at", now)
		}
		confirmedFamilyID = f.ID
		return tx.Model(&pair).Updates(map[string]interface{}{"family_id": f.ID, "verdict": models.RedundancyVerdictConfirmed, "reviewed_by": redundancyActor(c), "reviewed_at": now}).Error
	})
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	redundancyAction(db, "pair.confirm", redundancyActor(c), "succeeded", &pair.ID, nil, "")
	// Handoff only: Circulation owns applying its decaying demotion and keeps
	// recoverable deletion approval-only. The redundancy system never mutates
	// ranking or storage directly.
	if confirmedFamilyID != 0 && redundancyPolicy(db).EmitCirculationRecs {
		redundantID := pair.ItemBID
		var family models.RedundancyFamily
		if db.First(&family, confirmedFamilyID).Error == nil && family.CanonicalContentItemID == redundantID {
			redundantID = pair.ItemAID
		}
		var existing int64
		db.Model(&models.MediaCirculationRecommendation{}).Where("tenant_id = ? AND subject_id = ? AND verdict = ? AND status = ?", redundancyTenant, redundantID, "rank_down", models.MediaCirculationRecStatusPending).Count(&existing)
		if existing == 0 {
			reasons, _ := json.Marshal([]string{"Human-confirmed redundant media copy"})
			metrics, _ := json.Marshal(map[string]interface{}{"origin": "redundancy", "pair_id": pair.PublicID.String()})
			db.Create(&models.MediaCirculationRecommendation{TenantID: redundancyTenant, UnitType: models.MediaCirculationUnitItemFamily, SubjectID: redundantID, SubjectKind: "content_item", Verdict: "rank_down", Action: "demote", Score: pair.Confidence, Reasons: datatypes.JSON(reasons), Metrics: datatypes.JSON(metrics), Status: models.MediaCirculationRecStatusPending})
		}
		var redundant models.ContentItem
		if db.Where("public_id = ?", redundantID).First(&redundant).Error == nil && redundant.FileSizeBytes >= 500<<20 {
			var reclaim int64
			db.Model(&models.MediaCirculationRecommendation{}).Where("tenant_id = ? AND subject_id = ? AND verdict = ? AND status = ?", redundancyTenant, redundantID, "recoverable_delete", models.MediaCirculationRecStatusPending).Count(&reclaim)
			if reclaim == 0 {
				reasons, _ := json.Marshal([]string{"Human-confirmed redundant copy exceeds reclaim threshold; approval required"})
				metrics, _ := json.Marshal(map[string]interface{}{"origin": "redundancy", "family_id": confirmedFamilyID, "redundancy_reclaim": true, "approval_required": true})
				db.Create(&models.MediaCirculationRecommendation{TenantID: redundancyTenant, UnitType: models.MediaCirculationUnitItemFamily, SubjectID: redundantID, SubjectKind: "content_item", Verdict: "recoverable_delete", Action: "recoverable_delete", Score: pair.Confidence, Reasons: datatypes.JSON(reasons), Metrics: datatypes.JSON(metrics), Status: models.MediaCirculationRecStatusPending})
			}
		}
	}
	c.JSON(200, gin.H{"success": true})
}

func ListRedundancyActions(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.RedundancyAction
	db.Where("tenant_id = ?", redundancyTenant).Order("created_at desc").Limit(100).Find(&rows)
	c.JSON(200, gin.H{"data": rows})
}
func ListRedundancyRuns(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.RedundancyRun
	db.Where("tenant_id = ?", redundancyTenant).Order("started_at desc").Limit(100).Find(&rows)
	c.JSON(200, gin.H{"data": rows})
}
func GetRedundancyRun(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, e := uuid.Parse(c.Param("id"))
	if e != nil {
		c.JSON(400, gin.H{"error": "invalid run id"})
		return
	}
	var run models.RedundancyRun
	if db.Where("public_id = ? AND tenant_id = ?", id, redundancyTenant).First(&run).Error != nil {
		c.JSON(404, gin.H{"error": "run not found"})
		return
	}
	var pairs []models.RedundancyPairEvaluation
	db.Where("run_id = ?", run.ID).Order("created_at desc").Limit(200).Find(&pairs)
	c.JSON(200, gin.H{"data": gin.H{"run": run, "evaluations": pairs}})
}
func RejectRedundancyPair(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(400, gin.H{"error": "invalid pair id"})
		return
	}
	var in struct {
		Reason string `json:"reason"`
	}
	c.ShouldBindJSON(&in)
	r := db.Model(&models.RedundancyPair{}).Where("public_id = ? AND tenant_id = ?", id, redundancyTenant).Updates(map[string]interface{}{"verdict": models.RedundancyVerdictRejected, "tombstoned": true, "reviewed_by": redundancyActor(c), "reviewed_at": time.Now(), "reject_reason": in.Reason})
	if r.RowsAffected == 0 {
		c.JSON(404, gin.H{"error": "pair not found"})
		return
	}
	c.JSON(200, gin.H{"success": true})
}

func ListRedundancyFamilies(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.RedundancyFamily
	db.Where("tenant_id = ?", redundancyTenant).Order("last_confirmed_at desc").Limit(100).Find(&rows)
	c.JSON(200, gin.H{"data": rows})
}
func GetRedundancyFamily(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, e := uuid.Parse(c.Param("id"))
	if e != nil {
		c.JSON(400, gin.H{"error": "invalid family id"})
		return
	}
	var family models.RedundancyFamily
	if db.Where("public_id = ? AND tenant_id = ?", id, redundancyTenant).First(&family).Error != nil {
		c.JSON(404, gin.H{"error": "family not found"})
		return
	}
	var members []models.RedundancyFamilyMember
	db.Where("family_id = ? AND ended_at IS NULL", family.ID).Find(&members)
	c.JSON(200, gin.H{"data": gin.H{"family": family, "members": members}})
}
func SetRedundancyCanonical(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, e := uuid.Parse(c.Param("id"))
	if e != nil {
		c.JSON(400, gin.H{"error": "invalid family id"})
		return
	}
	var in struct {
		ContentItemID uuid.UUID `json:"content_item_id"`
	}
	if c.ShouldBindJSON(&in) != nil || in.ContentItemID == uuid.Nil {
		c.JSON(400, gin.H{"error": "content_item_id required"})
		return
	}
	var f models.RedundancyFamily
	if db.Where("public_id = ? AND tenant_id = ? AND status = 'active'", id, redundancyTenant).First(&f).Error != nil {
		c.JSON(404, gin.H{"error": "active family not found"})
		return
	}
	if db.Where("family_id = ? AND content_item_id = ? AND ended_at IS NULL", f.ID, in.ContentItemID).First(&models.RedundancyFamilyMember{}).Error != nil {
		c.JSON(422, gin.H{"error": "canonical must be an active family member"})
		return
	}
	db.Transaction(func(tx *gorm.DB) error {
		tx.Model(&models.RedundancyFamilyMember{}).Where("family_id = ? AND ended_at IS NULL", f.ID).Update("role", "redundant")
		tx.Model(&models.RedundancyFamilyMember{}).Where("family_id = ? AND content_item_id = ? AND ended_at IS NULL", f.ID, in.ContentItemID).Update("role", "canonical")
		return tx.Model(&f).Updates(map[string]interface{}{"canonical_content_item_id": in.ContentItemID, "canonical_locked_by": redundancyActor(c)}).Error
	})
	redundancyAction(db, "family.recanonicalize", redundancyActor(c), "succeeded", nil, &f.ID, "")
	c.JSON(200, gin.H{"success": true})
}
func DissolveRedundancyFamily(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, e := uuid.Parse(c.Param("id"))
	if e != nil {
		c.JSON(400, gin.H{"error": "invalid family id"})
		return
	}
	var in struct {
		Reason string `json:"reason"`
	}
	c.ShouldBindJSON(&in)
	var f models.RedundancyFamily
	if db.Where("public_id = ? AND tenant_id = ? AND status = 'active'", id, redundancyTenant).First(&f).Error != nil {
		c.JSON(404, gin.H{"error": "active family not found"})
		return
	}
	now := time.Now()
	db.Transaction(func(tx *gorm.DB) error {
		if e := tx.Model(&models.RedundancyFamilyMember{}).Where("family_id = ? AND ended_at IS NULL", f.ID).Update("ended_at", now).Error; e != nil {
			return e
		}
		return tx.Model(&f).Updates(map[string]interface{}{"status": "dissolved", "dissolved_at": now, "dissolved_by": redundancyActor(c), "dissolve_reason": in.Reason}).Error
	})
	redundancyAction(db, "family.dissolve", redundancyActor(c), "succeeded", nil, &f.ID, in.Reason)
	c.JSON(200, gin.H{"success": true})
}

// InternalRedundancyPrecheck is advisory for near matches. The only automatic
// skip-grade result is an exact durable URL already present in CMS.
func InternalRedundancyPrecheck(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var in struct {
		Candidates []struct {
			Title       string `json:"title"`
			DurationSec *int   `json:"duration_sec"`
			SourceURL   string `json:"source_url"`
		} `json:"candidates"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || len(in.Candidates) > 100 {
		c.JSON(400, gin.H{"error": "provide 1..100 candidates"})
		return
	}
	type result struct {
		Verdict        string     `json:"verdict"`
		ExistingItemID *uuid.UUID `json:"existing_item_id,omitempty"`
		Confidence     float64    `json:"confidence"`
		Reasons        []string   `json:"reasons"`
	}
	out := make([]result, 0, len(in.Candidates))
	for _, candidate := range in.Candidates {
		r := result{Verdict: "clear", Reasons: []string{}}
		var exact models.ContentItem
		if candidate.SourceURL != "" && db.Where("tenant_id = ? AND original_url = ?", redundancyTenant, candidate.SourceURL).First(&exact).Error == nil {
			r.Verdict = "exact_identity"
			r.ExistingItemID = &exact.PublicID
			r.Confidence = 1
			r.Reasons = []string{"canonical source URL already exists"}
			out = append(out, r)
			redundancyAction(db, "precheck", "aggregation-service", "succeeded", nil, nil, "exact_identity")
			continue
		}
		if candidate.DurationSec != nil && candidate.Title != "" {
			var candidates []models.ContentItem
			tolerance := redundancyMaxInt(15, int(float64(*candidate.DurationSec)*.02))
			db.Where("tenant_id = ? AND type IN ? AND parent_content_item_id IS NULL AND duration_sec BETWEEN ? AND ?", redundancyTenant, []string{"VIDEO", "PODCAST"}, *candidate.DurationSec-tolerance, *candidate.DurationSec+tolerance).Limit(50).Find(&candidates)
			for _, item := range candidates {
				score := titleSimilarity(candidate.Title, contentTitle(item))
				if score >= .55 {
					r.Verdict = "likely_duplicate"
					r.ExistingItemID = &item.PublicID
					r.Confidence = score
					r.Reasons = []string{"title and duration are similar; advisory only"}
					break
				}
			}
		}
		out = append(out, r)
		redundancyAction(db, "precheck", "aggregation-service", "succeeded", nil, nil, r.Verdict)
	}
	c.JSON(200, gin.H{"candidates": out})
}
func redundancyMaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var _ = utils.HTTPError{}
