package controllers

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

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

var errRedundancyBusy = errors.New("a redundancy run is already active")
var errRedundancyPaused = errors.New("redundancy scheduling is disabled or paused")
var errRedundancyPolicyChanged = errors.New("policy changed; reload current settings before saving your draft")
var errRedundancyDescriptorUnavailable = errors.New("text embedding descriptor unavailable; comparison retained for retry")

type redundancyRunGuard struct {
	conn *sql.Conn
	tx   *sql.Tx
}

func redundancyRunGuardKey(tenant string) string { return "wahb:redundancy-run/v2:" + tenant }

func acquireRedundancyRunGuard(db *gorm.DB, tenant string) (*redundancyRunGuard, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	// An explicit transaction pins a physical backend through Neon/PgBouncer.
	// A session advisory lock can otherwise survive on an idle pooled backend.
	// V2 isolates the new guard from historical leaked session locks; no shared
	// pooled backend is terminated or unlocked by this runner.
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	guard := &redundancyRunGuard{conn: conn, tx: tx}
	var acquired bool
	if err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", redundancyRunGuardKey(tenant)).Scan(&acquired); err != nil || !acquired {
		guard.release()
		if err != nil {
			return nil, err
		}
		return nil, errRedundancyBusy
	}
	return guard, nil
}

func (g *redundancyRunGuard) check() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var alive int
	return g.tx.QueryRowContext(ctx, "SELECT 1").Scan(&alive)
}

func (g *redundancyRunGuard) release() {
	_ = g.tx.Rollback()
	_ = g.conn.Close()
}

func redundancyPolicyFor(db *gorm.DB, tenant string) (models.RedundancyPolicy, error) {
	var p models.RedundancyPolicy
	err := db.Where("tenant_id=?", tenant).First(&p).Error
	if err != gorm.ErrRecordNotFound {
		return p, err
	}
	p = models.DefaultRedundancyPolicy(tenant)
	err = db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}}, DoNothing: true}).Create(&p).Error
	if err == nil {
		err = db.Where("tenant_id=?", tenant).First(&p).Error
	}
	return p, err
}
func redundancyAction(db *gorm.DB, tenant, kind, actor, outcome string, run, pair, family *uint, reason string, metadata interface{}) error {
	return db.Create(&models.RedundancyAction{TenantID: tenant, RunID: run, PairID: pair, FamilyID: family, ActionKind: kind, Actor: actor, Outcome: outcome, Reason: reason, Metadata: redundancyJSON(metadata)}).Error
}

type redundancyScanState struct {
	Scope              string      `json:"scope"`
	Upper              time.Time   `json:"upper"`
	CursorAt           time.Time   `json:"cursor_at"`
	CursorID           uuid.UUID   `json:"cursor_id"`
	PendingID          uuid.UUID   `json:"pending_id"`
	PendingAt          time.Time   `json:"pending_at"`
	Candidates         []uuid.UUID `json:"candidates"`
	CandidateCursor    uuid.UUID   `json:"candidate_cursor"`
	CandidatesComplete bool        `json:"candidates_complete"`
	PriorityCandidates []uuid.UUID `json:"priority_candidates"`
}
type redundancyFrontier struct {
	models.ContentItem
	RedundancyDirtyAt time.Time
}

const redundancyDirtySQL = "GREATEST(content_items.updated_at, COALESCE((SELECT t.created_at FROM transcripts t WHERE t.public_id=content_items.transcript_id AND t.content_item_id=content_items.public_id),content_items.updated_at))"

func redundancyLibrary(db *gorm.DB, tenant string) *gorm.DB {
	return db.Model(&models.ContentItem{}).Where("content_items.tenant_id=? AND content_items.type IN ('VIDEO','PODCAST') AND content_items.parent_content_item_id IS NULL AND content_items.duration_sec > 0 AND content_items.status='READY'", tenant).Where("COALESCE(content_items.playback_url,content_items.media_url,'')<>''")
}
func redundancyCanonicalLibrary(db *gorm.DB, tenant string) *gorm.DB {
	return redundancyLibrary(db, tenant).Where("COALESCE(storage_state,'hot') NOT IN ?", []string{models.StorageStateMissing, models.StorageStateRecoverableDeleted, models.StorageStateRecoveryPending, models.StorageStateUnrecoverable})
}

// Priority neighbors are only the first batch. Exhaust the entire duration
// block through bounded stable pages before advancing this frontier item.
func redundancyCandidatePage(db *gorm.DB, tenant string, item models.ContentItem, after uuid.UUID, priority []uuid.UUID) ([]uuid.UUID, error) {
	tolerance := redundancyMaxInt(15, int(float64(*item.DurationSec)*.02))
	var ids []uuid.UUID
	q := redundancyLibrary(db, tenant).Where("public_id<>? AND public_id>? AND duration_sec BETWEEN ? AND ?", item.PublicID, after, *item.DurationSec-tolerance, *item.DurationSec+tolerance)
	if len(priority) > 0 {
		q = q.Where("public_id NOT IN ?", priority)
	}
	err := q.Order("public_id").Limit(64).Pluck("public_id", &ids).Error
	return ids, err
}
func redundancyFrontierQuery(db *gorm.DB, p models.RedundancyPolicy, s redundancyScanState) *gorm.DB {
	q := redundancyLibrary(db, p.TenantID).Select("content_items.*, "+redundancyDirtySQL+" AS redundancy_dirty_at").Where(redundancyDirtySQL+" <= ?", s.Upper)
	if !s.CursorAt.IsZero() {
		q = q.Where("("+redundancyDirtySQL+",content_items.public_id) > (?,?::uuid)", s.CursorAt, s.CursorID)
	}
	return q.Order("redundancy_dirty_at ASC,content_items.public_id ASC").Limit(p.MaxFrontierItems)
}
func redundancyCandidates(db *gorm.DB, tenant string, item models.ContentItem) ([]uuid.UUID, error) {
	tolerance := redundancyMaxInt(15, int(float64(*item.DurationSec)*.02))
	ids := map[uuid.UUID]bool{}
	base := func() *gorm.DB {
		return redundancyLibrary(db, tenant).Where("public_id<>? AND duration_sec BETWEEN ? AND ?", item.PublicID, *item.DurationSec-tolerance, *item.DurationSec+tolerance)
	}
	var rows []uuid.UUID
	if err := base().Where("redundancy_normalized_title(title) % redundancy_normalized_title(?) AND similarity(redundancy_normalized_title(title),redundancy_normalized_title(?)) >= .55", contentTitle(item), contentTitle(item)).Order("public_id ASC").Limit(32).Pluck("public_id", &rows).Error; err != nil {
		return nil, err
	}
	for _, id := range rows {
		ids[id] = true
	}
	rows = nil
	// Duration-only neighbors preserve recall for retitled recordings with transcripts.
	if err := base().Clauses(clause.OrderBy{Expression: clause.Expr{SQL: "abs(duration_sec-?),public_id", Vars: []interface{}{*item.DurationSec}}}).Limit(8).Pluck("public_id", &rows).Error; err != nil {
		return nil, err
	}
	for _, id := range rows {
		ids[id] = true
	}
	space := currentTextSpaceIDForSimilarity()
	if space != "" && item.Embedding != nil && item.EmbeddingSpaceID != nil && *item.EmbeddingSpaceID == space {
		rows = nil
		if err := base().Where("embedding IS NOT NULL AND embedding_space_id=?", space).Clauses(clause.OrderBy{Expression: clause.Expr{SQL: "embedding <=> ?::vector,public_id", Vars: []interface{}{item.Embedding}}}).Limit(8).Pluck("public_id", &rows).Error; err != nil {
			return nil, err
		}
		for _, id := range rows {
			ids[id] = true
		}
	}
	return redundancySortedIDs(ids), nil
}
func redundancyCosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	dot, aa, bb := 0., 0., 0.
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		aa += x * x
		bb += y * y
	}
	if aa == 0 || bb == 0 {
		return 0
	}
	return math.Max(0, math.Min(1, dot/math.Sqrt(aa*bb)))
}
func redundancyLanes(db *gorm.DB, tenant string, a, b models.ContentItem, cache map[uuid.UUID]models.RedundancyFingerprint) (map[string]redundancyLane, string, error) {
	lanes := map[string]redundancyLane{"duration": {Score: 1, Available: true}, "title": {Score: titleSimilarity(contentTitle(a), contentTitle(b)), Available: true}, "transcript": {Reason: "absent"}, "embedding": {Reason: "space_mismatch"}, "thumbnail": {Reason: "space_mismatch"}}
	if a.DurationSec == nil || b.DurationSec == nil || redundancyAbs(*a.DurationSec-*b.DurationSec) > redundancyMaxInt(15, int(float64(*a.DurationSec)*.02)) {
		lanes["duration"] = redundancyLane{Available: true}
		return lanes, "duration_mismatch", nil
	}
	fps := []models.RedundancyFingerprint{}
	for _, item := range []models.ContentItem{a, b} {
		fp, ok := cache[item.PublicID]
		if !ok {
			var err error
			fp, err = loadRedundancyFingerprint(db, tenant, item.PublicID)
			if err != nil && err != gorm.ErrRecordNotFound {
				return lanes, "", err
			}
			cache[item.PublicID] = fp
		}
		fps = append(fps, fp)
	}
	if fps[0].ShingleCount > 0 && fps[1].ShingleCount > 0 {
		var x, y []uint64
		_ = json.Unmarshal(fps[0].Signature, &x)
		_ = json.Unmarshal(fps[1].Signature, &y)
		lanes["transcript"] = redundancyLane{Score: redundancySignatureSimilarity(x, y), Available: true}
	}
	space := currentTextSpaceIDForSimilarity()
	if space != "" && a.Embedding != nil && b.Embedding != nil && a.EmbeddingSpaceID != nil && b.EmbeddingSpaceID != nil && *a.EmbeddingSpaceID == space && *b.EmbeddingSpaceID == space {
		lanes["embedding"] = redundancyLane{Score: redundancyCosine(a.Embedding.Slice(), b.Embedding.Slice()), Available: true}
	}
	image := currentExpectedSpace(EmbeddingSpaceImage)
	if image.Fresh(descriptorCacheTTL) && a.ImageEmbedding != nil && b.ImageEmbedding != nil && a.ImageEmbeddingSpaceID != nil && b.ImageEmbeddingSpaceID != nil && *a.ImageEmbeddingSpaceID == image.SpaceID && *b.ImageEmbeddingSpaceID == image.SpaceID {
		lanes["thumbnail"] = redundancyLane{Score: redundancyCosine(a.ImageEmbedding.Slice(), b.ImageEmbedding.Slice()), Available: true}
	}
	var checkErr error
	// An unavailable descriptor is infrastructure debt, not evidence of distinct
	// recordings. Keep the checkpoint retryable unless the transcript decides it.
	if space == "" && a.Embedding != nil && b.Embedding != nil && a.EmbeddingSpaceID != nil && b.EmbeddingSpaceID != nil && *a.EmbeddingSpaceID == *b.EmbeddingSpaceID {
		lanes["embedding"] = redundancyLane{Reason: "descriptor_unavailable"}
		checkErr = errRedundancyDescriptorUnavailable
	}
	raw := redundancyJSON([]interface{}{a.UpdatedAt, b.UpdatedAt, a.ProcessingInputDigest, b.ProcessingInputDigest, fps[0].TranscriptChecksum, fps[1].TranscriptChecksum, space, image.SpaceID, lanes, redundancyEvaluator})
	return lanes, fmt.Sprintf("%x", sha256.Sum256(raw)), checkErr
}
func evaluateRedundancyPair(db *gorm.DB, p models.RedundancyPolicy, runID uint, a, b models.ContentItem, cache map[uuid.UUID]models.RedundancyFingerprint) (verdict string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("pair evaluator failed")
			verdict = "check_error"
		}
	}()
	if a.PublicID.String() > b.PublicID.String() {
		a, b = b, a
	}
	var pair models.RedundancyPair
	err = db.Where("tenant_id=? AND item_a_id=? AND item_b_id=?", p.TenantID, a.PublicID, b.PublicID).First(&pair).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return "check_error", err
	}
	if pair.Tombstoned || pair.Verdict == models.RedundancyVerdictConfirmed {
		return "skipped_reviewed", nil
	}
	lanes, fingerprint, checkErr := redundancyLanes(db, p.TenantID, a, b, cache)
	checkErr = redundancyEvidenceCheck(p, lanes, checkErr)
	fingerprint = redundancyEvaluationFingerprint(fingerprint, p)
	if checkErr == nil && pair.LatestEvaluationID != nil {
		var prior models.RedundancyPairEvaluation
		if err := db.First(&prior, *pair.LatestEvaluationID).Error; err != nil {
			return "check_error", err
		}
		if prior.InputFingerprint == fingerprint && prior.EvaluatorVersion == redundancyEvaluator {
			return "skipped_unchanged", nil
		}
	}
	score, machine := redundancyVerdict(lanes, p)
	if checkErr != nil {
		machine = "check_error"
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if pair.ID == 0 {
			pair = models.RedundancyPair{TenantID: p.TenantID, ItemAID: a.PublicID, ItemBID: b.PublicID, Verdict: models.RedundancyVerdictClear}
			if e := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&pair).Error; e != nil {
				return e
			}
		}
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND item_a_id=? AND item_b_id=?", p.TenantID, a.PublicID, b.PublicID).First(&pair).Error; e != nil {
			return e
		}
		if pair.Tombstoned || pair.Verdict == models.RedundancyVerdictConfirmed {
			return nil
		}
		e := models.RedundancyPairEvaluation{PairID: pair.ID, RunID: &runID, InputFingerprint: fingerprint, EvaluatorVersion: redundancyEvaluator, LaneScores: redundancyJSON(lanes), Confidence: score, MachineVerdict: machine}
		if x := tx.Create(&e).Error; x != nil {
			return x
		}
		updates := map[string]interface{}{"latest_evaluation_id": e.ID}
		if checkErr == nil {
			updates["confidence"] = score
			updates["verdict"] = machine
		}
		return tx.Model(&pair).Updates(updates).Error
	})
	if err != nil {
		return "check_error", err
	}
	return machine, checkErr
}
func runRedundancyScan(db *gorm.DB, trigger, actor string) (models.RedundancyRun, error) {
	return runRedundancyScanScope(db, trigger, actor, "frontier")
}
func runRedundancyScanScope(db *gorm.DB, trigger, actor, scope string) (models.RedundancyRun, error) {
	return runRedundancyTenant(db, "default", trigger, actor, scope)
}
func runRedundancyTenant(db *gorm.DB, tenant, trigger, actor, scope string) (run models.RedundancyRun, err error) {
	capacityDone, capacityErr := beginWorkloadSweep(db, tenant, "redundancy_hygiene", trigger)
	if capacityErr != nil {
		return run, capacityErr
	}
	defer capacityDone()
	if !tryStartRedundancyRun() {
		return run, errRedundancyBusy
	}
	defer finishRedundancyRun()
	guard, err := acquireRedundancyRunGuard(db, tenant)
	if err != nil {
		return run, err
	}
	defer guard.release()
	err = db.Connection(func(conn *gorm.DB) error {
		conn = conn.Session(&gorm.Session{NewDB: true})
		p, e := redundancyPolicyFor(conn, tenant)
		if e != nil {
			return e
		}
		if p.PausedUntil != nil && p.PausedUntil.After(time.Now()) || trigger == "scheduled" && !p.Enabled {
			return errRedundancyPaused
		}
		run = models.RedundancyRun{TenantID: tenant, Trigger: trigger, Status: "running", StartedAt: time.Now().UTC()}
		if e = conn.Create(&run).Error; e != nil {
			return e
		}
		state := redundancyScanState{}
		_ = json.Unmarshal(p.ScanState, &state)
		if state.Upper.IsZero() {
			state.Scope = scope
			state.Upper = run.StartedAt
			if scope != "full" && p.LastSweptAt != nil {
				state.CursorAt = *p.LastSweptAt
				state.CursorID = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			}
		}
		counts := map[string]int{}
		cache := map[uuid.UUID]models.RedundancyFingerprint{}
		var workErr error
		var frontier []redundancyFrontier
		if e = redundancyFrontierQuery(conn, p, state).Find(&frontier).Error; e != nil {
			workErr = e
		}
		if state.PendingID != uuid.Nil {
			filtered := frontier[:0]
			for _, row := range frontier {
				if row.PublicID != state.PendingID {
					filtered = append(filtered, row)
				}
			}
			frontier = filtered
			var item models.ContentItem
			if e = conn.Where("tenant_id=? AND public_id=?", tenant, state.PendingID).First(&item).Error; e != nil {
				workErr = e
			} else {
				frontier = append([]redundancyFrontier{{ContentItem: item, RedundancyDirtyAt: state.PendingAt}}, frontier...)
			}
		}
		if len(frontier) > p.MaxFrontierItems {
			frontier = frontier[:p.MaxFrontierItems]
		}
		counts["frontier_items"] = len(frontier)
		checkpoint := func() error {
			if err := guard.check(); err != nil {
				return err
			}
			return conn.Model(&models.RedundancyPolicy{}).Where("tenant_id=?", tenant).Update("scan_state", redundancyJSON(state)).Error
		}
		paused := false
		for _, item := range frontier {
			if workErr != nil || counts["pairs_scored"] >= p.MaxPairsScored {
				break
			}
			current, readErr := redundancyPolicyFor(conn, tenant)
			if readErr != nil {
				workErr = readErr
				break
			}
			if current.PausedUntil != nil && current.PausedUntil.After(time.Now()) || trigger == "scheduled" && !current.Enabled {
				paused = true
				break
			}
			if state.PendingID == uuid.Nil {
				candidates, x := redundancyCandidates(conn, tenant, item.ContentItem)
				if x != nil {
					workErr = x
					break
				}
				state.PendingID = item.PublicID
				state.PendingAt = item.RedundancyDirtyAt
				state.Candidates = candidates
				state.PriorityCandidates = append([]uuid.UUID(nil), candidates...)
				state.CandidateCursor = uuid.Nil
				state.CandidatesComplete = false
				if workErr = checkpoint(); workErr != nil {
					break
				}
			}
			for !state.CandidatesComplete || len(state.Candidates) > 0 {
				if len(state.Candidates) == 0 {
					page, x := redundancyCandidatePage(conn, tenant, item.ContentItem, state.CandidateCursor, state.PriorityCandidates)
					if x != nil {
						workErr = x
						break
					}
					state.Candidates = page
					state.CandidatesComplete = len(page) < 64
					if len(page) > 0 {
						state.CandidateCursor = page[len(page)-1]
					}
					if workErr = checkpoint(); workErr != nil {
						break
					}
				}
				pending := append([]uuid.UUID(nil), state.Candidates...)
				failed := []uuid.UUID{}
				for index, id := range pending {
					state.Candidates = append(append([]uuid.UUID(nil), failed...), pending[index:]...)
					if counts["pairs_scored"] >= p.MaxPairsScored {
						break
					}
					current, readErr := redundancyPolicyFor(conn, tenant)
					if readErr != nil {
						workErr = readErr
						break
					}
					if current.PausedUntil != nil && current.PausedUntil.After(time.Now()) || trigger == "scheduled" && !current.Enabled {
						paused = true
						break
					}
					var other models.ContentItem
					x := redundancyLibrary(conn, tenant).Where("public_id=?", id).First(&other).Error
					if x == gorm.ErrRecordNotFound {
						state.Candidates = append(append([]uuid.UUID(nil), failed...), pending[index+1:]...)
						if x = checkpoint(); x != nil {
							workErr = x
							break
						}
						continue
					}
					if x != nil {
						workErr = x
						break
					}
					verdict, x := evaluateRedundancyPair(conn, p, run.ID, item.ContentItem, other, cache)
					counts["pairs_scored"]++
					counts[verdict]++
					if x != nil {
						failed = append(failed, id)
						workErr = x
						if auditErr := redundancyAction(conn, tenant, "pair.evaluate", actor, "failed", &run.ID, nil, nil, "Pair evaluation failed; retained for retry", map[string]interface{}{"item_a": item.PublicID, "item_b": id}); auditErr != nil {
							workErr = auditErr
							break
						}
					}
					state.Candidates = append(append([]uuid.UUID(nil), failed...), pending[index+1:]...)
					if x = checkpoint(); x != nil {
						workErr = x
						break
					}
				}
				if workErr != nil || paused {
					break
				}
				if len(state.Candidates) > 0 {
					break
				}
			}
			if workErr != nil || paused || len(state.Candidates) > 0 || !state.CandidatesComplete {
				break
			}
			state.CursorAt = state.PendingAt
			state.CursorID = state.PendingID
			state.PendingID = uuid.Nil
			state.Candidates = nil
			if workErr = checkpoint(); workErr != nil {
				break
			}
		}
		complete := false
		if workErr == nil && !paused && state.PendingID == uuid.Nil {
			var next []redundancyFrontier
			e = redundancyFrontierQuery(conn, p, state).Limit(1).Find(&next).Error
			if e != nil {
				workErr = e
			} else {
				complete = len(next) == 0
			}
		}
		if workErr == nil && p.EmitCirculationRecs {
			workErr = reconcileRedundancyRecommendations(conn, tenant, actor, &run.ID)
			if workErr != nil {
				counts["handoff_errors"]++
			}
		}
		if workErr == nil {
			workErr = guard.check()
		}
		now := time.Now().UTC()
		run.FinishedAt = &now
		run.Status = "completed"
		if paused {
			run.Status = "paused"
		}
		if workErr != nil {
			run.Status = "failed"
			run.Error = "Scan did not complete; checkpoint retained for retry"
			run.ErrorClass = "check_error"
		}
		run.Counts = redundancyJSON(counts)
		run.Summary = fmt.Sprintf("%d candidates evaluated; %d proposals; backfill complete: %t", counts["pairs_scored"], counts[models.RedundancyVerdictProbable]+counts[models.RedundancyVerdictHighConfidence], complete)
		return conn.Transaction(func(tx *gorm.DB) error {
			if x := tx.Save(&run).Error; x != nil {
				return x
			}
			if complete && workErr == nil {
				if x := tx.Model(&models.RedundancyPolicy{}).Where("tenant_id=?", tenant).Updates(map[string]interface{}{"last_swept_at": state.Upper, "scan_state": datatypes.JSON([]byte("{}"))}).Error; x != nil {
					return x
				}
			}
			return redundancyAction(tx, tenant, "run", actor, run.Status, &run.ID, nil, nil, run.Summary, counts)
		})
	})
	return run, err
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
	if db.Where("enabled=true").Find(&policies).Error != nil {
		return
	}
	now := time.Now()
	for _, p := range policies {
		if p.PausedUntil != nil && p.PausedUntil.After(now) {
			continue
		}
		var last models.RedundancyRun
		err := db.Where("tenant_id=?", p.TenantID).Order("started_at DESC").First(&last).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			continue
		}
		if !last.StartedAt.IsZero() && now.Sub(last.StartedAt) < time.Duration(p.SweepIntervalMinutes)*time.Minute {
			continue
		}
		_, _ = runRedundancyTenant(db, p.TenantID, "scheduled", "system", "frontier")
	}
	pruneRedundancyRetention(db, now)
}
func pruneRedundancyRetention(db *gorm.DB, now time.Time) {
	cutoff := now.AddDate(0, 0, -30)
	_ = db.Where("id IN (SELECT id FROM redundancy_pairs WHERE verdict='not_duplicate' AND tombstoned=false AND family_id IS NULL AND updated_at<? ORDER BY id LIMIT 1000)", cutoff).Delete(&models.RedundancyPair{}).Error
	_ = db.Where("id IN (SELECT id FROM redundancy_pair_evaluations WHERE created_at<? AND machine_verdict IN ('not_duplicate','check_error') AND id NOT IN (SELECT latest_evaluation_id FROM redundancy_pairs WHERE latest_evaluation_id IS NOT NULL) ORDER BY id LIMIT 1000)", cutoff).Delete(&models.RedundancyPairEvaluation{}).Error
}
