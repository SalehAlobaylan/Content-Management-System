package controllers

import (
	"content-management-system/src/models"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Preferences Autopilot — bounded, ledgered runner (plan §4, §8). One run:
// preconditions → snapshot-before → §8 safe actions (observe records shadow +
// runs the incumbent baseline once; safe_auto runs the bounded actions) →
// snapshot-after → verdict. It branches observe/safe_auto only at the terminal
// mutation; the guardrail evaluation and scoring are identical in both.

var (
	errPreferenceAutopilotDisabled       = errors.New("preferences autopilot is not enabled for this tenant")
	errPreferenceAutopilotAlreadyRunning = errors.New("preferences autopilot is already running for this tenant")
)

var (
	preferenceAutopilotRunMu       sync.Mutex
	preferenceAutopilotRunInFlight = map[string]bool{}
)

func tryStartPreferenceAutopilotRun(tenantID string) bool {
	preferenceAutopilotRunMu.Lock()
	defer preferenceAutopilotRunMu.Unlock()
	if preferenceAutopilotRunInFlight[tenantID] {
		return false
	}
	preferenceAutopilotRunInFlight[tenantID] = true
	return true
}

func finishPreferenceAutopilotRun(tenantID string) {
	preferenceAutopilotRunMu.Lock()
	defer preferenceAutopilotRunMu.Unlock()
	delete(preferenceAutopilotRunInFlight, tenantID)
}

// ----------------------------------------------------------------
// Snapshot (§6 signals) — persisted on the run row (§11)
// ----------------------------------------------------------------

type preferenceFlipGate struct {
	Flag        string  `json:"flag"`
	Enabled     bool    `json:"enabled"`
	CoveragePct float64 `json:"coverage_pct"`
	FloorPct    int     `json:"floor_pct"`
	State       string  `json:"state"` // green | amber | red
}

type preferenceSnapshot struct {
	ActiveTopics       int64 `json:"active_topics"`
	NullCentroidTopics int64 `json:"null_centroid_topics"`
	DeadTopics         int64 `json:"dead_topics"`

	PendingProposals      int64   `json:"pending_proposals"`
	OldestPendingAgeHours float64 `json:"oldest_pending_age_hours"`
	HighConfidencePending int     `json:"high_confidence_pending"`

	ForyouCoveragePct float64 `json:"foryou_coverage_pct"`
	NewsCoveragePct   float64 `json:"news_coverage_pct"`
	StoryCoveragePct  float64 `json:"story_coverage_pct"`
	UnmappedBacklog   int64   `json:"unmapped_backlog"`

	NearDuplicatePairs  int       `json:"near_duplicate_pairs"`
	DuplicatePairs      []dupPair `json:"duplicate_pairs,omitempty"`
	RecomputeQueueDepth int64     `json:"recompute_queue_depth"`
	RecentlyActiveUsers int64     `json:"recently_active_users"`
	MuteViolations      int64     `json:"mute_violations"`

	BoostedServes int64  `json:"boosted_serves"`
	TotalServes   int64  `json:"total_serves"`
	BoostSanity   string `json:"boost_sanity"` // ok | unknown

	FlipGates map[string]preferenceFlipGate `json:"flip_gates"`
}

// computePreferenceSnapshot reads the §6 signals corpus-wide. Heavy by design — it
// runs inside a bounded pass, not on a GET (§11). `deep` gates the O(n²)
// near-duplicate scan (loads every active centroid): only the after-snapshot needs
// it (it feeds the verdict + cockpit), so the before-snapshot passes false to avoid
// paying for it twice per run.
func computePreferenceSnapshot(db *gorm.DB, tenantID string, policy models.PreferenceAutopilotPolicy, deep bool) preferenceSnapshot {
	snap := preferenceSnapshot{FlipGates: map[string]preferenceFlipGate{}}

	db.Model(&models.Topic{}).Where("tenant_id = ? AND active = ?", tenantID, true).Count(&snap.ActiveTopics)
	db.Model(&models.Topic{}).Where("tenant_id = ? AND active = ? AND centroid IS NULL", tenantID, true).Count(&snap.NullCentroidTopics)
	// Dead = approved N+ days ago and still zero members. Key on created_at (≈
	// approval time), NOT updated_at — member-count refresh bumps updated_at every
	// run, which would otherwise perpetually reset the dead-topic clock.
	deadBefore := time.Now().AddDate(0, 0, -policy.DeadTopicDays)
	db.Model(&models.Topic{}).Where("tenant_id = ? AND active = ? AND member_count = 0 AND created_at < ?", tenantID, true, deadBefore).Count(&snap.DeadTopics)

	db.Model(&models.TopicProposal{}).Where("tenant_id = ? AND status = ?", tenantID, "pending").Count(&snap.PendingProposals)
	var oldest models.TopicProposal
	if err := db.Where("tenant_id = ? AND status = ?", tenantID, "pending").Order("created_at ASC").First(&oldest).Error; err == nil {
		snap.OldestPendingAgeHours = time.Since(oldest.CreatedAt).Hours()
	}
	var hiConf int64
	db.Model(&models.TopicProposal{}).
		Where("tenant_id = ? AND status = ? AND predicted_verdict = ?", tenantID, "pending", models.PreferenceVerdictHighConf).
		Count(&hiConf)
	snap.HighConfidencePending = int(hiConf)

	// Coverage — mapped subjects over their eligible base. Each closure builds a
	// FRESH scoped query so the base + mapped counts never share a finished
	// statement (a fragile GORM reuse trap).
	snap.ForyouCoveragePct = coveragePct(
		func() *gorm.DB {
			return db.Model(&models.ContentItem{}).Where("tenant_id = ? AND status = ? AND is_feed_unit = ? AND feed_visibility = ? AND type IN ?",
				tenantID, models.ContentStatusReady, true, "visible", []string{"VIDEO", "PODCAST"})
		}, "public_id", "content_item_topics", "content_item_id")
	snap.NewsCoveragePct = coveragePct(
		func() *gorm.DB {
			return db.Model(&models.ContentItem{}).Where("tenant_id = ? AND status = ? AND type = ?", tenantID, models.ContentStatusReady, "NEWS")
		}, "public_id", "content_item_topics", "content_item_id")
	snap.StoryCoveragePct = coveragePct(
		func() *gorm.DB { return db.Model(&models.Story{}).Where("tenant_id = ?", tenantID) },
		"public_id", "story_topics", "story_id")

	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND status = ? AND embedding IS NOT NULL AND embedding_space_id = ?", tenantID, models.ContentStatusReady, currentTextSpaceIDForSimilarity()).
		Where("NOT EXISTS (SELECT 1 FROM content_item_topics cit WHERE cit.content_item_id = content_items.public_id)").
		Count(&snap.UnmappedBacklog)

	db.Model(&models.PreferenceAffinityRecomputeQueue{}).Where("tenant_id = ?", tenantID).Count(&snap.RecomputeQueueDepth)

	// Mute integrity invariant: a muted topic must never appear in that user's
	// affinity rows.
	db.Raw(`
		SELECT COUNT(*) FROM user_topic_prefs p
		JOIN user_topic_affinity a
		  ON a.tenant_id = p.tenant_id AND a.user_id = p.user_id AND a.topic_id = p.topic_id
		WHERE p.tenant_id = ? AND p.state = 'muted'
	`, tenantID).Scan(&snap.MuteViolations)

	if deep {
		snap.NearDuplicatePairs, snap.DuplicatePairs = findNearDuplicateTopics(db, tenantID, policy.DuplicateCosine, 20)
	}

	// Boost sanity from preference_stats over the last 7 days. Empty window = unknown.
	weekAgo := time.Now().AddDate(0, 0, -7).UTC().Truncate(24 * time.Hour)
	var serveAgg struct {
		Boosted int64
		Total   int64
	}
	db.Model(&models.PreferenceStat{}).
		Select("COALESCE(SUM(boosted_serves),0) AS boosted, COALESCE(SUM(total_serves),0) AS total").
		Where("tenant_id = ? AND day >= ?", tenantID, weekAgo).Scan(&serveAgg)
	snap.BoostedServes = serveAgg.Boosted
	snap.TotalServes = serveAgg.Total
	if serveAgg.Total > 0 {
		snap.BoostSanity = "ok"
	} else {
		snap.BoostSanity = "unknown"
	}

	settings := loadPreferenceSettings(db, tenantID)
	snap.FlipGates["foryou_enabled"] = buildFlipGate("foryou_enabled", settings.ForYouEnabled, snap.ForyouCoveragePct, policy.CoverageFloorForyouPct, snap.BoostSanity)
	snap.FlipGates["news_enabled"] = buildFlipGate("news_enabled", settings.NewsEnabled, snap.NewsCoveragePct, policy.CoverageFloorNewsPct, snap.BoostSanity)
	return snap
}

// coveragePct returns 100 * (subjects with ≥1 mapping) / (base subjects). newBase
// returns a FRESH query scoped to the base set on each call.
func coveragePct(newBase func() *gorm.DB, pkCol, mapTable, mapFK string) float64 {
	var base int64
	if err := newBase().Count(&base).Error; err != nil || base == 0 {
		return 0
	}
	var mapped int64
	newBase().
		Where(fmt.Sprintf("EXISTS (SELECT 1 FROM %s m WHERE m.%s = %s)", mapTable, mapFK, pkCol)).
		Count(&mapped)
	return 100 * float64(mapped) / float64(base)
}

func buildFlipGate(flag string, enabled bool, coverage float64, floor int, boostSanity string) preferenceFlipGate {
	g := preferenceFlipGate{Flag: flag, Enabled: enabled, CoveragePct: coverage, FloorPct: floor}
	switch {
	case coverage >= float64(floor):
		g.State = "green"
	case coverage >= float64(floor)*0.8:
		g.State = "amber"
	default:
		g.State = "red"
	}
	// A green coverage gate with no boost evidence yet is capped at amber pre-flip.
	if g.State == "green" && !enabled && boostSanity == "unknown" {
		g.State = "amber"
	}
	return g
}

type dupPair struct {
	ASlug  string  `json:"a_slug"`
	BSlug  string  `json:"b_slug"`
	AID    string  `json:"a_id"`
	BID    string  `json:"b_id"`
	Cosine float64 `json:"cosine"`
}

// findNearDuplicateTopics is a bounded O(n²) scan over active topic centroids. It
// returns the total pair count and up to `limit` highest-cosine pairs with slugs,
// so the cockpit can render merge suggestions without a heavy scan on GET.
func findNearDuplicateTopics(db *gorm.DB, tenantID string, threshold float64, limit int) (int, []dupPair) {
	topics, err := activeTopicVectors(db, tenantID)
	if err != nil || len(topics) < 2 {
		return 0, nil
	}
	slugByID := map[string]string{}
	{
		var rows []struct {
			PublicID uuid.UUID
			Slug     string
		}
		db.Model(&models.Topic{}).Select("public_id, slug").Where("tenant_id = ? AND active = ?", tenantID, true).Scan(&rows)
		for _, rrow := range rows {
			slugByID[rrow.PublicID.String()] = rrow.Slug
		}
	}
	count := 0
	pairs := make([]dupPair, 0, limit)
	for i := 0; i < len(topics); i++ {
		for j := i + 1; j < len(topics); j++ {
			c := cosine(topics[i].Vec, topics[j].Vec)
			if c < threshold {
				continue
			}
			count++
			pairs = append(pairs, dupPair{
				AID: topics[i].ID.String(), BID: topics[j].ID.String(),
				ASlug: slugByID[topics[i].ID.String()], BSlug: slugByID[topics[j].ID.String()],
				Cosine: c,
			})
		}
	}
	sort.SliceStable(pairs, func(a, b int) bool { return pairs[a].Cosine > pairs[b].Cosine })
	if len(pairs) > limit {
		pairs = pairs[:limit]
	}
	return count, pairs
}

// ----------------------------------------------------------------
// Runner
// ----------------------------------------------------------------

type preferenceAutopilotRunner struct {
	db      *gorm.DB
	run     *models.PreferenceAutopilotRun
	policy  models.PreferenceAutopilotPolicy
	observe bool

	success   int
	skipped   int
	errored   int
	attempts  int
	failures  int
	ledgerErr error
}

func (r *preferenceAutopilotRunner) writeAction(class, subjectType, subjectRef, status, guardrail, reason string, started time.Time) {
	now := time.Now()
	if err := r.db.Create(&models.PreferenceAutopilotAction{
		RunID: r.run.ID, TenantID: r.run.TenantID,
		ActionClass: class, SubjectType: subjectType, SubjectRef: subjectRef,
		Status: status, Guardrail: guardrail, Reason: reason,
		DurationMs: int(now.Sub(started).Milliseconds()),
		StartedAt:  started, FinishedAt: &now,
	}).Error; err != nil && r.ledgerErr == nil {
		r.ledgerErr = err
	}
	switch status {
	case models.PreferenceActionStatusSuccess, models.PreferenceActionStatusBaselineSuccess, models.PreferenceActionStatusWouldTrigger:
		r.success++
		if status != models.PreferenceActionStatusWouldTrigger {
			r.attempts++
		}
	case models.PreferenceActionStatusError, models.PreferenceActionStatusBaselineError:
		r.errored++
		r.failures++
		r.attempts++
	case models.PreferenceActionStatusSkipped, models.PreferenceActionStatusWouldSkip:
		r.skipped++
	}
}

func (r *preferenceAutopilotRunner) classBreakerTripped(class string) bool {
	if r.run.Trigger == "manual" {
		return false
	}
	var previous models.PreferenceAutopilotAction
	err := r.db.Where("tenant_id = ? AND action_class = ? AND run_id <> ?", r.run.TenantID, class, r.run.ID).
		Order("id DESC").First(&previous).Error
	if err != nil {
		return false
	}
	return previous.Status == models.PreferenceActionStatusError || previous.Status == models.PreferenceActionStatusBaselineError
}

type preferenceAutopilotRunOptions struct {
	Trigger   string
	CreatedBy string
}

func runPreferenceAutopilot(db *gorm.DB, tenantID string, opts preferenceAutopilotRunOptions) (models.PreferenceAutopilotRun, error) {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	if !tryStartPreferenceAutopilotRun(tenantID) {
		return models.PreferenceAutopilotRun{}, errPreferenceAutopilotAlreadyRunning
	}
	defer finishPreferenceAutopilotRun(tenantID)

	policy := loadPreferenceAutopilotPolicy(db, tenantID)
	if !policy.Enabled {
		return models.PreferenceAutopilotRun{}, errPreferenceAutopilotDisabled
	}

	trigger := strings.TrimSpace(opts.Trigger)
	if trigger == "" {
		trigger = "scheduled"
	}
	observe := policy.Mode != models.PreferenceAutopilotModeSafeAuto
	now := time.Now()

	snapBefore := computePreferenceSnapshot(db, tenantID, policy, false)
	run := models.PreferenceAutopilotRun{
		TenantID: tenantID, Trigger: trigger, Mode: policy.Mode,
		Status: models.PreferenceAutopilotRunStatusRunning, StartedAt: now,
		CreatedBy:   firstNonEmptyString(opts.CreatedBy, "automation"),
		StatsBefore: marshalAutopilotJSON(snapBefore),
	}
	if err := db.Create(&run).Error; err != nil {
		return run, err
	}
	runner := &preferenceAutopilotRunner{db: db, run: &run, policy: policy, observe: observe}

	emptyCatalog := snapBefore.ActiveTopics == 0
	scored := runner.execute(snapBefore, emptyCatalog)

	snapAfter := computePreferenceSnapshot(db, tenantID, policy, true)
	headline, summary, recommended := computePreferenceVerdict(snapAfter, scored, runner, policy)

	status := models.PreferenceAutopilotRunStatusCompleted
	if runner.errored > 0 && runner.success == 0 {
		status = models.PreferenceAutopilotRunStatusFailed
	} else if runner.errored > 0 {
		status = models.PreferenceAutopilotRunStatusPartial
	}
	finishedAt := time.Now()
	if runner.ledgerErr != nil {
		status = models.PreferenceAutopilotRunStatusFailed
		headline = models.PreferenceAutopilotHeadlineDegraded
		recommended = "Repair the action ledger, then run again."
	}
	if err := db.Model(&models.PreferenceAutopilotRun{}).Where("id = ?", run.ID).Updates(map[string]interface{}{
		"status": status, "headline": headline, "summary": summary, "recommended_action": recommended,
		"finished_at": finishedAt, "stats_after": marshalAutopilotJSON(snapAfter), "updated_at": finishedAt,
	}).Error; err != nil {
		return run, err
	}
	touchPreferenceAutopilotLastRun(db, tenantID, finishedAt)

	run.Status = status
	run.Headline = headline
	run.Summary = summary
	run.RecommendedAction = recommended
	run.FinishedAt = &finishedAt
	run.StatsAfter = marshalAutopilotJSON(snapAfter)
	return run, nil
}

// execute runs the §8 actions. Observe runs the incumbent baseline once (so
// maintenance never stops) plus shadow decisions; safe_auto runs the bounded
// actions. Returns the proposal scores for the verdict.
func (r *preferenceAutopilotRunner) execute(snap preferenceSnapshot, emptyCatalog bool) []proposalScore {
	// Proposal enrichment/scoring happens in BOTH modes (a read/analysis); persist
	// only in safe_auto (§8.7). suggest_reject stays advisory.
	scored := r.enrichProposals()

	if emptyCatalog {
		r.writeAction(models.PreferenceActionMapSweep, models.PreferenceSubjectAggregate, "", statusSkip(r.observe),
			models.PreferenceGuardEmptyCatalog, "No active topics with centroids — mapping/affinity skipped.", time.Now())
		if r.observe {
			r.runBaselineAsAction()
			r.shadowBoundedActions(snap, scored)
		} else {
			r.actionCentroidRefresh()
			r.actionMemberRefresh()
			r.actionMine(snap)
		}
		return scored
	}

	if r.observe {
		// Baseline: run the incumbent maintenance once so Observe never causes a
		// maintenance outage, and ledger it as baseline_* (§9, §0.1.9).
		r.runBaselineAsAction()
		// Shadow: record what the bounded runner WOULD do.
		r.shadowBoundedActions(snap, scored)
		return scored
	}

	// safe_auto — the bounded actions replace the baseline.
	r.actionAutoApprove(scored, snap)
	r.actionMapSweep()
	r.actionCentroidRefresh()
	r.actionDirtySweep()
	r.actionMemberRefresh()
	r.actionRecompute()
	r.actionMine(snap)
	r.actionMergeSuggest(snap)
	return scored
}

// preferenceAutoApproveGate is the pure earned-tier decision (§15, testable
// without a DB). Checks in order: switch on → trust re-verified at run time →
// decisive high-confidence verdict above the tier's OWN threshold → no blocker
// flag (defense in depth — the verdict already blocks on these) → per-run cap.
// Auto-REJECT has no gate because it has no code path: permanently forbidden.
func preferenceAutoApproveGate(policy models.PreferenceAutopilotPolicy, trust preferenceTrustBanner, sc proposalScore, approvedThisRun int) (bool, string, string) {
	if !policy.AutoApproveEnabled {
		return false, "", "" // silent: the tier is off, not a guardrail event
	}
	if !trust.Eligible {
		return false, models.PreferenceGuardTrustGate,
			fmt.Sprintf("Trust not eligible at run time (%d decisions, %.0f%% agreement; needs ≥%d at ≥%d%% with no mute violation).",
				trust.Decisions, trust.AgreementPct, policy.TrustMinDecisions, policy.TrustMinAgreementPct)
	}
	if sc.Verdict != models.PreferenceVerdictHighConf || sc.Confidence < policy.AutoApproveMinConfidence {
		return false, "", "" // silent: below the tier bar is the normal case, not an anomaly
	}
	if sc.Duplicate || sc.CategoryUnknown || sc.NeedsLabel || !sc.EmbeddingReady {
		return false, models.PreferenceGuardBlockerFlag, "Blocker flag present despite high-confidence verdict — held for human review."
	}
	if approvedThisRun >= policy.MaxAutoApprovals {
		return false, models.PreferenceGuardRunCap, fmt.Sprintf("Auto-approve cap (%d per run) reached.", policy.MaxAutoApprovals)
	}
	return true, "", ""
}

// actionAutoApprove executes the earned auto-approve tier over this run's scored
// proposals. Every approval is QUARANTINED: active=true, featured=false (never in
// the picker), created_from='autopilot', needs_remap=true, resolved_by the
// autopilot principal so trust evidence excludes it. The centroid reuses the
// proposal's cached embedding — zero extra Enrichment calls. A slug pre-check
// skips (never upserts over) an existing topic: the OnConflict clobber semantics
// of the approve core stay human-only.
func (r *preferenceAutopilotRunner) actionAutoApprove(scored []proposalScore, snap preferenceSnapshot) {
	if !r.policy.AutoApproveEnabled || len(scored) == 0 {
		return
	}
	started := time.Now()
	if r.classBreakerTripped(models.PreferenceActionAutoApprove) {
		r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectAggregate, "",
			models.PreferenceActionStatusSkipped, models.PreferenceGuardClassBreaker,
			"Previous auto-approval failed; quarantined for one scheduled run.", started)
		return
	}
	trust := computePreferenceTrust(r.db, r.run.TenantID, r.policy, snap.MuteViolations)
	approved := 0
	for _, sc := range scored {
		ok, guardrail, reason := preferenceAutoApproveGate(r.policy, trust, sc, approved)
		if !ok {
			if guardrail == models.PreferenceGuardTrustGate {
				// One aggregate row; per-proposal trust rows would be noise.
				r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectAggregate, "",
					models.PreferenceActionStatusSkipped, guardrail, reason, started)
				return
			}
			if guardrail != "" {
				r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectProposal,
					fmt.Sprintf("%d", sc.ProposalID), models.PreferenceActionStatusSkipped, guardrail, reason, time.Now())
			}
			if guardrail == models.PreferenceGuardRunCap {
				return
			}
			continue
		}
		if r.autoApproveOne(sc) {
			approved++
		}
	}
}

// autoApproveOne returns true only after the quarantined topic has been
// persisted. Skips and failures must not consume the per-run approval budget.
func (r *preferenceAutopilotRunner) autoApproveOne(sc proposalScore) bool {
	started := time.Now()
	subject := fmt.Sprintf("%d", sc.ProposalID)

	var p models.TopicProposal
	if err := r.db.Where("tenant_id = ? AND id = ? AND status = ?", r.run.TenantID, sc.ProposalID, "pending").First(&p).Error; err != nil {
		r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectProposal, subject,
			models.PreferenceActionStatusSkipped, models.PreferenceGuardAlreadyMapped,
			"Proposal no longer pending (resolved by a human mid-run).", started)
		return false
	}
	slug, ar, en := canonicalTopicLabels(p.SuggestedSlug, p.SuggestedLabelAR, p.SuggestedLabelEN)

	// Slug pre-check: the machine must never reach the upsert-clobber path.
	var existing int64
	if err := r.db.Model(&models.Topic{}).Where("tenant_id = ? AND slug = ?", r.run.TenantID, slug).Count(&existing).Error; err != nil {
		r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectProposal, subject,
			models.PreferenceActionStatusError, "", "Existing-topic check failed: "+err.Error(), started)
		return false
	}
	if existing > 0 {
		r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectProposal, subject,
			models.PreferenceActionStatusSkipped, models.PreferenceGuardSlugExists,
			fmt.Sprintf("Topic %q already exists — merge or approve is a human call.", slug), started)
		return false
	}

	topic := models.Topic{
		TenantID: r.run.TenantID, Slug: slug, LabelAR: ar, LabelEN: en,
		CategorySlug: p.SuggestedCategory,
		Active:       true, Featured: false, // quarantine: never straight into the picker
		CreatedFrom: models.PreferenceCreatedFromAutopilot,
		NeedsRemap:  true,
		Centroid:    p.Embedding, // cached proposal embedding — no Enrichment call
	}
	saved, err := approveTopicProposalCore(r.db, r.run.TenantID, p, topic, models.PreferenceAutopilotResolver)
	if err != nil {
		r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectProposal, subject,
			models.PreferenceActionStatusError, "", "Auto-approve failed: "+err.Error(), started)
		return false
	}
	r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectProposal, subject,
		models.PreferenceActionStatusSuccess, "",
		fmt.Sprintf("Auto-approved %q (confidence %.2f) — quarantined: unfeatured, created_from=autopilot, one-click revert.", saved.Slug, sc.Confidence), started)
	return true
}

func (r *preferenceAutopilotRunner) enrichProposals() []proposalScore {
	started := time.Now()
	if r.classBreakerTripped(models.PreferenceActionProposalEnrich) {
		r.writeAction(models.PreferenceActionProposalEnrich, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusSkipped, models.PreferenceGuardClassBreaker, "Previous proposal enrichment failed; quarantined for one scheduled run.", started)
		return nil
	}
	var pending []models.TopicProposal
	if err := r.db.Where("tenant_id = ? AND status = ?", r.run.TenantID, "pending").
		Order("created_at ASC").Limit(r.policy.MaxProposalsEnriched).Find(&pending).Error; err != nil {
		r.writeAction(models.PreferenceActionProposalEnrich, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusError, "", "Proposal query failed: "+err.Error(), started)
		return nil
	}
	if len(pending) == 0 {
		return nil
	}
	scorer := newProposalScorer(r.db, r.run.TenantID, r.policy, !r.observe)
	out := make([]proposalScore, 0, len(pending))
	for i := range pending {
		started := time.Now()
		sc := scorer.score(&pending[i])
		out = append(out, sc)
		status := models.PreferenceActionStatusSuccess
		if r.observe {
			status = models.PreferenceActionStatusWouldTrigger
		}
		r.writeAction(models.PreferenceActionProposalEnrich, models.PreferenceSubjectProposal,
			fmt.Sprintf("%d", pending[i].ID), status, "", sc.Reason, started)
	}
	return out
}

func (r *preferenceAutopilotRunner) actionMapSweep() {
	started := time.Now()
	res, err := limitedMapSweep(r.db, r.run.TenantID, r.policy.MaxItemCandidates, r.policy.MaxStoryCandidates,
		r.policy.ItemMapCursor, r.policy.StoryMapCursor)
	if err != nil {
		r.writeAction(models.PreferenceActionMapSweep, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusError, "", "Mapping sweep failed: "+err.Error(), started)
		return
	}
	if res.EmptyCatalog {
		r.writeAction(models.PreferenceActionMapSweep, models.PreferenceSubjectAggregate, "",
			models.PreferenceActionStatusSkipped, models.PreferenceGuardEmptyCatalog, "No active centroids.", started)
		return
	}
	// Persist advanced cursors (checkpoint) — derived write, never sets needs_remap.
	if err := r.db.Model(&models.PreferenceAutopilotPolicy{}).Where("tenant_id = ?", r.run.TenantID).
		Updates(map[string]interface{}{"item_map_cursor": res.NextItemCursor, "story_map_cursor": res.NextStoryCursor}).Error; err != nil {
		r.writeAction(models.PreferenceActionMapSweep, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusError, "", "Mapping checkpoint failed: "+err.Error(), started)
		return
	}
	r.writeAction(models.PreferenceActionMapSweep, models.PreferenceSubjectAggregate, "",
		models.PreferenceActionStatusSuccess, "",
		fmt.Sprintf("Mapped %d/%d items, %d/%d stories.", res.ItemsMapped, res.ItemsExamined, res.StoriesMapped, res.StoriesExamined), started)
}

func (r *preferenceAutopilotRunner) actionDirtySweep() {
	started := time.Now()
	res, err := sweepDirtyTopics(r.db, r.run.TenantID, r.policy.MaxDirtyTopics,
		r.policy.MaxItemCandidates, r.policy.MaxStoryCandidates, r.policy.DirtyItemCursor, r.policy.DirtyStoryCursor)
	if err != nil {
		r.writeAction(models.PreferenceActionDirtySweep, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusError, "", "Dirty sweep failed: "+err.Error(), started)
		return
	}
	if err := r.db.Model(&models.PreferenceAutopilotPolicy{}).Where("tenant_id = ?", r.run.TenantID).
		Updates(map[string]interface{}{"dirty_item_cursor": res.NextItemCursor, "dirty_story_cursor": res.NextStoryCursor}).Error; err != nil {
		r.writeAction(models.PreferenceActionDirtySweep, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusError, "", "Dirty checkpoint failed: "+err.Error(), started)
		return
	}
	if res.TopicsProcessed == 0 && res.ItemsExamined == 0 && res.StoriesExamined == 0 {
		return // nothing dirty — no ledger noise
	}
	r.writeAction(models.PreferenceActionDirtySweep, models.PreferenceSubjectAggregate, strings.Join(res.ClearedSlugs, ","),
		models.PreferenceActionStatusSuccess, "",
		fmt.Sprintf("Re-mapped %d dirty topic(s) over %d items / %d stories (cycle complete: %t).", res.TopicsProcessed, res.ItemsExamined, res.StoriesExamined, res.CycleComplete), started)
}

func (r *preferenceAutopilotRunner) actionCentroidRefresh() {
	started := time.Now()
	if r.classBreakerTripped(models.PreferenceActionCentroidRefresh) {
		r.writeAction(models.PreferenceActionCentroidRefresh, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusSkipped, models.PreferenceGuardClassBreaker, "Previous centroid recovery failed; quarantined for one scheduled run.", started)
		return
	}
	recovered, err := refreshNullCentroidsBounded(r.db, r.run.TenantID, r.policy.MaxCentroidRefresh)
	if err != nil {
		r.writeAction(models.PreferenceActionCentroidRefresh, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusError, models.PreferenceGuardEnrichmentDown, err.Error(), started)
		return
	}
	if recovered == 0 {
		return
	}
	r.writeAction(models.PreferenceActionCentroidRefresh, models.PreferenceSubjectAggregate, "",
		models.PreferenceActionStatusSuccess, "",
		fmt.Sprintf("Recovered %d NULL centroid(s) from labels; queued dirty for mapping.", recovered), started)
}

func (r *preferenceAutopilotRunner) actionMemberRefresh() {
	started := time.Now()
	if err := refreshTopicMemberCounts(r.db, r.run.TenantID); err != nil {
		r.writeAction(models.PreferenceActionMemberRefresh, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusError, "", "Member refresh failed: "+err.Error(), started)
		return
	}
	r.writeAction(models.PreferenceActionMemberRefresh, models.PreferenceSubjectAggregate, "",
		models.PreferenceActionStatusSuccess, "", "Refreshed topic member counts from mapping rows.", started)
}

// actionRecompute drains the durable recompute queue first (§8.5, §10), then
// recently-active authed users, capped per run.
func (r *preferenceAutopilotRunner) actionRecompute() {
	started := time.Now()
	cfg := loadPreferenceSettings(r.db, r.run.TenantID)
	budget := r.policy.MaxUsersRecompute
	done, queueErrors := r.drainRecomputeQueue(cfg, budget)
	remaining := budget - done
	activeDone, activeErrors := 0, 0
	if remaining > 0 {
		activeDone, activeErrors = r.recomputeRecentlyActive(cfg, remaining)
	}
	total := done + activeDone
	totalErrors := queueErrors + activeErrors
	if total == 0 && totalErrors == 0 {
		return
	}
	status := models.PreferenceActionStatusSuccess
	if totalErrors > 0 {
		status = models.PreferenceActionStatusError
	}
	r.writeAction(models.PreferenceActionRecompute, models.PreferenceSubjectAggregate, "",
		status, "",
		fmt.Sprintf("Recomputed affinity for %d user(s): %d queued, %d recently-active (%d errors).", total, done, activeDone, totalErrors), started)
}

func (r *preferenceAutopilotRunner) drainRecomputeQueue(cfg models.PreferenceSettings, budget int) (int, int) {
	return drainAffinityRecomputeQueue(r.db, r.run.TenantID, cfg, budget)
}

// drainAffinityRecomputeQueue drains up to `budget` queued users, recomputing each
// through the pure affinity function and clearing the row on success (§10). Shared
// by the safe_auto runner and the incumbent baseline so a catalog merge's queued
// repairs run in EVERY mode, not only safe_auto. Returns (done, errCount).
func drainAffinityRecomputeQueue(db *gorm.DB, tenantID string, cfg models.PreferenceSettings, budget int) (int, int) {
	if budget <= 0 {
		return 0, 0
	}
	var queued []models.PreferenceAffinityRecomputeQueue
	if err := db.Where("tenant_id = ?", tenantID).Order("updated_at ASC").Limit(budget).Find(&queued).Error; err != nil {
		return 0, 1
	}
	done, errCount := 0, 0
	for _, q := range queued {
		if err := recomputeUserAffinityCfg(db, q.UserID, tenantID, cfg); err != nil {
			errCount++
			if updateErr := db.Model(&models.PreferenceAffinityRecomputeQueue{}).
				Where("tenant_id = ? AND user_id = ?", tenantID, q.UserID).
				Updates(map[string]interface{}{"attempts": gorm.Expr("attempts + 1"), "last_error": err.Error(), "updated_at": time.Now()}).Error; updateErr != nil {
				errCount++
			}
			continue
		}
		if err := db.Where("tenant_id = ? AND user_id = ?", tenantID, q.UserID).Delete(&models.PreferenceAffinityRecomputeQueue{}).Error; err != nil {
			errCount++
			continue
		}
		done++
	}
	return done, errCount
}

func (r *preferenceAutopilotRunner) recomputeRecentlyActive(cfg models.PreferenceSettings, budget int) (int, int) {
	var since time.Time
	if r.policy.LastRunAt != nil {
		since = *r.policy.LastRunAt
	} else {
		since = time.Now().Add(-time.Hour)
	}
	var users []uuid.UUID
	if err := r.db.Model(&models.UserInteraction{}).
		Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
		Where("user_interactions.user_id IS NOT NULL AND user_interactions.created_at > ? AND content_items.tenant_id = ?", since, r.run.TenantID).
		Distinct("user_id").Limit(budget).Pluck("user_id", &users).Error; err != nil {
		return 0, 1
	}
	done, errCount := 0, 0
	for _, uid := range users {
		if err := recomputeUserAffinityCfg(r.db, uid, r.run.TenantID, cfg); err != nil {
			errCount++
			if enqueueAffinityRecompute(r.db, r.run.TenantID, uid, models.PreferenceRecomputeReasonFailed) != nil {
				errCount++
			}
			continue
		}
		done++
	}
	return done, errCount
}

// actionMine runs the daily capped mining with the pending-proposal backpressure
// ceiling (§9).
func (r *preferenceAutopilotRunner) actionMine(snap preferenceSnapshot) {
	started := time.Now()
	if r.policy.LastMineAt != nil && time.Since(*r.policy.LastMineAt) < 24*time.Hour {
		return // daily cadence
	}
	if snap.PendingProposals > int64(r.policy.MaxPendingProposals) {
		r.writeAction(models.PreferenceActionMine, models.PreferenceSubjectAggregate, "",
			models.PreferenceActionStatusSkipped, models.PreferenceGuardPendingCeiling,
			fmt.Sprintf("Pending proposals %d exceed ceiling %d — mining paused so the queue isn't buried.", snap.PendingProposals, r.policy.MaxPendingProposals), started)
		return
	}
	created, err := mineTopicProposalsCapped(r.db, r.run.TenantID, r.policy.MaxMinedProposals)
	if err != nil {
		r.writeAction(models.PreferenceActionMine, models.PreferenceSubjectAggregate, "",
			models.PreferenceActionStatusError, "", "Mining failed: "+err.Error(), started)
		return
	}
	now := time.Now()
	if err := r.db.Model(&models.PreferenceAutopilotPolicy{}).Where("tenant_id = ?", r.run.TenantID).
		Update("last_mine_at", now).Error; err != nil {
		r.writeAction(models.PreferenceActionMine, models.PreferenceSubjectAggregate, "", models.PreferenceActionStatusError, "", "Mining checkpoint failed: "+err.Error(), started)
		return
	}
	r.writeAction(models.PreferenceActionMine, models.PreferenceSubjectAggregate, "",
		models.PreferenceActionStatusSuccess, "", fmt.Sprintf("Mined %d new proposal(s).", created), started)
}

func (r *preferenceAutopilotRunner) actionMergeSuggest(snap preferenceSnapshot) {
	if snap.NearDuplicatePairs == 0 {
		return
	}
	started := time.Now()
	r.writeAction(models.PreferenceActionMergeSuggest, models.PreferenceSubjectAggregate, "",
		models.PreferenceActionStatusSuccess, "",
		fmt.Sprintf("%d near-duplicate topic pair(s) flagged for human merge review.", snap.NearDuplicatePairs), started)
}

// runBaselineAsAction executes the incumbent heartbeat maintenance once and
// ledgers it as baseline_success/baseline_error.
func (r *preferenceAutopilotRunner) runBaselineAsAction() {
	started := time.Now()
	if err := runPreferenceBaseline(r.db, r.run.TenantID, r.policy); err != nil {
		r.writeAction(models.PreferenceActionSnapshot, models.PreferenceSubjectAggregate, "baseline",
			models.PreferenceActionStatusBaselineError, "", "Baseline maintenance error: "+err.Error(), started)
		return
	}
	r.writeAction(models.PreferenceActionSnapshot, models.PreferenceSubjectAggregate, "baseline",
		models.PreferenceActionStatusBaselineSuccess, "", "Ran incumbent baseline maintenance (Observe preserves current behavior).", started)
}

// shadowBoundedActions records would_* rows for the bounded actions without
// mutating anything, so the ledger shows what safe_auto would do — including
// which proposals the earned auto-approve tier WOULD approve, so an observing
// admin sees exactly what flipping the switch would have done.
func (r *preferenceAutopilotRunner) shadowBoundedActions(snap preferenceSnapshot, scored []proposalScore) {
	now := time.Now()
	if r.policy.AutoApproveEnabled && len(scored) > 0 {
		trust := computePreferenceTrust(r.db, r.run.TenantID, r.policy, snap.MuteViolations)
		wouldApprove := 0
		for _, sc := range scored {
			ok, guardrail, reason := preferenceAutoApproveGate(r.policy, trust, sc, wouldApprove)
			if !ok {
				if guardrail == models.PreferenceGuardTrustGate {
					r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectAggregate, "",
						models.PreferenceActionStatusWouldSkip, guardrail, reason, now)
					break
				}
				if guardrail != "" {
					r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectProposal,
						fmt.Sprintf("%d", sc.ProposalID), models.PreferenceActionStatusWouldSkip, guardrail, reason, now)
				}
				if guardrail == models.PreferenceGuardRunCap {
					break
				}
				continue
			}
			wouldApprove++
			r.writeAction(models.PreferenceActionAutoApprove, models.PreferenceSubjectProposal,
				fmt.Sprintf("%d", sc.ProposalID), models.PreferenceActionStatusWouldTrigger, "",
				fmt.Sprintf("Would auto-approve %q (confidence %.2f, quarantined).", sc.Slug, sc.Confidence), now)
		}
	}
	r.writeAction(models.PreferenceActionMapSweep, models.PreferenceSubjectAggregate, "",
		models.PreferenceActionStatusWouldTrigger, "",
		fmt.Sprintf("Would sweep up to %d items / %d stories under caps.", r.policy.MaxItemCandidates, r.policy.MaxStoryCandidates), now)
	if snap.NullCentroidTopics > 0 {
		r.writeAction(models.PreferenceActionCentroidRefresh, models.PreferenceSubjectAggregate, "",
			models.PreferenceActionStatusWouldTrigger, "",
			fmt.Sprintf("Would recover up to %d NULL centroid(s) (%d present).", r.policy.MaxCentroidRefresh, snap.NullCentroidTopics), now)
	}
	if snap.NearDuplicatePairs > 0 {
		r.writeAction(models.PreferenceActionMergeSuggest, models.PreferenceSubjectAggregate, "",
			models.PreferenceActionStatusWouldTrigger, "",
			fmt.Sprintf("Would flag %d near-duplicate pair(s) for merge review.", snap.NearDuplicatePairs), now)
	}
}

func statusSkip(observe bool) string {
	if observe {
		return models.PreferenceActionStatusWouldSkip
	}
	return models.PreferenceActionStatusSkipped
}

func countHighConfidence(scored []proposalScore) int {
	n := 0
	for _, s := range scored {
		if s.Verdict == models.PreferenceVerdictHighConf {
			n++
		}
	}
	return n
}

// ----------------------------------------------------------------
// Baseline primitive (incumbent heartbeat, extracted — §0.1.9)
// ----------------------------------------------------------------

// runPreferenceBaseline is the SINGLE extraction of the old StartTopicsHeartbeat
// body: exactly one incremental remap + daily mine + recently-active recompute per
// call. disabled tenants (scheduler) and Observe (via runBaselineAsAction) run it;
// safe_auto never does. This guarantees exactly one maintenance path per tenant.
func runPreferenceBaseline(db *gorm.DB, tenantID string, policy models.PreferenceAutopilotPolicy) error {
	ensureDefaultTopicCategories(db, tenantID)
	hydrateMissingTopicCentroids(db, tenantID)
	if _, _, err := remapCatalogTopics(db, tenantID, false); err != nil {
		return err
	}
	// Reconcile human catalog edits. Deactivate/approve/label/category changes set
	// topics.needs_remap; pre-autopilot these were reconciled synchronously in the
	// admin handlers. Consuming the dirty flag here (bounded + checkpointed) restores
	// that behavior for disabled/observe tenants — otherwise a deactivated topic's
	// mappings would linger and keep boosting feeds until someone flips safe_auto.
	if dirtyRes, err := sweepDirtyTopics(db, tenantID, policy.MaxDirtyTopics,
		policy.MaxItemCandidates, policy.MaxStoryCandidates, policy.DirtyItemCursor, policy.DirtyStoryCursor); err == nil {
		_ = db.Model(&models.PreferenceAutopilotPolicy{}).Where("tenant_id = ?", tenantID).
			Updates(map[string]interface{}{"dirty_item_cursor": dirtyRes.NextItemCursor, "dirty_story_cursor": dirtyRes.NextStoryCursor}).Error
	}
	if policy.LastMineAt == nil || time.Since(*policy.LastMineAt) > 24*time.Hour {
		if _, err := mineTopicProposals(db, tenantID); err == nil {
			now := time.Now()
			_ = db.Model(&models.PreferenceAutopilotPolicy{}).Where("tenant_id = ?", tenantID).Update("last_mine_at", now).Error
		}
	}
	cfg := loadPreferenceSettings(db, tenantID)
	// Drain the durable recompute queue first (catalog merges enqueue affected users,
	// including inactive ones a merge could otherwise leave with stale affinity /
	// mute-integrity violations), then refresh recently-active users.
	drainAffinityRecomputeQueue(db, tenantID, cfg, policy.MaxUsersRecompute)
	var since time.Time
	if policy.LastRunAt != nil {
		since = *policy.LastRunAt
	} else {
		since = time.Now().Add(-time.Hour)
	}
	var users []uuid.UUID
	db.Model(&models.UserInteraction{}).
		Joins("JOIN content_items ON content_items.public_id = user_interactions.content_item_id").
		Where("user_interactions.user_id IS NOT NULL AND user_interactions.created_at > ? AND content_items.tenant_id = ?", since, tenantID).
		Distinct("user_id").Limit(500).Pluck("user_id", &users)
	for _, uid := range users {
		_ = recomputeUserAffinityCfg(db, uid, tenantID, cfg)
	}
	return nil
}

func touchPreferenceAutopilotLastRun(db *gorm.DB, tenantID string, at time.Time) {
	_ = db.Model(&models.PreferenceAutopilotPolicy{}).Where("tenant_id = ?", tenantID).
		Updates(map[string]interface{}{"last_run_at": at, "updated_at": at}).Error
}

func firstNonEmptyString(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// ----------------------------------------------------------------
// Verdict (§7)
// ----------------------------------------------------------------

func failureBreakerTripped(attempts, failures, thresholdPct int) bool {
	return attempts >= 4 && float64(failures)*100/float64(max1(attempts)) >= float64(thresholdPct)
}

func computePreferenceVerdict(snap preferenceSnapshot, scored []proposalScore, runner *preferenceAutopilotRunner, policy models.PreferenceAutopilotPolicy) (headline, summary, recommended string) {
	breaker := failureBreakerTripped(runner.attempts, runner.failures, policy.FailureBreakerPct)

	// The persisted predicted_verdict count (snap.HighConfidencePending) is authoritative
	// in safe_auto but always 0 in observe (which never persists). Fall back to the
	// just-computed shadow scores so an observing admin sees the real high-confidence
	// count when deciding whether to promote to safe_auto.
	if c := countHighConfidence(scored); c > snap.HighConfidencePending {
		snap.HighConfidencePending = c
	}

	foryou := snap.FlipGates["foryou_enabled"]
	news := snap.FlipGates["news_enabled"]
	coverageGap := (foryou.Enabled && foryou.CoveragePct < float64(foryou.FloorPct)) ||
		(news.Enabled && news.CoveragePct < float64(news.FloorPct))
	flipEligible := (!foryou.Enabled && foryou.State == "green") || (!news.Enabled && news.State == "green")
	backlog := snap.PendingProposals > int64(policy.MaxPendingProposals) || snap.OldestPendingAgeHours > 168

	switch {
	case breaker || (runner.errored > 0 && runner.success == 0):
		headline = models.PreferenceAutopilotHeadlineDegraded
		recommended = "Investigate run failures, then Run now after remediation."
	case snap.MuteViolations > 0 || snap.NullCentroidTopics > 0:
		headline = models.PreferenceAutopilotHeadlineIntegrityAlert
		recommended = integrityRecommendation(snap)
	case backlog:
		headline = models.PreferenceAutopilotHeadlineBacklog
		recommended = fmt.Sprintf("Review the proposal queue — %d pending, oldest %.0fh.", snap.PendingProposals, snap.OldestPendingAgeHours)
	case coverageGap:
		headline = models.PreferenceAutopilotHeadlineCoverageGap
		recommended = "Coverage below floor on an enabled surface — let mapping catch up before relying on boosts."
	case snap.PendingProposals > 0:
		headline = models.PreferenceAutopilotHeadlineReviewReady
		recommended = fmt.Sprintf("Review %d ranked proposal(s) (%d high-confidence).", snap.PendingProposals, snap.HighConfidencePending)
	case flipEligible:
		headline = models.PreferenceAutopilotHeadlineFlipEligible
		recommended = flipRecommendation(foryou, news)
	default:
		headline = models.PreferenceAutopilotHeadlineCurationCurrent
		recommended = "Nothing needed — catalog current, coverage healthy."
	}

	summary = fmt.Sprintf("For You coverage %.0f%% (%s), News %.0f%% (%s); %d pending (%d high-conf); %d dead, %d null-centroid, %d dup pairs.",
		snap.ForyouCoveragePct, foryou.State, snap.NewsCoveragePct, news.State,
		snap.PendingProposals, snap.HighConfidencePending, snap.DeadTopics, snap.NullCentroidTopics, snap.NearDuplicatePairs)
	return headline, summary, recommended
}

func integrityRecommendation(snap preferenceSnapshot) string {
	switch {
	case snap.MuteViolations > 0:
		return fmt.Sprintf("Mute-integrity violation: %d muted topic(s) still in affinity — recompute affected users.", snap.MuteViolations)
	default:
		return fmt.Sprintf("%d approved topic(s) have NULL centroids and map nothing — the next run recovers them from labels.", snap.NullCentroidTopics)
	}
}

func flipRecommendation(foryou, news preferenceFlipGate) string {
	if !foryou.Enabled && foryou.State == "green" {
		return "Flip foryou_enabled — mapping coverage clears the floor."
	}
	if !news.Enabled && news.State == "green" {
		return "Flip news_enabled — mapping coverage clears the floor."
	}
	return "A feed switch is eligible to flip."
}

func max1(v int) int {
	if v < 1 {
		return 1
	}
	return v
}
