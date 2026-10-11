package controllers

import (
	"content-management-system/src/models"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"sort"
	"strings"
	"time"
)

type integrityCurrentObservation struct {
	integrityObservation
	Suppressed bool                  `json:"suppressed"`
	Stale      bool                  `json:"stale"`
	LastKnown  *integrityObservation `json:"last_known,omitempty"`
	ExpectedAt *time.Time            `json:"expected_at,omitempty"`
}

func readIntegrityStatus(db *gorm.DB, tenant string) (gin.H, error) {
	policy, err := readFeedIntegrityPolicy(db, tenant)
	if err != nil {
		return nil, err
	}
	var runs []models.FeedIntegrityRun
	for _, tier := range []string{"light", "deep"} {
		var history []models.FeedIntegrityRun
		if err := db.Where("tenant_id=? AND tier=?", tenant, tier).Order("started_at DESC").Limit(40).Find(&history).Error; err != nil {
			return nil, err
		}
		runs = append(runs, history...)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].StartedAt.After(runs[j].StartedAt) })
	var episodes []models.FeedIntegrityEpisode
	if err := db.Where("tenant_id=? AND status IN ?", tenant, []string{"open", "recovering"}).Order("last_seen_at DESC").Find(&episodes).Error; err != nil {
		return nil, err
	}
	var suppressions []models.FeedIntegritySuppression
	if err := db.Where("tenant_id=? AND revoked_at IS NULL AND expires_at>?", tenant, time.Now()).Find(&suppressions).Error; err != nil {
		return nil, err
	}
	expected := newIntegrityCollector(models.FeedIntegrityRun{Tier: "deep"}, nil, nil)
	current := []integrityCurrentObservation{}
	scored := []integrityObservation{}
	now := time.Now().UTC()
	for _, seed := range expected.observations {
		row := integrityCurrentObservation{integrityObservation: *seed}
		found := false
		for _, run := range runs {
			if feedIntegrityCheckLane(seed.CheckKey) == "probe" && run.Tier != "deep" {
				continue
			}
			if strings.HasPrefix(seed.Variant, "window:") && seed.Variant != "window:today" && run.Tier != "deep" {
				continue
			}
			o, ok := integrityObservationFor(run, seed.CheckKey, seed.Feed, seed.Variant)
			ok = ok && (run.Status == models.FeedIntegrityRunCompleted || run.Status == models.FeedIntegrityRunPartial)
			if !found {
				found = true
				if ok {
					row.integrityObservation = o
				} else {
					row.Outcome = "check_error"
					row.ObservedAt = run.StartedAt
				}
			}
			if ok && (o.Outcome == "pass" || o.Outcome == "violation" || o.Outcome == "skipped:not_applicable") {
				copy := o
				row.LastKnown = &copy
				break
			}
		}
		if !found {
			row.Outcome = "check_error"
			row.ObservedAt = time.Time{}
		}
		interval := time.Duration(policy.LightIntervalMinutes) * time.Minute
		if feedIntegrityCheckLane(seed.CheckKey) == "probe" || seed.Variant == "window:week" || seed.Variant == "window:month" {
			interval = time.Duration(policy.DeepIntervalHours) * time.Hour
		}
		if !row.ObservedAt.IsZero() && policy.ScheduledEnabled && (policy.PausedUntil == nil || !policy.PausedUntil.After(now)) {
			next := row.ObservedAt.Add(interval)
			row.ExpectedAt = &next
			row.Stale = now.After(next)
			if row.Stale {
				row.Outcome = "check_error"
			}
		}
		// A different deep sample cannot overwrite unresolved target evidence.
		for _, ep := range episodes {
			if ep.CheckKey != seed.CheckKey || ep.Feed != seed.Feed || ep.Variant != seed.Variant {
				continue
			}
			if integrityScopeOutcome(row.integrityObservation, ep.Scope) == "pass" {
				continue
			}
			for _, run := range runs {
				if run.Status != models.FeedIntegrityRunCompleted && run.Status != models.FeedIntegrityRunPartial {
					continue
				}
				o, ok := integrityObservationFor(run, ep.CheckKey, ep.Feed, ep.Variant)
				if ok && integrityScopeOutcome(o, ep.Scope) == "violation" {
					if row.Outcome != "check_error" && !strings.HasPrefix(row.Outcome, "skipped:") {
						row.integrityObservation = o
					}
					break
				}
			}
		}
		row.Targets = append([]integrityTargetOutcome(nil), row.Targets...)
		for _, s := range suppressions {
			if s.CheckKey != row.CheckKey || (s.Feed != "" && s.Feed != row.Feed) || (s.Variant != "" && s.Variant != row.Variant) || s.StartsAt.After(now) {
				continue
			}
			if s.Scope == "" {
				row.Suppressed = true
				row.Outcome = "skipped:suppressed"
				row.AffectedCount = 0
				row.SampleCount = 0
				row.CandidateCount = 0
				break
			}
			for i, target := range row.Targets {
				if target.Scope != s.Scope || target.Outcome == "skipped:suppressed" {
					continue
				}
				row.Suppressed = true
				if target.Outcome == "violation" {
					row.AffectedCount = max(0, row.AffectedCount-1)
				}
				if target.Outcome == "pass" || target.Outcome == "violation" || target.Outcome == "check_error" {
					row.SampleCount = max(0, row.SampleCount-1)
				}
				row.CandidateCount = max(0, row.CandidateCount-1)
				row.Targets[i].Outcome = "skipped:suppressed"
			}
			if row.Suppressed && row.AffectedCount == 0 {
				row.Outcome = "skipped:suppressed"
			}
		}
		current = append(current, row)
		scored = append(scored, row.integrityObservation)
	}
	sort.Slice(current, func(i, j int) bool {
		return current[i].CheckKey+current[i].Variant < current[j].CheckKey+current[j].Variant
	})
	results := integrityResults(scored)
	headline := "all_clear"
	for _, feed := range []string{"pods", "news"} {
		r := results[feed]
		if r.ConsumerVerdict == "inconclusive" || r.ReadinessVerdict == "inconclusive" {
			headline = "inconclusive"
		} else if (r.ConsumerVerdict == "not_ready" || r.ReadinessVerdict == "not_ready") && headline == "all_clear" {
			headline = "not_ready"
		} else if (r.ConsumerVerdict != "healthy" || r.ReadinessVerdict != "healthy") && headline == "all_clear" {
			headline = "watching"
		}
	}
	if len(episodes) > 0 {
		headline = "attention"
	}
	posture := "scheduled"
	if !policy.ScheduledEnabled {
		posture = "manual_only"
	} else if policy.PausedUntil != nil && policy.PausedUntil.After(now) {
		posture = "paused"
	}
	var latest interface{}
	if len(runs) > 0 {
		latest = runs[0]
	}
	return gin.H{"scope": "CMS edge", "headline": headline, "posture": posture, "policy": policy, "latest_run": latest, "current_feed_results": results, "current_observations": current, "open_episodes": episodes, "active_suppressions": suppressions, "checks": feedIntegrityCheckCatalog()}, nil
}
func integrityReadError(c *gin.Context, err error) {
	c.JSON(500, gin.H{"error": "Feed Integrity evidence unavailable", "detail": fmt.Sprintf("%T", err)})
}
