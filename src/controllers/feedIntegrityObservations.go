package controllers

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"content-management-system/src/models"
	"gorm.io/datatypes"
)

const feedIntegrityObservationVersion = 1

type integrityTargetOutcome struct {
	Scope   string `json:"scope"`
	Role    string `json:"role,omitempty"`
	Outcome string `json:"outcome"`
}
type integrityObservation struct {
	Version        int                      `json:"version"`
	CheckKey       string                   `json:"check_key"`
	Feed           string                   `json:"feed"`
	Variant        string                   `json:"variant"`
	Axis           string                   `json:"axis"`
	Severity       string                   `json:"severity"`
	Outcome        string                   `json:"outcome"`
	Reason         string                   `json:"reason,omitempty"`
	CandidateCount int                      `json:"candidate_count"`
	SampleCount    int                      `json:"sample_count"`
	AffectedCount  int                      `json:"affected_count"`
	ObservedAt     time.Time                `json:"observed_at"`
	Coverage       string                   `json:"coverage"`
	Targets        []integrityTargetOutcome `json:"targets,omitempty"`
}
type integrityLane struct {
	Executed     int                    `json:"executed"`
	Observations []integrityObservation `json:"observations"`
}
type integrityCollector struct {
	mu           sync.Mutex
	run          models.FeedIntegrityRun
	observations map[string]*integrityObservation
	targets      map[string]map[string]string
	findings     []models.FeedIntegrityFinding
	suppressions []models.FeedIntegritySuppression
	openScopes   map[string]bool
	examples     map[string]int
}

func newIntegrityCollector(run models.FeedIntegrityRun, suppressions []models.FeedIntegritySuppression, episodes []models.FeedIntegrityEpisode) *integrityCollector {
	c := &integrityCollector{run: run, observations: map[string]*integrityObservation{}, targets: map[string]map[string]string{}, suppressions: suppressions, openScopes: map[string]bool{}}
	for _, ep := range episodes {
		c.openScopes[ep.CheckKey+"|"+ep.Feed+"|"+ep.Variant+"|"+ep.Scope] = true
	}
	for _, check := range feedIntegrityChecks {
		if check.Lane == "checker" || (check.Lane == "probe" && run.Tier != models.FeedIntegrityTierDeep) {
			continue
		}
		variants := []string{"default"}
		if check.Feed == "news" {
			variants = []string{"today"}
		}
		if check.Key == "inv_news_cache_rebuild_debt" {
			variants = []string{"today", "week", "month"}
		}
		if check.Lane == "edge" && check.Feed == "pods" {
			variants = []string{"default", "duration:5m", "duration:10m", "duration:15m", "duration:20m", "duration:30m", "duration:40m"}
		}
		if check.Lane == "edge" && check.Feed == "news" {
			variants = []string{"window:today"}
			if run.Tier == models.FeedIntegrityTierDeep {
				variants = append(variants, "window:week", "window:month")
			}
		}
		for _, v := range variants {
			o := &integrityObservation{Version: feedIntegrityObservationVersion, CheckKey: check.Key, Feed: check.Feed, Variant: v, Axis: check.Axis, Severity: check.Severity, Outcome: "skipped:not_evaluated", Coverage: "sampled", ObservedAt: run.StartedAt}
			if check.Lane == "inventory" {
				o.Coverage = "exhaustive"
			}
			c.observations[check.Key+"|"+check.Feed+"|"+v] = o
		}
	}
	return c
}
func integrityOutcome(status string) string {
	if status == "ok" {
		return "pass"
	}
	return status
}
func (c *integrityCollector) add(key, lane, feed, variant, axis, severity, status, targetType, target string, candidates int, evidence interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Compatibility proof rows are deliberately outside scored observations.
	if key == "edge_pods_page_success" || key == "edge_news_page_success" || key == "probe_url_success" {
		raw, _ := json.Marshal(evidence)
		c.findings = append(c.findings, models.FeedIntegrityFinding{RunID: c.run.ID, TenantID: c.run.TenantID, CheckKey: key, Lane: lane, Feed: feed, Variant: variant, Axis: axis, Severity: severity, Status: status, TargetType: targetType, TargetRef: target, CandidateCount: candidates, SampleCount: candidates, AffectedCount: 0, Evidence: datatypes.JSON(raw)})
		return
	}
	id := key + "|" + feed + "|" + variant
	o := c.observations[id]
	if o == nil {
		o = &integrityObservation{Version: 1, CheckKey: key, Feed: feed, Variant: variant, Axis: axis, Severity: severity, Outcome: "skipped:not_evaluated", Coverage: "sampled"}
		if lane == "inventory" {
			o.Coverage = "exhaustive"
		}
		c.observations[id] = o
	}
	f := models.FeedIntegrityFinding{RunID: c.run.ID, TenantID: c.run.TenantID, CheckKey: key, Lane: lane, Feed: feed, Variant: variant, Axis: axis, Severity: severity, Status: status, TargetType: targetType, TargetRef: target, CandidateCount: candidates, SampleCount: candidates}
	scope := feedIntegrityScope(f)
	if targetType == "probe" {
		scope = target
	}
	if integritySuppressed(c.suppressions, f, scope, time.Now().UTC()) {
		status = "skipped:suppressed"
		f.Status = status
	}
	outcome := integrityOutcome(status)
	if detail, ok := evidence.(map[string]interface{}); ok {
		if reason, ok := detail["reason"].(string); ok && (status == "check_error" || status == "violation") {
			o.Reason = reason
		}
	}
	if c.targets[id] == nil {
		c.targets[id] = map[string]string{}
	}
	prior, exists := c.targets[id][scope]
	if exists && prior == outcome {
		return
	}
	if !exists {
		c.targets[id][scope] = outcome
		o.CandidateCount += candidates
		if outcome == "pass" || outcome == "violation" || outcome == "check_error" {
			o.SampleCount += candidates
		}
	} else {
		c.targets[id][scope] = outcome
		if prior == "violation" {
			o.AffectedCount -= candidates
		}
	}
	if outcome == "violation" {
		o.AffectedCount += candidates
		f.AffectedCount = candidates
	}
	o.ObservedAt = time.Now().UTC()
	// Required unknowns win over a pass, while preserving violation targets.
	o.Outcome = "pass"
	rank := func(v string) int {
		if v == "check_error" {
			return 5
		}
		if strings.HasPrefix(v, "skipped:") && v != "skipped:not_applicable" {
			return 4
		}
		if v == "violation" {
			return 3
		}
		if v == "not_ready" {
			return 2
		}
		return 1
	}
	for _, v := range c.targets[id] {
		if rank(v) > rank(o.Outcome) || (rank(v) == rank(o.Outcome) && v < o.Outcome) {
			o.Outcome = v
		}
	}
	if len(c.targets[id]) == 1 && outcome == "skipped:not_applicable" {
		o.Outcome = outcome
	}
	if lane != "inventory" || c.openScopes[id+"|"+scope] || (outcome != "pass" && len(o.Targets) < 25) {
		// Inventory pass coverage is implicit only for an exhaustive zero-failure
		// predicate; open target passes are retained explicitly.
		found := false
		for i := range o.Targets {
			if o.Targets[i].Scope == scope {
				o.Targets[i].Outcome = outcome
				found = true
				break
			}
		}
		if !found {
			role := ""
			if targetType == "probe" {
				role = scope[strings.LastIndex(scope, ":")+1:]
			}
			o.Targets = append(o.Targets, integrityTargetOutcome{Scope: scope, Role: role, Outcome: outcome})
		}
	}
	if outcome != "pass" && outcome != "skipped:not_applicable" {
		if c.examples == nil {
			c.examples = map[string]int{}
		}
		if outcome == "violation" && c.examples[id] >= 25 && !c.openScopes[id+"|"+scope] {
			return
		}
		if outcome == "violation" {
			c.examples[id]++
		}
		raw, _ := json.Marshal(evidence)
		f.Evidence = datatypes.JSON(raw)
		c.findings = append(c.findings, f)
	}
}
func integritySuppressed(rows []models.FeedIntegritySuppression, f models.FeedIntegrityFinding, scope string, now time.Time) bool {
	for _, s := range rows {
		if s.CheckKey == f.CheckKey && s.RevokedAt == nil && !s.StartsAt.After(now) && s.ExpiresAt.After(now) && (s.Feed == "" || s.Feed == f.Feed) && (s.Variant == "" || s.Variant == f.Variant) && (s.Scope == "" || s.Scope == scope) {
			return true
		}
	}
	return false
}
func (c *integrityCollector) lanes() map[string]integrityLane {
	lanes := map[string]integrityLane{"inventory": {Executed: 1}, "edge": {Executed: 1}, "probe": {Executed: 0}}
	if c.run.Tier == models.FeedIntegrityTierDeep {
		lanes["probe"] = integrityLane{Executed: 1}
	}
	keys := make([]string, 0, len(c.observations))
	for key := range c.observations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		o := c.observations[key]
		lane := feedIntegrityCheckLane(o.CheckKey)
		if lane == "checker" {
			lane = "edge"
		}
		v := lanes[lane]
		v.Observations = append(v.Observations, *o)
		lanes[lane] = v
	}
	return lanes
}
func integrityRunObservations(run models.FeedIntegrityRun) []integrityObservation {
	var lanes map[string]integrityLane
	if json.Unmarshal(run.LaneResults, &lanes) != nil {
		return nil
	}
	var result []integrityObservation
	for _, lane := range []string{"inventory", "edge", "probe"} {
		for _, o := range lanes[lane].Observations {
			if o.Version == 1 {
				result = append(result, o)
			}
		}
	}
	return result
}
func integrityScopeOutcome(o integrityObservation, scope string) string {
	for _, t := range o.Targets {
		if t.Scope == scope {
			return t.Outcome
		}
	}
	if o.Coverage == "exhaustive" && o.Outcome == "pass" && o.AffectedCount == 0 {
		return "pass"
	}
	return "skipped:unsampled"
}
func integrityObservationFor(run models.FeedIntegrityRun, key, feed, variant string) (integrityObservation, bool) {
	for _, o := range integrityRunObservations(run) {
		if o.CheckKey == key && o.Feed == feed && o.Variant == variant {
			return o, true
		}
	}
	return integrityObservation{}, false
}
func integrityResults(observations []integrityObservation) map[string]*integrityResult {
	result := map[string]*integrityResult{}
	for _, feed := range []string{"pods", "news"} {
		result[feed] = integrityResultFor(feed, "all", observations)
	}
	for _, o := range observations {
		id := o.Feed + ":" + o.Variant
		if result[id] == nil {
			result[id] = integrityResultFor(o.Feed, o.Variant, observations)
		}
	}
	return result
}
func integrityResultFor(feed, variant string, observations []integrityObservation) *integrityResult {
	r := &integrityResult{Feed: feed, Variant: variant}
	for _, axis := range []string{models.FeedIntegrityAxisConsumer, models.FeedIntegrityAxisReadiness} {
		score, weight := 0.0, 0.0
		verdict := "healthy"
		unknown, notReady, critical := false, false, false
		for _, o := range observations {
			if o.Feed != feed || o.Axis != axis || (variant != "all" && o.Variant != variant) {
				continue
			}
			denom := o.SampleCount
			if o.Coverage == "exhaustive" {
				denom = o.CandidateCount
			}
			r.Checked += denom
			r.Violations += o.AffectedCount
			if o.Severity == "info" || o.Outcome == "skipped:not_applicable" {
				continue
			}
			if o.Outcome == "not_ready" {
				notReady = true
				continue
			}
			if o.Outcome == "check_error" || strings.HasPrefix(o.Outcome, "skipped:") {
				unknown = true
				continue
			}
			if denom == 0 {
				notReady = true
				continue
			}
			w := scorePenalty(o.Severity)
			weight += w
			score += w * (1 - float64(o.AffectedCount)/float64(denom)) * 100
			if o.AffectedCount > 0 {
				verdict = worsenIntegrityVerdict(verdict, o.Severity)
				if o.Severity == "critical" || (o.CheckKey == "probe_url_dead" && o.AffectedCount*2 > denom) {
					critical = true
				}
			}
		}
		if weight > 0 {
			score /= weight
		}
		if critical {
			score = 0
			verdict = "broken"
		} else if unknown {
			verdict = "inconclusive"
		} else if notReady || weight == 0 {
			verdict = "not_ready"
		}
		if axis == models.FeedIntegrityAxisConsumer {
			r.ConsumerScore = score
			r.ConsumerVerdict = verdict
		} else {
			r.ReadinessScore = score
			r.ReadinessVerdict = verdict
		}
	}
	return r
}
func (r integrityResult) MarshalJSON() ([]byte, error) {
	type plain integrityResult
	raw, _ := json.Marshal(plain(r))
	var v map[string]interface{}
	_ = json.Unmarshal(raw, &v)
	if r.ConsumerVerdict == "inconclusive" || r.ConsumerVerdict == "not_ready" {
		v["consumer_score"] = nil
	}
	if r.ReadinessVerdict == "inconclusive" || r.ReadinessVerdict == "not_ready" {
		v["readiness_score"] = nil
	}
	return json.Marshal(v)
}

func (c *integrityCollector) checkerObservations() {
	original := make([]integrityObservation, 0, len(c.observations))
	for _, o := range c.observations {
		original = append(original, *o)
	}
	for _, o := range original {
		if o.Severity == "info" {
			continue
		}
		status := "ok"
		if o.Outcome == "check_error" {
			status = "violation"
		} else if strings.HasPrefix(o.Outcome, "skipped:") || o.Outcome == "not_ready" {
			status = o.Outcome
		}
		scope := "check:" + o.CheckKey + ":" + o.Feed + ":" + o.Variant
		c.add("checker_unhealthy", "edge", "platform", "cms-edge", "readiness", "major", status, "checker", scope, 1, map[string]interface{}{"failed_check": o.CheckKey, "feed": o.Feed, "variant": o.Variant})
	}
}

func (c *integrityCollector) aggregateFindings() {
	for _, o := range c.observations {
		lane := feedIntegrityCheckLane(o.CheckKey)
		if lane == "checker" {
			lane = "edge"
		}
		status := o.Outcome
		c.findings = append(c.findings, models.FeedIntegrityFinding{RunID: c.run.ID, TenantID: c.run.TenantID, CheckKey: o.CheckKey, Lane: lane, Feed: o.Feed, Variant: o.Variant, Axis: o.Axis, Severity: o.Severity, Status: "summary", TargetType: "aggregate", TargetRef: o.CheckKey, CandidateCount: o.CandidateCount, SampleCount: o.SampleCount, AffectedCount: o.AffectedCount, Evidence: integrityJSON(map[string]interface{}{"observation_version": 1, "outcome": status, "coverage": o.Coverage, "candidate_count": o.CandidateCount, "sample_count": o.SampleCount, "affected_count": o.AffectedCount})})
	}
}
