package controllers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"content-management-system/src/intelligence"
	"content-management-system/src/models"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Several established pure owner helpers return a value and deliberately keep
// database errors out of their cockpit contract. Capture those read errors at
// this composition boundary instead of duplicating their domain SQL or treating
// a failed aggregate as zero. This recorder is local to one member adapter.
type opsOwnerReadRecorder struct {
	logger.Interface
	mu  sync.Mutex
	err error
}

func (r *opsOwnerReadRecorder) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		r.mu.Lock()
		if r.err == nil {
			r.err = err
		}
		r.mu.Unlock()
	}
	r.Interface.Trace(ctx, begin, fc, err)
}
func (r *opsOwnerReadRecorder) readError() error { r.mu.Lock(); defer r.mu.Unlock(); return r.err }

func opsTrustForMember(db *gorm.DB, m opsFleetMember, now time.Time) (map[string]opsTrustSummary, []opsAttentionItem, error) {
	summaries := map[string]opsTrustSummary{}
	items := []opsAttentionItem{}
	recorder := &opsOwnerReadRecorder{Interface: db.Logger}
	read := db.Session(&gorm.Session{Logger: recorder})
	add := func(tenant, class, state string, at time.Time, evidence string) {
		s := summaries[tenant]
		s.Total++
		if state == "trusted" {
			s.Earned++
		}
		if state == "demoted" {
			s.State = "demoted"
			items = append(items, opsAttentionItem{Key: m.Key + ":attention:" + tenant + ":trust:" + class, Fingerprint: opsFingerprint(tenant, class, state, evidence), System: m.Key, TenantID: tenant, Kind: "attention", Severity: "major", Title: m.Label + " class is demoted", Detail: class, Count: 1, FirstSeen: at, Href: m.CockpitPath})
		}
		s.Detail = "Earned trust from the owner's recorded decisions; execution eligibility is separate"
		summaries[tenant] = s
	}
	// No create/ensure/status handler is invoked here. These helpers only read
	// ledgers and apply their own pure trust predicates.
	switch m.Key {
	case "pipeline":
		var policies []models.PipelineAutopilotPolicy
		if err := read.Limit(501).Find(&policies).Error; err != nil {
			return summaries, items, err
		}
		if len(policies) > 500 {
			return summaries, items, fmt.Errorf("trust scope exceeds capacity")
		}
		for _, p := range policies {
			for class, stat := range computePipelineTrust(read, p.TenantID, p) {
				add(p.TenantID, class, stat.State, p.CreatedAt, fmt.Sprint(stat.Outcomes, stat.Failed))
			}
		}
	case "enrichment":
		var policies []models.EnrichmentAutopilotPolicy
		if err := read.Limit(501).Find(&policies).Error; err != nil {
			return summaries, items, err
		}
		if len(policies) > 500 {
			return summaries, items, fmt.Errorf("trust scope exceeds capacity")
		}
		for _, p := range policies {
			for class, stat := range computeEnrichmentTrust(read, p.TenantID, p) {
				add(p.TenantID, class, stat.State, p.CreatedAt, fmt.Sprint(stat.Attempts, stat.Failures))
			}
		}
	case "media_circulation":
		var policies []models.MediaCirculationPolicy
		if err := read.Limit(501).Find(&policies).Error; err != nil {
			return summaries, items, err
		}
		if len(policies) > 500 {
			return summaries, items, fmt.Errorf("trust scope exceeds capacity")
		}
		for _, p := range policies {
			trust := computeMediaAutopilotTrust(read, p.TenantID, p)
			if len(trust) == 0 {
				summaries[p.TenantID] = opsTrustSummary{State: "probation", Detail: "No owner trust decisions recorded"}
			}
			for class, stat := range trust {
				if isMediaApprovalTierVerdict(class) {
					continue
				}
				state := "probation"
				if stat.Earned {
					state = "trusted"
				}
				add(p.TenantID, class, state, p.CreatedAt, fmt.Sprint(stat.Decisions, stat.Reverts))
			}
		}
	case "media_studio":
		var policies []models.MediaStudioAutopilotPolicy
		if err := read.Limit(501).Find(&policies).Error; err != nil {
			return summaries, items, err
		}
		if len(policies) > 500 {
			return summaries, items, fmt.Errorf("trust scope exceeds capacity")
		}
		for _, p := range policies {
			stat := computeStudioReasonCodeTrust(read, p.TenantID, models.StudioReviewCodeMergedShort, p)
			state := "probation"
			if stat.Earned {
				state = "trusted"
			}
			if stat.LockedOff || stat.Reversals > 0 {
				state = "demoted"
			}
			add(p.TenantID, stat.Code, state, p.CreatedAt, fmt.Sprint(stat.Decisions, stat.Reversals))
		}
	case "preferences":
		var policies []models.PreferenceAutopilotPolicy
		if err := read.Limit(501).Find(&policies).Error; err != nil {
			return summaries, items, err
		}
		if len(policies) > 500 {
			return summaries, items, fmt.Errorf("trust scope exceeds capacity")
		}
		for _, p := range policies {
			var muteViolations int64
			read.Raw(`SELECT COUNT(*) FROM user_topic_prefs p JOIN user_topic_affinity a ON a.tenant_id=p.tenant_id AND a.user_id=p.user_id AND a.topic_id=p.topic_id WHERE p.tenant_id=? AND p.state='muted'`, p.TenantID).Scan(&muteViolations)
			stat := computePreferenceTrust(read, p.TenantID, p, muteViolations)
			state := "probation"
			if stat.Eligible {
				state = "trusted"
			}
			if stat.MuteViolation {
				state = "demoted"
			}
			add(p.TenantID, "topic_approval", state, p.CreatedAt, fmt.Sprint(stat.Decisions, stat.MuteViolation))
		}
	case "feed_integrity":
		var policies []models.FeedIntegrityPolicy
		if err := read.Limit(501).Find(&policies).Error; err != nil {
			return summaries, items, err
		}
		if len(policies) > 500 {
			return summaries, items, fmt.Errorf("trust scope exceeds capacity")
		}
		for _, p := range policies {
			stat := feedIntegrityTrust(read, p.TenantID, models.FeedIntegrityActionRefreshWindow, p)
			add(p.TenantID, stat.ActionClass, stat.State, p.CreatedAt, fmt.Sprint(stat.Decisions, stat.Failures))
		}
	}
	for tenant, s := range summaries {
		if s.State != "demoted" {
			s.State = "probation"
			if s.Total > 0 && s.Earned == s.Total {
				s.State = "trusted"
			}
		}
		summaries[tenant] = s
	}
	if err := recorder.readError(); err != nil {
		return map[string]opsTrustSummary{}, nil, err
	}
	if err := db.Statement.Context.Err(); err != nil {
		return map[string]opsTrustSummary{}, nil, err
	}
	return summaries, items, nil
}

// Storage has no periodic scheduler or durable heartbeat. Reuse a current owner
// observation when one exists; otherwise expose the DB inventory as partial
// evidence and explicitly withhold a health verdict. Observation never reaches
// Aggregation or invokes the owner's policy-creation path.
func opsStorageStatuses(db *gorm.DB, m opsFleetMember, lane opsFleetLane) []opsMemberStatus {
	var policies []models.StoragePolicy
	if err := db.Order("updated_at DESC,id DESC").Limit(501).Find(&policies).Error; err != nil {
		return []opsMemberStatus{{MemberKey: m.Key, MemberLabel: m.Label, Family: m.Family, Kind: m.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, State: "errored", Liveness: "not_applicable", CockpitPath: lane.CockpitPath, Error: opsReadError(err)}}
	}
	if len(policies) > 500 {
		return []opsMemberStatus{{MemberKey: m.Key, MemberLabel: m.Label, State: "errored", Liveness: "not_applicable", Error: "storage policy scope exceeds read capacity", CockpitPath: lane.CockpitPath}}
	}
	if len(policies) == 0 {
		return []opsMemberStatus{{MemberKey: m.Key, MemberLabel: m.Label, Family: m.Family, Kind: m.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, State: "unconfigured", Liveness: "not_applicable", CockpitPath: lane.CockpitPath}}
	}
	// Resolve the same tenant-override/global precedence as the owner, with
	// SELECTs only; a missing global policy never gets enrolled by observation.
	var tenants []string
	if err := db.Model(&models.ContentItem{}).Distinct("tenant_id").Limit(501).Pluck("tenant_id", &tenants).Error; err != nil {
		return []opsMemberStatus{{MemberKey: m.Key, MemberLabel: m.Label, LaneKey: lane.Key, State: "errored", Liveness: "not_applicable", Error: opsReadError(err), CockpitPath: lane.CockpitPath}}
	}
	if len(tenants) > 500 {
		return []opsMemberStatus{{MemberKey: m.Key, MemberLabel: m.Label, LaneKey: lane.Key, State: "errored", Liveness: "not_applicable", Error: "storage tenant scope exceeds read capacity", CockpitPath: lane.CockpitPath}}
	}
	effective := map[string]models.StoragePolicy{}
	var global *models.StoragePolicy
	for _, p := range policies {
		if p.TenantID == nil {
			if global == nil {
				copy := p
				global = &copy
			}
			continue
		}
		if _, ok := effective[*p.TenantID]; !ok {
			effective[*p.TenantID] = p
		}
	}
	for _, tenant := range tenants {
		if _, ok := effective[tenant]; !ok && global != nil {
			copy := *global
			scope := tenant
			copy.TenantID = &scope
			effective[tenant] = copy
		}
	}
	policies = nil
	for _, p := range effective {
		policies = append(policies, p)
	}
	if len(policies) == 0 {
		return []opsMemberStatus{{MemberKey: m.Key, MemberLabel: m.Label, Family: m.Family, Kind: m.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, State: "unknown", Liveness: "not_applicable", Evidence: "Global storage policy exists; tenant inventory has not been observed", CockpitPath: lane.CockpitPath}}
	}
	out := []opsMemberStatus{}
	recorder := &opsOwnerReadRecorder{Interface: db.Logger}
	read := db.Session(&gorm.Session{Logger: recorder})
	for _, p := range policies {
		s := opsMemberStatus{MemberKey: m.Key, MemberLabel: m.Label, Family: m.Family, Kind: m.Kind, LaneKey: lane.Key, LaneLabel: lane.Label, TenantID: *p.TenantID, State: "unknown", Liveness: "not_applicable", CockpitPath: lane.CockpitPath, Evidence: "Live storage metrics are unavailable; DB inventory alone cannot establish healthy storage"}
		if cached, ok := getCachedStorageHealth(*p.TenantID); ok {
			s.LastRunHeadline = cached.Summary
			s.LastRunStatus = cached.State
			s.Evidence = "Current owner storage observation: " + cached.Summary
			s.State = cached.State
			switch cached.State {
			case "healthy", "watch":
				s.State = "idle"
			case "critical", "pressure", "degraded", "degraded_no_cold":
				s.State = "attention"
			}
		} else {
			tracked := storageDBTrackedBytes(read, *p.TenantID)
			s.Evidence = fmt.Sprintf("DB tracks %d bytes against %d-byte quota; live bucket and cold-tier evidence unavailable", tracked, p.MaxStorageBytes)
			if p.MaxStorageBytes > 0 && float64(tracked)*100/float64(p.MaxStorageBytes) >= float64(p.TargetUtilizationPct) {
				s.State = "attention"
				s.LastRunStatus = "tracked_pressure"
			}
		}
		out = append(out, s)
	}
	if err := recorder.readError(); err != nil {
		for i := range out {
			out[i].State = "errored"
			out[i].Error = opsReadError(err)
		}
	}
	return out
}

func opsRankingAttention(db *gorm.DB, m opsFleetMember, now time.Time) ([]opsAttentionItem, error) {
	var tenants []string
	if err := db.Model(&models.ContentItem{}).Distinct("tenant_id").Where("type IN ('VIDEO','PODCAST')").Limit(501).Pluck("tenant_id", &tenants).Error; err != nil {
		return nil, err
	}
	if len(tenants) > 500 {
		return nil, fmt.Errorf("ranking tenant scope exceeds read capacity")
	}
	recorder := &opsOwnerReadRecorder{Interface: db.Logger}
	read := db.Session(&gorm.Session{Logger: recorder})
	items := []opsAttentionItem{}
	for _, tenant := range tenants {
		d := (intelligence.Engine{DB: read}).DiagnosticsSnapshot(tenant)
		count := d.UnscoredCount + d.StaleCount
		if count == 0 {
			continue
		}
		at := now
		if d.OldestComputeAt != nil {
			at = *d.OldestComputeAt
		}
		items = append(items, opsAttentionItem{Key: m.Key + ":attention:" + tenant + ":coverage", Fingerprint: opsFingerprint(tenant, fmt.Sprint(count)), System: m.Key, TenantID: tenant, Kind: "attention", Severity: "minor", Title: "Ranking score coverage needs refresh", Detail: fmt.Sprintf("Owner diagnostics: %d unscored and %d stale scores; scheduler evidence remains unknown", d.UnscoredCount, d.StaleCount), Count: int(count), FirstSeen: at, Href: m.CockpitPath})
	}
	return items, recorder.readError()
}
