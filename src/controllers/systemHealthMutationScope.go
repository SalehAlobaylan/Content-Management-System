package controllers

import (
	"content-management-system/src/models"
	"fmt"

	"gorm.io/gorm"
)

type systemMutationScopeKey struct{}
type systemMutationScope struct {
	Targets map[string]bool
	Denied  bool
	Reason  string
}

func systemMutationTargetKey(sibling, tenant string) string { return sibling + "\x00" + tenant }

// Preflight the union of acquisitions/renewals and outstanding releases before
// the run changes any pause. Individual incidents cannot reset this budget.
func prepareSystemMutationScope(db *gorm.DB, policy models.SystemAutopilotPolicy, anomalies []systemAnomaly, episodes []models.SystemIncidentEpisode) (systemMutationScope, error) {
	scope := systemMutationScope{Targets: map[string]bool{}}
	disabled := containmentDisabledSet(policy)
	for _, sibling := range systemSiblingAutopilots {
		needed := false
		for _, anomaly := range anomalies {
			if anomaly.ActionsBlocked == "" && isSystemHardDownVerdict(anomaly.Verdict) && !disabled[sibling.Key] && siblingDependsOnAnomaly(sibling, anomaly) {
				needed = true
			}
		}
		if !needed {
			continue
		}
		rows, err := systemSiblingPolicyRows(db, sibling)
		if err != nil {
			scope.Denied, scope.Reason = true, "Containment target scope is unavailable"
			return scope, err
		}
		for _, row := range rows {
			scope.Targets[systemMutationTargetKey(sibling.Key, row.TenantID)] = true
		}
	}
	for _, episode := range episodes {
		ledger, legacy := readSystemContainmentLedger(episode.Containment)
		if legacy {
			continue
		}
		for sibling, tenants := range ledger.Siblings {
			for tenant, entry := range tenants {
				if entry.WrittenUntil != "" && !entry.HumanOwned && entry.Outcome != "resumed" && entry.Outcome != "expired" {
					scope.Targets[systemMutationTargetKey(sibling, tenant)] = true
				}
			}
		}
	}
	if len(scope.Targets) > systemAutopilotTargetCap {
		scope.Denied = true
		scope.Reason = fmt.Sprintf("Run scope has %d sibling/tenant targets, exceeding the cap of %d; no pause or release is permitted", len(scope.Targets), systemAutopilotTargetCap)
	}
	return scope, nil
}

func systemMutationTargetAllowed(db *gorm.DB, sibling, tenant string) bool {
	scope, _ := db.Statement.Context.Value(systemMutationScopeKey{}).(*systemMutationScope)
	return scope == nil || !scope.Denied && scope.Targets[systemMutationTargetKey(sibling, tenant)]
}
