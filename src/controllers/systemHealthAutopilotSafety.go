package controllers

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"content-management-system/src/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type systemRecoveryEvidence struct {
	Snapshot systemHealthSnapshot
	Previous []systemRunSnapshot
}

// Both human close and observation updates lock the same row. Preserve the
// independently maintained containment ledger when writing an observation.
func saveSystemEpisodeObservation(db *gorm.DB, episode *models.SystemIncidentEpisode) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var current models.SystemIncidentEpisode
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, episode.ID).Error; err != nil {
			return err
		}
		if current.Status != models.SystemIncidentStatusOpen && current.Status != models.SystemIncidentStatusRecovering {
			return errSystemIncidentClosed
		}
		episode.Containment = current.Containment
		return tx.Save(episode).Error
	})
}

func systemRecoverySamples(episode models.SystemIncidentEpisode, snapshot systemHealthSnapshot, previous []systemRunSnapshot, required int) int {
	if !systemEpisodeObservablyHealthy(episode, snapshot) {
		return 0
	}
	count := 1
	for _, sample := range previous {
		if count >= required {
			break
		}
		if !systemEpisodeObservablyHealthyServices(episode, sample.Services) {
			break
		}
		if !sample.TooClose {
			count++
		}
	}
	return count
}

func systemReleaseEvidenceHealthy(episode models.SystemIncidentEpisode, evidence systemRecoveryEvidence, policy models.SystemAutopilotPolicy, now time.Time) bool {
	observedAt, err := time.Parse(time.RFC3339Nano, evidence.Snapshot.Timestamp)
	if err != nil || observedAt.After(now.Add(5*time.Second)) || now.Sub(observedAt) > time.Duration(policy.IntervalMinutes*2+1)*time.Minute {
		return false
	}
	return systemRecoverySamples(episode, evidence.Snapshot, evidence.Previous, policy.ResolveProbes) >= policy.ResolveProbes
}

type systemRecoveryProjection struct {
	HealthySamples  int        `json:"healthy_samples"`
	RequiredSamples int        `json:"required_samples"`
	EvidenceFresh   bool       `json:"evidence_fresh"`
	NextCheckAt     *time.Time `json:"next_check_at,omitempty"`
	Reason          string     `json:"reason"`
}

func systemEpisodeRecoveryStatus(episode models.SystemIncidentEpisode, policy models.SystemAutopilotPolicy, latest *models.SystemAutopilotRun, previous []systemRunSnapshot, now time.Time) systemRecoveryProjection {
	monitor := systemMonitorStatus(policy, latest, now)
	projection := systemRecoveryProjection{RequiredSamples: policy.ResolveProbes, EvidenceFresh: monitor.Fresh, NextCheckAt: monitor.NextDueAt, Reason: "Fresh service evidence is required before recovery can be verified"}
	if latest == nil || !monitor.Fresh {
		return projection
	}
	var evidence struct {
		Snapshot systemHealthSnapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(latest.ProbeResults, &evidence); err != nil {
		return projection
	}
	projection.HealthySamples = systemRecoverySamples(episode, evidence.Snapshot, previous, policy.ResolveProbes)
	if episode.Status == models.SystemIncidentStatusClosedByHuman {
		projection.Reason = "Closed by an administrator; this does not verify recovery or release pauses"
	} else if projection.HealthySamples >= projection.RequiredSamples {
		projection.Reason = "Recovery evidence is complete; release still requires current authority and exact pause ownership"
	} else {
		projection.Reason = "Keep checking the affected service; existing work may finish while automation is paused"
	}
	return projection
}

// Derive present protection from current policy rows, including when scheduled
// observation is disabled. Reads never alter historical ownership records.
func projectSystemContainment(db *gorm.DB, episodes []models.SystemIncidentEpisode, now time.Time) (systemContainmentProjection, error) {
	projection := systemContainmentProjection{Targets: []systemContainmentTargetProjection{}}
	rowsBySibling := map[string]map[string]systemSiblingPolicyRow{}
	for _, episode := range episodes {
		ledger, legacy := readSystemContainmentLedger(episode.Containment)
		if legacy {
			projection.Pending++
			continue
		}
		for key, tenants := range ledger.Siblings {
			sibling, known := systemSiblingByKey(key)
			if !known {
				projection.Pending++
				continue
			}
			rows, loaded := rowsBySibling[key]
			if !loaded {
				var policies []systemSiblingPolicyRow
				if err := db.Raw(fmt.Sprintf("SELECT tenant_id, %s AS paused_until FROM %s", sibling.PauseColumn, sibling.Table)).Scan(&policies).Error; err != nil {
					return projection, err
				}
				rows = map[string]systemSiblingPolicyRow{}
				for _, row := range policies {
					rows[row.TenantID] = row
				}
				rowsBySibling[key] = rows
			}
			for tenant, entry := range tenants {
				if entry.WrittenUntil == "" && !entry.HumanOwned && entry.Reason != models.SystemAutopilotGuardHumanPause {
					continue
				}
				target := projectSystemContainmentTarget(episode, key, tenant, entry, rows[tenant], now)
				switch target.Outcome {
				case "paused":
					projection.Active++
				case "release_pending":
					projection.Active++
					projection.Pending++
				case "human_owned":
					projection.HumanOwned++
				case "expired":
					projection.Expired++
				case "unknown":
					projection.Pending++
				}
				projection.Targets = append(projection.Targets, target)
			}
		}
	}
	sort.SliceStable(projection.Targets, func(i, j int) bool {
		left, right := projection.Targets[i], projection.Targets[j]
		return left.EpisodeID+left.Sibling+left.TenantID < right.EpisodeID+right.Sibling+right.TenantID
	})
	return projection, nil
}

func projectSystemContainmentTarget(episode models.SystemIncidentEpisode, sibling, tenant string, entry systemContainmentLedgerEntry, row systemSiblingPolicyRow, now time.Time) systemContainmentTargetProjection {
	target := systemContainmentTargetProjection{EpisodeID: episode.PublicID.String(), Sibling: sibling, TenantID: tenant, Outcome: entry.Outcome, Until: entry.WrittenUntil, Reason: entry.Reason}
	if entry.Outcome == "resumed" || entry.Outcome == "expired" {
		return target
	}
	expected, parseErr := time.Parse(time.RFC3339Nano, entry.WrittenUntil)
	if row.PausedUntil != nil && row.PausedUntil.After(now) {
		target.Until = row.PausedUntil.UTC().Format(time.RFC3339Nano)
		if !entry.HumanOwned && parseErr == nil && row.PausedUntil.Equal(expected) {
			target.Outcome = "paused"
			if episode.Status == models.SystemIncidentStatusResolved || episode.Status == models.SystemIncidentStatusClosedByHuman {
				target.Outcome = "release_pending"
				if target.Reason == "" {
					target.Reason = "Release requires verified recovery, current controls, and exact ownership"
				}
			}
		} else {
			target.Outcome, target.HumanOwned, target.Reason = "human_owned", true, "Current pause is controlled outside this incident; review it in the native automation policy"
		}
	} else if parseErr == nil && !expected.After(now) {
		target.Outcome, target.Reason = "expired", "The containment lease has expired; it no longer stops new work"
	} else if parseErr == nil || entry.HumanOwned || entry.Reason == models.SystemAutopilotGuardHumanPause {
		target.Outcome, target.HumanOwned, target.Reason = "human_owned", true, "The pause was cleared or replaced; System Health will not reacquire it"
		if episode.Status == models.SystemIncidentStatusResolved || episode.Status == models.SystemIncidentStatusClosedByHuman {
			target.Outcome = "human_released"
		}
	} else {
		target.Outcome, target.Reason = "unknown", "Containment ownership cannot be verified"
	}
	return target
}

func projectSystemAttention(db *gorm.DB, now time.Time) ([]systemAttentionProjection, error) {
	// Select the latest decision, including success, before filtering failures.
	// An older error must not survive a later successful action for that target.
	var decisions []models.SystemAutopilotAction
	if err := db.Raw(`SELECT DISTINCT ON (COALESCE(episode_id, 0), target, COALESCE(output->>'tenant_id', '')) *
 FROM system_autopilot_actions
 ORDER BY COALESCE(episode_id, 0), target, COALESCE(output->>'tenant_id', ''), started_at DESC, id DESC`).Scan(&decisions).Error; err != nil {
		return nil, err
	}
	latest, err := latestSystemRunWithError(db)
	if err != nil {
		return nil, err
	}
	var episodes []models.SystemIncidentEpisode
	ids := []uint{}
	for _, action := range decisions {
		if action.EpisodeID != nil {
			ids = append(ids, *action.EpisodeID)
		}
	}
	if len(ids) > 0 {
		if err := db.Where("id IN ?", ids).Find(&episodes).Error; err != nil {
			return nil, err
		}
	}
	containment, err := projectSystemContainment(db, episodes, now)
	if err != nil {
		return nil, err
	}
	return unresolvedSystemAttention(decisions, episodes, containment, latest), nil
}

func unresolvedSystemAttention(decisions []models.SystemAutopilotAction, episodes []models.SystemIncidentEpisode, containment systemContainmentProjection, latest *models.SystemAutopilotRun) []systemAttentionProjection {
	byID := map[uint]models.SystemIncidentEpisode{}
	for _, episode := range episodes {
		byID[episode.ID] = episode
	}
	items := []systemAttentionProjection{}
	for _, action := range decisions {
		if action.Status != "error" && action.Status != "attention" && action.Guardrail != models.SystemAutopilotGuardHumanPause && action.Guardrail != models.SystemAutopilotGuardEvidenceStale && action.Guardrail != models.SystemAutopilotGuardScopeCap {
			continue
		}
		item := systemAttentionProjection{Target: action.Target, Guardrail: action.Guardrail, Status: action.Status, Reason: action.Reason, At: action.StartedAt.UTC().Format(time.RFC3339)}
		var output struct {
			TenantID string `json:"tenant_id"`
		}
		_ = json.Unmarshal(action.Output, &output)
		item.TenantID = output.TenantID
		if action.EpisodeID == nil {
			if latest == nil || action.RunID != latest.ID {
				continue
			}
		} else {
			episode, exists := byID[*action.EpisodeID]
			if !exists {
				continue
			}
			item.EpisodeID = episode.PublicID.String()
			relevant, terminalTarget := false, false
			for _, target := range containment.Targets {
				if target.EpisodeID != item.EpisodeID || target.Sibling != item.Target || (item.TenantID != "" && target.TenantID != item.TenantID) {
					continue
				}
				switch target.Outcome {
				case "release_pending", "human_owned", "unknown":
					relevant = true
				case "resumed", "expired", "human_released":
					terminalTarget = true
				}
			}
			active := episode.Status == models.SystemIncidentStatusOpen || episode.Status == models.SystemIncidentStatusRecovering
			if !relevant && (!active || terminalTarget) {
				continue
			}
			// A scope-wide preflight failure is superseded by a subsequent successful
			// concrete target action even though that action has a tenant identity.
			superseded := false
			if item.TenantID == "" {
				for _, next := range decisions {
					if next.EpisodeID != nil && *next.EpisodeID == *action.EpisodeID && next.Target == action.Target && next.ID > action.ID && next.Status == "success" {
						superseded = true
					}
				}
			}
			if superseded {
				continue
			}
		}
		items = append(items, item)
	}
	return items
}
