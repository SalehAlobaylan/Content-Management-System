package utils

import (
	"fmt"
	"os"

	"gorm.io/gorm"
)

const (
	PreservesExpiredProcessingHistory = "preserves_expired_processing_history"
	RequiresOwnerReconciliation       = "requires_owner_reconciliation"
	// Future timestamped migrations must choose a policy in the authoring tests.
	LocalMigrationPolicyBaseline = "20261002150000"
)

type LocalMigration struct{ Version, Checksum string }
type LocalMigrationPolicy struct{ Checksum, WorkScope string }

// Maintainer policy, not an operator setting. Each entry records review of the
// exact canonical SQL bytes; it does not approve execution or destructive SQL.
var localMigrationPolicies = map[string]LocalMigrationPolicy{
	"20260929100000_content_reset_campaign_foundation.sql":      {"714c3ccedf74b27c54329ecb6a89c6cc5732d27d2f750daed20c350ad73ea9a6", PreservesExpiredProcessingHistory},
	"20260929110000_news_generation_item_membership.sql":        {"2c6b58248932471bba5c447ea6b71133f6f599e16b6be581bdcfeacd7af5cd10", PreservesExpiredProcessingHistory},
	"20260929120000_source_item_instance_registry.sql":          {"83b708527570382bd73aec9baa53bdc147859fe248c9d920bc31db76b97e21f2", PreservesExpiredProcessingHistory},
	"20260929130000_content_reset_feed_views.sql":               {"045b9a4e1563e48478380736192b9624f7c48ad92ffed62dd550a3708b2a6e1c", PreservesExpiredProcessingHistory},
	"20260929140000_source_observation_request_scope.sql":       {"e9dd5ec7368ec81d7c26bf424f087d100ad5cdb16ae1c3fec49a0592f8286ece", PreservesExpiredProcessingHistory},
	"20260929150000_source_observation_materialization.sql":     {"007b863d3d5f73a52479a43e9591837a769496ef14c595e99e0838965c696f27", PreservesExpiredProcessingHistory},
	"20260929160000_content_reset_execution_coordinator.sql":    {"6612ed4f267b0585e54b89db615007ed6023b1121987b2270d4362fcc03dce5d", PreservesExpiredProcessingHistory},
	"20260929170000_source_item_staged_instances.sql":           {"dbb683a45e7d530feb119de6fe0ff756bd17a6206e44504ff419c18d73d1d52c", PreservesExpiredProcessingHistory},
	"20260929180000_content_reset_staging_membership_fence.sql": {"6710922be21435e95e7e6b9632f0d7600cf49512069908511dd1e410345e9aa9", PreservesExpiredProcessingHistory},
	"20260929190000_content_reset_replay_branches.sql":          {"d8a8a3422ca502b60f8981f16274a732fdd184a23e20af95d20e9998fbbe4894", PreservesExpiredProcessingHistory},
	"20260929200000_content_reset_provider_slot_release.sql":    {"f0348c1ff5f17662a4d5b988ce6ea271004bdbd8afd20414e86183ae766fd9bc", PreservesExpiredProcessingHistory},
	"20261002100000_content_reset_async_steps.sql":              {"014f60808742300cb0fcfeff7907449a0d50bfc60002bddee3c26f13fbbd7126", PreservesExpiredProcessingHistory},
	"20261002110000_content_reset_workflow.sql":                 {"f143ed0a72b6df6db5716d934d889f91d28309cea1a7ae5ea266d90286d2fc3d", PreservesExpiredProcessingHistory},
	"20261002120000_content_reset_replay_reuse.sql":             {"8095e0c72ced5627d38c3fd51428daf3b7ef826021bf6c3228280dd3009b64af", PreservesExpiredProcessingHistory},
	"20261002130000_content_reset_proven_retry.sql":             {"f31eee65e88385a29700cd79cea87120cfb839d44ee4ef88ddc78659a16f8e20", PreservesExpiredProcessingHistory},
	"20261002140000_content_reset_news_projections.sql":         {"bffb3a2eb54bb8ea9bd0c3a7c24c325af20e3e5e799ab05f323b54253fb4edd5", PreservesExpiredProcessingHistory},
	"20261002150000_content_reset_completion.sql":               {"dac6dd7b57cd597209b11b203ba947e15030f41bf56c7ecd6365d8a372d2b663", PreservesExpiredProcessingHistory},
}

func QualifiedLocalSchemaMigration(version, checksum string) bool {
	policy, ok := localMigrationPolicies[version]
	return ok && policy.Checksum == checksum && policy.WorkScope == PreservesExpiredProcessingHistory
}

// Installing these two exact definitions does not execute their embedded DELETE.
// Processing compatibility alone never grants this destructive-SQL exception.
func QualifiedDeletionDefinitions(version, checksum string) bool {
	switch version {
	case "20260929100000_content_reset_campaign_foundation.sql":
		return checksum == "714c3ccedf74b27c54329ecb6a89c6cc5732d27d2f750daed20c350ad73ea9a6"
	case "20260929110000_news_generation_item_membership.sql":
		return checksum == "2c6b58248932471bba5c447ea6b71133f6f599e16b6be581bdcfeacd7af5cd10"
	}
	return false
}

func ValidateLocalMigrationPolicy(version, checksum string) error {
	policy, exists := localMigrationPolicies[version]
	if !exists {
		if len(version) >= 14 && version[:14] > LocalMigrationPolicyBaseline {
			return fmt.Errorf("%s needs a tested local migration policy: add its exact SHA-256 and choose %s or %s in src/utils/local_schema_upgrade.go", version, PreservesExpiredProcessingHistory, RequiresOwnerReconciliation)
		}
		return nil
	}
	if policy.Checksum != checksum {
		return fmt.Errorf("%s changed after its local policy review; verify the SQL and its tests before updating the policy checksum", version)
	}
	if policy.WorkScope != PreservesExpiredProcessingHistory && policy.WorkScope != RequiresOwnerReconciliation {
		return fmt.Errorf("%s has an invalid local migration work classification", version)
	}
	return nil
}

func migrationPolicyBlocker(migrations []LocalMigration) *MigrationWorkBlocker {
	for _, migration := range migrations {
		policy, ok := localMigrationPolicies[migration.Version]
		if !ok || policy.Checksum != migration.Checksum || (policy.WorkScope != PreservesExpiredProcessingHistory && policy.WorkScope != RequiresOwnerReconciliation) {
			return &MigrationWorkBlocker{Reason: "migration_not_reviewed", Migration: migration.Version}
		}
		if policy.WorkScope == RequiresOwnerReconciliation {
			return &MigrationWorkBlocker{Reason: "owner_reconciliation_required", Migration: migration.Version}
		}
	}
	return nil
}

// Preview and execution share this exact predicate. Unknown effects are never
// settled: only compatible, checksum-bound SQL can retain expired history.
func CheckLocalMigrationWork(tx *gorm.DB, migrations []LocalMigration) error {
	policyBlocker := migrationPolicyBlocker(migrations)
	compatible := len(migrations) > 0 && policyBlocker == nil
	return checkLocalMigrationWork(tx, compatible, policyBlocker)
}

func CheckQualifiedLocalSchemaWork(tx *gorm.DB) error {
	return checkLocalMigrationWork(tx, true, nil)
}

func checkLocalMigrationWork(tx *gorm.DB, compatible bool, policyBlocker *MigrationWorkBlocker) error {
	probes := append([]MigrationInflightProbe{{"lifecycle_operation_claims", "SELECT count(*) FROM lifecycle_operation_claims WHERE state='active'"}}, CMSMigrationInflightQueries...)
	for _, probe := range probes {
		var exists bool
		if err := tx.Raw("SELECT to_regclass(?) IS NOT NULL", "public."+probe.Subject).Scan(&exists).Error; err != nil {
			return err
		}
		if !exists {
			continue
		}
		query := probe.Query
		if compatible {
			query = localSchemaWorkQuery(probe)
		}
		var count int64
		if err := tx.Raw(query).Scan(&count).Error; err != nil {
			return fmt.Errorf("migration work %s is unknown: %w", probe.Subject, err)
		}
		if count > 0 {
			live := probe.Subject == "lifecycle_operation_claims" || probe.Subject == "pipeline_stage_leases"
			if probe.Subject == "content_stage_attempts" || probe.Subject == "pipeline_repair_requests" {
				live = compatible // The compatible predicate already checks live claims.
				if !compatible {
					liveQuery := localSchemaWorkQuery(probe)
					var liveCount int64
					if err := tx.Raw(liveQuery).Scan(&liveCount).Error; err != nil {
						return fmt.Errorf("migration live work %s is unknown: %w", probe.Subject, err)
					}
					live = liveCount > 0
				}
			}
			blocker := MigrationWorkBlocker{Reason: "owner_reconciliation_required", Subject: probe.Subject}
			if policyBlocker != nil {
				blocker = *policyBlocker
				blocker.Subject = probe.Subject
			}
			if live {
				blocker.Reason = "live_work"
				blocker.Migration = ""
			}
			return &MigrationWorkBlockedError{Blocker: blocker}
		}
	}
	return nil
}

// One predicate per retained-history owner, shared by both classifications.
func localSchemaWorkQuery(probe MigrationInflightProbe) string {
	switch probe.Subject {
	case "content_stage_attempts":
		return "SELECT count(*) FROM content_stage_attempts WHERE state IN ('claimed','running','verifying','uncertain','reconciling') AND (lease_expires_at IS NULL OR lease_expires_at > now())"
	case "pipeline_repair_requests":
		return "SELECT count(*) FROM pipeline_repair_requests WHERE state IN ('claimed','running','verifying') OR (state='uncertain' AND claim_expires_at > now())"
	}
	return probe.Query
}

func CheckLocalSchemaPreparationAuthority(tx *gorm.DB, migrations ...LocalMigration) error {
	if err := CheckLocalPreparationIdentity(tx); err != nil {
		return err
	}
	if os.Getenv("WAHB_LOCAL_PREPARATION") != "1" {
		return fmt.Errorf("qualified schema upgrade requires managed offline owner execution")
	}
	return CheckLocalMigrationWork(tx, migrations)
}

func CheckLocalSchemaMigrationAuthority(tx *gorm.DB, version, checksum string) error {
	if QualifiedLocalSchemaMigration(version, checksum) {
		// Hold historical work stable until commit; never force-complete its effects.
		for _, table := range []string{"content_stage_attempts", "pipeline_repair_requests", "pipeline_stage_leases"} {
			var exists bool
			if err := tx.Raw("SELECT to_regclass(?) IS NOT NULL", "public."+table).Scan(&exists).Error; err != nil {
				return err
			}
			if exists {
				if err := tx.Exec("LOCK TABLE " + table + " IN SHARE MODE").Error; err != nil {
					return err
				}
			}
		}
	}
	return CheckLocalSchemaPreparationAuthority(tx, LocalMigration{version, checksum})
}
