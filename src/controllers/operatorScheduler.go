package controllers

import (
	"context"
	"time"

	operatorpkg "content-management-system/src/operator"

	"gorm.io/gorm"
)

const operatorInvestigationHeartbeatInterval = time.Minute

// StartOperatorInvestigationHeartbeat resumes only expired, persisted work.
// It has no browser dependency: every candidate receives a fresh IAM snapshot
// and a fresh runtime-policy read before its read-only investigation resumes.
func StartOperatorInvestigationHeartbeat(db *gorm.DB) {
	go func() {
		// Recover once at boot instead of making a restart wait for the first
		// interval. Every candidate still gets a new IAM snapshot and lease.
		runOperatorInvestigationHeartbeat(db)
		ticker := time.NewTicker(operatorInvestigationHeartbeatInterval)
		defer ticker.Stop()
		for range ticker.C {
			runOperatorInvestigationHeartbeat(db)
		}
	}()
}

func runOperatorInvestigationHeartbeat(db *gorm.DB) {
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		return // fail closed; the next tick can retry without stale authority.
	}
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	store := operatorpkg.NewInvestigationStore(db)
	if _, err := store.RecoverExpiredClaims(ctx); err != nil {
		return
	}
	investigations, err := store.LoadBackgrounded(ctx, 10)
	if err != nil {
		return
	}
	for _, investigation := range investigations {
		input, err := operatorpkg.DecodeStoredInvestigationInput(investigation)
		if err != nil {
			_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "invalid_persisted_request")
			continue
		}
		access, err := accessClient.Snapshot(ctx, investigation.ActorID, investigation.TenantID)
		if err != nil {
			_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "access_unavailable")
			continue
		}
		_, policy, err := operatorExecutionPolicy(db, investigation.TenantID)
		if err != nil || !policy.ReadEnabled {
			_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "policy_unavailable")
			continue
		}
		var reasoner operatorpkg.Reasoner
		if policy.LLMEnabled {
			reasoner, _ = operatorpkg.NewHTTPReasonerFromEnv()
		}
		coordinator := operatorpkg.NewInvestigationCoordinator(operatorpkg.NewContextFabric(db, operatorpkg.DefaultAdapterRegistry()), store, reasoner)
		_, _ = coordinator.Process(ctx, investigation, access, policy, input)
	}
}
