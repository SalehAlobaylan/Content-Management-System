package controllers

import (
	"context"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"

	"gorm.io/gorm"
)

const operatorInvestigationHeartbeatInterval = time.Minute
const operatorInvestigationWorkerTimeout = 60 * time.Second

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
		// Each investigation owns its bounded budget. A slow first candidate
		// must not leave later candidates running under an expired poll context.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), operatorInvestigationWorkerTimeout)
			defer cancel()
			resumeOperatorInvestigation(ctx, db, accessClient, store, investigation)
		}()
	}
}

func resumeOperatorInvestigation(ctx context.Context, db *gorm.DB, accessClient *operatorpkg.IAMAccessClient, store *operatorpkg.InvestigationStore, investigation models.OperatorInvestigation) {
	input, err := operatorpkg.DecodeStoredInvestigationInput(investigation)
	if err != nil {
		_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "invalid_persisted_request")
		return
	}
	access, err := accessClient.Snapshot(ctx, investigation.ActorID, investigation.TenantID)
	if err != nil {
		_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "access_unavailable")
		return
	}
	_, policy, err := operatorExecutionPolicy(db, investigation.TenantID)
	if err != nil || !policy.ReadEnabled {
		_ = store.Fail(ctx, investigation.ID, investigation.TenantID, "policy_unavailable")
		return
	}
	var reasoner operatorpkg.Reasoner
	if policy.LLMEnabled {
		reasoner, _ = operatorpkg.NewHTTPReasonerFromEnv()
	}
	coordinator := operatorpkg.NewInvestigationCoordinator(newOperatorContextFabric(db), store, reasoner)
	_, _ = coordinator.Process(ctx, investigation, access, policy, input)
}
