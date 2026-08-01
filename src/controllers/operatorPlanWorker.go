package controllers

import (
	"context"
	"time"

	operatorpkg "content-management-system/src/operator"

	"gorm.io/gorm"
)

const operatorPlanWorkerInterval = 5 * time.Second

// StartOperatorPlanWorker consumes only CMS-persisted, signed action jobs.
// Approval is the only user transition that queues work; the browser never
// invokes an executor or supplies a credential to this worker.
func StartOperatorPlanWorker(db *gorm.DB) {
	go func() {
		runOperatorPlanWorker(db)
		ticker := time.NewTicker(operatorPlanWorkerInterval)
		defer ticker.Stop()
		for range ticker.C {
			runOperatorPlanWorker(db)
		}
	}()
}

func runOperatorPlanWorker(db *gorm.DB) {
	signingKey, err := operatorpkg.PlanSigningKeyFromEnv()
	if err != nil {
		return
	}
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	store := operatorpkg.NewPlanStore(db, signingKey)
	if _, err := store.RecoverExpiredPlanClaims(ctx); err != nil {
		return
	}
	if _, err := store.RecoverExpiredJobs(ctx); err != nil {
		return
	}
	// One bounded job per tick keeps the worker fair across tenants and ensures
	// every loop returns to fresh policy and IAM reads.
	job, plan, err := store.ClaimNextQueuedJob(ctx)
	if err != nil {
		return
	}
	_, policy, err := operatorExecutionPolicy(db, plan.TenantID)
	if err != nil || !policy.ExecutionEnabled {
		_ = store.BlockQueuedJob(ctx, job, plan, "execution_disabled")
		return
	}
	access, err := accessClient.Snapshot(ctx, plan.ActorID, plan.TenantID)
	if err != nil {
		_ = store.BlockQueuedJob(ctx, job, plan, "access_unavailable")
		return
	}
	claimed, canonical, descriptor, err := store.ClaimPlan(ctx, plan.ID, access)
	if err != nil {
		_ = store.BlockQueuedJob(ctx, job, plan, "claim_preconditions_changed")
		return
	}
	if job.ClaimToken == nil || store.MarkQueuedJobClaimed(ctx, job.ID, plan.TenantID, *job.ClaimToken) != nil {
		// The plan remains fenced as claimed and recovery will never re-execute a
		// started step. Let its lease expire into the verification-safe path.
		return
	}
	runningPlan, step, err := store.BeginClaimedPlan(ctx, claimed.ID, plan.TenantID, *claimed.ClaimToken)
	if err != nil {
		return
	}
	success, before, after, verified := executeRegisteredOperatorPlan(ctx, db, plan.TenantID, descriptor.Key, runningPlan, step, canonical, access)
	if err := store.FinishClaimedPlan(ctx, claimed.ID, plan.TenantID, *claimed.ClaimToken, success, before, after, verified); err != nil {
		return
	}
	_ = store.FinishQueuedJob(ctx, job.ID, plan.TenantID, success)
}
