package controllers

import (
	"context"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"gorm.io/gorm"
)

func newOperatorContextFabric(db *gorm.DB) *operatorpkg.ContextFabric {
	return operatorpkg.NewContextFabric(db, operatorpkg.DefaultAdapterRegistry()).WithStorageReader(func(ctx context.Context, tenant string) (operatorpkg.StorageSnapshot, []string, error) {
		return readOperatorStorageSnapshot(ctx, db, tenant)
	})
}

// This owner bridge never seeds policy, directly probes providers, reconciles inventory
// or retains browser credentials. The registered Aggregation GET uses the
// existing service token and a separate five-second read budget.
func readOperatorStorageSnapshot(ctx context.Context, db *gorm.DB, tenant string) (operatorpkg.StorageSnapshot, []string, error) {
	db = db.WithContext(ctx)
	snapshot := operatorpkg.StorageSnapshot{ProviderScope: "platform_global", CMSScope: "tenant", CleanupActivity: "unknown"}
	totals, tracked, err := readStorageContentTotals(db, tenant)
	if err != nil {
		return snapshot, nil, err
	}
	snapshot.TrackedBytes = tracked
	for _, total := range totals {
		snapshot.TrackedItems += total.Count
	}
	unknowns := []string{}
	var policy models.StoragePolicy
	result := db.Where("tenant_id = ?", tenant).Order("updated_at DESC, id DESC").Limit(1).Find(&policy)
	if result.Error != nil {
		return snapshot, nil, result.Error
	}
	if result.RowsAffected == 0 {
		result = db.Where("tenant_id IS NULL").Order("updated_at DESC, id DESC").Limit(1).Find(&policy)
		if result.Error != nil {
			return snapshot, nil, result.Error
		}
	}
	if result.RowsAffected > 0 {
		snapshot.PolicyLimitBytes, snapshot.CleanupEnabled = &policy.MaxStorageBytes, &policy.Enabled
		snapshot.CleanupTargetPct = &policy.TargetUtilizationPct
		snapshot.PolicyScope = "tenant"
		if policy.TenantID == nil {
			snapshot.PolicyScope = "platform_global"
		}
	} else {
		unknowns = append(unknowns, "No stored storage limit or cleanup policy is available.")
	}
	liveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	live, err := callAggregationStorageStatsContext(liveCtx, "")
	if err != nil {
		unknowns = append(unknowns, "The storage owner could not report shared-bucket usage; tenant CMS totals are still available.")
	} else {
		snapshot.ProviderUsedBytes, snapshot.ProviderObjectCount = &live.UsedBytes, &live.ObjectCount
	}
	return snapshot, unknowns, nil
}
