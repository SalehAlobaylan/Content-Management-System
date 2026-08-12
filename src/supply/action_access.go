package supply

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"content-management-system/src/accesssnapshot"
	"content-management-system/src/models"

	"gorm.io/gorm"
)

// Supply actions use the same tenant-admin permission that admits the CMS
// circulation surface. The approval snapshot is persisted so a revoked or
// changed administrator cannot use an old confirmation later.
const SupplyActionRequiredPermission = "aggregation:manage"

var (
	supplyActionAccessMu       sync.RWMutex
	supplyActionAccessProvider accesssnapshot.Provider
)

// SetSupplyActionAccessProviderForTest makes IAM behavior deterministic for
// isolated tests. Production leaves it unset and obtains a fresh IAM snapshot
// with the established CMS service identity for every check.
func SetSupplyActionAccessProviderForTest(provider accesssnapshot.Provider) func() {
	supplyActionAccessMu.Lock()
	previous := supplyActionAccessProvider
	supplyActionAccessProvider = provider
	supplyActionAccessMu.Unlock()
	return func() {
		supplyActionAccessMu.Lock()
		supplyActionAccessProvider = previous
		supplyActionAccessMu.Unlock()
	}
}

func currentSupplyActionAccessProvider() (accesssnapshot.Provider, error) {
	supplyActionAccessMu.RLock()
	provider := supplyActionAccessProvider
	supplyActionAccessMu.RUnlock()
	if provider != nil {
		return provider, nil
	}
	return accesssnapshot.NewIAMClientFromEnv()
}

func validateSupplyActionSnapshot(snapshot accesssnapshot.Snapshot, actorID, tenantID string) error {
	if err := snapshot.ValidateFor(actorID, tenantID); err != nil {
		return err
	}
	if !snapshot.IsAdmin && !snapshot.HasPermission(SupplyActionRequiredPermission) {
		return fmt.Errorf("%w: current actor lacks %s", accesssnapshot.ErrUnavailable, SupplyActionRequiredPermission)
	}
	return nil
}

// ValidateSupplyActionApprovalSnapshot is used by the Operator worker after
// that worker has already fetched the current IAM snapshot for its plan step.
// The native request still re-fetches it at claim/effect time.
func ValidateSupplyActionApprovalSnapshot(snapshot accesssnapshot.Snapshot, actorID, tenantID string) error {
	return validateSupplyActionSnapshot(snapshot, actorID, tenantID)
}

// CurrentSupplyActionAccess obtains an authoritative access version for a
// new preview or approval. It never trusts browser JWT claims as fresh proof.
func CurrentSupplyActionAccess(ctx context.Context, actorID, tenantID string) (accesssnapshot.Snapshot, error) {
	provider, err := currentSupplyActionAccessProvider()
	if err != nil {
		return accesssnapshot.Snapshot{}, err
	}
	snapshot, err := provider.Snapshot(ctx, strings.TrimSpace(actorID), strings.TrimSpace(tenantID))
	if err != nil {
		return accesssnapshot.Snapshot{}, err
	}
	if err := validateSupplyActionSnapshot(snapshot, actorID, tenantID); err != nil {
		return accesssnapshot.Snapshot{}, err
	}
	return snapshot, nil
}

// RecheckSupplyActionAccess binds every claim/effect to the same actor,
// tenant, and exact IAM access version that authorized its confirmation.
// Legacy rows without that immutable version are fail-closed for new effects.
func RecheckSupplyActionAccess(ctx context.Context, request models.MediaSupplyActionRequest) error {
	if strings.TrimSpace(request.ApprovedBy) == "" || strings.TrimSpace(request.ApprovalAccessVersion) == "" {
		return fmt.Errorf("%w: supply action has no authoritative approval access version", accesssnapshot.ErrUnavailable)
	}
	snapshot, err := CurrentSupplyActionAccess(ctx, request.ApprovedBy, request.TenantID)
	if err != nil {
		return err
	}
	if snapshot.AccessVersion != request.ApprovalAccessVersion {
		return fmt.Errorf("%w: supply action approval access version changed", accesssnapshot.ErrUnavailable)
	}
	return nil
}

func recheckSupplyActionAccessByID(db *gorm.DB, tenantID, requestID string) (models.MediaSupplyActionRequest, error) {
	var request models.MediaSupplyActionRequest
	if db == nil {
		return request, fmt.Errorf("media supply action access check requires a database")
	}
	if err := db.Where("tenant_id = ? AND public_id = ?", tenantID, requestID).First(&request).Error; err != nil {
		return request, err
	}
	return request, RecheckSupplyActionAccess(context.Background(), request)
}

func isAccessRecheckFailure(err error) bool {
	return errors.Is(err, accesssnapshot.ErrUnavailable)
}
