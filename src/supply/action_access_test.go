package supply

import (
	"context"
	"errors"
	"testing"
	"time"

	"content-management-system/src/accesssnapshot"
	"content-management-system/src/models"
)

type staticSupplyActionAccess struct {
	snapshot accesssnapshot.Snapshot
	err      error
}

func (provider staticSupplyActionAccess) Snapshot(_ context.Context, _, _ string) (accesssnapshot.Snapshot, error) {
	return provider.snapshot, provider.err
}

func TestSupplyActionAccessRequiresCurrentVersionAndPermission(t *testing.T) {
	restore := SetSupplyActionAccessProviderForTest(staticSupplyActionAccess{snapshot: accesssnapshot.Snapshot{
		UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"aggregation:manage"}, AccessVersion: "v1", ObservedAt: time.Now().UTC(),
	}})
	defer restore()
	snapshot, err := CurrentSupplyActionAccess(context.Background(), "admin-a", "tenant-a")
	if err != nil || snapshot.AccessVersion != "v1" {
		t.Fatalf("expected current permitted snapshot, got %#v / %v", snapshot, err)
	}
	request := models.MediaSupplyActionRequest{TenantID: "tenant-a", ApprovedBy: "admin-a", ApprovalAccessVersion: "v1"}
	if err := RecheckSupplyActionAccess(context.Background(), request); err != nil {
		t.Fatalf("expected matching current access, got %v", err)
	}
	request.ApprovalAccessVersion = "v0"
	if err := RecheckSupplyActionAccess(context.Background(), request); !errors.Is(err, accesssnapshot.ErrUnavailable) {
		t.Fatalf("expected changed access version to fail closed, got %v", err)
	}
}

func TestSupplyActionAccessRejectsPermissionLossAndLegacyApproval(t *testing.T) {
	restore := SetSupplyActionAccessProviderForTest(staticSupplyActionAccess{snapshot: accesssnapshot.Snapshot{
		UserID: "admin-a", TenantID: "tenant-a", Active: true, Permissions: []string{"feed:read"}, AccessVersion: "v1", ObservedAt: time.Now().UTC(),
	}})
	defer restore()
	if _, err := CurrentSupplyActionAccess(context.Background(), "admin-a", "tenant-a"); !errors.Is(err, accesssnapshot.ErrUnavailable) {
		t.Fatalf("expected permission loss to fail closed, got %v", err)
	}
	request := models.MediaSupplyActionRequest{TenantID: "tenant-a", ApprovedBy: "admin-a"}
	if err := RecheckSupplyActionAccess(context.Background(), request); !errors.Is(err, accesssnapshot.ErrUnavailable) {
		t.Fatalf("expected legacy action without access version to fail closed, got %v", err)
	}
}
