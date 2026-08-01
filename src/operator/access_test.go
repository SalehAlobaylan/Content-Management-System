package operator

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIAMAccessClientRequiresConfiguredMachineCapability(t *testing.T) {
	if _, err := NewIAMAccessClient("http://iam.test", "", nil); !errors.Is(err, ErrAccessUnavailable) {
		t.Fatalf("expected missing capability to fail closed, got %v", err)
	}
}

func TestIAMAccessClientFromEnvUsesEstablishedCMSIdentity(t *testing.T) {
	t.Setenv("IAM_BASE_URL", "http://iam.test")
	t.Setenv("OPERATOR_IAM_ACCESS_SNAPSHOT_TOKEN", "")
	t.Setenv("CMS_SERVICE_TOKEN", "cms-machine-token")
	client, err := NewIAMAccessClientFromEnv()
	if err != nil || client.token != "cms-machine-token" {
		t.Fatalf("expected CMS machine identity fallback, got %v", err)
	}
}

func TestIAMAccessClientRejectsUnknownAndCrossTenantResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer machine-token" {
			t.Fatal("expected machine credential")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"user_id":"admin-1","tenant_id":"tenant-b","active":true,"roles":[],"permissions":[],"is_admin":false,"access_version":"v1","observed_at":"2026-07-31T00:00:00Z","email":"never"}`))
	}))
	defer server.Close()
	client, err := NewIAMAccessClient(server.URL, "machine-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Snapshot(context.Background(), "admin-1", "tenant-a"); !errors.Is(err, ErrAccessUnavailable) {
		t.Fatalf("expected cross-tenant/unknown-field response to fail closed, got %v", err)
	}
}

func TestIAMAccessClientUsesReadOnlySnapshotLookup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Fatalf("expected GET snapshot lookup, got %s", request.Method)
		}
		if request.URL.Path != "/internal/access/users/admin-1" || request.URL.Query().Get("tenant_id") != "tenant-a" {
			t.Fatalf("unexpected IAM snapshot endpoint: %s", request.URL.String())
		}
		if request.Header.Get("Authorization") != "Bearer machine-token" {
			t.Fatal("expected dedicated machine credential")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"user_id":"admin-1","tenant_id":"tenant-a","active":true,"roles":["admin"],"permissions":["feed:manage"],"is_admin":true,"access_version":"v1","observed_at":"2026-07-31T00:00:00Z"}`))
	}))
	defer server.Close()

	client, err := NewIAMAccessClient(server.URL, "machine-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.Snapshot(context.Background(), "admin-1", "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ObservedAt.IsZero() || snapshot.ObservedAt.Location() != time.UTC || !snapshot.HasPermission("feed:manage") {
		t.Fatalf("unexpected validated snapshot: %+v", snapshot)
	}
}

func TestIAMAccessClientRejectsMissingObservationTime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"user_id":"admin-1","tenant_id":"tenant-a","active":true,"roles":[],"permissions":[],"is_admin":false,"access_version":"v1"}`))
	}))
	defer server.Close()

	client, err := NewIAMAccessClient(server.URL, "machine-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Snapshot(context.Background(), "admin-1", "tenant-a"); !errors.Is(err, ErrAccessUnavailable) {
		t.Fatalf("expected missing observation time to fail closed, got %v", err)
	}
}

func TestAccessSnapshotPermissionChecks(t *testing.T) {
	snapshot := AccessSnapshot{Permissions: []string{"feed:*"}}
	if !snapshot.HasPermission("feed:manage") || snapshot.HasPermission("source:read") {
		t.Fatal("expected wildcard permission matching to remain resource-scoped")
	}
}
