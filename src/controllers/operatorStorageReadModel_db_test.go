package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"gorm.io/gorm"
)

func TestOperatorStorageOwnerReadIsTenantScopedAndReadOnly(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("requires a guarded disposable CMS database")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.StoragePolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TABLE content_items (id bigserial PRIMARY KEY, tenant_id text, type text, status text, file_size_bytes bigint); INSERT INTO content_items (tenant_id,type,status,file_size_bytes) VALUES ('tenant-a','VIDEO','READY',1024),('tenant-a','PODCAST','PENDING',512),('tenant-a','VIDEO','ARCHIVED',99999),('tenant-b','VIDEO','READY',88888)`).Error; err != nil {
		t.Fatal(err)
	}
	global := models.StoragePolicy{MaxStorageBytes: 10240}
	tenant := "tenant-b"
	if err := db.Create(&global).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.StoragePolicy{TenantID: &tenant, MaxStorageBytes: 500, Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	unavailable := false
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/storage/stats" || r.Header.Get("Authorization") != "Bearer storage-test-secret" {
			t.Errorf("unsafe owner read: %s", r.URL.Path)
			w.WriteHeader(401)
			return
		}
		if unavailable {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"used_bytes":4096,"object_count":7,"by_artifact_type":{},"cold_enabled":false}`))
	}))
	defer owner.Close()
	t.Setenv("AGGREGATION_BASE_URL", owner.URL)
	t.Setenv("AGGREGATION_SERVICE_TOKEN", "storage-test-secret")
	check := func(tx *gorm.DB) error {
		if err := tx.Exec("SET TRANSACTION READ ONLY").Error; err != nil {
			return err
		}
		snapshot, unknowns, err := readOperatorStorageSnapshot(context.Background(), tx, "tenant-a")
		if err != nil {
			return err
		}
		if snapshot.TrackedBytes != 1536 || snapshot.TrackedItems != 2 || snapshot.PolicyLimitBytes == nil || *snapshot.PolicyLimitBytes != 10240 || snapshot.PolicyScope != "platform_global" || snapshot.CMSScope != "tenant" || snapshot.CleanupTargetPct == nil || *snapshot.CleanupTargetPct != 80 {
			t.Fatalf("scope or global-policy inheritance failed: %+v", snapshot)
		}
		if unavailable {
			if snapshot.ProviderUsedBytes != nil || len(unknowns) == 0 {
				t.Fatal("unavailable provider was presented as zero")
			}
		} else if snapshot.ProviderUsedBytes == nil || *snapshot.ProviderUsedBytes != 4096 || snapshot.ProviderObjectCount == nil || *snapshot.ProviderObjectCount != 7 {
			t.Fatal("owner measurements are absent")
		}
		return nil
	}
	if err := db.Transaction(check); err != nil {
		t.Fatal(err)
	}
	unavailable = true
	if err := db.Transaction(check); err != nil {
		t.Fatal(err)
	}
}
