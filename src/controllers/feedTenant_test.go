package controllers

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestTrustedPublicFeedTenantUsesServerConfigurationAndRejectsClaimMismatch(t *testing.T) {
	t.Setenv("DEFAULT_TENANT_ID", "tenant-a")
	c, _ := gin.CreateTestContext(nil)
	if tenant, err := trustedPublicFeedTenant(c); err != nil || tenant != "tenant-a" {
		t.Fatalf("anonymous public tenant resolution failed: tenant=%q err=%v", tenant, err)
	}
	c.Set("user_id", "user-a")
	c.Set("tenant_id", "tenant-b")
	if _, err := trustedPublicFeedTenant(c); err == nil {
		t.Fatal("authenticated tenant mismatch must fail closed")
	}
	c.Set("tenant_id", "tenant-a")
	if tenant, err := trustedPublicFeedTenant(c); err != nil || tenant != "tenant-a" {
		t.Fatalf("matching authenticated tenant rejected: tenant=%q err=%v", tenant, err)
	}
}

func TestTrustedPublicFeedTenantRejectsMissingConfiguration(t *testing.T) {
	t.Setenv("DEFAULT_TENANT_ID", "")
	c, _ := gin.CreateTestContext(nil)
	if _, err := trustedPublicFeedTenant(c); err == nil {
		t.Fatal("public feeds must not infer the default tenant")
	}
}
