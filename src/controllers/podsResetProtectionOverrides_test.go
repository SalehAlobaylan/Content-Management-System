package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"content-management-system/src/models"
	"content-management-system/src/podsreset"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestPodsResetProtectionOverrideScopeAndRevalidation(t *testing.T) {
	id, otherID := uuid.New(), uuid.New()
	protection := podsResetBlocker{Code: "approved_transcript", Message: "approved human caption"}
	work := podsResetBlocker{Code: "active_content_stage", Message: "work is not terminal"}
	storage := podsResetBlocker{Code: "storage_inventory_incomplete", Message: "inventory is unavailable"}
	targets := []podsResetPlanTarget{{ID: id, Blockers: []podsResetBlocker{protection, work, storage}, Decisions: podsreset.Decisions(false)},
		{ID: otherID, Blockers: []podsResetBlocker{}, Decisions: podsreset.Decisions(true)}}
	before, _ := json.Marshal(targets)
	applyPodsResetProtectionOverrides(targets, []string{"approved_transcript"})
	if len(targets[0].Blockers) != 2 || len(targets[0].OverriddenBlockers) != 1 || allPodsResetTargetsUnblocked(targets) {
		t.Fatalf("override bypassed work/storage or lost evidence: %+v", targets[0])
	}
	for _, decision := range targets[0].Decisions {
		if decision.Class == "editorial_transcript_or_chapter" && decision.Disposition != podsreset.DeleteOwnedPayload {
			t.Fatal("effective editorial decision did not record the explicit exception")
		}
	}
	after, _ := json.Marshal(targets)
	beforeHash, _ := podsResetManifestHash(before)
	afterHash, _ := podsResetManifestHash(after)
	if beforeHash == afterHash {
		t.Fatal("override was not bound to a different manifest")
	}
	manifest, _ := json.Marshal(map[string]any{"protection_override_codes": []string{"approved_transcript"}, "targets": targets})
	run := models.PodsResetRun{Manifest: datatypes.JSON(manifest)}
	current := []podsResetBlocker{protection, work, storage, {Code: "protected_content", Message: "new protection"}}
	remaining, err := podsResetRevalidateProtectionOverrides(run, id, current)
	if err != nil || len(remaining) != 3 || containsPodsResetBlocker(remaining, "approved_transcript") || !containsPodsResetBlocker(remaining, "protected_content") {
		t.Fatalf("new protection or hard blocker bypassed: %+v %v", remaining, err)
	}
	remaining, err = podsResetRevalidateProtectionOverrides(run, otherID, []podsResetBlocker{protection})
	if err != nil || len(remaining) != 1 {
		t.Fatal("override leaked into an item where the protection was not reviewed")
	}
	if _, err := podsResetRevalidateProtectionOverrides(run, uuid.New(), []podsResetBlocker{protection}); err == nil {
		t.Fatal("unselected item accepted override authority")
	}
	for _, code := range []string{"storage_inventory_incomplete", "active_stage_effect", "artifact_external_reference", "missing_retirement_identity", "future_unknown_protection"} {
		bad, _ := json.Marshal(map[string]any{"protection_override_codes": []string{code}, "targets": targets})
		if _, err := podsResetRevalidateProtectionOverrides(models.PodsResetRun{Manifest: bad}, id, current); err == nil {
			t.Fatalf("persisted unsupported override accepted: %s", code)
		}
	}
}

func TestPodsResetPreviewRejectsUnsupportedOverridesBeforeDatabaseAccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{
		`{"content_ids":["00000000-0000-4000-8000-000000000001"],"selection_reason":"old","protection_override_codes":["storage_inventory_incomplete"],"protection_override_reason":"force"}`,
		`{"content_ids":["00000000-0000-4000-8000-000000000001"],"selection_reason":"old","protection_override_codes":["approved_transcript"]}`,
		`{"content_ids":["00000000-0000-4000-8000-000000000001"],"selection_reason":"old","protection_override_codes":["active_stage_effect"],"protection_override_reason":"force"}`,
	} {
		response := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(response)
		c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "admin"})
		c.Request = httptest.NewRequest(http.MethodPost, "/plans", strings.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		CreatePodsResetPlan(c)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("unsupported/missing-reason override reached persistence: %d %s", response.Code, response.Body)
		}
	}
}
