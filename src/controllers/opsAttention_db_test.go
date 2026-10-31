package controllers

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
)

func TestOpsAttentionMutationsRequireTheDisplayedEvidence(t *testing.T) {
	db := opsTestDB(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	incident := models.ExperienceIncident{TenantID: "ops-fixture", ClientPlatform: "ios", Fingerprint: "review-race", MetricKey: "playback_start_success", Surface: "pods", CohortDim: "global", CohortVal: "all", Severity: "degraded", Status: "open", Summary: "Playback needs review", FirstSeenAt: now, LastSeenAt: now}
	if err := db.Create(&incident).Error; err != nil {
		t.Fatal(err)
	}
	key := opsAttentionKey("experience", "episode", incident.TenantID, incident.PublicID.String())
	fingerprint := opsFingerprint("experience", "episode", incident.TenantID, incident.PublicID.String())
	review := gin.H{"key": key, "fingerprint": fingerprint, "severity": "major", "count": 1, "ttl_minutes": 120}
	request := func(body gin.H, handler gin.HandlerFunc) int {
		t.Helper()
		raw, _ := json.Marshal(body)
		return opsFixtureRequest(db, http.MethodPost, "/attention", raw, handler).Code
	}
	if code := request(gin.H{"key": key}, AckOpsAttention); code != 400 {
		t.Fatalf("unreviewed ack returned %d", code)
	}
	if code := request(review, AckOpsAttention); code != 200 {
		t.Fatalf("reviewed ack returned %d", code)
	}
	var before models.OpsAttentionState
	if err := db.Where("tenant_id=? AND attention_key=?", "ops-fixture", key).First(&before).Error; err != nil {
		t.Fatal(err)
	}
	if before.State != "acked" || before.BaselineSeverity != "major" {
		t.Fatalf("ack baseline lost: %+v", before)
	}
	// An owner escalation arrives after the browser read but before its click.
	if err := db.Model(&incident).Update("severity", "critical").Error; err != nil {
		t.Fatal(err)
	}
	if code := request(review, SnoozeOpsAttention); code != 409 {
		t.Fatalf("old browser row suppressed a newer escalation: %d", code)
	}
	var unchanged models.OpsAttentionState
	if err := db.First(&unchanged, before.ID).Error; err != nil {
		t.Fatal(err)
	}
	if unchanged.State != "acked" || unchanged.BaselineSeverity != "major" || unchanged.SnoozedUntil != nil {
		t.Fatal("conflict changed the previous baseline")
	}
	review["severity"] = "critical"
	for _, change := range []gin.H{{"count": 2}, {"fingerprint": "older-evidence"}} {
		stale := gin.H{}
		for k, v := range review {
			stale[k] = v
		}
		for k, v := range change {
			stale[k] = v
		}
		if code := request(stale, SnoozeOpsAttention); code != 409 {
			t.Fatalf("changed count/evidence was accepted: %d", code)
		}
	}
	if code := request(review, SnoozeOpsAttention); code != 200 {
		t.Fatalf("fresh snooze returned %d", code)
	}
	var snoozed models.OpsAttentionState
	if err := db.First(&snoozed, before.ID).Error; err != nil {
		t.Fatal(err)
	}
	if snoozed.State != "snoozed" || snoozed.BaselineSeverity != "critical" || snoozed.SnoozedUntil == nil || !snoozed.SnoozedUntil.After(now) {
		t.Fatal("fresh snooze did not record the reviewed escalation")
	}
	if code := request(gin.H{"key": key}, ClearOpsAttentionState); code != 204 {
		t.Fatalf("restore required stale evidence: %d", code)
	}
	var count int64
	if err := db.Model(&models.OpsAttentionState{}).Where("tenant_id=? AND attention_key=?", "ops-fixture", key).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("restore did not clear the team state")
	}
}
