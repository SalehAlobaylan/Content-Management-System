package operator

import (
	"context"
	"testing"
	"time"

	"content-management-system/src/models"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func newTestConversationStore(db *gorm.DB) *ConversationStore {
	store := NewConversationStore(db)
	store.now = func() time.Time { return time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC) }
	return store
}

func TestConversationDeletionDoesNotTouchActionLedger(t *testing.T) {
	db := operatorStoreTestDB(t)
	conversation := newTestConversationStore(db)
	plans := newTestPlanStore(db)
	thread, err := conversation.CreateThread(context.Background(), "tenant-a", "admin-a", "Investigate handoff", "en")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conversation.AppendMessage(context.Background(), thread.ID, "tenant-a", "admin-a", "admin", "admin-a", map[string]any{"text": "why pending?"}); err != nil {
		t.Fatal(err)
	}
	investigation := models.OperatorInvestigation{TenantID: "tenant-a", ThreadID: &thread.ID, ActorID: "admin-a", State: "completed", VisibleContext: datatypes.JSON([]byte(`{}`)), Locale: "en", StartedAt: conversation.now(), ExpiresAt: conversation.now().Add(conversationRetention)}
	if err := db.Create(&investigation).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.OperatorEvidence{InvestigationID: investigation.ID, TenantID: "tenant-a", EvidenceID: "evidence-1", Authority: "live", Domain: "media", AdapterKey: "test", AdapterVersion: "v1", RequiredPermission: "media:read", RecordRefs: datatypes.JSON([]byte(`[]`)), DeepLink: "/platform/media/sources", ObservedAt: conversation.now(), FetchedAt: conversation.now(), MaxAgeSeconds: 60, ExpiresAt: conversation.now().Add(time.Minute), ContentHash: "hash", SourceVersion: "v1", Availability: "available"}).Error; err != nil {
		t.Fatal(err)
	}
	plan, err := plans.CreatePlan(context.Background(), samplePlan(), plans.now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := conversation.DeleteThreadContent(context.Background(), thread.PublicID, "tenant-a", "admin-a"); err != nil {
		t.Fatal(err)
	}
	for name, model := range map[string]any{"thread": &models.OperatorThread{}, "message": &models.OperatorMessage{}, "investigation": &models.OperatorInvestigation{}, "evidence": &models.OperatorEvidence{}} {
		var count int64
		if err := db.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("%s remaining=%d err=%v", name, count, err)
		}
	}
	var events int64
	if err := db.Model(&models.OperatorPlanEvent{}).Where("plan_id=?", plan.ID).Count(&events).Error; err != nil || events != 1 {
		t.Fatalf("plan ledger must survive chat deletion; events=%d err=%v", events, err)
	}
}

func TestConversationMessageRefreshesRetentionAndSweepDeletesExpired(t *testing.T) {
	db := operatorStoreTestDB(t)
	store := newTestConversationStore(db)
	thread, err := store.CreateThread(context.Background(), "tenant-a", "admin-a", "", "en")
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC) }
	if _, err := store.AppendMessage(context.Background(), thread.ID, "tenant-a", "admin-a", "operator", "", map[string]any{"text": "Current state"}); err != nil {
		t.Fatal(err)
	}
	var refreshed models.OperatorThread
	if err := db.First(&refreshed, thread.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !refreshed.ExpiresAt.Equal(store.now().Add(conversationRetention)) {
		t.Fatalf("expires_at=%s, want refreshed retention", refreshed.ExpiresAt)
	}
	store.now = func() time.Time { return time.Date(2026, time.September, 2, 12, 0, 0, 0, time.UTC) }
	result, err := store.SweepExpiredConversationContent(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.ThreadsDeleted != 1 {
		t.Fatalf("threads deleted=%d, want 1", result.ThreadsDeleted)
	}
}
