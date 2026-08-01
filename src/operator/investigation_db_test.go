package operator

import (
	"context"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func investigationTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.OperatorThread{}, &models.OperatorInvestigation{}, &models.OperatorInvestigationEvent{}, &models.OperatorEvidence{}); err != nil {
		t.Fatalf("migrate investigation schema: %v", err)
	}
	clear := func() {
		_ = db.Exec("DELETE FROM operator_evidence").Error
		_ = db.Exec("DELETE FROM operator_investigation_events").Error
		_ = db.Exec("DELETE FROM operator_investigations").Error
		_ = db.Exec("DELETE FROM operator_threads").Error
	}
	clear()
	t.Cleanup(clear)
	return db
}

func testInvestigationRequest(t *testing.T) InvestigationRequest {
	t.Helper()
	request, err := NewInvestigationRequest(IntentExplain, "Why is this pending?", "fast")
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestInvestigationStorePersistsOrderedPacketAndResponse(t *testing.T) {
	db := investigationTestDB(t)
	now := time.Date(2026, time.July, 31, 0, 0, 0, 0, time.UTC)
	store := NewInvestigationStore(db)
	store.now = func() time.Time { return now }
	packet := catalogPacket()
	packet.CollectionStartedAt, packet.CollectionEndedAt = now, now
	packet.Fingerprint = fingerprintPacket(packet)
	investigation, err := store.Create(context.Background(), "tenant-a", "admin-a", "en", packet.VisibleContext, testInvestigationRequest(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	claimToken, err := store.Begin(context.Background(), investigation.ID, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistPacket(context.Background(), investigation.ID, claimToken, packet); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), investigation.ID, "tenant-a", claimToken, []ResponseBlock{{Kind: "fact", Text: "Current evidence is available.", EvidenceIDs: []string{"ev-1"}}}); err != nil {
		t.Fatal(err)
	}
	var events []models.OperatorInvestigationEvent
	if err := db.Where("investigation_id=?", investigation.ID).Order("sequence ASC").Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("expected accepted, context, packet, block, done events; got %#v", events)
	}
	for index, event := range events {
		if event.Sequence != int64(index+1) {
			t.Fatalf("event sequence is not durable: %#v", events)
		}
	}
	var evidence []models.OperatorEvidence
	if err := db.Where("investigation_id=? AND tenant_id=?", investigation.ID, "tenant-a").Find(&evidence).Error; err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 1 || evidence[0].EvidenceID != "ev-1" {
		t.Fatalf("packet evidence was not persisted: %#v", evidence)
	}
	var completed models.OperatorInvestigation
	if err := db.First(&completed, investigation.ID).Error; err != nil {
		t.Fatal(err)
	}
	if completed.State != "completed" || completed.PacketFingerprint != packet.Fingerprint {
		t.Fatalf("unexpected investigation state: %#v", completed)
	}
}

func TestInvestigationStoreRejectsResponseWithUnknownEvidence(t *testing.T) {
	db := investigationTestDB(t)
	store := NewInvestigationStore(db)
	packet := catalogPacket()
	investigation, err := store.Create(context.Background(), "tenant-a", "admin-a", "en", packet.VisibleContext, testInvestigationRequest(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	claimToken, err := store.Begin(context.Background(), investigation.ID, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistPacket(context.Background(), investigation.ID, claimToken, packet); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), investigation.ID, "tenant-a", claimToken, []ResponseBlock{{Kind: "fact", Text: "Untrusted", EvidenceIDs: []string{"not-in-packet"}}}); err == nil {
		t.Fatal("response must not cite unknown evidence")
	}
}

func TestInvestigationStoreReplaysOnlyOwnedEventsAfterCursor(t *testing.T) {
	db := investigationTestDB(t)
	store := NewInvestigationStore(db)
	packet := catalogPacket()
	investigation, err := store.Create(context.Background(), "tenant-a", "admin-a", "en", packet.VisibleContext, testInvestigationRequest(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	claimToken, err := store.Begin(context.Background(), investigation.ID, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistPacket(context.Background(), investigation.ID, claimToken, packet); err != nil {
		t.Fatal(err)
	}
	loaded, events, err := store.LoadEvents(context.Background(), investigation.PublicID.String(), "tenant-a", "admin-a", 1, 200)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ID != investigation.ID || len(events) != 2 || events[0].Sequence != 2 || events[1].Sequence != 3 {
		t.Fatalf("unexpected replay: %#v", events)
	}
	if _, _, err := store.LoadEvents(context.Background(), investigation.PublicID.String(), "tenant-a", "admin-b", 0, 200); err == nil {
		t.Fatal("another creator must not replay this investigation")
	}
}

func TestInvestigationStoreBackgroundedRunHasDurableLifecycle(t *testing.T) {
	db := investigationTestDB(t)
	store := NewInvestigationStore(db)
	packet := catalogPacket()
	investigation, err := store.Create(context.Background(), "tenant-a", "admin-a", "en", packet.VisibleContext, testInvestigationRequest(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.QueueBackground(context.Background(), investigation.ID, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	claimToken, err := store.Begin(context.Background(), investigation.ID, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistPacket(context.Background(), investigation.ID, claimToken, packet); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), investigation.ID, "tenant-a", claimToken, []ResponseBlock{{Kind: "fact", Text: "Current evidence is available.", EvidenceIDs: []string{"ev-1"}}}); err != nil {
		t.Fatal(err)
	}
	_, events, err := store.LoadEvents(context.Background(), investigation.PublicID.String(), "tenant-a", "admin-a", 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 6 || events[1].EventType != "backgrounded" || events[2].EventType != "context_collecting" || events[len(events)-1].EventType != "done" {
		t.Fatalf("background lifecycle is not replayable: %#v", events)
	}
}

func TestInvestigationStoreRedactsAndFencesExpiredWorkerLease(t *testing.T) {
	db := investigationTestDB(t)
	store := NewInvestigationStore(db)
	now := time.Date(2026, time.July, 31, 2, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	packet := catalogPacket()
	request, err := NewInvestigationRequest(IntentExplain, "Authorization: super-secret-value Bearer abcdefghijklmnop", "fast")
	if err != nil {
		t.Fatal(err)
	}
	investigation, err := store.Create(context.Background(), "tenant-a", "admin-a", "en", packet.VisibleContext, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(investigation.Request) == "" || string(investigation.Request) == "{}" || strings.Contains(string(investigation.Request), "super-secret-value") || strings.Contains(string(investigation.Request), "abcdefghijklmnop") {
		t.Fatalf("request envelope was not safely persisted: %s", investigation.Request)
	}
	if err := store.QueueBackground(context.Background(), investigation.ID, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	firstClaim, err := store.Begin(context.Background(), investigation.ID, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(investigationClaimLease + time.Second)
	if recovered, err := store.RecoverExpiredClaims(context.Background()); err != nil || recovered != 1 {
		t.Fatalf("expected exactly one recovered claim, got recovered=%d err=%v", recovered, err)
	}
	secondClaim, err := store.Begin(context.Background(), investigation.ID, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if firstClaim == secondClaim {
		t.Fatal("recovered worker must receive a fresh claim token")
	}
	if err := store.PersistPacket(context.Background(), investigation.ID, firstClaim, packet); err == nil {
		t.Fatal("expired worker token must not persist a packet")
	}
	if err := store.FailClaim(context.Background(), investigation.ID, "tenant-a", firstClaim, "stale_worker"); err == nil {
		t.Fatal("a stale worker must not fail the successor claim")
	}
	if err := store.PersistPacket(context.Background(), investigation.ID, secondClaim, packet); err != nil {
		t.Fatalf("current claim must remain usable after stale worker failure: %v", err)
	}
}

func TestInvestigationStoreRejectsCrossTenantPacket(t *testing.T) {
	db := investigationTestDB(t)
	store := NewInvestigationStore(db)
	packet := catalogPacket()
	investigation, err := store.Create(context.Background(), "tenant-a", "admin-a", "en", packet.VisibleContext, testInvestigationRequest(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	packet.TenantID = "tenant-b"
	if err := store.PersistPacket(context.Background(), investigation.ID, uuid.New(), packet); err == nil {
		t.Fatal("cross-tenant packet must not persist")
	}
}

func TestInvestigationStoreInboxIsCreatorBoundAndAcknowledgable(t *testing.T) {
	db := investigationTestDB(t)
	store := NewInvestigationStore(db)
	packet := catalogPacket()
	investigation, err := store.Create(context.Background(), "tenant-a", "admin-a", "en", packet.VisibleContext, testInvestigationRequest(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.QueueBackground(context.Background(), investigation.ID, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	items, unread, err := store.ListInbox(context.Background(), "tenant-a", "admin-a", 20)
	if err != nil || len(items) != 1 || unread != 1 || items[0].PublicID != investigation.PublicID {
		t.Fatalf("unexpected inbox items=%#v unread=%d err=%v", items, unread, err)
	}
	if _, unread, err := store.ListInbox(context.Background(), "tenant-a", "admin-b", 20); err != nil || unread != 0 {
		t.Fatalf("another actor must not see inbox work, unread=%d err=%v", unread, err)
	}
	read, err := store.MarkInboxRead(context.Background(), investigation.PublicID.String(), "tenant-a", "admin-a")
	if err != nil || read.ReadAt == nil {
		t.Fatalf("expected durable inbox acknowledgement, item=%+v err=%v", read, err)
	}
	_, unread, err = store.ListInbox(context.Background(), "tenant-a", "admin-a", 20)
	if err != nil || unread != 0 {
		t.Fatalf("read inbox item must no longer be unread, unread=%d err=%v", unread, err)
	}
}
