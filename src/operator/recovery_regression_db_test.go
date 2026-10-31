package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"gorm.io/gorm"
)

// Canonical Operator DDL pins the real evidence uniqueness, recommendation
// state checks and feedback FKs that AutoMigrate-only fixtures do not enforce.
func operatorRegressionDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("requires an explicitly guarded disposable CMS database")
	}
	db := testdb.Open(t)
	for _, name := range []string{"20260730200000_wahb_operator_foundation.sql", "20260730210000_wahb_operator_execution.sql", "20260731002000_operator_investigation_leases.sql", "20260731003000_operator_inbox.sql", "20260731041000_wahb_operator_recommendation_rank.sql", "20260812110000_operator_conversation_workspace.sql"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(string(raw)).Error; err != nil {
			t.Fatalf("canonical %s: %v", name, err)
		}
	}
	return db
}

type forbiddenGreetingReasoner struct{ calls int }

func (r *forbiddenGreetingReasoner) Reason(context.Context, ReasonRequest) (ReasonResponse, error) {
	r.calls++
	return ReasonResponse{}, fmt.Errorf("greetings must not invoke a paid reasoner")
}

func TestOperatorGreetingAndRecoveryRegression(t *testing.T) {
	db := operatorRegressionDB(t)
	ctx := context.Background()
	for _, locale := range []string{"en", "ar"} {
		t.Run("greeting_"+locale, func(t *testing.T) {
			thread, err := NewConversationStore(db).CreateThread(ctx, "tenant-a", "admin-a", "Greeting verification", locale)
			if err != nil {
				t.Fatal(err)
			}
			reasoner := &forbiddenGreetingReasoner{}
			// No domain tables exist in this fixture and the fabric has no DB handle.
			// Any domain collection or retrieval would fail instead of hiding a read.
			c := NewInvestigationCoordinator(NewContextFabric(nil, DefaultAdapterRegistry()), NewInvestigationStore(db), reasoner)
			result, err := c.Run(ctx, approvalAccess(), RuntimePolicy{ReadEnabled: true, LLMEnabled: true}, InvestigationInput{VisibleContext: mediaCirculationVisibleContext(), Intent: IntentInvestigate, Locale: locale, Message: "hello?", Tier: "fast", ThreadID: &thread.ID})
			if err != nil {
				t.Fatal(err)
			}
			if reasoner.calls != 0 || result.Degraded || len(result.Response.Blocks) != 1 || result.Response.Blocks[0].Kind != "message" || len(result.Packet.Evidence) != 0 {
				t.Fatalf("greeting triggered operational work: %+v", result)
			}
			var saved models.OperatorInvestigation
			if err := db.First(&saved, result.Investigation.ID).Error; err != nil {
				t.Fatal(err)
			}
			if saved.State != "completed" {
				t.Fatalf("greeting state=%s", saved.State)
			}
			var messages []models.OperatorMessage
			if err := db.Where("thread_id=?", thread.ID).Order("id").Find(&messages).Error; err != nil {
				t.Fatal(err)
			}
			if len(messages) != 2 || messages[0].ActorType != "admin" || messages[1].ActorType != "operator" {
				t.Fatal("greeting was not durably delivered to its case")
			}
			var planCount int64
			if err := db.Model(&models.OperatorActionPlan{}).Count(&planCount).Error; err != nil || planCount != 0 {
				t.Fatal("greeting created an action plan")
			}
		})
	}
	t.Run("recovery_after_packet_commit", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		store := NewInvestigationStore(db)
		store.now = func() time.Time { return now }
		packet := catalogPacket()
		packet.Evidence[0].ContentHash = strings.Repeat("a", 64)
		packet.Recommendations = []Recommendation{{ID: "inspect:feed_integrity", Kind: "inspect", Title: "Review snapshot", Summary: "Review the saved evidence", DeepLink: "/platform/feed-integrity", EvidenceIDs: []string{"ev-1"}}}
		packet.Fingerprint = fingerprintPacket(packet)
		thread, err := NewConversationStore(db).CreateThread(ctx, "tenant-a", "admin-a", "Recovery verification", "en")
		if err != nil {
			t.Fatal(err)
		}
		request, err := NewInvestigationRequest(IntentResolve, "What is blocked?", "fast")
		if err != nil {
			t.Fatal(err)
		}
		investigation, err := store.Create(ctx, "tenant-a", "admin-a", "en", packet.VisibleContext, request, &thread.ID)
		if err != nil {
			t.Fatal(err)
		}
		first, err := store.Begin(ctx, investigation.ID, "tenant-a")
		if err != nil {
			t.Fatal(err)
		}
		if err = store.PersistPacket(ctx, investigation.ID, first, packet); err != nil {
			t.Fatal(err)
		}
		if err = store.PersistPacket(ctx, investigation.ID, first, packet); err != nil {
			t.Fatalf("same snapshot replay: %v", err)
		}
		var oldRecommendation models.OperatorRecommendation
		if err = db.Where("investigation_id=?", investigation.ID).First(&oldRecommendation).Error; err != nil {
			t.Fatal(err)
		}
		feedback := models.OperatorRecommendationFeedback{RecommendationID: oldRecommendation.ID, TenantID: "tenant-a", ActorID: "admin-a", FeedbackKind: "dismiss", Reason: "Prior snapshot"}
		if err = db.Create(&feedback).Error; err != nil {
			t.Fatal(err)
		}
		oldFingerprint := packet.Fingerprint
		now = now.Add(investigationClaimLease + time.Second)
		if n, err := store.RecoverExpiredClaims(ctx); err != nil || n != 1 {
			t.Fatalf("recovery n=%d err=%v", n, err)
		}
		second, err := store.Begin(ctx, investigation.ID, "tenant-a")
		if err != nil {
			t.Fatal(err)
		}
		packet.Evidence[0].ContentHash = strings.Repeat("b", 64)
		packet.Evidence[0].ObservedAt = now
		packet.Evidence[0].FetchedAt = now
		packet.Evidence[0].ExpiresAt = now.Add(time.Minute)
		packet.Fingerprint = fingerprintPacket(packet)
		if err = store.PersistPacket(ctx, investigation.ID, first, packet); err == nil {
			t.Fatal("expired worker changed the recovered packet")
		}
		if err = store.PersistPacket(ctx, investigation.ID, second, packet); err != nil {
			t.Fatalf("fresh retry duplicated committed evidence: %v", err)
		}
		var evidence []models.OperatorEvidence
		if err = db.Where("investigation_id=?", investigation.ID).Find(&evidence).Error; err != nil {
			t.Fatal(err)
		}
		if len(evidence) != 1 || evidence[0].ContentHash != strings.Repeat("b", 64) {
			t.Fatal("recovery did not replace only its working snapshot")
		}
		var old models.OperatorRecommendation
		if err = db.First(&old, oldRecommendation.ID).Error; err != nil {
			t.Fatal(err)
		}
		if old.State != "expired" || old.InvestigationID != nil || old.Rank != 0 {
			t.Fatal("the old recommendation remained actionable")
		}
		var kept models.OperatorRecommendationFeedback
		if err = db.First(&kept, feedback.ID).Error; err != nil {
			t.Fatal("recovery deleted recommendation feedback")
		}
		var superseded models.OperatorInvestigationEvent
		if err = db.Where("investigation_id=? AND event_type='packet_superseded'", investigation.ID).First(&superseded).Error; err != nil {
			t.Fatal(err)
		}
		var receipt struct {
			Fingerprint string                    `json:"packet_fingerprint"`
			Evidence    []models.OperatorEvidence `json:"evidence"`
		}
		if err = json.Unmarshal(superseded.Payload, &receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.Fingerprint != oldFingerprint || len(receipt.Evidence) != 1 || receipt.Evidence[0].ContentHash != strings.Repeat("a", 64) {
			t.Fatalf("prior proof was not preserved: fingerprint=%q want=%q evidence=%+v", receipt.Fingerprint, oldFingerprint, receipt.Evidence)
		}
		primary := "inspect:feed_integrity"
		if err = store.RankRecommendations(ctx, investigation.ID, "tenant-a", &primary, nil); err != nil {
			t.Fatal(err)
		}
		blocks := []ResponseBlock{{Kind: "fact", Text: "The refreshed snapshot is recorded.", EvidenceIDs: []string{"ev-1"}}}
		for i := 0; i < 30; i++ {
			blocks = append(blocks, ResponseBlock{Kind: "unknown", Text: fmt.Sprintf("Unknown detail %d remains unverified.", i)})
		}
		if err = store.Complete(ctx, investigation.ID, "tenant-a", second, blocks); err != nil {
			t.Fatal(err)
		}
		var events []models.OperatorInvestigationEvent
		if err = db.Where("investigation_id=?", investigation.ID).Order("sequence").Find(&events).Error; err != nil {
			t.Fatal(err)
		}
		responseCount := 0
		for i, event := range events {
			if event.Sequence != int64(i+1) {
				t.Fatal("batched response broke the durable event order")
			}
			if event.EventType == "response_block" {
				responseCount++
			}
		}
		if responseCount != len(blocks) || events[len(events)-1].EventType != "done" {
			t.Fatal("long reply was not atomically completed")
		}
		var completed models.OperatorInvestigation
		if err = db.First(&completed, investigation.ID).Error; err != nil || completed.State != "completed" {
			t.Fatal("recovered investigation did not complete")
		}
	})
}
