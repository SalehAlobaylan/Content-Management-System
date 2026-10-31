package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
)

type failedStorageReasoner struct{ calls int }

func (r *failedStorageReasoner) Reason(context.Context, ReasonRequest) (ReasonResponse, error) {
	r.calls++
	return ReasonResponse{}, fmt.Errorf("reasoning unavailable")
}

type storageCaveatReasoner struct{}

func (storageCaveatReasoner) Reason(_ context.Context, input ReasonRequest) (ReasonResponse, error) {
	return ReasonResponse{SchemaVersion: ContractVersion, Language: input.Language, TaskKind: input.TaskKind, Tier: input.Tier, Provider: "test", Model: "test", Cache: "disabled", Blocks: []ResponseBlock{{Kind: "conclusion", Text: "Storage measurements are available.", EvidenceIDs: []string{input.Packet.Evidence[0].EvidenceID}}}, Uncertainties: []string{"Current cleanup activity is unverified."}}, nil
}

func TestOperatorStorageQuestionUsesResolvedEvidenceAndFallback(t *testing.T) {
	db := operatorRegressionDB(t)
	if err := db.AutoMigrate(&models.StoragePolicy{}, &models.StorageSweepRun{}, &models.QualityProfile{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.StoragePolicy{MaxStorageBytes: 10240}).Error; err != nil {
		t.Fatal(err)
	}
	oldVersion := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	if err := db.Model(&models.StoragePolicy{}).Where("tenant_id IS NULL").UpdateColumn("updated_at", oldVersion).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.QualityProfile{Name: "Default"}).Error; err != nil {
		t.Fatal(err)
	}
	fabric := NewContextFabric(db, DefaultAdapterRegistry()).WithStorageReader(func(ctx context.Context, tenant string) (StorageSnapshot, []string, error) {
		if tenant != "tenant-a" {
			t.Fatal("wrong storage tenant")
		}
		provider, objects, quota := int64(4096), int64(7), int64(10240)
		return StorageSnapshot{TrackedBytes: 1536, TrackedItems: 2, ProviderUsedBytes: &provider, ProviderObjectCount: &objects, ProviderScope: "platform_global", PolicyLimitBytes: &quota, PolicyScope: "platform_global"}, nil, nil
	})
	reasoner := &failedStorageReasoner{}
	coordinator := NewInvestigationCoordinator(fabric, NewInvestigationStore(db), reasoner)
	input := InvestigationInput{VisibleContext: VisibleContext{SchemaVersion: ContractVersion, Domain: "global_ops", View: "operations", Filters: map[string]any{}, Subjects: []SubjectRef{}, AvailableIntents: []Intent{IntentExplain}}, Intent: IntentExplain, Locale: "en", Message: "what the status of the storage", Tier: "fast"}
	result, err := coordinator.Run(context.Background(), approvalAccess(), RuntimePolicy{ReadEnabled: true, LLMEnabled: true}, input)
	if err != nil {
		t.Fatal(err)
	}
	if reasoner.calls != 1 || !result.Degraded || result.Packet.VisibleContext.Domain != "storage_quality" {
		t.Fatalf("question used the wrong owner: %+v", result)
	}
	for _, fact := range result.Packet.Facts {
		if fact.Key == "storage_quality.policy" {
			row := fact.Value.(map[string]any)
			if _, exists := row["class_a_cap_pct"]; exists {
				t.Fatal("request-count thresholds leaked into the byte-usage projection")
			}
		}
	}
	for _, ref := range result.Packet.Evidence {
		if strings.Contains(ref.EvidenceID, ":policy:") && (ref.Availability != EvidenceAvailable || ref.SourceVersion != oldVersion.Format(time.RFC3339Nano)) {
			t.Fatal("a freshly read stored policy was confused with an old measurement")
		}
	}
	var saved models.OperatorInvestigation
	if err := db.First(&saved, result.Investigation.ID).Error; err != nil {
		t.Fatal(err)
	}
	var visible VisibleContext
	if err := json.Unmarshal(saved.VisibleContext, &visible); err != nil {
		t.Fatal(err)
	}
	if saved.State != "completed" || visible.Domain != "storage_quality" {
		t.Fatal("resolved context and completed answer were not saved")
	}
	text := ""
	for _, block := range result.Response.Blocks {
		text += block.Text + "\n"
	}
	if !strings.Contains(text, "1.50 KiB") || !strings.Contains(text, "4.00 KiB") || strings.Contains(text, "global_ops") {
		t.Fatal(text)
	}
	var plans int64
	if err := db.Model(&models.OperatorActionPlan{}).Count(&plans).Error; err != nil || plans != 0 {
		t.Fatal("storage status created an action plan")
	}
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("missing_reasoner_enabled_%t", enabled), func(t *testing.T) {
			coordinator := NewInvestigationCoordinator(fabric, NewInvestigationStore(db), nil)
			result, err := coordinator.Run(context.Background(), approvalAccess(), RuntimePolicy{ReadEnabled: true, LLMEnabled: enabled}, input)
			if err != nil {
				t.Fatal(err)
			}
			expected := "AI interpretation is turned off."
			if enabled {
				expected = "AI interpretation is unavailable."
			}
			if !result.Degraded || !strings.HasPrefix(result.Response.Blocks[len(result.Response.Blocks)-1].Text, expected) {
				t.Fatalf("fallback misreported policy versus availability: %+v", result.Response.Blocks)
			}
		})
	}
	t.Run("separate_caveat_survives_conversation_persistence", func(t *testing.T) {
		thread, err := NewConversationStore(db).CreateThread(context.Background(), "tenant-a", "admin-a", "Storage caveat", "en")
		if err != nil {
			t.Fatal(err)
		}
		draft := input
		draft.ThreadID = &thread.ID
		coordinator := NewInvestigationCoordinator(fabric, NewInvestigationStore(db), storageCaveatReasoner{})
		result, err := coordinator.Run(context.Background(), approvalAccess(), RuntimePolicy{ReadEnabled: true, LLMEnabled: true}, draft)
		if err != nil {
			t.Fatal(err)
		}
		var message models.OperatorMessage
		if err := db.Where("investigation_id=? AND actor_type='operator'", result.Investigation.ID).First(&message).Error; err != nil {
			t.Fatal(err)
		}
		var saved struct{ Blocks []ResponseBlock }
		if err := json.Unmarshal(message.Content, &saved); err != nil {
			t.Fatal(err)
		}
		if len(saved.Blocks) != 2 || saved.Blocks[1].Kind != "unknown" || saved.Blocks[1].Text != "Current cleanup activity is unverified." {
			t.Fatalf("durable answer lost its separate caveat: %s", message.Content)
		}
	})
}
