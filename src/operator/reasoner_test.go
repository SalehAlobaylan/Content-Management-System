package operator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"content-management-system/src/models"
)

func validReasonRequest() ReasonRequest {
	packet := catalogPacket()
	return ReasonRequest{SchemaVersion: ContractVersion, InvestigationID: "investigation-1", ThreadID: "thread-1", TaskKind: IntentExplain, Language: "en", Tier: "fast", AdminMessage: "Explain this.", Packet: packet, AdvertisedActions: []AdvertisedTool{{Key: "feed_integrity.refresh_snapshot", TargetIDs: []string{"today"}}}}
}

func TestHTTPReasonerValidatesServiceResponseAndObservedProvenance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/operator/reason" || r.Header.Get("Authorization") != "Bearer capability" {
			t.Fatalf("unexpected reasoner request: %s %q", r.URL.Path, r.Header.Get("Authorization"))
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request["schema_version"] != ContractVersion {
			t.Fatalf("unexpected request: %#v", request)
		}
		if actions, ok := request["advertised_actions"].([]any); !ok || len(actions) != 1 {
			t.Fatalf("advertised actions must be an array, got %#v", request["advertised_actions"])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": ContractVersion, "language": "en", "task_kind": "explain", "blocks": []map[string]any{{"kind": "fact", "text": "Context is current.", "evidence_ids": []string{"ev-1"}}}, "secondary_recommendation_ids": []string{}, "uncertainties": []string{}, "provider": "test", "model": "test-model", "tier": "fast", "fallback_used": false, "cache": "disabled"})
	}))
	defer server.Close()
	reasoner, err := NewHTTPReasoner(server.URL, "capability", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := reasoner.Reason(context.Background(), validReasonRequest())
	if err != nil {
		t.Fatal(err)
	}
	if response.Provider != "test" || len(response.Blocks) != 1 {
		t.Fatalf("unexpected reasoner response: %#v", response)
	}
}

func TestHTTPReasonerRejectsInventedEvidenceAndCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": ContractVersion, "language": "en", "task_kind": "explain", "blocks": []map[string]any{{"kind": "fact", "text": "Invented", "evidence_ids": []string{"not-in-packet"}}}, "secondary_recommendation_ids": []string{}, "uncertainties": []string{}, "provider": "test", "model": "test-model", "tier": "fast", "fallback_used": false, "cache": "hit"})
	}))
	defer server.Close()
	reasoner, err := NewHTTPReasoner(server.URL, "capability", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reasoner.Reason(context.Background(), validReasonRequest()); err == nil {
		t.Fatal("cached or invented response must be rejected")
	}
}

func TestHTTPReasonerRejectsUnadvertisedRecommendation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": ContractVersion, "language": "en", "task_kind": "explain", "blocks": []map[string]any{{"kind": "fact", "text": "Current.", "evidence_ids": []string{"ev-1"}}}, "primary_recommendation_id": "invented", "secondary_recommendation_ids": []string{}, "uncertainties": []string{}, "provider": "test", "model": "test-model", "tier": "fast", "fallback_used": false, "cache": "disabled"})
	}))
	defer server.Close()
	reasoner, err := NewHTTPReasoner(server.URL, "capability", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reasoner.Reason(context.Background(), validReasonRequest()); err == nil {
		t.Fatal("unadvertised recommendation must be rejected")
	}
}

func TestDeterministicDegradedBlocksPreserveUnknowns(t *testing.T) {
	packet := catalogPacket()
	packet.Unknowns = []string{"Queue acceptance is not proven."}
	blocks := DeterministicDegradedBlocks(packet, "LLM is unavailable.")
	if len(blocks) != 3 || blocks[0].EvidenceIDs[0] != "ev-1" || blocks[1].Kind != "unknown" || blocks[2].Kind != "degraded" {
		t.Fatalf("unexpected degraded blocks: %#v", blocks)
	}
}

func TestHTTPReasonerNormalizesEmptyAdvertisedArrays(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"advertised_recommendation_ids", "advertised_actions", "data_categories"} {
			if value, ok := request[field]; !ok || reflect.TypeOf(value).Kind() != reflect.Slice {
				t.Fatalf("%s must serialize as an array, got %#v", field, value)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": ContractVersion, "language": "en", "task_kind": "explain", "blocks": []map[string]any{{"kind": "fact", "text": "Context is current.", "evidence_ids": []string{"ev-1"}}}, "secondary_recommendation_ids": []string{}, "uncertainties": []string{}, "provider": "test", "model": "test-model", "tier": "fast", "fallback_used": false, "cache": "disabled"})
	}))
	defer server.Close()
	reasoner, err := NewHTTPReasoner(server.URL, "capability", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	request := validReasonRequest()
	request.AdvertisedActions = nil
	request.AdvertisedRecommendationIDs = nil
	request.DataCategories = nil
	if _, err := reasoner.Reason(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func TestDeterministicDegradedBlocksUseArabicForArabicThreads(t *testing.T) {
	packet := catalogPacket()
	packet.Unknowns = []string{"Queue acceptance is not proven."}
	blocks := DeterministicDegradedBlocksForLocale(packet, "ar", "استدلال الذكاء الاصطناعي غير متاح حالياً؛ تظل حزمة أدلة CMS متاحة.")
	if len(blocks) != 3 || blocks[0].Text != "جمع CMS السياق التشغيلي الحالي من السجلات المسجّلة." || blocks[1].Text == packet.Unknowns[0] || blocks[2].Kind != "degraded" {
		t.Fatalf("unexpected Arabic degraded blocks: %#v", blocks)
	}
}

func TestDeterministicDegradedBlocksSummarizeMediaContinuity(t *testing.T) {
	packet := catalogPacket()
	packet.Facts = append(packet.Facts, Fact{Key: "media_circulation.source_continuity", EvidenceIDs: []string{"ev-1"}, Value: map[string]any{"sources": []map[string]any{
		{"source_name": "Due podcast", "schedule_state": "due_unadmitted", "delivery_state": "unknown"},
		{"source_name": "Working podcast", "schedule_state": "in_flight", "delivery_state": "pending"},
		{"source_name": "Ready podcast", "schedule_state": "scheduled", "delivery_state": "verified"},
	}}})
	blocks := DeterministicDegradedBlocks(packet, "LLM is unavailable.")
	if len(blocks) < 4 || blocks[1].Kind != "fact" || !strings.Contains(blocks[1].Text, "1 due with no active source run") || blocks[1].EvidenceIDs[0] != "ev-1" {
		t.Fatalf("continuity snapshot must be evidence-backed: %#v", blocks)
	}
	if blocks[2].Kind != "interpretation" || !strings.Contains(blocks[2].Text, "Due podcast") || !strings.Contains(blocks[2].Text, "does not prove") {
		t.Fatalf("due source diagnosis must preserve the boundary: %#v", blocks)
	}
}

func TestDeterministicDegradedBlocksLocalizeMediaContinuity(t *testing.T) {
	packet := catalogPacket()
	packet.Facts = append(packet.Facts, Fact{Key: "media_circulation.source_continuity", EvidenceIDs: []string{"ev-1"}, Value: map[string]any{"sources": []map[string]any{{"source_name": "مصدر", "schedule_state": "due_unadmitted", "delivery_state": "unknown"}}}})
	blocks := DeterministicDegradedBlocksForLocale(packet, "ar", "تعذر الاستدلال")
	if len(blocks) < 3 || blocks[1].Kind != "fact" || !strings.Contains(blocks[1].Text, "مصادر الوسائط") || blocks[2].Kind != "interpretation" || !strings.Contains(blocks[2].Text, "لا يثبت") {
		t.Fatalf("Arabic continuity diagnosis must remain localized and bounded: %#v", blocks)
	}
}

func TestDeterministicDegradedBlocksSummarizeSupplyContinuityAndAttention(t *testing.T) {
	packet := catalogPacket()
	packet.Facts = append(packet.Facts,
		Fact{Key: "media_circulation.supply_continuity", EvidenceIDs: []string{"supply-headline"}, Value: map[string]any{
			"verdict": "source_due_not_admitted", "headline_boundary": "cms_admission", "owner": "CMS source-run scheduler", "evidence_completeness": "complete",
		}},
		Fact{Key: "media_circulation.supply_evaluator", EvidenceIDs: []string{"supply-evaluator"}, Value: map[string]any{"worker_state": "ready"}},
		Fact{Key: "media_circulation.supply_attention", EvidenceIDs: []string{"supply-attention"}, Value: map[string]any{"sampled_count": 2, "episodes": []map[string]any{{"state": models.MediaSupplyEpisodeOpen}, {"state": models.MediaSupplyEpisodeResolved}}}},
	)
	blocks := DeterministicDegradedBlocks(packet, "LLM is unavailable.")
	if len(blocks) < 4 || blocks[1].Kind != "fact" || !strings.Contains(blocks[1].Text, "source_due_not_admitted") || !strings.Contains(blocks[1].Text, "does not prove a retry or repair") {
		t.Fatalf("supply headline must stay evidence-backed and read-only: %#v", blocks)
	}
	if len(blocks[1].EvidenceIDs) != 2 || blocks[1].EvidenceIDs[0] != "supply-headline" || blocks[1].EvidenceIDs[1] != "supply-evaluator" {
		t.Fatalf("supply headline must cite both headline and liveness evidence: %#v", blocks[1])
	}
	if blocks[2].Kind != "fact" || !strings.Contains(blocks[2].Text, "1 open") || blocks[2].EvidenceIDs[0] != "supply-attention" {
		t.Fatalf("supply attention must be evidence-backed: %#v", blocks[2])
	}
}

func TestDeterministicDegradedBlocksLocalizeSupplyContinuity(t *testing.T) {
	packet := catalogPacket()
	packet.Facts = append(packet.Facts, Fact{Key: "media_circulation.supply_continuity", EvidenceIDs: []string{"supply-headline"}, Value: map[string]any{
		"verdict": "evidence_unavailable", "headline_boundary": "evidence", "owner": "CMS", "evidence_completeness": "partial",
	}})
	blocks := DeterministicDegradedBlocksForLocale(packet, "ar", "تعذر الاستدلال")
	if len(blocks) < 3 || blocks[1].Kind != "fact" || !strings.Contains(blocks[1].Text, "استمرارية الإمداد") || !strings.Contains(blocks[1].Text, "قراءة فقط") {
		t.Fatalf("Arabic supply headline must remain localized and bounded: %#v", blocks)
	}
}
