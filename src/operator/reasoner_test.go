package operator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
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
