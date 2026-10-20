package workload

import (
	"sort"
	"time"
)

type ActionClass struct {
	Key        string         `json:"key"`
	Label      string         `json:"label"`
	UnitLabel  string         `json:"unit_label"`
	Resources  []string       `json:"resources"`
	Weights    map[string]int `json:"weights"`
	SpendOwner string         `json:"spend_owner,omitempty"`
}

type Resource struct {
	Key          string        `json:"key"`
	Label        string        `json:"label"`
	Kind         string        `json:"kind"` // pressure | slots
	DefaultLimit ResourceLimit `json:"default_limit"`
}

type CoverageEntry struct {
	System          string     `json:"system"`
	Relationship    string     `json:"relationship"`
	Classes         []string   `json:"classes"`
	Boundary        string     `json:"boundary"`
	Reason          string     `json:"reason"`
	Requests24h     int64      `json:"requests_24h"`
	LastRequestedAt *time.Time `json:"last_requested_at,omitempty"`
	RuntimeObserved bool       `json:"runtime_observed"`
}

type RegistryView struct {
	Classes   []ActionClass   `json:"classes"`
	Resources []Resource      `json:"resources"`
	Coverage  []CoverageEntry `json:"coverage"`
}

var actionClasses = []ActionClass{
	{"source_pull", "Source acquisition", "source runs", []string{"agg_fetch_queue", "aggregation_svc"}, map[string]int{"aggregation_svc": 1}, ""},
	{"text_embedding", "Text embeddings", "items", []string{"enrichment_svc"}, map[string]int{"enrichment_svc": 1}, ""},
	{"image_embedding", "Image embeddings", "items", []string{"media_svc"}, map[string]int{"media_svc": 1}, ""},
	{"stt_batch", "Transcription", "jobs", []string{"media_svc", "stt_slots"}, map[string]int{"media_svc": 1, "stt_slots": 1}, "transcription"},
	{"llm_batch", "Language model work", "calls", []string{"enrichment_svc", "llm_slots"}, map[string]int{"enrichment_svc": 1, "llm_slots": 1}, "llm"},
	{"atomize", "Media atomization", "episodes", []string{"agg_media_queue", "aggregation_svc"}, map[string]int{"aggregation_svc": 1}, ""},
	{"heavy_sweep", "Database sweeps", "sweeps", []string{"cms_db"}, map[string]int{"cms_db": 1}, ""},
	{"news_stage", "News processing", "stages", []string{"agg_news_queue", "aggregation_svc"}, map[string]int{"aggregation_svc": 1}, ""},
	{"pods_stage", "Pods processing", "stages", []string{"agg_pods_queue", "aggregation_svc"}, map[string]int{"aggregation_svc": 1}, ""},
}

var resources = []Resource{
	{"agg_fetch_queue", "Source acquisition queue", "pressure", ResourceLimit{50, 100, 0}},
	{"agg_media_queue", "Media processing queue", "pressure", ResourceLimit{50, 100, 0}},
	{"agg_news_queue", "News processing queues", "pressure", ResourceLimit{50, 100, 0}},
	{"agg_pods_queue", "Pods processing queues", "pressure", ResourceLimit{25, 50, 0}},
	{"aggregation_svc", "Aggregation initiation slots", "slots", ResourceLimit{0, 0, 4}},
	{"enrichment_svc", "Enrichment initiation slots", "slots", ResourceLimit{0, 0, 4}},
	{"media_svc", "Media initiation slots", "slots", ResourceLimit{0, 0, 4}},
	{"stt_slots", "Transcription initiation slots", "slots", ResourceLimit{0, 0, 2}},
	{"llm_slots", "Language model initiation slots", "slots", ResourceLimit{0, 0, 2}},
	{"cms_db", "CMS database sweep slots", "slots", ResourceLimit{0, 0, 2}},
}

// Coverage records execution boundaries rather than obsolete scheduler names.
// Native terminal proofs are required to release handed-off work. Ingress and
// scheduling alone never count as an execution effect or a completion.
var coverage = []CoverageEntry{
	{"source_supply", "governed", []string{"source_pull"}, "supply source-run claim", "Canonical owner boundary; includes scheduled and operator-origin acquisition.", 0, nil, false},
	{"content_stages", "governed", []string{"text_embedding", "image_embedding", "llm_batch", "stt_batch", "atomize", "pods_stage"}, "contentstage native claim", "Canonical News/Pods stage owner.", 0, nil, false},
	{"artifact_coverage", "governed", []string{"text_embedding", "image_embedding", "stt_batch", "llm_batch"}, "artifact owner claim", "Native artifact work retains uncertain effects until proven settlement.", 0, nil, false},
	{"pipeline_repair", "governed", []string{"text_embedding", "pods_stage"}, "pipeline repair claim", "Recovery uses the same resource as the underlying native stage.", 0, nil, false},
	{"enrichment_autopilot", "governed", []string{"text_embedding", "image_embedding"}, "direct model effects", "Text and image calls target separate services; transcription delegates to content_stages.", 0, nil, false},
	{"enrichment_campaign", "governed", []string{"text_embedding", "image_embedding"}, "bounded bulk model effects", "Human-launched bulk enrichment remains a campaign; transcription delegates to the native owner.", 0, nil, false},
	{"media_circulation", "delegated", []string{"source_pull", "atomize"}, "native acquisition and atomization handoff", "Source cadence edits are metadata; actual acquisition is governed at the owner.", 0, nil, false},
	{"news_circulation", "delegated", []string{"source_pull"}, "source-run native owner", "Cadence changes are metadata; actual provider effects are governed at source_supply.", 0, nil, false},
	{"studio_autopilot", "governed", []string{"llm_batch"}, "proposal effects", "Transcription delegates to content_stages; editorial review remains native and human-owned.", 0, nil, false},
	{"preferences_autopilot", "governed", []string{"text_embedding", "llm_batch"}, "model calls", "Small own-table rollups remain exempt.", 0, nil, false},
	{"embedding_campaign", "governed", []string{"text_embedding", "image_embedding", "heavy_sweep"}, "campaign effects", "A human-started autonomous campaign has campaign origin, not a human bypass.", 0, nil, false},
	{"redundancy_hygiene", "governed", []string{"heavy_sweep"}, "similarity scan", "Database scan slots; no queue effect.", 0, nil, false},
	{"feed_integrity_deep", "governed", []string{"heavy_sweep"}, "deep verification pass", "Light status and recovery evidence stay available under pressure.", 0, nil, false},
	{"embedding_lifecycle", "governed", []string{"heavy_sweep"}, "audit scan", "Campaign effects use their own model class.", 0, nil, false},
	{"classification_backfill", "governed", []string{"llm_batch"}, "topic label effect", "Classification may continue without a language-model label.", 0, nil, false},
	{"source_intelligence", "governed", []string{"llm_batch", "text_embedding"}, "bounded account classification and relevance scoring", "Deterministic classification continues when optional model admission is deferred.", 0, nil, false},
	{"transcription_owner", "governed", []string{"stt_batch"}, "legacy direct job admission", "Durable lanes delegate to content_stages; legacy jobs reserve before attempt/budget creation and retain capacity until Media terminal proof.", 0, nil, false},
	{"discovery", "governed", []string{"text_embedding"}, "profile and suggestion relevance scoring", "Optional scores remain unavailable when model admission is deferred; sources are not represented as scored.", 0, nil, false},
	{"manual_content", "human", []string{"text_embedding"}, "authenticated single-item create", "Bounded direct human action records its capacity bypass.", 0, nil, false},
	{"manual_transcript_edit", "human", []string{"text_embedding"}, "authenticated single transcript edit", "Bounded direct human action records its capacity bypass.", 0, nil, false},
	{"manual_topic", "human", []string{"text_embedding"}, "authenticated single topic edit", "Bounded direct human action records its capacity bypass.", 0, nil, false},
	{"transcript_writeback", "exempt", []string{"text_embedding"}, "post-receipt refresh continuation", "A bounded best-effort refresh after authoritative STT writeback remains ungated in V1; this is not a new autonomous batch boundary.", 0, nil, false},
	{"serving_requests", "exempt", []string{}, "interactive feed retrieval and reranking", "User-facing retrieval remains governed by the executing service's local capacity, outside background-work initiation.", 0, nil, false},
	{"operator", "delegated", []string{"source_pull", "news_stage", "pods_stage", "llm_batch"}, "native execution owner", "Plans and approvals do not bypass native capacity; bounded read investigations are exempt.", 0, nil, false},
	{"pipeline_autopilot", "diagnostic", []string{}, "diagnostic projection", "Legacy ai_enqueue/media_enqueue execute no work and are not admitted classes.", 0, nil, false},
	{"system_health", "input", []string{}, "confirmed incident projection", "Must continue observing during saturation.", 0, nil, false},
	{"ai_spend", "input", []string{}, "tenant allowance adapter", "Financial owner remains authoritative; no platform-global money signal.", 0, nil, false},
	{"intelligence_refresh", "exempt", []string{}, "bounded self-paced recompute", "Finite small DB batches; reconsider if measured contention warrants governance.", 0, nil, false},
	{"related_backfill", "exempt", []string{}, "bounded startup backfill", "Self-paced finite backlog.", 0, nil, false},
	{"experience_retention_rollups", "exempt", []string{}, "own-table telemetry and observation", "Recovery/observation must continue during pressure; destructive work retains native approval.", 0, nil, false},
	{"supply_projection_recovery", "exempt", []string{}, "receipt reduction and verification", "Read/reconciliation cannot repeat provider effects and must free proven capacity.", 0, nil, false},
	{"workload_governor", "self", []string{}, "snapshots and maintenance", "Bounded bookkeeping only.", 0, nil, false},
}

func Registry() RegistryView {
	// Deep copy keeps callers from mutating the code-owned registry.
	v := RegistryView{Classes: make([]ActionClass, 0, len(actionClasses)), Resources: append([]Resource{}, resources...), Coverage: make([]CoverageEntry, 0, len(coverage))}
	for _, c := range actionClasses {
		c.Resources = append([]string{}, c.Resources...)
		weights := make(map[string]int, len(c.Weights))
		for k, n := range c.Weights {
			weights[k] = n
		}
		c.Weights = weights
		v.Classes = append(v.Classes, c)
	}
	for _, c := range coverage {
		c.Classes = append([]string{}, c.Classes...)
		v.Coverage = append(v.Coverage, c)
	}
	return v
}

func Class(key string) (ActionClass, bool) {
	for _, c := range Registry().Classes {
		if c.Key == key {
			return c, true
		}
	}
	return ActionClass{}, false
}
func resourceByKey(key string) (Resource, bool) {
	for _, r := range resources {
		if r.Key == key {
			return r, true
		}
	}
	return Resource{}, false
}
func resourceKeys() []string {
	keys := make([]string, 0, len(resources))
	for _, r := range resources {
		keys = append(keys, r.Key)
	}
	sort.Strings(keys)
	return keys
}
