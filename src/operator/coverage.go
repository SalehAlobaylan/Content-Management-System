package operator

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// coverageRead is a reviewed CMS projection, not a browser-selected table or
// query. It makes the Console-wide admission surface explicit while preserving
// the owning domain's tenant predicate and bounded record budget.
type coverageRead struct {
	Key          string
	Table        string
	TenantScoped bool
	Where        string
	Order        string
	Scope        string
}

type coverageAdapter struct {
	Descriptor AdapterDescriptor
	Reads      []coverageRead
}

func coverageAdapterKey(domain string) string { return "operator." + domain + ".state" }

func defaultCoverageAdapters() map[string]coverageAdapter {
	newAdapter := func(domain, permission, deepLink string, reads ...coverageRead) coverageAdapter {
		return coverageAdapter{Descriptor: AdapterDescriptor{Key: coverageAdapterKey(domain), Version: "v1", Domain: domain, RequiredPermission: permission, MaxRows: normalQuestionBudget.MaxRowsPerDomain, MaxAge: operationalEvidenceMaxAge, DeepLinkBase: deepLink}, Reads: reads}
	}
	tenant := func(key, table, order string) coverageRead {
		return coverageRead{Key: key, Table: table, TenantScoped: true, Order: order, Scope: "tenant"}
	}
	global := func(key, table, where, order string) coverageRead {
		return coverageRead{Key: key, Table: table, Where: where, Order: order, Scope: "platform_global"}
	}
	return map[string]coverageAdapter{
		"global_ops":         newAdapter("global_ops", "feed:read", "/platform/operations", tenant("policy", "operator_policies", "updated_at DESC")),
		"system_health":      newAdapter("system_health", "feed:read", "/platform/system-health", global("policy", "system_autopilot_policies", "scope = 'platform'", "updated_at DESC"), global("incident", "system_incident_episodes", "status IN ('open', 'recovering')", "last_seen_at DESC")),
		"real_experience":    newAdapter("real_experience", "feed:read", "/platform/real-experience", tenant("policy", "experience_policies", "updated_at DESC"), tenant("latest_run", "experience_evaluation_runs", "started_at DESC"), tenant("incident", "experience_incidents", "updated_at DESC")),
		"ai_economics":       newAdapter("ai_economics", "feed:manage", "/platform/economics", tenant("policy", "ai_spend_policies", "updated_at DESC"), tenant("latest_run", "ai_spend_runs", "created_at DESC"), tenant("episode", "ai_spend_episodes", "updated_at DESC")),
		"sources":            newAdapter("sources", "source:read", "/platform/sources", tenant("source", "content_sources", "updated_at DESC"), tenant("latest_run", "source_run_requests", "requested_at DESC")),
		"content":            newAdapter("content", "content:read", "/platform/content", tenant("item", "content_items", "updated_at DESC"), tenant("processing", "content_processing_events", "occurred_at DESC")),
		"news":               newAdapter("news", "feed:read", "/platform/news", tenant("snapshot", "news_snapshots", "built_at DESC"), tenant("ranking", "ranking_configs", "updated_at DESC")),
		"news_finding":       newAdapter("news_finding", "source:read", "/platform/news/finding", coverageRead{Key: "profile", Table: "discovery_profiles", TenantScoped: true, Where: "category = 'news'", Order: "updated_at DESC", Scope: "tenant"}, coverageRead{Key: "suggestion", Table: "source_suggestions", TenantScoped: true, Where: "category = 'news'", Order: "updated_at DESC", Scope: "tenant"}),
		"news_circulation":   newAdapter("news_circulation", "feed:read", "/platform/news/circulation", tenant("policy", "news_circulation_policies", "updated_at DESC"), tenant("latest_run", "news_autopilot_runs", "started_at DESC"), tenant("recommendation", "source_circulation_recommendations", "updated_at DESC")),
		"media_sources":      newAdapter("media_sources", "source:read", "/platform/media/sources", coverageRead{Key: "source", Table: "content_sources", TenantScoped: true, Where: "category = 'media'", Order: "updated_at DESC", Scope: "tenant"}, tenant("latest_run", "source_run_requests", "requested_at DESC"), coverageRead{Key: "profile", Table: "discovery_profiles", TenantScoped: true, Where: "category = 'media'", Order: "updated_at DESC", Scope: "tenant"}, coverageRead{Key: "suggestion", Table: "source_suggestions", TenantScoped: true, Where: "category = 'media'", Order: "updated_at DESC", Scope: "tenant"}),
		"atomization":        newAdapter("atomization", "content:read", "/platform/media/atomization", tenant("policy", "media_atomization_policies", "updated_at DESC"), tenant("latest_run", "media_atomization_runs", "updated_at DESC"), tenant("studio_policy", "media_studio_autopilot_policies", "updated_at DESC")),
		"redundancy":         newAdapter("redundancy", "content:read", "/platform/media/redundancy", tenant("policy", "redundancy_policies", "updated_at DESC"), tenant("latest_run", "redundancy_runs", "updated_at DESC"), tenant("pair", "redundancy_pairs", "updated_at DESC")),
		"media_library":      newAdapter("media_library", "content:read", "/platform/media", coverageRead{Key: "item", Table: "content_items", TenantScoped: true, Where: "type IN ('VIDEO', 'PODCAST')", Order: "updated_at DESC", Scope: "tenant"}, tenant("atomization_run", "media_atomization_runs", "updated_at DESC")),
		"storage_quality":    newAdapter("storage_quality", "content:read", "/platform/storage", tenant("policy", "storage_policies", "updated_at DESC"), tenant("sweep", "storage_sweep_runs", "updated_at DESC"), coverageRead{Key: "quality_profile", Table: "quality_profiles", TenantScoped: false, Where: "tenant_id IS NULL", Order: "updated_at DESC", Scope: "platform_global"}),
		"pipeline":           newAdapter("pipeline", "aggregation:read", "/platform/pipeline", tenant("policy", "pipeline_autopilot_policies", "updated_at DESC"), tenant("latest_run", "pipeline_autopilot_runs", "started_at DESC")),
		"enrichment":         newAdapter("enrichment", "content:read", "/platform/enrichment", tenant("policy", "enrichment_autopilot_policies", "updated_at DESC"), tenant("latest_run", "enrichment_autopilot_runs", "started_at DESC")),
		"intelligence":       newAdapter("intelligence", "feed:read", "/platform/intelligence", tenant("ranking", "ranking_configs", "updated_at DESC"), tenant("flag", "content_flags", "updated_at DESC")),
		"embeddings":         newAdapter("embeddings", "content:read", "/platform/intelligence/embeddings", tenant("policy", "embedding_lifecycle_policies", "updated_at DESC"), tenant("latest_run", "embedding_lifecycle_runs", "started_at DESC"), tenant("campaign", "embedding_campaigns", "updated_at DESC")),
		"topics_preferences": newAdapter("topics_preferences", "content:read", "/platform/topics", tenant("policy", "preference_autopilot_policies", "updated_at DESC"), tenant("topic", "topics", "updated_at DESC"), tenant("proposal", "topic_proposals", "updated_at DESC")),
		"moderation":         newAdapter("moderation", "content:read", "/platform/moderation", tenant("report", "moderation_reports", "updated_at DESC")),
		"auth_center":        newAdapter("auth_center", "iam:read", "/admin/users"),
		"operator":           newAdapter("operator", "feed:read", "/platform/operator", tenant("policy", "operator_policies", "updated_at DESC"), tenant("investigation", "operator_investigations", "started_at DESC")),
	}
}

func (fabric *ContextFabric) BuildCoveragePacket(ctx context.Context, visible VisibleContext, access AccessSnapshot) (DecisionPacket, error) {
	if err := visible.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	if err := fabric.registry.Validate(); err != nil {
		return DecisionPacket{}, err
	}
	adapter, ok := defaultCoverageAdapters()[visible.Domain]
	if !ok {
		return DecisionPacket{}, fmt.Errorf("%w: no coverage adapter for domain %q", ErrInvalidContract, visible.Domain)
	}
	descriptor, ok := fabric.registry.Descriptor(adapter.Descriptor.Key)
	if !ok || descriptor != adapter.Descriptor {
		return DecisionPacket{}, fmt.Errorf("%w: coverage adapter is not safely registered", ErrInvalidContract)
	}
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.HasPermission(descriptor.RequiredPermission) {
		return DecisionPacket{}, fmt.Errorf("%w: %s read access is required", ErrAccessUnavailable, descriptor.RequiredPermission)
	}

	startedAt, now := fabric.now(), fabric.now()
	evidence, facts, unknowns := []EvidenceRef{}, []Fact{}, []string{}
	for _, read := range adapter.Reads {
		row, observedAt, err := fabric.coverageRow(ctx, read, access.TenantID)
		if err != nil {
			return DecisionPacket{}, err
		}
		if row == nil {
			unknowns = append(unknowns, "No current "+read.Key+" record is available from the registered "+visible.Domain+" read model.")
			continue
		}
		row["scope"] = read.Scope
		ref := fabric.evidence(descriptor, access.TenantID, descriptor.Key+":"+read.Key+":"+coverageRecordID(row), []SubjectRef{{Type: read.Key, ID: coverageRecordID(row), Label: read.Scope}}, observedAt, observedAt, evidenceAvailabilityForUpdatedAt(observedAt, now), row)
		evidence = append(evidence, ref)
		facts = append(facts, Fact{Key: visible.Domain + "." + read.Key, Value: row, EvidenceIDs: []string{ref.EvidenceID}})
	}
	if visible.Domain == "auth_center" {
		ref := fabric.evidence(descriptor, access.TenantID, descriptor.Key+":current_access:"+access.UserID, []SubjectRef{{Type: "current_operator", ID: access.UserID}}, now, now, EvidenceAvailable, map[string]any{"roles": access.Roles, "permissions": access.Permissions, "is_admin": access.IsAdmin, "access_version": access.AccessVersion})
		evidence = append(evidence, ref)
		facts = append(facts, Fact{Key: "auth_center.current_access", Value: map[string]any{"user_id": access.UserID, "roles": access.Roles, "permissions": access.Permissions, "is_admin": access.IsAdmin}, EvidenceIDs: []string{ref.EvidenceID}})
	}
	return fabric.packet(visible, access, startedAt, facts, evidence, unknowns)
}

func (fabric *ContextFabric) coverageRow(ctx context.Context, read coverageRead, tenantID string) (map[string]any, time.Time, error) {
	query := fabric.db.WithContext(ctx).Table(read.Table)
	if read.TenantScoped {
		query = query.Where("tenant_id=?", tenantID)
	}
	if read.Where != "" {
		query = query.Where(read.Where)
	}
	row := map[string]any{}
	result := query.Order(read.Order).Limit(1).Find(&row)
	if result.Error != nil {
		return nil, time.Time{}, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, time.Time{}, nil
	}
	return sanitizeCoverageRow(row), coverageObservedAt(row, fabric.now()), nil
}

func coverageObservedAt(row map[string]any, fallback time.Time) time.Time {
	for _, key := range []string{"updated_at", "occurred_at", "started_at", "built_at", "created_at"} {
		if value, ok := row[key].(time.Time); ok && !value.IsZero() {
			return value
		}
	}
	return fallback
}

func coverageRecordID(row map[string]any) string {
	for _, key := range []string{"public_id", "id", "tenant_id", "scope"} {
		if value, ok := row[key]; ok && fmt.Sprint(value) != "" {
			return fmt.Sprint(value)
		}
	}
	return "current"
}

func sanitizeCoverageRow(row map[string]any) map[string]any {
	clean := make(map[string]any, len(row))
	for key, value := range row {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "embedding") || strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "password") || strings.Contains(lower, "credential") || strings.Contains(lower, "hash") || strings.Contains(lower, "manifest") || strings.Contains(lower, "target") {
			continue
		}
		clean[key] = value
	}
	return clean
}
