package operator

import "testing"

func TestDefaultAdapterRegistryRegistersTenantBoundTraversal(t *testing.T) {
	registry := DefaultAdapterRegistry()
	if err := registry.Validate(); err != nil {
		t.Fatalf("default registry must be valid: %v", err)
	}
	access := AccessSnapshot{UserID: "operator-a", TenantID: "tenant-a", Active: true, Permissions: []string{"source:read"}, AccessVersion: "v1"}
	edges, err := registry.BoundedTraversal("content_source", access, normalQuestionBudget)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 3 || len(registry.EdgesFrom("source_run_request")) != 2 {
		t.Fatalf("expected the registered source lineage traversal, got %#v", edges)
	}
	for _, edge := range edges {
		if edge.TenantKey != "tenant_id" || edge.MaxRows > normalQuestionBudget.MaxRowsPerDomain || edge.MaxDepth > normalQuestionBudget.MaxRelationDepth {
			t.Fatalf("unsafe registered edge: %#v", edge)
		}
	}
}

func TestBoundedTraversalExcludesUnauthorizedAndOverBudgetEdges(t *testing.T) {
	registry := DefaultAdapterRegistry()
	noAccess := AccessSnapshot{UserID: "operator-a", TenantID: "tenant-a", Active: true, AccessVersion: "v1"}
	edges, err := registry.BoundedTraversal("content_source", noAccess, normalQuestionBudget)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 0 {
		t.Fatalf("unauthorized relationship reads must not be admitted: %#v", edges)
	}
	if _, err := registry.BoundedTraversal("content_source", approvalAccess(), ReadBudget{MaxAdapters: 1, MaxRelationDepth: 2, MaxRowsPerDomain: 200}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.BoundedTraversal("content_source", approvalAccess(), ReadBudget{MaxAdapters: 9, MaxRelationDepth: 2, MaxRowsPerDomain: 200}); err == nil {
		t.Fatal("a budget above the hard maximum must be rejected")
	}
}

func TestRegistryRejectsUnboundRelationship(t *testing.T) {
	registry := DefaultAdapterRegistry()
	registry.edges["content_source"][0].TenantKey = "source_tenant"
	if err := registry.Validate(); err == nil {
		t.Fatal("an edge without the canonical tenant key must be rejected")
	}
}
