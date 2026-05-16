package controllers

import (
	"testing"

	"content-management-system/src/models"
)

// Tests for pickMostSpecificProfile — the resolution-order ranker that
// decides which ingest profile applies for a given (tenant, source) input.
// This is load-bearing for both the ingest hot path AND storage's
// re_encode action, so the precedence ladder needs to be locked down.

func sp(v string) *string { return &v }

func TestPickMostSpecificProfile_PrecedenceLadder(t *testing.T) {
	// All four rungs present at once — most specific must win.
	profiles := []models.QualityProfile{
		{ID: 1, Name: "global"},
		{ID: 2, Name: "source-only", SourceType: sp("YOUTUBE")},
		{ID: 3, Name: "tenant-only", TenantID: sp("A")},
		{ID: 4, Name: "tenant+source", TenantID: sp("A"), SourceType: sp("YOUTUBE")},
	}
	winner, tag := pickMostSpecificProfile(profiles, "A", "YOUTUBE")
	if winner == nil || winner.Name != "tenant+source" {
		t.Fatalf("expected tenant+source winner, got %+v", winner)
	}
	if tag != "tenant+source" {
		t.Errorf("expected tag 'tenant+source', got %q", tag)
	}
}

func TestPickMostSpecificProfile_TenantWinsOverSource(t *testing.T) {
	// No tenant+source row exists; tenant wins over source-only.
	profiles := []models.QualityProfile{
		{ID: 1, Name: "global"},
		{ID: 2, Name: "source-only", SourceType: sp("YOUTUBE")},
		{ID: 3, Name: "tenant-only", TenantID: sp("A")},
	}
	winner, tag := pickMostSpecificProfile(profiles, "A", "YOUTUBE")
	if winner == nil || winner.Name != "tenant-only" {
		t.Fatalf("expected tenant-only winner, got %+v", winner)
	}
	if tag != "tenant" {
		t.Errorf("expected tag 'tenant', got %q", tag)
	}
}

func TestPickMostSpecificProfile_SourceWinsOverGlobal(t *testing.T) {
	profiles := []models.QualityProfile{
		{ID: 1, Name: "global"},
		{ID: 2, Name: "source-only", SourceType: sp("YOUTUBE")},
	}
	winner, tag := pickMostSpecificProfile(profiles, "A", "YOUTUBE")
	if winner == nil || winner.Name != "source-only" {
		t.Fatalf("expected source-only winner, got %+v", winner)
	}
	if tag != "source" {
		t.Errorf("expected tag 'source', got %q", tag)
	}
}

func TestPickMostSpecificProfile_GlobalIsFallback(t *testing.T) {
	profiles := []models.QualityProfile{
		{ID: 1, Name: "global"},
	}
	winner, tag := pickMostSpecificProfile(profiles, "A", "YOUTUBE")
	if winner == nil || winner.Name != "global" {
		t.Fatalf("expected global winner, got %+v", winner)
	}
	if tag != "global" {
		t.Errorf("expected tag 'global', got %q", tag)
	}
}

func TestPickMostSpecificProfile_EmptyReturnsNone(t *testing.T) {
	winner, tag := pickMostSpecificProfile(nil, "A", "YOUTUBE")
	if winner != nil {
		t.Errorf("expected nil winner, got %+v", winner)
	}
	if tag != "none" {
		t.Errorf("expected 'none', got %q", tag)
	}
}

func TestPickMostSpecificProfile_FiltersWrongTenant(t *testing.T) {
	// Belt-and-suspenders: a tenant-B row in the candidate list should be
	// ignored when resolving for tenant A. (SQL pre-filter should normally
	// catch this, but the picker defends against it.)
	profiles := []models.QualityProfile{
		{ID: 1, Name: "tenant-B", TenantID: sp("B")},
		{ID: 2, Name: "global"},
	}
	winner, _ := pickMostSpecificProfile(profiles, "A", "YOUTUBE")
	if winner == nil || winner.Name != "global" {
		t.Fatalf("expected global (tenant B row must be filtered), got %+v", winner)
	}
}

func TestPickMostSpecificProfile_FiltersWrongSource(t *testing.T) {
	profiles := []models.QualityProfile{
		{ID: 1, Name: "telegram-only", SourceType: sp("TELEGRAM")},
		{ID: 2, Name: "global"},
	}
	winner, _ := pickMostSpecificProfile(profiles, "", "YOUTUBE")
	if winner == nil || winner.Name != "global" {
		t.Fatalf("expected global (telegram row must be filtered), got %+v", winner)
	}
}

func TestPickMostSpecificProfile_TieBreaksByLowestID(t *testing.T) {
	// Two global profiles — first-created wins (lowest ID).
	profiles := []models.QualityProfile{
		{ID: 7, Name: "later-global"},
		{ID: 2, Name: "earlier-global"},
	}
	winner, _ := pickMostSpecificProfile(profiles, "A", "YOUTUBE")
	if winner == nil || winner.Name != "earlier-global" {
		t.Fatalf("expected earlier-global (ID 2) to win the tie, got %+v", winner)
	}
}

func TestPickMostSpecificProfile_EmptyInputs(t *testing.T) {
	// When both tenant and source are empty (eg. ingest with no tenant context),
	// only the global profile matches; tenant/source-scoped rows are ignored.
	profiles := []models.QualityProfile{
		{ID: 1, Name: "global"},
		{ID: 2, Name: "source-only", SourceType: sp("YOUTUBE")},
	}
	winner, tag := pickMostSpecificProfile(profiles, "", "")
	if winner == nil || winner.Name != "global" {
		t.Fatalf("expected global winner for empty inputs, got %+v", winner)
	}
	if tag != "global" {
		t.Errorf("expected tag 'global', got %q", tag)
	}
}
