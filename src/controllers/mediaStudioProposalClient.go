package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Media Studio Clearance Autopilot — stage 6, Slice 4 (LLM proposal client).
//
// CMS calls Enrichment (the LLM-ops owner) to draft advisory publish/reject
// proposals for approval-tier chapter cases. A proposal is display-only: CMS
// re-checks every invariant in code and a human decides (S10). If Enrichment is
// unavailable the queue simply appears unranked (S10 degradation).

type studioProposalItem struct {
	ID              string   `json:"id"`
	Transcript      string   `json:"transcript"`
	Title           string   `json:"title"`
	Summary         string   `json:"summary"`
	ReviewReason    string   `json:"review_reason"`
	ReviewCode      string   `json:"review_code"`
	Confidence      *float64 `json:"confidence,omitempty"`
	StandaloneScore *float64 `json:"standalone_score,omitempty"`
	ContainsSponsor bool     `json:"contains_sponsor"`
	DurationSec     *int     `json:"duration_sec,omitempty"`
	ParentTitle     string   `json:"parent_title"`
}

type studioProposalChecks struct {
	DurationOK       bool `json:"duration_ok"`
	NoSponsorOverlap bool `json:"no_sponsor_overlap"`
	CoherentStart    bool `json:"coherent_start"`
	CoherentEnd      bool `json:"coherent_end"`
}

type studioProposal struct {
	ID         string               `json:"id"`
	Proposal   string               `json:"proposal"` // publish | reject
	Confidence float64              `json:"confidence"`
	Rationale  string               `json:"rationale"`
	Checked    studioProposalChecks `json:"checked"`
}

type studioProposalResponse struct {
	Proposals []studioProposal `json:"proposals"`
}

// generateChapterProposalsViaEnrichment sends a bounded batch of review cases to
// Enrichment and returns the proposals keyed by case id. Best-effort: a
// transport or config failure returns an error the caller degrades on.
func generateChapterProposalsViaEnrichment(items []studioProposalItem) (map[string]studioProposal, error) {
	if len(items) == 0 {
		return map[string]studioProposal{}, nil
	}
	baseURL := enrichmentBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("ENRICHMENT_BASE_URL is not configured")
	}
	token := enrichmentServiceToken()
	if token == "" {
		return nil, fmt.Errorf("enrichment service token is not configured")
	}

	body, err := json.Marshal(map[string]interface{}{"items": items})
	if err != nil {
		return nil, fmt.Errorf("marshal chapter-proposal request: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/studio/chapter-proposal", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build chapter-proposal request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	// Enrichment bounds each case to 12s with concurrency three; 75s leaves
	// response overhead while retaining partial valid results from the batch.
	client := &http.Client{Timeout: 75 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enrichment chapter-proposal request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("enrichment chapter-proposal returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var decoded studioProposalResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode chapter-proposal response: %w", err)
	}
	out := make(map[string]studioProposal, len(decoded.Proposals))
	for _, p := range decoded.Proposals {
		out[p.ID] = p
	}
	return out, nil
}
