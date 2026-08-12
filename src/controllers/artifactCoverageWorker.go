package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"content-management-system/src/artifacts"
	"gorm.io/gorm"
)

var artifactCoverageHeartbeat atomic.Int64

func StartArtifactCoverageWorker(db *gorm.DB) {
	runArtifactCoverageWorker(db)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			runArtifactCoverageWorker(db)
		}
	}()
}
func ArtifactCoverageWorkerHealthy(now time.Time) bool {
	last := artifactCoverageHeartbeat.Load()
	return last > 0 && now.UTC().Sub(time.Unix(0, last).UTC()) <= 90*time.Second
}
func runArtifactCoverageWorker(db *gorm.DB) {
	if err := artifacts.RecoverExpired(db); err != nil {
		log.Printf("artifact coverage recovery failed: %v", err)
		return
	}
	if _, err := artifacts.VerifyOne(db); err != nil {
		log.Printf("artifact coverage verification failed: %v", err)
		return
	}
	// The worker is live once its CMS-owned recovery and verification pass
	// completes. A remote owner being unavailable is reported separately by the
	// owner-readiness observer and must not make this local worker look dead.
	artifactCoverageHeartbeat.Store(time.Now().UTC().UnixNano())
	claim, found, err := artifacts.ClaimNext(db, artifacts.EnrichmentOwner)
	if err != nil {
		log.Printf("artifact coverage enrichment claim failed: %v", err)
		return
	}
	if !found {
		return
	}
	if _, err = artifacts.Begin(db, claim.Request.PublicID.String(), artifacts.EnrichmentOwner, claim.ClaimToken); err != nil {
		return
	}
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				if _, heartbeatErr := artifacts.Heartbeat(db, claim.Request.PublicID.String(), artifacts.EnrichmentOwner, claim.ClaimToken); heartbeatErr != nil {
					log.Printf("artifact coverage heartbeat failed: %v", heartbeatErr)
					return
				}
			}
		}
	}()
	proof, err := executeEnrichmentArtifact(claim)
	close(stopHeartbeat)
	<-heartbeatDone
	if err != nil {
		_ = artifacts.MarkUncertain(db, claim.Request.PublicID.String(), artifacts.EnrichmentOwner, claim.ClaimToken, "enrichment_owner_effect_uncertain")
		log.Printf("artifact coverage request %s entered uncertain verification: %v", claim.Request.PublicID, err)
		return
	}
	if err = artifacts.MarkAccepted(db, claim.Request.PublicID.String(), artifacts.EnrichmentOwner, claim.ClaimToken, proof); err != nil {
		log.Printf("artifact coverage acceptance failed: %v", err)
	}
}
func executeEnrichmentArtifact(claim artifacts.Claim) (map[string]any, error) {
	base := strings.TrimRight(enrichmentBaseURL(), "/")
	token := enrichmentServiceToken()
	if base == "" || token == "" {
		return nil, fmt.Errorf("enrichment dependency is not configured")
	}
	text := strings.TrimSpace(strings.Join([]string{derefStr(claim.Item.Title), derefStr(claim.Item.Excerpt), derefStr(claim.Item.BodyText)}, "\n\n"))
	if text == "" {
		return nil, fmt.Errorf("artifact input is empty")
	}
	correlation := map[string]string{"request_id": claim.Request.PublicID.String(), "attempt_id": claim.Attempt.PublicID.String(), "claim_token": claim.ClaimToken.String(), "fence_token": claim.Attempt.FenceToken.String(), "input_digest": claim.Request.InputDigest, "producer_event_id": "enrichment:" + claim.Attempt.PublicID.String()}
	path := "/internal/artifact-recovery/text-embedding"
	payload := map[string]any{"text": text, "content_id": claim.Item.PublicID.String(), "correlation": correlation}
	if claim.Request.Artifact == artifacts.ArtifactLLMMetadata {
		path = "/internal/artifact-recovery/llm-metadata"
		payload = map[string]any{"text": text, "content_id": claim.Item.PublicID.String(), "correlation": correlation}
	}
	raw, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := (&http.Client{Timeout: 45 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("enrichment owner status %d", response.StatusCode)
	}
	var outcome struct {
		WriteBackStatus string `json:"write_back_status"`
	}
	if json.Unmarshal(body, &outcome) != nil || outcome.WriteBackStatus != "ok" && outcome.WriteBackStatus != "persisted" {
		return nil, fmt.Errorf("enrichment owner did not confirm persistence")
	}
	return map[string]any{"owner": "enrichment", "write_back_status": outcome.WriteBackStatus, "artifact": claim.Request.Artifact}, nil
}
