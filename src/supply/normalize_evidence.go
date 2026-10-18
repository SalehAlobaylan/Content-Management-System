package supply

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// NormalizeInput freezes the exact source identities before a batch is queued.
// Absence on historical units is unknown evidence, never an empty batch.
type NormalizeInput struct {
	UpstreamItemID string `json:"upstream_item_id"`
	ObservationID  string `json:"observation_id,omitempty"`
	Fingerprint    string `json:"fingerprint"`
}

func canonicalNormalizeInputs(inputs []NormalizeInput) (datatypes.JSON, string, error) {
	if len(inputs) < 1 || len(inputs) > 100 {
		return nil, "", fmt.Errorf("normalization cohort requires 1..100 exact inputs")
	}
	ordered := append([]NormalizeInput(nil), inputs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].UpstreamItemID < ordered[j].UpstreamItemID })
	for i, input := range ordered {
		if strings.TrimSpace(input.UpstreamItemID) == "" || len(input.UpstreamItemID) > 255 || !isDigest(input.Fingerprint) || (i > 0 && ordered[i-1].UpstreamItemID == input.UpstreamItemID) {
			return nil, "", fmt.Errorf("normalization cohort contains an invalid or duplicate identity")
		}
		if input.ObservationID != "" {
			if _, err := uuid.Parse(input.ObservationID); err != nil {
				return nil, "", fmt.Errorf("normalization observation ID is invalid")
			}
		}
	}
	data, err := json.Marshal(ordered)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(data)
	return datatypes.JSON(data), hex.EncodeToString(digest[:]), nil
}

// observeNormalizeBatch requires a terminal disposition for every frozen input.
// A persisted prefix, a missing observation, or a changed digest stays unknown.
func observeNormalizeBatch(db *gorm.DB, unit models.SourceRunExecutionUnit) (expected, materialized int64, complete bool, err error) {
	var inputs []NormalizeInput
	if json.Unmarshal(unit.NormalizeInputs, &inputs) != nil || len(inputs) == 0 {
		return 0, 0, false, nil
	}
	_, digest, validationErr := canonicalNormalizeInputs(inputs)
	if validationErr != nil || digest != unit.NormalizeInputDigest {
		return 0, 0, false, nil
	}
	expected = int64(len(inputs))
	contentIDs := map[string]bool{}
	for _, input := range inputs {
		if input.ObservationID == "" {
			return expected, materialized, false, nil
		}
		var observation models.SourceUpstreamObservation
		if readErr := db.Where("tenant_id=? AND public_id=? AND content_source_id=? AND upstream_item_id=? AND upstream_fingerprint=?", unit.TenantID, input.ObservationID, unit.ContentSourceID, input.UpstreamItemID, input.Fingerprint).First(&observation).Error; readErr != nil {
			if readErr == gorm.ErrRecordNotFound {
				return expected, materialized, false, nil
			}
			return expected, materialized, false, readErr
		}
		var events []models.SourceUpstreamObservationEvent
		if err = db.Where("tenant_id=? AND observation_id=? AND event_type IN ? AND payload->>'execution_unit_id'=? AND payload->>'source_run_request_id'=?", unit.TenantID, observation.PublicID, []string{"materialized", "filtered"}, unit.PublicID.String(), unit.SourceRunRequestID.String()).Limit(2).Find(&events).Error; err != nil {
			return
		}
		if len(events) != 1 {
			return expected, materialized, false, nil
		}
		if events[0].EventType == "materialized" {
			var payload struct {
				ContentItemID string `json:"content_item_id"`
			}
			if json.Unmarshal(events[0].Payload, &payload) != nil || payload.ContentItemID == "" {
				return expected, materialized, false, nil
			}
			var count int64
			if err = db.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=? AND content_source_id=? AND metadata->>'source_run_execution_unit_id'=?", unit.TenantID, payload.ContentItemID, unit.ContentSourceID, unit.PublicID.String()).Count(&count).Error; err != nil {
				return
			}
			if count != 1 {
				return expected, materialized, false, nil
			}
			contentIDs[payload.ContentItemID] = true
		}
	}
	return expected, int64(len(contentIDs)), true, nil
}
