package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Media Circulation — apply/dismiss wiring (Slice 5).
//
// "Apply" dispatches a recommendation to EXISTING execution paths (no new engine).
// Per the locked "safe actions only" decision, only clean/reversible actions execute
// live — intake pull, source pause, and rank_down (suppress flag); destructive storage
// byte-actions are recorded and left to the existing storage sweep. Outcomes are written
// back onto the recommendation row — the D11 track record the stage-5 Autopilot consumes.

const (
	mediaCircOutcomePulled          = "pulled"
	mediaCircOutcomePaused          = "paused"
	mediaCircOutcomeRankedDown      = "ranked_down"
	mediaCircOutcomeDeferredToSweep = "deferred_to_sweep"
	mediaCircOutcomeAcknowledged    = "acknowledged"
	mediaCircOutcomeDismissed       = "dismissed"
	mediaCircOutcomeFailed          = "failed"
)

// plannedApplyOutcome maps a (unit_type, verdict) to its outcome kind and whether
// Apply performs a live side effect. Pure — unit-testable.
func plannedApplyOutcome(unitType, verdict string) (kind string, executes bool) {
	switch unitType {
	case models.MediaCirculationUnitSource:
		switch verdict {
		case mediaCircVerdictPullNow, mediaCircVerdictPullLimited, mediaCircVerdictDeepPull:
			return mediaCircOutcomePulled, true
		case mediaCircVerdictPauseSource:
			return mediaCircOutcomePaused, true
		default: // skip_source, needs_admin_review
			return mediaCircOutcomeAcknowledged, false
		}
	case models.MediaCirculationUnitItemFamily:
		switch verdict {
		case mediaCircVerdictRankDown:
			return mediaCircOutcomeRankedDown, true
		case mediaCircVerdictReEncode, mediaCircVerdictMoveToCold, mediaCircVerdictRecoverableDelete:
			return mediaCircOutcomeDeferredToSweep, false
		default: // protect
			return mediaCircOutcomeAcknowledged, false
		}
	}
	return mediaCircOutcomeAcknowledged, false
}

// applyRecommendation performs the safe side effect (if any) for a recommendation
// and returns the outcome. On execution failure it returns mediaCircOutcomeFailed +
// the error so the caller can leave the recommendation pending/retryable.
func applyRecommendation(db *gorm.DB, tenantID, setBy, authorization string, rec models.MediaCirculationRecommendation) (string, error) {
	kind, executes := plannedApplyOutcome(rec.UnitType, rec.Verdict)
	if !executes {
		return kind, nil
	}
	switch kind {
	case mediaCircOutcomePulled:
		allowed, err := recommendationAllowedIntake(rec)
		if err != nil {
			return mediaCircOutcomeFailed, err
		}
		if _, err := triggerMediaSourcePull(db, tenantID, rec.SubjectID, authorization, allowed); err != nil {
			return mediaCircOutcomeFailed, err
		}
		return mediaCircOutcomePulled, nil
	case mediaCircOutcomePaused:
		if err := db.Model(&models.ContentSource{}).
			Where("public_id = ? AND tenant_id = ?", rec.SubjectID, tenantID).
			Update("is_active", false).Error; err != nil {
			return mediaCircOutcomeFailed, err
		}
		return mediaCircOutcomePaused, nil
	case mediaCircOutcomeRankedDown:
		if err := upsertSuppressFlag(db, tenantID, rec.SubjectID, setBy); err != nil {
			return mediaCircOutcomeFailed, err
		}
		return mediaCircOutcomeRankedDown, nil
	}
	return kind, nil
}

// triggerMediaSourcePull reuses the same aggregation-trigger path as RunContentSource.
func triggerMediaSourcePull(db *gorm.DB, tenantID string, sourceID uuid.UUID, authorization string, allowedIntake int) (string, error) {
	var source models.ContentSource
	if err := db.Where("public_id = ? AND tenant_id = ?", sourceID, tenantID).First(&source).Error; err != nil {
		return "", err
	}
	if allowedIntake <= 0 {
		return "", errors.New("circulation recommendation has no allowed intake")
	}
	aggregationBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("AGGREGATION_BASE_URL")), "/")
	if aggregationBaseURL == "" {
		return "", errors.New("aggregation service URL is not configured")
	}
	sourceURL, err := extractSourceRunURL(source)
	if err != nil {
		return "", err
	}
	settings, _ := parseSourceAPIConfig(source.APIConfig)
	settings = limitSourceRunSettings(settings, allowedIntake)
	res, err := triggerAggregationSourceRun(aggregationBaseURL, authorization, aggregationTriggerRequest{
		SourceType: string(source.Type),
		URL:        sourceURL,
		Name:       source.Name,
		Settings:   settings,
	})
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	source.LastFetchedAt = &now
	_ = db.Save(&source).Error
	return res.JobID, nil
}

func recommendationAllowedIntake(rec models.MediaCirculationRecommendation) (int, error) {
	if len(rec.Metrics) == 0 {
		return 0, errors.New("circulation recommendation is missing allowed_intake")
	}
	var metrics map[string]interface{}
	if err := json.Unmarshal(rec.Metrics, &metrics); err != nil {
		return 0, fmt.Errorf("invalid circulation recommendation metrics: %w", err)
	}
	allowed := positiveIntSetting(metrics["allowed_intake"])
	if allowed <= 0 {
		return 0, errors.New("circulation recommendation has no allowed intake")
	}
	return allowed, nil
}

func limitSourceRunSettings(settings map[string]interface{}, allowedIntake int) map[string]interface{} {
	if settings == nil {
		settings = map[string]interface{}{}
	}
	limit := allowedIntake
	if existing := firstPositiveIntSetting(settings, "max_results", "maxResults"); existing > 0 && existing < limit {
		limit = existing
	}
	settings["max_results"] = limit
	settings["maxResults"] = limit

	atomizationLimit := limit
	if existing := firstPositiveIntSetting(settings, "initial_atomization_limit", "initialAtomizationLimit"); existing > 0 && existing < atomizationLimit {
		atomizationLimit = existing
	}
	settings["initial_atomization_limit"] = atomizationLimit
	settings["initialAtomizationLimit"] = atomizationLimit
	return settings
}

func firstPositiveIntSetting(settings map[string]interface{}, keys ...string) int {
	for _, key := range keys {
		if value := positiveIntSetting(settings[key]); value > 0 {
			return value
		}
	}
	return 0
}

func positiveIntSetting(value interface{}) int {
	switch v := value.(type) {
	case int:
		if v > 0 {
			return v
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	case json.Number:
		if i, err := v.Int64(); err == nil && i > 0 {
			return int(i)
		}
	}
	return 0
}

// upsertSuppressFlag mirrors UpsertContentFlag's upsert to set Suppress on the item.
func upsertSuppressFlag(db *gorm.DB, tenantID string, contentID uuid.UUID, setBy string) error {
	var flag models.ContentFlag
	isNew := db.Where("content_item_id = ? AND tenant_id = ?", contentID, tenantID).First(&flag).Error != nil
	if isNew {
		flag = models.ContentFlag{TenantID: tenantID, ContentItemID: contentID, BoostMultiplier: 1.5}
	}
	flag.Suppress = true
	flag.SetBy = setBy
	if strings.TrimSpace(flag.Notes) == "" {
		flag.Notes = "Media circulation rank_down"
	}
	return db.Save(&flag).Error
}
