package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	mediaCircVerdictAtomizeNow        = "atomize_now"
	mediaCircVerdictBlockedTranscript = "blocked_transcript"
	mediaCircVerdictAtomizationLeak   = "atomization_leak"

	mediaCircAtomizationRecCap = 100
)

type mediaCircAtomizationBacklog struct {
	PendingRuns           int64   `json:"pending_runs"`
	RunningRuns           int64   `json:"running_runs"`
	TranscriptWaitCount   int64   `json:"transcript_wait_count"`
	BacklogDepth          int64   `json:"backlog_depth"`
	IntakeDampeningFactor float64 `json:"intake_dampening_factor"`
}

type mediaCircBucketYield struct {
	Predicted int64 `json:"predicted"`
	Delivered int64 `json:"delivered"`
}

func computeAtomizationRecommendations(db *gorm.DB, tenantID string, overrides mediaCircOverrideIndex) []circulationRecInput {
	// Urgency order: leaks first, then actionable atomize-now, then blocked —
	// so the cap truncation never drops the most urgent class.
	recs := []circulationRecInput{}
	recs = append(recs, computeAtomizationLeakRecommendations(db, tenantID, overrides)...)
	recs = append(recs, computeAtomizeNowRecommendations(db, tenantID, overrides)...)
	recs = append(recs, computeBlockedTranscriptRecommendations(db, tenantID, overrides)...)
	if len(recs) > mediaCircAtomizationRecCap {
		return recs[:mediaCircAtomizationRecCap]
	}
	return recs
}

func computeAtomizeNowRecommendations(db *gorm.DB, tenantID string, overrides mediaCircOverrideIndex) []circulationRecInput {
	var parents []models.ContentItem
	db.Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("parent_content_item_id IS NULL").
		Where("duration_sec > ?", atomizationMinParentDurationSec).
		Where("transcript_id IS NOT NULL").
		Where("COALESCE(media_url, '') <> ''").
		Where("NOT EXISTS (SELECT 1 FROM media_atomization_runs r WHERE r.tenant_id = content_items.tenant_id AND r.parent_content_item_id = content_items.public_id AND r.status IN ('queued','running','completed','needs_review'))").
		Order("created_at ASC").
		Limit(mediaCircAtomizationRecCap).
		Find(&parents)

	out := make([]circulationRecInput, 0, len(parents))
	for _, parent := range parents {
		if row, ok := mediaCircAtomizationBlockingOverride(overrides, parent.PublicID); ok {
			out = append(out, protectedAtomizationOverrideRec(parent, row))
			continue
		}
		out = append(out, circulationRecInput{
			SubjectID:   parent.PublicID,
			SubjectKind: "content_item",
			Verdict:     mediaCircVerdictAtomizeNow,
			Action:      "atomize",
			Score:       float64(*parent.DurationSec) / 3600.0,
			Reasons:     []string{"Parent is over 40 minutes, has transcript, and has no active/completed atomization run."},
			Metrics: map[string]interface{}{
				"duration_sec": parent.DurationSec,
				"source_name":  stringPtrValue(parent.SourceName),
			},
		})
	}
	return out
}

func computeBlockedTranscriptRecommendations(db *gorm.DB, tenantID string, overrides mediaCircOverrideIndex) []circulationRecInput {
	var parents []models.ContentItem
	db.Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("parent_content_item_id IS NULL").
		Where("duration_sec > ?", atomizationMinParentDurationSec).
		Where("transcript_id IS NULL").
		Order("created_at ASC").
		Limit(mediaCircAtomizationRecCap).
		Find(&parents)

	out := make([]circulationRecInput, 0, len(parents))
	for _, parent := range parents {
		if row, ok := mediaCircAtomizationBlockingOverride(overrides, parent.PublicID); ok {
			out = append(out, protectedAtomizationOverrideRec(parent, row))
			continue
		}
		out = append(out, circulationRecInput{
			SubjectID:   parent.PublicID,
			SubjectKind: "content_item",
			Verdict:     mediaCircVerdictBlockedTranscript,
			Action:      "review",
			Score:       0.2,
			Reasons:     []string{"Parent is over 40 minutes but transcript is missing; atomization is blocked."},
			Metrics: map[string]interface{}{
				"duration_sec": parent.DurationSec,
				"source_name":  stringPtrValue(parent.SourceName),
			},
		})
	}
	return out
}

func computeAtomizationLeakRecommendations(db *gorm.DB, tenantID string, overrides mediaCircOverrideIndex) []circulationRecInput {
	type leakRow struct {
		PublicID    uuid.UUID
		DurationSec *int
		SourceName  *string
		LeakReason  string
	}
	rows := []leakRow{}
	db.Raw(`
		SELECT p.public_id, p.duration_sec, p.source_name,
			CASE
				WHEN p.is_feed_unit = TRUE AND p.feed_visibility = 'visible' AND p.duration_sec > ? THEN 'long_parent_visible'
				ELSE 'completed_zero_children'
			END AS leak_reason
		FROM content_items p
		WHERE p.tenant_id = ?
			AND p.type IN ('VIDEO','PODCAST')
			AND p.parent_content_item_id IS NULL
			AND p.duration_sec > ?
			AND (
				(p.is_feed_unit = TRUE AND p.feed_visibility = 'visible')
				OR (
					EXISTS (
						SELECT 1 FROM media_atomization_runs r
						WHERE r.tenant_id = p.tenant_id
							AND r.parent_content_item_id = p.public_id
							AND r.status = 'completed'
					)
					AND NOT EXISTS (
						SELECT 1 FROM content_items c
						WHERE c.tenant_id = p.tenant_id
							AND c.parent_content_item_id = p.public_id
							AND c.is_feed_unit = TRUE
							AND c.feed_visibility = 'visible'
							AND c.status = 'READY'
					)
				)
			)
		ORDER BY p.updated_at DESC
		LIMIT ?`, atomizationMinParentDurationSec, tenantID, atomizationMinParentDurationSec, mediaCircAtomizationRecCap).Scan(&rows)

	out := make([]circulationRecInput, 0, len(rows))
	for _, row := range rows {
		if override, ok := mediaCircAtomizationBlockingOverride(overrides, row.PublicID); ok {
			out = append(out, protectedAtomizationOverrideRec(models.ContentItem{PublicID: row.PublicID, DurationSec: row.DurationSec, SourceName: row.SourceName}, override))
			continue
		}
		reason := "Atomization leak detected; parent needs admin review."
		if row.LeakReason == "long_parent_visible" {
			reason = "Parent is visible raw in For You while over the 40-minute ceiling."
		}
		out = append(out, circulationRecInput{
			SubjectID:   row.PublicID,
			SubjectKind: "content_item",
			Verdict:     mediaCircVerdictAtomizationLeak,
			Action:      "review",
			Score:       0.8,
			Reasons:     []string{reason},
			Metrics: map[string]interface{}{
				"duration_sec": row.DurationSec,
				"source_name":  stringPtrValue(row.SourceName),
				"leak_reason":  row.LeakReason,
			},
		})
	}
	return out
}

func mediaCircAtomizationBlockingOverride(overrides mediaCircOverrideIndex, id uuid.UUID) (models.MediaCirculationOverride, bool) {
	if row, ok := mediaCircHasOverride(overrides, "item", id, models.MediaCirculationOverrideNoAtomize, models.MediaCirculationOverrideEditorialHold); ok {
		return row, true
	}
	if row, ok := mediaCircHasOverride(overrides, "family", id, models.MediaCirculationOverrideNoAtomize, models.MediaCirculationOverrideEditorialHold); ok {
		return row, true
	}
	return models.MediaCirculationOverride{}, false
}

func protectedAtomizationOverrideRec(parent models.ContentItem, override models.MediaCirculationOverride) circulationRecInput {
	return circulationRecInput{
		SubjectID:   parent.PublicID,
		SubjectKind: "content_item",
		Verdict:     mediaCircVerdictProtect,
		Action:      mediaCircVerdictProtect,
		Score:       1,
		Reasons:     []string{mediaCircOverrideReason(override)},
		Metrics: map[string]interface{}{
			"duration_sec":  parent.DurationSec,
			"source_name":   stringPtrValue(parent.SourceName),
			"override_type": override.OverrideType,
			"override_id":   override.PublicID.String(),
		},
	}
}

func computeMediaCircAtomizationBacklog(db *gorm.DB, tenantID string) mediaCircAtomizationBacklog {
	var pending, running, wait int64
	db.Model(&models.MediaAtomizationRun{}).Where("tenant_id = ? AND status = ?", tenantID, "queued").Count(&pending)
	db.Model(&models.MediaAtomizationRun{}).Where("tenant_id = ? AND status = ?", tenantID, "running").Count(&running)
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("parent_content_item_id IS NULL").
		Where("duration_sec > ?", atomizationMinParentDurationSec).
		Where("transcript_id IS NULL").
		Count(&wait)
	resp := mediaCircAtomizationBacklog{
		PendingRuns:         pending,
		RunningRuns:         running,
		TranscriptWaitCount: wait,
		BacklogDepth:        pending + running + wait,
	}
	resp.IntakeDampeningFactor = atomizationBacklogIntakeFactor(resp)
	return resp
}

func atomizationBacklogIntakeFactor(backlog mediaCircAtomizationBacklog) float64 {
	switch {
	case backlog.BacklogDepth >= 50:
		return 0.4
	case backlog.BacklogDepth >= 20:
		return 0.6
	case backlog.BacklogDepth >= 10:
		return 0.8
	default:
		return 1
	}
}

func enrichAppliedIntakeYields(db *gorm.DB, tenantID string) {
	var rows []models.MediaCirculationRecommendation
	cutoff := time.Now().UTC().AddDate(0, 0, -7)
	db.Where("tenant_id = ? AND unit_type = ? AND status = ? AND outcome = ? AND applied_at IS NOT NULL AND applied_at > ?",
		tenantID, models.MediaCirculationUnitSource, models.MediaCirculationRecStatusApplied, mediaCircOutcomePulled, cutoff).
		Limit(100).
		Find(&rows)
	for _, row := range rows {
		sourceName := stringMetric(mediaCircMetricsMap(row), "source_name")
		if sourceName == "" || row.AppliedAt == nil {
			continue
		}
		yield := yieldByBucketForSource(db, tenantID, sourceName, *row.AppliedAt)
		if len(yield) == 0 {
			continue
		}
		metrics := mediaCircMetricsMap(row)
		metrics["yield_by_bucket"] = yield
		raw, _ := json.Marshal(metrics)
		_ = db.Model(&models.MediaCirculationRecommendation{}).
			Where("id = ?", row.ID).
			Update("metrics", datatypes.JSON(raw)).Error
	}
}

func yieldByBucketForSource(db *gorm.DB, tenantID, sourceName string, since time.Time) map[string]int64 {
	type row struct {
		Bucket string
		Count  int64
	}
	var rows []row
	db.Model(&models.ContentItem{}).
		Select("duration_bucket AS bucket, COUNT(*) AS count").
		Where("tenant_id = ? AND source_name = ?", tenantID, sourceName).
		Where("is_feed_unit = TRUE AND status = ?", models.ContentStatusReady).
		Where("created_at >= ?", since).
		Where("duration_bucket IS NOT NULL").
		Group("duration_bucket").
		Scan(&rows)
	out := map[string]int64{}
	for _, row := range rows {
		out[row.Bucket] = row.Count
	}
	return out
}

func computeAppliedIntakeYieldByBucket(db *gorm.DB, tenantID string) map[string]mediaCircBucketYield {
	out := map[string]mediaCircBucketYield{}
	for _, bucket := range mediaCirculationBuckets {
		out[bucket] = mediaCircBucketYield{}
	}
	var rows []models.MediaCirculationRecommendation
	cutoff := time.Now().UTC().AddDate(0, 0, -7)
	db.Where("tenant_id = ? AND unit_type = ? AND status = ? AND outcome = ? AND applied_at > ?",
		tenantID, models.MediaCirculationUnitSource, models.MediaCirculationRecStatusApplied, mediaCircOutcomePulled, cutoff).
		Find(&rows)
	for _, rec := range rows {
		metrics := mediaCircMetricsMap(rec)
		addPredictedIntakeShare(out, stringSliceMetric(metrics, "matched_thin_buckets"), int64(positiveIntSetting(metrics["allowed_intake"])))
		if yb, ok := metrics["yield_by_bucket"].(map[string]interface{}); ok {
			for bucket, raw := range yb {
				y := out[bucket]
				y.Delivered += int64(positiveIntSetting(raw))
				out[bucket] = y
			}
		}
	}
	return out
}

// addPredictedIntakeShare splits one pull's allowed_intake evenly across its
// matched thin buckets (remainder to the first) so a multi-bucket pull isn't
// counted as full predicted supply in every bucket it matches.
func addPredictedIntakeShare(out map[string]mediaCircBucketYield, matched []string, allowed int64) {
	if allowed <= 0 || len(matched) == 0 {
		return
	}
	share := allowed / int64(len(matched))
	remainder := allowed % int64(len(matched))
	for i, bucket := range matched {
		y := out[bucket]
		y.Predicted += share
		if i == 0 {
			y.Predicted += remainder
		}
		out[bucket] = y
	}
}

func stringPtrValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
