package controllers

import (
	"content-management-system/src/models"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type storageProofMetrics struct {
	UsedBytes               int64   `json:"used_bytes"`
	QuotaBytes              int64   `json:"quota_bytes"`
	UtilizationPct          float64 `json:"utilization_pct"`
	DBTrackedBytes          int64   `json:"db_tracked_bytes"`
	ProtectedCount          int64   `json:"protected_count"`
	ProtectedBytes          int64   `json:"protected_bytes"`
	CandidateCount          int64   `json:"candidate_count"`
	CandidateBytes          int64   `json:"candidate_bytes"`
	ParentSourceCount       int64   `json:"parent_source_count"`
	ParentSourceBytes       int64   `json:"parent_source_bytes"`
	RecoverableDeletedCount int64   `json:"recoverable_deleted_count"`
	MissingCount            int64   `json:"missing_count"`
	ColdEnabled             bool    `json:"cold_enabled"`
}

type storageRecommendation struct {
	Key            string                 `json:"key"`
	Label          string                 `json:"label"`
	Detail         string                 `json:"detail"`
	Severity       string                 `json:"severity"`
	Action         string                 `json:"action"`
	EstimatedBytes int64                  `json:"estimated_bytes,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

type storageHealthResponse struct {
	State           string                  `json:"state"`
	Score           int                     `json:"score"`
	Summary         string                  `json:"summary"`
	GeneratedAt     string                  `json:"generated_at"`
	Policy          models.StoragePolicy    `json:"policy"`
	Proof           storageProofMetrics     `json:"proof"`
	Recommendations []storageRecommendation `json:"recommendations"`
}

func GetStorageHealth(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadEffectiveStoragePolicy(db, principal.TenantID)
	stats := storageStatsResponse{
		QuotaBytes: policy.MaxStorageBytes,
	}
	if live, err := callAggregationStorageStats(c.GetHeader("Authorization")); err == nil {
		stats.UsedBytes = live.UsedBytes
		stats.ObjectCount = live.ObjectCount
		stats.ByArtifactType = live.ByArtifactType
		stats.ColdEnabled = live.ColdEnabled
		stats.Cold = live.Cold
	} else {
		stats.AggregationError = err.Error()
		stats.UsedBytes = storageDBTrackedBytes(db, principal.TenantID)
	}
	if stats.QuotaBytes > 0 {
		stats.UtilizationPct = float64(stats.UsedBytes) / float64(stats.QuotaBytes) * 100
	}

	proof := storageProofFor(db, principal.TenantID, policy, stats)
	state := classifyStorageHealth(policy, proof, stats.AggregationError)
	score := storageHealthScore(state)
	recs := storageRecommendationsFor(policy, proof, state)
	c.JSON(http.StatusOK, storageHealthResponse{
		State:           state,
		Score:           score,
		Summary:         storageHealthSummary(state, proof),
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
		Policy:          policy,
		Proof:           proof,
		Recommendations: recs,
	})
}

func GetStorageRecommendations(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadEffectiveStoragePolicy(db, principal.TenantID)
	stats := storageStatsResponse{QuotaBytes: policy.MaxStorageBytes, UsedBytes: storageDBTrackedBytes(db, principal.TenantID)}
	if stats.QuotaBytes > 0 {
		stats.UtilizationPct = float64(stats.UsedBytes) / float64(stats.QuotaBytes) * 100
	}
	proof := storageProofFor(db, principal.TenantID, policy, stats)
	state := classifyStorageHealth(policy, proof, "")
	recs := storageRecommendationsFor(policy, proof, state)
	c.JSON(http.StatusOK, gin.H{
		"state":           state,
		"data":            recs,
		"recommendations": recs,
		"proof":           proof,
		"dry_run":         true,
	})
}

func storageDBTrackedBytes(db *gorm.DB, tenantID string) int64 {
	var total int64
	db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND status != ?", tenantID, models.ContentStatusArchived).
		Select("COALESCE(SUM(file_size_bytes),0)").
		Scan(&total)
	return total
}

func storageProofFor(db *gorm.DB, tenantID string, policy models.StoragePolicy, stats storageStatsResponse) storageProofMetrics {
	proof := storageProofMetrics{
		UsedBytes:      stats.UsedBytes,
		QuotaBytes:     policy.MaxStorageBytes,
		UtilizationPct: stats.UtilizationPct,
		DBTrackedBytes: storageDBTrackedBytes(db, tenantID),
		ColdEnabled:    stats.ColdEnabled,
	}
	candidateQ := buildCandidateQuery(db, filterFromPolicy(policy, tenantID, "", ""))
	candidateQ.Model(&models.ContentItem{}).Count(&proof.CandidateCount)
	candidateQ.Model(&models.ContentItem{}).Select("COALESCE(SUM(file_size_bytes),0)").Scan(&proof.CandidateBytes)

	protectedQ := protectedStorageItemsQuery(db, tenantID, policy)
	protectedQ.Count(&proof.ProtectedCount)
	protectedQ.Select("COALESCE(SUM(file_size_bytes),0)").Scan(&proof.ProtectedBytes)

	parentQ := atomizedParentSourceQuery(db, tenantID)
	parentQ.Count(&proof.ParentSourceCount)
	parentQ.Select("COALESCE(SUM(file_size_bytes),0)").Scan(&proof.ParentSourceBytes)

	db.Model(&models.ContentItem{}).Where("tenant_id = ? AND storage_state = ?", tenantID, models.StorageStateRecoverableDeleted).Count(&proof.RecoverableDeletedCount)
	db.Model(&models.ContentItem{}).Where("tenant_id = ? AND storage_state = ?", tenantID, models.StorageStateMissing).Count(&proof.MissingCount)
	return proof
}

func protectedStorageItemsQuery(db *gorm.DB, tenantID string, policy models.StoragePolicy) *gorm.DB {
	if policy.ProtectTopNByViews <= 0 {
		return db.Model(&models.ContentItem{}).Where("1 = 0").Session(&gorm.Session{})
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -policy.ProtectTopNWindowDays)
	newCutoff := time.Now().UTC().Add(-48 * time.Hour)
	perSourceLimit := policy.ProtectTopNByViews / 10
	if perSourceLimit < 1 {
		perSourceLimit = 1
	}
	perBucketLimit := policy.ProtectTopNByViews / 20
	if perBucketLimit < 1 {
		perBucketLimit = 1
	}
	globalHotIDs := db.Model(&models.ContentItem{}).
		Select("id").
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("status = ?", models.ContentStatusReady).
		Where("is_feed_unit = TRUE").
		Where("feed_visibility = 'visible'").
		Where("file_size_bytes > 0").
		Where("COALESCE(published_at, created_at) > ?", cutoff).
		Order("(view_count + like_count * 2 + share_count * 4) DESC, COALESCE(published_at, created_at) DESC").
		Limit(policy.ProtectTopNByViews)
	flaggedIDs := db.Model(&models.ContentFlag{}).
		Select("content_item_id").
		Where("tenant_id = ?", tenantID).
		Where("pin_to_top = TRUE OR boost = TRUE")
	velocityIDs := db.Model(&models.UserInteraction{}).
		Select("content_item_id").
		Where("created_at > ?", time.Now().UTC().Add(-24*time.Hour)).
		Group("content_item_id").
		Order("COUNT(*) DESC").
		Limit(policy.ProtectTopNByViews)
	sourceHotIDs := db.Raw(`
		SELECT id FROM (
			SELECT id,
				ROW_NUMBER() OVER (
					PARTITION BY COALESCE(source_name, '')
					ORDER BY (view_count + like_count * 2 + share_count * 4) DESC, COALESCE(published_at, created_at) DESC
				) AS rn,
				(view_count + like_count * 2 + share_count * 4) AS hot_score
			FROM content_items
			WHERE tenant_id = ?
				AND type IN ('VIDEO', 'PODCAST')
				AND status = 'READY'
				AND is_feed_unit = TRUE
				AND feed_visibility = 'visible'
				AND COALESCE(file_size_bytes, 0) > 0
				AND COALESCE(published_at, created_at) > ?
		) ranked_source_hot
		WHERE rn <= ? AND hot_score > 0`, tenantID, cutoff, perSourceLimit)
	bucketHotIDs := db.Raw(`
		SELECT id FROM (
			SELECT id,
				ROW_NUMBER() OVER (
					PARTITION BY COALESCE(duration_bucket, 'unknown')
					ORDER BY (view_count + like_count * 2 + share_count * 4) DESC, COALESCE(published_at, created_at) DESC
				) AS rn,
				(view_count + like_count * 2 + share_count * 4) AS hot_score
			FROM content_items
			WHERE tenant_id = ?
				AND type IN ('VIDEO', 'PODCAST')
				AND status = 'READY'
				AND is_feed_unit = TRUE
				AND feed_visibility = 'visible'
				AND COALESCE(file_size_bytes, 0) > 0
				AND duration_bucket IS NOT NULL
				AND COALESCE(published_at, created_at) > ?
		) ranked_bucket_hot
		WHERE rn <= ? AND hot_score > 0`, tenantID, cutoff, perBucketLimit)
	return db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("status = ?", models.ContentStatusReady).
		Where("is_feed_unit = TRUE").
		Where("feed_visibility = 'visible'").
		Where("file_size_bytes > 0").
		Where(
			db.Where("COALESCE(published_at, created_at) > ?", newCutoff).
				Or("public_id IN (?)", flaggedIDs).
				Or("public_id IN (?)", velocityIDs).
				Or("id IN (?)", globalHotIDs).
				Or("id IN (?)", sourceHotIDs).
				Or("id IN (?)", bucketHotIDs),
		).
		Session(&gorm.Session{})
}

func atomizedParentSourceQuery(db *gorm.DB, tenantID string) *gorm.DB {
	return db.Model(&models.ContentItem{}).
		Where("tenant_id = ?", tenantID).
		Where("parent_content_item_id IS NULL").
		Where("type IN ?", []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).
		Where("duration_sec IS NOT NULL AND duration_sec > ?", forYouHardMaxDurationSec).
		Where("(storage_tier IS NULL OR storage_tier != 'cold')").
		Where("COALESCE(file_size_bytes, 0) > 0").
		Where("EXISTS (SELECT 1 FROM content_items child WHERE child.parent_content_item_id = content_items.public_id AND child.status = ? AND child.is_feed_unit = TRUE)", models.ContentStatusReady).
		Session(&gorm.Session{})
}

func classifyStorageHealth(policy models.StoragePolicy, proof storageProofMetrics, aggregationError string) string {
	if aggregationError != "" {
		return "degraded"
	}
	if proof.QuotaBytes <= 0 {
		return "healthy"
	}
	if !proof.ColdEnabled && proof.UtilizationPct >= float64(policy.TargetUtilizationPct) {
		return "degraded_no_cold"
	}
	if proof.UtilizationPct >= 95 {
		return "critical"
	}
	if proof.UtilizationPct >= float64(policy.TargetUtilizationPct) {
		return "pressure"
	}
	if proof.UtilizationPct >= float64(policy.TargetUtilizationPct)*0.8 {
		return "watch"
	}
	return "healthy"
}

func storageHealthScore(state string) int {
	switch state {
	case "healthy":
		return 95
	case "watch":
		return 75
	case "pressure":
		return 50
	case "degraded_no_cold", "degraded":
		return 35
	case "critical":
		return 15
	default:
		return 60
	}
}

func storageHealthSummary(state string, proof storageProofMetrics) string {
	switch state {
	case "healthy":
		return "Storage is within target and no immediate action is required."
	case "watch":
		return "Storage is approaching the target; prepare a bounded re-encode or cleanup pass."
	case "pressure":
		return "Storage is over target; run bounded storage relief while protecting hot feed units."
	case "critical":
		return "Storage is critically high; use approval-gated cleanup if safe actions are insufficient."
	case "degraded_no_cold":
		return "Cold tier is unavailable, so storage relief is limited to re-encode and recoverable deletion guardrails."
	case "degraded":
		return "Live storage metrics are degraded; use DB-backed recommendations cautiously."
	default:
		return fmt.Sprintf("Storage state is %s.", state)
	}
}

func storageRecommendationsFor(policy models.StoragePolicy, proof storageProofMetrics, state string) []storageRecommendation {
	recs := []storageRecommendation{}
	if state == "healthy" && proof.ParentSourceBytes == 0 {
		return append(recs, storageRecommendation{
			Key:      "no_action",
			Label:    "No immediate storage action",
			Detail:   "Storage is within target and parent-source pressure is low.",
			Severity: "info",
			Action:   "none",
		})
	}
	if !proof.ColdEnabled && (state == "degraded_no_cold" || proof.ParentSourceBytes > 0) {
		recs = append(recs, storageRecommendation{
			Key:      "configure_cold_tier",
			Label:    "Configure cold tier",
			Detail:   "Cold storage is optional but strongly recommended before broad parent cleanup.",
			Severity: "warning",
			Action:   "configure_cold_storage",
		})
	}
	if proof.CandidateBytes > 0 {
		action := "run_storage_sweep"
		label := "Run bounded storage sweep"
		if policy.ArchiveAction == "re_encode" {
			action = "run_reencode"
			label = "Run bounded re-encode"
		}
		recs = append(recs, storageRecommendation{
			Key:            "candidate_relief",
			Label:          label,
			Detail:         fmt.Sprintf("%d candidate items can reduce hot storage without changing feed visibility.", proof.CandidateCount),
			Severity:       severityForState(state),
			Action:         action,
			EstimatedBytes: proof.CandidateBytes,
			Metadata:       map[string]interface{}{"archive_action": policy.ArchiveAction},
		})
	}
	if proof.ParentSourceBytes > 0 {
		action := "review_parent_cleanup"
		if proof.ColdEnabled {
			action = "move_atomized_parents_cold"
		} else if strings.EqualFold(policy.ArchiveAction, "re_encode") {
			action = "reencode_atomized_parents"
		}
		recs = append(recs, storageRecommendation{
			Key:            "atomized_parent_sources",
			Label:          "Clean up atomized parent source files",
			Detail:         "Child chapters are the product units; raw parents can be cold-moved, re-encoded, or recoverable-deleted under guardrails.",
			Severity:       severityForState(state),
			Action:         action,
			EstimatedBytes: proof.ParentSourceBytes,
			Metadata:       map[string]interface{}{"parent_source_count": proof.ParentSourceCount},
		})
	}
	if proof.MissingCount > 0 {
		recs = append(recs, storageRecommendation{
			Key:      "missing_artifacts",
			Label:    "Investigate missing artifacts",
			Detail:   "Some content items are marked missing and may need recovery or re-ingest.",
			Severity: "warning",
			Action:   "review_ledger",
		})
	}
	return recs
}

func severityForState(state string) string {
	switch state {
	case "critical", "degraded_no_cold":
		return "critical"
	case "pressure", "degraded":
		return "warning"
	default:
		return "info"
	}
}
