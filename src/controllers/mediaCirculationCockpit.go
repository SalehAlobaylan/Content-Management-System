package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type mediaCirculationCockpitHealth struct {
	Headline    string   `json:"headline"`
	Score       int      `json:"score"`
	Summary     string   `json:"summary"`
	Reasons     []string `json:"reasons"`
	GeneratedAt string   `json:"generated_at"`
	Enabled     bool     `json:"enabled"`
}

type mediaCirculationCockpitBucket struct {
	Bucket        string  `json:"bucket"`
	VisibleUnits  int64   `json:"visible_units"`
	State         string  `json:"state"`
	ThinFloor     int64   `json:"thin_floor"`
	SaturatedCeil int64   `json:"saturated_ceil"`
	SharePct      float64 `json:"share_pct"`
}

type mediaCirculationCockpitSummary struct {
	Total          int64            `json:"total"`
	Pending        int64            `json:"pending"`
	Applied        int64            `json:"applied"`
	Dismissed      int64            `json:"dismissed"`
	Superseded     int64            `json:"superseded"`
	ByUnitType     map[string]int64 `json:"by_unit_type"`
	ByVerdict      map[string]int64 `json:"by_verdict"`
	ByStatus       map[string]int64 `json:"by_status"`
	ByActionLane   map[string]int64 `json:"by_action_lane"`
	NeedsAttention int64            `json:"needs_attention"`
}

type mediaCirculationCockpitRecommendation struct {
	models.MediaCirculationRecommendation
	DisplayTitle    string   `json:"display_title"`
	DisplaySubtitle string   `json:"display_subtitle"`
	ActionLane      string   `json:"action_lane"`
	PriorityLabel   string   `json:"priority_label"`
	PrimaryMetric   string   `json:"primary_metric"`
	ProofPoints     []string `json:"proof_points"`
}

type mediaCirculationCockpitResponse struct {
	Health          mediaCirculationCockpitHealth           `json:"health"`
	Storage         storageProofMetrics                     `json:"storage"`
	Buckets         []mediaCirculationCockpitBucket         `json:"buckets"`
	Summary         mediaCirculationCockpitSummary          `json:"summary"`
	Policy          models.MediaCirculationPolicy           `json:"policy"`
	Recommendations []mediaCirculationCockpitRecommendation `json:"recommendations"`
}

func GetMediaCirculationCockpit(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	health := buildMediaCirculationHealth(db, principal.TenantID, c.GetHeader("Authorization"))
	rows := loadMediaCirculationCockpitRows(db, principal.TenantID)
	resp := mediaCirculationCockpitResponse{
		Health: mediaCirculationCockpitHealth{
			Headline:    health.Headline,
			Score:       health.Score,
			Summary:     health.Summary,
			Reasons:     health.Reasons,
			GeneratedAt: health.GeneratedAt,
			Enabled:     health.Policy.Enabled,
		},
		Storage:         health.Proof.Storage,
		Buckets:         cockpitBuckets(health.Proof.Buckets),
		Policy:          health.Policy,
		Recommendations: rows,
	}
	resp.Summary = summarizeCockpitRecommendations(rows)
	c.JSON(http.StatusOK, resp)
}

func loadMediaCirculationCockpitRows(db *gorm.DB, tenantID string) []mediaCirculationCockpitRecommendation {
	var rows []models.MediaCirculationRecommendation
	db.Where("tenant_id = ?", tenantID).
		Order("status = 'pending' DESC, applied ASC, score DESC, updated_at DESC").
		Limit(300).
		Find(&rows)
	if len(rows) == 0 {
		return []mediaCirculationCockpitRecommendation{}
	}

	sourceIDs := []uuid.UUID{}
	itemIDs := []uuid.UUID{}
	for _, r := range rows {
		switch r.UnitType {
		case models.MediaCirculationUnitSource:
			sourceIDs = append(sourceIDs, r.SubjectID)
		case models.MediaCirculationUnitItemFamily:
			itemIDs = append(itemIDs, r.SubjectID)
		}
	}

	sources := map[uuid.UUID]models.ContentSource{}
	if len(sourceIDs) > 0 {
		var sourceRows []models.ContentSource
		db.Where("tenant_id = ? AND public_id IN ?", tenantID, sourceIDs).Find(&sourceRows)
		for _, s := range sourceRows {
			sources[s.PublicID] = s
		}
	}

	items := map[uuid.UUID]models.ContentItem{}
	if len(itemIDs) > 0 {
		var itemRows []models.ContentItem
		db.Where("tenant_id = ? AND public_id IN ?", tenantID, itemIDs).Find(&itemRows)
		for _, it := range itemRows {
			items[it.PublicID] = it
		}
	}

	out := make([]mediaCirculationCockpitRecommendation, 0, len(rows))
	for _, r := range rows {
		metrics := mediaCircMetricsMap(r)
		displayTitle, displaySubtitle := cockpitDisplayText(r, metrics, sources, items)
		proof := cockpitProofPoints(r, metrics)
		out = append(out, mediaCirculationCockpitRecommendation{
			MediaCirculationRecommendation: r,
			DisplayTitle:                   displayTitle,
			DisplaySubtitle:                displaySubtitle,
			ActionLane:                     mediaCircActionLane(r.Verdict),
			PriorityLabel:                  mediaCircPriorityLabel(r),
			PrimaryMetric:                  cockpitPrimaryMetric(r, metrics),
			ProofPoints:                    proof,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Status == models.MediaCirculationRecStatusPending && out[j].Status != models.MediaCirculationRecStatusPending {
			return true
		}
		if out[i].Status != models.MediaCirculationRecStatusPending && out[j].Status == models.MediaCirculationRecStatusPending {
			return false
		}
		if out[i].ActionLane != out[j].ActionLane {
			return mediaCircLaneOrder(out[i].ActionLane) < mediaCircLaneOrder(out[j].ActionLane)
		}
		return out[i].Score > out[j].Score
	})
	return out
}

func cockpitBuckets(buckets []libraryBucketHealth) []mediaCirculationCockpitBucket {
	total := int64(0)
	for _, b := range buckets {
		total += b.VisibleUnits
	}
	out := make([]mediaCirculationCockpitBucket, 0, len(buckets))
	for _, b := range buckets {
		share := 0.0
		if total > 0 {
			share = float64(b.VisibleUnits) / float64(total) * 100
		}
		out = append(out, mediaCirculationCockpitBucket{
			Bucket:        b.Bucket,
			VisibleUnits:  b.VisibleUnits,
			State:         b.State,
			ThinFloor:     mediaCirculationBucketThinFloor,
			SaturatedCeil: mediaCirculationBucketSaturatedCeil,
			SharePct:      share,
		})
	}
	return out
}

func summarizeCockpitRecommendations(rows []mediaCirculationCockpitRecommendation) mediaCirculationCockpitSummary {
	s := mediaCirculationCockpitSummary{
		Total:        int64(len(rows)),
		ByUnitType:   map[string]int64{},
		ByVerdict:    map[string]int64{},
		ByStatus:     map[string]int64{},
		ByActionLane: map[string]int64{},
	}
	for _, r := range rows {
		s.ByUnitType[r.UnitType]++
		s.ByVerdict[r.Verdict]++
		s.ByStatus[r.Status]++
		s.ByActionLane[r.ActionLane]++
		switch r.Status {
		case models.MediaCirculationRecStatusPending:
			s.Pending++
			if cockpitNeedsAttention(r) {
				s.NeedsAttention++
			}
		case models.MediaCirculationRecStatusApplied:
			s.Applied++
		case models.MediaCirculationRecStatusDismissed:
			s.Dismissed++
		case models.MediaCirculationRecStatusSuperseded:
			s.Superseded++
		}
	}
	return s
}

func mediaCircActionLane(verdict string) string {
	switch verdict {
	case mediaCircVerdictPullNow, mediaCircVerdictDeepPull:
		return "pull"
	case mediaCircVerdictPullLimited, mediaCircVerdictSkipSource, mediaCircVerdictPauseSource:
		return "limit_skip"
	case mediaCircVerdictProtect:
		return "protect"
	case mediaCircVerdictReEncode, mediaCircVerdictMoveToCold, mediaCircVerdictRecoverableDelete:
		return "cool"
	case mediaCircVerdictRankDown:
		return "downrank"
	case mediaCircVerdictNeedsAdminReview:
		return "review"
	default:
		return "review"
	}
}

func mediaCircLaneOrder(lane string) int {
	switch lane {
	case "pull":
		return 0
	case "downrank":
		return 1
	case "cool":
		return 2
	case "review":
		return 3
	case "limit_skip":
		return 4
	case "protect":
		return 5
	default:
		return 9
	}
}

func mediaCircPriorityLabel(rec models.MediaCirculationRecommendation) string {
	if rec.Status != models.MediaCirculationRecStatusPending {
		return strings.ReplaceAll(rec.Status, "_", " ")
	}
	switch rec.Verdict {
	case mediaCircVerdictDeepPull:
		return "High-yield pull"
	case mediaCircVerdictPullNow:
		return "Pull now"
	case mediaCircVerdictRankDown:
		return "Reduce exposure"
	case mediaCircVerdictRecoverableDelete:
		return "Cost reclaim"
	case mediaCircVerdictMoveToCold:
		return "Move cold"
	case mediaCircVerdictReEncode:
		return "Optimize bytes"
	case mediaCircVerdictNeedsAdminReview:
		return "Needs review"
	case mediaCircVerdictPauseSource:
		return "Pause source"
	case mediaCircVerdictPullLimited:
		return "Limited pull"
	case mediaCircVerdictProtect:
		return "Protected"
	default:
		return strings.ReplaceAll(rec.Verdict, "_", " ")
	}
}

func cockpitNeedsAttention(r mediaCirculationCockpitRecommendation) bool {
	switch r.ActionLane {
	case "pull", "downrank", "cool", "review":
		return true
	default:
		return false
	}
}

func mediaCircMetricsMap(rec models.MediaCirculationRecommendation) map[string]interface{} {
	if len(rec.Metrics) == 0 {
		return map[string]interface{}{}
	}
	var metrics map[string]interface{}
	if err := json.Unmarshal(rec.Metrics, &metrics); err != nil || metrics == nil {
		return map[string]interface{}{}
	}
	return metrics
}

func cockpitDisplayText(rec models.MediaCirculationRecommendation, metrics map[string]interface{}, sources map[uuid.UUID]models.ContentSource, items map[uuid.UUID]models.ContentItem) (string, string) {
	if rec.UnitType == models.MediaCirculationUnitSource {
		if source, ok := sources[rec.SubjectID]; ok {
			return source.Name, fmt.Sprintf("%s source · interval %dm", source.Type, source.FetchIntervalMinutes)
		}
		if name := stringMetric(metrics, "source_name"); name != "" {
			return name, "Media source"
		}
		return "Unknown source", rec.SubjectID.String()
	}

	if item, ok := items[rec.SubjectID]; ok {
		title := "Untitled media"
		if item.Title != nil && strings.TrimSpace(*item.Title) != "" {
			title = strings.TrimSpace(*item.Title)
		}
		source := "unknown source"
		if item.SourceName != nil && strings.TrimSpace(*item.SourceName) != "" {
			source = strings.TrimSpace(*item.SourceName)
		}
		role := stringMetric(metrics, "role")
		if role != "" {
			return title, fmt.Sprintf("%s · %s", source, strings.ReplaceAll(role, "_", " "))
		}
		return title, source
	}
	return strings.ReplaceAll(rec.SubjectKind, "_", " "), rec.SubjectID.String()
}

func cockpitPrimaryMetric(rec models.MediaCirculationRecommendation, metrics map[string]interface{}) string {
	switch rec.UnitType {
	case models.MediaCirculationUnitSource:
		if allowed := intMetric(metrics, "allowed_intake"); allowed > 0 {
			return fmt.Sprintf("%d allowed", allowed)
		}
		if failure := floatMetric(metrics, "failure_rate"); failure > 0 {
			return fmt.Sprintf("%.0f%% failed", failure*100)
		}
	case models.MediaCirculationUnitItemFamily:
		if bytes := int64Metric(metrics, "file_size_bytes"); bytes > 0 {
			return humanBytes(bytes)
		}
		if value := floatMetric(metrics, "value"); value > 0 {
			return fmt.Sprintf("value %.2f", value)
		}
		if views := int64Metric(metrics, "view_count"); views > 0 {
			return fmt.Sprintf("%d views", views)
		}
	}
	return fmt.Sprintf("score %.3f", rec.Score)
}

func cockpitProofPoints(rec models.MediaCirculationRecommendation, metrics map[string]interface{}) []string {
	points := []string{}
	if rec.Reasons != nil {
		var reasons []string
		if err := json.Unmarshal(rec.Reasons, &reasons); err == nil {
			for _, r := range reasons {
				if strings.TrimSpace(r) != "" {
					points = append(points, r)
				}
			}
		}
	}
	if qp := floatMetric(metrics, "quality_prior"); qp > 0 {
		points = append(points, fmt.Sprintf("Quality prior %.2f", qp))
	}
	if bm := floatMetric(metrics, "bucket_demand_match"); bm > 0 {
		points = append(points, fmt.Sprintf("Bucket demand %.2f", bm))
	}
	if thin := stringSliceMetric(metrics, "matched_thin_buckets"); len(thin) > 0 {
		points = append(points, "Fills thin buckets: "+strings.Join(thin, ", ")+"m")
	}
	if role := stringMetric(metrics, "role"); role != "" {
		points = append(points, "Storage role: "+strings.ReplaceAll(role, "_", " "))
	}
	if len(points) > 5 {
		return points[:5]
	}
	return points
}

func stringMetric(metrics map[string]interface{}, key string) string {
	if raw, ok := metrics[key]; ok {
		if value, ok := raw.(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stringSliceMetric(metrics map[string]interface{}, key string) []string {
	raw, ok := metrics[key]
	if !ok {
		return nil
	}
	values, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	out := []string{}
	for _, v := range values {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func floatMetric(metrics map[string]interface{}, key string) float64 {
	if raw, ok := metrics[key]; ok {
		switch v := raw.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}

func intMetric(metrics map[string]interface{}, key string) int {
	return int(int64Metric(metrics, key))
}

func int64Metric(metrics map[string]interface{}, key string) int64 {
	if raw, ok := metrics[key]; ok {
		switch v := raw.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		case int64:
			return v
		}
	}
	return 0
}

func humanBytes(bytes int64) string {
	if bytes <= 0 {
		return "0 B"
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	value := float64(bytes)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", bytes, units[unit])
	}
	return fmt.Sprintf("%.1f %s", value, units[unit])
}
