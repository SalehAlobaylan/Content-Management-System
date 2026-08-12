package supply

import (
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// ReadyOnlyImpactInventory is a read-only release-gate report. It identifies
// media rows that would stop serving under the canonical READY-only Pods
// predicate, without reviving ARCHIVED rows or changing feed state.
type ReadyOnlyImpactInventory struct {
	TenantID    string                  `json:"tenant_id"`
	GeneratedAt time.Time               `json:"generated_at"`
	Cohorts     []ReadyOnlyImpactCohort `json:"cohorts"`
}

type ReadyOnlyImpactCohort struct {
	Key             string `json:"key"`
	Count           int64  `json:"count"`
	RemediationPath string `json:"remediation_path"`
}

// InspectReadyOnlyImpact never updates content. Counts are tenant-scoped and
// intentionally classify rows by their native lifecycle owner instead of
// treating archived inventory as a serving compatibility fallback.
func InspectReadyOnlyImpact(db *gorm.DB, tenantID string) (ReadyOnlyImpactInventory, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" {
		return ReadyOnlyImpactInventory{}, fmt.Errorf("READY-only impact inventory requires an explicit tenant")
	}
	report := ReadyOnlyImpactInventory{TenantID: tenantID, GeneratedAt: time.Now().UTC()}
	cohorts := []struct {
		key, remediation, condition string
		args                        []any
	}{
		{"archived_media", "storage_recovery_or_native_media_lifecycle", "type IN ? AND status = ?", []any{[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, models.ContentStatusArchived}},
		{"held_pre_ready", "pipeline_or_owner_stage_repair", "type IN ? AND status IN ?", []any{[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, []models.ContentStatus{models.ContentStatusPending, models.ContentStatusProcessing, models.ContentStatusFailed}}},
		{"audio_only_ready", "none_audio_first_is_valid", "type IN ? AND status = ? AND media_url IS NOT NULL AND media_url <> '' AND playback_url IS NULL", []any{[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, models.ContentStatusReady}},
		{"ready_hidden_or_nonunit", "media_circulation_or_atomization_review", "type IN ? AND status = ? AND (is_feed_unit = FALSE OR feed_visibility <> ?)", []any{[]models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, models.ContentStatusReady, "visible"}},
	}
	for _, cohort := range cohorts {
		var count int64
		if err := db.Model(&models.ContentItem{}).Where("tenant_id = ?", tenantID).Where(cohort.condition, cohort.args...).Count(&count).Error; err != nil {
			return ReadyOnlyImpactInventory{}, err
		}
		report.Cohorts = append(report.Cohorts, ReadyOnlyImpactCohort{Key: cohort.key, Count: count, RemediationPath: cohort.remediation})
	}
	return report, nil
}
