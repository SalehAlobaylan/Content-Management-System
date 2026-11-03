// Package podscontrol exposes the existing Pods lane control at every pipeline
// boundary. It never changes source selections or the published feed.
package podscontrol

import (
	"content-management-system/src/models"
	"crypto/sha256"
	"errors"
	"fmt"
	"gorm.io/gorm"
)

var ErrChanged = errors.New("Pods pipeline settings changed; refresh before applying")

func Default(tenant string) models.ContentStageControl {
	return models.ContentStageControl{TenantID: tenant, Lane: models.ContentStageLanePods, SchedulingEnabled: true, ExecutionEnabled: true, OptionalMetadataEnabled: true, TranscriptExecutionEnabled: true}
}

func Read(db *gorm.DB, tenant string) (models.ContentStageControl, error) {
	value := Default(tenant)
	err := db.Where("tenant_id=? AND lane='pods'", tenant).First(&value).Error
	if err == gorm.ErrRecordNotFound {
		return Default(tenant), nil
	}
	return value, err
}

func Version(value models.ContentStageControl) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%t/%t/%t/%t/%s", value.TenantID, value.SchedulingEnabled, value.ExecutionEnabled, value.OptionalMetadataEnabled, value.TranscriptExecutionEnabled, value.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z")))))
}

func Lock(db *gorm.DB, tenant string, exclusive bool) error {
	fn := "pg_advisory_xact_lock_shared"
	if exclusive {
		fn = "pg_advisory_xact_lock"
	}
	return db.Exec("SELECT "+fn+"(hashtextextended(?,0))", "pods-automation/v1/"+tenant).Error
}

// Allowed runs inside the caller's transaction, serializing new admissions and
// publication with pause. Receipts and verification remain available while held.
func Allowed(tx *gorm.DB, tenant string) (bool, error) {
	if err := Lock(tx, tenant, false); err != nil {
		return false, err
	}
	value, err := Read(tx, tenant)
	return value.SchedulingEnabled && value.ExecutionEnabled, err
}

// SourcePredicate is applied before bounded candidate selection, so paused Pods
// cannot consume another tenant's or News lane's fairness window.
// alias is code-owned, never taken from an HTTP request.
func SourcePredicate(db *gorm.DB, alias string) string {
	if !db.Migrator().HasTable(&models.ContentStageControl{}) {
		return alias + ".category<>'media'"
	}
	return fmt.Sprintf("(%s.category<>'media' OR NOT EXISTS (SELECT 1 FROM content_stage_controls pc WHERE pc.tenant_id=%s.tenant_id AND pc.lane='pods' AND (NOT pc.scheduling_enabled OR NOT pc.execution_enabled)))", alias, alias)
}
