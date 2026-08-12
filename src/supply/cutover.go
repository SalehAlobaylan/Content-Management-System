package supply

import (
	"fmt"
	"strings"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

const (
	admissionProtocolKey        = "source-run/v1"
	admissionEpochCompatibility = "compatibility"
	admissionEpochDurable       = "durable_required"
	admissionModeDurable        = "durable"
)

// RequireDurableAdmission makes the cutover contract explicit at every new
// CMS admission/claim boundary. In compatibility the durable writer remains
// off; after the epoch moves to durable_required every tenant/lane must have
// an independently provisioned durable row. A missing table/row is never
// interpreted as an implicit default tenant or a successful cutover.
func RequireDurableAdmission(db *gorm.DB, tenantID, lane string) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(lane) == "" {
		return fmt.Errorf("durable source-run admission requires explicit tenant and lane")
	}
	if lane != "media" && lane != "news" {
		return fmt.Errorf("source-run lane is not admitted")
	}
	var protocol models.SourceRunAdmissionProtocol
	if err := db.Where("protocol_key = ?", admissionProtocolKey).First(&protocol).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return fmt.Errorf("source-run durable admission is not activated")
		}
		return fmt.Errorf("source-run admission protocol is unavailable: %w", err)
	}
	if protocol.Epoch != admissionEpochDurable {
		return fmt.Errorf("source-run durable admission is not activated")
	}
	var cutover models.SourceRunAdmissionCutover
	if err := db.Where("tenant_id = ? AND lane = ?", tenantID, lane).First(&cutover).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return fmt.Errorf("source-run durable admission is not provisioned for tenant lane")
		}
		return fmt.Errorf("source-run admission cutover is unavailable: %w", err)
	}
	if cutover.Mode != admissionModeDurable || cutover.Protocol != ContractVersion {
		return fmt.Errorf("source-run tenant lane is not durable")
	}
	return nil
}

// RequireLegacyAdmission is the reciprocal compatibility guard used by old
// direct handlers. It preserves migration compatibility until the durable
// epoch is explicitly activated, then denies new legacy work.
func RequireLegacyAdmission(db *gorm.DB, tenantID, lane string) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(lane) == "" {
		return fmt.Errorf("legacy source-run admission requires explicit tenant and lane")
	}
	// Deployment remains mixed-version compatible: before the forward migration
	// exists, the established legacy writer remains available. Once the table
	// exists, an unavailable/invalid protocol fails closed instead.
	if !db.Migrator().HasTable(&models.SourceRunAdmissionProtocol{}) {
		return nil
	}
	var protocol models.SourceRunAdmissionProtocol
	if err := db.Where("protocol_key = ?", admissionProtocolKey).First(&protocol).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil // Migration absent: preserve the pre-cutover compatibility path.
		}
		return fmt.Errorf("source-run admission protocol is unavailable: %w", err)
	}
	if protocol.Epoch == admissionEpochDurable {
		return fmt.Errorf("legacy source-run admission is retired")
	}
	if protocol.Epoch != admissionEpochCompatibility {
		return fmt.Errorf("source-run admission protocol has an invalid epoch")
	}
	return nil
}
