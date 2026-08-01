package operator

import (
	"fmt"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// EnsureToolCapabilityEnabled interprets capability controls as disable-only
// emergency stops. A row can never add a tool: it can only suppress an entry
// that already exists in the static catalog. Every approval and execution
// claim calls this inside its transaction so a newly disabled capability fails
// closed even for a previously previewed plan.
func EnsureToolCapabilityEnabled(db *gorm.DB, tenantID, toolKey string, now time.Time) error {
	var disabled int64
	err := db.Model(&models.OperatorCapabilityControl{}).
		Where("tenant_id=? AND capability_kind=? AND capability_key=? AND disabled=? AND (expires_at IS NULL OR expires_at>?)", tenantID, "tool", toolKey, true, now).
		Count(&disabled).Error
	if err != nil {
		return fmt.Errorf("%w: tool capability control is unavailable", ErrAccessUnavailable)
	}
	if disabled != 0 {
		return fmt.Errorf("%w: tool capability is disabled", ErrForbiddenTool)
	}
	return nil
}

// EnsureAdapterCapabilityEnabled is used by scheduled investigations before
// they claim work. Adapter controls are independent from tool controls and can
// only turn a known read adapter off; they cannot add a new adapter.
func EnsureAdapterCapabilityEnabled(db *gorm.DB, tenantID, adapterKey string, now time.Time) error {
	var disabled int64
	err := db.Model(&models.OperatorCapabilityControl{}).
		Where("tenant_id=? AND capability_kind=? AND capability_key=? AND disabled=? AND (expires_at IS NULL OR expires_at>?)", tenantID, "adapter", adapterKey, true, now).
		Count(&disabled).Error
	if err != nil {
		return fmt.Errorf("%w: adapter capability control is unavailable", ErrAccessUnavailable)
	}
	if disabled != 0 {
		return fmt.Errorf("%w: adapter capability is disabled", ErrForbiddenTool)
	}
	return nil
}

// EnsureSystemCapabilityEnabled applies the four tenant-wide, disable-only
// stops. The key is deliberately closed over by callers; a control row cannot
// create a capability or turn an unrelated one back on.
func EnsureSystemCapabilityEnabled(db *gorm.DB, tenantID, key string, now time.Time) error {
	if key != "read" && key != "llm" && key != "execution" && key != "schedules" {
		return fmt.Errorf("%w: unknown system capability", ErrForbiddenTool)
	}
	var disabled int64
	err := db.Model(&models.OperatorCapabilityControl{}).
		Where("tenant_id=? AND capability_kind=? AND capability_key=? AND disabled=? AND (expires_at IS NULL OR expires_at>?)", tenantID, "system", key, true, now).
		Count(&disabled).Error
	if err != nil {
		return fmt.Errorf("%w: system capability control is unavailable", ErrAccessUnavailable)
	}
	if disabled != 0 {
		return fmt.Errorf("%w: system capability is disabled", ErrForbiddenTool)
	}
	return nil
}
