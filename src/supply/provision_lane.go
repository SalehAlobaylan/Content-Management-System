package supply

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
	"time"
)

// This exceptional owner operation provisions an inactive lane after a global
// durable cutover. It cannot promote compatibility, activate sources or work.
type InactiveLanePreview struct {
	Tenant             string `json:"tenant"`
	Lane               string `json:"lane"`
	ProtocolVersion    int64  `json:"protocol_version"`
	AlreadyProvisioned bool   `json:"already_provisioned"`
	Digest             string `json:"digest"`
}

func PreviewInactiveLane(db *gorm.DB, tenant, lane string) (InactiveLanePreview, error) {
	p := InactiveLanePreview{Tenant: tenant, Lane: lane}
	if strings.TrimSpace(tenant) == "" || (lane != "news" && lane != "media") {
		return p, fmt.Errorf("explicit tenant and News/Media lane required")
	}
	var protocol models.SourceRunAdmissionProtocol
	if err := db.Where("protocol_key=?", admissionProtocolKey).First(&protocol).Error; err != nil {
		return p, err
	}
	if protocol.Epoch != admissionEpochDurable {
		return p, fmt.Errorf("global source-run authority must already be durable")
	}
	p.ProtocolVersion = protocol.Version
	var count int64
	if err := db.Model(&models.ContentSource{}).Where("tenant_id=? AND category=? AND is_active=TRUE", tenant, lane).Count(&count).Error; err != nil {
		return p, err
	}
	if count > 0 {
		return p, fmt.Errorf("lane must have no active sources; use normal cutover qualification for active inventory")
	}
	if err := db.Model(&models.SourceRunRequest{}).Where("tenant_id=? AND lane=? AND state IN ?", tenant, lane, []string{"requested", "accepted", "running", "verification_required"}).Count(&count).Error; err != nil {
		return p, err
	}
	if count > 0 {
		return p, fmt.Errorf("inactive lane has unresolved source work")
	}
	var existing models.SourceRunAdmissionCutover
	err := db.Where("tenant_id=? AND lane=?", tenant, lane).First(&existing).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return p, err
	}
	if err == nil {
		if existing.Mode != admissionModeDurable || existing.Protocol != ContractVersion {
			return p, fmt.Errorf("existing lane authority requires investigation")
		}
		p.AlreadyProvisioned = true
	}
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	p.Digest = hex.EncodeToString(sum[:])
	return p, nil
}
func ProvisionInactiveLane(db *gorm.DB, tenant, lane, digest, actor string) error {
	if strings.TrimSpace(actor) == "" || digest == "" {
		return fmt.Errorf("actor and exact preview digest required")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		// Prevent a source activation between the reviewed absence proof and write.
		if err := tx.Exec("LOCK TABLE content_sources, source_run_requests IN SHARE MODE").Error; err != nil {
			return err
		}
		var protocol models.SourceRunAdmissionProtocol
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("protocol_key=?", admissionProtocolKey).First(&protocol).Error; err != nil {
			return err
		}
		fence, err := utils.ReadWriterFence(tx)
		if err != nil {
			return err
		}
		if fence.State != "open" {
			return fmt.Errorf("writer fence does not authorize lane provisioning")
		}
		p, err := PreviewInactiveLane(tx, tenant, lane)
		if err != nil {
			return err
		}
		if p.Digest != digest {
			return fmt.Errorf("lane preview changed")
		}
		if p.AlreadyProvisioned {
			return nil
		}
		now := time.Now().UTC()
		row := models.SourceRunAdmissionCutover{TenantID: tenant, Lane: lane, Mode: admissionModeDurable, Protocol: ContractVersion, Version: protocol.Version, ActivatedAt: &now, ActivatedBy: actor}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		payload, _ := json.Marshal(p)
		return tx.Create(&models.SourceRunCutoverAuditEvent{EventType: "inactive_lane_provisioned", FromEpoch: admissionEpochDurable, ToEpoch: admissionEpochDurable, VerificationDigest: digest, Actor: actor, Payload: datatypes.JSON(payload), OccurredAt: now}).Error
	})
}
