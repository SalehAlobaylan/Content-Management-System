package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/supply"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// createDurableSourceRun admits source work into the CMS-owned request ledger.
// Aggregation claims the request through the durable dispatcher; this boundary
// must never call Aggregation's retired /admin/trigger endpoint directly.
func createDurableSourceRun(
	db *gorm.DB,
	source models.ContentSource,
	requestedBy string,
	actorID string,
	suggestionID *uuid.UUID,
	now time.Time,
) (models.SourceRunRequest, bool, error) {
	var request models.SourceRunRequest
	var created bool
	err := db.Transaction(func(tx *gorm.DB) error {
		var current models.ContentSource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ? AND public_id = ?", source.TenantID, source.PublicID).
			First(&current).Error; err != nil {
			return err
		}
		lifecycleLane := current.Category
		if lifecycleLane == models.SourceCategoryMedia {
			lifecycleLane = "pods"
		}
		if err := lifecycle.Check(tx, lifecycle.Scope{
			TenantID: current.TenantID,
			Lane:     lifecycleLane,
			SourceID: current.PublicID.String(),
		}, lifecycle.PhaseSourceAdmission); err != nil {
			return err
		}

		active, err := supply.CountSourceAdmissionBlockers(tx, current.TenantID, current.PublicID)
		if err != nil {
			return err
		}
		if active > 0 {
			return ErrSourceRunAlreadyActive
		}

		identity, err := durableSourceRunIdentity(current, now)
		if err != nil {
			return err
		}
		metadata, err := durableSourceRunMetadataForTenant(tx, current)
		if err != nil {
			return err
		}
		request, created, err = supply.CreateRequest(tx, supply.CreateRequestInput{
			Source:              current,
			Identity:            identity,
			RequestedBy:         requestedBy,
			RequestedByActorID:  actorID,
			SourceSuggestionID:  suggestionID,
			EvidenceFingerprint: "manual-source-run:" + current.PublicID.String() + ":" + now.UTC().Format(time.RFC3339Nano),
			Metadata:            metadata,
		})
		return err
	})
	return request, created, err
}

func durableSourceRunIdentity(source models.ContentSource, now time.Time) (supply.RequestIdentity, error) {
	lane := strings.TrimSpace(source.Category)
	if lane != models.SourceCategoryNews && lane != models.SourceCategoryMedia {
		return supply.RequestIdentity{}, fmt.Errorf("source category is not an admitted run lane")
	}
	version := source.SourceConfigVersion
	if version < 1 {
		version = 1
	}
	argument := sha256.Sum256([]byte(strings.Join([]string{
		string(source.Type), lane, sourceRunFeedURL(source.FeedURL), string(source.APIConfig),
	}, "\n")))
	policy := sha256.Sum256([]byte(strings.Join([]string{
		supply.ContractVersion, "manual_source_run", lane, fmt.Sprintf("%d", version),
	}, "\n")))
	return supply.RequestIdentity{
		TenantID:            source.TenantID,
		ContentSourceID:     source.PublicID.String(),
		Lane:                lane,
		Purpose:             "manual",
		CadenceWindowStart:  now.UTC(),
		SourceConfigVersion: version,
		PolicyFingerprint:   hex.EncodeToString(policy[:]),
		ArgumentFingerprint: hex.EncodeToString(argument[:]),
	}, nil
}

func durableSourceRunMetadata(source models.ContentSource) (datatypes.JSON, error) {
	return durableSourceRunMetadataForTenant(nil, source)
}

func durableSourceRunMetadataForTenant(db *gorm.DB, source models.ContentSource) (datatypes.JSON, error) {
	metadata := map[string]any{
		"schema_version":     supply.ContractVersion,
		"max_results":        50,
		"max_provider_calls": 20,
		"max_bytes":          64 * 1024 * 1024,
	}
	if source.Category == models.SourceCategoryMedia {
		metadata["max_provider_calls"] = 8
		metadata["min_duration_minutes"] = 4.5
		metadata["max_results"] = supply.PodsSourceRunItemLimit(db, source.TenantID)
	}
	raw, err := json.Marshal(metadata)
	return datatypes.JSON(raw), err
}

func sourceRunFeedURL(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}
