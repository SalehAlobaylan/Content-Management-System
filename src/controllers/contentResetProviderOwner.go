package controllers

import (
	"context"
	"encoding/json"
	"errors"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type contentResetProviderReleaseOwner struct{}

func (contentResetProviderReleaseOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/source-run", Effect: "release_provider_slot", TargetType: "source_branch", Version: "v1"}
}

func (owner contentResetProviderReleaseOwner) page(command contentreset.Command) (uuid.UUID, error) {
	var parameters struct {
		PageID uuid.UUID `json:"page_id"`
	}
	if command.Contract != owner.Contract() || json.Unmarshal(command.Parameters, &parameters) != nil || parameters.PageID == uuid.Nil || contentreset.Hash(command.Parameters) != contentreset.Hash(map[string]uuid.UUID{"page_id": parameters.PageID}) {
		return uuid.Nil, errors.New("invalid provider release command")
	}
	return parameters.PageID, nil
}

func (owner contentResetProviderReleaseOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	pageID, err := owner.page(command)
	if err != nil {
		return contentreset.Observation{}, err
	}
	var release *models.ContentResetProviderRelease
	var terminal bool
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		release, terminal, err = supply.ReleaseContentResetProviderSlot(tx, command, token, pageID)
		return err
	})
	if errors.Is(err, supply.ErrReplayYield) {
		return resetObservation(command, "deferred", "provider_graph_pending", map[string]any{"provider_slot_released": false}), nil
	}
	if err != nil {
		return contentreset.Observation{}, err
	}
	return contentResetProviderReleaseObservation(command, release, terminal), nil
}

func (owner contentResetProviderReleaseOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	pageID, err := owner.page(command)
	if err != nil {
		return contentreset.Observation{}, err
	}
	var release *models.ContentResetProviderRelease
	var terminal bool
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		release, terminal, err = supply.ReadContentResetProviderRelease(tx, command, pageID)
		return err
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "failed", ReasonCode: "provider_release_not_committed", Evidence: json.RawMessage(`{"provider_slot_released":false}`), NoEffectProven: true}, nil
	}
	if err != nil {
		return contentreset.Observation{}, err
	}
	return contentResetProviderReleaseObservation(command, release, terminal), nil
}

func contentResetProviderReleaseObservation(command contentreset.Command, release *models.ContentResetProviderRelease, terminal bool) contentreset.Observation {
	evidence := map[string]any{"provider_slot_released": true, "native_request_terminal": terminal, "delivery_verified": false}
	if release != nil {
		evidence["release_id"] = release.PublicID
		evidence["proof_hash"] = release.ProofHash
		evidence["request_id"] = release.SourceRunRequestID
		evidence["released_at"] = release.ReleasedAt
	}
	payload, _ := json.Marshal(evidence)
	return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "provider_slot_settled", Evidence: payload}
}
