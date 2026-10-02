// Package contentreset owns campaign command delivery. Domain effects remain
// with their existing owners; an HTTP caller cannot submit executable commands.
package contentreset

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const OwnerQualificationRubricVersion = "content-reset-owner-qualification/v1"

var sha256Hex = regexp.MustCompile(`^[a-f0-9]{64}$`)

// qualificationVersions is the code-owned release expectation per owner
// contract. A durable qualification row only takes effect when its recorded
// version equals the version this build expects, so an old or hand-written row
// cannot silently enable a new effect implementation.
var qualificationVersions = map[Contract]string{
	{Owner: "cms/content-reset", Effect: "prepare", TargetType: "campaign", Version: "v1"}:                 "content-reset-owner/v1",
	{Owner: "cms/content-reset", Effect: "verify_readiness", TargetType: "campaign", Version: "v1"}:        "content-reset-owner/v1",
	{Owner: "cms/content-reset", Effect: "verify_complete", TargetType: "campaign", Version: "v1"}:         "content-reset-owner/v1",
	{Owner: "cms/news", Effect: "build_candidate", TargetType: "campaign", Version: "v1"}:                  "content-reset-owner/v1",
	{Owner: "cms/pods", Effect: "build_candidate", TargetType: "campaign", Version: "v1"}:                  "content-reset-owner/v1",
	{Owner: "cms/news", Effect: "retire_exact", TargetType: "manifest_batch", Version: "v1"}:               "content-reset-owner/v1",
	{Owner: "cms/pods", Effect: "retire_exact", TargetType: "manifest_batch", Version: "v1"}:               "content-reset-owner/v1",
	{Owner: "cms/news", Effect: "verify_serving", TargetType: "campaign", Version: "v1"}:                   "content-reset-owner/v1",
	{Owner: "cms/pods", Effect: "verify_serving", TargetType: "campaign", Version: "v1"}:                   "content-reset-owner/v1",
	{Owner: "cms/feedstate", Effect: "publish", TargetType: "campaign", Version: "v1"}:                     "content-reset-owner/v1",
	{Owner: "cms/feedstate", Effect: "rollback", TargetType: "campaign", Version: "v1"}:                    "content-reset-owner/v1",
	{Owner: "cms/storage", Effect: "cleanup_exact", TargetType: "manifest_batch", Version: "v1"}:           "content-reset-owner/v1",
	{Owner: "cms/source-run", Effect: "prepare_replay_branch", TargetType: "source", Version: "v1"}:        "content-reset-owner/v1",
	{Owner: "cms/source-run", Effect: "replay_page", TargetType: "source_branch", Version: "v1"}:           "content-reset-owner/v1",
	{Owner: "cms/source-run", Effect: "release_provider_slot", TargetType: "source_branch", Version: "v1"}: "content-reset-owner/v1",
	{Owner: "cms/source-run", Effect: "replay_and_handoff", TargetType: "source_branch", Version: "v1"}:    "content-reset-owner/v1",
	{Owner: "cms/source-run", Effect: "acquire_intake_pause", TargetType: "campaign", Version: "v1"}:       "content-reset-owner/v1",
}

// ExpectedQualificationVersion returns the code-owned version a durable
// qualification row must carry to enable this exact contract.
func ExpectedQualificationVersion(contract Contract) (string, bool) {
	version, ok := qualificationVersions[contract]
	return version, ok
}

// LoadQualifications reads active, reviewed qualification rows and returns only
// those that match this build's expected contract version and carry a
// well-formed environment and evidence digest.
func LoadQualifications(db *gorm.DB) (map[Contract]string, error) {
	qualified := map[Contract]string{}
	if db == nil || !db.Migrator().HasTable(&models.ContentResetOwnerQualification{}) {
		return qualified, nil
	}
	var rows []models.ContentResetOwnerQualification
	if err := db.Where("revoked_at IS NULL").Find(&rows).Error; err != nil {
		return qualified, err
	}
	for _, row := range rows {
		contract := Contract{Owner: row.ContractOwner, Effect: row.ContractEffect, TargetType: row.ContractTargetType, Version: row.ContractVersion}
		expected, ok := ExpectedQualificationVersion(contract)
		if !ok || expected != row.QualificationVersion {
			continue
		}
		if !sha256Hex.MatchString(strings.ToLower(row.EnvironmentHash)) || !sha256Hex.MatchString(strings.ToLower(row.EvidenceHash)) {
			continue
		}
		qualified[contract] = row.QualificationVersion
	}
	return qualified, nil
}

type QualificationInput struct {
	Contract             Contract
	QualificationVersion string
	EnvironmentHash      string
	EvidenceHash         string
	QualifiedBy          string
	QualifiedAt          time.Time
}

// RecordQualification persists one reviewed qualification. It refuses a version
// the current build does not expect, so a qualification recorded for another
// implementation cannot be reused.
func RecordQualification(tx *gorm.DB, input QualificationInput) (models.ContentResetOwnerQualification, error) {
	var row models.ContentResetOwnerQualification
	expected, ok := ExpectedQualificationVersion(input.Contract)
	if !ok || input.QualificationVersion != expected {
		return row, errors.New("content reset qualification version does not match this build")
	}
	if !sha256Hex.MatchString(strings.ToLower(input.EnvironmentHash)) || !sha256Hex.MatchString(strings.ToLower(input.EvidenceHash)) || strings.TrimSpace(input.QualifiedBy) == "" {
		return row, errors.New("content reset qualification requires environment and evidence digests")
	}
	if input.QualifiedAt.IsZero() {
		input.QualifiedAt = time.Now().UTC()
	}
	row = models.ContentResetOwnerQualification{
		PublicID:             uuid.New(),
		ContractOwner:        input.Contract.Owner,
		ContractEffect:       input.Contract.Effect,
		ContractTargetType:   input.Contract.TargetType,
		ContractVersion:      input.Contract.Version,
		QualificationVersion: input.QualificationVersion,
		EnvironmentHash:      strings.ToLower(input.EnvironmentHash),
		EvidenceHash:         strings.ToLower(input.EvidenceHash),
		QualifiedBy:          strings.TrimSpace(input.QualifiedBy),
		QualifiedAt:          input.QualifiedAt,
	}
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if result.Error != nil {
		return row, result.Error
	}
	if result.RowsAffected == 1 {
		return row, nil
	}
	var existing models.ContentResetOwnerQualification
	if err := tx.Where("contract_owner=? AND contract_effect=? AND contract_target_type=? AND contract_version=? AND revoked_at IS NULL",
		input.Contract.Owner, input.Contract.Effect, input.Contract.TargetType, input.Contract.Version).First(&existing).Error; err != nil {
		return row, err
	}
	if existing.QualificationVersion != input.QualificationVersion || existing.EvidenceHash != strings.ToLower(input.EvidenceHash) || existing.EnvironmentHash != strings.ToLower(input.EnvironmentHash) {
		return row, errors.New("content reset contract already has a different active qualification")
	}
	return existing, nil
}
