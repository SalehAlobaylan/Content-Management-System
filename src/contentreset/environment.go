package contentreset

import (
	"encoding/json"
	"errors"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"gorm.io/gorm"
)

// ValidateExecutionEnvironment is shared by coordinator owners and native
// replay jobs. Queue delay or a database restore cannot renew an old approval.
func ValidateExecutionEnvironment(db *gorm.DB, run models.ContentResetExecution) error {
	var approved struct {
		Environment Environment `json:"environment"`
	}
	if run.ContractHash != Hash(run.Contract) || json.Unmarshal(run.Contract, &approved) != nil {
		return errors.New("Content Reset approval contract integrity failure")
	}
	current, err := ReadEnvironment(db)
	if err != nil {
		return err
	}
	if current != approved.Environment {
		return errors.New("Content Reset approved environment changed")
	}
	return nil
}

// Environment pins a run to one database lineage/epoch and canonical schema.
// Restoring a database copy does not renew the old run's destructive authority.
type Environment struct {
	DatabaseID    string `json:"database_id"`
	LineageRootID string `json:"lineage_root_id"`
	Epoch         int64  `json:"epoch"`
	SchemaDigest  string `json:"schema_digest"`
}

func ReadEnvironment(db *gorm.DB) (Environment, error) {
	contract, err := utils.ReadDatabaseContract(db, "migrations")
	if err != nil {
		return Environment{}, err
	}
	if contract.IdentityState != "present" || contract.LedgerState != "verified" || contract.DatabaseID == "" || contract.LineageRootID == "" || contract.Epoch < 1 || len(contract.SchemaDigest) != 64 {
		return Environment{}, errors.New("Content Reset requires a verified canonical database contract")
	}
	fence, err := utils.ReadWriterFence(db)
	if err != nil {
		return Environment{}, err
	}
	if (fence.State != "open" && fence.State != "successor_open") || fence.Epoch != contract.Epoch {
		return Environment{}, errors.New("Content Reset database writer authority is fenced")
	}
	return Environment{contract.DatabaseID, contract.LineageRootID, contract.Epoch, contract.SchemaDigest}, nil
}
