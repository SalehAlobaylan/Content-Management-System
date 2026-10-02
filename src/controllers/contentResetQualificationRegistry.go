package controllers

import (
	"sync"

	"content-management-system/src/contentreset"
	"gorm.io/gorm"
)

// contentResetQualifiedContracts is the process-local projection of the durable
// qualification registry. It is refreshed from reviewed rows and never
// populated by an operator control.
var (
	contentResetQualificationMu    sync.RWMutex
	contentResetQualifiedContracts = map[contentreset.Contract]string{}
)

func refreshContentResetQualifications(db *gorm.DB) {
	if db == nil {
		return
	}
	loaded, err := contentreset.LoadQualifications(db)
	if err != nil {
		return
	}
	contentResetQualificationMu.Lock()
	contentResetQualifiedContracts = loaded
	contentResetQualificationMu.Unlock()
}

func contentResetQualificationValue(contract contentreset.Contract) string {
	contentResetQualificationMu.RLock()
	defer contentResetQualificationMu.RUnlock()
	return contentResetQualifiedContracts[contract]
}
