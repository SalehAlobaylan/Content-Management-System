package controllers

import (
	"content-management-system/src/contentreset"
	"encoding/json"
	"testing"
)

func TestContentResetEffectAdmissionRequiresCurrentApprovedQualification(t *testing.T) {
	owner := (contentResetReplayPageOwner{}).Contract()
	previous := contentResetQualifiedContracts
	contentResetQualifiedContracts = map[contentreset.Contract]string{owner: "qualified-release-proof"}
	t.Cleanup(func() { contentResetQualifiedContracts = previous })
	for name, contract := range map[string]contentResetExecutionContract{
		"approved":      {Owners: []contentreset.Contract{owner}, Qualifications: []string{"qualified-release-proof"}},
		"stale release": {Owners: []contentreset.Contract{owner}, Qualifications: []string{"old-proof"}},
		"absent proof":  {Owners: []contentreset.Contract{owner}},
		"another owner": {Owners: []contentreset.Contract{(contentResetPrepareOwner{}).Contract()}, Qualifications: []string{"qualified-release-proof"}},
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(contract)
			if contentResetEffectAdmission(owner, raw) != (name == "approved") {
				t.Fatal("qualification admission did not match the approved release")
			}
		})
	}
	if contentResetEffectAdmission(owner, []byte(`{`)) {
		t.Fatal("invalid approval admitted an effect")
	}
	contentResetQualifiedContracts = map[contentreset.Contract]string{}
	raw, _ := json.Marshal(contentResetExecutionContract{Owners: []contentreset.Contract{owner}, Qualifications: []string{"qualified-release-proof"}})
	if contentResetEffectAdmission(owner, raw) {
		t.Fatal("revoked qualification admitted new work")
	}
}
