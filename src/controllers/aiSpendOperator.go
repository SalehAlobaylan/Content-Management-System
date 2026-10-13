package controllers

import (
	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"context"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type spendGuardedReasoner struct {
	db   *gorm.DB
	next operatorpkg.Reasoner
}

func (r *spendGuardedReasoner) Reason(ctx context.Context, input operatorpkg.ReasonRequest) (operatorpkg.ReasonResponse, error) {
	provenance := spendContext(input.Packet.TenantID, "operator", input.InvestigationID)
	provenance.OverrideID = input.SpendOverrideID
	if err := admitAISpend(r.db.WithContext(ctx), provenance, .02); err != nil {
		if e := operatorpkg.NewInvestigationStore(r.db).RecordSpendSkip(ctx, input.InvestigationID, provenance.TenantID, err.Error()); e != nil {
			return operatorpkg.ReasonResponse{}, e
		}
		return operatorpkg.ReasonResponse{}, err
	}
	return r.next.Reason(ctx, input)
}
func wrapSpendReasoner(db *gorm.DB, next operatorpkg.Reasoner) operatorpkg.Reasoner {
	if next == nil {
		return nil
	}
	return &spendGuardedReasoner{db, next}
}
func confirmedOperatorSpend(db *gorm.DB, tenant, user string) (string, error) {
	g := models.AISpendOverride{ID: uuid.New(), TenantID: tenant, TriggerSource: "operator", CreatedBy: user, ExpiresAt: time.Now().UTC().Add(10 * time.Minute), AdmissionSnapshot: aiSpendJSON(aiSpendAdmissionSnapshot(db, tenant, "operator"))}
	if err := db.Create(&g).Error; err != nil {
		return "", err
	}
	return g.ID.String(), nil
}
