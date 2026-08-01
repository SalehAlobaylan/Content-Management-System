package controllers

import (
	"context"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"

	"gorm.io/gorm"
)

const operatorShadowHeartbeatInterval = 15 * time.Minute

// StartOperatorShadowHeartbeat runs an explicitly enrolled, read-only
// qualification workflow. It has no HTTP route, no LLM call, and no
// action-plan access; normal Operator availability never changes that boundary.
func StartOperatorShadowHeartbeat(db *gorm.DB) {
	go func() {
		runOperatorShadowHeartbeat(db)
		ticker := time.NewTicker(operatorShadowHeartbeatInterval)
		defer ticker.Stop()
		for range ticker.C {
			runOperatorShadowHeartbeat(db)
		}
	}()
}

func runOperatorShadowHeartbeat(db *gorm.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	accessClient, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		return
	}
	var enrollments []models.OperatorShadowEnrollment
	if err := db.WithContext(ctx).Where("state=?", "active").Find(&enrollments).Error; err != nil {
		return
	}
	if len(enrollments) == 0 {
		recordOperatorShadowQualificationFailure(db, "", "", "", "", "zero_enrollment")
		return
	}
	for _, enrollment := range enrollments {
		for _, domain := range operatorpkg.ShadowDomains {
			for _, locale := range []string{"en", "ar"} {
				// An access snapshot is deliberately fetched per evaluation. A
				// suspended or de-permissioned administrator must fence the next
				// domain/locale rather than inheriting authority from the prior one.
				access, err := accessClient.Snapshot(ctx, enrollment.UserID, enrollment.TenantID)
				if err != nil || !access.IsAdmin {
					recordOperatorShadowQualificationFailure(db, enrollment.TenantID, enrollment.UserID, domain, locale, "iam_unavailable")
					continue
				}
				operatorpkg.RunShadowSnapshot(ctx, db, access, domain, locale)
			}
		}
	}
}

func recordOperatorShadowQualificationFailure(db *gorm.DB, tenantID, actorID, domain, locale, failure string) {
	entry := models.OperatorShadowQualificationFailure{TenantID: tenantID, ActorID: actorID, Domain: domain, Locale: locale, Failure: failure, ObservedAt: time.Now().UTC()}
	_ = db.Create(&entry).Error
}
