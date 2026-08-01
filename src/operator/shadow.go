package operator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// ShadowDomains is code-owned and mirrors the canonical Console domain
// inventory. It is read only; a shadow run never constructs a plan or calls a
// reasoner/executor.
var ShadowDomains = []string{
	"global_ops", "system_health", "feed_integrity", "feed_recovery", "retention", "real_experience", "ai_economics", "sources", "content", "news", "news_finding", "news_circulation", "media_sources", "atomization", "media_circulation", "redundancy", "media_library", "storage_quality", "pipeline", "enrichment", "intelligence", "embeddings", "topics_preferences", "moderation", "auth_center", "operator",
}

func ShadowVisibleContext(domain string) VisibleContext {
	filters := map[string]any{}
	if domain == "feed_integrity" {
		filters["window"] = "today"
	}
	visible := VisibleContext{SchemaVersion: ContractVersion, Domain: domain, View: "shadow", Filters: filters, Subjects: []SubjectRef{{Type: "tenant", ID: "current"}}, AvailableIntents: []Intent{IntentExplain}}
	if domain == "feed_integrity" {
		visible.Subjects = []SubjectRef{{Type: "news_window", ID: "today"}}
		visible.Selection = &ExplicitSelection{Mode: "explicit", IDs: []string{"today"}, Count: 1}
	}
	return visible
}

// RunShadowSnapshot collects one domain packet under a freshly observed IAM
// snapshot for an explicitly enrolled qualification administrator. The run is
// strictly read-only and stores only a hash of access-version metadata.
func RunShadowSnapshot(ctx context.Context, db *gorm.DB, access AccessSnapshot, domain, locale string) models.OperatorShadowRun {
	started := time.Now().UTC()
	versionHash := fmt.Sprintf("%x", sha256.Sum256([]byte(access.AccessVersion)))
	run := models.OperatorShadowRun{TenantID: access.TenantID, ActorID: access.UserID, AccessVersionHash: versionHash, Domain: domain, Locale: locale, State: "failed", StartedAt: started}
	if err := validateShadowAccess(access); err != nil {
		run.ErrorClass = "access_unavailable"
		finished := time.Now().UTC()
		run.FinishedAt = &finished
		run.LatencyMS = finished.Sub(started).Milliseconds()
		_ = db.WithContext(ctx).Create(&run).Error
		return run
	}
	packet, err := NewContextFabric(db, DefaultAdapterRegistry()).BuildPacket(ctx, ShadowVisibleContext(domain), access)
	finished := time.Now().UTC()
	run.FinishedAt = &finished
	run.LatencyMS = finished.Sub(started).Milliseconds()
	if err != nil {
		run.ErrorClass = "context_unavailable"
		_ = db.WithContext(ctx).Create(&run).Error
		return run
	}
	run.State, run.PacketFingerprint, run.EvidenceCount, run.UnknownCount, run.ConflictCount = "completed", packet.Fingerprint, len(packet.Evidence), len(packet.Unknowns), len(packet.Conflicts)
	_ = db.WithContext(ctx).Create(&run).Error
	return run
}

// validateShadowAccess rejects the legacy fallback tenant before any shadow
// evidence read. Qualification is meaningful only for an explicitly enrolled
// tenant and a fresh administrator snapshot.
func validateShadowAccess(access AccessSnapshot) error {
	if strings.TrimSpace(access.TenantID) == "" || strings.EqualFold(strings.TrimSpace(access.TenantID), "default") {
		return fmt.Errorf("%w: explicit non-default shadow tenant is required", ErrInvalidContract)
	}
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil || !access.IsAdmin {
		return fmt.Errorf("%w: shadow access is unavailable", ErrInvalidContract)
	}
	return nil
}
