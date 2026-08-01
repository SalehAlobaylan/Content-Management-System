package operator

import (
	"context"
	"fmt"
	"strings"

	"content-management-system/src/models"
)

type InvestigationInput struct {
	VisibleContext VisibleContext
	Intent         Intent
	Locale         string
	Message        string
	Tier           string
	ThreadID       *uint
}

type InvestigationResult struct {
	Investigation models.OperatorInvestigation
	Packet        DecisionPacket
	Response      ReasonResponse
	Degraded      bool
}

// InvestigationCoordinator is the CMS-only sequence for an interactive
// question. Context is collected before Enrichment can see it; Enrichment
// receives only the validated packet and never gets plan/queue/database power.
type InvestigationCoordinator struct {
	fabric   *ContextFabric
	store    *InvestigationStore
	reasoner Reasoner
}

func NewInvestigationCoordinator(fabric *ContextFabric, store *InvestigationStore, reasoner Reasoner) *InvestigationCoordinator {
	return &InvestigationCoordinator{fabric: fabric, store: store, reasoner: reasoner}
}

func (coordinator *InvestigationCoordinator) Start(ctx context.Context, access AccessSnapshot, policy RuntimePolicy, input InvestigationInput) (models.OperatorInvestigation, error) {
	if coordinator == nil || coordinator.fabric == nil || coordinator.store == nil {
		return models.OperatorInvestigation{}, fmt.Errorf("%w: investigation coordinator is unavailable", ErrInvalidContract)
	}
	if !policy.ReadEnabled {
		return models.OperatorInvestigation{}, fmt.Errorf("%w: operator read capability is disabled", ErrForbiddenTool)
	}
	if err := access.ValidateFor(access.UserID, access.TenantID); err != nil {
		return models.OperatorInvestigation{}, err
	}
	if strings.TrimSpace(input.Message) == "" || len(input.Message) > 8000 || (input.Locale != "ar" && input.Locale != "en") || (input.Tier != "fast" && input.Tier != "reasoning") || !visibleIntentAllowed(input.VisibleContext, input.Intent) {
		return models.OperatorInvestigation{}, fmt.Errorf("%w: invalid investigation input", ErrInvalidContract)
	}
	request, err := NewInvestigationRequest(input.Intent, input.Message, input.Tier)
	if err != nil {
		return models.OperatorInvestigation{}, err
	}
	return coordinator.store.Create(ctx, access.TenantID, access.UserID, input.Locale, input.VisibleContext, request, input.ThreadID)
}

// Run keeps the foreground contract for deterministic callers and tests. HTTP
// entrypoints use Start + QueueBackground + Process so a browser disconnect
// never cancels the durable investigation.
func (coordinator *InvestigationCoordinator) Run(ctx context.Context, access AccessSnapshot, policy RuntimePolicy, input InvestigationInput) (InvestigationResult, error) {
	investigation, err := coordinator.Start(ctx, access, policy, input)
	if err != nil {
		return InvestigationResult{}, err
	}
	return coordinator.Process(ctx, investigation, access, policy, input)
}

// Process runs a previously persisted investigation. Callers must provide a
// freshly obtained IAM snapshot; stale browser authorization is never reused.
func (coordinator *InvestigationCoordinator) Process(ctx context.Context, investigation models.OperatorInvestigation, access AccessSnapshot, policy RuntimePolicy, input InvestigationInput) (InvestigationResult, error) {
	if coordinator == nil || coordinator.fabric == nil || coordinator.store == nil {
		return InvestigationResult{Investigation: investigation}, fmt.Errorf("%w: investigation coordinator is unavailable", ErrInvalidContract)
	}
	if !policy.ReadEnabled || access.ValidateFor(investigation.ActorID, investigation.TenantID) != nil || access.UserID != investigation.ActorID || access.TenantID != investigation.TenantID {
		_ = coordinator.store.Fail(ctx, investigation.ID, investigation.TenantID, "access_unavailable")
		return InvestigationResult{Investigation: investigation}, ErrAccessUnavailable
	}
	claimToken, err := coordinator.store.Begin(ctx, investigation.ID, investigation.TenantID)
	if err != nil {
		return InvestigationResult{Investigation: investigation}, err
	}
	packet, err := coordinator.fabric.BuildPacket(ctx, input.VisibleContext, access)
	if err != nil {
		_ = coordinator.store.FailClaim(ctx, investigation.ID, access.TenantID, claimToken, "context_unavailable")
		return InvestigationResult{Investigation: investigation}, err
	}
	if err := coordinator.fabric.AttachHistoricalRetrieval(ctx, &packet, access, input.Message); err != nil {
		// Retrieval is optional explanatory context; live CMS evidence stays useful.
		packet.Warnings = append(packet.Warnings, "Historical retrieval is unavailable; live CMS evidence remains authoritative.")
		packet.Fingerprint = fingerprintPacket(packet)
	}
	localizePacketRecommendations(&packet, input.Locale)
	if err := coordinator.store.PersistPacket(ctx, investigation.ID, claimToken, packet); err != nil {
		_ = coordinator.store.FailClaim(ctx, investigation.ID, access.TenantID, claimToken, "persistence_failed")
		return InvestigationResult{Investigation: investigation, Packet: packet}, err
	}
	if !policy.LLMEnabled || coordinator.reasoner == nil {
		blocks := DeterministicDegradedBlocksForLocale(packet, input.Locale, localizedDegradedReason(input.Locale, false))
		if err := coordinator.store.Complete(ctx, investigation.ID, access.TenantID, claimToken, blocks); err != nil {
			return InvestigationResult{Investigation: investigation, Packet: packet, Degraded: true}, err
		}
		return InvestigationResult{Investigation: investigation, Packet: packet, Response: ReasonResponse{SchemaVersion: ContractVersion, Language: input.Locale, TaskKind: input.Intent, Blocks: blocks, Provider: "cms", Model: "deterministic", Tier: input.Tier, Cache: "disabled"}, Degraded: true}, nil
	}
	recommendationIDs := make([]string, 0, len(packet.Recommendations))
	for _, recommendation := range packet.Recommendations {
		recommendationIDs = append(recommendationIDs, recommendation.ID)
	}
	output, reasonErr := coordinator.reasoner.Reason(ctx, ReasonRequest{SchemaVersion: ContractVersion, InvestigationID: investigation.PublicID.String(), ThreadID: investigation.PublicID.String(), TaskKind: input.Intent, Language: input.Locale, Tier: input.Tier, AdminMessage: input.Message, Packet: packet, AdvertisedRecommendationIDs: recommendationIDs, AdvertisedActions: []AdvertisedTool{}, DataCategories: []string{"operational_state"}})
	if reasonErr != nil {
		blocks := DeterministicDegradedBlocksForLocale(packet, input.Locale, localizedDegradedReason(input.Locale, true))
		if err := coordinator.store.Complete(ctx, investigation.ID, access.TenantID, claimToken, blocks); err != nil {
			return InvestigationResult{Investigation: investigation, Packet: packet, Degraded: true}, err
		}
		return InvestigationResult{Investigation: investigation, Packet: packet, Response: ReasonResponse{SchemaVersion: ContractVersion, Language: input.Locale, TaskKind: input.Intent, Blocks: blocks, Provider: "cms", Model: "deterministic", Tier: input.Tier, Cache: "disabled"}, Degraded: true}, nil
	}
	if err := coordinator.store.RankRecommendations(ctx, investigation.ID, access.TenantID, output.PrimaryRecommendationID, output.SecondaryRecommendationIDs); err != nil {
		return InvestigationResult{Investigation: investigation, Packet: packet, Response: output}, err
	}
	if err := coordinator.store.Complete(ctx, investigation.ID, access.TenantID, claimToken, output.Blocks); err != nil {
		return InvestigationResult{Investigation: investigation, Packet: packet, Response: output}, err
	}
	return InvestigationResult{Investigation: investigation, Packet: packet, Response: output}, nil
}

func localizedDegradedReason(locale string, unavailable bool) string {
	if locale == "ar" {
		if unavailable {
			return "استدلال الذكاء الاصطناعي غير متاح حالياً؛ تظل حزمة أدلة CMS متاحة."
		}
		return "تم تعطيل استدلال الذكاء الاصطناعي حالياً؛ تظل حزمة أدلة CMS متاحة."
	}
	if unavailable {
		return "LLM reasoning is unavailable; the CMS evidence packet remains available."
	}
	return "LLM reasoning is currently disabled; the CMS evidence packet remains available."
}

func localizePacketRecommendations(packet *DecisionPacket, locale string) {
	if locale != "ar" {
		return
	}
	for index := range packet.Recommendations {
		recommendation := &packet.Recommendations[index]
		if recommendation.Kind != "inspect" {
			continue
		}
		recommendation.Summary = localizedRecommendationSummary(*recommendation)
		recommendation.Title = "راجع السطح التشغيلي المسجّل"
		switch strings.TrimPrefix(recommendation.ID, "inspect:") {
		case "media_sources":
			recommendation.Title = "راجع دليل تسليم المصدر"
		case "media_circulation":
			recommendation.Title = "راجع توصيات التوزيع"
		case "feed_integrity":
			recommendation.Title = "راجع لقطة الأخبار"
		case "feed_recovery":
			recommendation.Title = "راجع عناصر تحكم استعادة الخلاصة"
		case "retention":
			recommendation.Title = "راجع حفظ بيانات الاحتفاظ"
		}
	}
	packet.Fingerprint = fingerprintPacket(*packet)
}

func visibleIntentAllowed(visible VisibleContext, intent Intent) bool {
	for _, allowed := range visible.AvailableIntents {
		if allowed == intent {
			return true
		}
	}
	return false
}
