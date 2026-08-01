package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type ReasonRequest struct {
	SchemaVersion               string           `json:"schema_version"`
	InvestigationID             string           `json:"investigation_id"`
	ThreadID                    string           `json:"thread_id"`
	TaskKind                    Intent           `json:"task_kind"`
	Language                    string           `json:"language"`
	Tier                        string           `json:"tier"`
	AdminMessage                string           `json:"admin_message"`
	ConversationObjective       string           `json:"conversation_objective"`
	Packet                      DecisionPacket   `json:"packet"`
	AdvertisedRecommendationIDs []string         `json:"advertised_recommendation_ids"`
	AdvertisedActions           []AdvertisedTool `json:"advertised_actions"`
	DataCategories              []string         `json:"data_categories"`
	CredentialRedactionCount    int              `json:"credential_redaction_count"`
}

type AdvertisedTool struct {
	Key       string   `json:"key"`
	TargetIDs []string `json:"target_ids"`
}

type ActionIntent struct {
	Key       string   `json:"action_key"`
	TargetIDs []string `json:"target_ids"`
}

type ReasonResponse struct {
	SchemaVersion              string          `json:"schema_version"`
	Language                   string          `json:"language"`
	TaskKind                   Intent          `json:"task_kind"`
	Blocks                     []ResponseBlock `json:"blocks"`
	PrimaryRecommendationID    *string         `json:"primary_recommendation_id"`
	SecondaryRecommendationIDs []string        `json:"secondary_recommendation_ids"`
	ActionIntent               *ActionIntent   `json:"action_intent"`
	Uncertainties              []string        `json:"uncertainties"`
	Provider                   string          `json:"provider"`
	Model                      string          `json:"model"`
	Tier                       string          `json:"tier"`
	FallbackUsed               bool            `json:"fallback_used"`
	Cache                      string          `json:"cache"`
}

type Reasoner interface {
	Reason(context.Context, ReasonRequest) (ReasonResponse, error)
}

type HTTPReasoner struct {
	endpoint string
	token    string
	client   *http.Client
}

func NewHTTPReasoner(baseURL, token string, client *http.Client) (*HTTPReasoner, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		return nil, fmt.Errorf("%w: enrichment URL is invalid", ErrInvalidContract)
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: enrichment service capability is unavailable", ErrInvalidContract)
	}
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	return &HTTPReasoner{endpoint: baseURL + "/v1/operator/reason", token: token, client: client}, nil
}

func NewHTTPReasonerFromEnv() (*HTTPReasoner, error) {
	token := strings.TrimSpace(os.Getenv("ENRICHMENT_SERVICE_TOKEN"))
	if token == "" {
		token = strings.TrimSpace(os.Getenv("SERVICE_AUTH_TOKEN"))
	}
	if token == "" {
		token = strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
	}
	return NewHTTPReasoner(os.Getenv("ENRICHMENT_BASE_URL"), token, nil)
}

func (reasoner *HTTPReasoner) Reason(ctx context.Context, input ReasonRequest) (ReasonResponse, error) {
	if err := input.Packet.Validate(); err != nil || input.SchemaVersion != ContractVersion || (input.Language != "ar" && input.Language != "en") || (input.Tier != "fast" && input.Tier != "reasoning") {
		return ReasonResponse{}, fmt.Errorf("%w: invalid reasoning request", ErrInvalidContract)
	}
	// Python's strict contract distinguishes an omitted list from JSON null.
	// Normalise optional advertised sets before crossing the service boundary so
	// an empty catalog remains a valid, explicitly empty capability set.
	if input.AdvertisedRecommendationIDs == nil {
		input.AdvertisedRecommendationIDs = []string{}
	}
	if input.AdvertisedActions == nil {
		input.AdvertisedActions = []AdvertisedTool{}
	}
	if input.DataCategories == nil {
		input.DataCategories = []string{}
	}
	body, err := json.Marshal(input)
	if err != nil {
		return ReasonResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, reasoner.endpoint, bytes.NewReader(body))
	if err != nil {
		return ReasonResponse{}, err
	}
	request.Header.Set("Authorization", "Bearer "+reasoner.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := reasoner.client.Do(request)
	if err != nil {
		return ReasonResponse{}, fmt.Errorf("reasoning service unavailable: %w", err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 256<<10)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		return ReasonResponse{}, err
	}
	if response.StatusCode != http.StatusOK {
		return ReasonResponse{}, fmt.Errorf("reasoning service returned status %d", response.StatusCode)
	}
	var output ReasonResponse
	if err := json.Unmarshal(responseBody, &output); err != nil {
		return ReasonResponse{}, fmt.Errorf("reasoning service returned invalid JSON")
	}
	if err := output.ValidateAgainst(input); err != nil {
		return ReasonResponse{}, err
	}
	return output, nil
}

func (output ReasonResponse) ValidateAgainst(input ReasonRequest) error {
	if output.SchemaVersion != ContractVersion || output.Language != input.Language || output.TaskKind != input.TaskKind || output.Tier != input.Tier || output.Cache != "disabled" || strings.TrimSpace(output.Provider) == "" || strings.TrimSpace(output.Model) == "" {
		return fmt.Errorf("%w: invalid reasoning response provenance", ErrInvalidContract)
	}
	evidence := make(map[string]struct{}, len(input.Packet.Evidence))
	for _, ref := range input.Packet.Evidence {
		evidence[ref.EvidenceID] = struct{}{}
	}
	for _, block := range output.Blocks {
		if err := block.Validate(); err != nil {
			return err
		}
		for _, id := range block.EvidenceIDs {
			if _, ok := evidence[id]; !ok {
				return fmt.Errorf("%w: reasoning response cites unknown evidence", ErrInvalidContract)
			}
		}
	}
	if len(output.Blocks) == 0 {
		return fmt.Errorf("%w: reasoning response is empty", ErrInvalidContract)
	}
	allowedRecommendations := make(map[string]struct{}, len(input.AdvertisedRecommendationIDs))
	for _, recommendationID := range input.AdvertisedRecommendationIDs {
		allowedRecommendations[recommendationID] = struct{}{}
	}
	if output.PrimaryRecommendationID != nil {
		if _, ok := allowedRecommendations[*output.PrimaryRecommendationID]; !ok {
			return fmt.Errorf("%w: response recommendation is not advertised", ErrInvalidContract)
		}
	}
	if len(output.SecondaryRecommendationIDs) > 3 {
		return fmt.Errorf("%w: too many secondary recommendations", ErrInvalidContract)
	}
	for _, recommendationID := range output.SecondaryRecommendationIDs {
		if _, ok := allowedRecommendations[recommendationID]; !ok {
			return fmt.Errorf("%w: response recommendation is not advertised", ErrInvalidContract)
		}
	}
	allowedActions := map[string]map[string]struct{}{}
	for _, action := range input.AdvertisedActions {
		ids := map[string]struct{}{}
		for _, id := range action.TargetIDs {
			ids[id] = struct{}{}
		}
		allowedActions[action.Key] = ids
	}
	if output.ActionIntent != nil {
		ids, ok := allowedActions[output.ActionIntent.Key]
		if !ok {
			return fmt.Errorf("%w: response action is not advertised", ErrInvalidContract)
		}
		for _, id := range output.ActionIntent.TargetIDs {
			if _, ok := ids[id]; !ok {
				return fmt.Errorf("%w: response target is not advertised", ErrInvalidContract)
			}
		}
	}
	return nil
}

// DeterministicDegradedBlocks make a read-only investigation useful when LLM
// use is disabled or unavailable, without creating action authority from prose.
func DeterministicDegradedBlocks(packet DecisionPacket, reason string) []ResponseBlock {
	return DeterministicDegradedBlocksForLocale(packet, "en", reason)
}

// DeterministicDegradedBlocksForLocale keeps CMS-owned fallback evidence useful
// in the thread locale when Enrichment is disabled or cannot return a valid
// response. It never introduces action authority or translates identifiers.
func DeterministicDegradedBlocksForLocale(packet DecisionPacket, locale, reason string) []ResponseBlock {
	blocks := []ResponseBlock{}
	factText := "CMS collected the current registered operational context."
	if locale == "ar" {
		factText = "جمع CMS السياق التشغيلي الحالي من السجلات المسجّلة."
	}
	if len(packet.Evidence) > 0 {
		blocks = append(blocks, ResponseBlock{Kind: "fact", Text: factText, EvidenceIDs: []string{packet.Evidence[0].EvidenceID}})
	}
	for _, unknown := range packet.Unknowns {
		if locale == "ar" {
			blocks = append(blocks, ResponseBlock{Kind: "unknown", Text: "توجد حالة تشغيلية مطلوبة لا تثبتها سجلات CMS الحالية."})
			continue
		}
		blocks = append(blocks, ResponseBlock{Kind: "unknown", Text: unknown})
	}
	for _, recommendation := range packet.Recommendations {
		text := recommendation.Summary
		if locale == "ar" {
			text = localizedRecommendationSummary(recommendation)
		}
		blocks = append(blocks, ResponseBlock{Kind: "recommendation", Text: text, EvidenceIDs: recommendation.EvidenceIDs})
	}
	blocks = append(blocks, ResponseBlock{Kind: "degraded", Text: reason})
	return blocks
}

func localizedRecommendationSummary(recommendation Recommendation) string {
	if recommendation.Kind != "inspect" {
		return "راجع الدليل التشغيلي الحالي في CMS قبل اتخاذ أي تغيير."
	}
	switch strings.TrimPrefix(recommendation.ID, "inspect:") {
	case "media_sources":
		return "راجع المصدر وطلب التشغيل الدائم ومسار المعالجة في مصادر الوسائط."
	case "media_circulation":
		return "راجع السياسة الحالية وآخر تشغيل مسجل وقائمة توصيات التوزيع المحدودة."
	case "feed_integrity":
		return "راجع حالة لقطة الأخبار المباشرة، وجهّز إجراء التحديث المسجل عند ملاءمته."
	case "feed_recovery":
		return "راجع أحدث خطة وتشغيل لاستعادة الخلاصة. تبقى عمليات الاستعادة المدمرة يدوية فقط."
	case "retention":
		return "راجع أدلة السعة والسياسة للاحتفاظ. تبقى عمليات الحفظ المدمرة يدوية فقط."
	default:
		return "راجع الدليل التشغيلي الحالي في CMS قبل اتخاذ أي تغيير."
	}
}
