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

	"content-management-system/src/models"
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
	blocks = append(blocks, mediaSupplyContinuityDegradedBlocks(packet, locale)...)
	blocks = append(blocks, mediaContinuityDegradedBlocks(packet, locale)...)
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

// mediaSupplyContinuityDegradedBlocks presents the deterministic Supply
// Continuity headline before the lower-level source sample. It only names
// fields carried in CMS facts and does not imply that a retry, repair, or any
// external worker action has occurred.
func mediaSupplyContinuityDegradedBlocks(packet DecisionPacket, locale string) []ResponseBlock {
	var headline, evaluator, exposure, attention *Fact
	for index := range packet.Facts {
		switch packet.Facts[index].Key {
		case "media_circulation.supply_continuity":
			headline = &packet.Facts[index]
		case "media_circulation.supply_evaluator":
			evaluator = &packet.Facts[index]
		case "media_circulation.pods_exposure":
			exposure = &packet.Facts[index]
		case "media_circulation.supply_attention":
			attention = &packet.Facts[index]
		}
	}
	if headline == nil || len(headline.EvidenceIDs) == 0 {
		return nil
	}
	value, ok := headline.Value.(map[string]any)
	if !ok {
		return nil
	}
	verdict, _ := value["verdict"].(string)
	boundary, _ := value["headline_boundary"].(string)
	owner, _ := value["owner"].(string)
	completeness, _ := value["evidence_completeness"].(string)
	evidenceIDs := append([]string{}, headline.EvidenceIDs...)
	workerState := "unknown"
	if evaluator != nil {
		if evaluatorValue, ok := evaluator.Value.(map[string]any); ok {
			if state, ok := evaluatorValue["worker_state"].(string); ok && strings.TrimSpace(state) != "" {
				workerState = state
			}
		}
		evidenceIDs = append(evidenceIDs, evaluator.EvidenceIDs...)
	}
	blocks := []ResponseBlock{{Kind: "fact", Text: localizedSupplyHeadline(locale, verdict, boundary, owner, completeness, workerState), EvidenceIDs: evidenceIDs}}
	if exposure != nil && len(exposure.EvidenceIDs) > 0 {
		if exposureValue, ok := exposure.Value.(map[string]any); ok {
			verdict, _ := exposureValue["verdict"].(string)
			eligible, _ := numericCount(exposureValue["base_eligible_count"])
			reachable, _ := numericCount(exposureValue["reachable_count"])
			returned, _ := numericCount(exposureValue["returned_count"])
			text := fmt.Sprintf("Pods exposure proof is %s: %d base-eligible, %d generation-reachable, and %d returned by the isolated non-perturbing probe.", verdict, eligible, reachable, returned)
			if locale == "ar" {
				text = fmt.Sprintf("دليل وصول Pods هو %s: عدد المؤهلة أساسياً %d، والقابلة للوصول في الجيل %d، والمعادة في الفحص المعزول غير المؤثر %d.", verdict, eligible, reachable, returned)
			}
			blocks = append(blocks, ResponseBlock{Kind: "fact", Text: text, EvidenceIDs: exposure.EvidenceIDs})
		}
	}
	if attention == nil || len(attention.EvidenceIDs) == 0 {
		return blocks
	}
	attentionValue, ok := attention.Value.(map[string]any)
	if !ok {
		return blocks
	}
	count, _ := attentionValue["sampled_count"].(int)
	if count <= 0 {
		return blocks
	}
	open, recovering, resolved := supplyEpisodeStateCounts(attentionValue["episodes"])
	blocks = append(blocks, ResponseBlock{Kind: "fact", Text: localizedSupplyAttention(locale, count, open, recovering, resolved), EvidenceIDs: attention.EvidenceIDs})
	return blocks
}

func supplyEpisodeStateCounts(value any) (open, recovering, resolved int) {
	for _, episode := range continuityRows(value) {
		state, _ := episode["state"].(string)
		switch state {
		case models.MediaSupplyEpisodeOpen:
			open++
		case models.MediaSupplyEpisodeRecovering:
			recovering++
		case models.MediaSupplyEpisodeResolved:
			resolved++
		}
	}
	return open, recovering, resolved
}

func numericCount(value any) (int, bool) {
	switch current := value.(type) {
	case int:
		return current, true
	case int64:
		return int(current), true
	case float64:
		return int(current), true
	default:
		return 0, false
	}
}

func localizedSupplyHeadline(locale, verdict, boundary, owner, completeness, workerState string) string {
	if locale == "ar" {
		return fmt.Sprintf("تقييم استمرارية الإمداد من CMS: النتيجة %s عند حد %s، والمالك %s. اكتمال الدليل %s؛ حالة عامل التقييم %s. هذه لقطة قراءة فقط ولا تثبت إعادة محاولة أو إصلاحاً.", verdict, boundary, owner, completeness, workerState)
	}
	return fmt.Sprintf("CMS Supply Continuity reports %s at the %s boundary (owner: %s). Evidence is %s; evaluator worker state is %s. This is read-only evidence and does not prove a retry or repair.", verdict, boundary, owner, completeness, workerState)
}

func localizedSupplyAttention(locale string, total, open, recovering, resolved int) string {
	if locale == "ar" {
		return fmt.Sprintf("سجّل CMS %d من سجلات تنبيه استمرارية الإمداد: %d مفتوحة، %d قيد التحقق من الحل، و%d محلولة في العينة.", total, open, recovering, resolved)
	}
	return fmt.Sprintf("CMS sampled %d Supply Continuity attention records: %d open, %d verifying resolution, and %d resolved.", total, open, recovering, resolved)
}

// mediaContinuityDegradedBlocks turns the registered Media Circulation source
// continuity fact into an evidence-cited diagnosis when the optional LLM is
// unavailable. It only summarizes CMS states already in the packet; it never
// asserts a provider failure, queue acceptance, or a recovery action.
func mediaContinuityDegradedBlocks(packet DecisionPacket, locale string) []ResponseBlock {
	for _, fact := range packet.Facts {
		if fact.Key != "media_circulation.source_continuity" || len(fact.EvidenceIDs) == 0 {
			continue
		}
		value, ok := fact.Value.(map[string]any)
		if !ok {
			return nil
		}
		rows := continuityRows(value["sources"])
		if rows == nil {
			return nil
		}
		schedule := map[string]int{}
		delivery := map[string]int{}
		dueNames := make([]string, 0, 3)
		for _, row := range rows {
			state, _ := row["schedule_state"].(string)
			schedule[state]++
			if state == "due_unadmitted" {
				if name, ok := row["source_name"].(string); ok && strings.TrimSpace(name) != "" && len(dueNames) < 3 {
					dueNames = append(dueNames, name)
				}
			}
			if state, ok := row["delivery_state"].(string); ok {
				delivery[state]++
			}
		}
		evidenceIDs := fact.EvidenceIDs
		blocks := []ResponseBlock{{Kind: "fact", Text: localizedContinuitySnapshot(locale, len(rows), schedule, delivery), EvidenceIDs: evidenceIDs}}
		if schedule["due_unadmitted"] > 0 {
			blocks = append(blocks, ResponseBlock{Kind: "interpretation", Text: localizedDueUnadmittedDiagnosis(locale, dueNames), EvidenceIDs: evidenceIDs})
		}
		return blocks
	}
	return nil
}

func continuityRows(value any) []map[string]any {
	switch rows := value.(type) {
	case []map[string]any:
		return rows
	case []any:
		out := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			mapped, ok := row.(map[string]any)
			if !ok {
				return nil
			}
			out = append(out, mapped)
		}
		return out
	default:
		return nil
	}
}

func localizedContinuitySnapshot(locale string, total int, schedule, delivery map[string]int) string {
	if locale == "ar" {
		return fmt.Sprintf("لقطة CMS شملت %d من مصادر الوسائط: %d مستحقّة بلا طلب تشغيل نشط، %d قيد التنفيذ، %d مجدولة، و%d بحالة جدول غير معروفة. تحقق وصول Pods: %d مثبت، %d قيد المراقبة، و%d غير معروف.", total, schedule["due_unadmitted"], schedule["in_flight"], schedule["scheduled"], schedule["unknown"], delivery["verified"], delivery["pending"], delivery["unknown"]+delivery["degraded"])
	}
	return fmt.Sprintf("CMS sampled %d Media sources: %d due with no active source run, %d in flight, %d scheduled, and %d with an unknown schedule. Pods delivery: %d verified, %d still observing, and %d unknown or degraded.", total, schedule["due_unadmitted"], schedule["in_flight"], schedule["scheduled"], schedule["unknown"], delivery["verified"], delivery["pending"], delivery["unknown"]+delivery["degraded"])
}

func localizedDueUnadmittedDiagnosis(locale string, names []string) string {
	if locale == "ar" {
		if len(names) > 0 {
			return "أول انقطاع مرصود هو قبول CMS للتشغيل: هذه المصادر مستحقّة بلا طلب تشغيل نشط (" + strings.Join(names, "، ") + "). لا يثبت هذا فشل المزود أو حالة الطابور."
		}
		return "أول انقطاع مرصود هو قبول CMS للتشغيل: توجد مصادر مستحقّة بلا طلب تشغيل نشط. لا يثبت هذا فشل المزود أو حالة الطابور."
	}
	if len(names) > 0 {
		return "The first observed break is CMS admission: these sources are due without an active source run (" + strings.Join(names, ", ") + "). This does not prove a provider failure or queue state."
	}
	return "The first observed break is CMS admission: one or more sources are due without an active source run. This does not prove a provider failure or queue state."
}

func localizedRecommendationSummary(recommendation Recommendation) string {
	if recommendation.Kind != "inspect" {
		return "راجع الدليل التشغيلي الحالي في CMS قبل اتخاذ أي تغيير."
	}
	switch strings.TrimPrefix(recommendation.ID, "inspect:") {
	case "media_sources":
		return "راجع المصدر وطلب التشغيل الدائم ومسار المعالجة في مصادر الوسائط."
	case "media_circulation":
		return "راجع حالة جدولة المصادر وأدلة وصول Pods والسياسة الحالية قبل اتخاذ أي تغيير."
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
