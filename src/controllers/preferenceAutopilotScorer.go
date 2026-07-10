package controllers

import (
	"content-management-system/src/models"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Preferences Autopilot — deterministic proposal scorer (plan §6, §15). The LLM
// role is Level 2 Advisor and is CONTAINED to bilingual label prefill; the score
// itself is Level 0 cosine math, immune to hallucination. Confidence never owns a
// catalog decision — it only ranks the human review queue and (in a later slice)
// gates an earned auto-approve tier.

const (
	// Scorer component weights (§6). support dominates; category is the weakest
	// signal and is dropped from the denominator when unavailable.
	prefScoreWeightSupport    = 0.50
	prefScoreWeightUniqueness = 0.30
	prefScoreWeightCategory   = 0.20

	// Fixed scoring constants (§6) — deliberately NOT env/policy: they define the
	// score's shape, not an operator tuning knob.
	prefSupportSaturation    = 25.0 // members at which support ≈ 1
	prefUniqueLow            = 0.70 // cosine below which a proposal is fully unique
	prefUniqueBand           = 0.20 // width of the uniqueness ramp
	prefCategoryMarginBand   = 0.12 // top1-top2 cosine gap that saturates category
	prefReviewImpressionSat  = 1000.0
	prefReviewAgeSatDays     = 14.0
	prefHighConfidenceMinMem = 5
)

// prefCategoryVector is a derived category centroid: the normalized mean of the
// active canonical-topic centroids in that category (§0.1.4). Categories with
// fewer than two usable centroids are omitted entirely, yielding category_unknown
// rather than a fabricated margin.
func buildDerivedCategoryVectors(topics []topicVector) map[string][]float32 {
	sums := map[string][]float32{}
	counts := map[string]int{}
	for _, t := range topics {
		cat := strings.TrimSpace(t.CategorySlug)
		if cat == "" || len(t.Vec) == 0 {
			continue
		}
		if sums[cat] == nil {
			sums[cat] = make([]float32, len(t.Vec))
		}
		if len(sums[cat]) != len(t.Vec) {
			continue
		}
		for i, v := range t.Vec {
			sums[cat][i] += v
		}
		counts[cat]++
	}
	out := map[string][]float32{}
	for cat, sum := range sums {
		if counts[cat] < 2 { // need ≥2 to form a meaningful mean (§0.1.4)
			continue
		}
		mean := make([]float32, len(sum))
		n := float32(counts[cat])
		for i, v := range sum {
			mean[i] = v / n
		}
		out[cat] = l2Normalize(mean)
	}
	return out
}

func l2Normalize(v []float32) []float32 {
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm == 0 {
		return v
	}
	norm = math.Sqrt(norm)
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = float32(float64(x) / norm)
	}
	return out
}

type proposalScorer struct {
	db       *gorm.DB
	tenantID string
	policy   models.PreferenceAutopilotPolicy
	topics   []topicVector
	catVecs  map[string][]float32
	persist  bool

	embedCalls     int
	translateCalls int
}

func newProposalScorer(db *gorm.DB, tenantID string, policy models.PreferenceAutopilotPolicy, persist bool) *proposalScorer {
	topics, _ := activeTopicVectors(db, tenantID)
	return &proposalScorer{
		db: db, tenantID: tenantID, policy: policy, persist: persist,
		topics:  topics,
		catVecs: buildDerivedCategoryVectors(topics),
	}
}

type proposalEvidence struct {
	MemberCount     int64 `json:"member_count"`
	ImpressionCount int64 `json:"impression_count"`
	ServedMembers   int64 `json:"served_member_count"`
}

func parseProposalEvidence(raw datatypes.JSON) proposalEvidence {
	var ev proposalEvidence
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &ev)
	}
	return ev
}

type proposalScore struct {
	ProposalID      uint    `json:"proposal_id"`
	Slug            string  `json:"slug"`
	Confidence      float64 `json:"confidence"`
	Verdict         string  `json:"verdict"`
	ReviewPriority  float64 `json:"review_priority"`
	Duplicate       bool    `json:"duplicate"`
	DuplicateOfSlug string  `json:"duplicate_of_slug,omitempty"`
	CategoryUnknown bool    `json:"category_unknown"`
	NeedsLabel      bool    `json:"needs_label"`
	EmbeddingReady  bool    `json:"embedding_ready"`
	Reason          string  `json:"reason"`
}

// score computes the deterministic confidence + verdict for one proposal and, when
// persist is set (safe_auto only), writes the advisor columns + frozen prediction.
// In observe it computes identically but writes nothing (§8.7, §12).
func (s *proposalScorer) score(p *models.TopicProposal) proposalScore {
	res := proposalScore{ProposalID: p.ID, Slug: p.SuggestedSlug}
	ev := parseProposalEvidence(p.Evidence)

	// Bilingual label prefill (Advisor, bounded). Fills a missing/sluggish AR or EN
	// from the other language via the Enrichment translate wrapper. Display prefill
	// only; a still-unresolved label is a blocker flag, not evidence quality.
	ar, en, needsLabel := s.fillLabels(p)

	// Embedding (cached; bounded per run). Without it, duplicate/category are
	// unknowable and high-confidence is blocked by construction.
	vec, embReady := s.ensureEmbedding(p, ar, en)
	res.EmbeddingReady = embReady

	support := clamp01(math.Log1p(float64(ev.MemberCount)) / math.Log1p(prefSupportSaturation))

	components := []float64{support}
	weights := []float64{prefScoreWeightSupport}

	duplicate := false
	dupSlug := ""
	if embReady && len(s.topics) > 0 {
		maxCos, argSlug := s.maxTopicCosine(vec)
		uniqueness := 1 - clamp01((maxCos-prefUniqueLow)/prefUniqueBand)
		components = append(components, uniqueness)
		weights = append(weights, prefScoreWeightUniqueness)
		if maxCos >= s.policy.DuplicateCosine {
			duplicate = true
			dupSlug = argSlug
		}
	}

	categoryUnknown := true
	if embReady && len(s.catVecs) >= 2 {
		top1, top2 := s.topTwoCategoryCosine(vec)
		category := clamp01((top1 - top2) / prefCategoryMarginBand)
		components = append(components, category)
		weights = append(weights, prefScoreWeightCategory)
		categoryUnknown = false
	}

	confidence := weightedMean(components, weights)
	res.Confidence = confidence
	res.Duplicate = duplicate
	res.DuplicateOfSlug = dupSlug
	res.CategoryUnknown = categoryUnknown
	res.NeedsLabel = needsLabel

	// Verdict (§6). High confidence needs the threshold AND no blocker AND member
	// support; low confidence is advisory-reject; everything else is review.
	blocked := duplicate || categoryUnknown || needsLabel || !embReady
	switch {
	case confidence >= s.policy.HighConfidence && !blocked && ev.MemberCount >= prefHighConfidenceMinMem:
		res.Verdict = models.PreferenceVerdictHighConf
	case confidence < s.policy.AdvisoryRejectFloor:
		res.Verdict = models.PreferenceVerdictSuggestRej
	default:
		res.Verdict = models.PreferenceVerdictReview
	}

	// Review priority is a SEPARATE tiebreaker (§6) — demand never changes confidence.
	res.ReviewPriority = 0.50*clamp01(math.Log1p(float64(ev.ImpressionCount))/math.Log1p(prefReviewImpressionSat)) +
		0.30*clampRatio(ev.ServedMembers, ev.MemberCount) +
		0.20*clamp01(daysSince(p.CreatedAt)/prefReviewAgeSatDays)

	res.Reason = s.explain(res, ev)

	if s.persist {
		s.persistScore(p, res, vec, embReady, ar, en)
	}
	return res
}

func (s *proposalScorer) explain(res proposalScore, ev proposalEvidence) string {
	parts := []string{fmt.Sprintf("confidence %.2f (%s)", res.Confidence, res.Verdict)}
	if res.Duplicate {
		parts = append(parts, fmt.Sprintf("near-duplicate of %q", res.DuplicateOfSlug))
	}
	if res.CategoryUnknown {
		parts = append(parts, "category unknown")
	}
	if res.NeedsLabel {
		parts = append(parts, "needs bilingual label")
	}
	if !res.EmbeddingReady {
		parts = append(parts, "embedding unavailable")
	}
	parts = append(parts, fmt.Sprintf("%d members", ev.MemberCount))
	return strings.Join(parts, "; ")
}

// fillLabels returns the resolved (ar, en) labels and whether a bilingual label is
// still missing. Translation is bounded per run and only fills a blank or sluggish
// field from the other language.
//
// It runs ONLY when persisting (safe_auto). In observe the filled labels are never
// written, so translating them would re-spend the LLM on identical inputs every
// scheduled run for zero retained value; observe therefore reports label-readiness
// from the raw stored labels (a lower bound the human sees resolved once safe_auto
// runs).
func (s *proposalScorer) fillLabels(p *models.TopicProposal) (string, string, bool) {
	ar := strings.TrimSpace(p.SuggestedLabelAR)
	en := strings.TrimSpace(p.SuggestedLabelEN)
	slug := p.SuggestedSlug

	enSluggish := isSluggishLabel(en, slug)
	// A real AR label must actually be in Arabic script — mining copies the EN slug
	// into suggested_label_ar, which is junk.
	arSluggish := isSluggishLabel(ar, slug) || !hasArabicScript(ar)

	if !s.persist {
		return ar, en, arSluggish || enSluggish
	}

	if arSluggish && !enSluggish && s.translateCalls < s.policy.MaxTranslationCalls {
		if t, err := translateViaEnrichment(en, "ar"); err == nil && hasArabicScript(t) {
			ar = t
			arSluggish = false
		}
		s.translateCalls++
	}
	if enSluggish && !arSluggish && s.translateCalls < s.policy.MaxTranslationCalls {
		if t, err := translateViaEnrichment(ar, "en"); err == nil && strings.TrimSpace(t) != "" {
			en = t
			enSluggish = false
		}
		s.translateCalls++
	}
	return ar, en, arSluggish || enSluggish
}

// ensureEmbedding reuses the cached proposal embedding when the input hash matches,
// otherwise embeds once (bounded by MaxEmbeddingCalls). The cache is persisted in
// BOTH modes: it is deterministic derived data keyed by an input hash, not an
// advisory suggestion, so caching it in observe is safe and avoids re-embedding the
// same unchanged proposals on every scheduled run (§0.1.8).
func (s *proposalScorer) ensureEmbedding(p *models.TopicProposal, ar, en string) ([]float32, bool) {
	hash := hashProposalEmbeddingInput(p.SuggestedSlug, ar, en, p.SuggestedCategory)
	if p.Embedding != nil && p.EmbeddingInputHash == hash {
		return p.Embedding.Slice(), true
	}
	if s.embedCalls >= s.policy.MaxEmbeddingCalls {
		return nil, false
	}
	text := strings.TrimSpace(en + " " + ar + " " + strings.ReplaceAll(p.SuggestedSlug, "-", " "))
	emb, err := embedQueryViaEnrichment(text)
	s.embedCalls++
	if err != nil || len(emb) != 1024 {
		return nil, false
	}
	vec := pgvector.NewVector(emb)
	now := time.Now()
	_ = s.db.Model(&models.TopicProposal{}).Where("id = ?", p.ID).Updates(map[string]interface{}{
		"embedding": vec, "embedding_input_hash": hash, "embedded_at": now,
	}).Error
	return emb, true
}

func (s *proposalScorer) maxTopicCosine(vec []float32) (float64, string) {
	maxCos := 0.0
	argID := ""
	for _, t := range s.topics {
		c := cosine(vec, t.Vec)
		if c > maxCos {
			maxCos = c
			argID = t.ID.String()
		}
	}
	if argID == "" {
		return maxCos, ""
	}
	// Resolve the slug for a human-readable duplicate-of hint.
	var slug string
	_ = s.db.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", s.tenantID, argID).Pluck("slug", &slug)
	return maxCos, slug
}

func (s *proposalScorer) topTwoCategoryCosine(vec []float32) (float64, float64) {
	scores := make([]float64, 0, len(s.catVecs))
	for _, cv := range s.catVecs {
		scores = append(scores, cosine(vec, cv))
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(scores)))
	top1, top2 := 0.0, 0.0
	if len(scores) > 0 {
		top1 = scores[0]
	}
	if len(scores) > 1 {
		top2 = scores[1]
	}
	return top1, top2
}

// persistScore writes the advisor columns + frozen prediction (safe_auto only).
// Labels are written back only when we actually improved them.
func (s *proposalScorer) persistScore(p *models.TopicProposal, res proposalScore, vec []float32, embReady bool, ar, en string) {
	flags := map[string]interface{}{
		"duplicate":        res.Duplicate,
		"duplicate_of":     res.DuplicateOfSlug,
		"category_unknown": res.CategoryUnknown,
		"needs_label":      res.NeedsLabel,
		"embedding_ready":  embReady,
		"review_priority":  res.ReviewPriority,
		"suggest_reject":   res.Verdict == models.PreferenceVerdictSuggestRej,
	}
	flagsJSON, _ := json.Marshal(flags)
	now := time.Now()
	updates := map[string]interface{}{
		"confidence":      res.Confidence,
		"autopilot_flags": datatypes.JSON(flagsJSON),
		"enriched_at":     now,
	}
	// Trust evidence is immutable once first recorded. Later scoring may refresh
	// confidence and advisor flags, but cannot rewrite the prediction a human will
	// eventually be compared against.
	if shouldFreezeProposalPrediction(p) {
		updates["predicted_verdict"] = res.Verdict
		updates["predicted_at"] = now
		updates["prediction_version"] = models.PreferencePredictionVersion
	}
	// Only overwrite labels with improved prefill (never clobber a human/better AR).
	if strings.TrimSpace(ar) != "" && strings.TrimSpace(ar) != strings.TrimSpace(p.SuggestedLabelAR) {
		updates["suggested_label_ar"] = ar
	}
	if strings.TrimSpace(en) != "" && strings.TrimSpace(en) != strings.TrimSpace(p.SuggestedLabelEN) {
		updates["suggested_label_en"] = en
	}
	_ = s.db.Model(&models.TopicProposal{}).Where("id = ? AND status = ?", p.ID, "pending").Updates(updates).Error
}

func shouldFreezeProposalPrediction(p *models.TopicProposal) bool {
	return strings.TrimSpace(p.PredictionVersion) == "" || strings.TrimSpace(p.PredictedVerdict) == ""
}

// ---- pure helpers ----

func hashProposalEmbeddingInput(slug, ar, en, category string) string {
	h := sha256.Sum256([]byte(slug + "\x00" + ar + "\x00" + en + "\x00" + category))
	return hex.EncodeToString(h[:16])
}

// isSluggishLabel reports whether a label is empty or just the slug's words (the
// mining placeholder), i.e. carries no real editorial signal.
func isSluggishLabel(label, slug string) bool {
	label = strings.TrimSpace(label)
	if label == "" {
		return true
	}
	return normalizedTopicSlug(label) == normalizedTopicSlug(slug)
}

func clampRatio(num, den int64) float64 {
	if den <= 0 {
		return 0
	}
	return clamp01(float64(num) / float64(den))
}

func weightedMean(values, weights []float64) float64 {
	var sum, wsum float64
	for i := range values {
		sum += values[i] * weights[i]
		wsum += weights[i]
	}
	if wsum == 0 {
		return 0
	}
	return sum / wsum
}

func daysSince(t time.Time) float64 {
	d := time.Since(t).Hours() / 24
	if d < 0 {
		return 0
	}
	return d
}
