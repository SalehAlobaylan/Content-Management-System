package controllers

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const redundancyEvaluator = "body-minhash/v3"

var redundancyWord = regexp.MustCompile(`[\p{L}\p{N}]+`)
var redundancyTitleNoise = regexp.MustCompile(`(?i)\[[^]]*\]|\([^)]*\)|\b(?:episode|ep|official|youtube)\b|(?:الحلقة|حلقة)\s*[\d٠-٩]+`)

func normalizeRedundancyTitle(s string) string {
	return strings.Join(redundancyWord.FindAllString(strings.ToLower(redundancyTitleNoise.ReplaceAllString(s, " ")), -1), " ")
}
func titleSimilarity(a, b string) float64 {
	a, b = normalizeRedundancyTitle(a), normalizeRedundancyTitle(b)
	if a == "" || b == "" {
		return 0
	}
	if a == b {
		return 1
	}
	set := func(s string) map[string]bool {
		m := map[string]bool{}
		for _, w := range strings.Fields(s) {
			m[w] = true
		}
		return m
	}
	x, y := set(a), set(b)
	hits := 0
	for w := range x {
		if y[w] {
			hits++
		}
	}
	return float64(hits) / float64(len(x)+len(y)-hits)
}
func transcriptBody(text string) string {
	w := redundancyWord.FindAllString(strings.ToLower(text), -1)
	trim := 0
	if len(w) >= 20 {
		trim = len(w) * 8 / 100
	}
	return strings.Join(w[trim:len(w)-trim], " ")
}
func redundancyTranscriptBody(t models.Transcript) string {
	var segments []struct {
		Start float64 `json:"start"`
		End   float64 `json:"end"`
		Text  string  `json:"text"`
	}
	if json.Unmarshal(t.Segments, &segments) == nil && len(segments) > 0 {
		end := 0.
		for _, s := range segments {
			if s.End > end {
				end = s.End
			}
		}
		if end > 180 {
			parts := []string{}
			for _, s := range segments {
				if s.Start >= 90 && s.End <= end-90 {
					parts = append(parts, s.Text)
				}
			}
			return strings.Join(redundancyWord.FindAllString(strings.ToLower(strings.Join(parts, " ")), -1), " ")
		}
	}
	return transcriptBody(t.FullText)
}
func redundancyShingles(s string) map[string]bool {
	w := strings.Fields(s)
	m := map[string]bool{}
	for i := 0; i+8 <= len(w); i++ {
		m[strings.Join(w[i:i+8], " ")] = true
	}
	return m
}
func shingleJaccard(a, b string) float64 {
	x, y := redundancyShingles(a), redundancyShingles(b)
	n := 0
	for k := range x {
		if y[k] {
			n++
		}
	}
	if len(x)+len(y)-n == 0 {
		return 0
	}
	return float64(n) / float64(len(x)+len(y)-n)
}
func redundancyMinHash(body string) []uint64 {
	shingles := redundancyShingles(body)
	if len(shingles) == 0 {
		return nil
	}
	sig := make([]uint64, 128)
	for i := range sig {
		sig[i] = math.MaxUint64
	}
	for s := range shingles {
		h := sha256.Sum256([]byte(s))
		base := binary.LittleEndian.Uint64(h[:8])
		for i := range sig {
			x := base + uint64(i+1)*0x9e3779b97f4a7c15
			x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
			x = (x ^ (x >> 27)) * 0x94d049bb133111eb
			x ^= x >> 31
			if x < sig[i] {
				sig[i] = x
			}
		}
	}
	return sig
}
func redundancySignatureSimilarity(a, b []uint64) float64 {
	if len(a) != 128 || len(b) != 128 {
		return 0
	}
	n := 0
	for i := range a {
		if a[i] == b[i] {
			n++
		}
	}
	return float64(n) / 128
}

// Only the content owner's active transcript is evidence. Canceled writebacks
// may persist newer transcript rows without activating them.
func activeRedundancyTranscript(db *gorm.DB, tenant string, id uuid.UUID) (models.Transcript, error) {
	var t models.Transcript
	err := db.Joins("JOIN content_items ci ON ci.transcript_id=transcripts.public_id AND ci.public_id=transcripts.content_item_id").Where("ci.tenant_id=? AND ci.public_id=?", tenant, id).First(&t).Error
	return t, err
}
func loadRedundancyFingerprint(db *gorm.DB, tenant string, id uuid.UUID) (models.RedundancyFingerprint, error) {
	t, err := activeRedundancyTranscript(db, tenant, id)
	if err != nil {
		return models.RedundancyFingerprint{}, err
	}
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(redundancyEvaluator+t.PublicID.String()+t.FullText+string(t.Segments))))
	var fp models.RedundancyFingerprint
	err = db.Where("tenant_id=? AND content_item_id=? AND transcript_checksum=?", tenant, id, checksum).First(&fp).Error
	if err == nil && len(fp.Signature) > 2 {
		return fp, nil
	}
	if err != nil && err != gorm.ErrRecordNotFound {
		return fp, err
	}
	body := redundancyTranscriptBody(t)
	sig, _ := json.Marshal(redundancyMinHash(body))
	if string(sig) == "null" {
		sig = []byte("[]")
	}
	fp = models.RedundancyFingerprint{TenantID: tenant, ContentItemID: id, TranscriptChecksum: checksum, BodyHash: fmt.Sprintf("%x", sha256.Sum256([]byte(body))), Signature: datatypes.JSON(sig), ShingleCount: len(redundancyShingles(body))}
	err = db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "content_item_id"}, {Name: "transcript_checksum"}}, DoUpdates: clause.AssignmentColumns([]string{"signature", "body_hash", "shingle_count", "updated_at"})}).Create(&fp).Error
	return fp, err
}

type redundancyLane struct {
	Score     float64 `json:"score"`
	Available bool    `json:"available"`
	Reason    string  `json:"reason,omitempty"`
}
type redundancyLaneSettings struct {
	TranscriptConfirm float64 `json:"transcript_confirm"`
	EmbeddingConfirm  float64 `json:"embedding_confirm"`
	TitleConfirm      float64 `json:"title_confirm"`
	TitleWeight       float64 `json:"title_weight"`
	TranscriptWeight  float64 `json:"transcript_weight"`
	EmbeddingWeight   float64 `json:"embedding_weight"`
	ThumbnailWeight   float64 `json:"thumbnail_weight"`
}

func redundancySettings(p models.RedundancyPolicy) redundancyLaneSettings {
	s := redundancyLaneSettings{.80, .93, .70, .10, .70, .18, .02}
	_ = json.Unmarshal(p.LaneSettings, &s)
	return s
}
func redundancyEvidenceCheck(p models.RedundancyPolicy, lanes map[string]redundancyLane, err error) error {
	t := lanes["transcript"]
	if errors.Is(err, errRedundancyDescriptorUnavailable) && t.Available && (t.Score < .25 || t.Score >= redundancySettings(p).TranscriptConfirm) {
		return nil // Independent transcript proof does not wait on vector infrastructure.
	}
	return err
}
func redundancyVerdict(l map[string]redundancyLane, p models.RedundancyPolicy) (float64, string) {
	s := redundancySettings(p)
	if !l["duration"].Available || l["duration"].Score == 0 {
		return 0, models.RedundancyVerdictClear
	}
	// Transcript disagreement is strong negative evidence; artwork/title cannot overturn it.
	if l["transcript"].Available && l["transcript"].Score < .25 {
		return l["transcript"].Score, models.RedundancyVerdictClear
	}
	sum, weight := 0., 0.
	for key, w := range map[string]float64{"title": s.TitleWeight, "transcript": s.TranscriptWeight, "embedding": s.EmbeddingWeight, "thumbnail": s.ThumbnailWeight} {
		if l[key].Available {
			sum += l[key].Score * w
			weight += w
		}
	}
	score := 0.
	if weight > 0 {
		score = sum / weight
	}
	strong := l["transcript"].Available && l["transcript"].Score >= .55
	corroborated := l["embedding"].Available && l["embedding"].Score >= .85 && l["title"].Score >= .70
	if strong && corroborated {
		score = math.Min(1, score+.08)
	}
	high := l["transcript"].Available && l["transcript"].Score >= s.TranscriptConfirm || l["embedding"].Available && l["embedding"].Score >= s.EmbeddingConfirm && l["title"].Score >= s.TitleConfirm
	if high {
		return math.Max(score, s.TranscriptConfirm), models.RedundancyVerdictHighConfidence
	}
	if (strong || corroborated) && score >= p.ProposalFloor {
		return score, models.RedundancyVerdictProbable
	}
	return score, models.RedundancyVerdictClear
}
func contentTitle(item models.ContentItem) string {
	if item.Title == nil {
		return ""
	}
	return *item.Title
}
func redundancyAbs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
func redundancyMaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func redundancyJSON(v interface{}) datatypes.JSON { b, _ := json.Marshal(v); return datatypes.JSON(b) }
func redundancyStringPtr(s string) *string        { return &s }

// Stable IDs prevent DB query order from changing frozen candidate continuation.
func redundancySortedIDs(m map[uuid.UUID]bool) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
