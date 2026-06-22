package controllers

import (
	"encoding/json"
	"testing"

	"content-management-system/src/models"
	"gorm.io/datatypes"
)

func mkCand(kind, domain, bio, url string, titles ...string) *models.SourceCandidate {
	fh, _ := json.Marshal(map[string]any{"bio": bio, "url": url})
	st := make([]map[string]any, 0, len(titles))
	for _, t := range titles {
		st = append(st, map[string]any{"title": t})
	}
	stj, _ := json.Marshal(st)
	return &models.SourceCandidate{
		Kind:         kind,
		Domain:       domain,
		FeedHealth:   datatypes.JSON(fh),
		SampleTitles: datatypes.JSON(stj),
	}
}

func TestClassifySource(t *testing.T) {
	cases := []struct {
		name string
		cand *models.SourceCandidate
		want string
	}{
		{"gov bio url", mkCand(models.CandidateKindTwitter, "spagov", "واس - وكالة الأنباء السعودية", "https://spa.gov.sa"), SourceClassOfficial},
		{"ministry name", mkCand(models.CandidateKindTwitter, "moisaudiarabia", "وزارة الداخلية", ""), SourceClassOfficial},
		{"allowlist outlet no keyword", mkCand(models.CandidateKindTwitter, "alarabiya", "العربية #أن_تعرف_أكثر", ""), SourceClassNews},
		{"news keyword tv", mkCand(models.CandidateKindTwitter, "alekhbariyatv", "الحساب الرسمي لقناة الإخبارية", ""), SourceClassNews},
		{"news handle token", mkCand(models.CandidateKindTwitter, "gulf_news", "", ""), SourceClassNews},
		{"journalist person", mkCand(models.CandidateKindTwitter, "someguy", "صحفي ومحلل سياسي", ""), SourceClassPerson},
		{"english journalist", mkCand(models.CandidateKindTwitter, "janedoe", "journalist and author", ""), SourceClassPerson},
		{"rss defaults news", mkCand("rss", "aljazeera.net", "", ""), SourceClassNews},
		{"rss gov host", mkCand("rss", "my.gov.sa", "", ""), SourceClassOfficial},
		{"plain other", mkCand(models.CandidateKindTwitter, "randombrand", "we sell shoes", ""), SourceClassOther},
		{"broadcaster authority -> news wins", mkCand(models.CandidateKindTwitter, "someradio", "هيئة الإذاعة والتلفزيون", ""), SourceClassNews},
		{"official allowlist ruler", mkCand(models.CandidateKindTwitter, "KingSalman", "", ""), SourceClassOfficial},
		{"official allowlist exchange", mkCand(models.CandidateKindTwitter, "tadawul", "", ""), SourceClassOfficial},
	}
	for _, c := range cases {
		if got := classifySource(c.cand); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
