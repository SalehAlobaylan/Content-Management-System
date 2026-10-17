package economics

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"testing"
	"time"
)

func consumptionTables(t *testing.T, db *gorm.DB) {
	t.Helper()
	t.Setenv("DEFAULT_TENANT_ID", "a")
	for _, q := range []string{`CREATE TABLE content_items(id bigserial PRIMARY KEY,public_id uuid UNIQUE,tenant_id text,author_id uuid,type text,story_id uuid,parent_content_item_id uuid,created_at timestamptz DEFAULT now())`, `CREATE TABLE user_interactions(id bigserial PRIMARY KEY,public_id uuid UNIQUE,user_id uuid,session_id text,content_item_id uuid,type text,metadata jsonb,created_at timestamptz,comment_moderation_status text,comment_moderation_reason text)`} {
		if e := db.Exec(q).Error; e != nil {
			t.Fatal(e)
		}
	}
}
func interaction(t *testing.T, db *gorm.DB, account, content uuid.UUID, kind string, m ConsumptionObservation) {
	t.Helper()
	payload, _ := json.Marshal(m)
	row := models.UserInteraction{PublicID: uuid.New(), UserID: &account, ContentItemID: content, Type: models.InteractionType(kind), Metadata: payload, CreatedAt: time.Now().UTC()}
	if e := db.Create(&row).Error; e != nil {
		t.Fatal(e)
	}
}
func TestCumulativeConsumptionReplayAndNewsDistinctness(t *testing.T) {
	db := economicsDB(t)
	consumptionTables(t, db)
	account, media, article, story := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	db.Exec(`INSERT INTO content_items(public_id,tenant_id,type) VALUES(?,'a','PODCAST')`, media)
	db.Exec(`INSERT INTO content_items(public_id,tenant_id,type,story_id) VALUES(?,'a','NEWS',?)`, article, story)
	now := time.Now().UTC()
	m := ConsumptionObservation{Version: 1, Client: "mobile", Segment: uuid.NewString(), ObservedAt: now, Semantics: "media_position_seconds_v1", Played: 30}
	interaction(t, db, account, media, "progress", m)
	m.Played = 60
	interaction(t, db, account, media, "progress", m)
	interaction(t, db, account, media, "complete", m)
	n := ConsumptionObservation{Version: 1, Client: "mobile", ObservedAt: now, Semantics: "story_open_v1", StoryID: story.String()}
	interaction(t, db, account, article, "view", n)
	interaction(t, db, account, article, "view", n)
	for i := 0; i < 2; i++ {
		if e := Rebuild(db, "a"); e != nil {
			t.Fatal(e)
		}
	}
	var played, opens int64
	db.Model(&Usage{}).Where("unit='media_seconds'").Select("sum(value)").Scan(&played)
	db.Model(&Usage{}).Where("unit='news_opens'").Select("sum(value)").Scan(&opens)
	if played != 60 || opens != 1 {
		t.Fatal("cumulative/repeated usage overcount", played, opens)
	}
	if e := db.Transaction(func(tx *gorm.DB) error { return Clear(tx, account.String(), time.Now().UTC()) }); e != nil {
		t.Fatal(e)
	}
	n.ObservedAt = time.Now().UTC()
	interaction(t, db, account, article, "view", n)
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var linked int64
	db.Model(&Weight{}).Where("subject=?", "account:"+account.String()).Count(&linked)
	if linked != 0 {
		t.Fatal("history clear recreated content links")
	}
	var kept int64
	db.Model(&Usage{}).Where("unit='news_opens'").Select("sum(value)").Scan(&kept)
	if kept != 1 {
		t.Fatal("history clear invented a second distinct preparation weight")
	}
}
func TestQualifiedProfileIncludesPublicAndLeavesIdleOverhead(t *testing.T) {
	db := economicsDB(t)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	account := "account:" + uuid.NewString()
	for _, u := range []Usage{{TenantID: "a", Day: day, Subject: account, Operation: "search", Unit: "requests", Variant: "ok", Value: 2}, {TenantID: "a", Day: day, Subject: "public", Operation: "search", Unit: "requests", Variant: "ok", Value: 1}} {
		db.Create(&u)
	}
	db.Create(&Profile{TenantID: "a", Resource: "database", Operation: "search", Version: 1, EffectiveFrom: day, Weight: 10, Evidence: "Controlled indexed search workload", Actor: "test"})
	gross := mustAmount(t, "150")
	c := Cost{TenantID: "a", SourceID: "fixture", LogicalKey: "database", Provider: "test", Resource: "database", Feature: "serving", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "100"), BeforeBenefits: &gross, Currency: "USD", Destination: "infrastructure", Metric: "requests", AllocableBPS: 3000, Reason: "30 percent measured serving pool; idle excluded", Actor: "test"}
	if _, e := AppendCost(db, c); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var rows []Summary
	db.Find(&rows)
	sum := Zero
	accountSum := Zero
	before := Zero
	for _, r := range rows {
		sum = Add(sum, Add(r.Direct, r.Shared))
		if r.Subject == account {
			accountSum = Add(accountSum, Add(r.Direct, r.Shared))
		}
		if r.BeforeBenefits != nil {
			before = Add(before, *r.BeforeBenefits)
		}
	}
	if sum != c.Net || accountSum != mustAmount(t, "20") || before != gross {
		t.Fatal("allocation did not conserve or charged idle/public work to account", sum, accountSum, before)
	}
}
func TestLifecycleBarrierSurvivesJWTKeyRotation(t *testing.T) {
	db := economicsDB(t)
	subject := "account:" + uuid.NewString()
	if e := db.Transaction(func(tx *gorm.DB) error { return Erase(tx, subject[8:]) }); e != nil {
		t.Fatal(e)
	}
	t.Setenv("JWT_SECRET", "rotated-test-key")
	resolved, _, e := ResolveSubject(db, subject, time.Now())
	if e != nil || resolved != "removed" {
		t.Fatal("JWT rotation invalidated deletion barrier", resolved, e)
	}
}

func TestCumulativePlaybackAcrossMonthsDoesNotDoubleCountOrInventAnAllocation(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			db := economicsDB(t)
			consumptionTables(t, db)
			account, content := uuid.New(), uuid.New()
			db.Exec(`INSERT INTO content_items(public_id,tenant_id,type) VALUES(?,'a','PODCAST')`, content)
			now := time.Now().UTC()
			month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
			observations := []ConsumptionObservation{{Version: 1, Client: "mobile", Segment: uuid.NewString(), Semantics: "media_position_seconds_v1", ObservedAt: month.Add(-time.Second), Played: 30}}
			next := observations[0]
			next.Played = 60
			next.ObservedAt = month.Add(time.Second)
			observations = append(observations, next)
			if reverse {
				observations[0], observations[1] = observations[1], observations[0]
			}
			for _, m := range observations {
				interaction(t, db, account, content, "progress", m)
				if e := ProjectConsumption(db, "a", now); e != nil {
					t.Fatal(e)
				}
			}
			var usage int64
			db.Model(&Usage{}).Where("unit='media_seconds'").Select("sum(value)").Scan(&usage)
			var weights []Weight
			db.Find(&weights)
			var total int64
			for _, w := range weights {
				total += w.Value
			}
			if usage != 60 || total != 60 {
				t.Fatal("a month boundary duplicated a cumulative checkpoint", usage, total)
			}
			metrics, e := AccountEfficiency(db, "a", "account:"+account.String(), month.AddDate(0, -1, 0), month.AddDate(0, 1, 0))
			if e != nil {
				t.Fatal(e)
			}
			if metrics[0].Amount != nil || metrics[0].Reason != "Cross-month cumulative playback lacks a qualified monthly baseline" {
				t.Fatal("monthly efficiency inferred a missing baseline", metrics)
			}
			for _, start := range []time.Time{month, month.AddDate(0, -1, 0)} {
				c := Cost{ContentID: content.String(), Metric: "media_seconds", Observation: "mobile", PeriodStart: start}
				if len(contentWeights(c, indexContentWeights(weights)[content.String()+":"+start.Format("2006-01")])) != 0 {
					t.Fatal("uncertain cross-month segment became financial allocation")
				}
			}
		})
	}
}
