package economics

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type ConsumptionObservation struct {
	Version    int       `json:"economics_version"`
	Client     string    `json:"economics_client"`
	Segment    string    `json:"economics_segment_id"`
	ObservedAt time.Time `json:"economics_observed_at"`
	Semantics  string    `json:"economics_observation"`
	Played     float64   `json:"actual_played_seconds"`
	StoryID    string    `json:"story_id"`
}
type weightKey struct {
	Month                                   int64
	Content, Subject, Segment, Kind, Client string
}

func weightIdentity(w Weight) weightKey {
	return weightKey{w.Month.Unix(), w.ContentID, w.Subject, w.Segment, w.Kind, w.Client}
}

type mediaSegmentKey struct{ Content, Subject, Segment, Client string }

func mediaSegment(w Weight) mediaSegmentKey {
	return mediaSegmentKey{w.ContentID, w.Subject, w.Segment, w.Client}
}

type interactionReceipt struct {
	InteractionID uuid.UUID
	TenantID      string
	ObservedAt    time.Time
	Status        string
}

// Business interactions are folded in bounded batches with no extra request,
// network loop or synchronous economics write on the consumer path.
func ProjectConsumption(tx *gorm.DB, tenant string, now time.Time, activated ...*time.Time) error {
	if PublicTenant() != tenant {
		return nil
	}
	var exists bool
	if e := tx.Raw("SELECT to_regclass('user_interactions') IS NOT NULL AND to_regclass('content_items') IS NOT NULL").Scan(&exists).Error; e != nil || !exists {
		return e
	}
	from := now.AddDate(0, 0, -90)
	if len(activated) > 0 && activated[0] != nil && activated[0].After(from) {
		from = *activated[0]
	}
	var rows []struct {
		models.UserInteraction
		CanonicalStoryID, ParentID *string
	}
	q := `SELECT i.*,c.story_id::text canonical_story_id,c.parent_content_item_id::text parent_id FROM user_interactions i JOIN content_items c ON c.public_id=i.content_item_id AND c.tenant_id=? WHERE i.created_at>=? AND NOT EXISTS(SELECT 1 FROM economics_interaction_receipts r WHERE r.interaction_id=i.public_id) AND i.metadata->>'economics_version'='1' AND i.type IN('view','progress','complete','meaningful','sampled','quick_skip') ORDER BY i.id LIMIT 1000`
	if e := tx.Raw(q, tenant, from).Scan(&rows).Error; e != nil {
		return e
	}
	if len(rows) == 0 {
		return nil
	}
	subjects := []string{}
	for _, r := range rows {
		if r.UserID != nil {
			subjects = append(subjects, "account:"+r.UserID.String())
		}
	}
	barriers, e := loadSubjectBarriers(tx, subjects)
	if e != nil {
		return e
	}
	// Fetch only segments touched by this bounded business-event batch.
	tuples := [][]any{}
	for _, r := range rows {
		var m ConsumptionObservation
		_ = json.Unmarshal(r.Metadata, &m)
		subject := "public"
		if r.UserID != nil {
			subject = "account:" + r.UserID.String()
		}
		subject, cleared := barriers.Resolve(subject, m.ObservedAt)
		if cleared {
			continue
		}
		ids := []string{r.ContentItemID.String()}
		if r.ParentID != nil {
			ids = append(ids, *r.ParentID)
		}
		if m.Semantics == "story_open_v1" && r.CanonicalStoryID != nil {
			ids = []string{*r.CanonicalStoryID}
			m.Segment = ""
		}
		for _, id := range ids {
			tuples = append(tuples, []any{id, subject, m.Segment, m.Client})
		}
	}
	if len(tuples) == 0 {
		tuples = append(tuples, []any{"", "", "", ""})
	}
	var old []Weight
	if e := tx.Where("tenant_id=? AND month>=date_trunc('month',?::timestamptz) AND (content_id,subject,segment,client) IN ?", tenant, from, tuples).Limit(100001).Find(&old).Error; e != nil {
		return e
	}
	if len(old) > 100000 {
		return fmt.Errorf("consumption working set exceeds bounded release capacity")
	}
	mediaTotals := map[mediaSegmentKey]int64{}
	known := map[weightKey]Weight{}
	for _, w := range old {
		known[weightIdentity(w)] = w
		if w.Kind == "media_seconds" {
			mediaTotals[mediaSegment(w)] += w.Value
		}
	}
	changed := map[weightKey]Weight{}
	usage := []Usage{}
	receipts := make([]interactionReceipt, 0, len(rows))
	for _, row := range rows {
		var m ConsumptionObservation
		_ = json.Unmarshal(row.Metadata, &m)
		status := "unsupported_observation"
		at := m.ObservedAt
		if (m.Client == "mobile" || m.Client == "web_completion") && m.Version == 1 && !at.IsZero() && !at.After(row.CreatedAt.Add(5*time.Minute)) && !at.Before(from) {
			subject := "public"
			if row.UserID != nil {
				subject = "account:" + row.UserID.String()
			}
			subject, cleared := barriers.Resolve(subject, at)
			if cleared {
				status = "lifecycle_removed"
			} else {
				month := time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC)
				ids := []string{row.ContentItemID.String()}
				kind, segment, operation := "", m.Segment, "playback"
				value := int64(0)
				if m.Semantics == "media_position_seconds_v1" && m.Played >= 0 && m.Played <= 86400 {
					if _, e := uuid.Parse(segment); e == nil {
						kind, value = "media_seconds", int64(m.Played)
						if row.ParentID != nil {
							ids = append(ids, *row.ParentID)
						}
					}
				}
				if m.Semantics == "story_open_v1" && row.Type == models.InteractionTypeView && row.CanonicalStoryID != nil && m.StoryID == *row.CanonicalStoryID {
					kind, value, segment, ids, operation = "news_opens", 1, "", []string{m.StoryID}, "news_open"
				}
				if kind == "news_opens" {
					// Public opens have no distinct-person denominator. Account reopens also
					// remain serving activity, independently of their one monthly prep weight.
					usage = append(usage, Usage{TenantID: tenant, Day: at.UTC().Truncate(24 * time.Hour), Subject: subject, Operation: operation, Unit: "news_open_events", Variant: "observed:" + m.Client, Value: 1})
					if subject == "public" {
						kind, status = "", "public_open_count_only"
					} else if barrier := barriers[subject]; barrier.ClearedAt != nil && !barrier.ClearedAt.Before(month) {
						kind, status = "", "distinctness_unavailable_after_clear"
					}
				}
				if kind != "" {
					for index, id := range ids {
						w := Weight{TenantID: tenant, Month: month, ContentID: id, Subject: subject, Segment: segment, Kind: kind, Client: m.Client, Value: value, ObservedAt: at}
						key := weightIdentity(w)
						prior, found := known[key]
						delta := value - prior.Value
						if kind == "media_seconds" {
							group := mediaSegment(w)
							delta = value - mediaTotals[group]
							if delta < 0 {
								delta = 0
							}
							w.Value = prior.Value + delta
							mediaTotals[group] += delta
						}
						if found {
							if prior.Value > w.Value {
								w.Value = prior.Value
							}
							if kind == "news_opens" {
								if prior.ObservedAt.After(w.ObservedAt) {
									w.ObservedAt = prior.ObservedAt
								}
							} else if prior.ObservedAt.Before(w.ObservedAt) {
								w.ObservedAt = prior.ObservedAt
							}
						}
						if !found || w.Value != prior.Value || !w.ObservedAt.Equal(prior.ObservedAt) {
							known[key], changed[key] = w, w
						}
						if index == 0 && delta > 0 {
							usage = append(usage, Usage{TenantID: tenant, Day: at.UTC().Truncate(24 * time.Hour), Subject: subject, Operation: operation, Unit: kind, Variant: "observed:" + m.Client, Value: delta})
						}
					}
					status = "folded"
				}
			}
		}
		receipts = append(receipts, interactionReceipt{row.PublicID, tenant, row.CreatedAt, status})
	}
	if len(changed) > 0 {
		weights := make([]Weight, 0, len(changed))
		for _, w := range changed {
			weights = append(weights, w)
		}
		if e := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "month"}, {Name: "content_id"}, {Name: "subject"}, {Name: "segment"}, {Name: "kind"}, {Name: "client"}}, DoUpdates: clause.AssignmentColumns([]string{"value", "observed_at"})}).CreateInBatches(weights, 500).Error; e != nil {
			return e
		}
	}
	if e := addUsage(tx, usage); e != nil {
		return e
	}
	return tx.Table("economics_interaction_receipts").CreateInBatches(receipts, 500).Error
}
