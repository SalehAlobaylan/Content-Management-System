package controllers

import (
	"content-management-system/src/models"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type integrityProbeTarget struct {
	ID, Role, URL, PlaybackType, Variant string
	Priority                             bool
}
type integrityPodsPage struct {
	Cursor *string    `json:"cursor"`
	Items  []PodsItem `json:"items"`
}
type integrityNewsStory struct {
	StoryID string        `json:"story_id"`
	Title   string        `json:"title"`
	Label   string        `json:"label"`
	Source  string        `json:"source_name"`
	Members []StoryMember `json:"members"`
}
type integrityNewsSlide struct {
	Featured integrityNewsStory   `json:"featured"`
	Related  []integrityNewsStory `json:"related"`
}
type integrityNewsPage struct {
	Cursor *string              `json:"cursor"`
	Slides []integrityNewsSlide `json:"slides"`
}

func integrityReadPage(ctx context.Context, client *http.Client, path string, payload interface{}) (http.Header, int, time.Duration, error) {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedIntegritySelfURL()+path, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	req.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
	resp, err := client.Do(req)
	elapsed := time.Since(started)
	if err != nil {
		return nil, 0, elapsed, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.Header, resp.StatusCode, elapsed, fmt.Errorf("http_%d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (2<<20)+1))
	if err != nil {
		return resp.Header, resp.StatusCode, time.Since(started), err
	}
	if len(raw) > 2<<20 {
		return resp.Header, resp.StatusCode, elapsed, fmt.Errorf("invalid_page_oversize")
	}
	err = json.Unmarshal(raw, payload)
	return resp.Header, resp.StatusCode, time.Since(started), err
}
func (c *integrityCollector) edgeResult(key, feed, variant, status, target string, n int, evidence interface{}) {
	severity := "major"
	axis := "consumer"
	for _, check := range feedIntegrityChecks {
		if check.Key == key {
			severity = check.Severity
			axis = check.Axis
		}
	}
	c.add(key, "edge", feed, variant, axis, severity, status, "page", target, n, evidence)
}
func (c *integrityCollector) unavailableEdge(feed, variant, pageRef string, err error, totalCanceled bool, status int, elapsed time.Duration, latencyBudgetMS int) {
	outcome := "violation"
	reason := "request_failed"
	var netErr net.Error
	timedOut := errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout())
	if totalCanceled {
		outcome = "check_error"
		reason = "run_canceled"
	} else if timedOut {
		// A timeout proves the latency budget was missed. It does not prove
		// that the endpoint returned an error or that every feed item is broken.
		outcome = "check_error"
		reason = "response_timeout"
	} else if status > 0 {
		reason = "invalid_response"
	}
	c.edgeResult("edge_"+feed+"_http", feed, variant, outcome, variant, 1, map[string]interface{}{"error": safeIntegrityError(err), "status": status, "reason": reason, "elapsed_ms": elapsed.Milliseconds(), "latency_budget_ms": latencyBudgetMS, "deadline_ms": latencyBudgetMS + 1000})
	for _, o := range c.observations {
		if feedIntegrityCheckLane(o.CheckKey) == "edge" && o.Feed == feed && o.Variant == variant && o.CheckKey != "edge_"+feed+"_http" && !(timedOut && !totalCanceled && o.CheckKey == "edge_"+feed+"_latency") {
			c.edgeResult(o.CheckKey, feed, variant, "skipped:page_unavailable", variant, 0, nil)
		}
	}
	if timedOut && !totalCanceled && elapsed > time.Duration(latencyBudgetMS)*time.Millisecond {
		c.predicate("edge_"+feed+"_latency", variant, "page", pageRef, true, map[string]interface{}{"reason": "response_timeout", "latency_at_least_ms": elapsed.Milliseconds(), "budget_ms": latencyBudgetMS})
	}
}
func integrityNewsDiagnostics(h http.Header, window string) (newsServeMeta, error) {
	meta := newsServeMeta{Source: h.Get("X-Wahb-Feed-Source"), Window: h.Get("X-Wahb-Snapshot-Window")}
	age, err := strconv.ParseInt(h.Get("X-Wahb-Snapshot-Age-Ms"), 10, 64)
	if err != nil || age < 0 {
		return meta, fmt.Errorf("invalid_snapshot_age")
	}
	meta.SnapshotAge = time.Duration(age) * time.Millisecond
	meta.SnapshotBuiltAt, err = time.Parse(time.RFC3339Nano, h.Get("X-Wahb-Snapshot-Built-At"))
	if err != nil {
		return meta, fmt.Errorf("invalid_snapshot_timestamp")
	}
	meta.SnapshotDirty, err = strconv.ParseBool(h.Get("X-Wahb-Snapshot-Dirty"))
	if err != nil || meta.Window != window || (meta.Source != "live" && meta.Source != "cache") {
		return meta, fmt.Errorf("invalid_snapshot_diagnostics")
	}
	if meta.Source == "cache" && (meta.SnapshotBuiltAt.IsZero() || meta.SnapshotBuiltAt.After(time.Now().Add(time.Second))) {
		return meta, fmt.Errorf("invalid_cache_timestamp")
	}
	return meta, nil
}
func collectFeedIntegrityEdge(ctx context.Context, db *gorm.DB, policy models.FeedIntegrityPolicy, tier string, c *integrityCollector) []integrityProbeTarget {
	db = db.WithContext(ctx)
	var redundancyPolicy models.RedundancyPolicy
	redundancyErr := db.Where("tenant_id=?", policy.TenantID).First(&redundancyPolicy).Error
	if errors.Is(redundancyErr, gorm.ErrRecordNotFound) {
		redundancyErr = nil
	}
	collapseActive := redundancyPolicy.Enabled && redundancyPolicy.CollapseEnabled
	targets := []integrityProbeTarget{}
	for _, minutes := range []int{0, 5, 10, 15, 20, 30, 40} {
		variant := "default"
		path := "/api/v1/feed/pods?limit=20"
		pages := policy.EdgePagesPerFeed
		if minutes > 0 {
			variant = fmt.Sprintf("duration:%dm", minutes)
			path += fmt.Sprintf("&duration=%d", minutes)
			pages = 1
		}
		var corpus int64
		q := applyDurationPreference(podsEligibleMediaQuery(db, policy.TenantID, supportsAtomizedPodsSchema(db)), minutes)
		corpusErr := q.Count(&corpus).Error
		client := &http.Client{Timeout: time.Duration(policy.PodsLatencyBudgetMS+1000) * time.Millisecond}
		seen := map[uuid.UUID]bool{}
		families := map[uint]uuid.UUID{}
		cursors := map[string]bool{}
		cursor := ""
		for page := 0; page < pages; page++ {
			requestPath := path
			if cursor != "" {
				requestPath += "&cursor=" + url.QueryEscape(cursor)
			}
			var payload integrityPodsPage
			_, status, elapsed, err := integrityReadPage(ctx, client, requestPath, &payload)
			if err != nil || payload.Items == nil {
				if err == nil {
					err = fmt.Errorf("missing_items")
				}
				c.unavailableEdge("pods", variant, fmt.Sprintf("page:%d", page+1), err, ctx.Err() != nil, status, elapsed, policy.PodsLatencyBudgetMS)
				break
			}
			pageRef := fmt.Sprintf("page:%d", page+1)
			c.edgeResult("edge_pods_http", "pods", variant, "ok", variant, 1, nil)
			c.predicate("edge_pods_latency", variant, "page", pageRef, elapsed > time.Duration(policy.PodsLatencyBudgetMS)*time.Millisecond, map[string]interface{}{"latency_ms": elapsed.Milliseconds()})
			c.add("edge_pods_page_success", "edge", "pods", variant, "consumer", "info", "ok", "page", pageRef, len(payload.Items), nil)
			if page == 0 {
				state := "ok"
				if corpusErr != nil {
					state = "check_error"
				} else if corpus < int64(max(policy.ExpectedMinPodsUnits, 1)) {
					state = "not_ready"
				} else if len(payload.Items) == 0 {
					state = "violation"
				}
				c.edgeResult("edge_pods_empty", "pods", variant, state, pageRef, 1, map[string]interface{}{"eligible": corpus, "minimum": policy.ExpectedMinPodsUnits, "error": safeIntegrityError(corpusErr)})
				state = "ok"
				if corpusErr != nil {
					state = "check_error"
				} else if corpus < int64(max(policy.ExpectedMinPodsUnits, 1)) {
					state = "not_ready"
				} else if len(payload.Items) < min(20, int(corpus)) {
					state = "violation"
				}
				c.edgeResult("edge_pods_fill", "pods", variant, state, pageRef, 1, map[string]interface{}{"eligible": corpus, "served": len(payload.Items), "target": min(20, int(corpus))})
			}
			ids := make([]uuid.UUID, 0, len(payload.Items))
			for _, item := range payload.Items {
				ids = append(ids, item.ID)
			}
			type familyRow struct {
				ContentItemID uuid.UUID
				FamilyID      uint
			}
			var rows []familyRow
			familyErr := redundancyErr
			if familyErr == nil && collapseActive {
				familyErr = db.Table("redundancy_family_members m").Select("m.content_item_id,m.family_id").Joins("JOIN redundancy_families f ON f.id=m.family_id").Where("m.content_item_id IN ? AND m.ended_at IS NULL AND f.status='active' AND f.tenant_id=?", ids, policy.TenantID).Scan(&rows).Error
			}
			familyMap := map[uuid.UUID]uint{}
			for _, row := range rows {
				familyMap[row.ContentItemID] = row.FamilyID
			}
			for _, item := range payload.Items {
				id := item.ID.String()
				playback := item.MediaURL
				if item.PlaybackURL != nil && *item.PlaybackURL != "" {
					playback = *item.PlaybackURL
				}
				kind := ""
				if item.PlaybackType != nil {
					kind = *item.PlaybackType
				}
				c.predicate("edge_pods_dup", variant, "content_item", id, seen[item.ID], nil)
				seen[item.ID] = true
				family := familyMap[item.ID]
				badFamily := false
				if family > 0 {
					prior, exists := families[family]
					badFamily = exists && prior != item.ID
					families[family] = item.ID
				}
				if familyErr != nil {
					c.add("edge_pods_family_dup", "edge", "pods", variant, "consumer", "major", "check_error", "content_item", id, 1, nil)
				} else if !collapseActive {
					c.add("edge_pods_family_dup", "edge", "pods", variant, "consumer", "major", "skipped:collapse_disabled", "content_item", id, 0, nil)
				} else {
					c.predicate("edge_pods_family_dup", variant, "content_item", id, badFamily, map[string]interface{}{"family_id": family})
				}
				c.predicate("edge_pods_required_metadata", variant, "content_item", id, item.ID == uuid.Nil || (item.Type != "VIDEO" && item.Type != "PODCAST") || strings.TrimSpace(item.Title) == "" || item.DurationSec <= 0 || playback == "", nil)
				c.predicate("edge_pods_bounds_served", variant, "content_item", id, item.DurationSec < podsMinDurationSec || item.DurationSec > podsHardMaxDurationSec, map[string]interface{}{"duration_sec": item.DurationSec})
				c.predicate("edge_pods_playback_fields", variant, "content_item", id, kind != "" && kind != "hls" && kind != "mp4" && kind != "audio", map[string]interface{}{"playback_type": kind})
				c.predicate("edge_pods_status_served", variant, "content_item", id, item.IsArchived, nil)
				if minutes > 0 {
					lo, hi := podsDurationBounds(minutes)
					c.predicate("edge_pods_duration_match", variant, "content_item", id, item.DurationSec < lo || item.DurationSec > hi, nil)
				}
				if playback != "" {
					targets = append(targets, integrityProbeTarget{ID: id, Role: "primary", URL: playback, PlaybackType: kind, Variant: "default"})
				}
				if item.FallbackPlaybackURL != nil && *item.FallbackPlaybackURL != "" {
					fk := ""
					if item.FallbackPlaybackType != nil {
						fk = *item.FallbackPlaybackType
					}
					targets = append(targets, integrityProbeTarget{ID: id, Role: "fallback", URL: *item.FallbackPlaybackURL, PlaybackType: fk, Variant: "default"})
				}
				if item.ThumbnailURL != "" {
					targets = append(targets, integrityProbeTarget{ID: id, Role: "artwork", URL: item.ThumbnailURL, Variant: "default"})
				}
			}
			loop := payload.Cursor != nil && *payload.Cursor != "" && (cursors[*payload.Cursor] || *payload.Cursor == cursor)
			c.predicate("edge_pods_cursor", variant, "page", pageRef, loop, nil)
			if loop || payload.Cursor == nil || *payload.Cursor == "" {
				break
			}
			cursor = *payload.Cursor
			cursors[cursor] = true
		}
	}
	windows := []string{"today"}
	if tier == "deep" {
		windows = append(windows, "week", "month")
	}
	var ranking models.RankingConfig
	rankingErr := db.Where("tenant_id=?", policy.TenantID).First(&ranking).Error
	if rankingErr == gorm.ErrRecordNotFound {
		ranking = models.DefaultRankingConfig(policy.TenantID)
		rankingErr = nil
	}
	for _, window := range windows {
		variant := "window:" + window
		cursor := ""
		seen := map[string]bool{}
		related := map[string]bool{}
		cursors := map[string]bool{}
		corpus, corpusErr := integrityNewsCorpus(db, policy.TenantID, window)
		client := &http.Client{Timeout: time.Duration(policy.NewsLatencyBudgetMS+1000) * time.Millisecond}
		for page := 0; page < policy.EdgePagesPerFeed; page++ {
			path := "/api/v1/feed/news?limit=10&window=" + window
			if cursor != "" {
				path += "&cursor=" + url.QueryEscape(cursor)
			}
			var payload integrityNewsPage
			headers, status, elapsed, err := integrityReadPage(ctx, client, path, &payload)
			if err != nil || payload.Slides == nil {
				if err == nil {
					err = fmt.Errorf("missing_slides")
				}
				c.unavailableEdge("news", variant, fmt.Sprintf("page:%d", page+1), err, ctx.Err() != nil, status, elapsed, policy.NewsLatencyBudgetMS)
				break
			}
			pageRef := fmt.Sprintf("page:%d", page+1)
			c.edgeResult("edge_news_http", "news", variant, "ok", variant, 1, nil)
			c.predicate("edge_news_latency", variant, "page", pageRef, elapsed > time.Duration(policy.NewsLatencyBudgetMS)*time.Millisecond, map[string]interface{}{"latency_ms": elapsed.Milliseconds()})
			diagnostics, diagnosticErr := integrityNewsDiagnostics(headers, window)
			if diagnosticErr != nil || rankingErr != nil {
				c.edgeResult("edge_news_cache_stale", "news", variant, "check_error", pageRef, 1, map[string]interface{}{"error": safeIntegrityError(diagnosticErr)})
			} else {
				ceiling := newsSnapshotTTL
				dirtyBad := diagnostics.SnapshotDirty
				if ranking.NewsFeedMode == "cached_only" {
					ceiling = newsSnapshotMaxStale
					dirtyBad = false
				}
				bad := diagnostics.Source == "cache" && (dirtyBad || diagnostics.SnapshotAge > ceiling)
				c.edgeResult("edge_news_cache_stale", "news", variant, map[bool]string{true: "violation", false: "ok"}[bad], pageRef, 1, diagnostics)
				c.add("edge_news_page_success", "edge", "news", variant, "consumer", "info", "ok", "page", pageRef, len(payload.Slides), map[string]interface{}{"diagnostics": diagnostics, "mode": ranking.NewsFeedMode})
			}
			if page == 0 {
				state := "ok"
				if corpusErr != nil {
					state = "check_error"
				} else if corpus < int64(max(policy.ExpectedMinNewsSlides, 1)) {
					state = "not_ready"
				} else if len(payload.Slides) == 0 {
					state = "violation"
				}
				c.edgeResult("edge_news_empty", "news", variant, state, pageRef, 1, map[string]interface{}{"eligible": corpus, "error": safeIntegrityError(corpusErr)})
			}
			pageFeatured := map[string]bool{}
			for _, slide := range payload.Slides {
				pageFeatured[slide.Featured.StoryID] = true
			}
			sources := map[string]bool{}
			for _, slide := range payload.Slides {
				id := slide.Featured.StoryID
				c.predicate("edge_news_dup", variant, "story", id, seen[id], nil)
				seen[id] = true
				parsedID, idErr := uuid.Parse(id)
				shape := idErr != nil || parsedID == uuid.Nil || (slide.Featured.Title == "" && slide.Featured.Label == "") || len(slide.Featured.Members) == 0 || len(slide.Related) > 3
				for _, member := range slide.Featured.Members {
					if member.ID == uuid.Nil || member.Type != "NEWS" {
						shape = true
					}
				}
				for _, r := range slide.Related {
					relatedID, relatedErr := uuid.Parse(r.StoryID)
					if relatedErr != nil || relatedID == uuid.Nil || r.StoryID == id || pageFeatured[r.StoryID] || related[r.StoryID] {
						shape = true
					}
					related[r.StoryID] = true
				}
				c.predicate("edge_news_shape", variant, "story", id, shape, nil)
				c.predicate("edge_news_thin", variant, "story", id, len(slide.Featured.Members) == 1 && len(slide.Related) == 0, nil)
				if slide.Featured.Source != "" {
					sources[slide.Featured.Source] = true
				}
			}
			c.predicate("edge_news_source_monopoly", variant, "page", pageRef, len(payload.Slides) >= 4 && len(sources) <= 1, map[string]interface{}{"sources": len(sources)})
			loop := payload.Cursor != nil && *payload.Cursor != "" && (cursors[*payload.Cursor] || *payload.Cursor == cursor)
			c.predicate("edge_news_cursor", variant, "page", pageRef, loop, nil)
			if loop || payload.Cursor == nil || *payload.Cursor == "" {
				break
			}
			cursor = *payload.Cursor
			cursors[cursor] = true
		}
	}
	for _, o := range c.observations {
		if feedIntegrityCheckLane(o.CheckKey) == "edge" && o.Outcome == "skipped:not_evaluated" {
			o.Outcome = "skipped:not_applicable"
			o.ObservedAt = time.Now().UTC()
		}
	}
	return targets
}
