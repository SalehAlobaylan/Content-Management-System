package controllers

import (
	"content-management-system/src/models"
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Report only observed versioned attempts until native coverage is qualified.
// Server receipt time limits the scan, and the observed start sets the cohort.
func GetPodsOutcomeEvidence(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	until := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	from := until.AddDate(0, 0, -7)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
	defer cancel()
	db := c.MustGet("db").(*gorm.DB).WithContext(ctx)
	var counts struct {
		Attempts     int64
		Completions  int64
		EvidenceRows int64
	}
	err := db.Raw(`
		WITH evidence AS (
			SELECT i.content_item_id, i.user_id, i.session_id,
				i.metadata->>'economics_segment_id' AS segment,
				i.metadata->>'pods_attempt_started_at' AS started,
				i.type
			FROM user_interactions i
			JOIN content_items c ON c.public_id=i.content_item_id AND c.tenant_id=?
			WHERE c.type IN ('VIDEO','PODCAST')
				AND i.created_at >= ? AND i.created_at < ?
				AND i.metadata->>'economics_client'='mobile'
				AND i.metadata->>'pods_attempt_version'='1'
				AND i.metadata->>'consumption_contract_version'='1'
				AND i.metadata->>'pods_attempt_started_at'>=?
				AND i.metadata->>'pods_attempt_started_at'<?
				AND length(i.metadata->>'economics_segment_id')=36
				AND i.type IN ('progress','complete','meaningful','sampled','quick_skip')
			LIMIT 100001
		), attempts AS (
			SELECT bool_or(type = ?) AS completed
			FROM evidence GROUP BY content_item_id, user_id, session_id, segment, started
		)
		SELECT count(*) AS attempts, count(*) FILTER (WHERE completed) AS completions,
			(SELECT count(*) FROM evidence) AS evidence_rows FROM attempts
	`, p.TenantID, from, until.Add(24*time.Hour), from.Format("2006-01-02T15:04:05.000Z"), until.Format("2006-01-02T15:04:05.000Z"), models.InteractionTypeComplete).Scan(&counts).Error
	if err != nil || counts.EvidenceRows > 100000 {
		c.JSON(503, gin.H{"message": "Playback evidence is unavailable for this window"})
		return
	}
	var sessions struct {
		Started      int64 `json:"started"`
		Closed       int64 `json:"closed"`
		BothFeeds    int64 `json:"both_feeds"`
		ObservedMS   int64 `json:"observed_ms"`
		EvidenceRows int64 `json:"-"`
	}
	sessionErr := db.Raw(`
		WITH evidence AS (
			SELECT session_id, page_load_id, journey_id, client_platform, release,
				event_type, occurred_at, duration_ms, measurements->>'product_session_feed_mask' AS feed_mask
			FROM experience_events
			WHERE tenant_id=? AND received_at>=? AND received_at<?
				AND client_platform IN ('ios','android')
				AND event_type IN ('product_session_started','product_session_closed')
			LIMIT 100001
		), grouped AS (
			SELECT min(occurred_at) FILTER (WHERE event_type='product_session_started') AS started_at,
				count(*) FILTER (WHERE event_type='product_session_closed') AS closes,
				max(duration_ms) AS duration_ms, max(feed_mask) AS feed_mask
			FROM evidence GROUP BY session_id,page_load_id,journey_id,client_platform,release
		)
		SELECT count(*) AS started, count(*) FILTER (WHERE closes=1) AS closed,
			count(*) FILTER (WHERE closes=1 AND feed_mask='3') AS both_feeds,
			COALESCE(sum(duration_ms) FILTER (WHERE closes=1),0) AS observed_ms,
			(SELECT count(*) FROM evidence) AS evidence_rows
		FROM grouped WHERE started_at>=? AND started_at<?
	`, p.TenantID, from, until.Add(24*time.Hour), from, until).Scan(&sessions).Error
	var sessionEvidence any
	if sessionErr == nil && sessions.EvidenceRows <= 100000 {
		sessionEvidence = sessions
	}
	c.JSON(200, gin.H{"data": gin.H{
		"from": from, "until": until, "observed_attempts": counts.Attempts, "observed_completions": counts.Completions,
		"coverage": "unqualified", "sessions": sessionEvidence,
	}})
}
