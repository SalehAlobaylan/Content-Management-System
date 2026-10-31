package controllers

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"gorm.io/gorm"
)

// Each query describes an owning system's current unresolved units. Results
// are bounded explicitly; overflow is a partial adapter, never an empty inbox.
type opsAttentionRow struct {
	TenantID       string    `json:"tenant_id"`
	NativeID       string    `json:"native_id"`
	Severity       string    `json:"severity"`
	Title          string    `json:"title"`
	Detail         string    `json:"detail"`
	Fingerprint    string    `json:"fingerprint"`
	ClientPlatform string    `json:"client_platform"`
	Count          int       `json:"count"`
	FirstSeen      time.Time `json:"first_seen"`
}

type opsAttentionQuerySpec struct {
	Kind, SQL string
	Args      []any
}

// Batch a member's independent native inbox sources into one network roundtrip.
// The JSON envelope keeps each source's optional columns separate and preserves
// its native data types; only the normalized read model leaves this adapter.
func opsAttentionQueries(db *gorm.DB, member opsFleetMember, queries []opsAttentionQuerySpec) ([]opsAttentionItem, error) {
	if len(queries) == 0 {
		return []opsAttentionItem{}, nil
	}
	parts := []string{}
	args := []any{}
	for _, q := range queries {
		parts = append(parts, "(SELECT ?::text AS kind,to_jsonb(native) AS body FROM (SELECT * FROM ("+q.SQL+") candidate ORDER BY first_seen,native_id LIMIT 501) native)")
		args = append(args, q.Kind)
		args = append(args, q.Args...)
	}
	var rows []struct {
		Kind string
		Body []byte
	}
	if err := db.Raw("SELECT * FROM ("+strings.Join(parts, " UNION ALL ")+") ops_member_attention", args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	items := make([]opsAttentionItem, 0, len(rows))
	for _, raw := range rows {
		row, err := opsDecodeAttentionRow(raw.Body)
		if err != nil {
			return items, err
		}
		items = append(items, opsNormalizeAttentionRow(member, raw.Kind, row))
	}
	opsSortAttention(items)
	if len(items) > opsReadRowLimit {
		return items[:opsReadRowLimit], fmt.Errorf("attention exceeds %d bounded detail rows; inspect owner cockpit", opsReadRowLimit)
	}
	return items, nil
}
func opsDecodeAttentionRow(body []byte) (opsAttentionRow, error) {
	type alias opsAttentionRow
	row := opsAttentionRow{}
	decoded := struct {
		*alias
		FirstSeen string `json:"first_seen"`
	}{alias: (*alias)(&row)}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return row, err
	}
	if decoded.FirstSeen == "" {
		return row, nil
	}
	at, err := time.Parse(time.RFC3339Nano, decoded.FirstSeen)
	if err != nil {
		at, err = time.ParseInLocation("2006-01-02T15:04:05.999999999", decoded.FirstSeen, time.UTC)
	}
	if err != nil {
		return row, err
	}
	row.FirstSeen = at
	return row, nil
}
func opsNormalizeAttentionRow(member opsFleetMember, kind string, row opsAttentionRow) opsAttentionItem {
	severity := opsNormalizeSeverity(row.Severity)
	count := row.Count
	if count < 1 {
		count = 1
	}
	href := member.CockpitPath
	if row.ClientPlatform != "" {
		u, _ := url.Parse(href)
		q := u.Query()
		q.Set("client", row.ClientPlatform)
		u.RawQuery = q.Encode()
		href = u.String()
	}
	fingerprint := row.Fingerprint
	if fingerprint == "" {
		fingerprint = opsFingerprint(member.Key, kind, row.TenantID, row.NativeID)
	}
	return opsAttentionItem{Key: opsAttentionKey(member.Key, kind, row.TenantID, row.NativeID), Fingerprint: fingerprint, System: member.Key, TenantID: row.TenantID, Kind: kind, Severity: severity, Title: row.Title, Detail: row.Detail, Count: count, FirstSeen: row.FirstSeen, Href: href, ClientPlatform: row.ClientPlatform}
}

func opsAttentionQuery(db *gorm.DB, member opsFleetMember, kind, query string, args ...any) ([]opsAttentionItem, error) {
	return opsAttentionQueries(db, member, []opsAttentionQuerySpec{{Kind: kind, SQL: query, Args: args}})
}

func opsAttentionForMember(db *gorm.DB, m opsFleetMember, now time.Time) ([]opsAttentionItem, error) {
	items := []opsAttentionItem{}
	var firstErr error
	queries := []opsAttentionQuerySpec{}
	add := func(kind, query string, args ...any) {
		queries = append(queries, opsAttentionQuerySpec{Kind: kind, SQL: query, Args: args})
	}
	switch m.Key {
	case "system_health":
		add("episode", `SELECT 'platform' AS tenant_id, public_id::text AS native_id, severity, COALESCE(NULLIF(summary,''),verdict) AS title, root_service AS detail, 1 AS count, first_detected_at AS first_seen FROM system_incident_episodes WHERE status IN ('open','recovering')`)
	case "feed_integrity":
		add("episode", `SELECT tenant_id, public_id::text AS native_id, severity, COALESCE(NULLIF(summary,''),check_key) AS title, feed AS detail, 1 AS count, first_detected_at AS first_seen FROM feed_integrity_episodes WHERE status IN ('open','recovering')`)
		add("approval", `SELECT a.tenant_id,a.public_id::text AS native_id,'major' AS severity,a.action_class AS title,a.reason AS detail,1 AS count,a.created_at AS first_seen FROM feed_integrity_actions a JOIN feed_integrity_episodes e ON e.id=a.episode_id AND e.tenant_id=a.tenant_id WHERE a.outcome='approval_required' AND e.status IN ('open','recovering')`)
		add("exception", `SELECT a.tenant_id,a.action_class AS native_id,'major' AS severity,'Repair breaker open' AS title,a.action_class AS detail,count(*)::int AS count,min(a.created_at) AS first_seen FROM feed_integrity_actions a WHERE a.outcome IN ('tool_failed','verification_failed') AND a.created_at>? AND NOT EXISTS (SELECT 1 FROM audit_logs r WHERE r.tenant_id=a.tenant_id AND r.action='feed_integrity.autopilot.breaker.reset' AND r.target_resource=a.action_class AND r.status='success' AND r.created_at>a.created_at) GROUP BY a.tenant_id,a.action_class HAVING count(*)>=2`, now.Add(-24*time.Hour))
		add("stalled", `SELECT r.tenant_id,'evaluation' AS native_id,'major' AS severity,'Completed feed checks await evaluation' AS title,'Reactive autopilot has not evaluated completed checks' AS detail,count(*)::int AS count,min(r.finished_at) AS first_seen FROM feed_integrity_runs r JOIN feed_integrity_policies p ON p.tenant_id=r.tenant_id WHERE p.autopilot_enabled AND (p.autopilot_paused_until IS NULL OR p.autopilot_paused_until<=?) AND r.status IN ('completed','partial') AND r.autopilot_evaluated_at IS NULL AND r.finished_at<? GROUP BY r.tenant_id`, now, now.Add(-30*time.Minute))
	case "experience":
		add("episode", `SELECT tenant_id,public_id::text AS native_id,severity,COALESCE(NULLIF(summary,''),'Consumer experience incident') AS title,client_platform||' / '||surface AS detail,client_platform,1 AS count,first_seen_at AS first_seen FROM experience_incidents WHERE status IN ('open','recovering')`)
	case "ai_spend":
		add("budget", `SELECT tenant_id,id::text AS native_id,CASE WHEN kind='bounded_stop' THEN 'critical' ELSE 'major' END AS severity,kind AS title,scope AS detail,1 AS count,first_seen_at AS first_seen FROM ai_spend_episodes WHERE status IN ('open','flapping','recovering')`)
	case "workload_governor":
		add("exception", `SELECT tenant_id,system||':'||class AS native_id,'major' AS severity,'Unresolved capacity ownership' AS title,system||' / '||class AS detail,count(*)::int AS count,min(created_at) AS first_seen FROM workload_permits WHERE state='uncertain' OR (state IN ('running','handed_off') AND expires_at<?) GROUP BY tenant_id,system,class`, now)
	case "media_circulation":
		add("approval", `SELECT r.tenant_id,r.public_id::text AS native_id,'major' AS severity,r.action AS title,r.verdict AS detail,1 AS count,r.created_at AS first_seen FROM media_circulation_recommendations r WHERE r.status='pending' AND EXISTS(SELECT 1 FROM media_circulation_actions a WHERE a.tenant_id=r.tenant_id AND a.recommendation_id=r.public_id AND a.status='approval_required')`)
	case "media_studio":
		// Chapter state, not historical action status, defines unresolved review.
		add("approval", `SELECT c.tenant_id,c.public_id::text AS native_id,'major' AS severity,COALESCE(NULLIF(c.title,''),'Chapter awaits editorial review') AS title,COALESCE(NULLIF(c.needs_review_reason,''),'Review the current chapter in Studio') AS detail,1 AS count,c.created_at AS first_seen FROM chapters c WHERE c.status='needs_review'`)
	case "retention":
		add("approval", `SELECT tenant_id,public_id::text AS native_id,'major' AS severity,action_class AS title,guardrail AS detail,1 AS count,created_at AS first_seen FROM retention_actions WHERE outcome='approval_required'`)
	case "preferences":
		add("attention", `SELECT tenant_id,id::text AS native_id,'minor' AS severity,COALESCE(NULLIF(suggested_label_en,''),suggested_slug) AS title,'Topic proposal pending review' AS detail,1 AS count,created_at AS first_seen FROM topic_proposals WHERE status='pending'`)
		add("attention", `SELECT q.tenant_id,'recompute_pressure' AS native_id,'major' AS severity,'Preference recompute queue is backed up' AS title,'Queue exceeds one scheduled pass of the owner policy' AS detail,count(*)::int AS count,min(q.created_at) AS first_seen FROM preference_affinity_recompute_queue q JOIN preference_autopilot_policies p ON p.tenant_id=q.tenant_id GROUP BY q.tenant_id,p.max_users_recompute HAVING count(*)>GREATEST(p.max_users_recompute,1)`)
	case "embedding_lifecycle":
		add("exception", `SELECT tenant_id,'exception:'||id::text AS native_id,'major' AS severity,COALESCE(NULLIF(failure_class,''),'Campaign exception') AS title,surface_key AS detail,1 AS count,created_at AS first_seen FROM embedding_campaign_exceptions WHERE status='open'`)
		add("attention", `SELECT f.tenant_id,f.surface_key||':'||f.check_key AS native_id,CASE WHEN bool_or(f.severity='critical') THEN 'critical' WHEN bool_or(f.severity='major' OR f.status='check_error') THEN 'major' ELSE 'minor' END AS severity,'Embedding audit: '||f.check_key AS title,f.surface_key AS detail,GREATEST(sum(f.count),count(*))::int AS count,min(f.created_at) AS first_seen FROM embedding_lifecycle_findings f WHERE f.run_id=(SELECT r.id FROM embedding_lifecycle_runs r WHERE r.tenant_id=f.tenant_id ORDER BY r.started_at DESC,r.id DESC LIMIT 1) AND f.status IN ('violation','check_error','overflow') GROUP BY f.tenant_id,f.surface_key,f.check_key`)
		add("exception", `SELECT tenant_id,'campaign:'||id::text AS native_id,'major' AS severity,'Embedding campaign '||state AS title,COALESCE(NULLIF(blocked_reason,''),space) AS detail,1 AS count,updated_at AS first_seen FROM embedding_campaigns c WHERE state IN ('paused','blocked','aborted') AND id=(SELECT n.id FROM embedding_campaigns n WHERE n.tenant_id=c.tenant_id AND n.space=c.space ORDER BY n.created_at DESC,n.id DESC LIMIT 1)`)
		add("exception", `SELECT tenant_id,'campaign:'||id::text AS native_id,'major' AS severity,'Embedding campaign progress is stale' AS title,'Progress or lease evidence requires owner review' AS detail,1 AS count,updated_at AS first_seen FROM embedding_campaigns WHERE state IN ('running','verifying') AND updated_at<? AND (lease_expires_at IS NULL OR lease_expires_at<?)`, now.Add(-2*time.Hour), now)
	case "pipeline":
		add("attention", `SELECT ci.tenant_id,'exhausted_failed' AS native_id,'major' AS severity,'Failed items exhausted repair attempts' AS title,'Owner attempt cap requires human review' AS detail,count(*)::int AS count,min(ci.updated_at) AS first_seen FROM content_items ci JOIN pipeline_autopilot_policies p ON p.tenant_id=ci.tenant_id JOIN (SELECT tenant_id,content_item_id,count(*) FILTER(WHERE status='success')+count(*) FILTER(WHERE status='error')/2 AS attempts FROM pipeline_autopilot_actions WHERE status IN ('success','error') GROUP BY tenant_id,content_item_id) a ON a.tenant_id=ci.tenant_id AND a.content_item_id=ci.public_id WHERE ci.status='FAILED' AND a.attempts>=p.max_attempts GROUP BY ci.tenant_id`)
		add("attention", `SELECT tenant_id,source_name AS native_id,'major' AS severity,'Source failures dominate the repair cohort' AS title,source_name AS detail,n::int AS count,first_seen FROM (SELECT tenant_id,COALESCE(NULLIF(source_name,''),source::text,'unknown') AS source_name,count(*) AS n,min(created_at) AS first_seen,sum(count(*)) OVER(PARTITION BY tenant_id) AS total FROM content_items WHERE status='FAILED' AND created_at>=? GROUP BY tenant_id,COALESCE(NULLIF(source_name,''),source::text,'unknown')) s WHERE n>=? AND n*100.0/NULLIF(total,0)>?`, now.Add(-7*24*time.Hour), pipelineSourceBrokenMinFailed, pipelineSourceBrokenPct)
	case "enrichment":
		add("budget", `SELECT tenant_id,'budget_capped' AS native_id,'major' AS severity,'Enrichment STT budget is capped' AS title,'Review transcription budget and missing artifacts' AS detail,1 AS count,started_at AS first_seen FROM (SELECT DISTINCT ON(tenant_id) * FROM enrichment_autopilot_runs ORDER BY tenant_id,started_at DESC,id DESC) r WHERE headline='budget_capped'`)
		add("attention", `SELECT a.tenant_id,a.artifact AS native_id,'major' AS severity,'Enrichment class is not draining' AS title,a.reason AS detail,1 AS count,a.created_at AS first_seen FROM enrichment_autopilot_actions a WHERE a.guardrail='escalate_stuck' AND a.run_id=(SELECT r.id FROM enrichment_autopilot_runs r WHERE r.tenant_id=a.tenant_id ORDER BY started_at DESC,id DESC LIMIT 1)`)
	case "redundancy":
		add("approval", `SELECT tenant_id,public_id::text AS native_id,'minor' AS severity,'Recording identity awaits review' AS title,verdict AS detail,1 AS count,created_at AS first_seen FROM redundancy_pairs WHERE NOT tombstoned AND verdict IN ('probable_duplicate','high_confidence_duplicate')`)
		add("attention", `SELECT f.tenant_id,f.public_id::text AS native_id,'major' AS severity,'Redundancy canonical needs review' AS title,'Canonical is unavailable or weaker than a sibling' AS detail,1 AS count,f.created_at AS first_seen FROM redundancy_families f JOIN content_items c ON c.public_id=f.canonical_content_item_id AND c.tenant_id=f.tenant_id WHERE f.status='active' AND (c.status<>'READY' OR COALESCE(c.playback_url,c.media_url,'')='' OR EXISTS(SELECT 1 FROM redundancy_family_members m JOIN content_items s ON s.public_id=m.content_item_id AND s.tenant_id=m.tenant_id WHERE m.family_id=f.id AND m.ended_at IS NULL AND m.role='redundant' AND s.view_count*3+s.like_count*10+s.share_count*15>c.view_count*3+c.like_count*10+c.share_count*15))`)
	case "news_circulation":
		add("attention", `SELECT tenant_id,'owner_health' AS native_id,'major' AS severity,'News circulation requires attention' AS title,COALESCE(NULLIF(summary,''),'Inspect latest recorded owner health') AS detail,1 AS count,started_at AS first_seen FROM (SELECT DISTINCT ON(tenant_id) * FROM news_autopilot_runs ORDER BY tenant_id,started_at DESC,id DESC) r WHERE health_after->>'state' IN ('attention','degraded','blocked','safety')`)
	case "ranking_intelligence":
		rows, err := opsRankingAttention(db, m, now)
		items = append(items, rows...)
		if err != nil {
			firstErr = err
		}
	case "storage":
		// Storage attention is derived from the same collected owner status.
	case "atomization":
		add("attention", `SELECT tenant_id,'blocked_execution' AS native_id,'major' AS severity,'Atomization execution has blocked work' AS title,'Editorial chapter review is owned by Studio' AS detail,count(*)::int AS count,min(created_at) AS first_seen FROM media_atomization_runs WHERE status IN ('blocked','failed') AND id IN (SELECT DISTINCT ON(tenant_id,parent_content_item_id) id FROM media_atomization_runs ORDER BY tenant_id,parent_content_item_id,created_at DESC,id DESC) GROUP BY tenant_id`)
	case "supply_continuity":
		add("episode", `SELECT tenant_id,public_id::text AS native_id,severity,summary AS title,first_failed_boundary||' / '||owner AS detail,1 AS count,first_seen_at AS first_seen,evidence_digest AS fingerprint FROM media_supply_episodes WHERE state IN ('open','recovering')`)
		add("approval", `SELECT tenant_id,public_id::text AS native_id,'major' AS severity,action_key AS title,'Supply action awaits an explicit owner approval' AS detail,1 AS count,created_at AS first_seen FROM media_supply_action_requests WHERE state='awaiting_approval'`)
		add("exception", `SELECT tenant_id,public_id::text AS native_id,'major' AS severity,'Supply effect needs reconciliation' AS title,COALESCE(NULLIF(failure_class,''),action_key) AS detail,1 AS count,updated_at AS first_seen FROM media_supply_action_requests WHERE state='uncertain'`)
	}
	rows, err := opsAttentionQueries(db, m, queries)
	items = append(items, rows...)
	if firstErr == nil {
		firstErr = err
	}
	return items, firstErr
}

func opsFilterAttention(items []opsAttentionItem, system, kind, severity, state string, includeAcked, includeSnoozed bool) []opsAttentionItem {
	out := []opsAttentionItem{}
	for _, item := range items {
		if system != "" && item.System != system || kind != "" && item.Kind != kind || severity != "" && item.Severity != severity {
			continue
		}
		if !includeAcked && item.State == "acked" {
			continue
		}
		if !includeSnoozed && item.Snoozed {
			continue
		}
		if state != "" && state != "all" {
			actual := item.State
			if actual == "" {
				actual = "open"
			}
			if actual != state {
				continue
			}
		}
		out = append(out, item)
	}
	return out
}

func opsAttentionKey(system, kind, tenant, native string) string {
	key := system + ":" + kind + ":" + tenant + ":" + native
	if len(key) > 255 {
		return system + ":" + kind + ":" + tenant + ":" + opsFingerprint(native)
	}
	return key
}
