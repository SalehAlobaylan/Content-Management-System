package controllers

import (
	"content-management-system/src/models"

	"gorm.io/gorm"
)

// Media Studio Clearance Autopilot — trust gate (H5, S15).
//
// A review-reason code earns auto-publish only after enough human decisions on
// that category prove the call is rubber-stamping: ≥ TrustMinDecisions with
// approve rate ≥ TrustMinApprovePct and post-publish reversal rate below
// TrustMaxReversalPct. Only explicit human decisions count — autopilot's own
// clears (principal StudioAuditPrincipal) are excluded (S15). Reversal accounting
// is refined in Slice 3; at launch no auto-publish exists so reversal is 0.

type studioReasonCodeTrust struct {
	Code        string  `json:"code"`
	Decisions   int     `json:"decisions"`
	Approvals   int     `json:"approvals"`
	Rejections  int     `json:"rejections"`
	ApprovePct  float64 `json:"approve_pct"`
	ReversalPct float64 `json:"reversal_pct"`
	Earned      bool    `json:"earned"`
	LockedOff   bool    `json:"locked_off"`
}

// computeStudioReasonCodeTrust tallies human approve/reject decisions on chapters
// carrying the given primary code, keyed on the audit stream (S15: human
// principal only).
func computeStudioReasonCodeTrust(db *gorm.DB, tenantID, code string, policy models.MediaStudioAutopilotPolicy) studioReasonCodeTrust {
	trust := studioReasonCodeTrust{Code: code}

	type row struct {
		Action string
		N      int64
	}
	var rows []row
	_ = db.Table("audit_logs a").
		Select("a.action AS action, COUNT(*) AS n").
		Joins("JOIN chapters ch ON ch.public_id = (a.payload->>'chapter_id')::uuid").
		Where("a.tenant_id = ? AND a.user_email <> ? AND a.action IN ? AND ch.needs_review_code = ?",
			tenantID, models.StudioAuditPrincipal,
			[]string{"media_studio.atomized_chapter_approved", "media_studio.atomized_chapter_rejected"},
			code).
		Group("a.action").
		Scan(&rows).Error

	for _, rr := range rows {
		switch rr.Action {
		case "media_studio.atomized_chapter_approved":
			trust.Approvals = int(rr.N)
		case "media_studio.atomized_chapter_rejected":
			trust.Rejections = int(rr.N)
		}
	}
	trust.Decisions = trust.Approvals + trust.Rejections
	if trust.Decisions > 0 {
		trust.ApprovePct = float64(trust.Approvals) / float64(trust.Decisions) * 100
	}
	trust.ReversalPct = studioReasonCodeReversalPct(db, tenantID, code)

	trust.Earned = trust.Decisions >= policy.TrustMinDecisions &&
		trust.ApprovePct >= float64(policy.TrustMinApprovePct) &&
		trust.ReversalPct < float64(policy.TrustMaxReversalPct)
	return trust
}

// studioReasonCodeReversalPct is the share of autopilot auto-publishes for this
// code that a human later EXPLICITLY reversed (S15) — a human-principal reject
// audit on the same chapter after the autopilot approved it. System-principal
// actions (the lead, intelligence decay) never count. Returns a percentage.
func studioReasonCodeReversalPct(db *gorm.DB, tenantID, code string) float64 {
	var res struct {
		Total    int64
		Reversed int64
	}
	// ap = distinct chapters this autopilot auto-published for this code, with the
	// publish time; reversed = those a human later rejected.
	err := db.Raw(`
		WITH ap AS (
			SELECT (a.payload->>'chapter_id')::uuid AS chapter_id,
			       MIN(a.created_at) AS published_at
			FROM audit_logs a
			JOIN chapters ch ON ch.public_id = (a.payload->>'chapter_id')::uuid
			WHERE a.tenant_id = ?
			  AND a.user_email = ?
			  AND a.action = 'media_studio.atomized_chapter_approved'
			  AND ch.needs_review_code = ?
			GROUP BY 1
		)
		SELECT COUNT(*) AS total,
		       COUNT(*) FILTER (WHERE EXISTS (
		           SELECT 1 FROM audit_logs r
		           WHERE r.tenant_id = ?
		             AND r.user_email <> ?
		             AND r.action = 'media_studio.atomized_chapter_rejected'
		             AND (r.payload->>'chapter_id')::uuid = ap.chapter_id
		             AND r.created_at > ap.published_at
		       )) AS reversed
		FROM ap`,
		tenantID, models.StudioAuditPrincipal, code,
		tenantID, models.StudioAuditPrincipal,
	).Scan(&res).Error
	if err != nil || res.Total == 0 {
		return 0
	}
	return float64(res.Reversed) / float64(res.Total) * 100
}

// studioReasonCodeTrustEarned is the runner's gate check.
func studioReasonCodeTrustEarned(db *gorm.DB, tenantID, code string, policy models.MediaStudioAutopilotPolicy) bool {
	return computeStudioReasonCodeTrust(db, tenantID, code, policy).Earned
}
