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
// TrustMaxReversalPct. Only positively identified human decisions count; one
// explicit human reversal locks the category off regardless of percentage.

type studioReasonCodeTrust struct {
	Code        string  `json:"code"`
	Decisions   int     `json:"decisions"`
	Approvals   int     `json:"approvals"`
	Rejections  int     `json:"rejections"`
	ApprovePct  float64 `json:"approve_pct"`
	ReversalPct float64 `json:"reversal_pct"`
	Reversals   int     `json:"reversals"`
	Earned      bool    `json:"earned"`
	LockedOff   bool    `json:"locked_off"`
}

// computeStudioReasonCodeTrust tallies human approve/reject decisions on chapters
// carrying the given primary code, keyed on one terminal action per chapter.
// Historical audit rows are recalculated with this deterministic latest-action
// rule; no historical row is rewritten or guessed at migration time.
func computeStudioReasonCodeTrust(db *gorm.DB, tenantID, code string, policy models.MediaStudioAutopilotPolicy) studioReasonCodeTrust {
	trust := studioReasonCodeTrust{Code: code}

	type row struct {
		Action string
		N      int64
	}
	var rows []row
	_ = db.Raw(`
		WITH human_terminal_decisions AS (
			SELECT DISTINCT ON ((a.payload->>'chapter_id')::uuid)
				(a.payload->>'chapter_id')::uuid AS chapter_id,
				a.action
			FROM audit_logs a
			WHERE a.tenant_id = ?
			  AND NULLIF(TRIM(a.user_id), '') IS NOT NULL
			  AND a.action IN ('media_studio.atomized_chapter_approved', 'media_studio.atomized_chapter_rejected')
			ORDER BY (a.payload->>'chapter_id')::uuid, a.created_at DESC, a.id DESC
		)
		SELECT d.action, COUNT(*) AS n
		FROM human_terminal_decisions d
		JOIN chapters ch ON ch.public_id = d.chapter_id AND ch.tenant_id = ?
		WHERE ch.needs_review_code = ?
		GROUP BY d.action`, tenantID, tenantID, code).Scan(&rows).Error

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
	trust.Reversals, trust.ReversalPct = studioReasonCodeReversals(db, tenantID, code)

	trust.Earned = trust.Decisions >= policy.TrustMinDecisions &&
		trust.ApprovePct >= float64(policy.TrustMinApprovePct) &&
		trust.Reversals == 0 &&
		trust.ReversalPct < float64(policy.TrustMaxReversalPct)
	return trust
}

// studioReasonCodeReversals returns the number and percentage of autopilot
// publications for this code that a human explicitly reversed. System-principal
// actions (the lead, intelligence decay) never count.
func studioReasonCodeReversals(db *gorm.DB, tenantID, code string) (int, float64) {
	var res struct {
		Total    int64
		Reversed int64
	}
	// ap = distinct chapters this autopilot auto-published for this code, with the
	// publish time; reversed = those later rejected or archived by a positively
	// identified human. Empty-user system rows (including all autopilots) never
	// qualify as editorial reversals.
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
			     AND NULLIF(TRIM(r.user_id), '') IS NOT NULL
			     AND r.action IN ('media_studio.atomized_chapter_rejected', 'media_studio.atomized_chapter_archived')
		             AND (r.payload->>'chapter_id')::uuid = ap.chapter_id
		             AND r.created_at > ap.published_at
		       )) AS reversed
		FROM ap`,
		tenantID, models.StudioAuditPrincipal, code,
		tenantID,
	).Scan(&res).Error
	if err != nil || res.Total == 0 {
		return 0, 0
	}
	return int(res.Reversed), float64(res.Reversed) / float64(res.Total) * 100
}

// studioReasonCodeTrustEarned is the runner's gate check.
func studioReasonCodeTrustEarned(db *gorm.DB, tenantID, code string, policy models.MediaStudioAutopilotPolicy) bool {
	return computeStudioReasonCodeTrust(db, tenantID, code, policy).Earned
}
