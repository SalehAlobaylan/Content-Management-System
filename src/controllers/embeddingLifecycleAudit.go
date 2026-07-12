package controllers

import (
	"fmt"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// Embedding & Model Lifecycle System (stage 10) — audit lanes (Slice 2).
//
// Lane A (inventory): exhaustive per-surface counts of current/stale/unstamped.
// Lane B (coherence): mixed-space centroid risk + numeric sanity + consumer
// guard coverage. Both are READ-ONLY. Table/column identifiers come only from
// the compile-time registry, never from input.

// surfaceAudit is the per-surface result rolled into the run.
type surfaceAudit struct {
	Surface        EmbeddingSurface
	ExpectedSpace  expectedSpace
	WithVec        int64  `json:"with_vec"`
	Current        int64  `json:"current"`
	Stale          int64  `json:"stale"`
	Unstamped      int64  `json:"unstamped"`
	Missing        int64  `json:"missing"`
	MixedSpace     int64  `json:"mixed_space"`
	NumericInvalid int64  `json:"numeric_invalid"`
	CheckFailed    bool   `json:"-"`
	Verdict        string `json:"verdict"`
	Note           string `json:"note,omitempty"`
	findings       []models.EmbeddingLifecycleFinding
}

// auditSurface runs Lanes A+B for one surface and returns its rolled verdict.
// Per-surface panic isolation is applied by the caller (family G11).
func auditSurface(db *gorm.DB, runID uint, tenant string, s EmbeddingSurface) surfaceAudit {
	res := surfaceAudit{Surface: s}
	es := currentExpectedSpace(s.Space)
	res.ExpectedSpace = es

	// Inventory counts that never depend on the expected identity.
	var basic struct {
		WithVec int64
		Missing int64
	}
	if err := db.Raw(fmt.Sprintf(
		`SELECT COUNT(*) FILTER (WHERE %[1]s IS NOT NULL) AS with_vec,
		        COUNT(*) FILTER (WHERE %[1]s IS NULL) AS missing
		 FROM %[2]s`, s.VecCol, s.Table)).Scan(&basic).Error; err != nil {
		res.Verdict = models.EmbeddingVerdictCheckError
		res.Note = "inventory query failed: " + err.Error()
		res.findings = append(res.findings, checkErrorFinding(runID, tenant, s.Key, models.EmbeddingCheckStaleInventory, err.Error()))
		return res
	}
	res.WithVec = basic.WithVec
	res.Missing = basic.Missing

	// Without a resolved expected identity we cannot tell current from stale.
	if !es.Fresh(descriptorCacheTTL) {
		res.Verdict = models.EmbeddingVerdictCheckError
		res.Note = "expected space unavailable: " + es.Err
		res.findings = append(res.findings, models.EmbeddingLifecycleFinding{
			RunID: runID, TenantID: tenant, SurfaceKey: s.Key,
			CheckKey: models.EmbeddingCheckSpaceUnavailable, Status: models.EmbeddingFindingCheckError,
			Severity: models.EmbeddingSevMajor, Evidence: jsonEvidence(map[string]any{"error": es.Err, "space": s.Space}),
		})
		return res
	}

	expectedProducer := es.ProducerFor(s.Recipe)

	// Dimension guard: a service reporting a dim that mismatches the column is a
	// structural dim-change — the in-place path is impossible (§3/§7).
	if es.Dimensions > 0 && es.Dimensions != s.Dim {
		res.Verdict = models.EmbeddingVerdictBlocked
		res.Note = fmt.Sprintf("dim change: service reports %d, column is %d", es.Dimensions, s.Dim)
		res.findings = append(res.findings, models.EmbeddingLifecycleFinding{
			RunID: runID, TenantID: tenant, SurfaceKey: s.Key,
			CheckKey: models.EmbeddingCheckDimChangeRequired, Status: models.EmbeddingFindingViolation,
			Severity: models.EmbeddingSevCritical,
			Evidence: jsonEvidence(map[string]any{"service_dim": es.Dimensions, "column_dim": s.Dim}),
		})
		return res
	}

	// Identity-relative counts.
	var counts struct {
		Current   int64
		Stale     int64
		Unstamped int64
	}
	if err := db.Raw(fmt.Sprintf(
		`SELECT
		   COUNT(*) FILTER (WHERE %[1]s = ? AND %[3]s = ?) AS current,
		   COUNT(*) FILTER (WHERE %[1]s IS NOT NULL AND %[3]s IS NOT NULL AND (%[1]s <> ? OR %[3]s <> ?)) AS stale,
		   COUNT(*) FILTER (WHERE %[2]s IS NOT NULL AND (%[3]s IS NULL OR %[1]s IS NULL)) AS unstamped
		 FROM %[4]s`,
		s.ProducerIDCol, s.VecCol, s.SpaceIDCol, s.Table),
		expectedProducer, es.SpaceID, expectedProducer, es.SpaceID).Scan(&counts).Error; err != nil {
		res.Verdict = models.EmbeddingVerdictCheckError
		res.Note = "counts query failed: " + err.Error()
		res.findings = append(res.findings, checkErrorFinding(runID, tenant, s.Key, models.EmbeddingCheckStaleInventory, err.Error()))
		return res
	}
	res.Current = counts.Current
	res.Stale = counts.Stale
	res.Unstamped = counts.Unstamped

	// Lane B — mixed-space centroid coherence (only for centroid surfaces).
	if s.Kind == SurfaceKindCentroid {
		mixed, err := countMixedSpaceCentroids(db, s)
		if err != nil {
			// Coherence failure is a check_error, never a false violation.
			res.findings = append(res.findings, checkErrorFinding(runID, tenant, s.Key, models.EmbeddingCheckMixedSpace, err.Error()))
			res.CheckFailed = true
		} else {
			res.MixedSpace = mixed
			if mixed > 0 {
				res.findings = append(res.findings, models.EmbeddingLifecycleFinding{
					RunID: runID, TenantID: tenant, SurfaceKey: s.Key,
					CheckKey: models.EmbeddingCheckMixedSpace, Status: models.EmbeddingFindingViolation,
					Severity: models.EmbeddingSevMajor, Count: int(mixed),
					Evidence: jsonEvidence(map[string]any{"centroids_with_mixed_members": mixed}),
				})
			}
		}
	}

	// Stable bounded numeric tripwire. pgvector enforces dimensions; vector_norm
	// still catches zero/corrupt values that make cosine unusable.
	var invalid int64
	sampleSize := 64
	var policy models.EmbeddingLifecyclePolicy
	if err := db.Where("tenant_id = ?", embeddingLifecycleTenant).First(&policy).Error; err == nil && policy.NumericSampleSize >= 0 {
		sampleSize = policy.NumericSampleSize
	}
	if sampleSize > 0 {
		if err := db.Raw(fmt.Sprintf(
			`SELECT COUNT(*) FROM (
		   SELECT %[1]s FROM %[2]s WHERE %[1]s IS NOT NULL
		   ORDER BY md5(%[3]s::text) LIMIT ?
		 ) sample WHERE vector_norm(%[1]s) <= 0`, s.VecCol, s.Table, s.IDCol), sampleSize).Scan(&invalid).Error; err != nil {
			res.findings = append(res.findings, checkErrorFinding(runID, tenant, s.Key, models.EmbeddingCheckNumericSanity, err.Error()))
			res.CheckFailed = true
		} else if invalid > 0 {
			res.NumericInvalid = invalid
			res.findings = append(res.findings, models.EmbeddingLifecycleFinding{
				RunID: runID, TenantID: tenant, SurfaceKey: s.Key,
				CheckKey: models.EmbeddingCheckNumericSanity, Status: models.EmbeddingFindingViolation,
				Severity: models.EmbeddingSevCritical, Count: int(invalid),
				Evidence: jsonEvidence(map[string]any{"invalid_sample_vectors": invalid}),
			})
		}
	}

	// Emit stale violation aggregate (per-target rows are the campaign lane's
	// concern; here the count is enough to drive the verdict + cockpit).
	if res.Stale > 0 {
		res.findings = append(res.findings, models.EmbeddingLifecycleFinding{
			RunID: runID, TenantID: tenant, SurfaceKey: s.Key,
			CheckKey: models.EmbeddingCheckStaleInventory, Status: models.EmbeddingFindingViolation,
			Severity: models.EmbeddingSevMajor, Count: int(res.Stale),
			Evidence: jsonEvidence(map[string]any{"stale": res.Stale, "expected_producer": expectedProducer}),
		})
	}

	// Aggregate pass row (always) records the healthy magnitude for the ledger.
	res.findings = append(res.findings, models.EmbeddingLifecycleFinding{
		RunID: runID, TenantID: tenant, SurfaceKey: s.Key,
		CheckKey: models.EmbeddingCheckStaleInventory, Status: models.EmbeddingFindingPass,
		Count: int(res.Current),
		Evidence: jsonEvidence(map[string]any{
			"current": res.Current, "stale": res.Stale, "unstamped": res.Unstamped, "with_vec": res.WithVec,
		}),
	})

	res.Verdict = rollupSurfaceVerdict(res)
	if res.Verdict == models.EmbeddingVerdictDrifting {
		if campaign, active := activeCampaignForSpace(db, s.Space); active &&
			(campaign.State == models.EmbeddingCampaignRunning || campaign.State == models.EmbeddingCampaignVerifying) {
			res.Verdict = models.EmbeddingVerdictMigrating
		}
	}
	return res
}

// rollupSurfaceVerdict maps counts to a per-surface verdict (§8). No campaign
// state exists in Slice 2, so stale>0 is always `drifting` (never `migrating`).
func rollupSurfaceVerdict(a surfaceAudit) string {
	switch {
	case a.CheckFailed:
		return models.EmbeddingVerdictCheckError
	case a.NumericInvalid > 0:
		return models.EmbeddingVerdictBlocked
	case a.MixedSpace > 0:
		return models.EmbeddingVerdictMixedSpace
	case a.Stale > 0:
		return models.EmbeddingVerdictDrifting
	case a.Unstamped > 0:
		return models.EmbeddingVerdictUnstampedDebt
	default:
		return models.EmbeddingVerdictCoherent
	}
}

// countMixedSpaceCentroids counts centroids whose stamped space differs from a
// stamped member's space. All-NULL (unstamped) centroids/members are ignored —
// they are inventory debt (Lane A), not mixed-space violations. Returns an
// error the caller turns into check_error, never a false positive.
func countMixedSpaceCentroids(db *gorm.DB, s EmbeddingSurface) (int64, error) {
	var q string
	switch s.Key {
	case "story_centroid":
		// Members: content_items.story_id -> stories.public_id.
		q = `SELECT COUNT(DISTINCT st.public_id)
		     FROM stories st
		     JOIN content_items ci ON ci.story_id = st.public_id
		     WHERE st.embedding_space_id IS NOT NULL
		       AND ci.embedding_space_id IS NOT NULL
		       AND ci.embedding_space_id <> st.embedding_space_id`
	case "topic_centroid":
		// Members: content_item_topics.topic_id -> topics.public_id.
		q = `SELECT COUNT(DISTINCT t.public_id)
		     FROM topics t
		     JOIN content_item_topics cit ON cit.topic_id = t.public_id
		     JOIN content_items ci ON ci.public_id = cit.content_item_id
		     WHERE t.centroid_space_id IS NOT NULL
		       AND ci.embedding_space_id IS NOT NULL
		       AND ci.embedding_space_id <> t.centroid_space_id`
	default:
		return 0, nil
	}
	var n int64
	if err := db.Raw(q).Scan(&n).Error; err != nil {
		return 0, err
	}
	return n, nil
}

// runHeadline maps the set of surface verdicts to the family headline vocabulary.
func runHeadline(surfaces []surfaceAudit) string {
	attention := false
	watching := false
	for _, s := range surfaces {
		switch s.Verdict {
		case models.EmbeddingVerdictDrifting, models.EmbeddingVerdictMixedSpace,
			models.EmbeddingVerdictBlocked, models.EmbeddingVerdictCheckError:
			attention = true
		case models.EmbeddingVerdictUnstampedDebt, models.EmbeddingVerdictMigrating:
			watching = true
		}
	}
	switch {
	case attention:
		return models.EmbeddingHeadlineAttention
	case watching:
		return models.EmbeddingHeadlineWatching
	default:
		return models.EmbeddingHeadlineAllClear
	}
}
