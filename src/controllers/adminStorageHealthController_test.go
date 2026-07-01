package controllers

import (
	"content-management-system/src/models"
	"strings"
	"testing"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func TestClassifyStorageHealth(t *testing.T) {
	policy := models.StoragePolicy{
		MaxStorageBytes:      1_000,
		TargetUtilizationPct: 80,
	}

	tests := []struct {
		name             string
		proof            storageProofMetrics
		aggregationError string
		want             string
	}{
		{
			name: "healthy below watch threshold",
			proof: storageProofMetrics{
				QuotaBytes:     1_000,
				UtilizationPct: 50,
				ColdEnabled:    true,
			},
			want: "healthy",
		},
		{
			name: "pressure at target with cold tier",
			proof: storageProofMetrics{
				QuotaBytes:     1_000,
				UtilizationPct: 82,
				ColdEnabled:    true,
			},
			want: "pressure",
		},
		{
			name: "critical before ordinary pressure",
			proof: storageProofMetrics{
				QuotaBytes:     1_000,
				UtilizationPct: 97,
				ColdEnabled:    true,
			},
			want: "critical",
		},
		{
			name: "degraded without cold tier at target",
			proof: storageProofMetrics{
				QuotaBytes:     1_000,
				UtilizationPct: 82,
				ColdEnabled:    false,
			},
			want: "degraded_no_cold",
		},
		{
			name: "degraded when live metrics fail",
			proof: storageProofMetrics{
				QuotaBytes:     1_000,
				UtilizationPct: 50,
				ColdEnabled:    true,
			},
			aggregationError: "aggregation unavailable",
			want:             "degraded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyStorageHealth(policy, tt.proof, tt.aggregationError); got != tt.want {
				t.Fatalf("classifyStorageHealth() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRecoverableDeleteRequiresRecoveryMetadata(t *testing.T) {
	_, err := createStorageArtifactEvent(nil, storageArtifactEventInput{
		ContentItemID: uuid.New(),
		EventType:     models.StorageArtifactEventRecoverableDeleted,
		Status:        models.StorageArtifactEventStatusSuccess,
	})
	if err == nil || !strings.Contains(err.Error(), "recovery metadata") {
		t.Fatalf("expected recovery metadata error, got %v", err)
	}
}

func TestStorageCandidateQueryIncludesAtomizedParents(t *testing.T) {
	db, _ := newMockGorm(t)
	stmt := buildCandidateQuery(db.Session(&gorm.Session{DryRun: true}), candidateFilter{
		tenantID:               "default",
		minAgeDays:             14,
		maxViewCount:           5,
		excludeColdTier:        true,
		includeAtomizedParents: true,
	}).Find(&[]models.ContentItem{}).Statement

	sql := stmt.SQL.String()
	if !strings.Contains(sql, "child.parent_content_item_id = content_items.public_id") {
		t.Fatalf("expected atomized parent child-exists condition in SQL, got %s", sql)
	}
	if !strings.Contains(sql, "child.is_feed_unit = TRUE") {
		t.Fatalf("expected atomized parent query to require valid feed-unit children, got %s", sql)
	}
}

func TestDeleteCandidateQueryRequiresRecoverableLowRiskArtifacts(t *testing.T) {
	db, _ := newMockGorm(t)
	stmt := buildCandidateQuery(db.Session(&gorm.Session{DryRun: true}), candidateFilter{
		tenantID:               "default",
		minAgeDays:             14,
		maxViewCount:           5,
		excludeColdTier:        true,
		includeAtomizedParents: true,
		archiveAction:          "delete",
	}).Find(&[]models.ContentItem{}).Statement

	sql := stmt.SQL.String()
	required := []string{
		"original_url IS NOT NULL",
		"source_feed_url IS NOT NULL",
		"source_episode_id IS NOT NULL",
		"idempotency_key IS NOT NULL",
		"NOT (is_feed_unit = TRUE AND status =",
		"media_suitability IS NULL OR media_suitability !=",
		"duration_sec IS NOT NULL AND duration_sec >",
		"child.feed_visibility = 'visible'",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("expected delete candidate query to contain %q, got %s", fragment, sql)
		}
	}
}

func TestProtectedStorageItemsQueryUsesHybridHotness(t *testing.T) {
	db, _ := newMockGorm(t)
	stmt := protectedStorageItemsQuery(db.Session(&gorm.Session{DryRun: true}), "default", models.StoragePolicy{
		ProtectTopNByViews:    50,
		ProtectTopNWindowDays: 30,
	}).Find(&[]models.ContentItem{}).Statement

	sql := stmt.SQL.String()
	required := []string{
		"content_flags",
		"user_interactions",
		"PARTITION BY COALESCE(source_name",
		"PARTITION BY COALESCE(duration_bucket",
		"view_count + like_count * 2 + share_count * 4",
		"feed_visibility = 'visible'",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("expected hybrid hotness query to contain %q, got %s", fragment, sql)
		}
	}
}

func TestStorageRecommendationsFlagUntrackedBucketGap(t *testing.T) {
	recs := storageRecommendationsFor(models.StoragePolicy{
		ArchiveAction: "re_encode",
	}, storageProofMetrics{
		UsedBytes:      9_000_000_000,
		DBTrackedBytes: 2_000_000_000,
		QuotaBytes:     5_000_000_000,
	}, "degraded_no_cold")

	for _, rec := range recs {
		if rec.Key == "untracked_bucket_gap" {
			if rec.Action != "run_reconcile" {
				t.Fatalf("expected run_reconcile action, got %q", rec.Action)
			}
			if rec.EstimatedBytes != 7_000_000_000 {
				t.Fatalf("expected gap estimate 7000000000, got %d", rec.EstimatedBytes)
			}
			return
		}
	}
	t.Fatalf("expected untracked bucket gap recommendation, got %#v", recs)
}
