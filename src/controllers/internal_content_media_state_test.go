package controllers

import (
	"testing"

	"content-management-system/src/models"
	"github.com/google/uuid"
)

func mediaAwaitingDuration(seconds int) models.ContentItem {
	waiting := "waiting_media"
	return models.ContentItem{
		Type:             models.ContentTypePodcast,
		DurationSec:      &seconds,
		IsFeedUnit:       false,
		FeedVisibility:   "hidden",
		ChapteringStatus: &waiting,
	}
}

func TestSetFeedUnitDurationBucketPublishesLegalRawMedia(t *testing.T) {
	item := mediaAwaitingDuration(15 * 60)
	setFeedUnitDurationBucket(&item)
	if !item.IsFeedUnit || item.FeedVisibility != "visible" || item.DurationBucket == nil || *item.DurationBucket != "15m" {
		t.Fatalf("legal raw media state = feed_unit:%v visibility:%q bucket:%v", item.IsFeedUnit, item.FeedVisibility, item.DurationBucket)
	}
}

func TestSetFeedUnitDurationBucketKeepsIllegalParentsHidden(t *testing.T) {
	for name, seconds := range map[string]int{"too short": podsMinDurationSec - 1, "needs atomization": podsHardMaxDurationSec + 1} {
		item := mediaAwaitingDuration(seconds)
		setFeedUnitDurationBucket(&item)
		if item.IsFeedUnit || item.FeedVisibility != "hidden" || item.DurationBucket != nil {
			t.Fatalf("%s state = feed_unit:%v visibility:%q bucket:%v", name, item.IsFeedUnit, item.FeedVisibility, item.DurationBucket)
		}
	}
}

func TestSetFeedUnitDurationBucketDoesNotOverrideAtomizedChildVisibility(t *testing.T) {
	item := mediaAwaitingDuration(10 * 60)
	parentID := uuid.New()
	item.ParentContentItemID = &parentID
	setFeedUnitDurationBucket(&item)
	if item.IsFeedUnit || item.FeedVisibility != "hidden" {
		t.Fatalf("child workflow state was overridden: feed_unit:%v visibility:%q", item.IsFeedUnit, item.FeedVisibility)
	}
}

func TestMediaDurationBelowAdmission(t *testing.T) {
	below, boundary := podsMinDurationSec-1, podsMinDurationSec
	if !mediaDurationBelowAdmission(models.ContentTypeVideo, &below) {
		t.Fatal("known undersized Video must be rejected")
	}
	if mediaDurationBelowAdmission(models.ContentTypePodcast, &boundary) {
		t.Fatal("270-second Podcast boundary must be admitted")
	}
	if mediaDurationBelowAdmission(models.ContentTypeVideo, nil) {
		t.Fatal("unknown duration must proceed to authoritative Media probing")
	}
	if mediaDurationBelowAdmission(models.ContentTypeNews, &below) {
		t.Fatal("News is not governed by the Pods duration contract")
	}
}

func TestMediaArtifactDurationInvalidRequiresKnownLegalDuration(t *testing.T) {
	below, boundary := podsMinDurationSec-1, podsMinDurationSec
	if !mediaArtifactDurationInvalid(models.ContentTypeVideo, nil) {
		t.Fatal("Video playback write-back must not accept an unknown duration")
	}
	if !mediaArtifactDurationInvalid(models.ContentTypePodcast, &below) {
		t.Fatal("undersized Podcast playback write-back must be rejected")
	}
	if mediaArtifactDurationInvalid(models.ContentTypeVideo, &boundary) {
		t.Fatal("270-second Video artifact must be admitted")
	}
	if mediaArtifactDurationInvalid(models.ContentTypeNews, nil) {
		t.Fatal("News artifacts are outside the Pods duration contract")
	}
}

func TestMediaArtifactDurationVerifiedRequiresMatchingFFprobeProvenance(t *testing.T) {
	duration := podsMinDurationSec
	item := mediaAwaitingDuration(duration)
	valid := map[string]interface{}{"duration_verification": map[string]interface{}{"source": "ffprobe", "duration_sec": float64(duration)}}
	if !mediaArtifactDurationVerified(item, valid, duration) {
		t.Fatal("matching FFprobe verification must be accepted")
	}
	if mediaArtifactDurationVerified(item, map[string]interface{}{"duration_verification": map[string]interface{}{"source": "provider", "duration_sec": float64(duration)}}, duration) {
		t.Fatal("provider metadata must not authorize a serving artifact")
	}
	if mediaArtifactDurationVerified(item, map[string]interface{}{"duration_verification": map[string]interface{}{"source": "ffprobe", "duration_sec": float64(duration + 1)}}, duration) {
		t.Fatal("mismatched FFprobe duration must not be accepted")
	}

	item.Metadata = []byte(`{"duration_verification":{"source":"ffprobe","duration_sec":270}}`)
	if !mediaArtifactDurationVerified(item, nil, duration) {
		t.Fatal("a matching persisted FFprobe verification must support idempotent repair")
	}
	if mediaArtifactDurationVerified(item, map[string]interface{}{"duration_verification": map[string]interface{}{"source": "provider", "duration_sec": float64(duration)}}, duration) {
		t.Fatal("an explicitly invalid incoming verification must not fall back to persisted metadata")
	}
}
