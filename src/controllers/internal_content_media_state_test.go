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
