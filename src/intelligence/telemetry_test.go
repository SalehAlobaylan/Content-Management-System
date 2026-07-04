package intelligence

import (
	"testing"

	"content-management-system/src/models"
)

func TestBucketLabelForDuration(t *testing.T) {
	cases := []struct {
		durationSec int
		want        string
	}{
		{271, "5m"},    // just over the For You floor
		{300, "5m"},    // exactly 5m
		{450, "5m"},    // 7.5m rounds down on tie-break to the smaller bucket
		{960, "15m"},   // 16m → 15m
		{1560, "30m"},  // 26m → 30m
		{2100, "30m"},  // 35m ties 30/40 → smaller wins
		{2400, "40m"},  // the ceiling
		{9000, "40m"},  // clamps at 40m
	}
	for _, c := range cases {
		if got := BucketLabelForDuration(c.durationSec); got != c.want {
			t.Fatalf("BucketLabelForDuration(%d) = %s, want %s", c.durationSec, got, c.want)
		}
	}
}

func TestItemBucketLabel(t *testing.T) {
	explicit := "20m"
	dur := 600
	withColumn := models.ContentItem{DurationBucket: &explicit, DurationSec: &dur}
	if got := itemBucketLabel(withColumn); got != "20m" {
		t.Fatalf("explicit bucket column must win, got %s", got)
	}
	derived := models.ContentItem{DurationSec: &dur}
	if got := itemBucketLabel(derived); got != "10m" {
		t.Fatalf("600s must derive to 10m, got %s", got)
	}
	empty := models.ContentItem{}
	if got := itemBucketLabel(empty); got != "" {
		t.Fatalf("no duration data must yield empty label, got %q", got)
	}
}
