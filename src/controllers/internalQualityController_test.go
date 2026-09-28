package controllers

import (
	"testing"

	"github.com/google/uuid"
)

func TestQualityVersionedObjectKeyMatchesAggregationContract(t *testing.T) {
	itemID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	for _, test := range []struct {
		version int
		want    string
	}{
		{version: 0, want: "content/11111111-2222-3333-4444-555555555555/processed.mp4"},
		{version: 1, want: "content/11111111-2222-3333-4444-555555555555/processed.mp4"},
		{version: 2, want: "content/11111111-2222-3333-4444-555555555555/processed.v2.mp4"},
		{version: 12, want: "content/11111111-2222-3333-4444-555555555555/processed.v12.mp4"},
	} {
		if got := qualityVersionedObjectKey(itemID, test.version); got != test.want {
			t.Errorf("qualityVersionedObjectKey(%d) = %q, want %q", test.version, got, test.want)
		}
	}
}
