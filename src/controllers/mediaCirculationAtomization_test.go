package controllers

import "testing"

// One pull matching several thin buckets must split its allowed_intake across
// them, not count the full amount as predicted supply in every bucket.
func TestAddPredictedIntakeShare(t *testing.T) {
	out := map[string]mediaCircBucketYield{}
	addPredictedIntakeShare(out, []string{"5m", "10m"}, 10)
	if out["5m"].Predicted != 5 || out["10m"].Predicted != 5 {
		t.Errorf("even split = %d/%d, want 5/5", out["5m"].Predicted, out["10m"].Predicted)
	}

	out = map[string]mediaCircBucketYield{}
	addPredictedIntakeShare(out, []string{"15m", "20m"}, 11)
	if out["15m"].Predicted != 6 || out["20m"].Predicted != 5 {
		t.Errorf("remainder split = %d/%d, want 6/5", out["15m"].Predicted, out["20m"].Predicted)
	}

	out = map[string]mediaCircBucketYield{}
	addPredictedIntakeShare(out, nil, 10)
	addPredictedIntakeShare(out, []string{"5m"}, 0)
	if out["5m"].Predicted != 0 {
		t.Errorf("no-op inputs should not add predicted intake, got %v", out)
	}
}

// durationBucketSQLExpr must stay the SQL mirror of durationBucketLabel; pin
// the Go rule on the boundary values the SQL is written against (nearest of
// 5/10/15/20/30/40, ties resolved to the smaller bucket).
func TestDurationBucketLabelBoundaries(t *testing.T) {
	cases := []struct {
		seconds int
		want    string
	}{
		{271, "5m"},       // 4:31 rounds to 5
		{16 * 60, "15m"},  // 16m nearer 15 than 20
		{26 * 60, "30m"},  // 26m nearer 30 than 20
		{35 * 60, "30m"},  // tie 30/40 resolves to the smaller bucket
		{150 * 60, "40m"}, // clamps to the largest bucket
	}
	for _, tc := range cases {
		if got := durationBucketLabel(tc.seconds * 1000); got != tc.want {
			t.Errorf("durationBucketLabel(%ds) = %s, want %s", tc.seconds, got, tc.want)
		}
	}
}
