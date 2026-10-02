package supply

import (
	"errors"
	"testing"
)

func TestReplayPageContinuationRule(t *testing.T) {
	cases := []struct {
		name      string
		index     int
		total     int
		exhausted bool
		wantErr   error
	}{
		{name: "single exhausted page hands off", index: 0, total: 1, exhausted: true},
		{name: "single unsettled page stays pending", index: 0, total: 1, exhausted: false, wantErr: ErrReplayHandoffPending},
		{name: "non-final unsettled page continues", index: 0, total: 3, exhausted: false},
		{name: "non-final exhausted page is invalid", index: 1, total: 3, exhausted: true, wantErr: ErrReplayCompletedEarly},
		{name: "final exhausted page hands off", index: 2, total: 3, exhausted: true},
		{name: "final unsettled page stays pending", index: 2, total: 3, exhausted: false, wantErr: ErrReplayHandoffPending},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := replayPageContinuationValid(testCase.index, testCase.total, testCase.exhausted)
			if testCase.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}
