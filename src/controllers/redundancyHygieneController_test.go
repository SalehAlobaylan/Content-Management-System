package controllers

import "testing"

func TestRedundancyTranscriptBodyExcludesSharedEdges(t *testing.T) {
	intro := "shared intro words that should not decide identity "
	bodyA := "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee zulu "
	bodyB := "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twentyone twentytwo twentythree twentyfour "
	outro := " shared outro words that should not decide identity"
	if got := shingleJaccard(transcriptBody(intro+bodyA+outro), transcriptBody(intro+bodyB+outro)); got >= .2 {
		t.Fatalf("shared edges must not look like duplicate bodies, got %f", got)
	}
}

func TestRedundancyTitleSimilarity(t *testing.T) {
	if got := titleSimilarity("Episode 214 The Grid", "The Grid Episode 214"); got < .5 {
		t.Fatalf("expected title overlap, got %f", got)
	}
	if got := titleSimilarity("completely unrelated", "different content"); got != 0 {
		t.Fatalf("expected no overlap, got %f", got)
	}
}

func TestRedundancyDurationGate(t *testing.T) {
	if redundancyAbs(3600-3660) > redundancyMaxInt(15, int(float64(3600)*.02)) {
		t.Fatal("2 percent duration drift should pass")
	}
}

func TestRedundancySeriesSiblingNeverGetsTranscriptConfirmation(t *testing.T) {
	a := "host intro repeated words shared shared shared shared shared shared shared shared " + "episode one has a unique discussion about markets policy currency inflation employment trade growth analysis evidence detail "
	b := "host intro repeated words shared shared shared shared shared shared shared shared " + "episode two has a unique discussion about astronomy planets telescopes galaxies orbit gravity science observation evidence detail "
	if got := shingleJaccard(transcriptBody(a), transcriptBody(b)); got >= .80 {
		t.Fatalf("series siblings must not confirm: %f", got)
	}
}
