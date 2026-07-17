package controllers

import "testing"

func TestSourceImageURLNeverDiscoversOverNetwork(t *testing.T) {
	explicit := "https://cdn.example/source.png"
	feed := "https://source.example/feed.xml"
	if got := sourceImageURL(&explicit, &feed); got == nil || *got != explicit {
		t.Fatalf("explicit image URL = %#v, want %q", got, explicit)
	}
	if got := sourceImageURL(nil, &feed); got != nil {
		t.Fatalf("missing explicit image must remain unset, got %q", *got)
	}
}
