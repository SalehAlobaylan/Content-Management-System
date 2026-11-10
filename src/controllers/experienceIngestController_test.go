package controllers

import (
	"testing"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
)

func validRuxEvent() incomingRuxEvent {
	in := incomingRuxEvent{
		EventID:       uuid.NewString(),
		SchemaVersion: models.RuxSchemaVersion,
		EventType:     "playback_started",
		OccurredAt:    time.Now().UTC().Format(time.RFC3339),
		SessionID:     "sess-abc",
		PageLoadID:    uuid.NewString(),
		Sequence:      3,
		Release:       "2026.07.11.1",
		Surface:       "pods",
	}
	in.Client.BrowserFamily = "safari"
	in.Client.BrowserMajor = 18
	in.Client.DeviceClass = "mobile"
	in.Client.NetworkClass = "unknown"
	return in
}

func TestValidateRuxEvent_HappyPath(t *testing.T) {
	in := validRuxEvent()
	out, ok := validateAndNormalizeRuxEvent(&in, time.Now())
	if !ok {
		t.Fatal("expected valid event to pass")
	}
	if out.EventType != "playback_started" || out.Surface != "pods" {
		t.Fatalf("unexpected normalization: %+v", out)
	}
	if out.BrowserFamily != "safari" || out.BrowserMajor != 18 {
		t.Fatalf("cohort not preserved: %+v", out)
	}
}

func TestValidateRuxEvent_RejectsUnknownEnums(t *testing.T) {
	cases := map[string]func(*incomingRuxEvent){
		"bad event_type": func(e *incomingRuxEvent) { e.EventType = "keystroke_captured" },
		"bad surface":    func(e *incomingRuxEvent) { e.Surface = "settings" },
		"bad schema":     func(e *incomingRuxEvent) { e.SchemaVersion = 99 },
		"bad event_id":   func(e *incomingRuxEvent) { e.EventID = "not-a-uuid" },
		"bad page_load":  func(e *incomingRuxEvent) { e.PageLoadID = "nope" },
		"empty session":  func(e *incomingRuxEvent) { e.SessionID = "" },
		"empty release":  func(e *incomingRuxEvent) { e.Release = "" },
		"bad occurred":   func(e *incomingRuxEvent) { e.OccurredAt = "yesterday" },
	}
	for name, mutate := range cases {
		in := validRuxEvent()
		mutate(&in)
		if _, ok := validateAndNormalizeRuxEvent(&in, time.Now()); ok {
			t.Errorf("%s: expected rejection", name)
		}
	}
}

func TestValidateRuxEvent_NormalizesUnknownCohortsSafely(t *testing.T) {
	// A brand-new browser/network must not drop telemetry — it normalizes to the
	// safe bucket rather than rejecting the event.
	in := validRuxEvent()
	in.Client.BrowserFamily = "brave-2027"
	in.Client.NetworkClass = "6g"
	in.Client.DeviceClass = "foldable"
	out, ok := validateAndNormalizeRuxEvent(&in, time.Now())
	if !ok {
		t.Fatal("expected event with unknown cohorts to still be accepted")
	}
	if out.BrowserFamily != "other" || out.NetworkClass != "unknown" || out.DeviceClass != "mobile" {
		t.Fatalf("unknown cohorts not normalized safely: %+v", out)
	}
}

func TestValidateRuxEvent_BoundsMeasurements(t *testing.T) {
	in := validRuxEvent()
	over := models.RuxMaxDurationMS + 1
	badCode := 9
	neg := -5
	in.Measurements = &incomingRuxMeasurements{DurationMS: &over, MediaErrorCode: &badCode, StallDurationMS: &neg}

	if _, ok := validateAndNormalizeRuxEvent(&in, time.Now()); ok {
		t.Fatal("v2 measurements outside supported range must reject the event")
	}

}

func TestValidateRuxEvent_AcceptsValidFailureClass(t *testing.T) {
	in := validRuxEvent()
	in.EventType = "playback_failed"
	fc := "autoplay_blocked"
	in.Measurements = &incomingRuxMeasurements{FailureClass: &fc}
	out, ok := validateAndNormalizeRuxEvent(&in, time.Now())
	if !ok || out.FailureClass == nil || *out.FailureClass != "autoplay_blocked" {
		t.Fatalf("valid failure_class not preserved: ok=%v out=%+v", ok, out)
	}
}

func TestValidateRuxEvent_ProductSessionRequiresNativeEvidence(t *testing.T) {
	now := time.Now().UTC()
	in := validRuxEvent()
	in.EventType = "product_session_closed"
	id := uuid.NewString()
	in.JourneyID = &id
	if _, ok := validateAndNormalizeRuxEvent(&in, now); ok {
		t.Fatal("legacy event accepted as a product session")
	}
	in.SchemaVersion = 3
	in.Client.Platform = "ios"
	in.Measurements = new(incomingRuxMeasurements)
	duration, mask := 25*60*1000, 3
	in.Measurements.DurationMS = &duration
	in.Measurements.ProductSessionFeedMask = &mask
	out, ok := validateAndNormalizeRuxEvent(&in, now)
	if !ok || out.DurationMS == nil || *out.DurationMS != duration {
		t.Fatal("valid native session duration was lost")
	}
	mask = 4
	if _, ok := validateAndNormalizeRuxEvent(&in, now); ok {
		t.Fatal("unrecognized feed mask accepted")
	}
}
