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
		Surface:       "foryou",
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
	if out.EventType != "playback_started" || out.Surface != "foryou" {
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
	in.Measurements = &struct {
		DurationMS      *int    `json:"duration_ms"`
		MediaErrorCode  *int    `json:"media_error_code"`
		StallDurationMS *int    `json:"stall_duration_ms"`
		FailureClass    *string `json:"failure_class"`
		Visible         *bool   `json:"visible"`
	}{DurationMS: &over, MediaErrorCode: &badCode, StallDurationMS: &neg}

	out, ok := validateAndNormalizeRuxEvent(&in, time.Now())
	if !ok {
		t.Fatal("out-of-range measurements should be dropped, not reject the event")
	}
	if out.DurationMS != nil {
		t.Errorf("over-range duration should be dropped, got %v", *out.DurationMS)
	}
	if out.MediaErrorCode != nil {
		t.Errorf("out-of-range media error code should be dropped, got %v", *out.MediaErrorCode)
	}
	if out.StallDurationMS != nil {
		t.Errorf("negative stall should be dropped, got %v", *out.StallDurationMS)
	}
}

func TestValidateRuxEvent_AcceptsValidFailureClass(t *testing.T) {
	in := validRuxEvent()
	in.EventType = "playback_failed"
	fc := "autoplay_blocked"
	in.Measurements = &struct {
		DurationMS      *int    `json:"duration_ms"`
		MediaErrorCode  *int    `json:"media_error_code"`
		StallDurationMS *int    `json:"stall_duration_ms"`
		FailureClass    *string `json:"failure_class"`
		Visible         *bool   `json:"visible"`
	}{FailureClass: &fc}
	out, ok := validateAndNormalizeRuxEvent(&in, time.Now())
	if !ok || out.FailureClass == nil || *out.FailureClass != "autoplay_blocked" {
		t.Fatalf("valid failure_class not preserved: ok=%v out=%+v", ok, out)
	}
}
