package controllers

import (
	"content-management-system/src/models"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type integrityFixtureTransport func(*http.Request) (*http.Response, error)

func (f integrityFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fixtureIntegrityClient(budget int, fixture integrityFixtureTransport) *http.Client {
	return &http.Client{Transport: &integrityProbeTransport{base: fixture, remaining: budget, hosts: map[string]chan struct{}{}, global: make(chan struct{}, 4), lookup: func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("203.0.113.10")}, nil
	}}}
}
func fixtureResponse(r *http.Request, status int, kind, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{kind}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}
}
func TestFeedIntegrityObservationScoresUnknownAndCritical(t *testing.T) {
	o := integrityObservation{Version: 1, CheckKey: "edge_pods_bounds_served", Feed: "pods", Axis: "consumer", Severity: "critical", Outcome: "violation", Coverage: "sampled", CandidateCount: 1000, SampleCount: 10, AffectedCount: 1}
	r := integrityResultFor("pods", "all", []integrityObservation{o})
	if r.ConsumerScore != 0 || r.ConsumerVerdict != "broken" {
		t.Fatal(r)
	}
	o.Severity = "major"
	o.Outcome = "check_error"
	o.AffectedCount = 0
	r = integrityResultFor("pods", "all", []integrityObservation{o})
	raw, _ := json.Marshal(r)
	if !strings.Contains(string(raw), `"consumer_score":null`) {
		t.Fatal(string(raw))
	}
	o.Outcome = "pass"
	o.SampleCount = 0
	r = integrityResultFor("pods", "all", []integrityObservation{o})
	if r.ConsumerVerdict != "not_ready" {
		t.Fatal(r)
	}
	o.Outcome = "violation"
	o.SampleCount = 10
	o.AffectedCount = 1
	r = integrityResultFor("pods", "all", []integrityObservation{o})
	if r.ConsumerScore != 90 {
		t.Fatalf("sample denominator: %v", r)
	}
}
func TestFeedIntegrityProbeFallbackHLSBudgetAndContentType(t *testing.T) {
	var calls atomic.Int32
	client := fixtureIntegrityClient(8, func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		switch r.URL.Path {
		case "/master.m3u8":
			if r.Method == "HEAD" {
				return fixtureResponse(r, 405, "", ""), nil
			}
			return fixtureResponse(r, 200, "application/vnd.apple.mpegurl", "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800\nmedia.m3u8"), nil
		case "/media.m3u8":
			return fixtureResponse(r, 200, "application/vnd.apple.mpegurl", "#EXTM3U\n#EXTINF:10,\nsegment.ts"), nil
		case "/segment.ts":
			return fixtureResponse(r, 200, "video/mp2t", ""), nil
		default:
			return fixtureResponse(r, 404, "text/html", ""), nil
		}
	})
	err, hls := probeIntegrityTarget(context.Background(), client, integrityProbeTarget{URL: "https://fixture.test/master.m3u8", Role: "primary", PlaybackType: "hls"})
	if err != nil || hls != nil || calls.Load() != 5 {
		t.Fatalf("complete HLS: %v %v %d", err, hls, calls.Load())
	}
	client = fixtureIntegrityClient(1, func(r *http.Request) (*http.Response, error) { return fixtureResponse(r, 200, "text/html", ""), nil })
	err, _ = probeIntegrityTarget(context.Background(), client, integrityProbeTarget{URL: "https://fixture.test/a.mp4", Role: "primary", PlaybackType: "mp4"})
	if integrityProbeOutcome(err) != "violation" {
		t.Fatalf("HTML is not media: %v", err)
	}
	client = fixtureIntegrityClient(0, func(r *http.Request) (*http.Response, error) { t.Fatal("zero budget network call"); return nil, nil })
	_, _, err = integrityProbeRequest(context.Background(), client, "HEAD", "https://fixture.test/a.mp4", 1024)
	if integrityProbeOutcome(err) != "skipped:budget" {
		t.Fatal(err)
	}
	client = fixtureIntegrityClient(1, func(r *http.Request) (*http.Response, error) { return nil, fmt.Errorf("timeout") })
	err, _ = probeIntegrityTarget(context.Background(), client, integrityProbeTarget{URL: "https://fixture.test/a.mp4"})
	if integrityProbeOutcome(err) != "check_error" {
		t.Fatal(err)
	}
}

func TestFeedIntegrityBasicAssetOutcomes(t *testing.T) {
	for _, role := range []string{"primary", "fallback", "artwork"} {
		for _, outcome := range []string{"ok", "violation", "check_error"} {
			t.Run(role+"/"+outcome, func(t *testing.T) {
				client := fixtureIntegrityClient(1, func(r *http.Request) (*http.Response, error) {
					if outcome == "check_error" {
						return nil, context.DeadlineExceeded
					}
					if outcome == "violation" {
						return fixtureResponse(r, 404, "text/html", ""), nil
					}
					kind := "audio/mpeg"
					if role == "artwork" {
						kind = "image/jpeg"
					}
					return fixtureResponse(r, 200, kind, ""), nil
				})
				err, _ := probeIntegrityTarget(context.Background(), client, integrityProbeTarget{URL: "https://fixture.test/asset", Role: role})
				if got := integrityProbeOutcome(err); got != outcome {
					t.Fatalf("got %s, want %s: %v", got, outcome, err)
				}
			})
		}
	}
}

func TestFeedIntegrityMediaTypeRequiresResponseEvidence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		probeErr, hlsErr error
		want             string
	}{
		{"missing asset", fmt.Errorf("http_404"), nil, "skipped:response_unavailable"},
		{"timeout", context.DeadlineExceeded, nil, "skipped:response_unavailable"},
		{"evaluator panic", fmt.Errorf("probe_evaluator_panic"), fmt.Errorf("probe_evaluator_panic"), "check_error"},
		{"HTML response", fmt.Errorf("media_content_type"), nil, "violation"},
		{"valid response", nil, nil, "ok"},
		{"manifest failed after valid response", fmt.Errorf("invalid_hls_manifest"), fmt.Errorf("invalid_hls_manifest"), "ok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := integrityMediaTypeOutcome(tc.probeErr, tc.hlsErr); got != tc.want {
				t.Fatalf("type evidence outcome %q, want %q", got, tc.want)
			}
		})
	}
}
func TestFeedIntegrityProbePrivateTargetsAndOverflow(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "::ffff:127.0.0.1", "fc00::1"} {
		lookup := func(context.Context, string, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr(ip)}, nil
		}
		if validateIntegrityURLWithResolver(context.Background(), "https://fixture.test/a", lookup) == nil {
			t.Fatal(ip)
		}
	}
	c := fixtureIntegrityClient(1, func(r *http.Request) (*http.Response, error) {
		return fixtureResponse(r, 200, "text/plain", strings.Repeat("x", 1025)), nil
	})
	_, _, err := integrityProbeRequest(context.Background(), c, "GET", "https://fixture.test/a", 1024)
	if err == nil || err.Error() != "invalid_response_oversize" {
		t.Fatal(err)
	}
	for _, raw := range []string{"file:///tmp/a", "https://user:secret@fixture.test/a"} {
		if validateIntegrityURL(context.Background(), raw) == nil {
			t.Fatal(raw)
		}
	}
}

func TestFeedIntegrityProbePanicAndMissingDeliveryStayUnknown(t *testing.T) {
	client := fixtureIntegrityClient(2, func(r *http.Request) (*http.Response, error) { panic("fixture transport panic") })
	for i := 0; i < 2; i++ {
		err, _ := probeIntegrityTarget(context.Background(), client, integrityProbeTarget{URL: "https://fixture.test/a.mp4", Role: "primary"})
		if integrityProbeOutcome(err) != "check_error" {
			t.Fatal("panic was not unknown", err)
		}
	}
	transport := client.Transport.(*integrityProbeTransport)
	if len(transport.global) != 0 || len(transport.hosts["fixture.test"]) != 0 {
		t.Fatal("panic leaked active request slots")
	}
	collector := newIntegrityCollector(models.FeedIntegrityRun{Tier: "deep"}, nil, nil)
	collectFeedIntegrityProbes(context.Background(), models.DefaultFeedIntegrityPolicy("default"), nil, collector)
	for _, o := range collector.observations {
		if feedIntegrityCheckLane(o.CheckKey) == "probe" && o.Outcome != "skipped:page_unavailable" {
			t.Fatal("missing delivery falsely proved role inapplicable", o)
		}
	}
	collector = newIntegrityCollector(models.FeedIntegrityRun{Tier: "deep"}, nil, nil)
	collector.add("edge_pods_http", "edge", "pods", "default", "consumer", "critical", "ok", "page", "page:1", 1, nil)
	collectFeedIntegrityProbes(context.Background(), models.DefaultFeedIntegrityPolicy("default"), nil, collector)
	if collector.observations["probe_url_dead|pods|default"].Outcome != "skipped:not_applicable" {
		t.Fatal("successful empty delivery was not evaluated as inapplicable")
	}
}
func TestFeedIntegritySuppressedAndScopeCoverage(t *testing.T) {
	now := time.Now().UTC()
	run := models.FeedIntegrityRun{Tier: "deep", StartedAt: now}
	s := models.FeedIntegritySuppression{CheckKey: "probe_url_dead", Feed: "pods", Variant: "default", Scope: "content:a:primary", StartsAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
	c := newIntegrityCollector(run, []models.FeedIntegritySuppression{s}, nil)
	c.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", "violation", "probe", "content:a:primary", 1, nil)
	o := c.observations["probe_url_dead|pods|default"]
	if o.AffectedCount != 0 || o.SampleCount != 0 || o.Outcome != "skipped:suppressed" {
		t.Fatal(o)
	}
	if integrityScopeOutcome(*o, "content:b:primary") != "skipped:unsampled" {
		t.Fatal("wrong target was clean")
	}
	// Suppression expires by time, and never suppresses another exact target.
	s.ExpiresAt = now.Add(-time.Second)
	expired := newIntegrityCollector(run, []models.FeedIntegritySuppression{s}, nil)
	expired.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", "violation", "probe", "content:a:primary", 1, nil)
	if expired.observations["probe_url_dead|pods|default"].Outcome != "violation" {
		t.Fatal("expired suppression hid a current failure")
	}
	s.ExpiresAt = now.Add(time.Hour)
	other := newIntegrityCollector(run, []models.FeedIntegritySuppression{s}, nil)
	other.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", "violation", "probe", "content:b:primary", 1, nil)
	if other.observations["probe_url_dead|pods|default"].Outcome != "violation" {
		t.Fatal("suppression hid another target")
	}
}
func TestFeedIntegrityNewsFreshnessAndDiagnostics(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		age         time.Duration
		dirty, want bool
	}{{59 * time.Second, false, true}, {61 * time.Second, false, false}, {time.Second, true, false}, {-time.Second, false, false}} {
		if newsSnapshotFreshForLive(now.Add(-tc.age), tc.dirty, now) != tc.want {
			t.Fatal(tc)
		}
	}
	h := http.Header{"X-Wahb-Feed-Source": {"cache"}, "X-Wahb-Snapshot-Window": {"today"}, "X-Wahb-Snapshot-Age-Ms": {"10"}, "X-Wahb-Snapshot-Built-At": {now.Format(time.RFC3339Nano)}, "X-Wahb-Snapshot-Dirty": {"false"}}
	if _, err := integrityNewsDiagnostics(h, "today"); err != nil {
		t.Fatal(err)
	}
	if _, err := integrityNewsDiagnostics(h, "week"); err == nil {
		t.Fatal("wrong-window proof")
	}
	h.Del("X-Wahb-Snapshot-Dirty")
	if _, err := integrityNewsDiagnostics(h, "today"); err == nil {
		t.Fatal("missing diagnostic proof")
	}
}

func TestFeedIntegrityTimeoutIsLatencyEvidenceNotEndpointFailure(t *testing.T) {
	for _, scenario := range []struct {
		name                              string
		err                               error
		canceled                          bool
		wantHTTP, wantReason, wantVerdict string
	}{
		{"timeout", context.DeadlineExceeded, false, "check_error", "response_timeout", "inconclusive"},
		{"run canceled", context.DeadlineExceeded, true, "check_error", "run_canceled", "inconclusive"},
		{"HTTP failure", fmt.Errorf("http_503"), false, "violation", "invalid_response", "broken"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			c := newIntegrityCollector(models.FeedIntegrityRun{Tier: "deep", StartedAt: time.Now()}, nil, nil)
			status := 0
			if scenario.wantVerdict == "broken" {
				status = 503
			}
			c.unavailableEdge("pods", "default", "page:1", scenario.err, scenario.canceled, status, 2500*time.Millisecond, 1500)
			httpCheck := c.observations["edge_pods_http|pods|default"]
			if httpCheck.Outcome != scenario.wantHTTP || httpCheck.Reason != scenario.wantReason {
				t.Fatalf("%+v", httpCheck)
			}
			if scenario.name == "timeout" {
				latency := c.observations["edge_pods_latency|pods|default"]
				if latency.Outcome != "violation" || integrityScopeOutcome(*latency, "page:page:1") != "violation" {
					t.Fatalf("timeout lost measurable latency: %+v", latency)
				}
			}
			var observations []integrityObservation
			for _, o := range c.observations {
				observations = append(observations, *o)
			}
			if r := integrityResultFor("pods", "all", observations); r.ConsumerVerdict != scenario.wantVerdict {
				t.Fatal(r)
			}
		})
	}
}

func TestFeedIntegrityRequestErrorRetainsCauseWithoutQueryCredentials(t *testing.T) {
	err := &url.Error{Op: "Get", URL: "https://user:password@example.test/feed?token=secret", Err: context.DeadlineExceeded}
	got := safeIntegrityError(err)
	if !strings.Contains(got, "context deadline exceeded") || strings.Contains(got, "secret") || strings.Contains(got, "password") || strings.Contains(got, "user:") {
		t.Fatal(got)
	}
}
