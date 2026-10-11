package controllers

import (
	"content-management-system/src/models"
	"context"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// The budget counts actual requests, including redirects, HEAD fallback and HLS
// children. A worker slot is not a request budget.
type integrityProbeTransport struct {
	base      http.RoundTripper
	mu        sync.Mutex
	remaining int
	hosts     map[string]chan struct{}
	global    chan struct{}
	lookup    func(context.Context, string, string) ([]netip.Addr, error)
}

func (t *integrityProbeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	host := t.hosts[req.URL.Host]
	if host == nil {
		host = make(chan struct{}, 2)
		t.hosts[req.URL.Host] = host
	}
	t.mu.Unlock()
	select {
	case t.global <- struct{}{}:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}

	select {
	case host <- struct{}{}:
	case <-req.Context().Done():
		<-t.global
		return nil, req.Context().Err()
	}
	t.mu.Lock()
	if t.remaining <= 0 {
		t.mu.Unlock()
		<-host
		<-t.global
		return nil, fmt.Errorf("request_budget_exhausted")
	}
	t.remaining--
	t.mu.Unlock()
	resp, err := func() (response *http.Response, err error) {
		defer func() {
			if recover() != nil {
				err = fmt.Errorf("probe_evaluator_panic")
			}
		}()
		return t.base.RoundTrip(req)
	}()
	if err != nil {
		<-host
		<-t.global
		return nil, err
	}
	resp.Body = &integrityProbeBody{ReadCloser: resp.Body, release: func() { <-host; <-t.global }}
	return resp, nil
}

type integrityProbeBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *integrityProbeBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}
func (t *integrityProbeTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
func newIntegrityProbeClient(policy models.FeedIntegrityPolicy) *http.Client {
	client := newIntegrityHTTPClient(time.Duration(policy.ProbeTimeoutMS) * time.Millisecond)
	client.Transport = &integrityProbeTransport{base: client.Transport, remaining: policy.ProbeURLBudget, hosts: map[string]chan struct{}{}, global: make(chan struct{}, max(1, min(4, policy.ProbeConcurrency)))}
	return client
}
func integrityProbeOutcome(err error) string {
	if err == nil {
		return "ok"
	}
	reason := err.Error()
	if strings.Contains(reason, "request_budget_exhausted") {
		return "skipped:budget"
	}
	if reason == "http_404" || reason == "http_410" || strings.HasPrefix(reason, "invalid_") || reason == "hls_has_no_child" || reason == "media_content_type" {
		return "violation"
	}
	return "check_error"
}

// A missing file proves availability failure, not a type mismatch. HLS
// traversal starts only after the primary response type was accepted.
func integrityMediaTypeOutcome(probeErr, hlsErr error) string {
	if probeErr != nil && probeErr.Error() == "probe_evaluator_panic" {
		return "check_error"
	}
	if probeErr != nil && probeErr.Error() == "media_content_type" {
		return "violation"
	}
	if probeErr == nil || hlsErr != nil {
		return "ok"
	}
	return "skipped:response_unavailable"
}
func validateIntegrityProbeURL(ctx context.Context, client *http.Client, raw string) error {
	lookup := net.DefaultResolver.LookupNetIP
	if t, ok := client.Transport.(*integrityProbeTransport); ok && t.lookup != nil {
		lookup = t.lookup
	}
	return validateIntegrityURLWithResolver(ctx, raw, lookup)
}
func integrityProbeRequest(ctx context.Context, client *http.Client, method, raw string, limit int) (http.Header, []byte, error) {
	if err := validateIntegrityProbeURL(ctx, client, raw); err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, raw, nil)
	if err != nil {
		return nil, nil, err
	}
	if method == http.MethodGet && limit == 1024 {
		req.Header.Set("Range", "bytes=0-1023")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.Header, nil, fmt.Errorf("http_%d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit+1)))
	if err != nil {
		return resp.Header, nil, err
	}
	if len(body) > limit {
		return resp.Header, nil, fmt.Errorf("invalid_response_oversize")
	}
	return resp.Header, body, nil
}
func plausibleIntegrityType(header http.Header, kind, role string) bool {
	contentType := strings.ToLower(strings.Split(header.Get("Content-Type"), ";")[0])
	if contentType == "" || contentType == "application/octet-stream" {
		return true
	}
	if role == "artwork" {
		return strings.HasPrefix(contentType, "image/")
	}
	if kind == "hls" {
		return strings.Contains(contentType, "mpegurl") || contentType == "text/plain"
	}
	if kind == "audio" {
		return strings.HasPrefix(contentType, "audio/")
	}
	return strings.HasPrefix(contentType, "audio/") || strings.HasPrefix(contentType, "video/")
}
func probeIntegrityTarget(ctx context.Context, client *http.Client, t integrityProbeTarget) (probeErr, hlsErr error) {
	defer func() {
		if recover() != nil {
			probeErr = fmt.Errorf("probe_evaluator_panic")
			hlsErr = probeErr
		}
	}()
	h, _, err := integrityProbeRequest(ctx, client, http.MethodHead, t.URL, 1024)
	if err != nil && (err.Error() == "http_405" || err.Error() == "http_403" || err.Error() == "http_501") {
		h, _, err = integrityProbeRequest(ctx, client, http.MethodGet, t.URL, 1024)
	}
	if err != nil {
		return err, nil
	}
	if !plausibleIntegrityType(h, t.PlaybackType, t.Role) {
		return fmt.Errorf("media_content_type"), nil
	}
	hls := t.PlaybackType == "hls" || strings.HasSuffix(strings.ToLower(strings.Split(t.URL, "?")[0]), ".m3u8")
	if hls {
		err = probeIntegrityManifest(ctx, client, t.URL, false)
		return err, err
	}
	return nil, nil
}
func probeIntegrityManifest(ctx context.Context, client *http.Client, raw string, child bool) error {
	_, body, err := integrityProbeRequest(ctx, client, http.MethodGet, raw, 256<<10)
	if err != nil {
		return err
	}
	text := strings.TrimSpace(string(body))
	if !strings.HasPrefix(text, "#EXTM3U") {
		return fmt.Errorf("invalid_hls_manifest")
	}
	master := strings.Contains(text, "#EXT-X-STREAM-INF:")
	media := strings.Contains(text, "#EXTINF:")
	if (!master && !media) || (child && !media) {
		return fmt.Errorf("invalid_hls_manifest")
	}
	base, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid_hls_child")
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		ref, err := url.Parse(line)
		if err != nil {
			return fmt.Errorf("invalid_hls_child")
		}
		resolved := base.ResolveReference(ref).String()
		if err := validateIntegrityProbeURL(ctx, client, resolved); err != nil {
			return err
		}
		if child {
			h, _, err := integrityProbeRequest(ctx, client, http.MethodHead, resolved, 1024)
			if err != nil && (err.Error() == "http_403" || err.Error() == "http_405" || err.Error() == "http_501") {
				h, _, err = integrityProbeRequest(ctx, client, http.MethodGet, resolved, 1024)
			}
			if err == nil && !plausibleIntegrityType(h, "", "primary") {
				return fmt.Errorf("media_content_type")
			}
			return err
		}
		if master {
			return probeIntegrityManifest(ctx, client, resolved, true)
		}
		h, _, err := integrityProbeRequest(ctx, client, http.MethodHead, resolved, 1024)
		if err != nil && (err.Error() == "http_403" || err.Error() == "http_405" || err.Error() == "http_501") {
			h, _, err = integrityProbeRequest(ctx, client, http.MethodGet, resolved, 1024)
		}
		if err == nil && !plausibleIntegrityType(h, "", "primary") {
			return fmt.Errorf("media_content_type")
		}
		return err
	}
	return fmt.Errorf("hls_has_no_child")
}

// Select complete asset bundles conservatively. Each HEAD may need one Range
// fallback; HLS may also fetch a master, media manifest and segment. Unselected
// candidates are sampling coverage, rather than failed requests.
func collectFeedIntegrityProbes(ctx context.Context, policy models.FeedIntegrityPolicy, targets []integrityProbeTarget, c *integrityCollector) {
	unique := map[string]bool{}
	bundles := map[string][]integrityProbeTarget{}
	ids := []string{}
	candidates := map[string]int{}
	sort.SliceStable(targets, func(i, j int) bool {
		if targets[i].Priority != targets[j].Priority {
			return targets[i].Priority
		}
		return targets[i].ID < targets[j].ID
	})
	for _, t := range targets {
		identity := t.ID + ":" + t.Role
		if unique[identity] {
			continue
		}
		unique[identity] = true
		if len(bundles[t.ID]) == 0 {
			ids = append(ids, t.ID)
		}
		bundles[t.ID] = append(bundles[t.ID], t)
		candidates[integrityProbeKey(t.Role)]++
		if integrityTargetHLS(t) {
			candidates["probe_hls_manifest"]++
		}
		if t.Role == "primary" {
			candidates["probe_content_type"]++
		}
	}
	selected := [][]integrityProbeTarget{}
	left := policy.ProbeURLBudget
	for _, id := range ids {
		cost := 0
		for _, t := range bundles[id] {
			cost += 2
			if integrityTargetHLS(t) {
				cost += 4
			}
		}
		if cost <= left {
			selected = append(selected, bundles[id])
			left -= cost
		}
	}
	client := newIntegrityProbeClient(policy)
	defer client.CloseIdleConnections()
	workers := max(1, min(4, policy.ProbeConcurrency))
	jobs := make(chan []integrityProbeTarget)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for bundle := range jobs {
				for _, t := range bundle {
					key := integrityProbeKey(t.Role)
					severity := "major"
					axis := "consumer"
					if t.Role == "fallback" {
						severity = "minor"
					}
					if t.Role == "artwork" {
						severity = "info"
						axis = "readiness"
					}
					scope := "content:" + t.ID + ":" + t.Role
					err, hlsErr := probeIntegrityTarget(ctx, client, t)
					outcome := integrityProbeOutcome(err)
					c.add(key, "probe", "pods", "default", axis, severity, outcome, "probe", scope, 1, map[string]interface{}{"url": redactIntegrityURL(t.URL), "role": t.Role, "reason": safeIntegrityError(err)})
					if t.Role == "primary" {
						c.add("probe_content_type", "probe", "pods", "default", "consumer", "major", integrityMediaTypeOutcome(err, hlsErr), "probe", scope, 1, map[string]interface{}{"declared_type": t.PlaybackType, "reason": safeIntegrityError(err)})
					}
					if integrityTargetHLS(t) {
						state := integrityProbeOutcome(hlsErr)
						if err != nil && hlsErr == nil {
							state = outcome
						}
						c.add("probe_hls_manifest", "probe", "pods", "default", "consumer", "major", state, "probe", scope, 1, nil)
					}
					if err == nil && t.Role == "primary" {
						c.add("probe_url_success", "probe", "pods", "default", "consumer", "info", "ok", "content_item", t.ID, 1, map[string]interface{}{"role": "primary", "complete": true})
					}
				}
			}
		}()
	}
	for _, bundle := range selected {
		jobs <- bundle
	}
	close(jobs)
	wg.Wait()
	deliveryObserved := len(targets) > 0
	if page := c.observations["edge_pods_http|pods|default"]; page != nil && page.Outcome == "pass" {
		deliveryObserved = true
	}
	for _, o := range c.observations {
		if feedIntegrityCheckLane(o.CheckKey) != "probe" {
			continue
		}
		o.CandidateCount = candidates[o.CheckKey]
		if o.Outcome == "skipped:not_evaluated" {
			o.Outcome = "skipped:not_applicable"
			if !deliveryObserved {
				o.Outcome = "skipped:page_unavailable"
			}
			if o.CandidateCount > 0 {
				o.Outcome = "skipped:budget"
			}
			o.ObservedAt = time.Now().UTC()
		}
	}
}
func integrityProbeKey(role string) string {
	if role == "fallback" {
		return "probe_fallback"
	}
	if role == "artwork" {
		return "probe_thumbnail"
	}
	return "probe_url_dead"
}
func integrityTargetHLS(t integrityProbeTarget) bool {
	return t.PlaybackType == "hls" || strings.HasSuffix(strings.ToLower(strings.Split(t.URL, "?")[0]), ".m3u8")
}

// Revisit open sampled defects before rotating healthy samples. An item that
// has left canonical visibility is explicitly excluded, never guessed clean.
func integrityOpenProbeTargets(db *gorm.DB, tenant string, episodes []models.FeedIntegrityEpisode, c *integrityCollector) []integrityProbeTarget {
	result := []integrityProbeTarget{}
	for _, ep := range episodes {
		if feedIntegrityCheckLane(ep.CheckKey) != "probe" {
			continue
		}
		parts := strings.Split(ep.Scope, ":")
		if len(parts) != 3 || parts[0] != "content" {
			continue
		}
		id, err := uuid.Parse(parts[1])
		if err != nil {
			continue
		}
		var item models.ContentItem
		err = podsEligibleMediaQuery(db, tenant, supportsAtomizedPodsSchema(db)).Where("content_items.public_id=?", id).First(&item).Error
		if err == gorm.ErrRecordNotFound {
			c.add(ep.CheckKey, "probe", "pods", "default", ep.Axis, ep.Severity, "ok", "probe", ep.Scope, 0, map[string]interface{}{"canonical_excluded": true})
			continue
		}
		if err != nil {
			c.add(ep.CheckKey, "probe", "pods", "default", ep.Axis, ep.Severity, "check_error", "probe", ep.Scope, 0, nil)
			continue
		}
		roleGone := (parts[2] == "fallback" && (item.FallbackPlaybackURL == nil || *item.FallbackPlaybackURL == "")) || (parts[2] == "artwork" && (item.ThumbnailURL == nil || *item.ThumbnailURL == ""))
		if roleGone {
			c.add(ep.CheckKey, "probe", "pods", "default", ep.Axis, ep.Severity, "ok", "probe", ep.Scope, 0, map[string]interface{}{"role_not_applicable": true})
		}
		dto := mapToPodsItem(item, false, false)
		raw, kind := derefStr(dto.PlaybackURL), derefStr(dto.PlaybackType)
		if raw == "" {
			raw = dto.MediaURL
		}
		result = append(result, integrityProbeTarget{ID: parts[1], Role: "primary", URL: raw, PlaybackType: kind, Priority: true})
		if item.FallbackPlaybackURL != nil && *item.FallbackPlaybackURL != "" {
			result = append(result, integrityProbeTarget{ID: parts[1], Role: "fallback", URL: *item.FallbackPlaybackURL, Priority: true})
		}
		if item.ThumbnailURL != nil && *item.ThumbnailURL != "" {
			result = append(result, integrityProbeTarget{ID: parts[1], Role: "artwork", URL: *item.ThumbnailURL, Priority: true})
		}
	}
	return result
}
