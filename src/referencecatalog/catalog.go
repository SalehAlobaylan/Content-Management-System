// Package referencecatalog owns portable definitions, never processing state.
package referencecatalog

import (
	"bytes"
	"content-management-system/src/definitionconfig"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Record struct {
	Key        string         `json:"key"`
	Kind       string         `json:"kind"`
	Scope      string         `json:"scope"`
	Definition map[string]any `json:"definition"`
	ProfileKey string         `json:"discovery_profile_key,omitempty"`
}
type Origin struct {
	Environment    string `json:"environment"`
	Category       string `json:"category,omitempty"`
	Tenant         string `json:"tenant"`
	CapturedAt     string `json:"captured_at"`
	ProjectionHash string `json:"projection_hash"`
}
type Manifest struct {
	FormatVersion     int               `json:"format_version"`
	CatalogName       string            `json:"catalog_name"`
	ReleaseVersion    string            `json:"release_version"`
	RequiredMigration string            `json:"required_migration"`
	Files             map[string]string `json:"files"`
	Origins           map[string]Origin `json:"origins"`
}
type Catalog struct {
	Manifest Manifest `json:"manifest"`
	Records  []Record `json:"records"`
}

const Migration = "20261102020000_reference_source_types.sql"

var Groups = []string{"topic-categories", "topics", "discovery-profiles", "news-sources", "media-sources", "quality-profiles"}
var fields = map[string][]string{
	"topic-categories":   strings.Fields("slug label_ar label_en sort_order active"),
	"topics":             strings.Fields("slug label_ar label_en category_slug active featured"),
	"discovery-profiles": strings.Fields("category name description keywords languages"),
	"news-sources":       strings.Fields("category type name feed_url image_url api_config metadata"),
	"media-sources":      strings.Fields("category type name feed_url image_url api_config metadata"),
	"quality-profiles":   strings.Fields("source_type name description video_codec max_height target_bitrate_kbps crf preset audio_codec audio_bitrate_kbps audio_channels audio_sample_rate_hz output_container media_kind max_width max_frame_rate video_profile video_level quality_tier profile_purpose passthrough_allowed remux_allowed schema_version thumbnail_offset_seconds thumbnail_max_height allowed_input_mime_types preset_key"),
}
var configFields = strings.Fields("username channel_username channel_id mode media_types min_text_length max_age_hours max_results min_duration_minutes max_duration_minutes min_duration_sec initial_atomization_limit chaptering_enabled auto_publish_high_confidence parent_feed_visible preserve_video remove_sponsor_segments min_chapter_minutes min_feed_unit_seconds soft_max_chapter_minutes hard_max_chapter_minutes atomization_min_parent_seconds max_chapters_per_parent chaptering_mode high_confidence_threshold preferred_playback_rendition fallback_playback_rendition audio_only_allowed")
var metadataFields = strings.Fields("language languages description author publisher channel_id channel_username username title image_url thumbnail_url website_url")

func has(a []string, k string) bool {
	for _, s := range a {
		if s == k {
			return true
		}
	}
	return false
}
func Hash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Decode rejects duplicate member names at every depth before typed decoding.
func Decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	if err := scan(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(v)
}
func scan(d *json.Decoder) error {
	t, e := d.Token()
	if e != nil {
		return e
	}
	if x, ok := t.(json.Delim); ok {
		switch x {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s := k.(string)
				if seen[s] {
					return fmt.Errorf("duplicate JSON field %s", s)
				}
				seen[s] = true
				if e = scan(d); e != nil {
					return e
				}
			}
		case '[':
			for d.More() {
				if e = scan(d); e != nil {
					return e
				}
			}
		default:
			return errors.New("invalid JSON delimiter")
		}
		_, e = d.Token()
		return e
	}
	return nil
}
func Read(dir string) (Catalog, error) {
	var c Catalog
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return c, e
	}
	if e = Decode(b, &c.Manifest); e != nil {
		return c, e
	}
	for _, g := range Groups {
		b, e = os.ReadFile(filepath.Join(dir, g+".json"))
		if e != nil {
			return c, e
		}
		var r []Record
		if e = Decode(b, &r); e != nil {
			return c, e
		}
		for _, entry := range r {
			if entry.Kind != g {
				return c, fmt.Errorf("record kind does not match group file %s", g)
			}
		}
		if Hash(r) != c.Manifest.Files[g+".json"] {
			return c, fmt.Errorf("hash mismatch: %s", g)
		}
		c.Records = append(c.Records, r...)
	}
	return c, Validate(c)
}
func Write(dir string, c Catalog) error {
	groups := map[string][]Record{}
	c.Manifest.Files = map[string]string{}
	for _, g := range Groups {
		rs := []Record{}
		for _, r := range c.Records {
			if r.Kind == g {
				rs = append(rs, r)
			}
		}
		sort.Slice(rs, func(i, j int) bool { return rs[i].Key < rs[j].Key })
		groups[g] = rs
		c.Manifest.Files[g+".json"] = Hash(rs)
	}
	if err := Validate(c); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return errors.New("output directory already exists or cannot be created")
	}
	complete := false
	defer func() {
		if !complete {
			os.RemoveAll(dir)
		}
	}()
	for _, g := range Groups {
		if err := writeJSON(filepath.Join(dir, g+".json"), groups[g]); err != nil {
			return err
		}
	}
	if err := writeJSON(filepath.Join(dir, "manifest.json"), c.Manifest); err != nil {
		return err
	}
	complete = true
	return nil
}

func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}
func (c Catalog) Digest() string { return Hash(c) }
func Validate(c Catalog) error {
	if c.Manifest.FormatVersion != 1 || c.Manifest.CatalogName != "wahb" || c.Manifest.ReleaseVersion == "" || c.Manifest.RequiredMigration != Migration {
		return errors.New("unsupported catalog contract")
	}
	if len(c.Manifest.Files) != len(Groups) {
		return errors.New("manifest must contain exactly the fixed group files")
	}
	seen := map[string]bool{}
	profiles := map[string]string{}
	identities := map[string]bool{}
	cats := map[string]bool{}
	for _, r := range c.Records {
		if r.Key == "" || strings.ContainsAny(r.Key, " \t\n/\\") || !has(Groups, r.Kind) {
			return errors.New("invalid catalog key or kind")
		}
		id := r.Kind + ":" + r.Key
		if seen[id] {
			return errors.New("duplicate catalog key")
		}
		seen[id] = true
		natural := r.Kind + ":" + r.Scope + ":" + identity(r.Kind, r.Definition)
		if identities[natural] {
			return errors.New("duplicate catalog natural identity")
		}
		identities[natural] = true
		if r.Scope != "tenant" && !(r.Kind == "quality-profiles" && r.Scope == "global") {
			return errors.New("invalid scope")
		}
		for _, field := range fields[r.Kind] {
			if _, ok := r.Definition[field]; !ok {
				return fmt.Errorf("incomplete definition: %s.%s", r.Key, field)
			}
		}
		for k, v := range r.Definition {
			if !has(fields[r.Kind], k) {
				return fmt.Errorf("unclassified field: %s.%s", r.Key, k)
			}
			if e := validateType(k, v); e != nil {
				return e
			}
			if e := checkValue(k, v); e != nil {
				return fmt.Errorf("%s: %w", r.Key, e)
			}
		}
		if r.Kind == "news-sources" || r.Kind == "media-sources" {
			category := "news"
			if r.Kind == "media-sources" {
				category = "media"
			}
			if r.Definition["category"] != category || str(r.Definition["name"]) == "" || !has(strings.Fields("RSS WEBSITE TELEGRAM TWITTER PODCAST YOUTUBE UPLOAD MANUAL"), str(r.Definition["type"])) {
				return errors.New("invalid source definition")
			}
			if str(r.Definition["feed_url"]) == "" {
				return errors.New("source requires a portable feed locator")
			}
			for _, k := range []string{"api_config", "metadata"} {
				if r.Definition[k] == nil {
					continue
				}
				m, ok := r.Definition[k].(map[string]any)
				if !ok {
					return errors.New("source JSON must be an object")
				}
				allowed := configFields
				if k == "metadata" {
					allowed = metadataFields
				}
				for key, v := range m {
					if e := validateType(key, v); e != nil {
						return e
					}
					if !has(allowed, key) {
						return fmt.Errorf("unclassified source field: %s.%s", k, key)
					}
					if e := checkValue(key, v); e != nil {
						return e
					}
					n, _ := v.(float64)
					if (key == "min_feed_unit_seconds" && (n < 270 || n > 2400)) || (key == "atomization_min_parent_seconds" && n < 2400) || (key == "hard_max_chapter_minutes" && (n < 4.5 || n > 40)) {
						return errors.New("illegal media policy")
					}
				}
			}
		}
		if r.Kind == "discovery-profiles" {
			if !has([]string{"news", "media"}, str(r.Definition["category"])) || str(r.Definition["name"]) == "" {
				return errors.New("invalid discovery definition")
			}
			profiles[r.Key] = str(r.Definition["category"])
		}
		if r.Kind == "topic-categories" {
			cats[str(r.Definition["slug"])] = true
		}
		if r.Kind == "topics" || r.Kind == "topic-categories" {
			if str(r.Definition["slug"]) == "" || str(r.Definition["label_ar"]) == "" || str(r.Definition["label_en"]) == "" {
				return errors.New("invalid taxonomy definition")
			}
		}
		if r.Kind == "quality-profiles" {
			if str(r.Definition["name"]) == "" || !definitionconfig.VideoCodec(str(r.Definition["video_codec"])) || !definitionconfig.AudioCodec(str(r.Definition["audio_codec"])) || !definitionconfig.Container(str(r.Definition["output_container"])) {
				return errors.New("invalid quality recipe")
			}
		}
	}
	for _, r := range c.Records {
		if r.ProfileKey != "" && profiles[r.ProfileKey] == "" {
			return errors.New("missing discovery profile reference")
		}
		if r.ProfileKey != "" {
			if (r.Kind != "news-sources" && r.Kind != "media-sources") || profiles[r.ProfileKey] != str(r.Definition["category"]) {
				return errors.New("discovery profile category does not match source")
			}
		}
		if r.Kind == "topics" && str(r.Definition["category_slug"]) != "" && !cats[str(r.Definition["category_slug"])] {
			return errors.New("missing topic category")
		}
	}
	for _, g := range []string{"news-sources", "media-sources", "news-discovery", "media-discovery", "topic-categories", "topics", "quality-profiles"} {
		o, ok := c.Manifest.Origins[g]
		want := "stable"
		if g == "media-sources" || g == "media-discovery" {
			want = "dev"
		}
		if !ok || o.Environment != want || o.Tenant == "" || o.ProjectionHash == "" {
			return fmt.Errorf("invalid origin for %s", g)
		}
	}
	for _, g := range []string{"news-sources", "media-sources", "news-discovery", "media-discovery", "topics", "topic-categories", "quality-profiles"} {
		selected := []Record{}
		for _, r := range c.Records {
			if r.Kind == g || (g == "news-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "news") || (g == "media-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "media") {
				selected = append(selected, r)
			}
		}
		sort.Slice(selected, func(i, j int) bool { return selected[i].Key < selected[j].Key })
		if c.Manifest.Origins[g].ProjectionHash != Hash(selected) {
			return fmt.Errorf("origin projection mismatch: %s", g)
		}
	}
	return nil
}
func checkValue(k string, v any) error {
	switch x := v.(type) {
	case string:
		if strings.Contains(x, "://") || has([]string{"feed_url", "image_url", "thumbnail_url", "website_url"}, k) {
			u, e := url.Parse(x)
			if e != nil || u.User != nil || !has([]string{"http", "https"}, u.Scheme) || !portableHost(u.Hostname()) {
				return errors.New("nonportable URL")
			}
			for q := range u.Query() {
				l := strings.ToLower(q)
				if strings.Contains(l, "token") || strings.Contains(l, "secret") || strings.Contains(l, "signature") || strings.Contains(l, "credential") || strings.Contains(l, "key") {
					return errors.New("credential-bearing URL")
				}
			}
		}
	case []any:
		for _, a := range x {
			if e := checkValue(k, a); e != nil {
				return e
			}
		}
	case map[string]any:
		for key, a := range x {
			if e := checkValue(key, a); e != nil {
				return e
			}
		}
	}
	return nil
}
func str(v any) string { s, _ := v.(string); return s }

var booleanFields = strings.Fields("active featured passthrough_allowed remux_allowed chaptering_enabled auto_publish_high_confidence parent_feed_visible preserve_video remove_sponsor_segments audio_only_allowed")
var numberFields = strings.Fields("sort_order max_height target_bitrate_kbps crf audio_bitrate_kbps audio_channels audio_sample_rate_hz max_width max_frame_rate schema_version thumbnail_offset_seconds thumbnail_max_height max_age_hours max_results min_duration_minutes max_duration_minutes min_duration_sec min_text_length initial_atomization_limit min_chapter_minutes min_feed_unit_seconds soft_max_chapter_minutes hard_max_chapter_minutes atomization_min_parent_seconds max_chapters_per_parent high_confidence_threshold")
var arrayFields = strings.Fields("keywords languages media_types allowed_input_mime_types")

func fieldType(k string) string {
	if has(booleanFields, k) {
		return "boolean"
	}
	if has(numberFields, k) {
		return "number"
	}
	if has(arrayFields, k) {
		return "array"
	}
	if k == "api_config" || k == "metadata" {
		return "object"
	}
	return "string"
}
func validateType(k string, v any) error {
	if v == nil {
		if has([]string{"image_url", "thumbnail_url", "website_url", "source_type", "allowed_input_mime_types"}, k) {
			return nil
		}
		return fmt.Errorf("null is not allowed for %s", k)
	}
	valid := false
	switch fieldType(k) {
	case "boolean":
		_, valid = v.(bool)
	case "number":
		n, ok := v.(float64)
		valid = ok && !math.IsInf(n, 0) && !math.IsNaN(n) && n >= 0
		if k != "max_frame_rate" && k != "high_confidence_threshold" {
			valid = valid && n == math.Trunc(n)
		}
		if k == "high_confidence_threshold" {
			valid = valid && n <= 1
		}
		if k == "max_height" || k == "thumbnail_max_height" {
			valid = valid && n <= 4320
		}
		if k == "target_bitrate_kbps" {
			valid = valid && n <= 100000
		}
		if k == "crf" {
			valid = valid && n <= 51
		}
	case "array":
		a, ok := v.([]any)
		valid = ok
		for _, x := range a {
			if _, ok := x.(string); !ok {
				valid = false
			}
		}
	case "object":
		_, valid = v.(map[string]any)
	default:
		_, valid = v.(string)
	}
	if !valid {
		return fmt.Errorf("invalid type/range for %s", k)
	}
	return nil
}
func Schema() map[string]any {
	variants := []any{}
	for _, g := range Groups {
		props := map[string]any{}
		for _, k := range fields[g] {
			typ := fieldType(k)
			p := valueSchema(k)
			if typ == "array" {
				p["items"] = map[string]any{"type": "string"}
			}
			if typ == "object" {
				allowed := configFields
				if k == "metadata" {
					allowed = metadataFields
				}
				nested := map[string]any{}
				for _, n := range allowed {
					nested[n] = valueSchema(n)
				}
				p["properties"] = nested
				p["additionalProperties"] = false
			}
			props[k] = p
		}
		scope := []string{"tenant"}
		if g == "quality-profiles" {
			scope = append(scope, "global")
		}
		variants = append(variants, map[string]any{"type": "object", "additionalProperties": false, "required": []string{"key", "kind", "scope", "definition"}, "properties": map[string]any{"key": map[string]any{"type": "string", "minLength": 1}, "kind": map[string]any{"const": g}, "scope": map[string]any{"enum": scope}, "discovery_profile_key": map[string]any{"type": "string"}, "definition": map[string]any{"type": "object", "additionalProperties": false, "required": fields[g], "properties": props}}})
	}
	return map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema", "title": "Wahb reference group v1; semantic rules additionally enforced by owner validator", "type": "array", "items": map[string]any{"oneOf": variants}}
}

// RemapKeys preserves reviewed permanent identities across an origin rename.
func RemapKeys(c *Catalog, mapping map[string]string) error {
	known := map[string]bool{}
	for _, r := range c.Records {
		known[r.Key] = true
	}
	for key, value := range mapping {
		if !known[key] || strings.TrimSpace(value) == "" {
			return fmt.Errorf("unknown or empty catalog key mapping: %s", key)
		}
	}
	for i := range c.Records {
		r := &c.Records[i]
		if k, ok := mapping[r.Key]; ok {
			r.Key = k
		}
		if k, ok := mapping[r.ProfileKey]; ok {
			r.ProfileKey = k
		}
	}
	for g, o := range c.Manifest.Origins {
		rs := []Record{}
		for _, r := range c.Records {
			if r.Kind == g || (g == "news-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "news") || (g == "media-discovery" && r.Kind == "discovery-profiles" && r.Definition["category"] == "media") {
				rs = append(rs, r)
			}
		}
		sort.Slice(rs, func(i, j int) bool { return rs[i].Key < rs[j].Key })
		o.ProjectionHash = Hash(rs)
		c.Manifest.Origins[g] = o
	}
	return nil
}

func portableHost(host string) bool {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if h == "" || h == "localhost" || strings.HasSuffix(h, ".localhost") || strings.HasSuffix(h, ".local") {
		return false
	}
	if ip := net.ParseIP(h); ip != nil {
		return !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsUnspecified() && !ip.IsLinkLocalUnicast()
	}
	return true
}

func valueSchema(k string) map[string]any {
	typ := fieldType(k)
	if typ == "number" && k != "max_frame_rate" && k != "high_confidence_threshold" {
		typ = "integer"
	}
	types := []string{typ}
	if has([]string{"image_url", "thumbnail_url", "website_url", "source_type", "allowed_input_mime_types"}, k) {
		types = append(types, "null")
	}
	s := map[string]any{"type": types}
	if typ == "integer" || typ == "number" {
		s["minimum"] = 0
	}
	switch k {
	case "high_confidence_threshold":
		s["maximum"] = 1
	case "crf":
		s["maximum"] = 51
	case "max_height", "thumbnail_max_height":
		s["maximum"] = 4320
	case "target_bitrate_kbps":
		s["maximum"] = 100000
	}
	if typ == "array" {
		s["items"] = map[string]any{"type": "string"}
	}
	return s
}
