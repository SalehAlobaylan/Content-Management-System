package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/pipeline"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Delivery policy resolution intentionally has a code-safe fallback. A newly
// migrated tenant must keep serving media even before an operator creates a
// scoped policy, but it must never fabricate an audio-to-MP4 wrapper.
func defaultDeliveryPolicy(mediaKind string) models.MediaDeliveryPolicy {
	return models.MediaDeliveryPolicy{
		MediaKind: mediaKind, ShortFormDelivery: true, AllowNativeAudio: true,
		AllowMP4Fallback: true, AllowHLS: mediaKind == "video",
		AllowPassthrough: true, AllowRemux: true, MaxDeliveryHeight: 720,
		CacheProfile: "immutable-media", Active: true,
	}
}

func deliveryDigest(value any) string {
	raw, _ := json.Marshal(value)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func numericValue(value any) int {
	switch raw := value.(type) {
	case float64:
		return int(raw)
	case float32:
		return int(raw)
	case int:
		return raw
	case json.Number:
		value, _ := raw.Int64()
		return int(value)
	default:
		return 0
	}
}

func renditionStringValue(value any) string {
	parsed, _ := value.(string)
	return parsed
}

func validateV3AudioRenditionContract(rendition map[string]any) error {
	tier := renditionStringValue(rendition["quality_tier"])
	ceiling := map[string]int{"data_saver": 64, "standard": 128, "high": 192}[tier]
	bitrate := numericValue(rendition["bitrate_kbps"])
	mimeType := strings.ToLower(strings.TrimSpace(renditionStringValue(rendition["mime_type"])))
	container := strings.ToLower(strings.TrimSpace(renditionStringValue(rendition["container"])))
	codec := strings.ToLower(strings.TrimSpace(renditionStringValue(rendition["codec"])))
	legalFormat := (mimeType == "audio/mp4" && container == "m4a" && codec == "aac") ||
		(mimeType == "audio/mpeg" && container == "mp3" && codec == "mp3")
	if ceiling == 0 || bitrate <= 0 || bitrate > ceiling || !legalFormat {
		return gorm.ErrInvalidData
	}
	return nil
}

func validateTypedAudioTierSet(renditions []map[string]any) error {
	seen := map[string]bool{}
	hasAudio := false
	for _, rendition := range renditions {
		if renditionStringValue(rendition["type"]) != "audio" {
			continue
		}
		isStrict := numericValue(rendition["schema_version"]) >= 3 ||
			(numericValue(rendition["bitrate_kbps"]) > 0 && renditionStringValue(rendition["container"]) != "" && renditionStringValue(rendition["codec"]) != "")
		if !isStrict {
			continue
		}
		if err := validateV3AudioRenditionContract(rendition); err != nil {
			return err
		}
		tier := renditionStringValue(rendition["quality_tier"])
		if seen[tier] {
			return gorm.ErrInvalidData
		}
		seen[tier] = true
		hasAudio = true
	}
	if hasAudio && !seen["data_saver"] {
		return gorm.ErrInvalidData
	}
	return nil
}

// A rendition generation is the immutable delivery decision for a source
// attempt. It is deliberately created before FFmpeg or storage writes, so a
// retry cannot silently change from native audio to a video wrapper.
type renditionGenerationRequest struct {
	TenantID         string         `json:"tenant_id"`
	ContentItemID    string         `json:"content_item_id"`
	SourceManifestID string         `json:"source_manifest_id"`
	RouteDecision    map[string]any `json:"route_decision"`
	RouteDigest      string         `json:"route_digest"`
	ProbeSnapshot    map[string]any `json:"probe_snapshot"`
	ProbeDigest      string         `json:"probe_digest"`
	PolicySnapshot   map[string]any `json:"policy_snapshot"`
	PolicyDigest     string         `json:"policy_digest"`
	AttemptID        string         `json:"attempt_id"`
	FenceToken       string         `json:"fence_token"`
}

func InternalCreateMediaRenditionGeneration(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req renditionGenerationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid rendition generation"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	contentID, err := uuid.Parse(strings.TrimSpace(req.ContentItemID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content_item_id"})
		return
	}
	if req.RouteDigest == "" || req.ProbeDigest == "" || req.PolicyDigest == "" || deliveryDigest(req.RouteDecision) != req.RouteDigest || deliveryDigest(req.ProbeSnapshot) != req.ProbeDigest || deliveryDigest(req.PolicySnapshot) != req.PolicyDigest {
		c.JSON(http.StatusBadRequest, gin.H{"error": "delivery decision digests do not match snapshots"})
		return
	}
	var item models.ContentItem
	if err := db.Where("public_id=? AND tenant_id=?", contentID, tenant).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "content item not found"})
		return
	}
	var sourceID *uuid.UUID
	if raw := strings.TrimSpace(req.SourceManifestID); raw != "" {
		parsed, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid source_manifest_id"})
			return
		}
		var manifest models.MediaArtifactManifest
		if err := db.Where("public_id=? AND tenant_id=? AND artifact_role='source' AND state IN ?", parsed, tenant, []string{"verified", "active"}).First(&manifest).Error; err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "source manifest is not verified"})
			return
		}
		sourceID = &parsed
	}
	var existing models.MediaRenditionGeneration
	if err := db.Where("tenant_id=? AND content_item_id=? AND route_digest=? AND probe_digest=? AND policy_digest=? AND state IN ?", tenant, contentID, req.RouteDigest, req.ProbeDigest, req.PolicyDigest, []string{"planning", "running", "verifying", "active"}).First(&existing).Error; err == nil {
		c.JSON(http.StatusOK, existing)
		return
	} else if err != gorm.ErrRecordNotFound {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "rendition generation lookup failed"})
		return
	}
	var next int
	_ = db.Model(&models.MediaRenditionGeneration{}).Where("tenant_id=? AND content_item_id=?", tenant, contentID).Select("COALESCE(MAX(generation_number),0)").Scan(&next).Error
	gen := models.MediaRenditionGeneration{PublicID: uuid.New(), TenantID: tenant, ContentItemID: contentID, GenerationNumber: next + 1, SourceManifestID: sourceID, RouteDecision: longFormJSON(req.RouteDecision), RouteDigest: req.RouteDigest, ProbeSnapshot: longFormJSON(req.ProbeSnapshot), ProbeDigest: req.ProbeDigest, PolicySnapshot: longFormJSON(req.PolicySnapshot), PolicyDigest: req.PolicyDigest, RenditionSet: longFormJSON([]any{}), State: "planning", TerminalProof: longFormJSON(map[string]any{})}
	if raw := strings.TrimSpace(req.AttemptID); raw != "" {
		value, e := uuid.Parse(raw)
		if e != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid attempt_id"})
			return
		}
		gen.AttemptID = &value
	}
	if raw := strings.TrimSpace(req.FenceToken); raw != "" {
		value, e := uuid.Parse(raw)
		if e != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid fence_token"})
			return
		}
		gen.FenceToken = &value
	}
	if err := db.Create(&gen).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "rendition generation creation rejected"})
		return
	}
	c.JSON(http.StatusCreated, gen)
}

type renditionGenerationTransitionRequest struct {
	TenantID      string           `json:"tenant_id"`
	RenditionSet  []map[string]any `json:"rendition_set"`
	TerminalProof map[string]any   `json:"terminal_proof"`
	FenceToken    string           `json:"fence_token"`
}

// validateRenditionSet makes activation an ownership boundary rather than a
// worker assertion. Every serving URL must be backed by a verified manifest of
// the same tenant/content item; HLS additionally requires a verified package.
func validateRenditionSet(tx *gorm.DB, gen *models.MediaRenditionGeneration, tenant string, renditions []map[string]any) error {
	if len(renditions) == 0 {
		return gorm.ErrInvalidData
	}
	if err := validateTypedAudioTierSet(renditions); err != nil {
		return err
	}
	seen := map[string]bool{}
	hasServing := false
	for _, rendition := range renditions {
		typ, _ := rendition["type"].(string)
		url, _ := rendition["url"].(string)
		manifestRaw, _ := rendition["manifest_id"].(string)
		if typ == "" || manifestRaw == "" || (typ != "source" && strings.TrimSpace(url) == "") {
			return gorm.ErrInvalidData
		}
		if numericValue(rendition["schema_version"]) >= 3 {
			if strings.TrimSpace(renditionStringValue(rendition["id"])) == "" ||
				strings.TrimSpace(renditionStringValue(rendition["role"])) == "" ||
				strings.TrimSpace(renditionStringValue(rendition["quality_tier"])) == "" ||
				renditionStringValue(rendition["rendition_generation_id"]) != gen.PublicID.String() ||
				renditionStringValue(rendition["policy_digest"]) != gen.PolicyDigest ||
				renditionStringValue(rendition["probe_digest"]) != gen.ProbeDigest {
				return gorm.ErrInvalidData
			}
		}
		if typ != "source" && typ != "audio" && typ != "mp4" && typ != "hls" {
			return gorm.ErrInvalidData
		}
		if seen[manifestRaw] {
			return gorm.ErrInvalidData
		}
		seen[manifestRaw] = true
		manifestID, err := uuid.Parse(manifestRaw)
		if err != nil {
			return gorm.ErrInvalidData
		}
		var manifest models.MediaArtifactManifest
		if err = tx.Where("public_id=? AND tenant_id=? AND content_item_id=? AND state IN ?", manifestID, tenant, gen.ContentItemID, []string{"verified", "active"}).First(&manifest).Error; err != nil {
			return err
		}
		if typ != "source" && manifest.PublicURL != url {
			return gorm.ErrInvalidData
		}
		if typ == "audio" {
			manifestType := strings.ToLower(strings.TrimSpace(manifest.ContentType))
			renditionType := strings.ToLower(strings.TrimSpace(renditionStringValue(rendition["mime_type"])))
			if manifestType != renditionType || (manifest.ArtifactRole != "delivery_audio" && manifest.ArtifactRole != "source") {
				return gorm.ErrInvalidData
			}
		}
		if typ == "hls" {
			if manifest.ArtifactRole != "hls_master" && manifest.ArtifactRole != "hls_access_master" {
				return gorm.ErrInvalidData
			}
			var pkg models.MediaHLSPackage
			packageQuery := tx.Where("tenant_id=? AND rendition_generation_id=? AND state IN ?", tenant, gen.PublicID, []string{"verified", "active"})
			if manifest.ArtifactRole == "hls_master" {
				packageQuery = packageQuery.Where("master_manifest_id=?", manifestID)
			}
			if err = packageQuery.First(&pkg).Error; err != nil {
				return err
			}
			if manifest.ArtifactRole == "hls_access_master" {
				var access models.MediaHLSAccessPoint
				if err = tx.Where("tenant_id=? AND package_id=? AND manifest_id=? AND state IN ?", tenant, pkg.PublicID, manifestID, []string{"verified", "active"}).First(&access).Error; err != nil {
					return err
				}
				if renditionStringValue(rendition["package_id"]) != pkg.PublicID.String() ||
					renditionStringValue(rendition["validation_digest"]) != pkg.ValidationDigest ||
					access.ValidationDigest != pkg.ValidationDigest {
					return gorm.ErrInvalidData
				}
			}
		}
		if typ != "source" {
			hasServing = true
		}
	}
	if !hasServing && gen.RouteDecision != nil && !strings.Contains(string(gen.RouteDecision), "source_only_long_form") {
		return gorm.ErrInvalidData
	}
	return nil
}

func selectPrimaryAndFallback(renditions []map[string]any) (map[string]any, map[string]any) {
	var primary map[string]any
	for _, rendition := range renditions {
		if rendition["is_primary"] == true {
			primary = rendition
			break
		}
	}
	if primary == nil {
		for _, rendition := range renditions {
			if renditionStringValue(rendition["type"]) != "source" {
				primary = rendition
				break
			}
		}
	}
	if primary == nil {
		return nil, nil
	}
	primaryID := renditionStringValue(primary["id"])
	requestedFallbackID := renditionStringValue(primary["fallback_rendition_id"])
	for _, rendition := range renditions {
		if renditionStringValue(rendition["id"]) == primaryID || renditionStringValue(rendition["type"]) == "source" {
			continue
		}
		if requestedFallbackID != "" && renditionStringValue(rendition["id"]) == requestedFallbackID {
			return primary, rendition
		}
	}
	for _, rendition := range renditions {
		typ := renditionStringValue(rendition["type"])
		if renditionStringValue(rendition["id"]) != primaryID && (typ == "mp4" || typ == "audio") {
			return primary, rendition
		}
	}
	return primary, nil
}

func InternalTransitionMediaRenditionGeneration(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	state := c.Param("state")
	if state != "running" && state != "verifying" && state != "failed" && state != "uncertain" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid rendition generation state"})
		return
	}
	var req renditionGenerationTransitionRequest
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid rendition generation transition"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid generation id"})
		return
	}
	var gen models.MediaRenditionGeneration
	if err = db.Where("public_id=? AND tenant_id=?", id, tenant).First(&gen).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "rendition generation not found"})
		return
	}
	if gen.FenceToken != nil && strings.TrimSpace(req.FenceToken) != gen.FenceToken.String() {
		c.JSON(http.StatusConflict, gin.H{"error": "stale rendition generation fence"})
		return
	}
	legal := map[string][]string{
		"running":   {"planning"},
		"verifying": {"running"},
		"failed":    {"planning", "running", "verifying"},
		"uncertain": {"planning", "running", "verifying"},
	}
	allowed := false
	for _, prior := range legal[state] {
		if gen.State == prior {
			allowed = true
			break
		}
	}
	if !allowed {
		c.JSON(http.StatusConflict, gin.H{"error": "illegal rendition generation transition"})
		return
	}
	updates := map[string]any{"state": state, "updated_at": time.Now().UTC()}
	if len(req.RenditionSet) > 0 {
		if err := validateRenditionSet(db, &gen, tenant, req.RenditionSet); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "rendition set is not manifest-verified"})
			return
		}
		digest := deliveryDigest(req.RenditionSet)
		updates["rendition_set"] = longFormJSON(req.RenditionSet)
		updates["rendition_digest"] = digest
	}
	if req.TerminalProof != nil {
		updates["terminal_proof"] = longFormJSON(req.TerminalProof)
	}
	if err = db.Model(&gen).Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "rendition generation transition failed"})
		return
	}
	db.First(&gen, gen.ID)
	c.JSON(http.StatusOK, gen)
}

func InternalActivateMediaRenditionGeneration(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req renditionGenerationTransitionRequest
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid activation receipt"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid generation id"})
		return
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		var gen models.MediaRenditionGeneration
		if e := tx.Where("public_id=? AND tenant_id=?", id, tenant).First(&gen).Error; e != nil {
			return e
		}
		if gen.State != "verifying" || gen.RenditionDigest == "" {
			return gorm.ErrInvalidData
		}
		var renditions []map[string]any
		if json.Unmarshal(gen.RenditionSet, &renditions) != nil || validateRenditionSet(tx, &gen, tenant, renditions) != nil {
			return gorm.ErrInvalidData
		}
		if gen.FenceToken != nil && strings.TrimSpace(req.FenceToken) != gen.FenceToken.String() {
			return gorm.ErrInvalidData
		}
		var content models.ContentItem
		if err := tx.Where("public_id=? AND tenant_id=?", gen.ContentItemID, tenant).First(&content).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		if e := tx.Model(&models.MediaRenditionGeneration{}).Where("tenant_id=? AND content_item_id=? AND state='active'", tenant, gen.ContentItemID).Updates(map[string]any{"state": "superseded", "updated_at": now}).Error; e != nil {
			return e
		}
		if err := tx.Model(&gen).Updates(map[string]any{"state": "active", "activation_at": now, "updated_at": now, "terminal_proof": longFormJSON(req.TerminalProof)}).Error; err != nil {
			return err
		}
		// Content rows point only at an active generation. Source-only long
		// parents intentionally do not receive playback fields; their chapters
		// become the future serving units after atomization activation.
		updates := map[string]any{"active_media_rendition_generation_id": gen.PublicID, "updated_at": now, "rendition_digest": gen.RenditionDigest}
		// A generation is the only source of public playback projection. Honor a
		// typed primary/fallback relationship rather than whichever map happened
		// to be first after JSON serialization.
		primary, fallback := selectPrimaryAndFallback(renditions)
		if primary != nil {
			kind, _ := primary["type"].(string)
			url, _ := primary["url"].(string)
			if kind != "source" && strings.TrimSpace(url) != "" {
				updates["playback_url"], updates["playback_type"] = url, kind
				if fallback != nil {
					if fallbackURL, _ := fallback["url"].(string); fallbackURL != "" {
						updates["fallback_playback_url"] = fallbackURL
					}
				}
				version := 2
				for _, rendition := range renditions {
					if numericValue(rendition["schema_version"]) >= 3 {
						version = 3
						break
					}
				}
				updates["media_renditions"], updates["rendition_set_version"] = longFormJSON(renditions), version
				if content.MediaSuitability == "visual_dependent" {
					updates["delivery_class"] = "visual_dependent"
				} else if kind == "audio" {
					updates["delivery_class"] = "audio_only"
				} else {
					updates["delivery_class"] = "audio_first_visual"
				}
			}
		}
		return tx.Model(&models.ContentItem{}).Where("public_id=? AND tenant_id=?", gen.ContentItemID, tenant).Updates(updates).Error
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "rendition activation rejected"})
		return
	}
	var gen models.MediaRenditionGeneration
	db.Where("public_id=?", id).First(&gen)
	c.JSON(http.StatusOK, gen)
}

func InternalGetMediaRenditionGeneration(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenant := strings.TrimSpace(c.DefaultQuery("tenant_id", "default"))
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid generation id"})
		return
	}
	var gen models.MediaRenditionGeneration
	if err = db.Where("public_id=? AND tenant_id=?", id, tenant).First(&gen).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "rendition generation not found"})
		return
	}
	c.JSON(http.StatusOK, gen)
}

type hlsPackageRequest struct {
	TenantID              string         `json:"tenant_id"`
	RenditionGenerationID string         `json:"rendition_generation_id"`
	MasterManifestID      string         `json:"master_manifest_id"`
	ProgressiveManifestID string         `json:"progressive_manifest_id"`
	VariantCount          int            `json:"variant_count"`
	ValidationEvidence    map[string]any `json:"validation_evidence"`
	ValidationDigest      string         `json:"validation_digest"`
}

func InternalCreateMediaHLSPackage(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req hlsPackageRequest
	if c.ShouldBindJSON(&req) != nil || req.VariantCount < 2 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "HLS package requires at least two variants"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	genID, genErr := uuid.Parse(strings.TrimSpace(req.RenditionGenerationID))
	masterID, masterErr := uuid.Parse(strings.TrimSpace(req.MasterManifestID))
	if genErr != nil || masterErr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid HLS package identity"})
		return
	}
	var gen models.MediaRenditionGeneration
	var master models.MediaArtifactManifest
	if db.Where("public_id=? AND tenant_id=? AND state IN ?", genID, tenant, []string{"running", "verifying"}).First(&gen).Error != nil || db.Where("public_id=? AND tenant_id=? AND artifact_role='hls_master' AND state IN ?", masterID, tenant, []string{"verified", "active"}).First(&master).Error != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "HLS generation or master manifest is not verified"})
		return
	}
	pkg := models.MediaHLSPackage{PublicID: uuid.New(), TenantID: tenant, RenditionGenerationID: genID, MasterManifestID: masterID, VariantCount: req.VariantCount, State: "uploading", ValidationEvidence: longFormJSON(map[string]any{})}
	if raw := strings.TrimSpace(req.ProgressiveManifestID); raw != "" {
		id, e := uuid.Parse(raw)
		if e != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid progressive_manifest_id"})
			return
		}
		pkg.ProgressiveManifestID = &id
	}
	if err := db.Where("tenant_id=? AND rendition_generation_id=?", tenant, genID).FirstOrCreate(&pkg).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "HLS package creation rejected"})
		return
	}
	c.JSON(http.StatusCreated, pkg)
}

func InternalVerifyMediaHLSPackage(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req hlsPackageRequest
	if c.ShouldBindJSON(&req) != nil || req.VariantCount < 2 || req.ValidationDigest == "" || deliveryDigest(req.ValidationEvidence) != req.ValidationDigest {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid HLS validation evidence"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid HLS package id"})
		return
	}
	var pkg models.MediaHLSPackage
	if err = db.Where("public_id=? AND tenant_id=?", id, tenant).First(&pkg).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "HLS package not found"})
		return
	}
	if err = db.Model(&pkg).Updates(map[string]any{"state": "verified", "variant_count": req.VariantCount, "validation_evidence": longFormJSON(req.ValidationEvidence), "validation_digest": req.ValidationDigest, "updated_at": time.Now().UTC()}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "HLS package verification failed"})
		return
	}
	db.First(&pkg, pkg.ID)
	c.JSON(http.StatusOK, pkg)
}

// An access point is a smaller, verified master playlist over a canonical
// package. It may only reference the package's owned CMAF objects; callers
// never get authority to manufacture a quality tier from arbitrary URLs.
type hlsAccessPointRequest struct {
	TenantID         string `json:"tenant_id"`
	PackageID        string `json:"package_id"`
	QualityTier      string `json:"quality_tier"`
	ManifestID       string `json:"manifest_id"`
	MaxHeight        int    `json:"max_height"`
	MaxBandwidthKbps int    `json:"max_bandwidth_kbps"`
	ValidationDigest string `json:"validation_digest"`
}

func InternalCreateMediaHLSAccessPoint(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req hlsAccessPointRequest
	if c.ShouldBindJSON(&req) != nil || (req.QualityTier != "standard" && req.QualityTier != "high") || req.MaxHeight <= 0 || req.MaxBandwidthKbps <= 0 || len(strings.TrimSpace(req.ValidationDigest)) != 64 ||
		(req.QualityTier == "standard" && (req.MaxHeight > 540 || req.MaxBandwidthKbps > 1200)) ||
		(req.QualityTier == "high" && (req.MaxHeight > 720 || req.MaxBandwidthKbps > 2500)) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid HLS access point"})
		return
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	packageID, packageErr := uuid.Parse(strings.TrimSpace(req.PackageID))
	manifestID, manifestErr := uuid.Parse(strings.TrimSpace(req.ManifestID))
	if packageErr != nil || manifestErr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid HLS access point identity"})
		return
	}
	var pkg models.MediaHLSPackage
	var manifest models.MediaArtifactManifest
	if db.Where("public_id=? AND tenant_id=? AND state='verified'", packageID, tenant).First(&pkg).Error != nil ||
		db.Where("public_id=? AND tenant_id=? AND artifact_role='hls_access_master' AND state IN ?", manifestID, tenant, []string{"verified", "active"}).First(&manifest).Error != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "HLS package or access master is not verified"})
		return
	}
	if req.ValidationDigest != pkg.ValidationDigest || manifest.PackageManifestID == nil || *manifest.PackageManifestID != pkg.MasterManifestID {
		c.JSON(http.StatusConflict, gin.H{"error": "HLS access point proof does not match its package"})
		return
	}
	access := models.MediaHLSAccessPoint{PublicID: uuid.New(), TenantID: tenant, PackageID: packageID, QualityTier: req.QualityTier, ManifestID: manifestID, MaxHeight: req.MaxHeight, MaxBandwidthKbps: req.MaxBandwidthKbps, ValidationDigest: req.ValidationDigest, State: "verified"}
	if err := db.Where("tenant_id=? AND package_id=? AND quality_tier=?", tenant, packageID, req.QualityTier).FirstOrCreate(&access).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "HLS access point creation rejected"})
		return
	}
	c.JSON(http.StatusCreated, access)
}

// Delivery inventory is intentionally read-only. It classifies only records
// with explicit CMS evidence; unmatched storage is never inferred, relabeled
// or eligible for automatic cleanup.
func AdminMediaDeliveryInventory(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenant := principal.TenantID
	var rows []struct {
		ContentItemID uuid.UUID `json:"content_item_id"`
		Metadata      []byte    `json:"metadata"`
		MediaURL      *string   `json:"media_url"`
		PlaybackURL   *string   `json:"playback_url"`
		PlaybackType  string    `json:"playback_type"`
	}
	if err := db.Table("content_items").Select("public_id AS content_item_id, metadata, media_url, playback_url, playback_type").Where("tenant_id=? AND type IN ?", tenant, []string{"VIDEO", "PODCAST"}).Order("updated_at DESC").Limit(500).Scan(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delivery inventory failed"})
		return
	}
	items := make([]gin.H, 0, len(rows))
	for _, row := range rows {
		var metadata map[string]any
		_ = json.Unmarshal(row.Metadata, &metadata)
		classification := "healthy"
		if row.MediaURL == nil || strings.TrimSpace(*row.MediaURL) == "" {
			classification = "missing_proven_source"
		} else if row.PlaybackURL == nil || strings.TrimSpace(*row.PlaybackURL) == "" {
			classification = "missing_playback_or_long_parent"
		} else if row.PlaybackType == "hls" {
			classification = "legacy_or_unverified_hls"
		}
		if _, proven := metadata["source_artifact_manifest_id"].(string); !proven && classification == "healthy" {
			classification = "unproven_object"
		}
		if renditions, ok := metadata["media_renditions"].([]any); ok && len(renditions) == 0 {
			classification = "malformed_renditions"
		}
		if static, ok := metadata["static_image_mp4"].(bool); ok && static {
			classification = "static_video_mp4"
		}
		if attached, ok := metadata["attached_artwork"].(bool); ok && attached {
			classification = "attached_artwork_misclassified"
		}
		if row.PlaybackType == "mp4" {
			if native, ok := metadata["native_audio_manifest_id"].(string); !ok || native == "" {
				classification = "missing_native_audio"
			}
		}
		items = append(items, gin.H{"content_item_id": row.ContentItemID, "classification": classification, "media_url": row.MediaURL, "playback_url": row.PlaybackURL, "playback_type": row.PlaybackType, "metadata": metadata})
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "read_only": true, "limit": 500})
}

func validateDeliveryPolicy(policy *models.MediaDeliveryPolicy) error {
	if policy.MediaKind != "audio" && policy.MediaKind != "video" {
		return gorm.ErrInvalidData
	}
	if policy.Name == "" || (policy.PrimaryMode != "audio" && policy.PrimaryMode != "progressive" && policy.PrimaryMode != "hls") {
		return gorm.ErrInvalidData
	}
	if policy.HLSSegmentDurationSec != 6 || policy.HLSSegmentFormat != "cmaf" {
		return gorm.ErrInvalidData
	}
	if policy.AllowHLS && policy.MediaKind == "video" && policy.HLSMinVariants < 2 {
		return gorm.ErrInvalidData
	}
	if policy.PrimaryMode == "hls" && (!policy.AllowHLS || !policy.GenerateProgressiveFallback) {
		return gorm.ErrInvalidData
	}
	seenVariants := map[string]bool{}
	enabledHLSVariants := 0
	for _, variant := range policy.Variants {
		if (variant.RenditionType != "audio" && variant.RenditionType != "progressive" && variant.RenditionType != "hls") ||
			(variant.QualityTier != "data_saver" && variant.QualityTier != "standard" && variant.QualityTier != "high") ||
			(policy.MediaKind == "audio" && variant.RenditionType != "audio") ||
			(policy.MediaKind == "video" && variant.RenditionType == "hls" && !policy.AllowHLS) {
			return gorm.ErrInvalidData
		}
		identity := variant.RenditionType + ":" + variant.QualityTier
		if seenVariants[identity] {
			return gorm.ErrInvalidData
		}
		seenVariants[identity] = true
		if variant.Enabled && variant.RenditionType == "hls" {
			enabledHLSVariants++
		}
	}
	if policy.PrimaryMode == "hls" && enabledHLSVariants < 2 {
		return gorm.ErrInvalidData
	}
	return nil
}

func AdminListMediaDeliveryPolicies(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var policies []models.MediaDeliveryPolicy
	q := db.Preload("Variants", "enabled = ?", true).
		Where("tenant_id=? OR tenant_id IS NULL", principal.TenantID).
		Order("active DESC, media_kind, name")
	if err := q.Find(&policies).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delivery policy list failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": policies})
}
func AdminCreateMediaDeliveryPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var policy models.MediaDeliveryPolicy
	if c.ShouldBindJSON(&policy) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid delivery policy"})
		return
	}
	policy.ID = 0
	policy.PublicID = uuid.New()
	policy.TenantID = &principal.TenantID
	policy.CreatedBy = principal.Email
	policy.UpdatedBy = principal.Email
	policy.Name = strings.TrimSpace(policy.Name)
	if policy.RolloutState == "" {
		policy.RolloutState = "shadow"
	}
	if policy.HLSSegmentDurationSec == 0 {
		policy.HLSSegmentDurationSec = 6
	}
	if policy.HLSSegmentFormat == "" {
		policy.HLSSegmentFormat = "cmaf"
	}
	if policy.HLSMinVariants == 0 {
		policy.HLSMinVariants = 2
	}
	policy.PolicyDigest = deliveryDigest(policy)
	if err := validateDeliveryPolicy(&policy); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported or incomplete delivery policy"})
		return
	}
	if err := db.Create(&policy).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "delivery policy scope already exists"})
		return
	}
	c.JSON(http.StatusCreated, policy)
}
func AdminUpdateMediaDeliveryPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid delivery policy id"})
		return
	}
	var existing models.MediaDeliveryPolicy
	if db.Preload("Variants").Where("public_id=? AND tenant_id=?", id, principal.TenantID).First(&existing).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "delivery policy not found"})
		return
	}
	var patch map[string]any
	if c.ShouldBindJSON(&patch) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid delivery policy"})
		return
	}
	// Policy edits are partial in Console. Merge onto the stored immutable
	// shape first so omitted booleans/variants can never be reset to zero.
	stored, _ := json.Marshal(existing)
	for key, value := range patch {
		var merged map[string]any
		_ = json.Unmarshal(stored, &merged)
		merged[key] = value
		stored, _ = json.Marshal(merged)
	}
	var candidate models.MediaDeliveryPolicy
	if json.Unmarshal(stored, &candidate) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid delivery policy patch"})
		return
	}
	candidate.ID, candidate.PublicID, candidate.CreatedAt, candidate.CreatedBy = existing.ID, existing.PublicID, existing.CreatedAt, existing.CreatedBy
	candidate.TenantID = &principal.TenantID
	candidate.UpdatedBy = principal.Email
	candidate.PolicyDigest = deliveryDigest(candidate)
	if err := validateDeliveryPolicy(&candidate); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported or incomplete delivery policy"})
		return
	}
	_, replacesVariants := patch["variants"]
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Omit("Variants").Save(&candidate).Error; err != nil {
			return err
		}
		if !replacesVariants {
			return nil
		}
		if err := tx.Where("policy_id=?", existing.ID).Delete(&models.MediaDeliveryPolicyVariant{}).Error; err != nil {
			return err
		}
		for index := range candidate.Variants {
			candidate.Variants[index].ID = 0
			candidate.Variants[index].PolicyID = existing.ID
		}
		if len(candidate.Variants) == 0 {
			return nil
		}
		return tx.Create(&candidate.Variants).Error
	}); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "delivery policy update rejected"})
		return
	}
	c.JSON(http.StatusOK, candidate)
}
func AdminSetMediaDeliveryPolicyActive(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid delivery policy id"})
		return
	}
	var body struct {
		Active bool `json:"active"`
	}
	if c.ShouldBindJSON(&body) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid activation"})
		return
	}
	if db.Model(&models.MediaDeliveryPolicy{}).Where("public_id=? AND tenant_id=?", id, principal.TenantID).Updates(map[string]any{"active": body.Active, "updated_at": time.Now().UTC(), "updated_by": principal.Email}).RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "delivery policy not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

// Preview is intentionally side-effect free. Operators can see the resolved
// policy scope and whether the requested HLS route is legal before creating a
// repair request; no queue or storage authority crosses this boundary.
func AdminPreviewMediaDeliveryRoute(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content id"})
		return
	}
	var item models.ContentItem
	if db.Where("public_id=? AND tenant_id=?", id, principal.TenantID).First(&item).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "content item not found"})
		return
	}
	kind := "video"
	if item.Type == models.ContentTypePodcast {
		kind = "audio"
	}
	policy := defaultDeliveryPolicy(kind)
	var matching []models.MediaDeliveryPolicy
	db.Preload("Variants", "enabled=?", true).Where("active=? AND media_kind=? AND (tenant_id=? OR tenant_id IS NULL)", true, kind, principal.TenantID).Find(&matching)
	if len(matching) > 0 {
		policy = matching[0]
	}
	route := "progressive_transcode"
	if item.DurationSec != nil && *item.DurationSec > 2400 {
		route = "source_only_long_form"
	} else if kind == "audio" {
		route = "audio_passthrough"
	} else if policy.AllowHLS && policy.RolloutState == "active" && policy.HLSMinVariants >= 2 {
		route = "adaptive_hls_transcode"
	}
	response := gin.H{"content_item_id": item.PublicID, "policy": policy, "route": route, "requires_new_generation": true, "side_effect_free": true}
	if candidate, source, candidateErr := pipeline.MediaDeliveryCandidate(db, item.TenantID, item.PublicID); candidateErr == nil {
		response["repair_preview"] = gin.H{"preview_digest": candidate.EvidenceDigest, "source_manifest_id": source.PublicID, "source_bytes": source.SizeBytes, "proposed_route": route, "estimated_renditions": []string{"native_audio", "progressive_or_hls"}, "requires_new_generation": true}
	} else {
		response["repair_unavailable_reason"] = "no proven source manifest"
	}
	c.JSON(http.StatusOK, response)
}

type mediaDeliveryRepairRequest struct {
	ContentItemID string `json:"content_item_id"`
	PreviewDigest string `json:"preview_digest"`
}

// AdminRequestMediaDeliveryRepair is the only Console mutation entry point.
// It accepts a previously issued immutable preview digest, never a URL, queue
// name, generation id, or storage key.
func AdminRequestMediaDeliveryRepair(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req mediaDeliveryRepairRequest
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid delivery repair request"})
		return
	}
	itemID, err := uuid.Parse(strings.TrimSpace(req.ContentItemID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content item id"})
		return
	}
	repair, err := pipeline.CreateMediaDeliveryRepair(c.MustGet("db").(*gorm.DB), principal.TenantID, itemID, strings.TrimSpace(req.PreviewDigest), principal.Email)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "delivery repair request rejected"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"repair": repair, "preview_digest": req.PreviewDigest})
}

func AdminGetMediaDeliveryRepair(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid repair id"})
		return
	}
	var repair models.PipelineRepairRequest
	if err = c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND public_id=? AND stage=?", principal.TenantID, id, models.PipelineStageMediaDeliveryGeneration).First(&repair).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "delivery repair not found"})
		return
	}
	var attempts []models.PipelineRepairAttempt
	c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND repair_request_id=?", principal.TenantID, repair.PublicID).Order("attempt_number DESC").Find(&attempts)
	c.JSON(http.StatusOK, gin.H{"repair": repair, "attempts": attempts})
}

func AdminListMediaDeliveryRepairs(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := 25
	if parsed, err := strconv.Atoi(c.DefaultQuery("limit", "25")); err == nil && parsed > 0 {
		limit = min(parsed, 50)
	}
	var repairs []models.PipelineRepairRequest
	if err := db.Where("tenant_id=? AND stage=?", principal.TenantID, models.PipelineStageMediaDeliveryGeneration).Order("updated_at DESC").Limit(limit).Find(&repairs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delivery repair history failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"repairs": repairs, "limit": limit})
}

// AdminMediaDeliveryDiagnostics is deliberately a compact drill-down instead
// of a storage browser. It returns CMS-persisted proof only and keeps object
// store credentials and queue state outside the mobile contract.
func AdminMediaDeliveryDiagnostics(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	itemID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid content item id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var item models.ContentItem
	if err = db.Where("tenant_id=? AND public_id=?", principal.TenantID, itemID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "content item not found"})
		return
	}
	var generations []models.MediaRenditionGeneration
	db.Where("tenant_id=? AND content_item_id=?", principal.TenantID, itemID).Order("generation_number DESC").Find(&generations)
	generationIDs := make([]uuid.UUID, 0, len(generations))
	for _, generation := range generations {
		generationIDs = append(generationIDs, generation.PublicID)
	}
	var packages []models.MediaHLSPackage
	var accessPoints []models.MediaHLSAccessPoint
	if len(generationIDs) > 0 {
		db.Where("tenant_id=? AND rendition_generation_id IN ?", principal.TenantID, generationIDs).Find(&packages)
		packageIDs := make([]uuid.UUID, 0, len(packages))
		for _, pkg := range packages {
			packageIDs = append(packageIDs, pkg.PublicID)
		}
		if len(packageIDs) > 0 {
			db.Where("tenant_id=? AND package_id IN ?", principal.TenantID, packageIDs).Find(&accessPoints)
		}
	}
	c.JSON(http.StatusOK, gin.H{"content_item": gin.H{"id": item.PublicID, "active_rendition_generation_id": item.ActiveMediaRenditionGenerationID, "rendition_set_version": item.RenditionSetVersion, "rendition_digest": item.RenditionDigest, "delivery_class": item.DeliveryClass}, "generations": generations, "hls_packages": packages, "hls_access_points": accessPoints})
}

// AdminRollbackMediaDeliveryGeneration reactivates only a still-verified
// superseded generation. It is non-destructive: the failed active generation
// stays recorded and no artifact is deleted or made cleanup-eligible here.
func AdminRollbackMediaDeliveryGeneration(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	targetID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid generation id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	err = db.Transaction(func(tx *gorm.DB) error {
		var target models.MediaRenditionGeneration
		if err := tx.Where("tenant_id=? AND public_id=? AND state='superseded'", principal.TenantID, targetID).First(&target).Error; err != nil {
			return err
		}
		var renditions []map[string]any
		if json.Unmarshal(target.RenditionSet, &renditions) != nil || validateRenditionSet(tx, &target, principal.TenantID, renditions) != nil {
			return gorm.ErrInvalidData
		}
		now := time.Now().UTC()
		if err := tx.Model(&models.MediaRenditionGeneration{}).Where("tenant_id=? AND content_item_id=? AND state='active'", principal.TenantID, target.ContentItemID).Updates(map[string]any{"state": "superseded", "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&target).Updates(map[string]any{"state": "active", "activation_at": now, "updated_at": now, "terminal_proof": longFormJSON(map[string]any{"rollback": true, "actor": principal.Email})}).Error; err != nil {
			return err
		}
		primary, fallback := selectPrimaryAndFallback(renditions)
		updates := map[string]any{"active_media_rendition_generation_id": target.PublicID, "rendition_digest": target.RenditionDigest, "updated_at": now}
		if primary != nil {
			if url, _ := primary["url"].(string); url != "" {
				updates["playback_url"] = url
				updates["playback_type"], _ = primary["type"].(string)
				updates["media_renditions"] = longFormJSON(renditions)
				if numericValue(primary["schema_version"]) >= 3 {
					updates["rendition_set_version"] = 3
				} else {
					updates["rendition_set_version"] = 2
				}
				if fallback != nil {
					updates["fallback_playback_url"], _ = fallback["url"].(string)
				}
				var content models.ContentItem
				if err := tx.Where("tenant_id=? AND public_id=?", principal.TenantID, target.ContentItemID).First(&content).Error; err != nil {
					return err
				}
				kind := renditionStringValue(primary["type"])
				if content.MediaSuitability == "visual_dependent" {
					updates["delivery_class"] = "visual_dependent"
				} else if kind == "audio" {
					updates["delivery_class"] = "audio_only"
				} else {
					updates["delivery_class"] = "audio_first_visual"
				}
			}
		}
		return tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=?", principal.TenantID, target.ContentItemID).Updates(updates).Error
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "generation rollback rejected"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "rolled_back", "generation_id": targetID})
}

func InternalResolveMediaDeliveryPolicy(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.DefaultQuery("tenant_id", "default"))
	sourceType := strings.TrimSpace(c.Query("source_type"))
	mediaKind := strings.TrimSpace(c.DefaultQuery("media_kind", "video"))
	if mediaKind != "audio" && mediaKind != "video" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "media_kind must be audio or video"})
		return
	}
	suitability := strings.TrimSpace(c.Query("suitability"))
	shortForm := strings.TrimSpace(c.DefaultQuery("short_form", "true")) != "false"
	var candidates []models.MediaDeliveryPolicy
	q := db.Preload("Variants", "enabled = ?", true).Where("active = ? AND media_kind = ? AND short_form_delivery = ?", true, mediaKind, shortForm)
	if err := q.Where("(tenant_id = ? OR tenant_id IS NULL) AND (source_type = ? OR source_type IS NULL) AND (suitability = ? OR suitability IS NULL)", tenantID, sourceType, suitability).Find(&candidates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delivery policy lookup failed"})
		return
	}
	if len(candidates) == 0 {
		c.JSON(http.StatusOK, gin.H{"policy": defaultDeliveryPolicy(mediaKind), "matched_on": "safe_default"})
		return
	}
	best := candidates[0]
	bestScore := -1
	for _, candidate := range candidates {
		score := 0
		if candidate.TenantID != nil {
			score += 4
		}
		if candidate.SourceType != nil {
			score += 2
		}
		if candidate.Suitability != nil {
			score++
		}
		if score > bestScore {
			best, bestScore = candidate, score
		}
	}
	c.JSON(http.StatusOK, gin.H{"policy": best, "matched_on": "scoped"})
}

func GetPlaybackPreferences(c *gin.Context) {
	uid, ok := authedUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	pref := models.UserPlaybackPreference{TenantID: "default", UserID: uid, AudioQuality: "standard", StreamingQuality: "auto", PreferAudioWhenAvailable: true}
	if err := db.Where("tenant_id = ? AND user_id = ?", pref.TenantID, uid).FirstOrCreate(&pref).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to load playback preferences"})
		return
	}
	c.JSON(http.StatusOK, pref)
}

type updatePlaybackPreferencesRequest struct {
	AudioQuality             string `json:"audio_quality"`
	StreamingQuality         string `json:"streaming_quality"`
	AllowCellularHighQuality *bool  `json:"allow_cellular_high_quality"`
	PreferAudioWhenAvailable *bool  `json:"prefer_audio_when_available"`
}

func UpdatePlaybackPreferences(c *gin.Context) {
	uid, ok := authedUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	var req updatePlaybackPreferencesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid playback preferences"})
		return
	}
	updates := map[string]any{}
	if req.AudioQuality != "" {
		if req.AudioQuality != "data_saver" && req.AudioQuality != "standard" && req.AudioQuality != "high" {
			c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid audio_quality"})
			return
		}
		updates["audio_quality"] = req.AudioQuality
	}
	if req.StreamingQuality != "" {
		if req.StreamingQuality != "auto" && req.StreamingQuality != "data_saver" && req.StreamingQuality != "standard" && req.StreamingQuality != "high" {
			c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid streaming_quality"})
			return
		}
		updates["streaming_quality"] = req.StreamingQuality
	}
	if req.AllowCellularHighQuality != nil {
		updates["allow_cellular_high_quality"] = *req.AllowCellularHighQuality
	}
	if req.PreferAudioWhenAvailable != nil {
		updates["prefer_audio_when_available"] = *req.PreferAudioWhenAvailable
	}
	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "No playback preference supplied"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	pref := models.UserPlaybackPreference{TenantID: "default", UserID: uid, AudioQuality: "standard", StreamingQuality: "auto", PreferAudioWhenAvailable: true}
	if err := db.Where("tenant_id = ? AND user_id = ?", pref.TenantID, uid).FirstOrCreate(&pref).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to save playback preferences"})
		return
	}
	if err := db.Model(&pref).Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to save playback preferences"})
		return
	}
	if err := db.Where("tenant_id = ? AND user_id = ?", "default", uid).First(&pref).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to load saved playback preferences"})
		return
	}
	c.JSON(http.StatusOK, pref)
}
