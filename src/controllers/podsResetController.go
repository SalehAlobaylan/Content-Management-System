package controllers

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/feedstate"
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/podsreset"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const podsResetApprovalTemplate = "RESET PODS %d %s IRREVERSIBLE"

type podsResetCandidateStage struct {
	ProcessingGeneration int64     `json:"processing_generation"`
	Stage                string    `json:"stage"`
	State                string    `json:"state"`
	PolicyVersion        string    `json:"policy_version"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type podsResetCandidate struct {
	ContentItemID               uuid.UUID                 `json:"content_item_id"`
	Type                        models.ContentType        `json:"type"`
	Source                      models.SourceType         `json:"source"`
	Status                      models.ContentStatus      `json:"status"`
	ContentSourceID             *uuid.UUID                `json:"content_source_id,omitempty"`
	CreatedAt                   time.Time                 `json:"created_at"`
	UpdatedAt                   time.Time                 `json:"updated_at"`
	ProcessingGeneration        int64                     `json:"processing_generation"`
	RetirementIdentityAvailable bool                      `json:"retirement_identity_available"`
	ProcessingProvenance        string                    `json:"processing_provenance"`
	ProvenanceNote              string                    `json:"provenance_note"`
	StageEvidence               []podsResetCandidateStage `json:"stage_evidence"`
}

func formatPodsResetBytes(value int64) string {
	const gib = int64(1024 * 1024 * 1024)
	return fmt.Sprintf("%d GiB", value/gib)
}

type podsResetObjectWire struct {
	StorageTier string `json:"storage_tier"`
	Bucket      string `json:"bucket"`
	ObjectKey   string `json:"object_key"`
	ETag        string `json:"etag"`
	SizeBytes   int64  `json:"size_bytes"`
}

type podsResetBlocker struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type podsResetInventoryResponse struct {
	Data struct {
		Complete        bool                       `json:"complete"`
		ConfiguredTiers []string                   `json:"configured_tiers"`
		StorageBindings []podsreset.StorageBinding `json:"storage_bindings"`
		VersionModel    string                     `json:"version_model"`
		Items           []struct {
			ContentItemID string                `json:"content_item_id"`
			Objects       []podsResetObjectWire `json:"objects"`
		} `json:"items"`
	} `json:"data"`
}

type podsResetManifestTarget struct {
	ID                  uuid.UUID                  `json:"content_item_id"`
	StorageTiers        []string                   `json:"storage_tiers"`
	StorageBindings     []podsreset.StorageBinding `json:"storage_bindings"`
	StorageVersionModel string                     `json:"storage_version_model"`
	MetadataCounts      map[string]int64           `json:"metadata_counts"`
	Objects             []podsreset.ObjectIdentity `json:"objects"`
}

func itemStorageTarget(manifest datatypes.JSON, contentItemID uuid.UUID) *podsResetManifestTarget {
	var value struct {
		Targets []podsResetManifestTarget `json:"targets"`
	}
	if json.Unmarshal(manifest, &value) != nil {
		return nil
	}
	for index := range value.Targets {
		if value.Targets[index].ID == contentItemID {
			return &value.Targets[index]
		}
	}
	return nil
}

type podsResetEnvironmentIdentity struct {
	Database string `gorm:"column:database_name" json:"database_name"`
	Schema   string `gorm:"column:schema_name" json:"schema_name"`
}

func podsResetEnvironment(db *gorm.DB) (podsResetEnvironmentIdentity, error) {
	var environment podsResetEnvironmentIdentity
	if err := db.Raw("SELECT current_database() AS database_name, current_schema() AS schema_name").Scan(&environment).Error; err != nil {
		return podsResetEnvironmentIdentity{}, fmt.Errorf("could not identify reset database environment: %w", err)
	}
	if strings.TrimSpace(environment.Database) == "" || strings.TrimSpace(environment.Schema) == "" {
		return podsResetEnvironmentIdentity{}, fmt.Errorf("reset database environment identity is incomplete")
	}
	return environment, nil
}

func podsResetFenceTriggerName(table string) string {
	name := "pods_reset_fence_" + table
	if len([]byte(name)) > 63 {
		digest := md5.Sum([]byte(table))
		name = "pods_reset_fence_" + hex.EncodeToString(digest[:])
	}
	return name
}

func podsResetTriggerDefinitionUsable(enabled, definition, expectedFunction, expectedEvents string) bool {
	if enabled != "O" && enabled != "A" {
		return false
	}
	if !strings.Contains(strings.ToUpper(definition), strings.ToUpper(expectedEvents)) {
		return false
	}
	marker := "EXECUTE FUNCTION "
	upperDefinition := strings.ToUpper(definition)
	index := strings.Index(upperDefinition, marker)
	if index < 0 {
		return false
	}
	functionCall := strings.TrimSpace(definition[index+len(marker):])
	openParen := strings.IndexByte(functionCall, '(')
	if openParen < 0 {
		return false
	}
	qualifiedName := strings.TrimSpace(functionCall[:openParen])
	parts := strings.Split(qualifiedName, ".")
	functionName := strings.Trim(strings.TrimSpace(parts[len(parts)-1]), `"`)
	return strings.EqualFold(functionName, expectedFunction)
}

func podsResetSameStrings(left, right []string) bool {
	leftCopy, rightCopy := append([]string(nil), left...), append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	if len(leftCopy) != len(rightCopy) {
		return false
	}
	for index := range leftCopy {
		if leftCopy[index] != rightCopy[index] {
			return false
		}
	}
	return true
}

func podsResetObjectsMatchStorageBindings(objects []podsResetObjectWire, bindings []podsreset.StorageBinding) bool {
	bucketByTier := make(map[string]string, len(bindings))
	for _, binding := range bindings {
		bucketByTier[binding.StorageTier] = binding.Bucket
	}
	for _, object := range objects {
		if bucketByTier[object.StorageTier] == "" || bucketByTier[object.StorageTier] != object.Bucket {
			return false
		}
	}
	return true
}

func podsResetInventoryChanged(expected []podsreset.ObjectIdentity, actual []podsResetObjectWire) bool {
	expectedByKey := make(map[string]podsreset.ObjectIdentity, len(expected))
	for _, object := range expected {
		expectedByKey[podsResetObjectKey(object.StorageTier, object.Bucket, object.ObjectKey)] = object
	}
	actualKeys := make(map[string]bool, len(actual))
	for _, object := range actual {
		key := podsResetObjectKey(object.StorageTier, object.Bucket, object.ObjectKey)
		approved, found := expectedByKey[key]
		if !found || actualKeys[key] || approved.ETag != object.ETag || approved.SizeBytes != object.SizeBytes {
			return true
		}
		actualKeys[key] = true
	}
	// Missing frozen keys are safe: they can be accounted as already absent.
	return false
}

func podsResetItemNeedsProcessing(state string) bool {
	return state == "fenced" || state == "verification_pending" || state == "objects_deleted"
}

func podsResetSchemaFingerprint(db *gorm.DB) (string, error) {
	type schemaColumn struct {
		Table    string `gorm:"column:table_name" json:"table"`
		Column   string `gorm:"column:column_name" json:"column"`
		Type     string `gorm:"column:column_type" json:"type"`
		Nullable string `gorm:"column:is_nullable" json:"nullable"`
		Default  string `gorm:"column:default_value" json:"default"`
	}
	type trigger struct {
		Table      string `gorm:"column:table_name" json:"table"`
		Name       string `gorm:"column:trigger_name" json:"name"`
		Definition string `json:"definition"`
		Enabled    string `json:"enabled"`
	}
	type constraint struct {
		Table      string `gorm:"column:table_name" json:"table"`
		Name       string `gorm:"column:constraint_name" json:"name"`
		Definition string `json:"definition"`
	}
	type referenceColumn struct {
		Table  string `gorm:"column:table_name" json:"table"`
		Column string `gorm:"column:column_name" json:"column"`
	}
	database, err := podsResetEnvironment(db)
	if err != nil {
		return "", err
	}
	var columns []schemaColumn
	if err := db.Raw(`
		SELECT table_name, column_name, udt_name AS column_type,
		       is_nullable, COALESCE(column_default, '') AS default_value
		FROM information_schema.columns
		WHERE table_schema = current_schema()
	ORDER BY table_name, column_name`).Scan(&columns).Error; err != nil {
		return "", fmt.Errorf("could not fingerprint current-schema columns: %w", err)
	}
	var referenceColumns []referenceColumn
	if err := db.Raw(`
		SELECT ic.table_name, ic.column_name
		FROM information_schema.columns ic
		JOIN information_schema.tables it
		  ON it.table_catalog=ic.table_catalog
		 AND it.table_schema=ic.table_schema
		 AND it.table_name=ic.table_name
		JOIN pg_class direct_table ON direct_table.relname=ic.table_name
		JOIN pg_namespace direct_schema ON direct_schema.oid=direct_table.relnamespace AND direct_schema.nspname=ic.table_schema
		WHERE ic.table_schema=current_schema() AND it.table_type='BASE TABLE'
		  AND direct_table.relkind IN ('r','p')
		  AND NOT EXISTS (SELECT 1 FROM pg_inherits partition_link WHERE partition_link.inhrelid=direct_table.oid)
		  AND (ic.column_name IN ('content_item_id','parent_content_item_id')
		       OR (ic.table_name='moderation_reports' AND ic.column_name='target_id')
		       OR (ic.table_name='retention_holds' AND ic.column_name='target_id')
		       OR (ic.table_name='media_circulation_overrides' AND ic.column_name='subject_id')
		       OR (ic.table_name='media_circulation_recommendations' AND ic.column_name='subject_id')
		       OR (ic.table_name='media_supply_action_previews' AND ic.column_name='target_id')
		       OR (ic.table_name='media_supply_action_requests' AND ic.column_name='target_id')
		       OR (ic.table_name='feed_generation_memberships' AND ic.column_name='member_id')
		       OR (ic.table_name='feed_recovery_plan_targets' AND ic.column_name='target_id')
		       OR (ic.table_name='experience_events' AND ic.column_name='content_id')
		       OR (ic.table_name='enrichment_autopilot_actions' AND ic.column_name='content_id')
		       OR (ic.table_name='news_month_archive_story_sources' AND ic.column_name='original_content_id')
		       OR (ic.table_name='news_ingest_tombstones' AND ic.column_name='original_content_id')
		       OR (ic.table_name='retention_compaction_batches' AND ic.column_name='target_ids')
		       OR (ic.table_name='retention_compaction_manifests' AND ic.column_name IN ('anchor_content_ids','protected_content_ids','retire_content_ids')))
		UNION
		SELECT local_table.relname AS table_name, local_column.attname AS column_name
		FROM pg_constraint fk
		JOIN pg_class local_table ON local_table.oid=fk.conrelid
		JOIN pg_namespace local_schema ON local_schema.oid=local_table.relnamespace
		JOIN generate_subscripts(fk.conkey, 1) AS key_position(position) ON TRUE
		JOIN pg_attribute local_column ON local_column.attrelid=fk.conrelid AND local_column.attnum=fk.conkey[key_position.position]
		JOIN pg_attribute referenced_column ON referenced_column.attrelid=fk.confrelid AND referenced_column.attnum=fk.confkey[key_position.position]
		WHERE fk.contype='f' AND fk.confrelid='content_items'::regclass
		  AND referenced_column.attname='public_id'
		  AND local_schema.nspname=current_schema()
		  AND local_table.relkind IN ('r','p')
		  AND NOT EXISTS (SELECT 1 FROM pg_inherits partition_link WHERE partition_link.inhrelid=local_table.oid)
		ORDER BY table_name, column_name`).Scan(&referenceColumns).Error; err != nil {
		return "", fmt.Errorf("could not inspect retired content reference coverage: %w", err)
	}
	var triggers []trigger
	if err := db.Raw(`
		SELECT c.relname AS table_name, t.tgname AS trigger_name, pg_get_triggerdef(t.oid) AS definition,
		       t.tgenabled::text AS enabled
		FROM pg_trigger t
		JOIN pg_class c ON c.oid=t.tgrelid
		JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname=current_schema() AND t.tgname LIKE 'pods_reset_%' AND NOT t.tgisinternal
		ORDER BY c.relname, t.tgname`).Scan(&triggers).Error; err != nil {
		return "", fmt.Errorf("could not inspect retirement trigger coverage: %w", err)
	}
	covered := map[string]trigger{}
	for _, trigger := range triggers {
		covered[trigger.Table+"\n"+trigger.Name] = trigger
	}
	usableTrigger := func(table, name, function, events string) bool {
		trigger, ok := covered[table+"\n"+name]
		return ok && podsResetTriggerDefinitionUsable(trigger.Enabled, trigger.Definition, function, events)
	}
	for _, column := range referenceColumns {
		// Reset-owned progress and immutable evidence intentionally keep the
		// retired content identity as their audit key; they are guarded by the
		// reset state machine rather than the producer write fence.
		if strings.HasPrefix(column.Table, "pods_reset_") {
			continue
		}
		if _, ok := podsreset.RelationshipPolicyFor(column.Table, column.Column); !ok {
			return "", fmt.Errorf("content reference %s.%s has no explicit reset disposition", column.Table, column.Column)
		}
		name := podsResetFenceTriggerName(column.Table)
		function := "enforce_pods_reset_retirement_fence"
		events := "BEFORE INSERT OR UPDATE"
		if column.Table == "content_items" {
			name = "pods_reset_content_identity_fence"
			events = "BEFORE INSERT OR UPDATE OR DELETE"
		} else if column.Table == "pods_reset_retirements" {
			continue // The retirement ledger is only written by the reset owner.
		} else {
			switch column.Table + "." + column.Column {
			case "moderation_reports.target_id":
				name = "pods_reset_moderation_report_fence"
				function = "enforce_pods_reset_moderation_report_fence"
				events = "BEFORE INSERT OR UPDATE"
			case "media_supply_action_requests.target_id":
				name = "pods_reset_supply_action_fence"
				function = "enforce_pods_reset_supply_action_fence"
			case "media_supply_action_previews.target_id":
				name = "pods_reset_supply_preview_fence"
				function = "enforce_pods_reset_supply_preview_fence"
			case "media_circulation_recommendations.subject_id":
				name = "pods_reset_circulation_recommendation_fence"
				function = "enforce_pods_reset_circulation_recommendation_fence"
			case "media_circulation_overrides.subject_id":
				name = "pods_reset_circulation_override_fence"
				function = "enforce_pods_reset_circulation_override_fence"
			case "retention_holds.target_id":
				name = "pods_reset_retention_hold_fence"
				function = "enforce_pods_reset_retention_hold_fence"
			case "feed_generation_memberships.member_id":
				name = "pods_reset_feed_membership_fence"
				function = "enforce_pods_reset_feed_membership_fence"
			case "feed_recovery_plan_targets.target_id":
				name = "pods_reset_feed_recovery_target_fence"
				function = "enforce_pods_reset_feed_recovery_target_fence"
			case "retention_compaction_batches.target_ids":
				name = "pods_reset_retention_batch_fence"
				function = "enforce_pods_reset_retention_batch_fence"
			case "retention_compaction_manifests.anchor_content_ids",
				"retention_compaction_manifests.protected_content_ids",
				"retention_compaction_manifests.retire_content_ids":
				name = "pods_reset_retention_manifest_fence"
				function = "enforce_pods_reset_retention_manifest_fence"
			}
		}
		if !usableTrigger(column.Table, name, function, events) {
			return "", fmt.Errorf("content reference table %s lacks an enabled retirement trigger", column.Table)
		}
	}
	if !usableTrigger("content_items", "pods_reset_content_identity_fence", "enforce_pods_reset_retirement_fence", "BEFORE INSERT OR UPDATE OR DELETE") {
		return "", fmt.Errorf("content_items lacks an enabled permanent identity retirement trigger")
	}
	for _, required := range []struct{ table, name, function, events string }{
		{"content_items", "pods_reset_transcript_link_fence", "enforce_pods_reset_transcript_link_fence", "BEFORE INSERT OR UPDATE"},
		{"moderation_reports", "pods_reset_moderation_report_fence", "enforce_pods_reset_moderation_report_fence", "BEFORE INSERT OR UPDATE"},
		{"media_circulation_overrides", "pods_reset_circulation_override_fence", "enforce_pods_reset_circulation_override_fence", "BEFORE INSERT OR UPDATE"},
		{"media_circulation_recommendations", "pods_reset_circulation_recommendation_fence", "enforce_pods_reset_circulation_recommendation_fence", "BEFORE INSERT OR UPDATE"},
		{"retention_holds", "pods_reset_retention_hold_fence", "enforce_pods_reset_retention_hold_fence", "BEFORE INSERT OR UPDATE"},
		{"feed_generation_memberships", "pods_reset_feed_membership_fence", "enforce_pods_reset_feed_membership_fence", "BEFORE INSERT OR UPDATE"},
		{"media_supply_action_previews", "pods_reset_supply_preview_fence", "enforce_pods_reset_supply_preview_fence", "BEFORE INSERT OR UPDATE"},
		{"media_supply_action_requests", "pods_reset_supply_action_fence", "enforce_pods_reset_supply_action_fence", "BEFORE INSERT OR UPDATE"},
		{"feed_recovery_plan_targets", "pods_reset_feed_recovery_target_fence", "enforce_pods_reset_feed_recovery_target_fence", "BEFORE INSERT OR UPDATE"},
	} {
		if !usableTrigger(required.table, required.name, required.function, required.events) {
			return "", fmt.Errorf("%s lacks an enabled %s race guard", required.table, required.function)
		}
	}
	var externalReferenceCount int64
	if err := db.Raw(`
		SELECT COUNT(*)
		FROM pg_constraint fk
		JOIN pg_class local_table ON local_table.oid=fk.conrelid
		JOIN pg_namespace local_schema ON local_schema.oid=local_table.relnamespace
		WHERE fk.contype='f' AND fk.confrelid='content_items'::regclass
		  AND local_schema.nspname<>current_schema()`).Scan(&externalReferenceCount).Error; err != nil {
		return "", fmt.Errorf("could not inspect cross-schema content references: %w", err)
	}
	if externalReferenceCount > 0 {
		return "", fmt.Errorf("content identity has %d incoming reference(s) outside the fenced schema", externalReferenceCount)
	}
	var constraints []constraint
	if err := db.Raw(`
		SELECT conrelid::regclass::text AS table_name, conname AS constraint_name,
		       pg_get_constraintdef(oid) AS definition
		FROM pg_constraint fk
		JOIN pg_class local_table ON local_table.oid=fk.conrelid
		JOIN pg_namespace local_schema ON local_schema.oid=local_table.relnamespace
		WHERE fk.contype = 'f' AND fk.confrelid = 'content_items'::regclass
		  AND local_schema.nspname=current_schema()
		ORDER BY conrelid::regclass::text, conname`).Scan(&constraints).Error; err != nil {
		return "", fmt.Errorf("could not fingerprint content reference constraints: %w", err)
	}
	return podsreset.Hash(struct {
		Database     podsResetEnvironmentIdentity   `json:"database"`
		Columns      []schemaColumn                 `json:"columns"`
		References   []referenceColumn              `json:"references"`
		Dispositions []podsreset.RelationshipPolicy `json:"relationship_dispositions"`
		Triggers     []trigger                      `json:"triggers"`
		Constraints  []constraint                   `json:"constraints"`
	}{database, columns, referenceColumns, podsreset.RelationshipPolicies(), triggers, constraints})
}

func podsResetIdentityHash(tenant, idempotencyKey string) string {
	identity := strings.TrimSpace(idempotencyKey)
	if identity == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(tenant + "\n" + identity))
	return hex.EncodeToString(sum[:])
}

// PodsResetWriteFenceMiddleware is installed on every service-to-service
// mutation route. Besides direct content-item IDs, it resolves durable work
// and artifact IDs back to their owner item so late callbacks cannot republish
// a permanently retired identity.
func PodsResetWriteFenceMiddleware(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == http.MethodGet || c.Request.Method == http.MethodHead || c.Request.Method == http.MethodOptions || c.FullPath() == "/internal/pods-reset/authorize-object-deletion" {
			c.Next()
			return
		}
		if !db.Migrator().HasTable(&models.PodsResetRetirement{}) {
			c.Next()
			return
		}
		route := c.FullPath()
		targetIDs := map[uuid.UUID]bool{}
		var payload map[string]json.RawMessage
		if c.Request.Body != nil && c.Request.ContentLength != 0 {
			body, err := io.ReadAll(io.LimitReader(c.Request.Body, 20<<20))
			if err != nil || len(body) >= 20<<20 {
				c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "write-fence request body exceeds inspection limit"})
				return
			}
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			_ = json.Unmarshal(body, &payload)
		}
		tenant := "default"
		if raw := payload["tenant_id"]; len(raw) > 0 {
			_ = json.Unmarshal(raw, &tenant)
		}
		if raw := payload["content_item_id"]; len(raw) > 0 {
			var value string
			_ = json.Unmarshal(raw, &value)
			addPodsResetUUID(targetIDs, value)
		}
		if raw := payload["parent_content_item_id"]; len(raw) > 0 {
			var value string
			_ = json.Unmarshal(raw, &value)
			addPodsResetUUID(targetIDs, value)
		}
		if raw := payload["content_ids"]; len(raw) > 0 {
			var values []string
			_ = json.Unmarshal(raw, &values)
			for _, value := range values {
				addPodsResetUUID(targetIDs, value)
			}
		}
		if strings.HasPrefix(route, "/internal/content-items/:id") {
			addPodsResetUUID(targetIDs, c.Param("id"))
		}
		id := strings.TrimSpace(c.Param("id"))
		if id != "" {
			switch {
			case strings.HasPrefix(route, "/internal/pipeline-repairs/:id/"):
				var request models.PipelineRepairRequest
				if db.Select("content_item_id").Where("public_id=?", id).First(&request).Error == nil {
					targetIDs[request.ContentItemID] = true
				}
			case strings.HasPrefix(route, "/internal/artifact-coverage/"):
				var request models.ArtifactCoverageRequest
				if db.Select("content_item_id").Where("public_id=?", id).First(&request).Error == nil {
					targetIDs[request.ContentItemID] = true
				}
			case strings.HasPrefix(route, "/internal/artifact-manifests/:id/"):
				var manifest models.MediaArtifactManifest
				if db.Select("content_item_id", "parent_content_item_id").Where("public_id=?", id).First(&manifest).Error == nil {
					if manifest.ContentItemID != nil {
						targetIDs[*manifest.ContentItemID] = true
					}
					if manifest.ParentContentItemID != nil {
						targetIDs[*manifest.ParentContentItemID] = true
					}
				}
			case strings.HasPrefix(route, "/internal/transcription-generations/:id"):
				var generation models.TranscriptionGeneration
				if db.Select("content_item_id").Where("public_id=?", id).First(&generation).Error == nil {
					targetIDs[generation.ContentItemID] = true
				}
			case strings.HasPrefix(route, "/internal/transcription-segments/:id"):
				var unit models.TranscriptionSegmentUnit
				if db.Select("generation_id").Where("public_id=?", id).First(&unit).Error == nil {
					var generation models.TranscriptionGeneration
					if db.Select("content_item_id").Where("public_id=?", unit.GenerationID).First(&generation).Error == nil {
						targetIDs[generation.ContentItemID] = true
					}
				}
			case strings.HasPrefix(route, "/internal/atomization-generations/:id"):
				var generation models.AtomizationGeneration
				if db.Select("parent_content_item_id").Where("public_id=?", id).First(&generation).Error == nil {
					targetIDs[generation.ParentContentItemID] = true
				}
			case strings.HasPrefix(route, "/internal/atomization-chapter-units/:id"):
				var unit models.AtomizationChapterUnit
				if db.Select("generation_id").Where("public_id=?", id).First(&unit).Error == nil {
					var generation models.AtomizationGeneration
					if db.Select("parent_content_item_id").Where("public_id=?", unit.GenerationID).First(&generation).Error == nil {
						targetIDs[generation.ParentContentItemID] = true
					}
				}
			case strings.HasPrefix(route, "/internal/media-rendition-generations/:id"):
				var generation models.MediaRenditionGeneration
				if db.Select("content_item_id").Where("public_id=?", id).First(&generation).Error == nil {
					targetIDs[generation.ContentItemID] = true
				}
			case strings.HasPrefix(route, "/internal/content-stages/:id/"):
				var stage models.ContentStageRequest
				if db.Select("content_item_id").Where("public_id=?", id).First(&stage).Error == nil {
					targetIDs[stage.ContentItemID] = true
				}
			case strings.HasPrefix(route, "/internal/transcription-jobs/:id"):
				var job models.TranscriptionJob
				if db.Select("content_item_id").Where("public_id=?", id).First(&job).Error == nil {
					targetIDs[job.ContentItemID] = true
				}
			case strings.HasPrefix(route, "/internal/atomization-work/:id/"):
				var request models.AtomizationWorkRequest
				if db.Select("parent_content_item_id").Where("public_id=?", id).First(&request).Error == nil {
					targetIDs[request.ParentContentItemID] = true
				}
			}
		}
		if actionID := strings.TrimSpace(c.Param("action")); actionID != "" {
			var action models.MediaSupplyActionRequest
			if db.Select("target_type", "target_id").Where("public_id=?", actionID).First(&action).Error == nil && action.TargetType == "content_item" {
				targetIDs[action.TargetID] = true
			}
		}
		if route == "/internal/content-items" && len(payload["idempotency_key"]) > 0 {
			var key string
			_ = json.Unmarshal(payload["idempotency_key"], &key)
			normalized := normalizeIdempotencyKey(key)
			identityHash := podsResetIdentityHash(strings.TrimSpace(tenant), normalized)
			if identityHash != "" {
				var count int64
				if err := db.Model(&models.PodsResetRetirement{}).Where("tenant_id=? AND identity_hash=?", strings.TrimSpace(tenant), identityHash).Count(&count).Error; err != nil {
					c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "retirement identity fence is unavailable"})
					return
				}
				if count > 0 {
					c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "content identity is permanently retired", "code": "PODS_RESET_RETIRED"})
					return
				}
			}
		}
		if len(targetIDs) > 0 {
			ids := make([]uuid.UUID, 0, len(targetIDs))
			for targetID := range targetIDs {
				ids = append(ids, targetID)
			}
			var count int64
			if err := db.Model(&models.PodsResetRetirement{}).Where("content_item_id IN ? AND state IN ?", ids, []string{"retiring", "retired"}).Count(&count).Error; err != nil {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "retirement write fence is unavailable"})
				return
			}
			if count > 0 {
				c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "content item is permanently retired", "code": "PODS_RESET_RETIRED"})
				return
			}
		}
		c.Next()
	}
}

func addPodsResetUUID(targets map[uuid.UUID]bool, raw string) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil && id != uuid.Nil {
		targets[id] = true
	}
}

func podsResetManifestHash(raw []byte) (string, error) {
	var normalized interface{}
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return "", err
	}
	return podsreset.Hash(normalized)
}

func podsResetApprovalPhrase(run models.PodsResetRun, itemCount int) string {
	short := strings.ToUpper(run.ManifestHash[:8])
	return fmt.Sprintf(podsResetApprovalTemplate, itemCount, short)
}

func appendPodsResetBlocker(blockers *[]podsResetBlocker, code, message string) {
	for _, existing := range *blockers {
		if existing.Code == code {
			return
		}
	}
	*blockers = append(*blockers, podsResetBlocker{Code: code, Message: message})
}

// ListPodsResetCandidates is a bounded, read-only discovery surface. It never
// labels age or current visibility as legacy eligibility; operators must
// explicitly select the returned UUIDs for a normal reset preview.
func ListPodsResetCandidates(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 100"})
			return
		}
		limit = parsed
	}

	parseTimeFilter := func(key string) (*time.Time, bool) {
		raw := strings.TrimSpace(c.Query(key))
		if raw == "" {
			return nil, true
		}
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return nil, false
		}
		parsed = parsed.UTC()
		return &parsed, true
	}
	createdAfter, createdAfterOK := parseTimeFilter("created_after")
	createdBefore, createdBeforeOK := parseTimeFilter("created_before")
	processingAfter, processingAfterOK := parseTimeFilter("processing_after")
	processingBefore, processingBeforeOK := parseTimeFilter("processing_before")
	if !createdAfterOK || !createdBeforeOK || !processingAfterOK || !processingBeforeOK ||
		(createdAfter != nil && createdBefore != nil && !createdAfter.Before(*createdBefore)) ||
		(processingAfter != nil && processingBefore != nil && !processingAfter.Before(*processingBefore)) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "candidate time filters must be RFC3339 and have increasing after/before bounds"})
		return
	}

	query := c.MustGet("db").(*gorm.DB).Model(&models.ContentItem{}).
		Where("tenant_id=? AND type IN ?", principal.TenantID, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast})
	if contentType := strings.ToUpper(strings.TrimSpace(c.Query("type"))); contentType != "" && contentType != "ALL" {
		if contentType != string(models.ContentTypeVideo) && contentType != string(models.ContentTypePodcast) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "type must be VIDEO, PODCAST, or ALL"})
			return
		}
		query = query.Where("type=?", contentType)
	}
	if status := strings.ToUpper(strings.TrimSpace(c.Query("status"))); status != "" && status != "ALL" {
		allowed := map[string]bool{"PENDING": true, "PROCESSING": true, "READY": true, "FAILED": true, "ARCHIVED": true}
		if !allowed[status] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "status must be a known content status or ALL"})
			return
		}
		query = query.Where("status=?", status)
	}
	if sourceID := strings.TrimSpace(c.Query("source_id")); sourceID != "" {
		if _, err := uuid.Parse(sourceID); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "source_id must be a UUID"})
			return
		}
		query = query.Where("content_source_id=?", sourceID)
	}
	if cursor := strings.TrimSpace(c.Query("cursor")); cursor != "" {
		if _, err := uuid.Parse(cursor); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "cursor must be a UUID"})
			return
		}
		query = query.Where("public_id > ?", cursor)
	}
	if createdAfter != nil {
		query = query.Where("created_at >= ?", *createdAfter)
	}
	if createdBefore != nil {
		query = query.Where("created_at < ?", *createdBefore)
	}
	const latestStageTime = `(SELECT MAX(sr.updated_at) FROM content_stage_requests sr WHERE sr.tenant_id=content_items.tenant_id AND sr.content_item_id=content_items.public_id AND sr.processing_generation=content_items.processing_generation)`
	if processingAfter != nil {
		query = query.Where("("+latestStageTime+") IS NOT NULL AND ("+latestStageTime+") >= ?", *processingAfter)
	}
	if processingBefore != nil {
		query = query.Where("("+latestStageTime+") IS NOT NULL AND ("+latestStageTime+") < ?", *processingBefore)
	}

	var rows []models.ContentItem
	if err := query.Select("public_id", "type", "source", "status", "content_source_id", "created_at", "updated_at", "processing_generation", "idempotency_key").Order("public_id ASC").Limit(limit + 1).Find(&rows).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not read candidate content inventory"})
		return
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	ids := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.PublicID)
	}
	stageByItem := map[uuid.UUID][]podsResetCandidateStage{}
	if len(ids) > 0 {
		var stages []models.ContentStageRequest
		if err := c.MustGet("db").(*gorm.DB).Select("content_item_id", "processing_generation", "stage", "state", "policy_version", "updated_at").
			Where("tenant_id=? AND content_item_id IN ?", principal.TenantID, ids).Order("updated_at DESC").Find(&stages).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not read processing provenance evidence"})
			return
		}
		for _, stage := range stages {
			if len(stageByItem[stage.ContentItemID]) >= 16 {
				continue
			}
			stageByItem[stage.ContentItemID] = append(stageByItem[stage.ContentItemID], podsResetCandidateStage{
				ProcessingGeneration: stage.ProcessingGeneration, Stage: stage.Stage, State: stage.State,
				PolicyVersion: stage.PolicyVersion, UpdatedAt: stage.UpdatedAt,
			})
		}
	}
	candidates := make([]podsResetCandidate, 0, len(rows))
	for _, row := range rows {
		identityAvailable := row.IdempotencyKey != nil && strings.TrimSpace(*row.IdempotencyKey) != ""
		candidates = append(candidates, podsResetCandidate{
			ContentItemID: row.PublicID, Type: row.Type, Source: row.Source, Status: row.Status,
			ContentSourceID: row.ContentSourceID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
			ProcessingGeneration: row.ProcessingGeneration, RetirementIdentityAvailable: identityAvailable,
			ProcessingProvenance: "unknown",
			ProvenanceNote:       "CMS has no recorded Pods infrastructure-rule version for this item; timestamps and stage history do not prove legacy eligibility.",
			StageEvidence:        stageByItem[row.PublicID],
		})
	}
	nextCursor := ""
	if hasMore && len(rows) > 0 {
		nextCursor = rows[len(rows)-1].PublicID.String()
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": candidates, "has_more": hasMore, "next_cursor": nextCursor, "page_size": limit, "provenance_policy": "unknown_without_rule_version"}})
}

func podsResetItemBlockers(db *gorm.DB, tenant string, item models.ContentItem, selected map[uuid.UUID]bool) ([]podsResetBlocker, bool, error) {
	blockers := []podsResetBlocker{}
	if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
		appendPodsResetBlocker(&blockers, "not_pods_media", "Only VIDEO and PODCAST items are in scope.")
	}
	if item.Status == models.ContentStatusPending || item.Status == models.ContentStatusProcessing {
		appendPodsResetBlocker(&blockers, "content_not_terminal", "The item is pending or processing; wait for its pipeline owner to reach a terminal state.")
	}
	if item.IdempotencyKey == nil || strings.TrimSpace(*item.IdempotencyKey) == "" {
		appendPodsResetBlocker(&blockers, "missing_retirement_identity", "This item has no stable idempotency identity, so delayed ingest cannot be fenced safely.")
	}
	if item.AuthorID != nil {
		appendPodsResetBlocker(&blockers, "user_authored_content", "User-authored media is outside the automated legacy Pods reset policy.")
	}
	if item.ManualAtomizationRequestedAt != nil || item.AtomizationOverride != nil {
		appendPodsResetBlocker(&blockers, "manual_atomization_decision", "A manual atomization decision must be resolved by its owner before reset.")
	}
	var retirementCount int64
	if err := db.Model(&models.PodsResetRetirement{}).Where("tenant_id=? AND content_item_id=?", tenant, item.PublicID).Count(&retirementCount).Error; err != nil {
		return blockers, false, err
	}
	if retirementCount > 0 {
		appendPodsResetBlocker(&blockers, "already_retired", "This item already has a permanent retirement fence.")
	}
	var activeStageCount int64
	if err := db.Model(&models.ContentStageRequest{}).
		Where("tenant_id=? AND content_item_id=? AND state NOT IN ?", tenant, item.PublicID,
			[]string{models.ContentStageVerified, models.ContentStageDeferred, models.ContentStageFailed, models.ContentStageCancelled, models.ContentStageSuperseded}).
		Count(&activeStageCount).Error; err != nil {
		return blockers, false, err
	}
	if activeStageCount > 0 {
		appendPodsResetBlocker(&blockers, "active_content_stage", "One or more durable processing stages are not terminal.")
	}
	var activeStorageSweepCount int64
	if err := db.Model(&models.StorageSweepRun{}).Where("tenant_id=? AND finished_at IS NULL", tenant).Count(&activeStorageSweepCount).Error; err != nil {
		return blockers, false, err
	}
	if activeStorageSweepCount > 0 {
		appendPodsResetBlocker(&blockers, "active_storage_sweep", "A tenant storage sweep is still running; wait for the storage owner to finish before reset.")
	}
	var activeJobCount int64
	if err := db.Model(&models.TranscriptionJob{}).
		Where("tenant_id=? AND content_item_id=? AND status IN ?", tenant, item.PublicID,
			[]string{models.TranscriptionJobStatusQueued, models.TranscriptionJobStatusRunning, models.TranscriptionJobStatusWritebackFailed}).
		Count(&activeJobCount).Error; err != nil {
		return blockers, false, err
	}
	if activeJobCount > 0 {
		appendPodsResetBlocker(&blockers, "active_transcription_job", "A transcription job or writeback is still active or uncertain.")
	}
	var activeBatchItemCount int64
	if err := db.Model(&models.TranscriptionBatchItem{}).
		Where("tenant_id=? AND content_item_id=? AND status IN ?", tenant, item.PublicID, []string{models.TranscriptionBatchItemStatusPending, models.TranscriptionBatchItemStatusAccepted}).
		Count(&activeBatchItemCount).Error; err != nil {
		return blockers, false, err
	}
	if activeBatchItemCount > 0 {
		appendPodsResetBlocker(&blockers, "active_transcription_batch_item", "A pending or accepted transcription batch item may still create or write back a transcript.")
	}
	var activeManifestCount int64
	if err := db.Model(&models.MediaArtifactManifest{}).
		Where("tenant_id=? AND (content_item_id=? OR (content_item_id IS NULL AND parent_content_item_id=?)) AND state IN ?", tenant, item.PublicID, item.PublicID,
			[]string{"uploading", "uploaded", "uncertain", "cleanup_eligible"}).Count(&activeManifestCount).Error; err != nil {
		return blockers, false, err
	}
	if activeManifestCount > 0 {
		appendPodsResetBlocker(&blockers, "active_artifact_upload", "An artifact upload, verification, or version cleanup is in progress or has an uncertain provider result.")
	}
	var activeTranscriptionCount int64
	if err := db.Model(&models.TranscriptionGeneration{}).
		Where("tenant_id=? AND content_item_id=? AND state NOT IN ?", tenant, item.PublicID,
			[]string{"verified", "deferred", "failed", "superseded"}).Count(&activeTranscriptionCount).Error; err != nil {
		return blockers, false, err
	}
	if activeTranscriptionCount > 0 {
		appendPodsResetBlocker(&blockers, "active_transcription_generation", "A transcription generation is not terminal.")
	}
	var activeAtomizationCount int64
	if err := db.Model(&models.AtomizationGeneration{}).
		Where("tenant_id=? AND parent_content_item_id=? AND state NOT IN ?", tenant, item.PublicID,
			[]string{"superseded", "failed"}).Count(&activeAtomizationCount).Error; err != nil {
		return blockers, false, err
	}
	if activeAtomizationCount > 0 {
		appendPodsResetBlocker(&blockers, "active_atomization_generation", "An atomization generation is not terminal.")
	}
	var activeAtomizationRunCount int64
	if err := db.Model(&models.MediaAtomizationRun{}).
		Where("tenant_id=? AND parent_content_item_id=? AND status IN ?", tenant, item.PublicID, []string{"queued", "processing", "running"}).
		Count(&activeAtomizationRunCount).Error; err != nil {
		return blockers, false, err
	}
	if activeAtomizationRunCount > 0 {
		appendPodsResetBlocker(&blockers, "active_atomization_run", "A legacy atomization run is queued or running and may still publish child media.")
	}
	var activeAtomizationWorkCount int64
	if err := db.Model(&models.AtomizationWorkRequest{}).
		Where("tenant_id=? AND parent_content_item_id=? AND state IN ?", tenant, item.PublicID,
			[]string{"queued", "claimed", "running", "verifying", "uncertain"}).
		Count(&activeAtomizationWorkCount).Error; err != nil {
		return blockers, false, err
	}
	if activeAtomizationWorkCount > 0 {
		appendPodsResetBlocker(&blockers, "active_atomization_work", "An atomization request or lease is queued, running, or uncertain.")
	}
	var activePipelineRepairCount int64
	if err := db.Model(&models.PipelineRepairRequest{}).
		Where("tenant_id=? AND content_item_id=? AND state IN ?", tenant, item.PublicID,
			[]string{models.PipelineRepairAwaitingApproval, models.PipelineRepairQueued, models.PipelineRepairClaimed, models.PipelineRepairRunning, models.PipelineRepairVerifying, models.PipelineRepairUncertain}).
		Count(&activePipelineRepairCount).Error; err != nil {
		return blockers, false, err
	}
	if activePipelineRepairCount > 0 {
		appendPodsResetBlocker(&blockers, "active_pipeline_repair", "A pipeline repair is awaiting approval, queued, leased, running, or uncertain.")
	}
	var activePipelineLeaseCount int64
	if err := db.Model(&models.PipelineStageLease{}).
		Where("tenant_id=? AND content_item_id=? AND state IN ?", tenant, item.PublicID, []string{"claimed", "running", "verifying", "unknown"}).
		Count(&activePipelineLeaseCount).Error; err != nil {
		return blockers, false, err
	}
	if activePipelineLeaseCount > 0 {
		appendPodsResetBlocker(&blockers, "active_pipeline_lease", "A pipeline stage lease is active or has an unknown effect outcome.")
	}
	var activeCoverageCount int64
	if err := db.Model(&models.ArtifactCoverageRequest{}).
		Where("tenant_id=? AND content_item_id=? AND state IN ?", tenant, item.PublicID, []string{"queued", "claimed", "running", "verifying", "uncertain"}).
		Count(&activeCoverageCount).Error; err != nil {
		return blockers, false, err
	}
	if activeCoverageCount > 0 {
		appendPodsResetBlocker(&blockers, "active_artifact_coverage", "An artifact coverage request is queued, active, or uncertain.")
	}
	var activeSupplyActionCount int64
	if err := db.Model(&models.MediaSupplyActionRequest{}).
		Where("tenant_id=? AND target_type='content_item' AND target_id=? AND state IN ?", tenant, item.PublicID,
			[]string{"awaiting_approval", "queued", "claimed", "running", "verifying", "uncertain"}).
		Count(&activeSupplyActionCount).Error; err != nil {
		return blockers, false, err
	}
	if activeSupplyActionCount > 0 {
		appendPodsResetBlocker(&blockers, "active_supply_action", "A media supply action could admit new work for this item.")
	}
	var activeSupplyPreviewCount int64
	if err := db.Model(&models.MediaSupplyActionPreview{}).
		Where("tenant_id=? AND target_type='content_item' AND target_id=? AND state='active' AND expires_at>?", tenant, item.PublicID, time.Now().UTC()).
		Count(&activeSupplyPreviewCount).Error; err != nil {
		return blockers, false, err
	}
	if activeSupplyPreviewCount > 0 {
		appendPodsResetBlocker(&blockers, "active_supply_action_preview", "An unexpired supply-action preview could still be approved into new work.")
	}
	var activeCirculationRecommendationCount int64
	if err := db.Model(&models.MediaCirculationRecommendation{}).
		Where("tenant_id=? AND unit_type='item_family' AND subject_id=? AND status NOT IN ?", tenant, item.PublicID,
			[]string{models.MediaCirculationRecStatusApplied, models.MediaCirculationRecStatusDismissed, models.MediaCirculationRecStatusSuperseded}).
		Count(&activeCirculationRecommendationCount).Error; err != nil {
		return blockers, false, err
	}
	if activeCirculationRecommendationCount > 0 {
		appendPodsResetBlocker(&blockers, "active_circulation_recommendation", "An unresolved item-family circulation recommendation may still initiate an eviction or admission action.")
	}
	var activeFeedRecoveryTargetCount int64
	if err := db.Raw(`
		SELECT COUNT(DISTINCT target.id)
		FROM feed_recovery_plan_targets target
		JOIN feed_recovery_plans plan ON plan.id=target.plan_id AND plan.tenant_id=target.tenant_id
		WHERE target.tenant_id=? AND target.target_type='media_content' AND target.target_id=?
		  AND (
		    (plan.state IN ('awaiting_approval','approved') AND plan.expires_at>?)
		    OR EXISTS (
		      SELECT 1 FROM feed_recovery_runs run
		      WHERE run.plan_id=plan.id
		        AND run.phase NOT IN ('failed','cancelled','rolled_back')
		        AND NOT (run.phase='rollback_ready' AND run.rollback_deadline IS NOT NULL AND run.rollback_deadline<=?)
		    )
		  )`, tenant, item.PublicID, time.Now().UTC(), time.Now().UTC()).Scan(&activeFeedRecoveryTargetCount).Error; err != nil {
		return blockers, false, fmt.Errorf("could not inspect feed recovery plan targets: %w", err)
	}
	if activeFeedRecoveryTargetCount > 0 {
		appendPodsResetBlocker(&blockers, "active_feed_recovery_plan", "An unexpired recovery plan or unresolved recovery run still owns this target.")
	}
	var activeRetentionBatchCount int64
	if err := db.Raw(`SELECT COUNT(*) FROM retention_compaction_batches
		WHERE tenant_id=? AND jsonb_exists(target_ids, ?) AND state <> 'verification_passed'`, tenant, item.PublicID.String()).Scan(&activeRetentionBatchCount).Error; err != nil {
		return blockers, false, fmt.Errorf("could not inspect retention compaction batches: %w", err)
	}
	if activeRetentionBatchCount > 0 {
		appendPodsResetBlocker(&blockers, "active_retention_compaction_batch", "An unverified retention batch still owns this exact target set.")
	}
	var activeRetentionManifestCount int64
	if err := db.Raw(`
		SELECT COUNT(DISTINCT manifest.id)
		FROM retention_compaction_manifests manifest
		CROSS JOIN LATERAL jsonb_array_elements_text(
			COALESCE(manifest.anchor_content_ids, '[]'::jsonb) ||
			COALESCE(manifest.protected_content_ids, '[]'::jsonb) ||
			COALESCE(manifest.retire_content_ids, '[]'::jsonb)
		) target(value)
		WHERE manifest.tenant_id=? AND target.value=?
		  AND ((manifest.state IN ('prepared','approved') AND manifest.expires_at>?)
		       OR manifest.state NOT IN ('prepared','approved','expired','executed','blocked'))`, tenant, item.PublicID.String(), time.Now().UTC()).Scan(&activeRetentionManifestCount).Error; err != nil {
		return blockers, false, fmt.Errorf("could not inspect retention compaction manifests: %w", err)
	}
	if activeRetentionManifestCount > 0 {
		appendPodsResetBlocker(&blockers, "active_retention_compaction_manifest", "An active or unknown retention manifest still references the target.")
	}
	var activeRenditionCount int64
	if err := db.Model(&models.MediaRenditionGeneration{}).
		Where("tenant_id=? AND content_item_id=? AND state IN ?", tenant, item.PublicID,
			[]string{"planning", "running", "verifying", "uncertain"}).Count(&activeRenditionCount).Error; err != nil {
		return blockers, false, err
	}
	if activeRenditionCount > 0 {
		appendPodsResetBlocker(&blockers, "active_rendition_generation", "A media rendition generation is still running or uncertain.")
	}
	var activeStorageOperationCount int64
	if err := db.Model(&models.StorageOperationSaga{}).
		Where("tenant_id=? AND content_item_id=? AND state NOT IN ?", tenant, item.PublicID, []string{"cms_committed", "failed", "cancelled"}).
		Count(&activeStorageOperationCount).Error; err != nil {
		return blockers, false, err
	}
	if activeStorageOperationCount > 0 {
		appendPodsResetBlocker(&blockers, "active_storage_operation", "A durable storage operation has not reached a known terminal owner state.")
	}
	var activeRecoveryPurgeCount int64
	if err := db.Model(&models.FeedRecoveryMediaPurgeItem{}).
		Where("tenant_id=? AND content_item_id=? AND state IN ?", tenant, item.PublicID, []string{"prepared", "object_delete_requested", "object_deleted", "cms_delete_pending", "blocked"}).
		Count(&activeRecoveryPurgeCount).Error; err != nil {
		return blockers, false, err
	}
	if activeRecoveryPurgeCount > 0 {
		appendPodsResetBlocker(&blockers, "active_feed_recovery_purge", "A separate Feed Recovery purge saga still owns this item or its provider objects.")
	}
	var activeFeedMembershipRepairCount int64
	if err := db.Model(&models.FeedGenerationMembershipRepair{}).
		Where("tenant_id=? AND content_item_id=? AND state IN ?", tenant, item.PublicID, []string{"queued", "running", "verifying", "uncertain"}).
		Count(&activeFeedMembershipRepairCount).Error; err != nil {
		return blockers, false, err
	}
	if activeFeedMembershipRepairCount > 0 {
		appendPodsResetBlocker(&blockers, "active_feed_membership_repair", "A queued or uncertain feed repair may reattach this item to a serving generation.")
	}
	var draftCount int64
	if err := db.Model(&models.MediaChapterDraft{}).
		Where("tenant_id=? AND parent_content_item_id=?", tenant, item.PublicID).
		Count(&draftCount).Error; err != nil {
		return blockers, false, err
	}
	if draftCount > 0 {
		appendPodsResetBlocker(&blockers, "chapter_draft", "A human-authored chapter draft or applied editorial plan must be exported or resolved by an editor before reset.")
	}
	transcriptIDs, err := podsResetOwnedTranscriptIDs(db, item)
	if err != nil {
		return blockers, false, err
	}
	if len(transcriptIDs) > 0 {
		var approvedTranscriptCount int64
		if err := db.Model(&models.Transcript{}).
			Where("public_id IN ? AND (approved_at IS NOT NULL OR source=?)", transcriptIDs, models.TranscriptSourceYouTubeHuman).
			Count(&approvedTranscriptCount).Error; err != nil {
			return blockers, false, err
		}
		var approvedVersionCount int64
		if err := db.Model(&models.TranscriptVersion{}).
			Where("transcript_id IN ? AND (approved_at IS NOT NULL OR source=?)", transcriptIDs, models.TranscriptSourceYouTubeHuman).
			Count(&approvedVersionCount).Error; err != nil {
			return blockers, false, err
		}
		if approvedTranscriptCount > 0 || approvedVersionCount > 0 {
			appendPodsResetBlocker(&blockers, "approved_transcript", "An approved human caption is preserved by default and must be separately reviewed before reset.")
		}
		var manualChapterCount int64
		if err := db.Model(&models.Chapter{}).Where("tenant_id=? AND transcript_id IN ? AND source=?", tenant, transcriptIDs, models.ChapterSourceManual).Count(&manualChapterCount).Error; err != nil {
			return blockers, false, err
		}
		if manualChapterCount > 0 {
			appendPodsResetBlocker(&blockers, "manual_chapter_metadata", "Manual chapter metadata must be exported or resolved by an editor before reset.")
		}
	}
	var survivingChildren int64
	query := db.Model(&models.ContentItem{}).Where("tenant_id=? AND parent_content_item_id=?", tenant, item.PublicID)
	if len(selected) > 0 {
		var selectedIDs []uuid.UUID
		for id := range selected {
			selectedIDs = append(selectedIDs, id)
		}
		query = query.Where("public_id NOT IN ?", selectedIDs)
	}
	if err := query.Count(&survivingChildren).Error; err != nil {
		return blockers, false, err
	}
	if survivingChildren > 0 {
		appendPodsResetBlocker(&blockers, "surviving_child", "The item is a parent of a child outside this exact selection; family expansion is not automatic.")
	}
	selectedIDs := make([]uuid.UUID, 0, len(selected))
	for id := range selected {
		selectedIDs = append(selectedIDs, id)
	}
	if len(selectedIDs) == 0 {
		selectedIDs = []uuid.UUID{item.PublicID}
	}
	var survivingChapterReferences int64
	if err := db.Table("chapters ch").
		Joins("JOIN transcripts tr ON tr.public_id=ch.transcript_id").
		Where("ch.tenant_id=? AND ch.child_content_item_id=? AND tr.content_item_id NOT IN ?", tenant, item.PublicID, selectedIDs).
		Count(&survivingChapterReferences).Error; err != nil {
		return blockers, false, fmt.Errorf("could not inspect surviving chapter references: %w", err)
	}
	if survivingChapterReferences > 0 {
		appendPodsResetBlocker(&blockers, "surviving_chapter_reference", "An unselected transcript chapter still references this child; resolve the owning parent before reset.")
	}
	if item.ParentContentItemID != nil {
		var parent models.ContentItem
		if err := db.Where("tenant_id=? AND public_id=?", tenant, *item.ParentContentItemID).First(&parent).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return blockers, false, err
		}
		if parent.PublicID != uuid.Nil && !selected[parent.PublicID] {
			// A child can be retired without deleting its parent; its provenance link remains in the retained identity.
		}
	}
	candidateIDs := []uuid.UUID{item.PublicID}
	protected, err := retentionProtectedContentIDs(db, tenant, candidateIDs)
	if err != nil {
		return blockers, false, err
	}
	if protected[item.PublicID] {
		appendPodsResetBlocker(&blockers, "protected_content", "An interaction, hold, open moderation report, or content flag protects this item.")
	}
	var circulationProtectionCount int64
	if err := db.Model(&models.MediaCirculationOverride{}).
		Where("tenant_id=? AND subject_kind IN ? AND subject_id=? AND override_type IN ? AND (expires_at IS NULL OR expires_at>?)",
			tenant, []string{"item", "family"}, item.PublicID,
			[]string{models.MediaCirculationOverrideEditorialHold, models.MediaCirculationOverrideNeverArchive, models.MediaCirculationOverrideKeepLatestNHot}, time.Now().UTC()).
		Count(&circulationProtectionCount).Error; err != nil {
		return blockers, false, fmt.Errorf("could not inspect media circulation protections: %w", err)
	}
	if circulationProtectionCount > 0 {
		appendPodsResetBlocker(&blockers, "media_circulation_protection", "An active editorial hold, never-archive, or keep-latest protection must be explicitly resolved by its owner before reset.")
	}
	redundancyCount := int64(0)
	for _, dependency := range []struct{ table, predicate string }{
		{table: "redundancy_pairs", predicate: "item_a_id=? OR item_b_id=?"},
		{table: "redundancy_families", predicate: "canonical_content_item_id=?"},
		{table: "redundancy_family_members", predicate: "content_item_id=?"},
		{table: "redundancy_fingerprints", predicate: "content_item_id=?"},
	} {
		table, predicate := dependency.table, dependency.predicate
		args := []interface{}{item.PublicID}
		if table == "redundancy_pairs" {
			args = append(args, item.PublicID)
		}
		var count int64
		if err := db.Table(table).Where(predicate, args...).Count(&count).Error; err != nil {
			// A missing table or failed dependency query is not no-reference proof.
			return blockers, false, fmt.Errorf("could not inspect %s dependency links: %w", table, err)
		}
		redundancyCount += count
	}
	if redundancyCount > 0 {
		appendPodsResetBlocker(&blockers, "redundancy_link", "The item participates in redundancy evidence; resolve the owner action before reset.")
	}
	transcriptShared := false
	if len(transcriptIDs) > 0 {
		var sharedCount int64
		if err := db.Model(&models.ContentItem{}).Where("transcript_id IN ? AND public_id<>?", transcriptIDs, item.PublicID).Count(&sharedCount).Error; err != nil {
			return blockers, false, err
		}
		transcriptShared = sharedCount > 0
	}
	return blockers, transcriptShared, nil
}

func podsResetOwnedTranscriptIDs(db *gorm.DB, item models.ContentItem) ([]uuid.UUID, error) {
	ids := []uuid.UUID{}
	if item.TranscriptID != nil {
		ids = append(ids, *item.TranscriptID)
	}
	var owned []uuid.UUID
	if err := db.Model(&models.Transcript{}).Where("content_item_id=?", item.PublicID).Pluck("public_id", &owned).Error; err != nil {
		return nil, err
	}
	seen := map[uuid.UUID]bool{}
	out := make([]uuid.UUID, 0, len(ids)+len(owned))
	for _, id := range append(ids, owned...) {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out, nil
}

func podsResetMetadataCounts(db *gorm.DB, tenant string, item models.ContentItem) (map[string]int64, error) {
	counts := map[string]int64{"content_items": 1}
	count := func(table, query string, args ...interface{}) error {
		var value int64
		if err := db.Table(table).Where(query, args...).Count(&value).Error; err != nil {
			return fmt.Errorf("could not inventory %s: %w", table, err)
		}
		counts[table] = value
		return nil
	}
	countQuery := func(name, query string, args ...interface{}) error {
		var value int64
		if err := db.Raw(query, args...).Scan(&value).Error; err != nil {
			return fmt.Errorf("could not inventory %s: %w", name, err)
		}
		counts[name] = value
		return nil
	}
	transcriptIDs, err := podsResetOwnedTranscriptIDs(db, item)
	if err != nil {
		return nil, err
	}
	if err := count("content_item_topics", "content_item_id=?", item.PublicID); err != nil {
		return nil, err
	}
	if err := count("user_interactions", "content_item_id=?", item.PublicID); err != nil {
		return nil, err
	}
	if err := count("content_flags", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("experience_events", "tenant_id=? AND content_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("enrichment_autopilot_actions", "tenant_id=? AND content_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("moderation_reports", "tenant_id=? AND target_type='content' AND target_id=? AND status='open'", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("retention_holds", "tenant_id=? AND target_type='content' AND target_id=? AND released_at IS NULL AND (expires_at IS NULL OR expires_at>NOW())", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_circulation_overrides", "tenant_id=? AND subject_kind IN ? AND subject_id=? AND override_type IN ? AND (expires_at IS NULL OR expires_at>NOW())", tenant, []string{"item", "family"}, item.PublicID, []string{models.MediaCirculationOverrideEditorialHold, models.MediaCirculationOverrideNeverArchive, models.MediaCirculationOverrideKeepLatestNHot}); err != nil {
		return nil, err
	}
	if err := count("media_circulation_recommendations", "tenant_id=? AND unit_type='item_family' AND subject_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_supply_action_previews", "tenant_id=? AND target_type='content_item' AND target_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_supply_action_requests", "tenant_id=? AND target_type='content_item' AND target_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("news_month_archive_story_sources", "original_content_id=?", item.PublicID); err != nil {
		return nil, err
	}
	if err := count("news_ingest_tombstones", "tenant_id=? AND original_content_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("news_month_archive_stories", "lead_content_id=?", item.PublicID); err != nil {
		return nil, err
	}
	if err := count("feed_recovery_plan_targets", "tenant_id=? AND target_type='media_content' AND target_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := countQuery("retention_compaction_batches", "SELECT COUNT(*) FROM retention_compaction_batches WHERE tenant_id=? AND jsonb_exists(target_ids, ?)", tenant, item.PublicID.String()); err != nil {
		return nil, err
	}
	if err := countQuery("retention_compaction_manifests", `
		SELECT COUNT(DISTINCT manifest.id)
		FROM retention_compaction_manifests manifest
		CROSS JOIN LATERAL jsonb_array_elements_text(
			COALESCE(manifest.anchor_content_ids, '[]'::jsonb) ||
			COALESCE(manifest.protected_content_ids, '[]'::jsonb) ||
			COALESCE(manifest.retire_content_ids, '[]'::jsonb)
		) target(value)
		WHERE manifest.tenant_id=? AND target.value=?`, tenant, item.PublicID.String()); err != nil {
		return nil, err
	}
	if err := countQuery("story_retention_holds", "SELECT COUNT(*) FROM retention_holds h JOIN content_items c ON c.story_id=h.target_id WHERE c.tenant_id=? AND c.public_id=? AND h.tenant_id=? AND h.target_type='story' AND h.released_at IS NULL AND (h.expires_at IS NULL OR h.expires_at>NOW())", tenant, item.PublicID, tenant); err != nil {
		return nil, err
	}
	if len(transcriptIDs) == 0 {
		counts["transcripts"] = 0
		counts["chapters"] = 0
		counts["transcript_versions"] = 0
		counts["approved_transcript_versions"] = 0
	} else {
		if err := count("transcripts", "content_item_id=? OR public_id IN ?", item.PublicID, transcriptIDs); err != nil {
			return nil, err
		}
		if err := count("chapters", "tenant_id=? AND transcript_id IN ?", tenant, transcriptIDs); err != nil {
			return nil, err
		}
		if err := count("transcript_versions", "transcript_id IN ?", transcriptIDs); err != nil {
			return nil, err
		}
		if err := countQuery("approved_transcript_versions", "SELECT COUNT(*) FROM transcript_versions WHERE transcript_id IN ? AND (approved_at IS NOT NULL OR source=?)", transcriptIDs, models.TranscriptSourceYouTubeHuman); err != nil {
			return nil, err
		}
	}
	if err := countQuery("chapters_child_references", "SELECT COUNT(*) FROM chapters ch WHERE ch.child_content_item_id=?", item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_artifact_manifests", "tenant_id=? AND (content_item_id=? OR (content_item_id IS NULL AND parent_content_item_id=?))", tenant, item.PublicID, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_rendition_generations", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_hls_packages", "tenant_id=? AND rendition_generation_id IN (SELECT public_id FROM media_rendition_generations WHERE tenant_id=? AND content_item_id=?)", tenant, tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_hls_access_points", "tenant_id=? AND package_id IN (SELECT public_id FROM media_hls_packages WHERE tenant_id=? AND rendition_generation_id IN (SELECT public_id FROM media_rendition_generations WHERE tenant_id=? AND content_item_id=?))", tenant, tenant, tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("transcription_generations", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("transcription_batch_items", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("transcription_segment_units", "tenant_id=? AND generation_id IN (SELECT public_id FROM transcription_generations WHERE tenant_id=? AND content_item_id=?)", tenant, tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("atomization_generations", "tenant_id=? AND parent_content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_atomization_runs", "tenant_id=? AND parent_content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("atomization_chapter_units", "tenant_id=? AND generation_id IN (SELECT public_id FROM atomization_generations WHERE tenant_id=? AND parent_content_item_id=?)", tenant, tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("content_stage_requests", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("content_stage_events", "tenant_id=? AND request_id IN (SELECT public_id FROM content_stage_requests WHERE tenant_id=? AND content_item_id=?)", tenant, tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("pipeline_repair_requests", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("artifact_coverage_requests", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("atomization_work_requests", "tenant_id=? AND parent_content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("feed_recovery_media_purge_items", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("feed_generation_membership_repairs", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("feed_generation_memberships", "member_type='feed_unit' AND member_id=?", item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_intelligence_scores", "content_item_id=?", item.PublicID); err != nil {
		return nil, err
	}
	if err := count("transcript_quality", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("media_storage_artifact_events", "tenant_id=? AND (content_item_id=? OR parent_content_item_id=?)", tenant, item.PublicID, item.PublicID); err != nil {
		return nil, err
	}
	if err := count("storage_operation_sagas", "tenant_id=? AND content_item_id=?", tenant, item.PublicID); err != nil {
		return nil, err
	}
	for _, dependency := range []struct{ table, predicate string }{
		{table: "redundancy_pairs", predicate: "item_a_id=? OR item_b_id=?"},
		{table: "redundancy_families", predicate: "canonical_content_item_id=?"},
		{table: "redundancy_family_members", predicate: "content_item_id=?"},
		{table: "redundancy_fingerprints", predicate: "content_item_id=?"},
	} {
		args := []interface{}{item.PublicID}
		if dependency.table == "redundancy_pairs" {
			args = append(args, item.PublicID)
		}
		if err := count(dependency.table, dependency.predicate, args...); err != nil {
			return nil, err
		}
	}
	return counts, nil
}

type podsResetPlanTarget struct {
	ID                  uuid.UUID                     `json:"content_item_id"`
	Snapshot            podsreset.Snapshot            `json:"snapshot"`
	Decisions           []podsreset.DataClassDecision `json:"data_decisions"`
	Blockers            []podsResetBlocker            `json:"blockers"`
	StorageTiers        []string                      `json:"storage_tiers"`
	StorageBindings     []podsreset.StorageBinding    `json:"storage_bindings"`
	StorageVersionModel string                        `json:"storage_version_model"`
	MetadataCounts      map[string]int64              `json:"metadata_counts"`
	Objects             []podsreset.ObjectIdentity    `json:"objects"`
}

// buildPodsResetPlanTargets materializes the exact Plan 119 preflight for an
// explicit item set. It performs read-only inventory/provider checks and
// returns the immutable target snapshot used by both the standalone preview
// and a campaign-delegated retirement batch.
func buildPodsResetPlanTargets(db *gorm.DB, tenant string, ids []uuid.UUID) ([]podsResetPlanTarget, int, int64, error) {
	selected := make(map[uuid.UUID]bool, len(ids))
	for _, id := range ids {
		selected[id] = true
	}
	targets := make([]podsResetPlanTarget, 0, len(ids))
	var totalBytes int64
	var totalObjects int
	for _, id := range ids {
		var item models.ContentItem
		t := podsResetPlanTarget{ID: id, Decisions: podsreset.Decisions(false), Blockers: []podsResetBlocker{}, MetadataCounts: map[string]int64{}, Objects: []podsreset.ObjectIdentity{}}
		if err := db.Where("tenant_id=? AND public_id=?", tenant, id).First(&item).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				appendPodsResetBlocker(&t.Blockers, "item_not_found", "The explicit ID does not exist in the authenticated tenant.")
			} else {
				return nil, 0, 0, errors.New("could not load selected content")
			}
		} else {
			t.Snapshot = podsreset.SnapshotFor(item)
			t.MetadataCounts, err = podsResetMetadataCounts(db, tenant, item)
			if err != nil {
				return nil, 0, 0, errors.New("could not inventory related metadata")
			}
			var transcriptShared bool
			t.Blockers, transcriptShared, err = podsResetItemBlockers(db, tenant, item, selected)
			if err != nil {
				return nil, 0, 0, errors.New("could not prove reset dependencies")
			}
			var alreadyRetired int64
			identityHash := podsResetIdentityHash(item.TenantID, t.Snapshot.IdempotencyKey)
			if identityHash != "" {
				if err := db.Model(&models.PodsResetRetirement{}).Where("tenant_id=? AND identity_hash=?", item.TenantID, identityHash).Count(&alreadyRetired).Error; err != nil {
					return nil, 0, 0, errors.New("could not inspect retirement identity")
				}
				if alreadyRetired > 0 {
					appendPodsResetBlocker(&t.Blockers, "retired_identity_collision", "The source identity is permanently fenced by a prior reset.")
				}
			}
			t.Decisions = podsreset.Decisions(transcriptShared)
		}
		if item.PublicID != uuid.Nil && (item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast) {
			payload := gin.H{"tenant_id": tenant, "content_ids": []string{id.String()}}
			body, status, callErr := callAggregationInternal(http.MethodPost, "/internal/pods-reset/inventory", payload)
			if callErr != nil || status < 200 || status >= 300 {
				appendPodsResetBlocker(&t.Blockers, "storage_inventory_incomplete", "Aggregation could not prove a complete inventory for this item.")
			} else {
				var response podsResetInventoryResponse
				if err := json.Unmarshal(body, &response); err != nil || !response.Data.Complete || len(response.Data.Items) != 1 || response.Data.Items[0].ContentItemID != id.String() {
					appendPodsResetBlocker(&t.Blockers, "storage_inventory_invalid", "Aggregation returned an incomplete or mismatched item inventory.")
				} else {
					t.StorageTiers = append([]string(nil), response.Data.ConfiguredTiers...)
					t.StorageBindings = append([]podsreset.StorageBinding(nil), response.Data.StorageBindings...)
					t.StorageVersionModel = response.Data.VersionModel
					if t.StorageVersionModel != "cloudflare-r2-current-key-delete-v1" {
						appendPodsResetBlocker(&t.Blockers, "storage_version_model_unqualified", "Storage object-version semantics are not qualified for exact deletion.")
					}
					if len(t.StorageTiers) == 0 || !containsString(t.StorageTiers, "primary") || (len(t.StorageTiers) > 2) || (len(t.StorageTiers) == 2 && !containsString(t.StorageTiers, "cold")) {
						appendPodsResetBlocker(&t.Blockers, "storage_tier_inventory_invalid", "Aggregation did not provide a valid configured-tier inventory.")
					}
					if err := podsreset.ValidateStorageBindings(t.StorageTiers, t.StorageBindings); err != nil {
						appendPodsResetBlocker(&t.Blockers, "storage_binding_inventory_invalid", "Aggregation did not bind every configured tier to an exact bucket and provider endpoint identity.")
					}
					for _, object := range response.Data.Items[0].Objects {
						t.Objects = append(t.Objects, podsreset.ObjectIdentity{StorageTier: object.StorageTier, Bucket: object.Bucket, ObjectKey: object.ObjectKey, ETag: object.ETag, SizeBytes: object.SizeBytes})
					}
					if !podsResetObjectsMatchStorageBindings(response.Data.Items[0].Objects, t.StorageBindings) {
						appendPodsResetBlocker(&t.Blockers, "storage_binding_object_mismatch", "An inventoried object is not in the frozen bucket for its configured storage tier.")
					}
					if err := podsreset.ValidateObjectSet(id.String(), t.Objects); err != nil {
						appendPodsResetBlocker(&t.Blockers, "storage_inventory_invalid", "An object identity was duplicate, incomplete, or outside the exact content prefix.")
						t.Objects = []podsreset.ObjectIdentity{}
					} else if ownershipErr := podsResetArtifactOwnershipBlockers(db, tenant, id, t.Objects); ownershipErr != nil {
						appendPodsResetBlocker(&t.Blockers, "artifact_ownership_unproven", "Artifact registry references a missing, shared, or unlisted object identity.")
					}
				}
			}
		}
		totalObjects += len(t.Objects)
		for _, object := range t.Objects {
			totalBytes += object.SizeBytes
		}
		targets = append(targets, t)
	}
	return targets, totalObjects, totalBytes, nil
}

func CreatePodsResetPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var request struct {
		ContentIDs      []string `json:"content_ids"`
		SelectionReason string   `json:"selection_reason"`
	}
	if err := c.ShouldBindJSON(&request); err != nil || len(request.ContentIDs) == 0 || len(request.ContentIDs) > podsreset.MaxItems {
		c.JSON(http.StatusBadRequest, gin.H{"error": "content_ids must contain 1 to 30 explicit UUIDs"})
		return
	}
	request.SelectionReason = strings.TrimSpace(request.SelectionReason)
	if request.SelectionReason == "" || len(request.SelectionReason) > 500 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "selection_reason must contain 1 to 500 characters"})
		return
	}
	ids := make([]uuid.UUID, 0, len(request.ContentIDs))
	selected := make(map[uuid.UUID]bool, len(request.ContentIDs))
	for _, raw := range request.ContentIDs {
		id, err := uuid.Parse(strings.TrimSpace(raw))
		if err != nil || id == uuid.Nil || selected[id] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "content_ids must be unique valid UUIDs"})
			return
		}
		selected[id] = true
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	db := c.MustGet("db").(*gorm.DB)
	schemaFingerprint, err := podsResetSchemaFingerprint(db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reset schema fingerprint is unavailable"})
		return
	}
	environment, err := podsResetEnvironment(db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reset environment identity is unavailable"})
		return
	}
	targets, totalObjects, totalBytes, err := buildPodsResetPlanTargets(db, principal.TenantID, ids)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	if totalObjects > podsreset.MaxRunObjects || totalBytes > podsreset.MaxRunBytes {
		for index := range targets {
			appendPodsResetBlocker(&targets[index].Blockers, "run_budget_exceeded", fmt.Sprintf("The frozen selection exceeds the bounded run budget of %d objects or %s; split it into smaller explicit previews.", podsreset.MaxRunObjects, formatPodsResetBytes(podsreset.MaxRunBytes)))
		}
	}
	manifest := gin.H{
		"policy_version": podsreset.PolicyVersion, "schema_fingerprint": schemaFingerprint, "tenant_id": principal.TenantID, "environment_identity": environment,
		"relationship_dispositions": podsreset.RelationshipPolicies(),
		"requested_ids":             ids, "selection_reason": request.SelectionReason, "targets": targets,
		"counts":  gin.H{"selected_ids": len(ids), "object_count": totalObjects, "object_bytes": totalBytes},
		"outcome": "purge_only", "rollback": "unavailable_after_retirement_fence",
	}
	rawManifest, err := json.Marshal(manifest)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not encode reset manifest"})
		return
	}
	manifestHash, err := podsResetManifestHash(rawManifest)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not hash reset manifest"})
		return
	}
	run := models.PodsResetRun{
		TenantID: principal.TenantID, State: "preview", Phase: "preflight",
		ManifestHash: manifestHash, SchemaFingerprint: schemaFingerprint, PolicyVersion: podsreset.PolicyVersion,
		Manifest: datatypes.JSON(rawManifest), CreatedBy: principal.Email, ExpiresAt: time.Now().UTC().Add(podsreset.PlanTTL),
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		for ordinal, t := range targets {
			snapshotHash := strings.Repeat("0", 64)
			if t.Snapshot.PublicID != "" {
				snapshotHash, err = podsreset.Hash(t.Snapshot)
				if err != nil {
					return err
				}
			}
			decision, _ := json.Marshal(t.Decisions)
			blockers, _ := json.Marshal(t.Blockers)
			state := "planned"
			if len(t.Blockers) > 0 {
				state = "blocked"
			}
			resetItem := models.PodsResetItem{RunID: run.PublicID, TenantID: principal.TenantID, ContentItemID: t.ID, Ordinal: ordinal, SnapshotHash: snapshotHash, Decision: datatypes.JSON(decision), State: state, BlockedReasons: datatypes.JSON(blockers)}
			if err := tx.Create(&resetItem).Error; err != nil {
				return err
			}
			for _, object := range t.Objects {
				row := models.PodsResetObject{RunID: run.PublicID, ContentItemID: t.ID, StorageTier: object.StorageTier, Bucket: object.Bucket, ObjectKey: object.ObjectKey, ETag: object.ETag, SizeBytes: object.SizeBytes, State: "planned"}
				if err := tx.Create(&row).Error; err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not persist reset preview"})
		return
	}
	retentionAudit(db, principal, "pods_reset.preview", run.PublicID.String(), "success", map[string]interface{}{"manifest_hash": manifestHash, "selected_count": len(ids), "object_count": totalObjects, "object_bytes": totalBytes})
	c.JSON(http.StatusCreated, gin.H{"data": gin.H{"run": run, "items": targets, "approval_phrase": podsResetApprovalPhrase(run, len(ids)), "counts": gin.H{"selected_ids": len(ids), "object_count": totalObjects, "object_bytes": totalBytes}, "can_approve": allPodsResetTargetsUnblocked(targets)}})
}

func podsResetArtifactOwnershipBlockers(db *gorm.DB, tenant string, id uuid.UUID, objects []podsreset.ObjectIdentity) error {
	var manifests []models.MediaArtifactManifest
	if err := db.Where("tenant_id=? AND (content_item_id=? OR (content_item_id IS NULL AND parent_content_item_id=?))", tenant, id, id).Find(&manifests).Error; err != nil {
		return err
	}
	known := make(map[string]bool, len(objects))
	for _, object := range objects {
		known[podsResetObjectKey(object.StorageTier, object.Bucket, object.ObjectKey)] = true
	}
	ownership := make([]podsreset.ArtifactOwnership, 0, len(manifests))
	for _, manifest := range manifests {
		identity := podsResetObjectKey(manifest.StorageTier, manifest.Bucket, manifest.ObjectKey)
		state := manifest.State
		if manifest.DeletedAt != nil {
			state = "deleted"
		}
		if state != "deleted" && !known[identity] {
			return fmt.Errorf("artifact registry key is outside complete provider inventory")
		}
		producerEventID := ""
		if manifest.ProducerEventID != uuid.Nil {
			producerEventID = manifest.ProducerEventID.String()
		}
		contentItemID, parentContentItemID := "", ""
		if manifest.ContentItemID != nil {
			contentItemID = manifest.ContentItemID.String()
		}
		if manifest.ParentContentItemID != nil {
			parentContentItemID = manifest.ParentContentItemID.String()
		}
		ownership = append(ownership, podsreset.ArtifactOwnership{
			ContentItemID: contentItemID, ParentContentItemID: parentContentItemID,
			StorageTier: manifest.StorageTier, Bucket: manifest.Bucket, ObjectKey: manifest.ObjectKey,
			ETag: manifest.ETag, SizeBytes: manifest.SizeBytes, ArtifactRole: manifest.ArtifactRole,
			ProducerEventID: producerEventID, State: state,
		})
		if manifest.State == "deleted" {
			continue
		}
		var shared int64
		query := db.Model(&models.MediaArtifactManifest{}).
			Where("tenant_id=? AND storage_tier=? AND bucket=? AND object_key=? AND public_id<>? AND state<>'deleted' AND deleted_at IS NULL", tenant, manifest.StorageTier, manifest.Bucket, manifest.ObjectKey, manifest.PublicID)
		if err := query.Count(&shared).Error; err != nil {
			return err
		}
		if shared > 0 {
			return fmt.Errorf("artifact object is shared by another item")
		}
	}
	return podsreset.ValidateArtifactManifestCoverage(id.String(), objects, ownership)
}

func allPodsResetTargetsUnblocked(targets interface{}) bool {
	encoded, _ := json.Marshal(targets)
	var values []struct {
		Blockers []podsResetBlocker `json:"blockers"`
	}
	if json.Unmarshal(encoded, &values) != nil || len(values) == 0 {
		return false
	}
	for _, value := range values {
		if len(value.Blockers) > 0 {
			return false
		}
	}
	return true
}

func loadPodsResetRun(db *gorm.DB, tenant string, rawID string) (models.PodsResetRun, error) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		return models.PodsResetRun{}, gorm.ErrRecordNotFound
	}
	var run models.PodsResetRun
	err = db.Where("tenant_id=? AND public_id=?", tenant, id).First(&run).Error
	return run, err
}

func podsResetCanCancel(db *gorm.DB, run models.PodsResetRun) (bool, error) {
	if run.State != "preview" && run.State != "approved" && run.State != "executing" && run.State != "partial" {
		return false, nil
	}
	var irreversible int64
	if err := db.Model(&models.PodsResetRetirement{}).Where("run_public_id=?", run.PublicID).Count(&irreversible).Error; err != nil {
		return false, err
	}
	var startedItems int64
	if err := db.Model(&models.PodsResetItem{}).Where("run_id=? AND state IN ?", run.PublicID, []string{"fenced", "objects_deleted", "complete"}).Count(&startedItems).Error; err != nil {
		return false, err
	}
	var startedObjects int64
	if err := db.Model(&models.PodsResetObject{}).Where("run_id=? AND state <> 'planned'", run.PublicID).Count(&startedObjects).Error; err != nil {
		return false, err
	}
	var actions int64
	if err := db.Model(&models.PodsResetAction{}).Where("run_id=?", run.PublicID).Count(&actions).Error; err != nil {
		return false, err
	}
	return irreversible == 0 && startedItems == 0 && startedObjects == 0 && actions == 0, nil
}

func GetPodsResetPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := loadPodsResetRun(db, principal.TenantID, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pods reset run not found"})
		return
	}
	var items []models.PodsResetItem
	var objects []models.PodsResetObject
	var actions []models.PodsResetAction
	if err := db.Where("run_id=?", run.PublicID).Order("ordinal").Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load reset items"})
		return
	}
	if err := db.Where("run_id=?", run.PublicID).Order("content_item_id, storage_tier, object_key").Find(&objects).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load reset objects"})
		return
	}
	if err := db.Where("run_id=?", run.PublicID).Order("content_item_id, action").Find(&actions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load reset action ledger"})
		return
	}
	canCancel, err := podsResetCanCancel(db, run)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not verify pre-effect cancellation safety"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "items": items, "objects": objects, "actions": actions, "can_cancel": canCancel, "rollback": "unavailable_after_retirement_fence"}})
}

func ListPodsResetRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var runs []models.PodsResetRun
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(50).Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list Pods reset runs"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": runs}})
}

func ApprovePodsResetPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var request struct {
		Phrase string `json:"phrase"`
	}
	if c.ShouldBindJSON(&request) != nil || strings.TrimSpace(request.Phrase) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "exact irreversible approval phrase is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := loadPodsResetRun(db, principal.TenantID, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pods reset plan not found"})
		return
	}
	if run.ContentResetCampaignID != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "campaign-delegated Pods reset runs are controlled through the Content Reset coordinator", "code": "CAMPAIGN_DELEGATED"})
		return
	}
	var items []models.PodsResetItem
	if err := db.Where("run_id=?", run.PublicID).Order("ordinal").Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load reset items"})
		return
	}
	if run.State != "preview" || time.Now().UTC().After(run.ExpiresAt) {
		c.JSON(http.StatusConflict, gin.H{"error": "preview is stale; refresh it before approval"})
		return
	}
	if run.PolicyVersion != podsreset.PolicyVersion {
		c.JSON(http.StatusConflict, gin.H{"error": "reset policy changed after preview; create a fresh preview", "code": "POLICY_DRIFT"})
		return
	}
	if len(items) == 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "empty reset plan cannot be approved"})
		return
	}
	for _, item := range items {
		if item.State != "planned" {
			c.JSON(http.StatusConflict, gin.H{"error": "blocked targets must be removed by creating a new exact preview"})
			return
		}
	}
	wanted := podsResetApprovalPhrase(run, len(items))
	if strings.TrimSpace(request.Phrase) != wanted {
		c.JSON(http.StatusConflict, gin.H{"error": "approval phrase does not match the exact manifest"})
		return
	}
	phraseHash := sha256.Sum256([]byte(wanted))
	result := db.Model(&models.PodsResetRun{}).Where("tenant_id=? AND public_id=? AND state='preview' AND expires_at>?", principal.TenantID, run.PublicID, time.Now().UTC()).Updates(map[string]interface{}{"state": "approved", "phase": "approved", "approved_by": principal.Email, "approved_at": time.Now().UTC(), "approval_phrase_hash": hex.EncodeToString(phraseHash[:]), "updated_at": time.Now().UTC()})
	if result.Error != nil || result.RowsAffected != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "plan changed during approval"})
		return
	}
	retentionAudit(db, principal, "pods_reset.approve", run.PublicID.String(), "success", map[string]interface{}{"manifest_hash": run.ManifestHash})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": run.PublicID, "state": "approved", "manifest_hash": run.ManifestHash}})
}

func CancelPodsResetPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := loadPodsResetRun(db, principal.TenantID, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pods reset plan not found"})
		return
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", principal.TenantID, run.PublicID).First(&run).Error; err != nil {
			return err
		}
		if run.ContentResetCampaignID != nil {
			return fmt.Errorf("campaign-delegated Pods reset runs are controlled through the Content Reset coordinator")
		}
		if run.State != "preview" && run.State != "approved" && run.State != "executing" && run.State != "partial" {
			return fmt.Errorf("reset is not in a cancellable state")
		}
		canCancel, err := podsResetCanCancel(tx, run)
		if err != nil {
			return err
		}
		if !canCancel {
			return fmt.Errorf("retirement or a storage effect has begun; only pause and resume are safe")
		}
		result := tx.Model(&models.PodsResetRun{}).Where("tenant_id=? AND public_id=? AND state IN ?", principal.TenantID, run.PublicID, []string{"preview", "approved", "executing", "partial"}).Updates(map[string]interface{}{"state": "cancelled", "phase": "cancelled_before_effect", "execution_token": nil, "execution_lease_until": nil, "execution_epoch": gorm.Expr("execution_epoch + 1"), "pause_requested": false, "updated_at": time.Now().UTC()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("reset state changed before cancellation")
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	retentionAudit(db, principal, "pods_reset.cancel", run.PublicID.String(), "success", map[string]interface{}{"manifest_hash": run.ManifestHash})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": run.PublicID, "state": "cancelled"}})
}

func RequestPodsResetPause(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := loadPodsResetRun(db, principal.TenantID, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pods reset run not found"})
		return
	}
	if run.ContentResetCampaignID != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "campaign-delegated Pods reset runs are controlled through the Content Reset coordinator", "code": "CAMPAIGN_DELEGATED"})
		return
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", principal.TenantID, run.PublicID).First(&run).Error; err != nil {
			return err
		}
		if run.State != "approved" && run.State != "executing" && run.State != "partial" {
			return fmt.Errorf("only an approved or resumable reset can be paused")
		}
		result := tx.Model(&models.PodsResetRun{}).Where("tenant_id=? AND public_id=? AND state IN ?", principal.TenantID, run.PublicID, []string{"approved", "executing", "partial"}).
			Updates(map[string]interface{}{"pause_requested": true, "updated_at": time.Now().UTC()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("reset state changed before pause request")
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	retentionAudit(db, principal, "pods_reset.pause_requested", run.PublicID.String(), "success", map[string]interface{}{"manifest_hash": run.ManifestHash})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": run.PublicID, "pause_requested": true}})
}

func ResumePodsResetRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := loadPodsResetRun(db, principal.TenantID, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pods reset run not found"})
		return
	}
	if run.ContentResetCampaignID != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "campaign-delegated Pods reset runs are controlled through the Content Reset coordinator", "code": "CAMPAIGN_DELEGATED"})
		return
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", principal.TenantID, run.PublicID).First(&run).Error; err != nil {
			return err
		}
		if run.State != "approved" && run.State != "partial" {
			return fmt.Errorf("wait for the current execution batch to stop before resuming")
		}
		if run.ExecutionToken != nil && run.ExecutionLeaseUntil != nil && run.ExecutionLeaseUntil.After(time.Now().UTC()) {
			return fmt.Errorf("an executor still holds the reset lease")
		}
		result := tx.Model(&models.PodsResetRun{}).Where("tenant_id=? AND public_id=? AND state IN ?", principal.TenantID, run.PublicID, []string{"approved", "partial"}).
			Updates(map[string]interface{}{"pause_requested": false, "phase": "resume_ready", "updated_at": time.Now().UTC()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("reset state changed before resume")
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	retentionAudit(db, principal, "pods_reset.resume", run.PublicID.String(), "success", map[string]interface{}{"manifest_hash": run.ManifestHash})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": run.PublicID, "pause_requested": false, "state": run.State}})
}

type podsResetDeleteResponse struct {
	Data struct {
		ContentItemID        string                     `json:"content_item_id"`
		StorageBindings      []podsreset.StorageBinding `json:"storage_bindings"`
		DeletedCount         int                        `json:"deletedCount"`
		FreedBytes           int64                      `json:"freedBytes"`
		ObjectsAbsent        bool                       `json:"objectsAbsent"`
		DeletedObjects       []podsResetObjectWire      `json:"deletedObjects"`
		AlreadyAbsentObjects []podsResetObjectWire      `json:"alreadyAbsentObjects"`
		FencingToken         string                     `json:"fencing_token"`
	} `json:"data"`
}

type podsResetVerificationProbe struct {
	Number              int                        `json:"number"`
	ObservedAt          time.Time                  `json:"observed_at"`
	ObjectsAbsent       bool                       `json:"objects_absent"`
	ObservedObjectCount int                        `json:"observed_object_count"`
	StorageVersionModel string                     `json:"storage_version_model"`
	StorageTiers        []string                   `json:"storage_tiers"`
	StorageBindings     []podsreset.StorageBinding `json:"storage_bindings"`
}

type podsResetVerificationEvidence struct {
	Probes []podsResetVerificationProbe `json:"probes"`
}

func podsResetAppendVerificationProbe(existing datatypes.JSON, probe podsResetVerificationProbe) (datatypes.JSON, error) {
	evidence := podsResetVerificationEvidence{Probes: []podsResetVerificationProbe{}}
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, &evidence); err != nil {
			return nil, err
		}
	}
	evidence.Probes = append(evidence.Probes, probe)
	encoded, err := json.Marshal(evidence)
	return datatypes.JSON(encoded), err
}

func ExecutePodsResetRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := loadPodsResetRun(db, principal.TenantID, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pods reset run not found"})
		return
	}
	if run.ContentResetCampaignID != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "campaign-delegated Pods reset runs execute through the Content Reset coordinator", "code": "CAMPAIGN_DELEGATED"})
		return
	}
	outcome, err := executePodsResetRunInProcess(db, principal, run)
	if err != nil {
		var execErr *podsResetExecutionError
		if errors.As(err, &execErr) {
			body := gin.H{"error": execErr.Message}
			if execErr.Code != "" {
				body["code"] = execErr.Code
			}
			c.JSON(execErr.Status, body)
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	data := gin.H{"id": outcome.Run.PublicID, "state": outcome.State, "phase": outcome.Phase, "rollback": outcome.Rollback, "items": outcome.Items, "totals": outcome.Totals}
	if outcome.Warning != "" {
		data["warning"] = outcome.Warning
	}
	status := http.StatusOK
	if outcome.State == "partial" {
		status = http.StatusAccepted
		data["resume_allowed"] = true
	}
	if outcome.State == "complete" {
		data["outcome"] = "purge_only"
	}
	c.JSON(status, gin.H{"data": data})
}

func podsResetAcquireExecutionClaim(db *gorm.DB, tenant string, runID uuid.UUID) (models.PodsResetRun, error) {
	var run models.PodsResetRun
	claim := uuid.New()
	now := time.Now().UTC()
	leaseUntil := now.Add(10 * time.Minute)
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", tenant, runID).First(&run).Error; err != nil {
			return err
		}
		if run.PauseRequested {
			return fmt.Errorf("reset is paused; explicitly resume it before execution")
		}
		// The delegating campaign is the authority for a delegated run. Its
		// execution row is locked in the same transaction as the executor
		// claim so a pause or terminal transition committed before admission
		// cannot be overtaken by the pass.
		if run.ContentResetCampaignID != nil {
			var campaign models.ContentResetCampaign
			if err := tx.Where("tenant_id=? AND id=?", tenant, *run.ContentResetCampaignID).First(&campaign).Error; err != nil {
				return fmt.Errorf("delegating Content Reset campaign is unavailable")
			}
			switch campaign.State {
			case "executing", "published", "cleanup_pending", "partial":
			default:
				return fmt.Errorf("delegating Content Reset campaign is not active")
			}
			var execution models.ContentResetExecution
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=?", tenant, campaign.ID).First(&execution).Error; err != nil {
				return fmt.Errorf("delegating Content Reset campaign has no durable execution")
			}
			if execution.PauseRequested {
				return fmt.Errorf("delegating Content Reset campaign is paused")
			}
			if execution.CompletedAt != nil || execution.RolledBackAt != nil {
				return fmt.Errorf("delegating Content Reset campaign is terminal")
			}
		}
		if run.State != "approved" && run.State != "executing" && run.State != "partial" {
			return fmt.Errorf("run is not approved or resumable")
		}
		if run.State == "approved" && run.ContentResetCampaignID == nil && now.After(run.ExpiresAt) {
			return fmt.Errorf("approval expired; create a fresh preview")
		}
		var targets []models.PodsResetItem
		if err := tx.Where("tenant_id = ? AND run_id = ?", tenant, run.PublicID).
			Order("content_item_id ASC").Find(&targets).Error; err != nil {
			return fmt.Errorf("load exact Pods reset targets: %w", err)
		}
		if len(targets) == 0 {
			return fmt.Errorf("Pods reset target ledger is empty")
		}
		resources := make([]lifecycle.Resource, 0, len(targets))
		for _, target := range targets {
			resources = append(resources, lifecycle.Resource{
				Type: lifecycle.ResourceItem,
				Key:  "pods/-/" + target.ContentItemID.String(),
			})
		}
		if err := lifecycle.CheckResources(tx, tenant, resources, lifecycle.PhaseContentWrite); err != nil {
			return err
		}
		if run.ExecutionToken != nil && run.ExecutionLeaseUntil != nil && run.ExecutionLeaseUntil.After(now) {
			return fmt.Errorf("another executor holds a live reset lease")
		}
		if run.FencingToken == nil {
			token := uuid.New()
			run.FencingToken = &token
		}
		run.State, run.Phase, run.ExecutionToken, run.ExecutionLeaseUntil = "executing", "retirement_fence", &claim, &leaseUntil
		run.ExecutionEpoch++
		return tx.Model(&models.PodsResetRun{}).Where("tenant_id=? AND public_id=?", tenant, runID).Updates(map[string]interface{}{
			"state": run.State, "phase": run.Phase, "fencing_token": run.FencingToken,
			"execution_token": claim, "execution_lease_until": leaseUntil, "execution_epoch": run.ExecutionEpoch, "updated_at": now,
		}).Error
	})
	return run, err
}

func podsResetRenewExecutionClaim(db *gorm.DB, run *models.PodsResetRun) error {
	if run.ExecutionToken == nil {
		return fmt.Errorf("missing executor lease token")
	}
	leaseUntil := time.Now().UTC().Add(10 * time.Minute)
	result := db.Model(&models.PodsResetRun{}).Where("public_id=? AND state='executing' AND execution_token=?", run.PublicID, run.ExecutionToken).
		Updates(map[string]interface{}{"execution_lease_until": leaseUntil, "updated_at": time.Now().UTC()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("executor lease token changed")
	}
	run.ExecutionLeaseUntil = &leaseUntil
	return nil
}

func podsResetReleaseExecutionClaim(db *gorm.DB, run models.PodsResetRun) error {
	if run.ExecutionToken == nil {
		return nil
	}
	return db.Model(&models.PodsResetRun{}).Where("public_id=? AND execution_token=?", run.PublicID, run.ExecutionToken).
		Updates(map[string]interface{}{"execution_token": nil, "execution_lease_until": nil, "updated_at": time.Now().UTC()}).Error
}

type podsResetTotals struct {
	HardDeletedRows      int64 `json:"hard_deleted_rows"`
	RetiredIdentities    int64 `json:"retired_identities"`
	ObjectsDeleted       int64 `json:"objects_deleted"`
	ObjectsAlreadyAbsent int64 `json:"objects_already_absent"`
	BytesFreed           int64 `json:"bytes_freed"`
	BlockedItems         int64 `json:"blocked_items"`
	PendingItems         int64 `json:"pending_items"`
}

func podsResetResultTotals(db *gorm.DB, runID uuid.UUID) (podsResetTotals, error) {
	totals := podsResetTotals{}
	var err error
	if err = db.Model(&models.PodsResetRetirement{}).Where("run_public_id=? AND state='retired'", runID).Count(&totals.RetiredIdentities).Error; err != nil {
		return totals, err
	}
	if err = db.Model(&models.PodsResetObject{}).Where("run_id=? AND state='deleted' AND deleted_by_run=TRUE", runID).Count(&totals.ObjectsDeleted).Error; err != nil {
		return totals, err
	}
	if err = db.Model(&models.PodsResetObject{}).Where("run_id=? AND state='deleted' AND deleted_by_run=FALSE", runID).Count(&totals.ObjectsAlreadyAbsent).Error; err != nil {
		return totals, err
	}
	if err = db.Model(&models.PodsResetObject{}).Where("run_id=?", runID).Select("COALESCE(SUM(freed_bytes),0)").Scan(&totals.BytesFreed).Error; err != nil {
		return totals, err
	}
	if err = db.Model(&models.PodsResetItem{}).Where("run_id=? AND state='blocked'", runID).Count(&totals.BlockedItems).Error; err != nil {
		return totals, err
	}
	if err = db.Model(&models.PodsResetItem{}).Where("run_id=? AND state NOT IN ?", runID, []string{"complete", "blocked"}).Count(&totals.PendingItems).Error; err != nil {
		return totals, err
	}
	return totals, nil
}

func uuidSet(ids []uuid.UUID) map[uuid.UUID]bool {
	out := map[uuid.UUID]bool{}
	for _, id := range ids {
		out[id] = true
	}
	return out
}
func podsResetObjectKey(tier, bucket, key string) string { return tier + "\n" + bucket + "\n" + key }
func podsResetObjectSet(values []podsResetObjectWire) map[string]bool {
	out := map[string]bool{}
	for _, value := range values {
		out[podsResetObjectKey(value.StorageTier, value.Bucket, value.ObjectKey)] = true
	}
	return out
}

func podsResetValidateObjectOutcomes(expected []models.PodsResetObject, deleted, absent []podsResetObjectWire, reportedDeletedCount int) error {
	approved := make(map[string]models.PodsResetObject, len(expected))
	for _, object := range expected {
		approved[podsResetObjectKey(object.StorageTier, object.Bucket, object.ObjectKey)] = object
	}
	seen := map[string]string{}
	for category, outcomes := range map[string][]podsResetObjectWire{"deleted": deleted, "already_absent": absent} {
		for _, object := range outcomes {
			key := podsResetObjectKey(object.StorageTier, object.Bucket, object.ObjectKey)
			frozen, ok := approved[key]
			if !ok || seen[key] != "" || frozen.ETag != object.ETag || frozen.SizeBytes != object.SizeBytes {
				return fmt.Errorf("provider outcome is outside or differs from the approved object set")
			}
			seen[key] = category
		}
	}
	if len(seen) != len(approved) || reportedDeletedCount != len(deleted) {
		return fmt.Errorf("provider outcome does not exactly account for every approved object")
	}
	return nil
}

func podsResetMarkPartial(db *gorm.DB, run *models.PodsResetRun, item *models.PodsResetItem, reason string) {
	_ = db.Model(&models.PodsResetItem{}).Where("id=? AND run_id=? AND state <> 'complete'", item.ID, run.PublicID).Updates(map[string]interface{}{"last_error": reason, "updated_at": time.Now().UTC()}).Error
	result := db.Model(&models.PodsResetRun{}).Where("public_id=? AND state IN ?", run.PublicID, []string{"executing", "partial"}).Updates(map[string]interface{}{"state": "partial", "phase": "partial", "error": reason, "updated_at": time.Now().UTC()})
	if result.Error == nil && result.RowsAffected == 0 {
		var persisted models.PodsResetRun
		if db.Select("state").Where("public_id=?", run.PublicID).First(&persisted).Error == nil {
			run.State = persisted.State
			return
		}
	}
	run.State = "partial"
}

func podsResetFenceItem(db *gorm.DB, run models.PodsResetRun, resetItem *models.PodsResetItem, content *models.ContentItem, selected map[uuid.UUID]bool) error {
	if run.FencingToken == nil || run.ExecutionToken == nil {
		return fmt.Errorf("missing reset executor or permanent fencing token")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var activeRun models.PodsResetRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state='executing' AND execution_token=? AND fencing_token=? AND execution_lease_until>?", run.TenantID, run.PublicID, run.ExecutionToken, run.FencingToken, time.Now().UTC()).First(&activeRun).Error; err != nil {
			return fmt.Errorf("reset executor fence is no longer active: %w", err)
		}
		// A delegated run's admission fence includes the live parent campaign
		// pause/authority, checked under the campaign execution row lock so a
		// pause acknowledged before this effect cannot be overtaken.
		if err := delegatedPodsCampaignAdmissionLocked(tx, run); err != nil {
			return err
		}
		if err := podsResetSetWriteFence(tx, run); err != nil {
			return err
		}
		var locked models.ContentItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", content.TenantID, content.PublicID).First(&locked).Error; err != nil {
			return err
		}
		lockedSnapshotHash, err := podsreset.Hash(podsreset.SnapshotFor(locked))
		if err != nil {
			return fmt.Errorf("could not recompute the locked content snapshot: %w", err)
		}
		if lockedSnapshotHash != resetItem.SnapshotHash {
			return fmt.Errorf("selected content changed after its pre-inventory snapshot check")
		}
		identityHash := podsResetIdentityHash(locked.TenantID, podsreset.SnapshotFor(locked).IdempotencyKey)
		if identityHash == "" {
			return fmt.Errorf("missing retirement identity")
		}
		blockers, _, checkErr := podsResetItemBlockers(tx, locked.TenantID, locked, selected)
		if checkErr != nil {
			return checkErr
		}
		if len(blockers) > 0 {
			return fmt.Errorf("dependencies changed before retirement fence: %s", blockers[0].Code)
		}
		approvedTarget := itemStorageTarget(run.Manifest, locked.PublicID)
		if approvedTarget == nil {
			return fmt.Errorf("approved target storage ownership is missing")
		}
		if err := podsResetArtifactOwnershipBlockers(tx, locked.TenantID, locked.PublicID, approvedTarget.Objects); err != nil {
			return fmt.Errorf("artifact ownership changed before retirement fence: %w", err)
		}
		var current models.PodsResetItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND state='planned'", resetItem.ID).First(&current).Error; err != nil {
			return err
		}
		var retirement models.PodsResetRetirement
		err = tx.Where("tenant_id=? AND content_item_id=?", locked.TenantID, locked.PublicID).First(&retirement).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			retirement = models.PodsResetRetirement{TenantID: locked.TenantID, ContentItemID: locked.PublicID, RunPublicID: run.PublicID, ManifestHash: run.ManifestHash, FencingToken: *run.FencingToken, IdentityHash: identityHash, State: "retiring"}
			if err := tx.Create(&retirement).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else if retirement.RunPublicID != run.PublicID || retirement.FencingToken != *run.FencingToken || retirement.ManifestHash != run.ManifestHash || retirement.IdentityHash != identityHash {
			return fmt.Errorf("content identity has a different retirement fence")
		}
		locked.Status = models.ContentStatusArchived
		locked.FeedVisibility = "hidden"
		locked.IsFeedUnit = false
		locked.PlaybackURL = nil
		locked.PlaybackType = nil
		locked.FallbackPlaybackURL = nil
		locked.MediaURL = nil
		locked.ThumbnailURL = nil
		locked.MediaRenditions = nil
		locked.ActiveMediaRenditionGenerationID = nil
		if err := tx.Save(&locked).Error; err != nil {
			return err
		}
		if err := feedstate.SyncMediaMembership(tx, locked); err != nil {
			return err
		}
		if tx.Migrator().HasTable(&models.FeedGenerationMembership{}) {
			if err := tx.Where("member_type='feed_unit' AND member_id=?", locked.PublicID).Delete(&models.FeedGenerationMembership{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&models.PodsResetItem{}).Where("id=? AND state='planned'", current.ID).Updates(map[string]interface{}{"state": "fenced", "updated_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		return nil
	})
}

func podsResetFinalizeItem(db *gorm.DB, run models.PodsResetRun, resetItem *models.PodsResetItem) error {
	now := time.Now().UTC()
	if resetItem.VerificationProbeCount != 2 {
		return fmt.Errorf("two distinct origin-absence probes are required before database finalization")
	}
	var approvedManifest struct {
		Targets []struct {
			ID             uuid.UUID                     `json:"content_item_id"`
			Decisions      []podsreset.DataClassDecision `json:"data_decisions"`
			MetadataCounts map[string]int64              `json:"metadata_counts"`
		} `json:"targets"`
	}
	if err := json.Unmarshal(run.Manifest, &approvedManifest); err != nil {
		return fmt.Errorf("approved metadata decision manifest is invalid: %w", err)
	}
	var approvedTarget *struct {
		ID             uuid.UUID                     `json:"content_item_id"`
		Decisions      []podsreset.DataClassDecision `json:"data_decisions"`
		MetadataCounts map[string]int64              `json:"metadata_counts"`
	}
	for index := range approvedManifest.Targets {
		if approvedManifest.Targets[index].ID == resetItem.ContentItemID {
			approvedTarget = &approvedManifest.Targets[index]
			break
		}
	}
	if approvedTarget == nil {
		return fmt.Errorf("approved target metadata decisions are missing")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if run.ExecutionToken == nil || run.FencingToken == nil {
			return fmt.Errorf("missing reset executor or permanent fencing token")
		}
		var activeRun models.PodsResetRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state='executing' AND execution_token=? AND fencing_token=? AND execution_lease_until>?", run.TenantID, run.PublicID, run.ExecutionToken, run.FencingToken, now).First(&activeRun).Error; err != nil {
			return fmt.Errorf("reset executor fence is no longer active: %w", err)
		}
		if err := podsResetSetWriteFence(tx, run); err != nil {
			return err
		}
		var verifiedItem models.PodsResetItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND run_id=? AND state='objects_deleted' AND verification_probe_count=2", resetItem.ID, run.PublicID).First(&verifiedItem).Error; err != nil {
			return fmt.Errorf("durable two-probe verification is not complete: %w", err)
		}
		var item models.ContentItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", run.TenantID, resetItem.ContentItemID).First(&item).Error; err != nil {
			return err
		}
		transcriptIDs, err := podsResetOwnedTranscriptIDs(tx, item)
		if err != nil {
			return err
		}
		sharedTranscriptIDs := []uuid.UUID{}
		if len(transcriptIDs) > 0 {
			var lockedTranscripts []models.Transcript
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id IN ?", transcriptIDs).Order("public_id ASC").Find(&lockedTranscripts).Error; err != nil {
				return fmt.Errorf("could not lock transcript ownership rows: %w", err)
			}
			if len(lockedTranscripts) != len(transcriptIDs) {
				return fmt.Errorf("one or more selected transcript identities no longer exist")
			}
			if err := tx.Model(&models.ContentItem{}).Where("transcript_id IN ? AND public_id<>?", transcriptIDs, item.PublicID).Distinct().Pluck("transcript_id", &sharedTranscriptIDs).Error; err != nil {
				return err
			}
		}
		var objects int64
		if err := tx.Model(&models.PodsResetObject{}).Where("run_id=? AND content_item_id=? AND state <> 'deleted'", run.PublicID, item.PublicID).Count(&objects).Error; err != nil {
			return err
		}
		if objects != 0 {
			return fmt.Errorf("not all frozen objects have terminal absence evidence")
		}
		metadataEffects := map[string]int64{}
		recordMutation := func(name string, result *gorm.DB) error {
			if result.Error != nil {
				return result.Error
			}
			metadataEffects[name] = result.RowsAffected
			return nil
		}
		updates := map[string]interface{}{
			"title": nil, "body_text": nil, "excerpt": nil, "content_language": nil,
			"original_url": nil, "source_feed_url": nil, "author": nil, "source_name": nil, "duration_sec": nil, "topic_tags": "{}", "story_id": nil,
			"embedding": nil, "embedding_model": nil, "embedding_space_id": nil, "embedding_producer_id": nil,
			"embedding_sparse": nil,
			"image_embedding":  nil, "image_embedding_model": nil, "image_embedding_space_id": nil, "image_embedding_producer_id": nil,
			"metadata": nil, "transcript_id": nil, "caption_state": nil, "transcript_source": nil,
			"chapter_index": nil, "chapter_start_ms": nil, "chapter_end_ms": nil, "chapter_confidence": nil,
			"chaptering_status": nil, "duration_bucket": nil, "source_episode_id": nil,
			"media_suitability_confidence": nil, "media_suitability_reasons": nil,
			"atomization_override": nil, "atomization_override_reason": nil, "atomization_override_by": nil, "atomization_override_at": nil,
			"manual_atomization_requested_at": nil, "like_count": 0, "comment_count": 0, "share_count": 0, "view_count": 0,
			"impression_count": 0, "last_served_at": nil,
			"retired_payload_at": now, "file_size_bytes": 0, "storage_deleted_at": now,
			"storage_state": models.StorageStateUnrecoverable, "storage_state_reason": "Pods reset removed origin objects; no independent restore copy is represented by this run",
			"storage_recovery_status": models.StorageRecoveryUnrecoverable,
		}
		// Keep stable identity, type/source, lineage, and processing ledgers; clear item-owned content and derived vectors.
		contentResult := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=?", run.TenantID, item.PublicID).Updates(updates)
		if err := recordMutation("content_item_payload_cleared", contentResult); err != nil {
			return err
		}
		if contentResult.RowsAffected != 1 {
			return fmt.Errorf("retired content payload update did not affect exactly one item")
		}
		if tx.Migrator().HasTable(&models.ContentItemTopic{}) {
			if err := recordMutation("topic_memberships_detached", tx.Where("content_item_id=?", item.PublicID).Delete(&models.ContentItemTopic{})); err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.Chapter{}) {
			if err := recordMutation("chapter_child_references_detached", tx.Model(&models.Chapter{}).Where("child_content_item_id=?", item.PublicID).Update("child_content_item_id", nil)); err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.MediaIntelligenceScore{}) {
			if err := recordMutation("derived_media_scores_removed", tx.Where("content_item_id=?", item.PublicID).Delete(&models.MediaIntelligenceScore{})); err != nil {
				return err
			}
		}
		if tx.Migrator().HasTable(&models.TranscriptQuality{}) {
			if err := recordMutation("transcript_quality_removed", tx.Where("tenant_id=? AND content_item_id=?", run.TenantID, item.PublicID).Delete(&models.TranscriptQuality{})); err != nil {
				return err
			}
		}
		if len(transcriptIDs) > 0 {
			ownedTranscriptIDs := transcriptIDs
			if len(sharedTranscriptIDs) > 0 {
				shared := uuidSet(sharedTranscriptIDs)
				ownedTranscriptIDs = make([]uuid.UUID, 0, len(transcriptIDs))
				for _, transcriptID := range transcriptIDs {
					if !shared[transcriptID] {
						ownedTranscriptIDs = append(ownedTranscriptIDs, transcriptID)
					}
				}
			}
			if len(ownedTranscriptIDs) > 0 {
				if err := recordMutation("owned_transcript_payloads_cleared", tx.Model(&models.Transcript{}).Where("public_id IN ?", ownedTranscriptIDs).Updates(map[string]interface{}{"full_text": "", "summary": nil, "word_timestamps": "{}", "segments": "[]", "chapters": "[]", "retired_payload_at": now})); err != nil {
					return err
				}
				if tx.Migrator().HasTable(&models.TranscriptVersion{}) {
					if err := recordMutation("owned_transcript_version_payloads_cleared", tx.Model(&models.TranscriptVersion{}).Where("transcript_id IN ?", ownedTranscriptIDs).Updates(map[string]interface{}{"full_text": "", "summary": nil, "word_timestamps": "{}", "segments": "[]", "chapters": "[]"})); err != nil {
						return err
					}
				}
				if err := recordMutation("owned_chapter_rows_removed", tx.Where("transcript_id IN ?", ownedTranscriptIDs).Delete(&models.Chapter{})); err != nil {
					return err
				}
			}
		}
		// Deactivate derived media projections; append-only event/attempt history remains unchanged.
		if err := recordMutation("artifact_manifests_deactivated", tx.Table("media_artifact_manifests").Where("tenant_id=? AND (content_item_id=? OR (content_item_id IS NULL AND parent_content_item_id=?))", run.TenantID, item.PublicID, item.PublicID).
			Updates(map[string]interface{}{"state": "deleted", "public_url": "", "deleted_at": now})); err != nil {
			return err
		}
		if err := recordMutation("rendition_generations_superseded", tx.Table("media_rendition_generations").Where("tenant_id=? AND content_item_id=?", run.TenantID, item.PublicID).
			Updates(map[string]interface{}{"state": "superseded"})); err != nil {
			return err
		}
		if err := recordMutation("hls_access_points_superseded", tx.Table("media_hls_access_points").Where("tenant_id=? AND package_id IN (SELECT public_id FROM media_hls_packages WHERE tenant_id=? AND rendition_generation_id IN (SELECT public_id FROM media_rendition_generations WHERE tenant_id=? AND content_item_id=?))", run.TenantID, run.TenantID, run.TenantID, item.PublicID).
			Updates(map[string]interface{}{"state": "superseded", "updated_at": now})); err != nil {
			return err
		}
		if err := recordMutation("hls_packages_deactivated", tx.Table("media_hls_packages").Where("tenant_id=? AND rendition_generation_id IN (SELECT public_id FROM media_rendition_generations WHERE tenant_id=? AND content_item_id=?)", run.TenantID, run.TenantID, item.PublicID).
			Update("state", "failed")); err != nil {
			return err
		}
		if err := recordMutation("transcription_generations_superseded", tx.Table("transcription_generations").Where("tenant_id=? AND content_item_id=?", run.TenantID, item.PublicID).
			Updates(map[string]interface{}{"state": "superseded"})); err != nil {
			return err
		}
		generationQuery := "tenant_id=? AND content_item_id=?"
		generationArgs := []interface{}{run.TenantID, item.PublicID}
		if len(sharedTranscriptIDs) > 0 {
			generationQuery += " AND (merged_transcript_id IS NULL OR merged_transcript_id NOT IN ?)"
			generationArgs = append(generationArgs, sharedTranscriptIDs)
		}
		if err := recordMutation("transcription_generation_links_cleared", tx.Table("transcription_generations").Where(generationQuery, generationArgs...).Update("merged_transcript_id", nil)); err != nil {
			return err
		}
		if err := recordMutation("transcription_segments_superseded", tx.Table("transcription_segment_units").Where("tenant_id=? AND generation_id IN (SELECT public_id FROM transcription_generations WHERE "+generationQuery+")", append([]interface{}{run.TenantID}, generationArgs...)...).
			Updates(map[string]interface{}{"state": "superseded", "transcript_text": "", "transcript_segments": "[]"})); err != nil {
			return err
		}
		if err := recordMutation("atomization_generations_superseded", tx.Table("atomization_generations").Where("tenant_id=? AND parent_content_item_id=?", run.TenantID, item.PublicID).
			Updates(map[string]interface{}{"state": "superseded", "plan": "[]"})); err != nil {
			return err
		}
		if err := recordMutation("atomization_chapter_units_superseded", tx.Table("atomization_chapter_units").Where("tenant_id=? AND generation_id IN (SELECT public_id FROM atomization_generations WHERE tenant_id=? AND parent_content_item_id=?)", run.TenantID, run.TenantID, item.PublicID).
			Updates(map[string]interface{}{"state": "superseded", "result": "{}"})); err != nil {
			return err
		}
		retirementResult := tx.Model(&models.PodsResetRetirement{}).Where("tenant_id=? AND content_item_id=? AND run_public_id=? AND fencing_token=?", run.TenantID, item.PublicID, run.PublicID, run.FencingToken).
			Updates(map[string]interface{}{"state": "retired", "retired_at": now})
		if err := recordMutation("retirement_identity_committed", retirementResult); err != nil {
			return err
		}
		if retirementResult.RowsAffected != 1 {
			return fmt.Errorf("permanent retirement identity did not reach the retired state")
		}
		var objectEvidence struct {
			ObjectCount          int64 `gorm:"column:object_count"`
			ObjectsDeleted       int64 `gorm:"column:objects_deleted"`
			ObjectsAlreadyAbsent int64 `gorm:"column:objects_already_absent"`
			BytesFreed           int64 `gorm:"column:bytes_freed"`
		}
		if err := tx.Model(&models.PodsResetObject{}).Where("run_id=? AND content_item_id=?", run.PublicID, item.PublicID).
			Select("COUNT(*) AS object_count, COUNT(*) FILTER (WHERE deleted_by_run=TRUE) AS objects_deleted, COUNT(*) FILTER (WHERE deleted_by_run=FALSE) AS objects_already_absent, COALESCE(SUM(freed_bytes),0) AS bytes_freed").Scan(&objectEvidence).Error; err != nil {
			return err
		}
		resultJSON, err := json.Marshal(map[string]interface{}{
			"hard_deleted_rows": 0, "retired_identities": 1,
			"object_count": objectEvidence.ObjectCount, "objects_deleted": objectEvidence.ObjectsDeleted,
			"objects_already_absent": objectEvidence.ObjectsAlreadyAbsent, "bytes_freed": objectEvidence.BytesFreed,
			"metadata_counts_before": approvedTarget.MetadataCounts, "metadata_decisions": approvedTarget.Decisions,
			"metadata_effects":             metadataEffects,
			"shared_transcripts_preserved": len(sharedTranscriptIDs),
			"rollback":                     "unavailable", "manifest_hash": run.ManifestHash,
		})
		if err != nil {
			return err
		}
		action := models.PodsResetAction{RunID: run.PublicID, TenantID: run.TenantID, ContentItemID: item.PublicID, Action: "payload_retired", ManifestHash: run.ManifestHash, Result: datatypes.JSON(resultJSON)}
		if err := tx.Create(&action).Error; err != nil {
			return err
		}
		itemResult := tx.Model(&models.PodsResetItem{}).Where("id=? AND state='objects_deleted'", resetItem.ID).Updates(map[string]interface{}{"state": "complete", "last_error": "", "completed_at": now, "updated_at": now})
		if itemResult.Error != nil {
			return itemResult.Error
		}
		if itemResult.RowsAffected != 1 {
			return fmt.Errorf("reset item is not ready to finalize")
		}
		return nil
	})
}

func podsResetSetWriteFence(tx *gorm.DB, run models.PodsResetRun) error {
	if run.FencingToken == nil {
		return fmt.Errorf("missing reset write fence")
	}
	if err := tx.Exec("SELECT set_config('wahb.pods_reset.run_id', ?, true)", run.PublicID.String()).Error; err != nil {
		return err
	}
	return tx.Exec("SELECT set_config('wahb.pods_reset.fencing_token', ?, true)", run.FencingToken.String()).Error
}

func InternalAuthorizePodsResetObjectDeletion(c *gin.Context) {
	principal, ok := utils.GetMachinePrincipal(c)
	if !ok || principal != utils.MachinePrincipalAggregation {
		c.JSON(http.StatusForbidden, gin.H{"error": "Aggregation principal required"})
		return
	}
	var request struct {
		RunID           string                     `json:"run_id"`
		TenantID        string                     `json:"tenant_id"`
		ContentItemID   string                     `json:"content_item_id"`
		ManifestHash    string                     `json:"manifest_hash"`
		FencingToken    string                     `json:"fencing_token"`
		ExecutionToken  string                     `json:"execution_token"`
		StorageBindings []podsreset.StorageBinding `json:"storage_bindings"`
		Objects         []podsResetObjectWire      `json:"objects"`
	}
	if c.ShouldBindJSON(&request) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid reset object authorization request"})
		return
	}
	runID, e1 := uuid.Parse(request.RunID)
	itemID, e2 := uuid.Parse(request.ContentItemID)
	fence, e3 := uuid.Parse(request.FencingToken)
	executionToken, e4 := uuid.Parse(request.ExecutionToken)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || request.ManifestHash == "" || len(request.Objects) > podsreset.MaxObjects {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid reset fence identity"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.PodsResetRun
	if err := db.Where("public_id=? AND tenant_id=? AND manifest_hash=? AND fencing_token=? AND execution_token=? AND execution_lease_until>? AND state='executing'", runID, request.TenantID, request.ManifestHash, fence, executionToken, time.Now().UTC()).First(&run).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "reset fence or executor lease is not active"})
		return
	}
	manifestHash, err := podsResetManifestHash(run.Manifest)
	if err != nil || manifestHash != request.ManifestHash {
		c.JSON(http.StatusConflict, gin.H{"error": "persisted reset manifest failed integrity validation"})
		return
	}
	var manifest struct {
		Targets []struct {
			ID              uuid.UUID                  `json:"content_item_id"`
			StorageTiers    []string                   `json:"storage_tiers"`
			StorageBindings []podsreset.StorageBinding `json:"storage_bindings"`
			Objects         []podsreset.ObjectIdentity `json:"objects"`
		} `json:"targets"`
	}
	if err := json.Unmarshal(run.Manifest, &manifest); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "persisted reset object manifest is invalid"})
		return
	}
	var approvedObjects []podsreset.ObjectIdentity
	var approvedStorageTiers []string
	var approvedStorageBindings []podsreset.StorageBinding
	targetFound := false
	for _, target := range manifest.Targets {
		if target.ID == itemID {
			targetFound = true
			approvedObjects = target.Objects
			approvedStorageTiers = target.StorageTiers
			approvedStorageBindings = target.StorageBindings
			break
		}
	}
	if !targetFound || podsreset.ValidateStorageBindings(approvedStorageTiers, approvedStorageBindings) != nil || !podsreset.SameStorageBindings(approvedStorageBindings, request.StorageBindings) || !podsResetObjectsMatchStorageBindings(request.Objects, request.StorageBindings) {
		c.JSON(http.StatusConflict, gin.H{"error": "storage provider and bucket bindings differ from the approved target inventory"})
		return
	}
	if len(approvedObjects) != len(request.Objects) {
		c.JSON(http.StatusConflict, gin.H{"error": "submitted objects do not match approved target inventory"})
		return
	}
	approvedSet := map[string]podsreset.ObjectIdentity{}
	for _, object := range approvedObjects {
		approvedSet[podsResetObjectKey(object.StorageTier, object.Bucket, object.ObjectKey)] = object
	}
	for _, object := range request.Objects {
		approved, found := approvedSet[podsResetObjectKey(object.StorageTier, object.Bucket, object.ObjectKey)]
		if !found || approved.ETag != object.ETag || approved.SizeBytes != object.SizeBytes {
			c.JSON(http.StatusConflict, gin.H{"error": "submitted object fingerprint differs from approved manifest"})
			return
		}
	}
	var item models.PodsResetItem
	if err := db.Where("run_id=? AND tenant_id=? AND content_item_id=? AND state='fenced'", runID, request.TenantID, itemID).First(&item).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "reset item is not fenced"})
		return
	}
	var stored []models.PodsResetObject
	if err := db.Where("run_id=? AND content_item_id=?", runID, itemID).Order("storage_tier,bucket,object_key").Find(&stored).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not validate frozen object set"})
		return
	}
	if len(stored) != len(request.Objects) {
		c.JSON(http.StatusConflict, gin.H{"error": "object set differs from approved manifest"})
		return
	}
	for index, row := range stored {
		object := request.Objects[index]
		if object.StorageTier != row.StorageTier || object.Bucket != row.Bucket || object.ObjectKey != row.ObjectKey || object.ETag != row.ETag || object.SizeBytes != row.SizeBytes || (row.State != "deleting" && row.State != "deleted") {
			c.JSON(http.StatusConflict, gin.H{"error": "object identities or persisted deletion states differ from manifest"})
			return
		}
	}
	var content models.ContentItem
	if err := db.Where("tenant_id=? AND public_id=? AND status=? AND feed_visibility=?", request.TenantID, itemID, models.ContentStatusArchived, "hidden").First(&content).Error; err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "retired CMS serving state is not established"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"authorized": true}})
}
