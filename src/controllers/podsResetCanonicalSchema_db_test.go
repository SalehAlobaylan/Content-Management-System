package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/podsreset"
	"content-management-system/src/tests/testdb"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// The full immutable migration chain must support both ordinary writes and
// bounded retirement. Never load service env files or use a managed target.
func TestPodsResetFullCanonicalSchema(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("requires the guarded local disposable database factory")
	}
	db := testdb.Open(t)
	dsn := db.Dialector.(*postgres.Dialector).Config.DSN
	cmd := exec.Command("go", "run", "./cmd/migrate", "--all", "--allow-destructive", "--bootstrap-empty")
	cmd.Dir = "../.."
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DATABASE_URL=") && !strings.HasPrefix(value, "CMS_RUNTIME_DATABASE_URL=") && !strings.HasPrefix(value, "CMS_MIGRATION_BOOTSTRAP_DISPOSABLE=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	cmd.Env = append(cmd.Env, "DATABASE_URL="+dsn, "CMS_MIGRATION_BOOTSTRAP_DISPOSABLE=I_UNDERSTAND_THIS_DATABASE_IS_DISPOSABLE")
	output, err := cmd.CombinedOutput()
	if err != nil {
		if len(output) > 6000 {
			output = output[len(output)-6000:]
		}
		t.Fatalf("canonical migration chain: %v\n%s", err, output)
	}
	if hash, err := podsResetSchemaFingerprint(db); err != nil || len(hash) != 64 {
		t.Fatalf("full canonical reference coverage: %q %v", hash, err)
	}
	var sequence int64
	if err := db.Raw(`INSERT INTO content_items (tenant_id,type,source,status,title,original_url,source_name,idempotency_key)
  VALUES ('reset-schema-test','VIDEO','YOUTUBE','PENDING','ordinary intake','https://example.test/ordinary','test','ordinary-input') RETURNING inventory_sequence`).Scan(&sequence).Error; err != nil || sequence < 1 {
		t.Fatalf("normal insert lost inventory compatibility: %d %v", sequence, err)
	}
	var itemID uuid.UUID
	if err := db.Raw(`SELECT public_id FROM content_items WHERE tenant_id='reset-schema-test' AND idempotency_key='ordinary-input'`).Row().Scan(&itemID); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`UPDATE content_items SET title='ordinary writeback' WHERE public_id=?`, itemID).Error; err != nil {
		t.Fatalf("normal update lost schema compatibility: %v", err)
	}
	verifyCanonicalNewsTypeCompatibility(t, db)
	t.Run("upgraded_legacy_quality_history", func(t *testing.T) {
		tx := db.Begin()
		if tx.Error != nil {
			t.Fatal(tx.Error)
		}
		defer tx.Rollback()
		// Older installations retain this retired table outside the fresh chain.
		if err := tx.Exec(`CREATE TABLE quality_history (
  id bigserial PRIMARY KEY, tenant_id varchar, content_item_id uuid NOT NULL REFERENCES content_items(public_id),
  to_profile_id bigint NOT NULL, savings_bytes bigint, created_at timestamptz
)`).Error; err != nil {
			t.Fatal(err)
		}
		if err := tx.Exec(`INSERT INTO quality_history (tenant_id,content_item_id,to_profile_id,savings_bytes,created_at)
VALUES ('reset-schema-test',?,1,1024,now())`, itemID).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := podsResetSchemaFingerprint(tx); err == nil || !strings.Contains(err.Error(), "quality_history lacks an enabled retirement trigger") {
			t.Fatalf("unfenced legacy reference accepted: %v", err)
		}
		// The original reset migration already installs this on upgraded schemas.
		if err := tx.Exec(`CREATE TRIGGER pods_reset_fence_quality_history
BEFORE INSERT OR UPDATE ON quality_history
FOR EACH ROW EXECUTE FUNCTION enforce_pods_reset_retirement_fence('content_item_id')`).Error; err != nil {
			t.Fatal(err)
		}
		if hash, err := podsResetSchemaFingerprint(tx); err != nil || len(hash) != 64 {
			t.Fatalf("fenced legacy quality history rejected: %q %v", hash, err)
		}
		var savings int64
		if err := tx.Raw(`SELECT savings_bytes FROM quality_history WHERE content_item_id=?`, itemID).Scan(&savings).Error; err != nil || savings != 1024 {
			t.Fatalf("legacy history changed during preflight: %d %v", savings, err)
		}
		if err := tx.Exec(`ALTER TABLE quality_history DISABLE TRIGGER pods_reset_fence_quality_history`).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := podsResetSchemaFingerprint(tx); err == nil || !strings.Contains(err.Error(), "quality_history lacks an enabled retirement trigger") {
			t.Fatalf("disabled legacy fence accepted: %v", err)
		}
	})
	t.Run("large_preview_preserves_scope_and_protections", func(t *testing.T) {
		verifyPodsResetLargeCanonicalPreview(t, db, itemID)
	})
	t.Run("batched_artifact_ownership", func(t *testing.T) {
		verifyPodsResetBatchedArtifactOwnership(t, db)
	})
	t.Run("explicit_protection_override_effects", func(t *testing.T) {
		verifyPodsResetProtectionOverrideEffects(t, db)
	})
	t.Run("continuation_across_preparation_and_verification_batches", func(t *testing.T) {
		verifyPodsResetBatchContinuation(t, db)
	})
	if blocked, err := podsResetHasCampaignReference(db, "reset-schema-test", itemID); err != nil || blocked {
		t.Fatalf("ordinary item unexpectedly blocked: %v %v", blocked, err)
	}
	if err := db.Exec(`WITH campaign AS (
  INSERT INTO content_reset_campaigns (tenant_id,operation,lane,state,idempotency_key,request_hash,created_by)
  VALUES ('reset-schema-test','clear','pods','planning','legacy-test',repeat('0',64),'test') RETURNING id
), revision AS (
  INSERT INTO content_reset_revisions (tenant_id,campaign_id,revision,request,state,created_by)
  SELECT 'reset-schema-test',id,1,'{}','planning','test' FROM campaign RETURNING id
)
INSERT INTO content_reset_targets (tenant_id,revision_id,content_item_id,item_ordinal,lane,disposition,snapshot_hash,snapshot)
SELECT 'reset-schema-test',id,?,1,'pods','selected',repeat('0',64),'{}' FROM revision`, itemID).Error; err != nil {
		t.Fatal(err)
	}
	if blocked, err := podsResetHasCampaignReference(db, "reset-schema-test", itemID); err != nil || !blocked {
		t.Fatalf("legacy target escaped native reset blocker: %v %v", blocked, err)
	}
	if blocked, err := podsResetHasCampaignReference(db, "another-tenant", itemID); err != nil || blocked {
		t.Fatalf("legacy blocker crossed tenants: %v %v", blocked, err)
	}
	migration, err := os.ReadFile("../../migrations/20261006100000_pods_reset_campaign_reference_compatibility.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(migration)).Error; err == nil || !strings.Contains(err.Error(), "campaign obligations require the previous owner runtime") {
		t.Fatalf("forward migration ignored old campaign obligations: %v", err)
	}
	if hash, err := podsResetSchemaFingerprint(db); err != nil || len(hash) != 64 {
		t.Fatalf("blocked migration changed schema fences: %q %v", hash, err)
	}
	if err := db.Exec(`CREATE TABLE future_helper_reference (content_item_id uuid REFERENCES content_items(public_id))`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := podsResetSchemaFingerprint(db); err == nil || !strings.Contains(err.Error(), "no explicit reset disposition") {
		t.Fatalf("unknown full-schema reference accepted: %v", err)
	}
}

func verifyPodsResetLargeCanonicalPreview(t *testing.T, db *gorm.DB, parentID uuid.UUID) {
	t.Helper()
	const itemCount = 131    // Cross the old total-item cap.
	const objectsPerItem = 9 // Cross the 1,000-object persistence batch boundary.
	var ids []uuid.UUID
	if err := db.Raw(`INSERT INTO content_items (tenant_id,type,source,status,title,idempotency_key,parent_content_item_id)
SELECT 'reset-schema-test','VIDEO','YOUTUBE','FAILED','preview child','preview-child-'||n,?
FROM generate_series(1,?) AS n RETURNING public_id`, parentID, itemCount-1).Scan(&ids).Error; err != nil {
		t.Fatal(err)
	}
	protectedID := ids[0]
	if err := db.Exec(`INSERT INTO content_flags (tenant_id,content_item_id,exclude_from_feed) VALUES ('reset-schema-test',?,true)`, protectedID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO transcripts (content_item_id,full_text,source) VALUES (?,'preserved human caption','youtube_human')`, protectedID).Error; err != nil {
		t.Fatal(err)
	}
	ids = append(ids, parentID)
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/pods-reset/inventory" {
			t.Errorf("unexpected provider effect: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request struct {
			ContentIDs []string `json:"content_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ContentIDs) != 1 {
			t.Errorf("invalid exact inventory request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		objects := make([]gin.H, 0, objectsPerItem)
		for index := 0; index < objectsPerItem; index++ {
			objects = append(objects, gin.H{"storage_tier": "primary", "bucket": "test", "object_key": fmt.Sprintf("content/%s/media-%d", request.ContentIDs[0], index), "etag": "preview-etag", "size_bytes": 8})
		}
		json.NewEncoder(w).Encode(gin.H{"data": gin.H{"complete": true, "configured_tiers": []string{"primary"},
			"storage_bindings": []gin.H{{"storage_tier": "primary", "bucket": "test", "endpoint_fingerprint": strings.Repeat("a", 64)}},
			"version_model":    "cloudflare-r2-current-key-delete-v1", "items": []gin.H{{"content_item_id": request.ContentIDs[0], "objects": objects}}}})
	}))
	defer owner.Close()
	t.Setenv("AGGREGATION_BASE_URL", owner.URL)
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "test-only")
	body, _ := json.Marshal(gin.H{"content_ids": ids, "selection_reason": "Verify large preview"})
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "reset-schema-test", UserID: "admin"})
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/pods-reset/plans", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	CreatePodsResetPlan(c)
	var result struct {
		Data struct {
			Run        models.PodsResetRun   `json:"run"`
			Items      []podsResetPlanTarget `json:"items"`
			Counts     map[string]int64      `json:"counts"`
			CanApprove bool                  `json:"can_approve"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || response.Code != http.StatusCreated || len(result.Data.Items) != itemCount || result.Data.Counts["selected_ids"] != itemCount || result.Data.Counts["object_count"] != itemCount*objectsPerItem || result.Data.Counts["object_bytes"] != itemCount*objectsPerItem*8 || result.Data.CanApprove {
		t.Fatalf("large preview failed: %d %s (%v)", response.Code, response.Body, err)
	}
	for index, item := range result.Data.Items {
		if item.ID != ids[index] {
			t.Fatalf("target order/identity changed at %d", index)
		}
		if item.ID == parentID && containsPodsResetBlocker(item.Blockers, "surviving_child") {
			t.Fatal("full selected family lost during concurrent checks")
		}
		if item.ID == protectedID && (item.MetadataCounts["content_flags"] != 1 || item.MetadataCounts["transcripts"] != 1 || !containsPodsResetBlocker(item.Blockers, "protected_content") || !containsPodsResetBlocker(item.Blockers, "approved_transcript")) {
			t.Fatalf("batched inventory lost protections: %+v", item)
		}
	}
	var persisted, effects int64
	if err := db.Model(&models.PodsResetItem{}).Where("run_id=?", result.Data.Run.PublicID).Count(&persisted).Error; err != nil || persisted != itemCount {
		t.Fatalf("incomplete frozen plan: %d %v", persisted, err)
	}
	var objects []models.PodsResetObject
	if err := db.Where("run_id=?", result.Data.Run.PublicID).Order("content_item_id,object_key").Find(&objects).Error; err != nil || len(objects) != itemCount*objectsPerItem {
		t.Fatalf("incomplete saved object inventory: %d %v", len(objects), err)
	}
	for index, object := range objects {
		if object.ContentItemID != ids[index/objectsPerItem] || object.ObjectKey != fmt.Sprintf("content/%s/media-%d", ids[index/objectsPerItem], index%objectsPerItem) || object.ETag != "preview-etag" || object.SizeBytes != 8 || object.State != "planned" || object.DeletedByRun {
			t.Fatalf("object identity/effect state changed: %+v", object)
		}
	}
	if err := db.Model(&models.PodsResetRetirement{}).Count(&effects).Error; err != nil || effects != 0 {
		t.Fatalf("preview performed retirement: %d %v", effects, err)
	}
}

func verifyPodsResetBatchedArtifactOwnership(t *testing.T, db *gorm.DB) {
	t.Helper()
	const tenant = "reset-ownership-test"
	var ids []uuid.UUID
	if err := db.Raw(`INSERT INTO content_items (tenant_id,type,source,status,title,idempotency_key)
SELECT ?,'VIDEO','YOUTUBE','FAILED','ownership fixture','ownership-'||n FROM generate_series(1,2) n RETURNING public_id`, tenant).Scan(&ids).Error; err != nil {
		t.Fatal(err)
	}
	object := podsreset.ObjectIdentity{StorageTier: "primary", Bucket: "test", ObjectKey: "content/" + ids[0].String() + "/source", ETag: "fixture-etag", SizeBytes: 8}
	for _, parentScoped := range []bool{false, true} {
		for _, otherState := range []string{"verified", "deleted", "soft_deleted"} {
			t.Run(fmt.Sprintf("parent_%t_other_%s", parentScoped, otherState), func(t *testing.T) {
				tx := db.Begin()
				defer tx.Rollback()
				owned := models.MediaArtifactManifest{TenantID: tenant, ContentItemID: &ids[0], ArtifactRole: "source", StorageTier: object.StorageTier, Bucket: object.Bucket, ObjectKey: object.ObjectKey, ETag: object.ETag, SizeBytes: object.SizeBytes, CreatorRole: "test", ProducerEventID: uuid.New(), InputDigest: strings.Repeat("a", 64), State: "verified"}
				if parentScoped {
					owned.ContentItemID, owned.ParentContentItemID = nil, &ids[0]
				}
				if err := tx.Omit("VerificationEvidence", "TerminalProof").Create(&owned).Error; err != nil {
					t.Fatal(err)
				}
				if err := podsResetArtifactOwnershipBlockers(tx, tenant, ids[0], []podsreset.ObjectIdentity{object}); err != nil {
					t.Fatalf("exclusive ownership rejected: %v", err)
				}
				// Simulate legacy duplicate registry evidence inside this rolled-back
				// fixture. The canonical fresh schema normally prevents duplicates.
				if err := tx.Exec(`ALTER TABLE media_artifact_manifests DROP CONSTRAINT media_artifact_manifests_tenant_id_storage_tier_bucket_obje_key`).Error; err != nil {
					t.Fatal(err)
				}
				other := models.MediaArtifactManifest{TenantID: tenant, ContentItemID: &ids[1], ArtifactRole: "source", StorageTier: object.StorageTier, Bucket: object.Bucket, ObjectKey: object.ObjectKey, ETag: object.ETag, SizeBytes: object.SizeBytes, CreatorRole: "test", ProducerEventID: uuid.New(), InputDigest: strings.Repeat("b", 64), State: "verified"}
				if otherState == "deleted" {
					other.State = "deleted"
				} else if otherState == "soft_deleted" {
					now := time.Now().UTC()
					other.DeletedAt = &now
				}
				if err := tx.Omit("VerificationEvidence", "TerminalProof").Create(&other).Error; err != nil {
					t.Fatal(err)
				}
				err := podsResetArtifactOwnershipBlockers(tx, tenant, ids[0], []podsreset.ObjectIdentity{object})
				if otherState == "verified" {
					if err == nil || !strings.Contains(err.Error(), "shared by another item") {
						t.Fatalf("shared identity escaped inventory: %v", err)
					}
				} else if err != nil {
					t.Fatalf("deleted owner blocked exclusive inventory: %v", err)
				}
			})
		}
	}
}

func containsPodsResetBlocker(blockers []podsResetBlocker, code string) bool {
	for _, blocker := range blockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

// This exercises the database fence/finalizer against canonical migrations.
// The owner fixture returns synthetic file identities; it never calls a provider.
// It does not establish real-provider qualification.
func verifyPodsResetProtectionOverrideEffects(t *testing.T, db *gorm.DB) {
	t.Helper()
	const tenant = "override-test"
	var ids []uuid.UUID
	if err := db.Raw(`INSERT INTO content_items (tenant_id,type,source,status,title,idempotency_key)
SELECT ?,'VIDEO','YOUTUBE','PENDING','protected payload','override-item-'||n
FROM generate_series(1,3) n RETURNING public_id`, tenant).Scan(&ids).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range ids[:2] {
		if err := db.Exec(`INSERT INTO content_flags (tenant_id,content_item_id,exclude_from_feed) VALUES (?,?,true)`, tenant, id).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(`INSERT INTO transcripts (content_item_id,full_text,source) VALUES (?,'approved caption payload','youtube_human')`, id).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		if err := db.Exec(`INSERT INTO content_stage_requests (tenant_id,content_item_id,processing_generation,lane,stage,owner,blocking_scope,state,input_fingerprint,idempotency_key)
VALUES (?,?,1,'pods','pods_text_embedding','enrichment','optional','queued',repeat('a',64),?)`, tenant, id, "reset-stage-"+id.String()).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(`INSERT INTO media_atomization_runs (tenant_id,parent_content_item_id,status,phase) VALUES (?,?,'running','legacy')`, tenant, id).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(`INSERT INTO media_circulation_recommendations (tenant_id,unit_type,subject_id,verdict,action,status) VALUES (?,'item_family',?,'retire','evict','pending')`, tenant, id).Error; err != nil {
			t.Fatal(err)
		}
	}
	var sharedTranscript uuid.UUID
	if err := db.Raw(`SELECT public_id FROM transcripts WHERE content_item_id=?`, ids[1]).Row().Scan(&sharedTranscript); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`UPDATE content_items SET transcript_id=? WHERE public_id=?`, sharedTranscript, ids[2]).Error; err != nil {
		t.Fatal(err)
	}
	fixtureObjects := []podsreset.ObjectIdentity{{StorageTier: "primary", Bucket: "test", ObjectKey: "content/" + ids[0].String() + "/original.mp4", ETag: "fixture-etag", SizeBytes: 64}}
	for _, ownerTenant := range []string{tenant, "foreign-tenant"} {
		tx := db.Begin()
		if err := tx.Exec(`INSERT INTO media_artifact_manifests (tenant_id,content_item_id,artifact_role,bucket,object_key,creator_role,producer_event_id,input_digest,state)
VALUES (?,?,'source','test',?,'test',?,repeat('a',64),'verified')`, ownerTenant, ids[2], fixtureObjects[0].ObjectKey, uuid.New()).Error; err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err := podsResetExclusiveFileScope(tx, tenant, ids[0], fixtureObjects, uuidSet(ids[:2])); err == nil {
			tx.Rollback()
			t.Fatal("registry override accepted an external owner")
		}
		tx.Rollback()
	}
	tx := db.Begin()
	if err := tx.Exec(`INSERT INTO content_stage_attempts (tenant_id,request_id,attempt_number,lane,stage,owner,input_fingerprint,state,claim_token,fence_token,deterministic_job_id,lease_expires_at,heartbeat_at,effect_started_at)
SELECT tenant_id,public_id,1,lane,stage,owner,input_fingerprint,'uncertain',?,?,?,now()-interval '1 hour',now(),now()-interval '2 hours'
FROM content_stage_requests WHERE content_item_id=?`, uuid.New(), uuid.New(), "test-effect", ids[0]).Error; err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	var effectContent models.ContentItem
	if err := tx.Where("public_id=?", ids[0]).First(&effectContent).Error; err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if blockers, _, err := podsResetItemBlockers(tx, tenant, effectContent, uuidSet(ids[:2])); err != nil || !containsPodsResetBlocker(blockers, "active_stage_effect") {
		tx.Rollback()
		t.Fatalf("unresolved provider effect accepted: %+v %v", blockers, err)
	}
	tx.Rollback()
	var fixtureDeleted atomic.Bool
	var fixtureFailure atomic.Bool
	fixtureFailure.Store(true)
	var deleteCalls atomic.Int32
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/pods-reset/delete-media-item" {
			var request struct {
				RunID           uuid.UUID                  `json:"run_id"`
				TenantID        string                     `json:"tenant_id"`
				ContentItemID   string                     `json:"content_item_id"`
				ManifestHash    string                     `json:"manifest_hash"`
				FencingToken    string                     `json:"fencing_token"`
				ExecutionToken  string                     `json:"execution_token"`
				StorageBindings []podsreset.StorageBinding `json:"storage_bindings"`
				Objects         []podsResetObjectWire      `json:"objects"`
			}
			var executing models.PodsResetRun
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if err := db.Where("public_id=?", request.RunID).First(&executing).Error; err != nil || executing.ManifestHash != request.ManifestHash || executing.FencingToken == nil || executing.FencingToken.String() != request.FencingToken || executing.ExecutionToken == nil || executing.ExecutionToken.String() != request.ExecutionToken {
				t.Error("deletion request lost the approved manifest or execution claim")
				w.WriteHeader(http.StatusConflict)
				return
			}
			if ready, err := podsResetSelectionFenced(db, executing); err != nil || !ready {
				t.Error("file deletion started before all selected contents were fenced")
				w.WriteHeader(http.StatusConflict)
				return
			}
			expectedCount := 0
			if request.ContentItemID == ids[0].String() {
				expectedCount = 1
			} else if request.ContentItemID != ids[1].String() {
				t.Error("deletion escaped the selected contents")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(request.Objects) != expectedCount || (expectedCount == 1 && request.Objects[0].ObjectKey != fixtureObjects[0].ObjectKey) {
				t.Error("deletion escaped the exact approved object set")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			raw, _ := json.Marshal(request)
			authorization := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(authorization)
			c.Set("db", db)
			c.Set(utils.InternalPrincipalContextKey, utils.MachinePrincipalAggregation)
			c.Request = httptest.NewRequest(http.MethodPost, "/internal/pods-reset/authorize-object-deletion", bytes.NewReader(raw))
			c.Request.Header.Set("Content-Type", "application/json")
			InternalAuthorizePodsResetObjectDeletion(c)
			if authorization.Code != http.StatusOK {
				t.Errorf("CMS rejected the actual deletion callback: %d %s", authorization.Code, authorization.Body)
				w.WriteHeader(http.StatusConflict)
				return
			}
			if fixtureFailure.Swap(false) {
				w.WriteHeader(http.StatusConflict)
				return
			}
			deleteCalls.Add(1)
			if expectedCount == 1 {
				fixtureDeleted.Store(true)
			}
			json.NewEncoder(w).Encode(gin.H{"data": gin.H{"content_item_id": request.ContentItemID, "fencing_token": request.FencingToken,
				"storage_bindings": request.StorageBindings, "objectsAbsent": true, "deletedCount": expectedCount,
				"deletedObjects": request.Objects, "alreadyAbsentObjects": []podsResetObjectWire{}}})
			return
		}
		if r.URL.Path != "/internal/pods-reset/inventory" {
			t.Errorf("unexpected storage effect: %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request struct {
			ContentIDs []string `json:"content_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ContentIDs) != 1 {
			t.Errorf("invalid inventory request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		objects := []gin.H{}
		if request.ContentIDs[0] == ids[0].String() && !fixtureDeleted.Load() {
			objects = append(objects, gin.H{"storage_tier": "primary", "bucket": "test", "object_key": "content/" + ids[0].String() + "/original.mp4", "etag": "fixture-etag", "size_bytes": 64})
		}
		json.NewEncoder(w).Encode(gin.H{"data": gin.H{"complete": true, "configured_tiers": []string{"primary"},
			"storage_bindings": []gin.H{{"storage_tier": "primary", "bucket": "test", "endpoint_fingerprint": strings.Repeat("a", 64)}},
			"version_model":    "cloudflare-r2-current-key-delete-v1", "items": []gin.H{{"content_item_id": request.ContentIDs[0], "objects": objects}}}})
	}))
	defer owner.Close()
	t.Setenv("AGGREGATION_BASE_URL", owner.URL)
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "test-only")
	principal := utils.AdminPrincipal{TenantID: tenant, UserID: "admin", Email: "admin@example.test"}
	invoke := func(handler gin.HandlerFunc, runID uuid.UUID, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		response := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(response)
		c.Set("db", db)
		c.Set(utils.AdminPrincipalContextKey, principal)
		c.Params = gin.Params{{Key: "id", Value: runID.String()}}
		c.Request = httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(raw))
		c.Request.Header.Set("Content-Type", "application/json")
		handler(c)
		return response
	}
	preview := func(codes []string) (models.PodsResetRun, bool) {
		response := invoke(CreatePodsResetPlan, uuid.Nil, gin.H{"content_ids": ids[:2], "selection_reason": "retire reviewed obsolete media", "protection_override_codes": codes, "protection_override_reason": "reviewed obsolete captions and flags"})
		var result struct {
			Data struct {
				Run        models.PodsResetRun `json:"run"`
				CanApprove bool                `json:"can_approve"`
			} `json:"data"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || response.Code != http.StatusCreated {
			t.Fatalf("preview failed: %d %s %v", response.Code, response.Body, err)
		}
		return result.Data.Run, result.Data.CanApprove
	}
	// A selected chapter can reference its selected parent's folder. Both must
	// be fenced before file authorization can succeed.
	if err := db.Exec(`UPDATE content_items SET media_url=? WHERE public_id=?`, "https://storage.test/content/"+ids[0].String()+"/original.mp4", ids[1]).Error; err != nil {
		t.Fatal(err)
	}
	blocked, canApprove := preview(nil)
	if canApprove {
		t.Fatal("default protections bypassed")
	}
	if response := invoke(ApprovePodsResetPlan, blocked.PublicID, gin.H{"phrase": podsResetApprovalPhrase(blocked, 2)}); response.Code != http.StatusConflict {
		t.Fatalf("blocked default preview approved: %d %s", response.Code, response.Body)
	}
	if _, canApprove := preview([]string{"approved_transcript", "protected_content"}); canApprove {
		t.Fatal("content-policy choices bypassed unfinished work or registry evidence")
	}
	run, canApprove := preview([]string{"approved_transcript", "protected_content", "content_not_terminal", "active_content_stage", "active_atomization_run", "active_circulation_recommendation", "artifact_ownership_unproven"})
	if !canApprove || run.ManifestHash == blocked.ManifestHash {
		t.Fatal("override preview did not get separate eligible authority")
	}
	if response := invoke(ApprovePodsResetPlan, run.PublicID, gin.H{"phrase": podsResetApprovalPhrase(blocked, 2)}); response.Code != http.StatusConflict {
		t.Fatal("old manifest phrase approved new choices")
	}
	if response := invoke(ApprovePodsResetPlan, run.PublicID, gin.H{"phrase": podsResetApprovalPhrase(run, 2)}); response.Code != http.StatusOK {
		t.Fatalf("override approval failed: %d %s", response.Code, response.Body)
	}
	// Approved plans remain executable after their preview's expiry. Execution
	// still checks the live content, schema and exact file inventory.
	if err := db.Model(&models.PodsResetRun{}).Where("public_id=?", run.PublicID).Update("expires_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	var err error
	run, err = podsResetAcquireExecutionClaim(db, tenant, run.PublicID)
	if err != nil {
		t.Fatal(err)
	}
	defer podsResetReleaseExecutionClaim(db, run)
	var items []models.PodsResetItem
	if err := db.Where("run_id=?", run.PublicID).Find(&items).Error; err != nil || len(items) != 2 {
		t.Fatalf("frozen items: %v", err)
	}
	for index := range items {
		item := &items[index]
		var content models.ContentItem
		if err := db.Where("public_id=?", item.ContentItemID).First(&content).Error; err != nil {
			t.Fatal(err)
		}
		if item.ContentItemID == ids[0] {
			// Another item's file reference remains protected despite the override.
			if err := db.Exec(`UPDATE content_items SET media_url=? WHERE public_id=?`, "https://storage.test/content/"+ids[0].String()+"/original.mp4", ids[2]).Error; err != nil {
				t.Fatal(err)
			}
			if err := podsResetFenceItem(db, run, item, &content, uuidSet(ids[:2])); err == nil {
				t.Fatal("registry override deleted another content item's referenced file")
			}
			if err := db.Exec(`UPDATE content_items SET media_url=NULL WHERE public_id=?`, ids[2]).Error; err != nil {
				t.Fatal(err)
			}
		}
		if err := podsResetFenceItem(db, run, item, &content, uuidSet(ids[:2])); err != nil {
			t.Fatalf("explicit protection fence: %v", err)
		}
		if ready, err := podsResetSelectionFenced(db, run); err != nil || ready != (index == len(items)-1) {
			t.Fatalf("file deletion authorized before the complete selection fence: %v %v", ready, err)
		}
	}
	// A new external reference after all fences must still stop deletion.
	tx = db.Begin()
	if err := tx.Exec(`UPDATE content_items SET media_url=? WHERE public_id=?`, "https://storage.test/content/"+ids[0].String()+"/original.mp4", ids[2]).Error; err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := podsResetRevalidateArtifactOwnership(tx, run, ids[0], fixtureObjects); err == nil {
		tx.Rollback()
		t.Fatal("late external file reference permitted deletion")
	}
	tx.Rollback()
	if err := podsResetReleaseExecutionClaim(db, run); err != nil {
		t.Fatal(err)
	}
	if response := invoke(ExecutePodsResetRun, run.PublicID, gin.H{"manifest_hash": run.ManifestHash, "confirmed_item_count": 1}); response.Code != http.StatusBadRequest || deleteCalls.Load() != 0 {
		t.Fatalf("wrong count reached storage deletion: %d %s", response.Code, response.Body)
	}
	confirmation := gin.H{"manifest_hash": run.ManifestHash, "confirmed_item_count": 2}
	if response := invoke(ExecutePodsResetRun, run.PublicID, confirmation); response.Code != http.StatusAccepted || !strings.Contains(response.Body.String(), `"phase":"partial"`) || !strings.Contains(response.Body.String(), `"warning":"Aggregation rejected file deletion`) || deleteCalls.Load() != 0 {
		t.Fatalf("failed deletion was advertised as automatic progress: %d %s", response.Code, response.Body)
	}
	if response := invoke(ExecutePodsResetRun, run.PublicID, confirmation); response.Code != http.StatusAccepted || deleteCalls.Load() != 2 || !fixtureDeleted.Load() {
		t.Fatalf("approved execution did not delete the fixture files: %d %s", response.Code, response.Body)
	}
	if response := invoke(ExecutePodsResetRun, run.PublicID, confirmation); response.Code != http.StatusAccepted || deleteCalls.Load() != 2 {
		t.Fatalf("verification retry repeated file deletion: %d %s", response.Code, response.Body)
	}
	// Advance only the fixture's verification clock; exercise the real second
	// inventory probe and finalizer instead of fabricating successful evidence.
	if err := db.Model(&models.PodsResetItem{}).Where("run_id=?", run.PublicID).Update("verification_not_before", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	if response := invoke(ExecutePodsResetRun, run.PublicID, confirmation); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"complete"`) || deleteCalls.Load() != 2 {
		t.Fatalf("execution did not complete after verified absence: %d %s", response.Code, response.Body)
	}
	var ownedText, sharedText string
	if err := db.Raw(`SELECT full_text FROM transcripts WHERE content_item_id=?`, ids[0]).Row().Scan(&ownedText); err != nil {
		t.Fatal(err)
	}
	if err := db.Raw(`SELECT full_text FROM transcripts WHERE public_id=?`, sharedTranscript).Row().Scan(&sharedText); err != nil {
		t.Fatal(err)
	}
	if ownedText != "" || sharedText != "approved caption payload" {
		t.Fatalf("caption ownership violated: owned=%q shared=%q", ownedText, sharedText)
	}
	var flags int64
	if err := db.Table("content_flags").Where("tenant_id=?", tenant).Count(&flags).Error; err != nil || flags != 2 {
		t.Fatalf("protection history lost: %d %v", flags, err)
	}
	var survivor models.ContentItem
	if err := db.Where("public_id=?", ids[2]).First(&survivor).Error; err != nil || survivor.RetiredPayloadAt != nil || survivor.Title == nil || *survivor.Title != "protected payload" || survivor.TranscriptID == nil || *survivor.TranscriptID != sharedTranscript {
		t.Fatalf("surviving content changed: %+v %v", survivor, err)
	}
	for _, row := range []struct{ table, ownerColumn, stateColumn, selected, survivor string }{
		{"content_stage_requests", "content_item_id", "state", "cancelled", "queued"},
		{"media_atomization_runs", "parent_content_item_id", "status", "failed", "running"},
		{"media_circulation_recommendations", "subject_id", "status", "superseded", "pending"},
	} {
		var count int64
		if err := db.Table(row.table).Where("tenant_id=? AND "+row.ownerColumn+" IN ? AND "+row.stateColumn+"=?", tenant, ids[:2], row.selected).Count(&count).Error; err != nil || count != 2 {
			t.Fatalf("selected work was not discarded in %s: %d %v", row.table, count, err)
		}
		if err := db.Table(row.table).Where("tenant_id=? AND "+row.ownerColumn+"=? AND "+row.stateColumn+"=?", tenant, ids[2], row.survivor).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("unselected work changed in %s: %d %v", row.table, count, err)
		}
	}
	var actions []models.PodsResetAction
	if err := db.Where("run_id=?", run.PublicID).Find(&actions).Error; err != nil || len(actions) != 2 {
		t.Fatalf("action ledger: %v", err)
	}
	for _, action := range actions {
		if !bytes.Contains(action.Result, []byte("approved_transcript")) || !bytes.Contains(action.Result, []byte("reviewed obsolete captions and flags")) {
			t.Fatal("override evidence absent from action ledger")
		}
	}
	if err := db.Exec(`UPDATE content_items SET title='late producer write' WHERE public_id=?`, ids[0]).Error; err == nil {
		t.Fatal("override weakened permanent producer fence")
	}
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, principal)
	c.Request = httptest.NewRequest(http.MethodGet, "/admin/pods-reset/candidates", nil)
	ListPodsResetCandidates(c)
	var candidates struct {
		Data struct {
			Items      []podsResetCandidate `json:"items"`
			TotalCount int64                `json:"total_count"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &candidates); err != nil || response.Code != http.StatusOK || candidates.Data.TotalCount != 1 || len(candidates.Data.Items) != 1 || candidates.Data.Items[0].ContentItemID != ids[2] {
		t.Fatalf("deleted payloads remained reset candidates: %d %s %v", response.Code, response.Body, err)
	}
}

func verifyPodsResetBatchContinuation(t *testing.T, db *gorm.DB) {
	t.Helper()
	const tenant = "batch-continuation-test"
	var ids []uuid.UUID
	if err := db.Raw(`INSERT INTO content_items (tenant_id,type,source,status,title,idempotency_key)
SELECT ?,'VIDEO','YOUTUBE','FAILED','batch payload','batch-item-'||n
FROM generate_series(1,4) n RETURNING public_id`, tenant).Scan(&ids).Error; err != nil {
		t.Fatal(err)
	}
	var deleteCalls atomic.Int32
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RunID           uuid.UUID                  `json:"run_id"`
			ContentIDs      []string                   `json:"content_ids"`
			ContentItemID   string                     `json:"content_item_id"`
			FencingToken    string                     `json:"fencing_token"`
			StorageBindings []podsreset.StorageBinding `json:"storage_bindings"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/internal/pods-reset/inventory":
			if len(request.ContentIDs) != 1 {
				t.Error("inventory lost exact item scope")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			json.NewEncoder(w).Encode(gin.H{"data": gin.H{"complete": true, "configured_tiers": []string{"primary"},
				"storage_bindings": []gin.H{{"storage_tier": "primary", "bucket": "test", "endpoint_fingerprint": strings.Repeat("a", 64)}},
				"version_model":    "cloudflare-r2-current-key-delete-v1", "items": []gin.H{{"content_item_id": request.ContentIDs[0], "objects": []gin.H{}}}}})
		case "/internal/pods-reset/delete-media-item":
			var executing models.PodsResetRun
			if err := db.Where("public_id=?", request.RunID).First(&executing).Error; err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusConflict)
				return
			}
			if ready, err := podsResetSelectionFenced(db, executing); err != nil || !ready {
				t.Error("deletion began before the entire selection was prepared")
				w.WriteHeader(http.StatusConflict)
				return
			}
			deleteCalls.Add(1)
			json.NewEncoder(w).Encode(gin.H{"data": gin.H{"content_item_id": request.ContentItemID, "fencing_token": request.FencingToken,
				"storage_bindings": request.StorageBindings, "objectsAbsent": true, "deletedCount": 0,
				"deletedObjects": []podsResetObjectWire{}, "alreadyAbsentObjects": []podsResetObjectWire{}}})
		default:
			t.Errorf("unexpected storage operation: %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer owner.Close()
	t.Setenv("AGGREGATION_BASE_URL", owner.URL)
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "test-only")
	invoke := func(handler gin.HandlerFunc, runID uuid.UUID, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		response := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(response)
		c.Set("db", db)
		c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: tenant, UserID: "admin"})
		c.Params = gin.Params{{Key: "id", Value: runID.String()}}
		c.Request = httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(raw))
		c.Request.Header.Set("Content-Type", "application/json")
		handler(c)
		return response
	}
	response := invoke(CreatePodsResetPlan, uuid.Nil, gin.H{"content_ids": ids, "selection_reason": "Verify complete batch continuation"})
	var preview struct {
		Data struct {
			Run        models.PodsResetRun `json:"run"`
			CanApprove bool                `json:"can_approve"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil || response.Code != http.StatusCreated || !preview.Data.CanApprove {
		t.Fatalf("batch preview: %d %s %v", response.Code, response.Body, err)
	}
	run := preview.Data.Run
	if response := invoke(ApprovePodsResetPlan, run.PublicID, gin.H{"phrase": podsResetApprovalPhrase(run, 4)}); response.Code != http.StatusOK {
		t.Fatalf("batch approval: %d %s", response.Code, response.Body)
	}
	confirmation := gin.H{"manifest_hash": run.ManifestHash, "confirmed_item_count": 4}
	for index, expectedDeletes := range []int32{0, 3, 4} {
		response := invoke(ExecutePodsResetRun, run.PublicID, confirmation)
		if response.Code != http.StatusAccepted || deleteCalls.Load() != expectedDeletes {
			t.Fatalf("continuation %d starved later items or deleted before full preparation: %d %s (deletions=%d)", index, response.Code, response.Body, deleteCalls.Load())
		}
		if index == 0 && !strings.Contains(response.Body.String(), `"phase":"fencing"`) {
			t.Fatal("preparation progress was not distinguishable from file deletion")
		}
	}
	// Advance only the disposable fixture's probe clock.
	if err := db.Model(&models.PodsResetItem{}).Where("run_id=?", run.PublicID).Update("verification_not_before", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	if response := invoke(ExecutePodsResetRun, run.PublicID, confirmation); response.Code != http.StatusAccepted {
		t.Fatalf("finalization batch: %d %s", response.Code, response.Body)
	}
	if response := invoke(ExecutePodsResetRun, run.PublicID, confirmation); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"complete"`) || deleteCalls.Load() != 4 {
		t.Fatalf("full selection did not complete without repeating file deletion: %d %s", response.Code, response.Body)
	}
	var remaining int64
	if err := db.Model(&models.ContentItem{}).Where("tenant_id=? AND retired_payload_at IS NULL", tenant).Count(&remaining).Error; err != nil || remaining != 0 {
		t.Fatalf("completed selection retained payloads: %d %v", remaining, err)
	}
}

func verifyCanonicalNewsTypeCompatibility(t *testing.T, db *gorm.DB) {
	t.Helper()
	t.Run("fresh_news_intake", func(t *testing.T) {
		tx := db.Begin()
		if tx.Error != nil {
			t.Fatal(tx.Error)
		}
		defer tx.Rollback()
		verifyCanonicalNewsInserts(t, tx)
	})
	// Reproduce an upgraded database with the original check and retained data.
	if err := db.Exec(`ALTER TABLE content_items DROP CONSTRAINT content_items_type_check;
ALTER TABLE content_items ADD CONSTRAINT content_items_type_check
CHECK (type IN ('ARTICLE','VIDEO','TWEET','COMMENT','PODCAST'));
INSERT INTO content_items (tenant_id,type,source,status,title,body_text,idempotency_key)
VALUES ('reset-schema-test','ARTICLE','RSS','ARCHIVED','retained legacy','retained body','legacy-news')`).Error; err != nil {
		t.Fatal(err)
	}
	var before, after string
	if err := db.Raw(`SELECT row_to_json(c)::text FROM content_items c WHERE idempotency_key='legacy-news'`).Row().Scan(&before); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../migrations/20261006110000_content_news_type_compatibility.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(migration)).Error; err != nil {
		t.Fatalf("upgrade NEWS constraint: %v", err)
	}
	if err := db.Raw(`SELECT row_to_json(c)::text FROM content_items c WHERE idempotency_key='legacy-news'`).Row().Scan(&after); err != nil || before != after {
		t.Fatalf("NEWS compatibility changed retained legacy data: %v", err)
	}
	verifyCanonicalNewsInserts(t, db)
	if err := db.Exec(`INSERT INTO content_items (tenant_id,type,source,title) VALUES ('reset-schema-test','UNKNOWN','RSS','invalid')`).Error; err == nil || !strings.Contains(err.Error(), "content_items_type_check") {
		t.Fatalf("NEWS compatibility admitted an unsupported kind: %v", err)
	}
}

func verifyCanonicalNewsInserts(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, format := range []string{"ARTICLE", "TWEET", "COMMENT"} {
		var sequence int64
		if err := db.Raw(`INSERT INTO content_items (tenant_id,type,format,source,status,title,idempotency_key)
VALUES ('reset-schema-test','NEWS',?,'RSS','PENDING','canonical news',?) RETURNING inventory_sequence`, format, "canonical-"+format).Row().Scan(&sequence); err != nil || sequence < 1 {
			t.Fatalf("NEWS %s intake failed: sequence=%d error=%v", format, sequence, err)
		}
	}
}
