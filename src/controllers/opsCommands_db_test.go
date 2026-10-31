package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func opsCommandDB(t *testing.T) *gorm.DB {
	t.Helper()
	return opsTestDB(t)
}
func opsReviewForTest(t *testing.T, db *gorm.DB, p utils.AdminPrincipal, scope opsCommandScope) opsCommandReview {
	t.Helper()
	targets, _, err := opsPreviewTargets(db, p, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err = opsCheckTargetStatus(db, targets, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return opsCommandReview{SnapshotAt: time.Now().UTC(), TargetDigest: opsTargetsDigest(scope, targets)}
}
func opsCount(t *testing.T, row models.OpsFleetCommand, key string) int {
	t.Helper()
	counts := map[string]int{}
	if err := json.Unmarshal(row.Counts, &counts); err != nil {
		t.Fatal(err)
	}
	return counts[key]
}
func TestOpsCommandsIntegration(t *testing.T) {
	db := opsCommandDB(t)
	principal := utils.AdminPrincipal{TenantID: "ops-test", UserID: "ops-admin", Email: "ops@example.test"}
	seed := func(t *testing.T, tenant string) {
		t.Helper()
		p := models.DefaultPipelineAutopilotPolicy(tenant)
		if err := db.Create(&p).Error; err != nil {
			t.Fatal(err)
		}
	}
	scope := func(tenant string) opsCommandScope {
		return opsCommandScope{Command: "pause_member", MemberKey: "pipeline", LaneKey: "autopilot", TenantID: tenant}
	}
	execute := func(t *testing.T, s opsCommandScope, ttl int, key string) models.OpsFleetCommand {
		t.Helper()
		r, err := opsExecuteCommand(db, principal, s, opsReviewForTest(t, db, principal, s), "Qualification", ttl, key)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	current := func(t *testing.T, tenant string) *time.Time {
		t.Helper()
		var p models.PipelineAutopilotPolicy
		if err := db.Where("tenant_id=?", tenant).First(&p).Error; err != nil {
			t.Fatal(err)
		}
		return p.PausedUntil
	}

	t.Run("legacy UTC timestamp accepts a ledger value in another zone", func(t *testing.T) {
		seed(t, "time-zone")
		member, lane, _ := opsFindLane("pipeline", "autopilot")
		target := opsPauseTarget{Member: member, Lane: lane, TenantID: "time-zone", Exists: true}
		instant := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
		local := instant.In(time.FixedZone("ledger-zone", 3*60*60))
		canonical, err := opsWriteTarget(db, target, &local, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if canonical == nil || !canonical.Equal(instant) || !current(t, "time-zone").Equal(instant) {
			t.Fatalf("legacy timestamp shifted the restored instant: %v want %v", canonical, instant)
		}
	})
	t.Run("exact tenant and nested source restoration", func(t *testing.T) {
		seed(t, "tenant-a")
		seed(t, "tenant-b")
		a := execute(t, scope("tenant-a"), 30, "nested-a")
		aUntil := current(t, "tenant-a")
		if aUntil == nil || current(t, "tenant-b") != nil || opsCount(t, a, "paused") != 1 {
			t.Fatal("pause escaped tenant scope")
		}
		b := execute(t, scope("tenant-a"), 60, "nested-b")
		// Earlier source cannot remove the later extension.
		skipped := execute(t, opsCommandScope{Command: "resume", SourceCommandID: a.PublicID.String()}, 0, "resume-a-early")
		if opsCount(t, skipped, "skipped") != 1 {
			t.Fatal("earlier source stole a newer pause")
		}
		restored := execute(t, opsCommandScope{Command: "resume", SourceCommandID: b.PublicID.String()}, 0, "resume-b")
		if opsCount(t, restored, "restored") != 1 || !current(t, "tenant-a").Equal(*aUntil) {
			t.Fatalf("nested resume did not reveal prior pause: status=%s counts=%s actual=%v expected=%v", restored.Status, restored.Counts, current(t, "tenant-a"), aUntil)
		}
		cleared := execute(t, opsCommandScope{Command: "resume", SourceCommandID: a.PublicID.String()}, 0, "resume-a")
		if opsCount(t, cleared, "cleared") != 1 || current(t, "tenant-a") != nil {
			t.Fatal("source pause did not clear")
		}
	})
	t.Run("closely spaced foreign pause remains owned", func(t *testing.T) {
		seed(t, "near")
		pause := execute(t, scope("near"), 30, "near-a")
		foreign := current(t, "near").Add(500 * time.Millisecond)
		if err := db.Model(&models.PipelineAutopilotPolicy{}).Where("tenant_id=?", "near").Update("paused_until", foreign).Error; err != nil {
			t.Fatal(err)
		}
		result := execute(t, opsCommandScope{Command: "resume", SourceCommandID: pause.PublicID.String()}, 0, "near-resume")
		if opsCount(t, result, "skipped") != 1 || !current(t, "near").Equal(foreign) {
			t.Fatal("subsecond foreign ownership was cleared")
		}
	})
	t.Run("concurrent idempotent submissions return one envelope", func(t *testing.T) {
		seed(t, "concurrent")
		s := scope("concurrent")
		review := opsReviewForTest(t, db, principal, s)
		var wg sync.WaitGroup
		results := make(chan models.OpsFleetCommand, 2)
		errs := make(chan error, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, e := opsExecuteCommand(db, principal, s, review, "Qualification", 30, "same-key")
				results <- r
				errs <- e
			}()
		}
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		id := uuid.Nil
		for r := range results {
			if id != uuid.Nil && r.PublicID != id {
				t.Fatal("duplicate envelope")
			}
			id = r.PublicID
		}
		var n int64
		db.Model(&models.OpsFleetCommand{}).Where("idempotency_key='same-key'").Count(&n)
		if n != 1 {
			t.Fatalf("envelopes=%d", n)
		}
	})
	t.Run("stale and changed reviews cannot apply", func(t *testing.T) {
		seed(t, "stale")
		s := scope("stale")
		review := opsReviewForTest(t, db, principal, s)
		review.SnapshotAt = time.Now().Add(-2 * time.Minute)
		if _, err := opsExecuteCommand(db, principal, s, review, "Qualification", 30, "stale-key"); err == nil {
			t.Fatal("accepted stale review")
		}
		review = opsReviewForTest(t, db, principal, s)
		human := time.Now().Add(3 * time.Hour)
		db.Model(&models.PipelineAutopilotPolicy{}).Where("tenant_id='stale'").Update("paused_until", human)
		canonicalHuman := current(t, "stale")
		row, err := opsExecuteCommand(db, principal, s, review, "Qualification", 30, "changed-key")
		if err != nil {
			t.Fatal(err)
		}
		if row.Status != "failed" || !current(t, "stale").Equal(*canonicalHuman) {
			t.Fatalf("changed review mutated pause: status=%s counts=%s actual=%v expected=%v", row.Status, row.Counts, current(t, "stale"), canonicalHuman)
		}
	})
	t.Run("effect failure rolls back and persists failed ledger", func(t *testing.T) {
		seed(t, "rollback")
		trigger := `CREATE FUNCTION ops_reject_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.tenant_id='rollback' THEN RAISE EXCEPTION 'qualification effect failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER ops_reject_pause BEFORE UPDATE ON pipeline_autopilot_policies FOR EACH ROW EXECUTE FUNCTION ops_reject_pause()`
		if err := db.Exec(trigger).Error; err != nil {
			t.Fatal(err)
		}
		defer db.Exec("DROP TRIGGER ops_reject_pause ON pipeline_autopilot_policies; DROP FUNCTION ops_reject_pause()")
		row := execute(t, scope("rollback"), 30, "rollback-key")
		if row.Status != "failed" || current(t, "rollback") != nil || opsCount(t, row, "failed") != 1 {
			t.Fatal("failed command did not roll back")
		}
		var audits int64
		db.Model(&models.AuditLog{}).Where("payload->>'command_id'=?", row.PublicID.String()).Count(&audits)
		if audits != 1 {
			t.Fatal("failed command has no audit")
		}
		var actions []models.OpsFleetCommandAction
		db.Where("command_id=?", row.ID).Find(&actions)
		if len(actions) != 1 || !strings.Contains(actions[0].Reason, "qualification effect failure") {
			t.Fatal("failed explanation missing")
		}
	})

	t.Run("interrupted effects roll back and retain a failure receipt", func(t *testing.T) {
		seed(t, "interrupted")
		s := scope("interrupted")
		review := opsReviewForTest(t, db, principal, s)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := db.Callback().Row().Before("gorm:row").Register("ops_qualification_cancel", func(tx *gorm.DB) {
			if strings.HasPrefix(tx.Statement.SQL.String(), "UPDATE pipeline_autopilot_policies") {
				cancel()
			}
		}); err != nil {
			t.Fatal(err)
		}
		defer db.Callback().Row().Remove("ops_qualification_cancel")
		row, err := opsExecuteCommand(db.WithContext(ctx), principal, s, review, "Qualification", 30, "interrupted-key")
		if err != nil {
			t.Fatal(err)
		}
		if row.Status != "failed" || current(t, "interrupted") != nil {
			t.Fatal("cancelled request retained an effect")
		}
		var actions []models.OpsFleetCommandAction
		db.Where("command_id=?", row.ID).Find(&actions)
		if len(actions) != 1 || actions[0].Guardrail != "request_interrupted" {
			t.Fatalf("interrupted receipt missing: %+v", actions)
		}
	})
	t.Run("unconfigured rows stay absent and over-cap cannot lock or mutate", func(t *testing.T) {
		r := execute(t, scope("missing"), 30, "missing-key")
		if opsCount(t, r, "skipped") != 1 {
			t.Fatal("unconfigured target not explained")
		}
		var n int64
		db.Model(&models.PipelineAutopilotPolicy{}).Where("tenant_id='missing'").Count(&n)
		if n != 0 {
			t.Fatal("command created policy")
		}
		rows := make([]models.PipelineAutopilotPolicy, opsCommandMaxTargets+1)
		for i := range rows {
			rows[i] = models.DefaultPipelineAutopilotPolicy(fmt.Sprintf("cap-%03d", i))
		}
		if err := db.Create(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := opsPauseTargets(db, opsCommandScope{Command: "pause_member", MemberKey: "pipeline", LaneKey: "autopilot"}); err == nil {
			t.Fatal("target cap bypassed")
		}
		db.Model(&models.PipelineAutopilotPolicy{}).Where("tenant_id LIKE 'cap-%' AND paused_until IS NOT NULL").Count(&n)
		if n != 0 {
			t.Fatal("over-cap read changed policies")
		}
	})
}
