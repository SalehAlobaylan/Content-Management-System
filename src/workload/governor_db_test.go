package workload

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func governorDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" && os.Getenv("CMS_TEST_DATABASE_URL") == "" {
		t.Skip("requires explicitly guarded disposable CMS test database")
	}
	db := testdb.Open(t)
	// testdb guards the target before opening it. Each admin-backed invocation
	// gets a random database; the local fallback is disposable by explicit opt-in.
	if err := db.Exec("DROP TABLE IF EXISTS workload_decisions,workload_permits,workload_capacity_snapshots,workload_governor_policies").Error; err != nil {
		t.Fatal(err)
	}
	sql, err := os.ReadFile("../../migrations/20261020010000_workload_governor.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Exec(string(sql)).Error; err != nil {
		t.Fatalf("canonical workload migration: %v", err)
	}
	return db
}

func healthyEngine(t *testing.T, db *gorm.DB, mode string, classes ...string) *Engine {
	t.Helper()
	p := defaultPolicy()
	p.Mode = mode
	p.EnforcedClasses = jsonValue(classes)
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	e := NewEngine(db)
	e.failureObserver = func(err error) { t.Logf("isolated Governor admission failure: %v", err) }
	e.SampleProvider = func(_ context.Context, keys []string) (map[string]Sample, error) {
		now := time.Now().UTC()
		result := map[string]Sample{}
		for _, key := range keys {
			result[key] = Sample{Available: true, Healthy: true, ObservedAt: now}
		}
		return result, nil
	}
	e.SpendProvider = func(_ context.Context, _ *gorm.DB, r Request) (SpendSignal, error) {
		if r.TenantID == "" {
			return SpendSignal{}, errors.New("tenant required")
		}
		return SpendSignal{Available: true, State: "normal", ObservedAt: time.Now().UTC()}, nil
	}
	if err := e.RefreshSnapshots(); err != nil {
		t.Fatal(err)
	}
	return e
}

func uniqueRequest(class string) Request {
	r := validRequest(class, 1)
	r.RunID = uuid.NewString()
	r.OwnerID = r.RunID
	return r
}

func TestGovernorDBSnapshotSamplingDoesNotBlockAdmission(t *testing.T) {
	db := governorDB(t)
	e := healthyEngine(t, db, ModeEnforce, "text_embedding")
	started, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	defer release()
	e.SampleProvider = func(ctx context.Context, keys []string) (map[string]Sample, error) {
		observedAt := time.Now().UTC()
		close(started)
		select {
		case <-proceed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		samples := map[string]Sample{}
		for _, key := range keys {
			samples[key] = Sample{Available: true, Healthy: true, ObservedAt: observedAt}
		}
		return samples, nil
	}
	refreshed := make(chan error, 1)
	go func() { refreshed <- e.RefreshSnapshots() }()
	select {
	case <-started:
	case err := <-refreshed:
		t.Fatalf("refresh failed before sampling: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("refresh did not begin sampling")
	}
	decision := e.RequestPermit(uniqueRequest("text_embedding"))
	// Concurrent native evidence must survive completion of the older refresh.
	now := time.Now().UTC()
	newer := snapshotFromSample("enrichment_svc", Sample{Available: true, Healthy: false, Incident: true, ObservedAt: now}, models.WorkloadCapacitySnapshot{}, defaultPolicy(), now)
	if err := db.Create(&newer).Error; err != nil {
		release()
		<-refreshed
		t.Fatal(err)
	}
	release()
	if err := <-refreshed; err != nil {
		t.Fatalf("background refresh failed: %v", err)
	}
	if !decision.Allowed() || decision.Unavailable || decision.WouldVerdict != "admit" {
		t.Fatalf("background sampling blocked a healthy admission: %+v", decision)
	}
	latest, err := latestSnapshots(db, []string{"enrichment_svc"})
	if err != nil || latest["enrichment_svc"].ID != newer.ID || latest["enrichment_svc"].State != "saturated" {
		t.Fatalf("older collection replaced newer native incident evidence: %+v, %v", latest, err)
	}
}

func TestGovernorDBSnapshotRefreshAllowsRemoteSamplingLatency(t *testing.T) {
	db := governorDB(t)
	e := NewEngine(db)
	e.SampleProvider = func(ctx context.Context, keys []string) (map[string]Sample, error) {
		// Reproduce the observed database latency past the old sampling cutoff.
		if err := db.WithContext(ctx).Exec("SELECT pg_sleep(2.1)").Error; err != nil {
			return nil, err
		}
		samples := map[string]Sample{}
		for _, key := range keys {
			samples[key] = Sample{Available: true, Healthy: true, ObservedAt: time.Now().UTC()}
		}
		return samples, nil
	}
	if err := e.RefreshSnapshots(); err != nil {
		t.Fatalf("remote sampling latency discarded heartbeat: %v", err)
	}
	status, err := e.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.HeartbeatAt == nil || time.Since(*status.HeartbeatAt) > 15*time.Second {
		t.Fatal("successful collection did not persist a current heartbeat")
	}
	for _, resource := range status.Resources {
		if !resource.Fresh || !resource.Available || !resource.Healthy {
			t.Fatalf("collected resource was not current: %+v", resource)
		}
	}
}

func TestGovernorDBConcurrentOverlappingResourcesNeverOvergrant(t *testing.T) {
	db := governorDB(t)
	e := healthyEngine(t, db, ModeEnforce, "text_embedding", "llm_batch")
	p, err := GetPolicy(db)
	if err != nil {
		t.Fatal(err)
	}
	limits := limitsFor(p)
	limits["enrichment_svc"] = ResourceLimit{Capacity: 2}
	limits["llm_slots"] = ResourceLimit{Capacity: 1}
	if err = db.Model(&p).Update("resource_limits", jsonValue(limits)).Error; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan Decision, 12)
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			class := "text_embedding"
			if i%2 == 0 {
				class = "llm_batch"
			}
			results <- e.RequestPermit(uniqueRequest(class))
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	grants := 0
	for d := range results {
		if d.Unavailable {
			t.Fatalf("healthy contention failed open: %+v", d)
		}
		grants += d.GrantedUnits
	}
	if grants > 2 || grants < 1 {
		t.Fatalf("shared enrichment slots overgranted or no useful work: %d", grants)
	}
	reserved, err := reservedUnits(db, []string{"enrichment_svc", "llm_slots"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if reserved["enrichment_svc"] > 2 || reserved["llm_slots"] > 1 {
		t.Fatalf("overlapping class reservation overflow: %+v", reserved)
	}
}

func TestGovernorDBObserveHumanAndCampaignHaveDistinctAuthority(t *testing.T) {
	db := governorDB(t)
	e := healthyEngine(t, db, ModeObserve, "text_embedding")
	r := uniqueRequest("text_embedding")
	r.Units = 9
	d := e.RequestPermit(r)
	if !d.Allowed() || d.GrantedUnits != 9 || d.WouldVerdict != "admit_reduced" || d.Unavailable {
		t.Fatalf("Observe changed native work: %+v", d)
	}
	if err := ReleasePermit(db, d.PermitID, 9); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.WorkloadGovernorPolicy{}).Where("scope='platform'").Update("mode", ModeEnforce).Error; err != nil {
		t.Fatal(err)
	}
	human := uniqueRequest("text_embedding")
	human.Units = 9
	human.Origin = OriginHuman
	d = e.RequestPermit(human)
	if !d.Allowed() || d.GrantedUnits != 9 || !d.Bypass {
		t.Fatalf("trusted direct human did not bypass: %+v", d)
	}
	campaign := uniqueRequest("text_embedding")
	campaign.Origin = OriginCampaign
	if d = e.RequestPermit(campaign); d.Allowed() || d.Bypass || d.Verdict != "defer" {
		t.Fatalf("human-started campaign bypassed capacity: %+v", d)
	}
}

func TestGovernorDBIdempotencyExpiryAndNativeUncertainty(t *testing.T) {
	db := governorDB(t)
	e := healthyEngine(t, db, ModeEnforce, "image_embedding")
	r := uniqueRequest("image_embedding")
	d := e.RequestPermit(r)
	if !d.Allowed() || d.PermitID == "" {
		t.Fatal(d)
	}
	duplicate := e.RequestPermit(r)
	if duplicate.PermitID != d.PermitID {
		t.Fatalf("duplicate request allocated twice: %+v %+v", d, duplicate)
	}
	if err := db.Model(&models.WorkloadPermit{}).Where("id=?", d.PermitID).Update("expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if err := Maintain(db); err != nil {
		t.Fatal(err)
	}
	reacquired := e.RequestPermit(r)
	if !reacquired.Allowed() || reacquired.PermitID != d.PermitID {
		t.Fatalf("safe pre-start retry stranded: %+v", reacquired)
	}
	if err := BeginPermit(db, d.PermitID); err != nil {
		t.Fatal(err)
	}
	if err := MarkUncertain(db, d.PermitID, "native effect outcome unknown"); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.WorkloadPermit{}).Where("id=?", d.PermitID).Updates(map[string]any{"expires_at": time.Now().Add(-48 * time.Hour), "created_at": time.Now().Add(-48 * 24 * time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := Maintain(db); err != nil {
		t.Fatal(err)
	}
	var permit models.WorkloadPermit
	if err := db.First(&permit, "id=?", d.PermitID).Error; err != nil {
		t.Fatal(err)
	}
	if permit.State != StateUncertain {
		t.Fatalf("TTL/retention fabricated terminal proof: %+v", permit)
	}
	if err := RevokePermit(db, d.PermitID, "admin", "free slots"); !errors.Is(err, ErrUnsafeRevoke) {
		t.Fatalf("started uncertain work revoked: %v", err)
	}
	if err := SettleOwner(db, r.TenantID, r.OwnerKind, r.OwnerID, 1, "verified exact native terminal receipt"); err != nil {
		t.Fatal(err)
	}
	reserved, err := reservedUnits(db, []string{"media_svc"}, time.Now().UTC())
	if err != nil || reserved["media_svc"] != 0 {
		t.Fatalf("owner terminal did not settle: %+v %v", reserved, err)
	}
}

func TestGovernorDBRequestSettlementDoesNotFreeOlderUnknownAttempt(t *testing.T) {
	db := governorDB(t)
	e := healthyEngine(t, db, ModeEnforce, "text_embedding")
	r := uniqueRequest("text_embedding")
	r.RequestKey = "claim:1"
	first := e.RequestPermit(r)
	if err := BeginRequest(db, r); err != nil {
		t.Fatal(err)
	}
	if err := MarkRequestUncertain(db, r, "older attempt unknown"); err != nil {
		t.Fatal(err)
	}
	r.RequestKey = "claim:2"
	next := e.RequestPermit(r)
	if !next.Allowed() {
		t.Fatal(next)
	}
	if err := BeginRequest(db, r); err != nil {
		t.Fatal(err)
	}
	if err := SettleRequest(db, r, 1, "claim 2 verified completion"); err != nil {
		t.Fatal(err)
	}
	var previous models.WorkloadPermit
	if err := db.First(&previous, "id=?", first.PermitID).Error; err != nil {
		t.Fatal(err)
	}
	if previous.State != StateUncertain {
		t.Fatal("newer success falsely settled older uncertain generation")
	}
	reserved, err := reservedUnits(db, []string{"enrichment_svc"}, time.Now().UTC())
	if err != nil || reserved["enrichment_svc"] != 1 {
		t.Fatalf("lost older effect capacity: %+v %v", reserved, err)
	}
}

func TestGovernorDBMissingSchemaDoesNotAbortNativeReceiptTransaction(t *testing.T) {
	db := governorDB(t)
	if err := db.Exec("DROP TABLE workload_decisions").Error; err != nil {
		t.Fatal(err)
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		d := RequestPermit(tx, uniqueRequest("text_embedding"))
		if !d.Allowed() || !d.Unavailable {
			t.Fatalf("missing ledger froze native effect: %+v", d)
		}
		if err := SettleOwner(tx, "tenant-a", "native", "owner", 1, "receipt"); err != nil {
			return err
		}
		// A subsequent native write proves failed optional SQL did not leave the
		// parent's PostgreSQL transaction aborted.
		return tx.Exec("CREATE TEMPORARY TABLE native_receipt_can_commit(id integer)").Error
	})
	if err != nil {
		t.Fatalf("native receipt transaction poisoned: %v", err)
	}
}

func TestGovernorDBRevokedQueuedOwnerCannotStart(t *testing.T) {
	db := governorDB(t)
	e := healthyEngine(t, db, ModeEnforce, "source_pull")
	r := uniqueRequest("source_pull")
	r.OwnerKind = "source_run"
	d := e.RequestPermit(r)
	if !d.Allowed() {
		t.Fatal(d)
	}
	if err := RevokePermit(db, d.PermitID, "admin", "cancel before provider effect"); err != nil {
		t.Fatal(err)
	}
	if err := BeginOwner(db, r.TenantID, r.OwnerKind, r.OwnerID); !errors.Is(err, ErrNotAdmitted) {
		t.Fatalf("revoked queued owner still starts: %v", err)
	}
	if err := BeginOwner(db, r.TenantID, r.OwnerKind, "no-permit-failopen-owner"); err != nil {
		t.Fatalf("absent optional grant incorrectly blocked: %v", err)
	}
}

func TestGovernorDBPolicyReviewQualificationAndEmergencyTTL(t *testing.T) {
	db := governorDB(t)
	_ = healthyEngine(t, db, ModeObserve, "heavy_sweep")
	p, err := GetPolicy(db)
	if err != nil {
		t.Fatal(err)
	}
	u := PolicyUpdate{Revision: p.Revision, Mode: ModeEnforce, EnforcedClasses: []string{"heavy_sweep"}, ResourceLimits: limitsFor(p), SnapshotMaxAgeSeconds: p.SnapshotMaxAgeSeconds, RecoverySeconds: p.RecoverySeconds, LeaseSeconds: p.LeaseSeconds, StarvationMinutes: p.StarvationMinutes, ReviewNote: "reviewed native and delivery evidence", Confirmation: "ENFORCE CAPACITY"}
	q, err := GetQualification(db)
	if err != nil {
		t.Fatal(err)
	}
	u.QualificationDigest = q.Digest
	if _, err = UpdatePolicy(db, u, "admin"); !errors.Is(err, ErrNotQualified) {
		t.Fatalf("unexercised evidence promoted: %v", err)
	}
	paused, err := Pause(db, p.Revision, 5, "admin", "bounded incident bypass")
	if err != nil {
		t.Fatal(err)
	}
	if paused.Mode != ModeObserve || effectiveMode(paused, time.Now()) != ModeOff {
		t.Fatalf("pause overwrote saved mode: %+v", paused)
	}
	if effectiveMode(paused, paused.PausedUntil.Add(time.Second)) != ModeObserve {
		t.Fatal("pause did not expire into prior mode")
	}
	if _, err = Pause(db, p.Revision, 0, "admin", "stale revision"); !errors.Is(err, ErrConflict) {
		t.Fatal("stale operator overwrote newer pause")
	}
	status, err := GetStatus(db)
	if err != nil || status.EffectiveMode != ModeOff || len(status.Resources) != len(resources) || len(status.Coverage) == 0 {
		t.Fatalf("invalid cockpit projection: %+v %v", status, err)
	}
	var audit models.WorkloadDecision
	if err = db.Where("event='policy_paused'").First(&audit).Error; err != nil || audit.Actor != "admin" || !strings.Contains(string(audit.Evidence), "bounded incident bypass") {
		t.Fatal("emergency override not auditable")
	}
}
