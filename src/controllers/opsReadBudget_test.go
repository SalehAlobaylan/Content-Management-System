package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestOpsReadDeadlineKeepsSlowMemberVisibleAndNeverGreen(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	members := []opsFleetMember{{Key: "probe", Label: "Slow owner", Kind: "engine", Lanes: []opsFleetLane{{Key: "observation", StatusSQL: "SELECT tenant_id,enabled,mode,paused_until,last_run_at,interval_minutes,created_at FROM slow_owner_policies", Scheduled: true, PauseColumn: "paused_until"}}}}
	mock.ExpectQuery("SELECT p\\.\\* FROM \\(SELECT tenant_id").WillDelayFor(3 * time.Second).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "enabled", "mode", "paused_until", "last_run_at", "interval_minutes", "created_at"}))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	s := collectOpsSnapshotMembers(db.WithContext(ctx), time.Now().UTC(), true, members)
	if time.Since(start) > time.Second {
		t.Fatal("read did not honor caller deadline")
	}
	if s.Complete || len(s.Errors) == 0 || len(s.Fleet) != 1 || s.Fleet[0].MemberKey != "probe" || s.Fleet[0].State != "errored" || !s.Fleet[0].Pausable {
		t.Fatalf("slow owner disappeared or became healthy: %+v", s)
	}
	if headline, _ := opsHeadline(s.Attention, s.Fleet); headline != "attention" {
		t.Fatalf("slow failed adapter produced %s", headline)
	}
	if err := mock.ExpectationsWereMet(); err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
}

func TestOpsReadAttentionBatchKeepsKindsAndNativeUTCTimestamps(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	rows := sqlmock.NewRows([]string{"kind", "body"}).AddRow("episode", []byte(`{"tenant_id":"t","native_id":"native","severity":"degraded","title":"Native failure","detail":"ios / pods","client_platform":"ios","count":1,"first_seen":"2026-10-29T12:00:00.123456"}`)).AddRow("approval", []byte(`{"tenant_id":"t","native_id":"approval","severity":"minor","count":2,"first_seen":"2026-10-29T15:00:00.123456+03:00"}`))
	mock.ExpectQuery(`SELECT \* FROM \(\(SELECT`).WithArgs("episode", "approval").WillReturnRows(rows)
	member := opsFleetMember{Key: "experience", CockpitPath: "/platform/real-experience"}
	items, err := opsAttentionQueries(db, member, []opsAttentionQuerySpec{{Kind: "episode", SQL: "SELECT 1"}, {Kind: "approval", SQL: "SELECT 2"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Kind != "episode" || items[0].Severity != "major" || items[0].Href != "/platform/real-experience?client=ios" {
		t.Fatalf("native normalization lost in batch: %+v", items)
	}
	if !items[0].FirstSeen.Equal(items[1].FirstSeen) || items[0].TenantID != "t" || items[1].Count != 2 {
		t.Fatalf("batch changed source identity/time: %+v", items)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
