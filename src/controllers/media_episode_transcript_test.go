package controllers

import (
	"content-management-system/src/models"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestEpisodeTranscriptQueryUsesContentOwnership(t *testing.T) {
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		t.Run(tenant, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			transcriptID := uuid.New()
			parent := models.ContentItem{PublicID: uuid.New(), TenantID: tenant, TranscriptID: &transcriptID}
			var transcript models.Transcript
			query := episodeTranscriptQuery(db.Session(&gorm.Session{DryRun: true}), parent).
				Select("public_id", "source", "provider", "approved_at", "approved_by").First(&transcript)
			sql := query.Statement.SQL.String()
			for _, required := range []string{"transcripts.public_id=$1", "transcripts.content_item_id=$2", "owner.public_id=transcripts.content_item_id", "owner.tenant_id=$3", "owner.transcript_id=transcripts.public_id"} {
				if !strings.Contains(sql, required) {
					t.Fatalf("missing ownership predicate %q: %s", required, sql)
				}
			}
			if strings.Contains(sql, "transcripts.tenant_id") {
				t.Fatal("transcripts has no tenant_id column")
			}
			rows := sqlmock.NewRows([]string{"public_id", "source", "provider", "approved_at", "approved_by"})
			if tenant == "tenant-a" {
				rows.AddRow(transcriptID, "youtube_human", nil, nil, nil)
			}
			mock.ExpectQuery(regexp.QuoteMeta(sql)).WithArgs(transcriptID, parent.PublicID, tenant, 1).WillReturnRows(rows)
			err = episodeTranscriptQuery(db, parent).Select("public_id", "source", "provider", "approved_at", "approved_by").First(&transcript).Error
			if tenant == "tenant-a" {
				if err != nil || transcript.Source == nil || *transcript.Source != "youtube_human" {
					t.Fatalf("caption provenance unavailable: %+v %v", transcript, err)
				}
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				t.Fatalf("foreign owner should have no transcript: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDraftInputsUsesOwnedTranscriptWithoutTenantColumn(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	transcriptID := uuid.New()
	duration := 3103
	parent := models.ContentItem{PublicID: uuid.New(), TenantID: "tenant-a", TranscriptID: &transcriptID, DurationSec: &duration}
	mock.ExpectQuery(`SELECT .*transcripts.*transcripts.public_id=\$1 AND transcripts.content_item_id=\$2.*EXISTS.*owner.tenant_id=\$3 AND owner.transcript_id=transcripts.public_id`).WithArgs(transcriptID, parent.PublicID, parent.TenantID, 1).WillReturnRows(sqlmock.NewRows([]string{"public_id", "content_item_id", "segments"}).AddRow(transcriptID, parent.PublicID, `[]`))
	_, _, _, err = draftInputs(db, parent)
	if err == nil || err.Error() != "Timestamped transcript required" {
		t.Fatalf("expected input validation after successful transcript read: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
