package contentreset

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func testCommand() Command {
	return Command{CommandID: uuid.New(), CampaignID: uuid.New(), RevisionID: uuid.New(), TenantID: "tenant", ManifestHash: Hash("manifest"), Contract: Contract{"cms/test", "effect", "campaign", "v1"}, TargetID: "target", Parameters: json.RawMessage(`{}`)}
}

func TestOwnerObservationRequiresBoundObjectEvidence(t *testing.T) {
	command := testCommand()
	valid := Observation{CommandID: command.CommandID, CommandHash: Hash(command), State: "waiting", Evidence: json.RawMessage(`{"admitted":true}`)}
	if !validObservation(command, valid) {
		t.Fatal("bound waiting observation rejected")
	}
	for _, raw := range []string{"null", "[]", "true", "invalid"} {
		bad := valid
		bad.Evidence = json.RawMessage(raw)
		if validObservation(command, bad) {
			t.Fatalf("accepted non-object evidence %s", raw)
		}
	}
	valid.State = "deferred"
	if validObservation(command, valid) {
		t.Fatal("deferral without proof of no admission accepted")
	}
	valid.NoEffectProven = true
	if !validObservation(command, valid) {
		t.Fatal("proven deferral rejected")
	}
	valid.State = "waiting"
	if validObservation(command, valid) {
		t.Fatal("admitted wait cannot claim no effect")
	}
}

func TestRetryRequiresExactFailedReceiptAndNoEffectProof(t *testing.T) {
	command := testCommand()
	observation := Observation{CommandID: command.CommandID, CommandHash: Hash(command), State: "failed", Evidence: json.RawMessage(`{"admitted":false}`), NoEffectProven: true}
	commandRaw, _ := json.Marshal(command)
	receipt, _ := json.Marshal(observation)
	step := models.ContentResetStep{PublicID: command.CommandID, TenantID: command.TenantID, Owner: command.Contract.Owner, EffectType: command.Contract.Effect, TargetType: command.Contract.TargetType, TargetID: command.TargetID, State: "failed", Command: commandRaw, Receipt: receipt}
	if !Retryable(step) {
		t.Fatal("proven SQL absence is not retryable")
	}
	for _, state := range []string{"pending", "claimed", "waiting", "deferred", "outcome_unknown", "blocked", "succeeded", "cancelled"} {
		bad := step
		bad.State = state
		if Retryable(bad) {
			t.Fatalf("accepted retry of %s", state)
		}
	}
	token := uuid.New()
	leased := step
	leased.LeaseToken = &token
	if Retryable(leased) {
		t.Fatal("accepted retry with an outstanding lease")
	}
	observation.NoEffectProven = false
	step.Receipt, _ = json.Marshal(observation)
	if Retryable(step) {
		t.Fatal("accepted admitted failure")
	}
	observation.NoEffectProven = true
	observation.CommandID = uuid.New()
	step.Receipt, _ = json.Marshal(observation)
	if Retryable(step) {
		t.Fatal("accepted another command's failure proof")
	}
}

func TestProgressWaitAndDeferralDoNotPauseCampaign(t *testing.T) {
	for _, state := range []string{"waiting", "deferred"} {
		t.Run(state, func(t *testing.T) {
			connection, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: connection}), &gorm.Config{SkipDefaultTransaction: true, Logger: logger.Default.LogMode(logger.Silent)})
			if err != nil {
				t.Fatal(err)
			}
			command := testCommand()
			token := uuid.New()
			until := time.Now().Add(time.Minute)
			work := claim{Step: models.ContentResetStep{ID: 3, CampaignID: 1, RevisionID: 2, LeaseToken: &token}, Command: command}
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT .* FROM "content_reset_campaigns"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "state"}).AddRow(1, command.CampaignID, command.TenantID, "executing"))
			mock.ExpectQuery(`SELECT .* FROM "content_reset_executions"`).WillReturnRows(sqlmock.NewRows([]string{"id", "campaign_id", "revision_id"}).AddRow(4, 1, 2))
			mock.ExpectQuery(`SELECT .* FROM "content_reset_steps"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "campaign_id", "revision_id", "state", "lease_token", "lease_until", "owner"}).AddRow(3, command.CommandID, command.TenantID, 1, 2, "claimed", token, until, command.Contract.Owner))
			mock.ExpectExec(`UPDATE "content_reset_steps" SET`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(`INSERT INTO "content_reset_evidence"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(5, uuid.New()))
			// Any UPDATE to the campaign/run would fail this exact SQL sequence.
			mock.ExpectCommit()
			observation := Observation{CommandID: command.CommandID, CommandHash: Hash(command), State: state, Evidence: json.RawMessage(`{"verified":true}`), NoEffectProven: state == "deferred"}
			if err := (&Engine{}).finish(db, work, observation); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOwnerPauseFenceDefersWithoutUncertainty(t *testing.T) {
	command := testCommand()
	work := claim{Command: command, Step: models.ContentResetStep{State: "claimed"}}
	paused := normalizeOwnerResult(work, Observation{}, ErrPaused)
	if paused.State != "deferred" || !paused.NoEffectProven || paused.ReasonCode != "execution_paused" {
		t.Fatalf("pause fence did not defer exactly once: %+v", paused)
	}
	if !validObservation(command, paused) {
		t.Fatal("pause deferral is not a valid observation")
	}
	uncertain := normalizeOwnerResult(work, Observation{}, errors.New("storage timeout"))
	if uncertain.State != "outcome_unknown" {
		t.Fatalf("owner failure must stay uncertain, got %s", uncertain.State)
	}
	valid := Observation{CommandID: command.CommandID, CommandHash: Hash(command), State: "succeeded", Evidence: json.RawMessage(`{"done":true}`)}
	if got := normalizeOwnerResult(work, valid, nil); got.State != "succeeded" {
		t.Fatalf("valid observation changed: %+v", got)
	}
}

func TestFinishOnTerminalCampaignPersistsReceiptOnly(t *testing.T) {
	for _, campaignState := range []string{"complete", "closed_partial"} {
		t.Run(campaignState, func(t *testing.T) {
			connection, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: connection}), &gorm.Config{SkipDefaultTransaction: true, Logger: logger.Default.LogMode(logger.Silent)})
			if err != nil {
				t.Fatal(err)
			}
			command := testCommand()
			token := uuid.New()
			until := time.Now().Add(time.Minute)
			work := claim{Step: models.ContentResetStep{ID: 3, CampaignID: 1, RevisionID: 2, LeaseToken: &token}, Command: command, Reconcile: true}
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT .* FROM "content_reset_campaigns"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "state"}).AddRow(1, command.CampaignID, command.TenantID, campaignState))
			mock.ExpectQuery(`SELECT .* FROM "content_reset_executions"`).WillReturnRows(sqlmock.NewRows([]string{"id", "campaign_id", "revision_id"}).AddRow(4, 1, 2))
			mock.ExpectQuery(`SELECT .* FROM "content_reset_steps"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "campaign_id", "revision_id", "state", "lease_token", "lease_until", "owner"}).AddRow(3, command.CommandID, command.TenantID, 1, 2, "outcome_unknown", token, until, command.Contract.Owner))
			mock.ExpectExec(`UPDATE "content_reset_steps" SET`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(`INSERT INTO "content_reset_evidence"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(5, uuid.New()))
			// A terminal campaign must never be reopened, paused or moved back to partial.
			mock.ExpectCommit()
			observation := Observation{CommandID: command.CommandID, CommandHash: Hash(command), State: "failed", Evidence: json.RawMessage(`{"completion":"lost"}`)}
			if err := (&Engine{}).finish(db, work, observation); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type readbackSpy struct{ executions, readbacks int }

func (s *readbackSpy) Contract() Contract { return Contract{"cms/test", "effect", "campaign", "v1"} }
func (s *readbackSpy) Execute(context.Context, *gorm.DB, Command, uuid.UUID) (Observation, error) {
	s.executions++
	return Observation{}, nil
}
func (s *readbackSpy) Reconcile(context.Context, *gorm.DB, Command) (Observation, error) {
	s.readbacks++
	return Observation{}, nil
}

func TestAdmittedAndUnknownClaimsOnlyInvokeReadback(t *testing.T) {
	for _, state := range []string{"waiting", "outcome_unknown"} {
		t.Run(state, func(t *testing.T) {
			spy := &readbackSpy{}
			_, err := invokeOwner(context.Background(), nil, spy, claim{Command: testCommand(), Reconcile: true, Step: models.ContentResetStep{State: state}})
			if err != nil || spy.readbacks != 1 || spy.executions != 0 {
				t.Fatalf("readbacks=%d executions=%d err=%v", spy.readbacks, spy.executions, err)
			}
		})
	}
}
