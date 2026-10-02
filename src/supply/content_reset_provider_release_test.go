package supply

import (
	"content-management-system/src/contentreset"
	"sync"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm/schema"
)

func providerGraphFixture(t *testing.T) (models.SourceRunRequest, models.SourceRunAttempt, []models.SourceRunExecutionUnit) {
	t.Helper()
	requestID, attemptID, sourceID, rootID, pageID, batchID, fence := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	request := models.SourceRunRequest{PublicID: requestID, TenantID: "tenant", ContentSourceID: sourceID, Purpose: "content_reset_replay", State: string(RequestVerificationRequired), ManifestState: string(ManifestSealed), ExpectedPageCount: 1, ExpectedBatchCount: 1, ExpectedUnitCount: 3, RootExecutionUnitID: &rootID}
	attempt := models.SourceRunAttempt{PublicID: attemptID, TenantID: "tenant", ContentSourceID: sourceID, SourceRunRequestID: requestID, State: string(AttemptVerificationRequired), RootExecutionUnitID: &rootID, FenceToken: fence}
	digest, err := ManifestChildDigest([]string{"normalize:initial:batch-0"})
	if err != nil {
		t.Fatal(err)
	}
	unit := func(id uuid.UUID, kind string) models.SourceRunExecutionUnit {
		return models.SourceRunExecutionUnit{PublicID: id, TenantID: "tenant", ContentSourceID: sourceID, SourceRunRequestID: requestID, SourceRunAttemptID: attemptID, AttemptFenceToken: fence, UnitType: kind, State: string(UnitSucceeded), TerminalOutcome: string(OutcomeNewItems)}
	}
	root, page, batch := unit(rootID, "coordinator"), unit(pageID, "fetch_page"), unit(batchID, "normalize_batch")
	root.State = string(UnitVerificationRequired)
	root.TerminalOutcome = ""
	page.ParentUnitID = &rootID
	page.PageID = "initial"
	page.UnitKey = "fetch:initial"
	page.DeclaredChildCount = 1
	page.DeclaredChildDigest = digest
	batch.ParentUnitID = &pageID
	batch.PageID = "initial"
	batch.UnitKey = "normalize:initial:batch-0"
	return request, attempt, []models.SourceRunExecutionUnit{root, page, batch}
}

func TestContentResetProviderReleaseRetryRequiresMatchingStamps(t *testing.T) {
	request, attempt, _ := providerGraphFixture(t)
	command := contentreset.Command{TenantID: request.TenantID, CommandID: uuid.New()}
	page := models.ContentResetReplayPage{TenantID: request.TenantID, PublicID: uuid.New(), SourceRunRequestID: request.PublicID}
	now := time.Now().UTC().Truncate(time.Microsecond)
	request.ProviderEffectsReleasedAt, attempt.ProviderEffectsReleasedAt = &now, &now
	proof := datatypes.JSON(`{"provider_effects_settled":true}`)
	fixture := models.ContentResetProviderRelease{PublicID: uuid.New(), TenantID: request.TenantID, PageID: page.PublicID, SourceRunRequestID: request.PublicID, SourceRunAttemptID: attempt.PublicID, CommandID: command.CommandID, Proof: proof, ProofHash: contentreset.Hash(proof), ReleasedAt: now}
	if err := validateProviderReleaseReadback(fixture, command, page, request, attempt); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*models.ContentResetProviderRelease, *models.SourceRunRequest, *models.SourceRunAttempt){
		"missing request stamp": func(_ *models.ContentResetProviderRelease, r *models.SourceRunRequest, _ *models.SourceRunAttempt) {
			r.ProviderEffectsReleasedAt = nil
		},
		"missing attempt stamp": func(_ *models.ContentResetProviderRelease, _ *models.SourceRunRequest, a *models.SourceRunAttempt) {
			a.ProviderEffectsReleasedAt = nil
		},
		"stamp mismatch": func(_ *models.ContentResetProviderRelease, _ *models.SourceRunRequest, a *models.SourceRunAttempt) {
			later := now.Add(time.Microsecond)
			a.ProviderEffectsReleasedAt = &later
		},
		"wrong command": func(p *models.ContentResetProviderRelease, _ *models.SourceRunRequest, _ *models.SourceRunAttempt) {
			p.CommandID = uuid.New()
		},
		"wrong page": func(p *models.ContentResetProviderRelease, _ *models.SourceRunRequest, _ *models.SourceRunAttempt) {
			p.PageID = uuid.New()
		},
		"wrong tenant": func(p *models.ContentResetProviderRelease, _ *models.SourceRunRequest, _ *models.SourceRunAttempt) {
			p.TenantID = "other"
		},
		"changed proof": func(p *models.ContentResetProviderRelease, _ *models.SourceRunRequest, _ *models.SourceRunAttempt) {
			p.Proof = datatypes.JSON(`{}`)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p, r, a := fixture, request, attempt
			mutate(&p, &r, &a)
			if validateProviderReleaseReadback(p, command, page, r, a) == nil {
				t.Fatal("unproven release retry accepted")
			}
		})
	}
}

func TestContentResetProviderReleaseRequiresClosedGraph(t *testing.T) {
	r, a, u := providerGraphFixture(t)
	terminal, err := validateReplayProviderGraph(r, a, u)
	if err != nil || terminal {
		t.Fatalf("valid pending consumer graph rejected: %v", err)
	}
	cases := map[string]func(*models.SourceRunRequest, *models.SourceRunAttempt, []models.SourceRunExecutionUnit){
		"ordinary run": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			r.Purpose = "baseline"
		},
		"open manifest": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			r.ManifestState = string(ManifestOpen)
		},
		"running normalize": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			u[2].State = string(UnitRunning)
		},
		"unknown normalize": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			u[2].State = string(UnitVerificationRequired)
		},
		"partial normalize": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			u[2].TerminalOutcome = string(OutcomePartial)
		},
		"failed provider": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			u[1].State = string(UnitFailed)
		},
		"wrong fence": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			u[2].AttemptFenceToken = uuid.New()
		},
		"wrong parent": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			u[2].ParentUnitID = a.RootExecutionUnitID
		},
		"missing coverage": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			r.ExpectedUnitCount++
		},
		"wrong child digest": func(r *models.SourceRunRequest, a *models.SourceRunAttempt, u []models.SourceRunExecutionUnit) {
			u[1].DeclaredChildDigest = string(make([]byte, 64))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r, a, u := providerGraphFixture(t)
			mutate(&r, &a, u)
			if _, err := validateReplayProviderGraph(r, a, u); err == nil {
				t.Fatal("unsettled or mismatched graph released")
			}
		})
	}
}

func TestContentResetProviderReleaseRecognizesNativeVerifiedTerminal(t *testing.T) {
	r, a, u := providerGraphFixture(t)
	now := time.Now().UTC()
	r.State = string(RequestSucceeded)
	r.VerifiedAt = &now
	a.State = string(AttemptSucceeded)
	u[0].State = string(UnitSucceeded)
	terminal, err := validateReplayProviderGraph(r, a, u)
	if err != nil || !terminal {
		t.Fatalf("native terminal proof rejected: %v", err)
	}
	r.VerifiedAt = nil
	if _, err := validateReplayProviderGraph(r, a, u); err == nil {
		t.Fatal("unverified terminal request accepted")
	}
}

func TestContentResetReleaseStampsAreExcludedFromGenericWrites(t *testing.T) {
	// The new columns are read-only in model schemas so deploying code before
	// the explicit migration does not add absent columns to ordinary INSERTs.
	for _, model := range []any{&models.SourceRunRequest{}, &models.SourceRunAttempt{}} {
		s, err := schema.Parse(model, &sync.Map{}, schema.NamingStrategy{})
		if err != nil {
			t.Fatal(err)
		}
		field := s.LookUpField("ProviderEffectsReleasedAt")
		if field == nil || !field.Readable || field.Creatable || field.Updatable {
			t.Fatal("provider release stamp permits a generic ORM write")
		}
	}
}
