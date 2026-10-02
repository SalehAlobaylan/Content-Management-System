package supply

import (
	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
)

func replayPageFixture(t *testing.T) ReplayPageContext {
	t.Helper()
	raw, err := os.ReadFile("../../../contracts/content-reset-page-v1-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Page ReplayPageContext `json:"page"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture.Page
}

func TestContentResetReplayPageCommandBindsOrdinal(t *testing.T) {
	command := contentreset.Command{Contract: contentreset.Contract{Owner: "cms/source-run", Effect: "replay_page", TargetType: "source_branch", Version: "v1"}, TargetID: uuid.NewString(), Parameters: json.RawMessage(`{"ordinal":2}`)}
	if err := validateReplayPageCommand(command, 2); err != nil {
		t.Fatal(err)
	}
	for _, parameters := range []string{`{}`, `{"ordinal":1}`, `{"ordinal":2,"extra":true}`, `{"ordinal":"2"}`, `null`, `invalid`} {
		command.Parameters = json.RawMessage(parameters)
		if validateReplayPageCommand(command, 2) == nil {
			t.Fatalf("unbound page accepted: %s", parameters)
		}
	}
	command.Parameters = json.RawMessage(`{"ordinal":2}`)
	command.TargetID = uuid.Nil.String()
	if validateReplayPageCommand(command, 2) == nil {
		t.Fatal("nil replay branch accepted")
	}
}

func TestContentResetReplayCheckpointIntegrity(t *testing.T) {
	page := models.ContentResetReplayPage{PublicID: uuid.New(), TenantID: "tenant", InputCursor: "previous"}
	fixture := models.ContentResetReplayCheckpoint{PublicID: uuid.New(), TenantID: page.TenantID, PageID: page.PublicID, ProviderReceiptID: uuid.New(), NextCursor: "next", NextCursorHash: contentreset.Hash("next"), Observed: 2, Admitted: 1, OutsideWindow: 1, ObservedBytes: 10}
	if err := validateReplayCheckpoint(fixture, page); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*models.ContentResetReplayCheckpoint){
		"wrong tenant":    func(c *models.ContentResetReplayCheckpoint) { c.TenantID = "other" },
		"wrong page":      func(c *models.ContentResetReplayCheckpoint) { c.PageID = uuid.New() },
		"missing receipt": func(c *models.ContentResetReplayCheckpoint) { c.ProviderReceiptID = uuid.Nil },
		"cursor hash":     func(c *models.ContentResetReplayCheckpoint) { c.NextCursorHash = contentreset.Hash("other") },
		"repeated cursor": func(c *models.ContentResetReplayCheckpoint) {
			c.NextCursor = page.InputCursor
			c.NextCursorHash = contentreset.Hash(c.NextCursor)
		},
		"false exhaustion": func(c *models.ContentResetReplayCheckpoint) { c.Exhausted = true },
		"item coverage":    func(c *models.ContentResetReplayCheckpoint) { c.Observed++ },
		"bytes":            func(c *models.ContentResetReplayCheckpoint) { c.ObservedBytes = ReplayMaxPageBytes + 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			checkpoint := fixture
			mutate(&checkpoint)
			if validateReplayCheckpoint(checkpoint, page) == nil {
				t.Fatal("invalid checkpoint accepted")
			}
		})
	}
}

func TestContentResetReplayPartialOutputFailsNativeUnit(t *testing.T) {
	for _, event := range []ReceiptEvent{ReceiptEventProviderTerminal, ReceiptEventNormalizeTerminal, ReceiptEventFinalization} {
		if terminalUnitStateForRequest(event, OutcomePartial, "content_reset_replay") != UnitFailed {
			t.Fatalf("partial replay %s reports success", event)
		}
		if terminalUnitStateForRequest(event, OutcomePartial, "baseline") != UnitSucceeded {
			t.Fatalf("ordinary partial intake behavior changed: %s", event)
		}
		if terminalUnitStateForRequest(event, OutcomeUnknown, "content_reset_replay") != UnitVerificationRequired {
			t.Fatal("unknown replay evidence must remain uncertain")
		}
	}
}

func TestContentResetReplaySharedContract(t *testing.T) {
	page := replayPageFixture(t)
	if err := page.Validate(); err != nil {
		t.Fatal(err)
	}
	if contentreset.Hash(page.Spec) != page.SpecHash {
		t.Fatal("CMS and Aggregation disagree on spec identity")
	}
}

func TestContentResetReplayRejectsWideningAndUnsupportedCapabilities(t *testing.T) {
	cases := map[string]func(*ReplayPageContext){
		"altered budget": func(p *ReplayPageContext) { p.Spec.MaxItems++ },
		"unsupported provider": func(p *ReplayPageContext) {
			p.Spec.SourceType = "YOUTUBE"
			p.Spec.ProviderContract = "YOUTUBE:reset-page/v1"
			p.SpecHash = contentreset.Hash(p.Spec)
		},
		"from-now needs boundary proof": func(p *ReplayPageContext) { p.Spec.Mode = "from_now"; p.SpecHash = contentreset.Hash(p.Spec) },
		"continuation without cursor":   func(p *ReplayPageContext) { p.Ordinal = 2 },
		"initial with cursor":           func(p *ReplayPageContext) { p.InputCursor = "opaque" },
		"page cap":                      func(p *ReplayPageContext) { p.Ordinal = 101; p.InputCursor = "opaque" },
		"oversized cursor":              func(p *ReplayPageContext) { p.Ordinal = 2; p.InputCursor = string(make([]byte, 4097)) },
		"reversed interval": func(p *ReplayPageContext) {
			end := p.Spec.WindowStart.Add(-1)
			p.Spec.WindowEnd = end
			p.SpecHash = contentreset.Hash(p.Spec)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			page := replayPageFixture(t)
			mutate(&page)
			if page.Validate() == nil {
				t.Fatal("invalid replay page accepted")
			}
		})
	}
}
