// Package supply contains the protocol contracts for the CMS-owned source-run
// lifecycle. It intentionally has no HTTP, queue, or provider dependency, so
// its transition and identity rules can be shared by controllers and workers
// without importing an execution owner.
package supply

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

const ContractVersion = "source-run/v1"

type RequestState string
type AttemptState string
type ExecutionUnitState string
type ManifestState string
type ReceiptStage string
type ReceiptEvent string
type SourceRunOutcome string
type VerificationVerdict string

const (
	RequestRequested            RequestState = "requested"
	RequestAccepted             RequestState = "accepted"
	RequestRunning              RequestState = "running"
	RequestVerificationRequired RequestState = "verification_required"
	RequestCompleted            RequestState = "completed" // legacy terminal read state
	RequestSucceeded            RequestState = "succeeded"
	RequestPartial              RequestState = "partial"
	RequestBlocked              RequestState = "blocked"
	RequestFailed               RequestState = "failed"
	RequestCancelled            RequestState = "cancelled"
	RequestExpired              RequestState = "expired"

	AttemptAuthorized           AttemptState = "authorized"
	AttemptClaimed              AttemptState = "claimed"
	AttemptRunning              AttemptState = "running"
	AttemptVerificationRequired AttemptState = "verification_required"
	AttemptSucceeded            AttemptState = "succeeded"
	AttemptPartial              AttemptState = "partial"
	AttemptBlocked              AttemptState = "blocked"
	AttemptFailed               AttemptState = "failed"
	AttemptCancelled            AttemptState = "cancelled"
	AttemptExpired              AttemptState = "expired"

	UnitAuthorized           ExecutionUnitState = "authorized"
	UnitAccepted             ExecutionUnitState = "accepted"
	UnitRunning              ExecutionUnitState = "running"
	UnitVerificationRequired ExecutionUnitState = "verification_required"
	UnitSucceeded            ExecutionUnitState = "succeeded"
	UnitFailed               ExecutionUnitState = "failed"
	UnitCancelled            ExecutionUnitState = "cancelled"
	UnitExpired              ExecutionUnitState = "expired"

	ManifestOpen    ManifestState = "open"
	ManifestSealing ManifestState = "sealing"
	ManifestSealed  ManifestState = "sealed"

	ReceiptStageDispatch  ReceiptStage = "dispatch"
	ReceiptStageFetch     ReceiptStage = "fetch"
	ReceiptStageNormalize ReceiptStage = "normalize"
	ReceiptStageDelivery  ReceiptStage = "delivery"

	ReceiptEventAccepted               ReceiptEvent = "accepted"
	ReceiptEventExecutionStarted       ReceiptEvent = "execution_started"
	ReceiptEventProviderRequestStarted ReceiptEvent = "provider_request_started"
	ReceiptEventProviderPage           ReceiptEvent = "provider_page"
	ReceiptEventProviderTerminal       ReceiptEvent = "provider_terminal"
	ReceiptEventNormalizeScheduled     ReceiptEvent = "normalize_scheduled"
	ReceiptEventNormalizeTerminal      ReceiptEvent = "normalize_terminal"
	ReceiptEventFinalization           ReceiptEvent = "finalization"
	ReceiptEventFailed                 ReceiptEvent = "failed"
	ReceiptEventCancelled              ReceiptEvent = "cancelled"
	ReceiptEventDLQ                    ReceiptEvent = "dlq"

	OutcomeNewItems                   SourceRunOutcome = "new_items"
	OutcomeNoChange                   SourceRunOutcome = "no_change"
	OutcomeUpstreamChangeDeferred     SourceRunOutcome = "upstream_change_deferred"
	OutcomeObservationBlockedByIntake SourceRunOutcome = "observation_blocked_by_intake"
	OutcomeConfigurationBlocked       SourceRunOutcome = "configuration_blocked"
	OutcomePartial                    SourceRunOutcome = "partial"
	OutcomeProviderFailed             SourceRunOutcome = "provider_failed"
	OutcomeCancelled                  SourceRunOutcome = "cancelled"
	OutcomeDeadLettered               SourceRunOutcome = "dead_lettered"
	OutcomeUnknown                    SourceRunOutcome = "unknown"

	VerdictPresent VerificationVerdict = "present"
	VerdictAbsent  VerificationVerdict = "absent"
	VerdictUnknown VerificationVerdict = "unknown"
)

var requestTransitions = map[RequestState]map[RequestState]bool{
	RequestRequested:            {RequestAccepted: true, RequestRunning: true, RequestBlocked: true, RequestFailed: true, RequestCancelled: true, RequestExpired: true},
	RequestAccepted:             {RequestRunning: true, RequestVerificationRequired: true, RequestBlocked: true, RequestFailed: true, RequestCancelled: true, RequestExpired: true},
	RequestRunning:              {RequestVerificationRequired: true, RequestSucceeded: true, RequestPartial: true, RequestBlocked: true, RequestFailed: true, RequestCancelled: true, RequestExpired: true},
	RequestVerificationRequired: {RequestSucceeded: true, RequestPartial: true, RequestBlocked: true, RequestFailed: true, RequestCancelled: true, RequestExpired: true},
}

var attemptTransitions = map[AttemptState]map[AttemptState]bool{
	AttemptAuthorized:           {AttemptClaimed: true, AttemptCancelled: true, AttemptExpired: true},
	AttemptClaimed:              {AttemptRunning: true, AttemptVerificationRequired: true, AttemptCancelled: true, AttemptExpired: true},
	AttemptRunning:              {AttemptVerificationRequired: true, AttemptSucceeded: true, AttemptPartial: true, AttemptBlocked: true, AttemptFailed: true, AttemptCancelled: true, AttemptExpired: true},
	AttemptVerificationRequired: {AttemptSucceeded: true, AttemptPartial: true, AttemptBlocked: true, AttemptFailed: true, AttemptCancelled: true, AttemptExpired: true},
}

var unitTransitions = map[ExecutionUnitState]map[ExecutionUnitState]bool{
	UnitAuthorized:           {UnitAccepted: true, UnitCancelled: true, UnitExpired: true},
	UnitAccepted:             {UnitRunning: true, UnitCancelled: true, UnitExpired: true},
	UnitRunning:              {UnitVerificationRequired: true, UnitSucceeded: true, UnitFailed: true, UnitCancelled: true, UnitExpired: true},
	UnitVerificationRequired: {UnitSucceeded: true, UnitFailed: true, UnitCancelled: true, UnitExpired: true},
}

var manifestTransitions = map[ManifestState]map[ManifestState]bool{
	ManifestOpen:    {ManifestSealing: true},
	ManifestSealing: {ManifestSealed: true},
}

var terminalOutcomeCategories = map[SourceRunOutcome]RequestState{
	OutcomeNewItems:                   RequestSucceeded,
	OutcomeNoChange:                   RequestSucceeded,
	OutcomeUpstreamChangeDeferred:     RequestSucceeded,
	OutcomeObservationBlockedByIntake: RequestBlocked,
	OutcomeConfigurationBlocked:       RequestBlocked,
	OutcomePartial:                    RequestPartial,
	OutcomeProviderFailed:             RequestFailed,
	OutcomeCancelled:                  RequestCancelled,
	OutcomeDeadLettered:               RequestFailed,
	OutcomeUnknown:                    RequestVerificationRequired,
}

// receiptCapabilities is the allowlist for producer evidence. Only entries
// with an independent tri-state verifier are eligible for later Safe Auto;
// unknown is a first-class verdict rather than an inferred absence.
var receiptCapabilities = map[ReceiptStage]map[ReceiptEvent]bool{
	ReceiptStageDispatch: {
		ReceiptEventAccepted: true, ReceiptEventFailed: true, ReceiptEventCancelled: true,
	},
	ReceiptStageFetch: {
		ReceiptEventExecutionStarted: true, ReceiptEventProviderRequestStarted: true, ReceiptEventProviderPage: true,
		ReceiptEventProviderTerminal: true, ReceiptEventFailed: true, ReceiptEventCancelled: true, ReceiptEventDLQ: true,
	},
	ReceiptStageNormalize: {
		ReceiptEventNormalizeScheduled: true, ReceiptEventNormalizeTerminal: true, ReceiptEventFailed: true,
		ReceiptEventCancelled: true, ReceiptEventDLQ: true,
	},
	ReceiptStageDelivery: {
		ReceiptEventFinalization: true, ReceiptEventFailed: true,
	},
}

func CanTransitionRequest(from, to RequestState) bool    { return requestTransitions[from][to] }
func CanTransitionAttempt(from, to AttemptState) bool    { return attemptTransitions[from][to] }
func CanTransitionUnit(from, to ExecutionUnitState) bool { return unitTransitions[from][to] }
func CanTransitionManifest(from, to ManifestState) bool  { return manifestTransitions[from][to] }

func CanAuthorizeManifestChild(state ManifestState) bool { return state == ManifestOpen }

func IsAllowedReceipt(stage ReceiptStage, event ReceiptEvent) bool {
	return receiptCapabilities[stage][event]
}

func TerminalCategoryForOutcome(outcome SourceRunOutcome) (RequestState, bool) {
	category, ok := terminalOutcomeCategories[outcome]
	return category, ok
}

func IsKnownOutcome(outcome SourceRunOutcome) bool {
	_, ok := terminalOutcomeCategories[outcome]
	return ok
}

func ValidateRequestTransition(from, to RequestState) error {
	if !CanTransitionRequest(from, to) {
		return fmt.Errorf("source-run request transition %q -> %q is not permitted", from, to)
	}
	return nil
}

func ValidateAttemptTransition(from, to AttemptState) error {
	if !CanTransitionAttempt(from, to) {
		return fmt.Errorf("source-run attempt transition %q -> %q is not permitted", from, to)
	}
	return nil
}

func ValidateUnitTransition(from, to ExecutionUnitState) error {
	if !CanTransitionUnit(from, to) {
		return fmt.Errorf("source-run execution-unit transition %q -> %q is not permitted", from, to)
	}
	return nil
}

func IsTerminalRequest(state RequestState) bool {
	return state == RequestCompleted || state == RequestSucceeded || state == RequestPartial || state == RequestBlocked || state == RequestFailed || state == RequestCancelled || state == RequestExpired
}

func IsTerminalAttempt(state AttemptState) bool {
	return state == AttemptSucceeded || state == AttemptPartial || state == AttemptBlocked || state == AttemptFailed || state == AttemptCancelled || state == AttemptExpired
}

func IsTerminalUnit(state ExecutionUnitState) bool {
	return state == UnitSucceeded || state == UnitFailed || state == UnitCancelled || state == UnitExpired
}

// RequestIdentity contains only server-derived, stable values. It deliberately
// has no provider URL or arbitrary browser payload, preventing duplicate work
// from being keyed by caller-controlled formatting.
type RequestIdentity struct {
	TenantID            string
	ContentSourceID     string
	Lane                string
	Purpose             string
	CadenceWindowStart  time.Time
	SourceConfigVersion int64
	PolicyFingerprint   string
	ArgumentFingerprint string
	PlanStepToolTarget  string
}

func (i RequestIdentity) Validate() error {
	if strings.TrimSpace(i.TenantID) == "" || strings.TrimSpace(i.ContentSourceID) == "" || strings.TrimSpace(i.Lane) == "" || strings.TrimSpace(i.Purpose) == "" {
		return fmt.Errorf("tenant, source, lane, and purpose are required for a source-run identity")
	}
	if i.SourceConfigVersion < 1 {
		return fmt.Errorf("source config version must be positive")
	}
	if strings.TrimSpace(i.PolicyFingerprint) == "" || strings.TrimSpace(i.ArgumentFingerprint) == "" {
		return fmt.Errorf("policy and argument fingerprints are required")
	}
	switch strings.TrimSpace(i.Purpose) {
	case "baseline", "exploration", "deferred_drain", "circulation", "operator_run_once", "manual", "missed_admission_repair", "partial_repair":
	default:
		return fmt.Errorf("source-run purpose is not registered")
	}
	return nil
}

func (i RequestIdentity) IdempotencyKey() (string, error) {
	if err := i.Validate(); err != nil {
		return "", err
	}
	canonical := strings.Join([]string{
		ContractVersion,
		strings.TrimSpace(i.TenantID),
		strings.TrimSpace(i.ContentSourceID),
		strings.TrimSpace(i.Lane),
		strings.TrimSpace(i.Purpose),
		i.CadenceWindowStart.UTC().Format(time.RFC3339Nano),
		fmt.Sprintf("%d", i.SourceConfigVersion),
		strings.TrimSpace(i.PolicyFingerprint),
		strings.TrimSpace(i.ArgumentFingerprint),
		strings.TrimSpace(i.PlanStepToolTarget),
	}, "\n")
	digest := sha256.Sum256([]byte(canonical))
	return "source-run:" + hex.EncodeToString(digest[:]), nil
}

func DeterministicUnitJobID(tenantID, requestID, attemptID, unitID, fence string) (string, error) {
	parts := []string{tenantID, requestID, attemptID, unitID, fence}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return "", fmt.Errorf("tenant, request, attempt, unit, and fence are required for a deterministic job ID")
		}
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return "source-unit:" + hex.EncodeToString(digest[:]), nil
}

// ManifestChildDigest binds a fetch-page declaration to the exact, typed
// child-unit keys CMS authorized for that page. Sorting makes repeated
// deliveries and different producer iteration order converge to one value.
func ManifestChildDigest(unitKeys []string) (string, error) {
	if unitKeys == nil {
		return "", fmt.Errorf("manifest child keys are required")
	}
	keys := append([]string(nil), unitKeys...)
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return "", fmt.Errorf("manifest child key is required")
		}
	}
	sort.Strings(keys)
	digest := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return hex.EncodeToString(digest[:]), nil
}

// LeaseAllowsEffect differentiates a renewable execution lease from an
// immutable attempt fence. Both must match before a worker can start a side
// effect or report a receipt; an expiry is always fail-closed.
func LeaseAllowsEffect(expectedFence, suppliedFence, expectedLease, suppliedLease string, expiresAt, now time.Time) bool {
	return strings.TrimSpace(expectedFence) != "" && expectedFence == suppliedFence && strings.TrimSpace(expectedLease) != "" && expectedLease == suppliedLease && expiresAt.After(now.UTC())
}
