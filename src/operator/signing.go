package operator

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// PlanSigningKeyFromEnv preserves the former dedicated override for existing
// deployments. By default it derives a purpose-bound signing key from the
// established CMS service identity, avoiding an additional Operator-only
// secret/configuration gate while keeping plan signatures distinct.
func PlanSigningKeyFromEnv() ([]byte, error) {
	key := strings.TrimSpace(os.Getenv("OPERATOR_PLAN_SIGNING_KEY"))
	if key != "" {
		if len(key) < 32 {
			return nil, fmt.Errorf("%w: operator plan signing key is too short", ErrInvalidContract)
		}
		return []byte(key), nil
	}
	serviceToken := strings.TrimSpace(os.Getenv("CMS_SERVICE_TOKEN"))
	if serviceToken == "" {
		return nil, fmt.Errorf("%w: CMS service identity is unavailable", ErrInvalidContract)
	}
	mac := hmac.New(sha256.New, []byte(serviceToken))
	_, _ = mac.Write([]byte("wahb-operator/plan-signing/v1"))
	return mac.Sum(nil), nil
}

// CanonicalPlan is the minimum signed envelope. The executor later expands the
// exact steps, branches, and rollback metadata, but never lets a model or
// browser supply the signed bytes.
type CanonicalPlan struct {
	SchemaVersion       string         `json:"schema_version"`
	PlanID              string         `json:"plan_id"`
	TenantID            string         `json:"tenant_id"`
	ActorID             string         `json:"actor_id"`
	ToolKey             string         `json:"tool_key"`
	ToolVersion         string         `json:"tool_version"`
	TargetIDs           []string       `json:"target_ids"`
	NormalizedArguments map[string]any `json:"normalized_arguments"`
	EvidenceIDs         []string       `json:"evidence_ids"`
	EvidenceFingerprint string         `json:"evidence_fingerprint"`
	AccessVersion       string         `json:"access_version"`
	RiskTier            RiskTier       `json:"risk_tier"`
	Cancellation        string         `json:"cancellation"`
	Rollback            string         `json:"rollback"`
	Contingencies       []string       `json:"contingencies"`
}

func (plan CanonicalPlan) Validate() error {
	if plan.SchemaVersion != ContractVersion || strings.TrimSpace(plan.PlanID) == "" || strings.TrimSpace(plan.TenantID) == "" || strings.TrimSpace(plan.ActorID) == "" || strings.TrimSpace(plan.ToolKey) == "" || strings.TrimSpace(plan.ToolVersion) == "" || strings.TrimSpace(plan.EvidenceFingerprint) == "" || strings.TrimSpace(plan.AccessVersion) == "" {
		return fmt.Errorf("%w: canonical plan identity is incomplete", ErrInvalidContract)
	}
	if len(plan.TargetIDs) < 1 || len(plan.TargetIDs) > 20 || len(plan.EvidenceIDs) < 1 || strings.TrimSpace(plan.Cancellation) == "" || strings.TrimSpace(plan.Rollback) == "" || len(plan.Contingencies) == 0 {
		return fmt.Errorf("%w: canonical plan targets or evidence are invalid", ErrInvalidContract)
	}
	for _, contingency := range plan.Contingencies {
		if strings.TrimSpace(contingency) == "" {
			return fmt.Errorf("%w: canonical plan contingencies are invalid", ErrInvalidContract)
		}
	}
	if plan.RiskTier != RiskRoutine && plan.RiskTier != RiskHigh {
		return fmt.Errorf("%w: canonical plan must be a mutation risk tier", ErrInvalidContract)
	}
	return nil
}

func CanonicalPlanDigest(plan CanonicalPlan) (string, []byte, error) {
	if err := plan.Validate(); err != nil {
		return "", nil, err
	}
	serialized, err := json.Marshal(plan)
	if err != nil {
		return "", nil, fmt.Errorf("marshal canonical plan: %w", err)
	}
	digest := sha256.Sum256(serialized)
	return hex.EncodeToString(digest[:]), serialized, nil
}

func SignCanonicalPlan(key []byte, plan CanonicalPlan) (digest, signature string, err error) {
	if len(key) < 32 {
		return "", "", fmt.Errorf("%w: plan signing key must be at least 32 bytes", ErrInvalidContract)
	}
	digest, serialized, err := CanonicalPlanDigest(plan)
	if err != nil {
		return "", "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(serialized)
	return digest, hex.EncodeToString(mac.Sum(nil)), nil
}

func VerifyCanonicalPlanSignature(key []byte, plan CanonicalPlan, expectedDigest, expectedSignature string) error {
	digest, signature, err := SignCanonicalPlan(key, plan)
	if err != nil {
		return err
	}
	if _, err := hex.DecodeString(expectedSignature); err != nil || !hmac.Equal([]byte(digest), []byte(expectedDigest)) || !hmac.Equal([]byte(signature), []byte(expectedSignature)) {
		return fmt.Errorf("%w: canonical plan signature mismatch", ErrInvalidContract)
	}
	return nil
}
