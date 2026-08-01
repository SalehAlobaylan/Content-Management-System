package operator

import (
	"fmt"
	"sort"
	"strings"
)

// AuthorityClaim is a normalized statement from an authoritative adapter.
// Different claims for the same key are preserved as a blocking conflict,
// rather than silently selecting whichever database row was read last.
type AuthorityClaim struct {
	Key        string
	Value      any
	EvidenceID string
	Authority  EvidenceAuthority
}

// ResolveAuthorityClaims makes source precedence explicit. A more current,
// higher-authority source can supersede a lower-authority historical aid, but
// equal-precedence disagreement is retained as a blocking conflict.
func ResolveAuthorityClaims(claims []AuthorityClaim) (map[string]AuthorityClaim, []string, error) {
	resolved := make(map[string]AuthorityClaim)
	byKey := make(map[string][]AuthorityClaim)
	for _, claim := range claims {
		if strings.TrimSpace(claim.Key) == "" || strings.TrimSpace(claim.EvidenceID) == "" || authorityRank(claim.Authority) == 0 {
			return nil, nil, fmt.Errorf("%w: authority claims require a key, evidence, and known authority", ErrInvalidContract)
		}
		byKey[claim.Key] = append(byKey[claim.Key], claim)
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	conflicts := make([]string, 0)
	for _, key := range keys {
		claimsForKey := byKey[key]
		sort.SliceStable(claimsForKey, func(i, j int) bool {
			left, right := authorityRank(claimsForKey[i].Authority), authorityRank(claimsForKey[j].Authority)
			if left != right {
				return left > right
			}
			return claimsForKey[i].EvidenceID < claimsForKey[j].EvidenceID
		})
		resolved[key] = claimsForKey[0]
		primaryValue := fingerprintValue(claimsForKey[0].Value)
		for _, claim := range claimsForKey[1:] {
			if authorityRank(claim.Authority) == authorityRank(claimsForKey[0].Authority) && fingerprintValue(claim.Value) != primaryValue {
				conflicts = append(conflicts, "Conflicting authoritative values for "+key+"; no action can be planned until the source records agree.")
				break
			}
		}
	}
	return resolved, conflicts, nil
}

func authorityRank(authority EvidenceAuthority) int {
	switch authority {
	case EvidenceLive:
		return 5
	case EvidenceTemporal:
		return 4
	case EvidenceRetrieved:
		return 3
	case EvidenceDerived:
		return 2
	case EvidenceMemory:
		return 1
	default:
		return 0
	}
}

func DetectAuthorityConflicts(claims []AuthorityClaim) ([]string, error) {
	values := make(map[string]map[string]struct{})
	for _, claim := range claims {
		if strings.TrimSpace(claim.Key) == "" || strings.TrimSpace(claim.EvidenceID) == "" {
			return nil, fmt.Errorf("%w: authority claims require a key and evidence", ErrInvalidContract)
		}
		if values[claim.Key] == nil {
			values[claim.Key] = map[string]struct{}{}
		}
		values[claim.Key][fingerprintValue(claim.Value)] = struct{}{}
	}
	keys := make([]string, 0, len(values))
	for key, distinct := range values {
		if len(distinct) > 1 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	conflicts := make([]string, 0, len(keys))
	for _, key := range keys {
		conflicts = append(conflicts, "Conflicting authoritative values for "+key+"; no action can be planned until the source records agree.")
	}
	return conflicts, nil
}
