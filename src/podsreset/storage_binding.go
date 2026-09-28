package podsreset

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

type StorageBinding struct {
	StorageTier         string `json:"storage_tier"`
	Bucket              string `json:"bucket"`
	EndpointFingerprint string `json:"endpoint_fingerprint"`
}

func ValidateStorageBindings(tiers []string, bindings []StorageBinding) error {
	wanted := map[string]bool{}
	for _, tier := range tiers {
		if tier != "primary" && tier != "cold" || wanted[tier] {
			return fmt.Errorf("invalid or duplicate storage tier")
		}
		wanted[tier] = true
	}
	if len(wanted) == 0 || len(bindings) != len(wanted) {
		return fmt.Errorf("storage binding set does not match configured tiers")
	}
	seen := map[string]bool{}
	for _, binding := range bindings {
		if !wanted[binding.StorageTier] || seen[binding.StorageTier] || strings.TrimSpace(binding.Bucket) == "" || len(binding.EndpointFingerprint) != 64 {
			return fmt.Errorf("storage binding is incomplete or outside configured tiers")
		}
		if _, err := hex.DecodeString(binding.EndpointFingerprint); err != nil {
			return fmt.Errorf("storage endpoint fingerprint is invalid")
		}
		seen[binding.StorageTier] = true
	}
	return nil
}

func SameStorageBindings(left, right []StorageBinding) bool {
	key := func(bindings []StorageBinding) []string {
		out := make([]string, 0, len(bindings))
		for _, binding := range bindings {
			out = append(out, binding.StorageTier+"\n"+binding.Bucket+"\n"+strings.ToLower(binding.EndpointFingerprint))
		}
		sort.Strings(out)
		return out
	}
	leftKeys, rightKeys := key(left), key(right)
	if len(leftKeys) != len(rightKeys) {
		return false
	}
	for index := range leftKeys {
		if leftKeys[index] != rightKeys[index] {
			return false
		}
	}
	return true
}
