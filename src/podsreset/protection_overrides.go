package podsreset

import (
	"fmt"
	"sort"
)

type ProtectionOverride struct {
	Code        string `json:"code"`
	Label       string `json:"label"`
	Consequence string `json:"consequence"`
}

// Explicit fresh-start exceptions. Missing file inventory, shared ownership,
// live provider effects, identity, family and unknown failures remain blocking.
var protectionOverrides = []ProtectionOverride{
	{"approved_transcript", "Approved captions", "Exclusively owned captions and version text will be permanently cleared. Shared transcripts stay intact."},
	{"chapter_draft", "Editorial chapter drafts", "The source content will be removed. Chapter draft records remain linked to its retired identity."},
	{"manual_chapter_metadata", "Manual chapters", "Exclusively owned manual chapters will be deleted. Shared transcript chapters stay intact."},
	{"manual_atomization_decision", "Manual atomization decisions", "The selected content and its manual atomization settings will be cleared."},
	{"protected_content", "Interactions, flags and moderation holds", "Content will be removed despite these protections. Interaction, flag, hold and moderation records remain linked to the retired identity."},
	{"media_circulation_protection", "Editorial and storage protections", "Content will be removed despite editorial holds, never-archive or keep-latest protections. Protection records are retained."},
	{"user_authored_content", "User-authored media", "The selected user-authored payload will be permanently cleared. User accounts and interaction history are retained."},
	{"content_not_terminal", "Pending or processing content", "The selected content will be permanently retired even if its content status is pending or processing."},
	{"active_content_stage", "Unfinished processing records", "Selected stage requests will be cancelled after the retirement fence. Attempt and receipt history remain intact; later writebacks are rejected."},
	{"active_atomization_run", "Legacy atomization records", "Selected legacy atomization runs will be stopped and marked failed by reset. Their history remains; later child publication is rejected."},
	{"active_circulation_recommendation", "Circulation recommendations", "Selected item-family recommendations will be superseded so they cannot initiate new content actions."},
	{"artifact_ownership_unproven", "Stale or missing file-registry metadata", "Delete the complete verified inventory in the selected content folder despite stale registry records. Selected parents and chapters are fenced together; content outside this plan stays protected."},
}

func ProtectionOverrides() []ProtectionOverride {
	return append([]ProtectionOverride(nil), protectionOverrides...)
}

func ValidateProtectionOverrides(codes []string) ([]string, error) {
	allowed := make(map[string]bool, len(protectionOverrides))
	for _, option := range protectionOverrides {
		allowed[option.Code] = true
	}
	seen := map[string]bool{}
	for _, code := range codes {
		if !allowed[code] {
			return nil, fmt.Errorf("%q is not an overridable fresh-start blocker", code)
		}
		seen[code] = true
	}
	result := make([]string, 0, len(seen))
	for code := range seen {
		result = append(result, code)
	}
	sort.Strings(result)
	return result, nil
}
