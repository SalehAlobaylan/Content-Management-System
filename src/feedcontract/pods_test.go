package feedcontract

import (
	"strings"
	"testing"
)

func TestActiveGenerationMembershipJoinDoesNotExposeTenantColumn(t *testing.T) {
	join := activeGenerationMembershipJoin("content_items.story_id")
	if !strings.Contains(join, "SELECT generation_membership.member_id") {
		t.Fatalf("active generation join must project only member_id: %s", join)
	}
	if strings.Contains(join, "SELECT generation_head.tenant_id") || strings.Contains(join, "SELECT generation_membership.*") {
		t.Fatalf("active generation join leaked columns into the caller namespace: %s", join)
	}
	if !strings.Contains(join, "active_generation_member.member_id = content_items.story_id") {
		t.Fatalf("active generation join lost the caller member identity: %s", join)
	}
}
