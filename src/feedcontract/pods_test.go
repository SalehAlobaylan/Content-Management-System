package feedcontract

import (
	"regexp"
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
	if !strings.Contains(join, ".member_id = content_items.story_id") {
		t.Fatalf("active generation join lost the caller member identity: %s", join)
	}
}

func TestNewsStoryAndItemMembershipJoinsCompose(t *testing.T) {
	for _, itemJoin := range []string{activeGenerationMembershipJoin("content_items.public_id"), legacyNewsItemMembershipJoin("content_items.public_id")} {
		query := activeGenerationMembershipJoin("content_items.story_id") + " " + itemJoin
		aliases := regexp.MustCompile(`\)\s+(\w+)\s+ON`).FindAllStringSubmatch(query, -1)
		if len(aliases) != 2 || aliases[0][1] == aliases[1][1] {
			t.Fatalf("composed News fences reuse an SQL alias: %s", query)
		}
		for _, column := range []string{"content_items.story_id", "content_items.public_id"} {
			if !strings.Contains(query, ".member_id = "+column) {
				t.Fatalf("composed query lost membership fence for %s", column)
			}
		}
	}
}
