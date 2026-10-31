package operator

import (
	"strings"
	"testing"
)

func TestStorageFallbackReportsMeasurementsAndTheirScope(t *testing.T) {
	provider, objects, quota, enabled := int64(4096), int64(7), int64(10240), false
	packet := DecisionPacket{VisibleContext: VisibleContext{Domain: "storage_quality"}, Facts: []Fact{{Key: "storage_quality.usage", Value: StorageSnapshot{TrackedBytes: 1536, TrackedItems: 2, ProviderUsedBytes: &provider, ProviderObjectCount: &objects, ProviderScope: "platform_global", PolicyLimitBytes: &quota, PolicyScope: "platform_global", CleanupEnabled: &enabled}, EvidenceIDs: []string{"storage-proof"}}}}
	blocks := DeterministicDegradedBlocksForLocale(packet, "en", "AI interpretation is unavailable.")
	joined := ""
	for _, block := range blocks {
		joined += block.Text + "\n"
		if err := block.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"1.50 KiB", "2 non-archived", "4.00 KiB", "7 objects", "platform-wide", "10.00 KiB", "platform default", "disabled"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("storage answer is missing %q: %s", want, joined)
		}
	}
	packet.Facts[0].Value = StorageSnapshot{TrackedBytes: 1536, TrackedItems: 2, ProviderScope: "platform_global"}
	packet.Unknowns = []string{"The storage owner could not report shared-bucket usage; tenant CMS totals are still available."}
	for _, locale := range []string{"en", "ar"} {
		blocks = DeterministicDegradedBlocksForLocale(packet, locale, "Unavailable")
		if len(blocks) != 3 || blocks[0].Kind != "fact" || blocks[1].Kind != "unknown" {
			t.Fatalf("missing provider became a zero or removed tenant facts: %+v", blocks)
		}
	}
}
