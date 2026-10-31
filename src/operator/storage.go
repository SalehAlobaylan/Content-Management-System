package operator

import "context"

// StorageSnapshot contains only owner-read measurements. A nil provider value
// is unknown, never zero; bucket figures are deployment-wide, not tenant usage.
type StorageSnapshot struct {
	TrackedBytes        int64  `json:"cms_tracked_bytes"`
	TrackedItems        int64  `json:"cms_tracked_items"`
	ProviderUsedBytes   *int64 `json:"provider_used_bytes,omitempty"`
	ProviderObjectCount *int64 `json:"provider_object_count,omitempty"`
	ProviderScope       string `json:"provider_scope"`
	CMSScope            string `json:"cms_scope"`
	CleanupTargetPct    *int   `json:"cleanup_target_utilization_pct,omitempty"`
	PolicyLimitBytes    *int64 `json:"configured_limit_bytes,omitempty"`
	PolicyScope         string `json:"policy_scope,omitempty"`
	CleanupEnabled      *bool  `json:"automatic_cleanup_policy_enabled,omitempty"`
	CleanupActivity     string `json:"current_cleanup_activity"`
}

type StorageSnapshotReader func(context.Context, string) (StorageSnapshot, []string, error)

func (fabric *ContextFabric) WithStorageReader(reader StorageSnapshotReader) *ContextFabric {
	fabric.storageReader = reader
	return fabric
}

func (fabric *ContextFabric) BuildStoragePacket(ctx context.Context, visible VisibleContext, access AccessSnapshot) (DecisionPacket, error) {
	packet, err := fabric.BuildCoveragePacket(ctx, visible, access)
	if err != nil {
		return DecisionPacket{}, err
	}
	if fabric.storageReader == nil {
		packet.Unknowns = append(packet.Unknowns, "Storage usage measurements are unavailable from the storage owner.")
	} else {
		snapshot, unknowns, err := fabric.storageReader(ctx, access.TenantID)
		if err != nil {
			return DecisionPacket{}, err
		}
		descriptor, _ := fabric.registry.Descriptor(coverageAdapterKey("storage_quality"))
		now := fabric.now()
		ref := fabric.evidence(descriptor, access.TenantID, "storage_usage:"+access.TenantID, []SubjectRef{{Type: "storage_usage", ID: access.TenantID}}, now, now, EvidenceAvailable, snapshot)
		if len(unknowns) > 0 {
			ref.Availability = EvidencePartial
		}
		packet.Evidence = append(packet.Evidence, ref)
		packet.Facts = append(packet.Facts, Fact{Key: "storage_quality.usage", Value: snapshot, EvidenceIDs: []string{ref.EvidenceID}})
		packet.Unknowns = append(packet.Unknowns, unknowns...)
	}
	if len(packet.Unknowns) > 0 {
		packet.Completeness = "partial"
	}
	packet.Recommendations = deterministicRecommendations(visible, packet.Evidence, packet.Unknowns)
	packet.CollectionEndedAt = fabric.now()
	packet.Fingerprint = fingerprintPacket(packet)
	return packet, packet.Validate()
}
