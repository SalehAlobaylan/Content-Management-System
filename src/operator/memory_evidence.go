package operator

import "time"

// AttachRetrievedMemory preserves the hard authority boundary: retrieved
// material is labeled historical and can explain a case, never replace facts.
func AttachRetrievedMemory(packet *DecisionPacket, hits []MemoryHit, now time.Time) {
	if packet == nil {
		return
	}
	for _, hit := range hits {
		id := "memory:" + hit.Kind + ":" + hit.RecordID
		ref := EvidenceRef{EvidenceID: id, Authority: EvidenceRetrieved, Domain: "operator_memory", AdapterKey: "operator.memory.retrieval", AdapterVersion: "v1", TenantID: packet.TenantID, RequiredPermission: "operator:read", RecordRefs: []SubjectRef{{Type: hit.Kind, ID: hit.RecordID, Label: "historical"}}, DeepLink: "/platform/operator", ObservedAt: now, FetchedAt: now, MaxAgeSeconds: 3600, ExpiresAt: now.Add(time.Hour), ContentHash: fingerprintValue(hit.Content), SourceVersion: hit.SourceVersion, Availability: EvidenceAvailable}
		packet.Evidence = append(packet.Evidence, ref)
		packet.Facts = append(packet.Facts, Fact{Key: "operator_memory." + hit.Kind, Value: map[string]any{"title": hit.Title, "content": hit.Content, "historical": true, "score": hit.Score}, EvidenceIDs: []string{id}})
	}
}
