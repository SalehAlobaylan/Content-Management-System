package supply

// ResolutionObservation preserves the difference between a recovered boundary,
// a boundary still observed as broken, and evidence CMS cannot currently read.
// `unknown` is intentionally never eligible for automatic episode closure.
type ResolutionObservation string

const (
	ResolutionObservationPresent ResolutionObservation = "present"
	ResolutionObservationAbsent  ResolutionObservation = "absent"
	ResolutionObservationUnknown ResolutionObservation = "unknown"

	EpisodeResolutionResolved  = "resolved"
	EpisodeResolutionStillOpen = "still_open"
	EpisodeResolutionUnknown   = "unknown"
)

type EpisodeResolutionInput struct {
	Verdict                   SupplyVerdict
	SourceAdmissionRecovered  ResolutionObservation
	SourceDeliveryRecovered   ResolutionObservation
	ActiveSourceConfigured    ResolutionObservation
	FreshEvidenceAvailable    ResolutionObservation
	ConsumerBoundaryRecovered ResolutionObservation
}

type EpisodeResolution struct {
	State   string `json:"state"`
	Kind    string `json:"kind"`
	Summary string `json:"summary"`
}

// AssessEpisodeResolution defines the static recovery contract for an open
// attention episode. It does not issue work or infer a recovery from the
// absence of the old alert. Every eligible closure requires fresh CMS evidence
// at the same boundary that originally failed.
func AssessEpisodeResolution(input EpisodeResolutionInput) EpisodeResolution {
	switch input.Verdict {
	case SupplyVerdictSourceDueNotAdmitted:
		return assessRecoveryObservation(input.SourceAdmissionRecovered, "source_admission_restored")
	case SupplyVerdictSourceRunWithoutIngest, SupplyVerdictPodsDeliveryDegraded:
		return assessRecoveryObservation(input.SourceDeliveryRecovered, "pods_delivery_verified_after_episode")
	case SupplyVerdictNoActiveMediaSources:
		return assessRecoveryObservation(input.ActiveSourceConfigured, "active_media_source_scheduled")
	case SupplyVerdictEvidenceUnavailable:
		return assessRecoveryObservation(input.FreshEvidenceAvailable, "cms_evidence_boundary_restored")
	case SupplyVerdictNoEligibleInventory, SupplyVerdictGenerationOmission,
		SupplyVerdictServingOmission, SupplyVerdictPodsInventoryStale:
		return assessRecoveryObservation(input.ConsumerBoundaryRecovered, "pods_consumer_boundary_restored")
	default:
		return EpisodeResolution{State: EpisodeResolutionUnknown, Kind: "unsupported_verdict", Summary: "CMS has no registered automatic recovery proof for this episode verdict."}
	}
}

func assessRecoveryObservation(observation ResolutionObservation, kind string) EpisodeResolution {
	switch observation {
	case ResolutionObservationPresent:
		return EpisodeResolution{State: EpisodeResolutionResolved, Kind: kind, Summary: "Fresh CMS evidence independently proves this episode boundary recovered."}
	case ResolutionObservationAbsent:
		return EpisodeResolution{State: EpisodeResolutionStillOpen, Kind: kind, Summary: "Fresh CMS evidence still does not prove this episode boundary recovered."}
	default:
		return EpisodeResolution{State: EpisodeResolutionUnknown, Kind: kind, Summary: "CMS cannot currently read enough evidence to close this episode."}
	}
}
