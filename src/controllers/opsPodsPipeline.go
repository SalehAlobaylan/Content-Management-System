package controllers

import "fmt"

// A read-only projection of the existing Pods lane owner. Its indefinite pause
// is not compatible with the fleet's expiring pause/restore command contract.
const opsPodsPipelineSQL = `SELECT t.tenant_id,
 COALESCE(c.scheduling_enabled,true) AND COALESCE(c.execution_enabled,true) AS enabled,
 EXISTS(SELECT 1 FROM content_stage_cutovers pc WHERE pc.tenant_id=t.tenant_id AND pc.lane='pods' AND pc.mode='durable_required')
 AND EXISTS(SELECT 1 FROM source_run_admission_protocol ap WHERE ap.protocol_key='source-run/v1' AND ap.epoch='durable_required')
 AND EXISTS(SELECT 1 FROM source_run_admission_cutovers ac WHERE ac.tenant_id=t.tenant_id AND ac.lane='media' AND ac.mode='durable' AND ac.protocol='source-run/v1') AS pipeline_ready,
 'pipeline' AS mode,
 COALESCE(s.running,0)+COALESCE(r.running,0) AS pipeline_running,
 COALESCE(s.admitted,0)+COALESCE(r.admitted,0) AS pipeline_admitted,
 COALESCE(s.verifying,0)+COALESCE(r.verifying,0) AS pipeline_verifying,
 COALESCE(s.unresolved,0) AS pipeline_unresolved,
 COALESCE(s.queued,0)+COALESCE(r.queued,0) AS pipeline_queued
 FROM (
 SELECT tenant_id FROM content_stage_controls WHERE lane='pods'
 UNION SELECT tenant_id FROM content_sources WHERE category='media'
 UNION SELECT tenant_id FROM content_stage_cutovers WHERE lane='pods'
 ) t LEFT JOIN content_stage_controls c ON c.tenant_id=t.tenant_id AND c.lane='pods'
 LEFT JOIN (
 SELECT q.tenant_id,
 COUNT(*) FILTER (WHERE q.state='running') AS running,
 COUNT(*) FILTER (WHERE q.state='claimed') AS admitted,
 COUNT(*) FILTER (WHERE q.state='verifying') AS verifying,
 COUNT(*) FILTER (WHERE q.state IN ('uncertain','reconciling')) AS unresolved,
 COUNT(*) FILTER (WHERE q.state IN ('queued','deferred')) AS queued
 FROM content_stage_requests q JOIN content_items i
 ON i.tenant_id=q.tenant_id AND i.public_id=q.content_item_id AND i.processing_generation=q.processing_generation
 WHERE q.lane='pods' AND i.status<>'ARCHIVED' GROUP BY q.tenant_id
 ) s ON s.tenant_id=t.tenant_id
 LEFT JOIN (
 SELECT tenant_id,COUNT(*) FILTER (WHERE state='running') AS running,
 COUNT(*) FILTER (WHERE state='accepted') AS admitted,
 COUNT(*) FILTER (WHERE state='verification_required') AS verifying,
 COUNT(*) FILTER (WHERE state='requested') AS queued
 FROM source_run_requests WHERE lane='media' GROUP BY tenant_id
 ) r ON r.tenant_id=t.tenant_id`

func opsPodsPipelineStatus(status opsMemberStatus, row opsStatusRow) opsMemberStatus {
	status.Liveness = "not_applicable"
	status.Containment.PauseScope = "Use Pods pipeline controls; excluded from fleet pause"
	status.State = "idle"
	scope := "Pipeline enabled."
	if !row.Enabled {
		status.State = "paused"
		scope = "Pipeline paused until resumed; manual processing also waits."
	} else if !row.PipelineReady {
		status.State = "attention"
		scope = "Pipeline enabled; processing needs setup."
	} else if row.PipelineUnresolved > 0 {
		status.State = "attention"
	} else if row.PipelineRunning > 0 {
		status.State = "running"
	}
	status.Evidence = fmt.Sprintf("%s Running steps/source checks: %d. Admitted work waiting to start: %d. Results being verified: %d. Outcomes needing checking: %d. Queued steps/source requests: %d. Published Pods stay available.", scope, row.PipelineRunning, row.PipelineAdmitted, row.PipelineVerifying, row.PipelineUnresolved, row.PipelineQueued)
	return status
}
