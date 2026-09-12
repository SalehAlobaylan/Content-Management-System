-- Subprocess diagnostics (notably ffmpeg progress) can exceed the original
-- varchar(512) column.  Failure summaries are bounded in application code,
-- but widening the column keeps the audit trail useful for future workers and
-- prevents a noisy error from rolling back an otherwise valid terminal state.
ALTER TABLE content_stage_attempts
  ALTER COLUMN failure_summary TYPE TEXT;
