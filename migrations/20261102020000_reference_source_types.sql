-- Expand the historical source-kind constraint to the existing CMS SourceType contract.
-- Add and validate the wider replacement before removing the old constraint.
ALTER TABLE content_sources ADD CONSTRAINT content_sources_type_reference_check
 CHECK (type IN ('RSS','WEBSITE','TELEGRAM','TWITTER','PODCAST','YOUTUBE','UPLOAD','MANUAL')) NOT VALID;
ALTER TABLE content_sources VALIDATE CONSTRAINT content_sources_type_reference_check;
ALTER TABLE content_sources DROP CONSTRAINT IF EXISTS content_sources_type_check;
ALTER TABLE content_sources RENAME CONSTRAINT content_sources_type_reference_check TO content_sources_type_check;
