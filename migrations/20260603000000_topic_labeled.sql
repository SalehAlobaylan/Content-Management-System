-- Full re-cluster support.
--
-- A full re-cluster pass rebuilds the whole taxonomy: k-means over all article
-- embeddings creates fresh clusters with placeholder names, which are then
-- named by the LLM in batches. `labeled` marks which topics still await naming.
-- Growing-taxonomy topics are labeled at creation, so the default is true.

ALTER TABLE topics
    ADD COLUMN IF NOT EXISTS labeled BOOLEAN NOT NULL DEFAULT true;
