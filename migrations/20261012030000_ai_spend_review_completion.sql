-- Extend future attribution without guessing actors on existing mixed cells.
ALTER TABLE ai_spend_rollups ADD COLUMN human_actor_id text NOT NULL DEFAULT '';
DROP INDEX ai_spend_rollups_dimensions;
CREATE UNIQUE INDEX ai_spend_rollups_dimensions ON ai_spend_rollups(tenant_id,day,spend_class,operation,provider,model,trigger_source,system_run_id,human_actor_id);
ALTER TABLE ai_spend_policies ADD COLUMN compute_watch_limits jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE ai_spend_episodes ADD COLUMN closed_by text NOT NULL DEFAULT '', ADD COLUMN closed_at timestamptz;
ALTER TABLE ai_spend_episodes ADD COLUMN reopen_times jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE ai_spend_overrides ADD COLUMN admission_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE ai_spend_emitters ADD COLUMN closed boolean NOT NULL DEFAULT false;
-- This is an explicitly estimated class fallback, using the existing reference
-- rate. It is operator-tunable, never a claim about an unmatched provider bill.
INSERT INTO ai_price_book(spend_class,provider,model_pattern,input_usd_per_1m,output_usd_per_1m,unit_usd,effective_from,note,created_by)
VALUES('llm','*','*',1.50,9.00,0,'2000-01-01','Estimated class fallback using existing reference rates; unpriced remains visible','migration_seed');
