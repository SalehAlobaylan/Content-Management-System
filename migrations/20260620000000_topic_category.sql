-- Story category: one slug from the finite news taxonomy
-- (politics/economy/world/conflict/sports/technology/science/health/culture/
-- society/general), classified by the same LLM digest call that fills
-- summary/bullets. NULL = not yet classified; "general"/unknown render no chip.
ALTER TABLE topics ADD COLUMN IF NOT EXISTS category text;
