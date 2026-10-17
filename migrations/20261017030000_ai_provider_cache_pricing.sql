-- Optional provider-side cache pricing; NULL preserves standard input pricing.
ALTER TABLE ai_price_book ADD COLUMN cached_input_usd_per_1m double precision;
ALTER TABLE ai_price_book ADD CONSTRAINT ai_price_book_cached_input_finite
 CHECK (cached_input_usd_per_1m IS NULL OR
  (cached_input_usd_per_1m >= 0 AND cached_input_usd_per_1m < 'Infinity'::double precision));
