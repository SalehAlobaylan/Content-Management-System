-- Preserve the existing invoice owner and allow net provider credit/refund months.
-- Admission/budgets remain based on canonical usage receipts, not invoices.
ALTER TABLE ai_spend_invoices DROP CONSTRAINT IF EXISTS ai_spend_invoices_amount_usd_check;
ALTER TABLE ai_spend_invoices ADD CONSTRAINT ai_spend_invoices_amount_usd_finite
 CHECK(amount_usd > '-Infinity'::double precision AND amount_usd < 'Infinity'::double precision);
