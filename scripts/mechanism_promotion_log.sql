-- Inferred mechanism promotion log (apply in Supabase SQL editor).
-- Service role inserts via cloud9-api; summaries are grouped by signature in app code.

CREATE TABLE IF NOT EXISTS public.inferred_mechanism_promotions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  signature text NOT NULL,
  trigger_rule text NOT NULL,
  config_precondition text NOT NULL DEFAULT '',
  fact_kinds jsonb NOT NULL DEFAULT '[]'::jsonb,
  proposed_mechanism jsonb NOT NULL,
  claim_aspect text NOT NULL DEFAULT '',
  claim_text text NOT NULL DEFAULT '',
  template text NOT NULL DEFAULT '',
  deployment_namespace text NOT NULL DEFAULT '',
  user_id uuid NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS inferred_mechanism_promotions_signature_idx
  ON public.inferred_mechanism_promotions (signature);

CREATE INDEX IF NOT EXISTS inferred_mechanism_promotions_created_at_idx
  ON public.inferred_mechanism_promotions (created_at DESC);
