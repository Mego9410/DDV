-- User-submitted chat bug reports for model review and improvement.
CREATE TABLE IF NOT EXISTS public.bug_reports (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz NOT NULL DEFAULT now(),
  description text NOT NULL,
  chat_thread jsonb NOT NULL DEFAULT '[]'::jsonb,
  status text NOT NULL DEFAULT 'open',
  user_agent text,
  page_url text,
  CONSTRAINT bug_reports_description_nonempty CHECK (char_length(btrim(description)) > 0),
  CONSTRAINT bug_reports_status_check CHECK (status = ANY (ARRAY['open'::text, 'reviewed'::text, 'closed'::text]))
);

CREATE INDEX IF NOT EXISTS bug_reports_created_at_idx ON public.bug_reports (created_at DESC);
CREATE INDEX IF NOT EXISTS bug_reports_status_idx ON public.bug_reports (status);

ALTER TABLE public.bug_reports ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.bug_reports FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.bug_reports TO service_role;

COMMENT ON TABLE public.bug_reports IS 'User-submitted chat bug reports for model review and improvement.';
