-- Client report leads + magic-link unlock tokens.
create table if not exists public.report_leads (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  name text not null,
  email text not null,
  location text null,
  surgery_count integer null,
  report_json jsonb not null,
  token_hash text not null unique,
  expires_at timestamptz not null,
  verified_at timestamptz null,
  unlocked_at timestamptz null,
  email_id text null
);

create index if not exists report_leads_email_idx on public.report_leads (lower(email));
create index if not exists report_leads_expires_idx on public.report_leads (expires_at);

alter table public.report_leads enable row level security;

revoke all on table public.report_leads from anon, authenticated;
grant all on table public.report_leads to service_role;

comment on table public.report_leads is
  'Client benchmark report requests gated by Resend magic-link email verification.';
