-- Latest-only analytics schema (1 row per practice) + logging
-- Apply in Supabase SQL editor, or point DATABASE_URL at Supabase and run the backend once.

create extension if not exists pgcrypto;

-- Practices (latest-only)
create table if not exists public.practices (
  id uuid primary key default gen_random_uuid(),
  practice_key text unique not null,

  practice_name text null,
  display_name text null,
  address_text text null,
  postcode text null,
  county text null,

  surgery_count integer null,

  associate_cost_amount numeric null,
  associate_cost_pct numeric null, -- 0..100
  accounts_period_end date null,

  source_file text null,
  raw_json jsonb not null default '{}'::jsonb,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists practices_county_surgery_idx on public.practices(county, surgery_count);
create index if not exists practices_postcode_idx on public.practices(postcode);
create index if not exists practices_accounts_period_end_idx on public.practices(accounts_period_end);

-- Extraction log (field-level confidence + evidence)
create table if not exists public.extraction_log (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),

  practice_key text not null,
  source_file text null,
  accounts_period_end date null,

  field_confidence jsonb not null default '{}'::jsonb,
  missing_fields text[] not null default '{}'::text[],
  low_conf_fields text[] not null default '{}'::text[],
  evidence jsonb null,
  notes text null
);

create index if not exists extraction_log_practice_created_idx
  on public.extraction_log(practice_key, created_at desc);

-- Request log (every NLQ / analytics request)
create table if not exists public.request_log (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),

  request_type text not null default 'nlq',
  query_text text not null,

  intent jsonb null,
  sql_template text null,
  params jsonb null,

  status text not null default 'ok', -- ok|no_results|error|blocked
  row_count integer null,
  latency_ms integer null,
  warnings text[] not null default '{}'::text[],
  error_message text null
);

create index if not exists request_log_created_idx on public.request_log(created_at desc);
