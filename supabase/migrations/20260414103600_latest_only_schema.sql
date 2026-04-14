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
  city text null,
  county text null,
  address_line1 text null,
  address_line2 text null,
  visited_on date null,

  surgery_count integer null,

  -- Core valuation metrics
  goodwill numeric null,
  efandf numeric null,
  total numeric null,
  freehold numeric null,
  grand_total numeric null,

  -- NHS contract details (UDA block)
  nhs_contract_number text null,
  uda_contract_value_gbp numeric null,
  uda_count numeric null,
  uda_rate_gbp numeric null,
  uda_uplift_value_gbp numeric null,

  -- Split of income (selected common types)
  income_split_fpi_percent numeric null,
  income_split_fpi_value numeric null,
  income_split_fpi_applied_percent numeric null,
  income_split_fpi_applied_value numeric null,

  income_split_nhs_percent numeric null,
  income_split_nhs_value numeric null,
  income_split_nhs_applied_percent numeric null,
  income_split_nhs_applied_value numeric null,

  income_split_denplan_percent numeric null,
  income_split_denplan_value numeric null,
  income_split_denplan_applied_percent numeric null,
  income_split_denplan_applied_value numeric null,

  income_split_rent_percent numeric null,
  income_split_rent_value numeric null,
  income_split_rent_applied_percent numeric null,
  income_split_rent_applied_value numeric null,

  associate_cost_amount numeric null,
  associate_cost_pct numeric null, -- 0..100
  accounts_period_end date null,

  -- Certified Accounts (latest + previous year end)
  certified_accounts_period_end_prev date null,

  cert_income_gbp numeric null,
  cert_income_percent numeric null,
  cert_income_gbp_prev numeric null,
  cert_income_percent_prev numeric null,

  cert_other_inc_gbp numeric null,
  cert_other_inc_percent numeric null,
  cert_other_inc_gbp_prev numeric null,
  cert_other_inc_percent_prev numeric null,

  cert_associates_gbp numeric null,
  cert_associates_percent numeric null,
  cert_associates_gbp_prev numeric null,
  cert_associates_percent_prev numeric null,

  cert_wages_gbp numeric null,
  cert_wages_percent numeric null,
  cert_wages_gbp_prev numeric null,
  cert_wages_percent_prev numeric null,

  cert_hygiene_gbp numeric null,
  cert_hygiene_percent numeric null,
  cert_hygiene_gbp_prev numeric null,
  cert_hygiene_percent_prev numeric null,

  cert_materials_gbp numeric null,
  cert_materials_percent numeric null,
  cert_materials_gbp_prev numeric null,
  cert_materials_percent_prev numeric null,

  cert_labs_gbp numeric null,
  cert_labs_percent numeric null,
  cert_labs_gbp_prev numeric null,
  cert_labs_percent_prev numeric null,

  cert_net_profit_gbp numeric null,
  cert_net_profit_percent numeric null,
  cert_net_profit_gbp_prev numeric null,
  cert_net_profit_percent_prev numeric null,

  source_file text null,
  raw_json jsonb not null default '{}'::jsonb,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists practices_county_surgery_idx on public.practices(county, surgery_count);
create index if not exists practices_postcode_idx on public.practices(postcode);
create index if not exists practices_accounts_period_end_idx on public.practices(accounts_period_end);
create index if not exists practices_cert_prev_end_idx on public.practices(certified_accounts_period_end_prev);
create index if not exists practices_city_idx on public.practices(city);
create index if not exists practices_address_line1_idx on public.practices(address_line1);
create index if not exists practices_nhs_contract_number_idx on public.practices(nhs_contract_number);
create index if not exists practices_visited_on_idx on public.practices(visited_on);

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
