-- Expand latest-only practices table to include Certified Accounts metrics
-- (latest + previous year end) as first-class columns.

alter table if exists public.practices
  add column if not exists certified_accounts_period_end_prev date null;

alter table if exists public.practices
  add column if not exists cert_income_gbp numeric null,
  add column if not exists cert_income_percent numeric null,
  add column if not exists cert_income_gbp_prev numeric null,
  add column if not exists cert_income_percent_prev numeric null,

  add column if not exists cert_other_inc_gbp numeric null,
  add column if not exists cert_other_inc_percent numeric null,
  add column if not exists cert_other_inc_gbp_prev numeric null,
  add column if not exists cert_other_inc_percent_prev numeric null,

  add column if not exists cert_associates_gbp numeric null,
  add column if not exists cert_associates_percent numeric null,
  add column if not exists cert_associates_gbp_prev numeric null,
  add column if not exists cert_associates_percent_prev numeric null,

  add column if not exists cert_wages_gbp numeric null,
  add column if not exists cert_wages_percent numeric null,
  add column if not exists cert_wages_gbp_prev numeric null,
  add column if not exists cert_wages_percent_prev numeric null,

  add column if not exists cert_hygiene_gbp numeric null,
  add column if not exists cert_hygiene_percent numeric null,
  add column if not exists cert_hygiene_gbp_prev numeric null,
  add column if not exists cert_hygiene_percent_prev numeric null,

  add column if not exists cert_materials_gbp numeric null,
  add column if not exists cert_materials_percent numeric null,
  add column if not exists cert_materials_gbp_prev numeric null,
  add column if not exists cert_materials_percent_prev numeric null,

  add column if not exists cert_labs_gbp numeric null,
  add column if not exists cert_labs_percent numeric null,
  add column if not exists cert_labs_gbp_prev numeric null,
  add column if not exists cert_labs_percent_prev numeric null,

  add column if not exists cert_net_profit_gbp numeric null,
  add column if not exists cert_net_profit_percent numeric null,
  add column if not exists cert_net_profit_gbp_prev numeric null,
  add column if not exists cert_net_profit_percent_prev numeric null;

create index if not exists practices_cert_prev_end_idx
  on public.practices(certified_accounts_period_end_prev);

