-- Expand practices table with:
-- - normalized address components
-- - core valuation metrics (goodwill/ef&f/total/freehold/grand total)
-- - NHS contract details (UDA block + contract number)
-- - split of income (common types)

alter table if exists public.practices
  add column if not exists city text null,
  add column if not exists address_line1 text null,
  add column if not exists address_line2 text null;

alter table if exists public.practices
  add column if not exists goodwill numeric null,
  add column if not exists efandf numeric null,
  add column if not exists total numeric null,
  add column if not exists freehold numeric null,
  add column if not exists grand_total numeric null;

alter table if exists public.practices
  add column if not exists nhs_contract_number text null,
  add column if not exists uda_contract_value_gbp numeric null,
  add column if not exists uda_count numeric null,
  add column if not exists uda_rate_gbp numeric null,
  add column if not exists uda_uplift_value_gbp numeric null;

-- Split of income (selected common types)
alter table if exists public.practices
  add column if not exists income_split_fpi_percent numeric null,
  add column if not exists income_split_fpi_value numeric null,
  add column if not exists income_split_fpi_applied_percent numeric null,
  add column if not exists income_split_fpi_applied_value numeric null,

  add column if not exists income_split_nhs_percent numeric null,
  add column if not exists income_split_nhs_value numeric null,
  add column if not exists income_split_nhs_applied_percent numeric null,
  add column if not exists income_split_nhs_applied_value numeric null,

  add column if not exists income_split_denplan_percent numeric null,
  add column if not exists income_split_denplan_value numeric null,
  add column if not exists income_split_denplan_applied_percent numeric null,
  add column if not exists income_split_denplan_applied_value numeric null,

  add column if not exists income_split_rent_percent numeric null,
  add column if not exists income_split_rent_value numeric null,
  add column if not exists income_split_rent_applied_percent numeric null,
  add column if not exists income_split_rent_applied_value numeric null;

create index if not exists practices_city_idx on public.practices(city);
create index if not exists practices_address_line1_idx on public.practices(address_line1);
create index if not exists practices_nhs_contract_number_idx on public.practices(nhs_contract_number);

