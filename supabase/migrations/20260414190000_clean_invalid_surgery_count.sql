-- Clean invalid surgery_count outliers that skew aggregates.
-- Dentistry surgery_count should be a small integer; values like 18000 are ingestion errors.
--
-- Strategy:
-- - Set clearly invalid values to NULL (so they are excluded from avg/median/etc).
-- - Add a CHECK constraint to prevent reintroducing invalid values.

begin;

-- Null out invalid values (conservative upper bound).
update public.practices
set surgery_count = null
where surgery_count is not null
  and (surgery_count < 1 or surgery_count > 50);

-- Prevent future bad data.
alter table public.practices
  drop constraint if exists practices_surgery_count_reasonable;

alter table public.practices
  add constraint practices_surgery_count_reasonable
  check (surgery_count is null or (surgery_count between 1 and 50));

commit;

