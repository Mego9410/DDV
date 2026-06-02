-- Data cleanup: null out clearly-corrupt account period-end dates.
--
-- The source spreadsheets contain parse/typo errors that produced impossible
-- years (e.g. 0500-08-14, 1242-04-14, 2711-04-14). These poison any date-based
-- aggregate. We null them so analytics ignore them. The original values remain
-- available in practices.raw_json, so this is recoverable.
--
-- Sane range: 2000-01-01 .. 2027-12-31 (dataset is latest-only historical
-- accounts; the newest legitimate period end observed is 2026-08-23).

update public.practices
set accounts_period_end = null
where accounts_period_end is not null
  and (accounts_period_end < date '2000-01-01' or accounts_period_end > date '2027-12-31');

update public.practices
set certified_accounts_period_end_prev = null
where certified_accounts_period_end_prev is not null
  and (certified_accounts_period_end_prev < date '2000-01-01' or certified_accounts_period_end_prev > date '2027-12-31');
