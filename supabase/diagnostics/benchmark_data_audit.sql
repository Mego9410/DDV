-- ============================================================
-- Benchmark data audit (READ ONLY)
-- Paste into the Supabase SQL editor. Nothing here writes.
-- Goal: explain why an "average" practice reads as far above median.
--
-- Change these two lines to test a different case:
--   \set the location / surgery inline in Audit 3 (search AUDIT-3).
-- ============================================================

-- ------------------------------------------------------------
-- AUDIT 1 — column health & unit sanity (all practices)
-- For each benchmarked column: how many rows actually carry a
-- usable value, and the shape of the distribution. Watch for:
--   * low `pos` (few practices report this at all -> small sample)
--   * `suspicious_low` > 0 (values that look like £000s or a
--     partial period mixed in with full-£ figures -> skew)
--   * p50 far below what a real practice of that size earns
-- ------------------------------------------------------------
with cols(metric, col) as (
  values
    ('turnover','cert_income_gbp'),
    ('net_profit','cert_net_profit_gbp'),
    ('nhs_income','income_split_nhs_value'),
    ('fpi_income','income_split_fpi_value'),
    ('rent_income','income_split_rent_value'),
    ('uda_rate','uda_rate_gbp'),
    ('associates','cert_associates_gbp'),
    ('wages','cert_wages_gbp'),
    ('hygiene','cert_hygiene_gbp'),
    ('materials','cert_materials_gbp'),
    ('labs','cert_labs_gbp'),
    ('associate_cost','associate_cost_amount')
)
select
  c.metric,
  s.total_rows,
  s.pos                                   as usable_gt0,
  round(100.0 * s.pos / nullif(s.total_rows,0), 1) as usable_pct,
  s.suspicious_low,                       -- value in 1..9999 (unit smell for £-totals)
  s.p10, s.p50, s.p90, s.max_v
from cols c
cross join lateral (
  select
    count(*)                                                                as total_rows,
    count(*) filter (where v > 0)                                          as pos,
    count(*) filter (where v > 0 and v < 10000)                           as suspicious_low,
    percentile_cont(0.10) within group (order by v) filter (where v > 0)  as p10,
    percentile_cont(0.50) within group (order by v) filter (where v > 0)  as p50,
    percentile_cont(0.90) within group (order by v) filter (where v > 0)  as p90,
    max(v) filter (where v > 0)                                           as max_v
  from (
    select (to_jsonb(p) ->> c.col)::numeric as v from public.practices p
  ) x
) s
order by c.metric;

-- ------------------------------------------------------------
-- AUDIT 2 — surgery_count coverage
-- Size-matched comparisons only work if surgery_count is populated
-- and spread. If most rows are null or a single value, the
-- "same-size" pool is tiny/meaningless.
-- ------------------------------------------------------------
select
  coalesce(surgery_count::text, '(null)') as surgery_count,
  count(*)                                 as practices,
  count(*) filter (where cert_income_gbp > 0) as with_turnover
from public.practices
group by surgery_count
order by (surgery_count is null), surgery_count;

-- ------------------------------------------------------------
-- AUDIT 3 — cohort sample sizes for one case, per metric, as the
-- expansion ladder widens the surgery band. This is the money
-- query: it shows how many practices actually back each "local"
-- median, and the median at each band.
--   Edit :loc and :surg below.
-- ------------------------------------------------------------
-- AUDIT-3 params:
with params as (select 'Surrey'::text as loc, 4::int as surg),
cols(metric, col) as (
  values
    ('turnover','cert_income_gbp'),
    ('net_profit','cert_net_profit_gbp'),
    ('nhs_income','income_split_nhs_value'),
    ('fpi_income','income_split_fpi_value'),
    ('uda_rate','uda_rate_gbp'),
    ('associates','cert_associates_gbp'),
    ('wages','cert_wages_gbp'),
    ('hygiene','cert_hygiene_gbp'),
    ('materials','cert_materials_gbp'),
    ('labs','cert_labs_gbp')
),
bands(step, lo_off, hi_off) as (
  values (1,0,0), (2,-1,1), (3,-2,2)   -- the three "place" steps
)
select
  c.metric,
  b.step,
  format('%s..%s', p.surg + b.lo_off, p.surg + b.hi_off) as surgery_band,
  agg.n_local,
  agg.median_local
from params p
cross join cols c
cross join bands b
cross join lateral (
  select
    count(*) filter (where v > 0)                                        as n_local,
    percentile_cont(0.50) within group (order by v) filter (where v > 0) as median_local
  from (
    select (to_jsonb(pr) ->> c.col)::numeric as v
    from public.practices pr
    where (
        lower(btrim(coalesce(pr.city,'')))   = lower(p.loc)
        or lower(btrim(coalesce(pr.county,''))) = lower(p.loc)
      )
      and pr.surgery_count between (p.surg + b.lo_off) and (p.surg + b.hi_off)
  ) x
) agg
order by c.metric, b.step;

-- ------------------------------------------------------------
-- AUDIT 4 — the size effect on the national headline.
-- The report leads with "vs national" computed across ALL sizes.
-- Compare it to the median for the SAME surgery count. A big gap
-- means the all-sizes national number structurally inflates
-- larger practices (and deflates smaller ones).
--   Edit :surg below.
-- ------------------------------------------------------------
with params as (select 4::int as surg),
cols(metric, col) as (
  values
    ('turnover','cert_income_gbp'),
    ('net_profit','cert_net_profit_gbp'),
    ('associates','cert_associates_gbp'),
    ('wages','cert_wages_gbp'),
    ('materials','cert_materials_gbp'),
    ('labs','cert_labs_gbp')
)
select
  c.metric,
  nat.median_all_sizes,
  sz.n_same_size,
  sz.median_same_size,
  round((100.0 * (sz.median_same_size - nat.median_all_sizes)
        / nullif(nat.median_all_sizes,0))::numeric, 1) as same_size_vs_all_pct
from params p
cross join cols c
cross join lateral (
  select percentile_cont(0.50) within group (order by v) filter (where v > 0) as median_all_sizes
  from (select (to_jsonb(pr) ->> c.col)::numeric v from public.practices pr) x
) nat
cross join lateral (
  select
    count(*) filter (where v > 0) as n_same_size,
    percentile_cont(0.50) within group (order by v) filter (where v > 0) as median_same_size
  from (
    select (to_jsonb(pr) ->> c.col)::numeric v
    from public.practices pr
    where pr.surgery_count = p.surg
  ) x
) sz
order by c.metric;
