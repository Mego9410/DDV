-- Helpers to identify & remediate invalid surgery_count rows.
-- These are useful for tracing ingestion issues back to source files.

-- View: rows with implausible surgery_count values.
create or replace view public.invalid_surgery_count_rows as
select
  practice_key,
  display_name,
  city,
  county,
  postcode,
  surgery_count,
  source_file,
  updated_at
from public.practices
where surgery_count is not null
  and (surgery_count < 1 or surgery_count > 50)
order by surgery_count desc nulls last, updated_at desc;

comment on view public.invalid_surgery_count_rows is
  'Practices with implausible surgery_count values (likely ingestion errors).';

-- Query to fix (sets invalid values to NULL).
-- NOTE: this is intentionally provided as a commented snippet since the cleanup migration already applies it.
-- update public.practices
-- set surgery_count = null
-- where surgery_count is not null
--   and (surgery_count < 1 or surgery_count > 50);

