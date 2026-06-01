-- Read-only SQL execution RPC for the natural-language analyst chat.
--
-- The chat agent writes its own SELECT statements to analyse the full
-- `public.practices` dataset. This function is the ONLY path it uses to touch
-- the database, so it is hardened to be strictly read-only:
--   * forces a READ ONLY transaction (any write/DDL raises an error)
--   * rejects multiple statements and obvious non-SELECT statements
--   * caps rows returned and applies a short statement timeout
--   * returns rows as a JSON array
--
-- It is intentionally generic (no column whitelist) so the agent can answer
-- arbitrary analytical questions across every column/row.

create or replace function public.ddv_run_select(q text, max_rows int default 2000)
returns jsonb
language plpgsql
as $$
declare
  result jsonb;
  cleaned text;
  lowered text;
  capped int := least(greatest(coalesce(max_rows, 2000), 1), 5000);
begin
  if q is null or length(btrim(q)) = 0 then
    raise exception 'Empty query';
  end if;

  -- Normalise: drop a single trailing semicolon, then forbid any remaining ones
  -- (prevents stacked statements like "select 1; drop table ...").
  cleaned := btrim(q);
  cleaned := regexp_replace(cleaned, ';\s*$', '');
  if position(';' in cleaned) > 0 then
    raise exception 'Multiple statements are not allowed';
  end if;

  lowered := lower(cleaned);
  if left(btrim(lowered), 6) <> 'select' and left(btrim(lowered), 4) <> 'with' then
    raise exception 'Only SELECT / WITH queries are allowed';
  end if;

  -- Defence in depth (the read-only transaction below is the real guard).
  if lowered ~ '\m(insert|update|delete|drop|alter|truncate|grant|revoke|merge|vacuum|reindex|refresh|call|do|copy)\M' then
    raise exception 'Disallowed keyword detected in query';
  end if;

  -- Any attempt to write inside this transaction now errors out.
  set local transaction_read_only = on;
  set local statement_timeout = '8s';

  execute format(
    'select coalesce(jsonb_agg(row_to_json(_capped)), ''[]''::jsonb) from (select * from (%s) _inner limit %s) _capped',
    cleaned, capped
  ) into result;

  return result;
end;
$$;

comment on function public.ddv_run_select(text, int) is
  'Read-only SELECT executor for the DDV analyst chat. Forces a read-only transaction, blocks non-SELECT statements, caps rows, returns a JSON array.';
