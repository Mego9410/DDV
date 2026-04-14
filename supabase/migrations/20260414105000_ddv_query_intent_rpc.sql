-- Server-side aggregate execution for NLQ intents.
-- Keeps raw practice rows inside Postgres.

create or replace function public.ddv_query_intent(intent jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_metric text := coalesce(intent->>'metric', 'associate_cost_amount');
  v_agg text := coalesce(intent->>'agg', 'count');
  v_filters jsonb := coalesce(intent->'filters', '[]'::jsonb);
  sql text;
  where_sql text := 'true';
  f jsonb;
  field text;
  op text;
  val jsonb;
  metric_sql text;
  agg_sql text;
  result numeric;
  result_count bigint;
begin
  -- metric column
  if v_metric = 'associate_cost_pct' then
    metric_sql := 'associate_cost_pct';
  else
    metric_sql := 'associate_cost_amount';
  end if;

  -- aggregate expression
  if v_agg = 'avg' then
    agg_sql := format('avg(%I)', metric_sql);
  elsif v_agg = 'min' then
    agg_sql := format('min(%I)', metric_sql);
  elsif v_agg = 'max' then
    agg_sql := format('max(%I)', metric_sql);
  elsif v_agg = 'median' then
    agg_sql := format('percentile_cont(0.5) within group (order by %I)', metric_sql);
  else
    agg_sql := 'count(*)';
  end if;

  -- filters
  if jsonb_typeof(v_filters) = 'array' then
    for f in select * from jsonb_array_elements(v_filters)
    loop
      field := coalesce(f->>'field', '');
      op := coalesce(f->>'op', '=');
      val := f->'value';

      if field not in ('county', 'surgery_count', 'accounts_period_end') then
        continue;
      end if;

      if op = '=' then
        where_sql := where_sql || format(' and %I = %L', field, val::text);
      elsif op = '>=' then
        where_sql := where_sql || format(' and %I >= %L', field, val::text);
      elsif op = '<=' then
        where_sql := where_sql || format(' and %I <= %L', field, val::text);
      elsif op = 'in' then
        -- expects array; if not, treat as single
        if jsonb_typeof(val) = 'array' then
          where_sql := where_sql || format(
            ' and %I = any (array(select jsonb_array_elements_text(%L::jsonb)))',
            field,
            val::text
          );
        else
          where_sql := where_sql || format(' and %I = %L', field, val::text);
        end if;
      elsif op = 'between' then
        if jsonb_typeof(val) = 'array' and jsonb_array_length(val) = 2 then
          where_sql := where_sql || format(
            ' and %I >= %L and %I <= %L',
            field, (val->>0),
            field, (val->>1)
          );
        end if;
      end if;
    end loop;
  end if;

  if v_agg = 'count' then
    sql := format('select %s from public.practices where %s', agg_sql, where_sql);
    execute sql into result_count;
    return jsonb_build_object('value', result_count);
  else
    sql := format(
      'select %s from public.practices where %s and %I is not null',
      agg_sql, where_sql, metric_sql
    );
    execute sql into result;
    return jsonb_build_object('value', result);
  end if;
end;
$$;

revoke all on function public.ddv_query_intent(jsonb) from public;

