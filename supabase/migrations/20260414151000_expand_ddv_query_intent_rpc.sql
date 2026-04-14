-- Expand NLQ intent aggregate execution for DDV analytics.
-- Keeps raw practice rows inside Postgres; returns aggregates only.
--
-- Supported metrics (intent.metric):
-- - surgery_count
-- - turnover_gbp (cert_income_gbp)
-- - associate_cost_amount
-- - associate_cost_pct
-- - cert_associates_gbp
-- - cert_associates_percent
--
-- Supported filters (intent.filters[].field):
-- - county, city, postcode
-- - surgery_count
-- - accounts_period_end
-- - visited_on
--
-- Supported aggs (intent.agg): avg|min|max|median|count

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
  val_text text;

  metric_col text;
  agg_sql text;

  value_numeric numeric;
  n_total bigint;
  n_nonnull bigint;
begin
  -- Metric column mapping (whitelist)
  if v_metric = 'associate_cost_pct' then
    metric_col := 'associate_cost_pct';
  elsif v_metric = 'associate_cost_amount' then
    metric_col := 'associate_cost_amount';
  elsif v_metric = 'surgery_count' then
    metric_col := 'surgery_count';
  elsif v_metric = 'turnover_gbp' then
    metric_col := 'cert_income_gbp';
  elsif v_metric = 'cert_associates_gbp' then
    metric_col := 'cert_associates_gbp';
  elsif v_metric = 'cert_associates_percent' then
    metric_col := 'cert_associates_percent';
  else
    -- Unknown metric -> safe default (count)
    metric_col := null;
    v_agg := 'count';
  end if;

  -- Aggregate expression
  if v_agg = 'avg' then
    agg_sql := format('avg(%I)', metric_col);
  elsif v_agg = 'min' then
    agg_sql := format('min(%I)', metric_col);
  elsif v_agg = 'max' then
    agg_sql := format('max(%I)', metric_col);
  elsif v_agg = 'median' then
    agg_sql := format('percentile_cont(0.5) within group (order by %I)', metric_col);
  else
    agg_sql := 'count(*)';
    v_agg := 'count';
  end if;

  -- Filters (whitelist fields + ops)
  if jsonb_typeof(v_filters) = 'array' then
    for f in select * from jsonb_array_elements(v_filters)
    loop
      field := coalesce(f->>'field', '');
      op := coalesce(f->>'op', '=');
      val := f->'value';
      val_text := val #>> '{}';

      if field not in (
        'county',
        'city',
        'postcode',
        'surgery_count',
        'accounts_period_end',
        'visited_on'
      ) then
        continue;
      end if;

      if op = '=' then
        if field in ('accounts_period_end', 'visited_on') then
          where_sql := where_sql || format(' and %I = %L::date', field, val_text);
        elsif field = 'surgery_count' then
          where_sql := where_sql || format(' and %I = %L::int', field, val_text);
        else
          where_sql := where_sql || format(' and %I = %L', field, val_text);
        end if;

      elsif op = '>=' then
        if field in ('accounts_period_end', 'visited_on') then
          where_sql := where_sql || format(' and %I >= %L::date', field, val_text);
        elsif field = 'surgery_count' then
          where_sql := where_sql || format(' and %I >= %L::int', field, val_text);
        else
          where_sql := where_sql || format(' and %I >= %L', field, val_text);
        end if;

      elsif op = '<=' then
        if field in ('accounts_period_end', 'visited_on') then
          where_sql := where_sql || format(' and %I <= %L::date', field, val_text);
        elsif field = 'surgery_count' then
          where_sql := where_sql || format(' and %I <= %L::int', field, val_text);
        else
          where_sql := where_sql || format(' and %I <= %L', field, val_text);
        end if;

      elsif op = 'in' then
        -- expects array; if not, treat as single
        if jsonb_typeof(val) = 'array' then
          where_sql := where_sql || format(
            ' and %I = any (array(select jsonb_array_elements_text(%L::jsonb)))',
            field,
            val::text
          );
        else
          if field in ('accounts_period_end', 'visited_on') then
            where_sql := where_sql || format(' and %I = %L::date', field, val_text);
          elsif field = 'surgery_count' then
            where_sql := where_sql || format(' and %I = %L::int', field, val_text);
          else
            where_sql := where_sql || format(' and %I = %L', field, val_text);
          end if;
        end if;

      elsif op = 'between' then
        if jsonb_typeof(val) = 'array' and jsonb_array_length(val) = 2 then
          if field in ('accounts_period_end', 'visited_on') then
            where_sql := where_sql || format(
              ' and %I >= %L::date and %I <= %L::date',
              field, (val->>0),
              field, (val->>1)
            );
          elsif field = 'surgery_count' then
            where_sql := where_sql || format(
              ' and %I >= %L::int and %I <= %L::int',
              field, (val->>0),
              field, (val->>1)
            );
          else
            where_sql := where_sql || format(
              ' and %I >= %L and %I <= %L',
              field, (val->>0),
              field, (val->>1)
            );
          end if;
        end if;
      end if;
    end loop;
  end if;

  -- Total rows matching filters
  sql := format('select count(*) from public.practices where %s', where_sql);
  execute sql into n_total;

  -- Count-only or unknown metric
  if v_agg = 'count' or metric_col is null then
    return jsonb_build_object(
      'metric', v_metric,
      'agg', 'count',
      'value', n_total,
      'n', n_total,
      'null_excluded', 0
    );
  end if;

  -- Aggregate + n_nonnull (excluding nulls for the metric)
  sql := format(
    'select %s, count(*) from public.practices where %s and %I is not null',
    agg_sql,
    where_sql,
    metric_col
  );
  execute sql into value_numeric, n_nonnull;

  return jsonb_build_object(
    'metric', v_metric,
    'agg', v_agg,
    'value', value_numeric,
    'n', n_nonnull,
    'null_excluded', (n_total - n_nonnull)
  );
end;
$$;

revoke all on function public.ddv_query_intent(jsonb) from public;

