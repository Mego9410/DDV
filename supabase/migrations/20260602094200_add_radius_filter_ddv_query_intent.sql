-- Add radius (near) filters to ddv_query_intent for NLQ chat:
-- {"field":"near","op":"within_miles","value":{"place":"Brighton","radius_miles":30}}
-- Also supports passing coordinates directly:
-- {"field":"near","op":"within_miles","value":{"lat":51.540,"lng":0.023,"radius_miles":20}}
--
-- Requires postgis + practices.geog (see 20260602093500_add_geospatial_support.sql)
-- and postcode_geocode cache (populated by backfill script).

create or replace function public.ddv_query_intent(intent jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_metric text := coalesce(intent->>'metric', 'associate_cost_amount');
  v_agg text := lower(coalesce(intent->>'agg', 'count'));
  v_filters jsonb := coalesce(intent->'filters', '[]'::jsonb);
  v_group_by jsonb := coalesce(intent->'group_by', '[]'::jsonb);
  v_limit int := greatest(1, least(coalesce(nullif(intent->>'limit','')::int, 15), 1000));
  v_order_by_by text := coalesce(intent #>> '{order_by,by}', 'value');
  v_order_by_dir text := lower(coalesce(intent #>> '{order_by,dir}', 'desc'));

  sql text;
  where_sql text := 'true';

  f jsonb;
  field text;
  op text;
  val jsonb;
  val_text text;

  metric_col text;
  agg_sql text;

  -- geo radius filter
  near_place text;
  near_radius_miles double precision;
  near_radius_meters double precision;
  center_geog geography(Point,4326);
  center_lat double precision;
  center_lng double precision;

  group_cols_sql text := '';
  group_by_arr text[] := '{}'::text[];
  group_json_sql text := '';
  order_sql text := '';

  value_numeric numeric;
  result_json jsonb;
  n_total bigint := 0;
  n_nonnull_total bigint := 0;
  n_nonnull bigint := 0;
begin
  -- Metric column mapping (whitelist).
  -- Note: metric_col = null indicates "count-only" semantics (practice_count).
  if v_metric in ('practice_count', 'count', 'practices') then
    metric_col := null;
    v_agg := 'count';
  elsif v_metric = 'associate_cost_pct' then
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
  -- Valuation metrics
  elsif v_metric = 'grand_total' then
    metric_col := 'grand_total';
  elsif v_metric = 'goodwill' then
    metric_col := 'goodwill';
  elsif v_metric = 'efandf' then
    metric_col := 'efandf';
  elsif v_metric = 'total' then
    metric_col := 'total';
  elsif v_metric = 'freehold' then
    metric_col := 'freehold';
  else
    -- Unknown metric -> safe default (count)
    metric_col := null;
    v_agg := 'count';
  end if;

  -- Aggregate expression
  if v_agg = 'avg' then
    if metric_col is null then
      agg_sql := 'count(*)';
      v_agg := 'count';
    else
      agg_sql := format('avg(%I)', metric_col);
    end if;
  elsif v_agg = 'min' then
    if metric_col is null then
      agg_sql := 'count(*)';
      v_agg := 'count';
    else
      agg_sql := format('min(%I)', metric_col);
    end if;
  elsif v_agg = 'max' then
    if metric_col is null then
      agg_sql := 'count(*)';
      v_agg := 'count';
    else
      agg_sql := format('max(%I)', metric_col);
    end if;
  elsif v_agg = 'median' then
    if metric_col is null then
      agg_sql := 'count(*)';
      v_agg := 'count';
    else
      agg_sql := format('percentile_cont(0.5) within group (order by %I)', metric_col);
    end if;
  elsif v_agg = 'sum' then
    if metric_col is null then
      agg_sql := 'count(*)';
      v_agg := 'count';
    else
      agg_sql := format('sum(%I)', metric_col);
    end if;
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
        'near',
        'surgery_count',
        'accounts_period_end',
        'visited_on'
      ) then
        continue;
      end if;

      -- Radius filter: within_miles
      if field = 'near' and op = 'within_miles' then
        near_place := nullif(btrim(val #>> '{place}'), '');
        near_radius_miles := nullif((val #>> '{radius_miles}')::double precision, 0);
        if near_place is null or near_radius_miles is null or near_radius_miles <= 0 then
          -- If no place was provided, allow passing coordinates directly.
          near_radius_miles := nullif((val #>> '{radius_miles}')::double precision, 0);
          center_lat := nullif((val #>> '{lat}')::double precision, 0);
          center_lng := nullif((val #>> '{lng}')::double precision, 0);
          if near_radius_miles is null or near_radius_miles <= 0 or center_lat is null or center_lng is null then
            where_sql := where_sql || ' and false';
            continue;
          end if;
        end if;

        near_radius_meters := near_radius_miles * 1609.344;

        -- Resolve center by postcode first (cached), otherwise by place name centroid
        if center_lat is null or center_lng is null then
          center_lat := null;
          center_lng := null;
        end if;

        if (center_lat is null or center_lng is null) and near_place is not null and near_place ~* '^[A-Z]{1,2}\\d[A-Z\\d]?\\s*\\d[A-Z]{2}$' then
          select g.lat, g.lng into center_lat, center_lng
          from public.postcode_geocode g
          where g.postcode = regexp_replace(upper(btrim(near_place)), '\\s+', '', 'g')
          limit 1;
        end if;

        if (center_lat is null or center_lng is null) and near_place is not null then
          -- Centroid from practices that match the place in either city or county.
          select avg(p.lat), avg(p.lng) into center_lat, center_lng
          from public.practices p
          where p.lat is not null and p.lng is not null
            and (
              lower(btrim(p.city)) = lower(btrim(near_place))
              or lower(btrim(p.county)) = lower(btrim(near_place))
              or btrim(coalesce(p.city,'')) ilike '%' || btrim(near_place) || '%'
              or btrim(coalesce(p.county,'')) ilike '%' || btrim(near_place) || '%'
            );
        end if;

        if center_lat is null or center_lng is null then
          where_sql := where_sql || ' and false';
          continue;
        end if;

        center_geog := st_setsrid(st_makepoint(center_lng, center_lat), 4326)::geography;
        where_sql := where_sql || format(' and geog is not null and st_dwithin(geog, %L::geography, %s)', center_geog::text, near_radius_meters);
        continue;
      end if;

      if op = '=' then
        if field in ('accounts_period_end', 'visited_on') then
          where_sql := where_sql || format(' and %I = %L::date', field, val_text);
        elsif field = 'surgery_count' then
          where_sql := where_sql || format(' and %I = %L::int', field, val_text);
        elsif field in ('city', 'county') then
          where_sql := where_sql || format(
            ' and (lower(btrim(city)) = lower(btrim(%L)) or lower(btrim(county)) = lower(btrim(%L)))',
            val_text,
            val_text
          );
        elsif field = 'postcode' then
          where_sql := where_sql || format(
            E' and regexp_replace(upper(btrim(%I)), ''\\s+'', '''', ''g'') = regexp_replace(upper(btrim(%L)), ''\\s+'', '''', ''g'')',
            field,
            val_text
          );
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
          if field in ('city', 'county') then
            where_sql := where_sql || format(
              ' and exists (select 1 from jsonb_array_elements_text(%L::jsonb) as x(v) where lower(btrim(city)) = lower(btrim(x.v)) or lower(btrim(county)) = lower(btrim(x.v)))',
              val::text
            );
          elsif field = 'postcode' then
            where_sql := where_sql || format(
              E' and exists (select 1 from jsonb_array_elements_text(%L::jsonb) as x(v) where regexp_replace(upper(btrim(%I)), ''\\s+'', '''', ''g'') = regexp_replace(upper(btrim(x.v)), ''\\s+'', '''', ''g''))',
              val::text,
              field
            );
          else
            where_sql := where_sql || format(
              ' and %I = any (array(select jsonb_array_elements_text(%L::jsonb)))',
              field,
              val::text
            );
          end if;
        else
          if field in ('accounts_period_end', 'visited_on') then
            where_sql := where_sql || format(' and %I = %L::date', field, val_text);
          elsif field = 'surgery_count' then
            where_sql := where_sql || format(' and %I = %L::int', field, val_text);
          elsif field in ('city', 'county') then
            where_sql := where_sql || format(
              ' and (lower(btrim(city)) = lower(btrim(%L)) or lower(btrim(county)) = lower(btrim(%L)))',
              val_text,
              val_text
            );
          elsif field = 'postcode' then
            where_sql := where_sql || format(
              E' and regexp_replace(upper(btrim(%I)), ''\\s+'', '''', ''g'') = regexp_replace(upper(btrim(%L)), ''\\s+'', '''', ''g'')',
              field,
              val_text
            );
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

  -- Totals
  sql := format('select count(*) from public.practices where %s', where_sql);
  execute sql into n_total;

  if metric_col is not null and v_agg <> 'count' then
    sql := format('select count(*) from public.practices where %s and %I is not null', where_sql, metric_col);
    execute sql into n_nonnull_total;
  else
    n_nonnull_total := n_total;
  end if;

  -- Group-by parsing (unchanged; whitelist)
  if jsonb_typeof(v_group_by) = 'array' and jsonb_array_length(v_group_by) > 0 then
    for field in
      select value from jsonb_array_elements_text(v_group_by)
    loop
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

      group_by_arr := array_append(group_by_arr, field);
      group_cols_sql := case when group_cols_sql = '' then format('%I', field) else group_cols_sql || ', ' || format('%I', field) end;
      group_json_sql := case
        when group_json_sql = '' then format('jsonb_build_object(%L, %I)', field, field)
        else group_json_sql || ' || ' || format('jsonb_build_object(%L, %I)', field, field)
      end;
    end loop;
  end if;

  -- order_by validation
  if v_order_by_dir not in ('asc', 'desc') then
    v_order_by_dir := 'desc';
  end if;

  if group_cols_sql <> '' then
    if v_order_by_by is null or v_order_by_by = '' then
      v_order_by_by := 'value';
    end if;

    if v_order_by_by = 'value' then
      order_sql := format('value %s', v_order_by_dir);
    elsif v_order_by_by = any(group_by_arr) then
      order_sql := format('%I %s', v_order_by_by, v_order_by_dir);
    else
      order_sql := 'value desc';
    end if;

    -- Grouped execution
    if v_agg = 'count' or metric_col is null then
      sql := format(
        'with grp as (
           select %s, count(*)::numeric as value, count(*)::bigint as n
           from public.practices
           where %s
           group by %s
         )
         select jsonb_build_object(
           %L, %L,
           %L, %L,
           %L, to_jsonb(%L::text[]),
           %L, %s,
           %L, %s,
           %L, %s
         )
         from (
           select coalesce(jsonb_agg(jsonb_build_object(%L, (%s), %L, value, %L, n) order by %s), %L::jsonb) as rows
           from (select * from grp order by %s limit %s) t
         ) x',
        group_cols_sql,
        where_sql,
        group_cols_sql,
        'metric', v_metric,
        'agg', 'count',
        'group_by', group_by_arr,
        'n_total', n_total,
        'n', n_total,
        'null_excluded', 0,
        'group', group_json_sql,
        'value',
        'n',
        order_sql,
        '[]',
        order_sql, v_limit
      );
      execute sql into result_json;
      return result_json;
    else
      sql := format(
        'with grp as (
           select %s, (%s)::numeric as value, count(*)::bigint as n
           from public.practices
           where %s and %I is not null
           group by %s
         )
         select jsonb_build_object(
           %L, %L,
           %L, %L,
           %L, to_jsonb(%L::text[]),
           %L, %s,
           %L, %s,
           %L, %s
         )
         from (
           select
             coalesce(jsonb_agg(jsonb_build_object(%L, (%s), %L, value, %L, n) order by %s), %L::jsonb) as rows
           from (select * from grp order by %s limit %s) t
         ) x',
        group_cols_sql,
        agg_sql,
        where_sql,
        metric_col,
        group_cols_sql,
        'metric', v_metric,
        'agg', v_agg,
        'group_by', group_by_arr,
        'n_total', n_total,
        'n', n_nonnull_total,
        'null_excluded', (n_total - n_nonnull_total),
        'group', group_json_sql,
        'value',
        'n',
        order_sql,
        '[]',
        order_sql, v_limit
      );
      execute sql into result_json;
      return result_json;
    end if;
  end if;

  -- Ungrouped execution
  if v_agg = 'count' or metric_col is null then
    return jsonb_build_object(
      'metric', v_metric,
      'agg', 'count',
      'value', n_total,
      'n', n_total,
      'null_excluded', 0
    );
  end if;

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

