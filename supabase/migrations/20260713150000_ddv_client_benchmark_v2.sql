-- Client benchmark, reconciled.
--
-- Builds on 20260713140000 (expand_radius): keeps the per-metric local
-- ladder (place -> widening radius -> national same-size) so a client is
-- never stranded on "not enough local peers", and adds two fixes:
--
--   1. SIZE-MATCHED national. The national headline is compared against
--      practices of a similar size (widening the surgery band only if the
--      exact size is thin, then all sizes as a last resort) instead of the
--      whole population. An average multi-surgery practice no longer reads
--      as far above a median dominated by single-surgery practices.
--   2. OUTLIER GUARDS. Per-metric sanity bounds exclude obvious data errors
--      before every median (e.g. a UDA rate of £1,102, a £ total under £250).
--
-- This function is timestamped after 140000 so it is the final definition.
-- Aggregate-only: returns medians and deltas, never sample counts.

create or replace function public.ddv_client_benchmark(payload jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_location text := btrim(coalesce(payload->>'location', ''));
  v_surgery int := nullif(payload->>'surgery_count', '')::int;
  v_lat double precision := nullif(payload->>'lat', '')::double precision;
  v_lng double precision := nullif(payload->>'lng', '')::double precision;
  v_metrics jsonb := coalesce(payload->'metrics', '[]'::jsonb);
  v_is_13_plus boolean;

  -- Minimum peers required to show a local median.
  c_min_n constant bigint := 5;
  -- Target sample for a size-matched national median before widening.
  c_nat_target constant bigint := 20;

  v_metric jsonb;
  v_id text;
  v_col text;
  v_your numeric;

  -- per-metric sanity bounds (outlier guards)
  v_lo numeric;
  v_hi numeric;

  v_step int;
  v_best_step int := 1;
  v_mode text;
  v_surg_min int;
  v_surg_max int;
  v_radius_miles double precision;
  v_center geography(Point,4326);

  v_where text;
  v_sql text;
  v_n bigint;
  v_median numeric;
  v_min_n bigint;
  v_all_displayable boolean;

  v_geo_missing bigint := 0;
  v_geo_unresolved boolean := false;

  metric_results jsonb := '[]'::jsonb;
  national_medians jsonb := '{}'::jsonb;
  national_ns jsonb := '{}'::jsonb;
  same_size_medians jsonb := '{}'::jsonb;
  same_size_ns jsonb := '{}'::jsonb;
  local_medians jsonb := '{}'::jsonb;
  local_ns jsonb := '{}'::jsonb;

  step_local_medians jsonb;
  step_local_ns jsonb;

  cohort_label text;
  surg_label text;
  pct_nat numeric;
  pct_loc numeric;
  local_suppressed boolean;
  local_obj jsonb;
  row_obj jsonb;

  final_surg_min int;
  final_surg_max int;
  final_mode text;
  final_radius double precision;

  -- Max expansion steps (place bands + radii + national same-size fallback)
  c_max_step constant int := 14;
begin
  if v_location = '' then
    raise exception 'location is required';
  end if;
  if v_surgery is null or v_surgery < 1 or v_surgery > 50 then
    raise exception 'surgery_count must be between 1 and 50';
  end if;
  if jsonb_typeof(v_metrics) <> 'array' or jsonb_array_length(v_metrics) = 0 then
    raise exception 'metrics array is required';
  end if;
  if jsonb_array_length(v_metrics) > 12 then
    raise exception 'at most 12 metrics allowed';
  end if;

  v_is_13_plus := v_surgery >= 13;

  -- ---------------------------------------------------------------
  -- NATIONAL, size-matched + guarded.
  -- ---------------------------------------------------------------
  for v_metric in select * from jsonb_array_elements(v_metrics)
  loop
    v_id := lower(btrim(coalesce(v_metric->>'id', '')));
    v_col := public.ddv_client_metric_column(v_id);
    if v_col is null then
      raise exception 'unknown metric id: %', v_id;
    end if;
    begin
      v_your := (v_metric->>'value')::numeric;
    exception when others then
      raise exception 'invalid value for metric %', v_id;
    end;
    if v_your is null or not (v_your = v_your) then
      raise exception 'value required for metric %', v_id;
    end if;

    -- UDA rate is a per-UDA rate (~£15-40), not a £ total.
    if v_id = 'uda_rate' then
      v_lo := 5; v_hi := 150;
    else
      v_lo := 250; v_hi := 25000000;
    end if;

    v_median := null;
    v_n := 0;
    -- Widen the surgery band until the size-matched pool is big enough.
    for v_step in 1..4 loop
      if v_is_13_plus then
        v_surg_min := greatest(1, 13 - (v_step - 1));
        v_surg_max := 50;
      else
        v_surg_min := greatest(1, v_surgery - (v_step - 1));
        v_surg_max := least(50, v_surgery + (v_step - 1));
      end if;
      execute format(
        $q$
          select count(%1$I)::bigint,
                 percentile_cont(0.5) within group (order by %1$I)
          from public.practices
          where surgery_count between %2$s and %3$s
            and %1$I is not null and %1$I >= %4$s and %1$I <= %5$s
        $q$,
        v_col, v_surg_min, v_surg_max, v_lo, v_hi
      ) into v_n, v_median;
      exit when v_median is not null and v_n >= c_nat_target;
    end loop;

    -- Last resort: all sizes (still guarded) if size-matched stays thin.
    if v_median is null or v_n < c_nat_target then
      execute format(
        $q$
          select count(%1$I)::bigint,
                 percentile_cont(0.5) within group (order by %1$I)
          from public.practices
          where %1$I is not null and %1$I >= %2$s and %1$I <= %3$s
        $q$,
        v_col, v_lo, v_hi
      ) into v_n, v_median;
    end if;

    national_medians := national_medians || jsonb_build_object(v_id, v_median);
    national_ns := national_ns || jsonb_build_object(v_id, v_n);
  end loop;

  if v_lat is not null and v_lng is not null
     and v_lat = v_lat and v_lng = v_lng
     and v_lat between -90 and 90 and v_lng between -180 and 180 then
    v_center := st_setsrid(st_makepoint(v_lng, v_lat), 4326)::geography;
  else
    v_geo_unresolved := true;
    v_center := null;
  end if;

  -- ---------------------------------------------------------------
  -- LOCAL ladder: place -> widening radius -> national same-size.
  -- ---------------------------------------------------------------
  for v_step in 1..c_max_step loop
    if v_is_13_plus then
      case v_step
        when 1 then v_mode := 'place';  v_surg_min := 13; v_surg_max := 50; v_radius_miles := null;
        when 2 then v_mode := 'place';  v_surg_min := 12; v_surg_max := 50; v_radius_miles := null;
        when 3 then v_mode := 'place';  v_surg_min := 11; v_surg_max := 50; v_radius_miles := null;
        when 4 then v_mode := 'radius'; v_surg_min := 12; v_surg_max := 50; v_radius_miles := 25;
        when 5 then v_mode := 'radius'; v_surg_min := 11; v_surg_max := 50; v_radius_miles := 50;
        when 6 then v_mode := 'radius'; v_surg_min := 11; v_surg_max := 50; v_radius_miles := 75;
        when 7 then v_mode := 'radius'; v_surg_min := 11; v_surg_max := 50; v_radius_miles := 100;
        when 8 then v_mode := 'radius'; v_surg_min := 11; v_surg_max := 50; v_radius_miles := 150;
        when 9 then v_mode := 'radius'; v_surg_min := 11; v_surg_max := 50; v_radius_miles := 200;
        when 10 then v_mode := 'radius'; v_surg_min := 1; v_surg_max := 50; v_radius_miles := 100;
        when 11 then v_mode := 'radius'; v_surg_min := 1; v_surg_max := 50; v_radius_miles := 150;
        when 12 then v_mode := 'radius'; v_surg_min := 1; v_surg_max := 50; v_radius_miles := 200;
        when 13 then v_mode := 'radius'; v_surg_min := 1; v_surg_max := 50; v_radius_miles := 300;
        else
          v_mode := 'same_size';
          v_surg_min := 11;
          v_surg_max := 50;
          v_radius_miles := null;
      end case;
    else
      case v_step
        when 1 then
          v_mode := 'place';
          v_surg_min := v_surgery;
          v_surg_max := v_surgery;
          v_radius_miles := null;
        when 2 then
          v_mode := 'place';
          v_surg_min := greatest(1, v_surgery - 1);
          v_surg_max := least(50, v_surgery + 1);
          v_radius_miles := null;
        when 3 then
          v_mode := 'place';
          v_surg_min := greatest(1, v_surgery - 2);
          v_surg_max := least(50, v_surgery + 2);
          v_radius_miles := null;
        when 4 then
          v_mode := 'radius';
          v_surg_min := greatest(1, v_surgery - 1);
          v_surg_max := least(50, v_surgery + 1);
          v_radius_miles := 25;
        when 5 then
          v_mode := 'radius';
          v_surg_min := greatest(1, v_surgery - 2);
          v_surg_max := least(50, v_surgery + 2);
          v_radius_miles := 50;
        when 6 then
          v_mode := 'radius';
          v_surg_min := greatest(1, v_surgery - 2);
          v_surg_max := least(50, v_surgery + 2);
          v_radius_miles := 75;
        when 7 then
          v_mode := 'radius';
          v_surg_min := greatest(1, v_surgery - 2);
          v_surg_max := least(50, v_surgery + 2);
          v_radius_miles := 100;
        when 8 then
          v_mode := 'radius';
          v_surg_min := greatest(1, v_surgery - 2);
          v_surg_max := least(50, v_surgery + 2);
          v_radius_miles := 150;
        when 9 then
          v_mode := 'radius';
          v_surg_min := greatest(1, v_surgery - 2);
          v_surg_max := least(50, v_surgery + 2);
          v_radius_miles := 200;
        when 10 then
          v_mode := 'radius';
          v_surg_min := 1;
          v_surg_max := 50;
          v_radius_miles := 100;
        when 11 then
          v_mode := 'radius';
          v_surg_min := 1;
          v_surg_max := 50;
          v_radius_miles := 150;
        when 12 then
          v_mode := 'radius';
          v_surg_min := 1;
          v_surg_max := 50;
          v_radius_miles := 200;
        when 13 then
          v_mode := 'radius';
          v_surg_min := 1;
          v_surg_max := 50;
          v_radius_miles := 300;
        else
          v_mode := 'same_size';
          v_surg_min := greatest(1, v_surgery - 2);
          v_surg_max := least(50, v_surgery + 2);
          v_radius_miles := null;
      end case;
    end if;

    if v_mode = 'radius' and v_center is null then
      continue;
    end if;

    if v_mode = 'place' then
      v_where := format(
        $w$
          (
            lower(btrim(coalesce(city, ''))) = lower(%L)
            or lower(btrim(coalesce(county, ''))) = lower(%L)
          )
          and surgery_count between %s and %s
        $w$,
        v_location, v_location, v_surg_min, v_surg_max
      );
    elsif v_mode = 'radius' then
      v_where := format(
        $w$
          geog is not null
          and st_dwithin(
            geog,
            st_setsrid(st_makepoint(%s, %s), 4326)::geography,
            %s * 1609.344
          )
          and surgery_count between %s and %s
        $w$,
        v_lng, v_lat, v_radius_miles, v_surg_min, v_surg_max
      );
    else
      v_where := format(
        $w$
          surgery_count between %s and %s
        $w$,
        v_surg_min, v_surg_max
      );
    end if;

    step_local_medians := '{}'::jsonb;
    step_local_ns := '{}'::jsonb;
    v_all_displayable := true;
    v_min_n := null;

    for v_metric in select * from jsonb_array_elements(v_metrics)
    loop
      v_id := lower(btrim(coalesce(v_metric->>'id', '')));
      v_col := public.ddv_client_metric_column(v_id);
      if v_id = 'uda_rate' then v_lo := 5; v_hi := 150; else v_lo := 250; v_hi := 25000000; end if;

      v_sql := format(
        $q$
          select
            count(%1$I)::bigint,
            percentile_cont(0.5) within group (order by %1$I)
          from public.practices
          where (%2$s)
            and %1$I is not null and %1$I >= %3$s and %1$I <= %4$s
        $q$,
        v_col,
        v_where,
        v_lo,
        v_hi
      );
      execute v_sql into v_n, v_median;
      step_local_medians := step_local_medians || jsonb_build_object(v_id, v_median);
      step_local_ns := step_local_ns || jsonb_build_object(v_id, v_n);

      if v_min_n is null or v_n < v_min_n then
        v_min_n := v_n;
      end if;
      if v_n < c_min_n then
        v_all_displayable := false;
      end if;
    end loop;

    local_medians := step_local_medians;
    local_ns := step_local_ns;
    final_surg_min := v_surg_min;
    final_surg_max := v_surg_max;
    final_mode := v_mode;
    final_radius := v_radius_miles;
    v_best_step := v_step;

    if v_all_displayable then
      exit;
    end if;
  end loop;

  if final_mode is null then
    final_mode := 'same_size';
    final_surg_min := case when v_is_13_plus then 11 else greatest(1, v_surgery - 2) end;
    final_surg_max := case when v_is_13_plus then 50 else least(50, v_surgery + 2) end;
    final_radius := null;
  end if;

  if final_mode = 'radius' and v_center is not null then
    select count(*)::bigint into v_geo_missing
    from public.practices
    where geog is null
      and surgery_count between final_surg_min and final_surg_max;
  end if;

  -- National same-size medians for the final band (guarded).
  for v_metric in select * from jsonb_array_elements(v_metrics)
  loop
    v_id := lower(btrim(coalesce(v_metric->>'id', '')));
    v_col := public.ddv_client_metric_column(v_id);
    if v_id = 'uda_rate' then v_lo := 5; v_hi := 150; else v_lo := 250; v_hi := 25000000; end if;
    v_sql := format(
      $q$
        select
          count(%1$I)::bigint,
          percentile_cont(0.5) within group (order by %1$I)
        from public.practices
        where surgery_count between %2$s and %3$s
          and %1$I is not null and %1$I >= %4$s and %1$I <= %5$s
      $q$,
      v_col, final_surg_min, final_surg_max, v_lo, v_hi
    );
    execute v_sql into v_n, v_median;
    same_size_medians := same_size_medians || jsonb_build_object(v_id, v_median);
    same_size_ns := same_size_ns || jsonb_build_object(v_id, v_n);
  end loop;

  -- Per-metric safety net: if still thin, use national same-size, then national.
  for v_metric in select * from jsonb_array_elements(v_metrics)
  loop
    v_id := lower(btrim(coalesce(v_metric->>'id', '')));
    v_n := coalesce((local_ns->>v_id)::bigint, 0);
    if v_n < c_min_n then
      local_medians := local_medians || jsonb_build_object(v_id, (same_size_medians->>v_id)::numeric);
      local_ns := local_ns || jsonb_build_object(v_id, coalesce((same_size_ns->>v_id)::bigint, 0));
    end if;
    v_n := coalesce((local_ns->>v_id)::bigint, 0);
    if v_n < c_min_n or (local_medians->>v_id) is null then
      local_medians := local_medians || jsonb_build_object(v_id, (national_medians->>v_id)::numeric);
      local_ns := local_ns || jsonb_build_object(v_id, coalesce((national_ns->>v_id)::bigint, 0));
    end if;
  end loop;

  if final_surg_min = final_surg_max then
    surg_label := format('%s surgery', final_surg_min);
  elsif final_surg_max >= 50 and final_surg_min >= 11 then
    surg_label := format('%s+ surgery', final_surg_min);
  else
    surg_label := format('%s–%s surgery', final_surg_min, final_surg_max);
  end if;

  if final_mode = 'radius' then
    cohort_label := format(
      '%s practices within %s miles of %s',
      surg_label, final_radius::int, v_location
    );
  elsif final_mode = 'same_size' then
    cohort_label := format('National %s peer group (local sample too thin)', surg_label);
  else
    cohort_label := format('%s practices in %s', surg_label, v_location);
  end if;

  for v_metric in select * from jsonb_array_elements(v_metrics)
  loop
    v_id := lower(btrim(coalesce(v_metric->>'id', '')));
    v_your := (v_metric->>'value')::numeric;
    v_n := coalesce((local_ns->>v_id)::bigint, 0);
    local_suppressed := v_n < 1 or (local_medians->>v_id) is null;

    pct_nat := null;
    if (national_medians->>v_id) is not null
       and (national_medians->>v_id)::numeric <> 0 then
      pct_nat := round(
        ((v_your - (national_medians->>v_id)::numeric)
          / (national_medians->>v_id)::numeric) * 100,
        1
      );
    end if;

    pct_loc := null;
    if not local_suppressed
       and (local_medians->>v_id) is not null
       and (local_medians->>v_id)::numeric <> 0 then
      pct_loc := round(
        ((v_your - (local_medians->>v_id)::numeric)
          / (local_medians->>v_id)::numeric) * 100,
        1
      );
    end if;

    if local_suppressed then
      local_obj := null;
    else
      local_obj := jsonb_build_object(
        'median', (local_medians->>v_id)::numeric,
        'n', v_n
      );
    end if;

    row_obj := jsonb_build_object(
      'id', v_id,
      'your_value', v_your,
      'national', jsonb_build_object(
        'median', (national_medians->>v_id)::numeric,
        'n', coalesce((national_ns->>v_id)::bigint, 0)
      ),
      'national_same_size', jsonb_build_object(
        'median', (same_size_medians->>v_id)::numeric,
        'n', coalesce((same_size_ns->>v_id)::bigint, 0)
      ),
      'local', local_obj,
      'pct_vs_national', pct_nat,
      'pct_vs_local', pct_loc,
      'local_suppressed', local_suppressed
    );
    metric_results := metric_results || jsonb_build_array(row_obj);
  end loop;

  return jsonb_build_object(
    'cohort', jsonb_build_object(
      'location', v_location,
      'mode', final_mode,
      'radius_miles', final_radius,
      'surgery_min', final_surg_min,
      'surgery_max', final_surg_max,
      'requested_surgery_count', v_surgery,
      'expansion_step', v_best_step,
      'label', cohort_label,
      'geo_missing', v_geo_missing,
      'geo_unresolved', v_geo_unresolved and final_mode = 'place'
    ),
    'metrics', metric_results
  );
end;
$$;

comment on function public.ddv_client_benchmark(jsonb) is
  'Public client benchmark: size-matched + guarded national, plus per-metric local ladder (place -> radius -> national same-size). Aggregate-only.';

revoke all on function public.ddv_client_benchmark(jsonb) from public;
revoke all on function public.ddv_client_benchmark(jsonb) from anon, authenticated;
grant execute on function public.ddv_client_benchmark(jsonb) to service_role;
