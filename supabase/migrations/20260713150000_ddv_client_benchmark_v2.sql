-- ============================================================
-- Client benchmark v2 — fairer peer selection.
--
-- Fixes the "average practice reads as far above median" problem:
--   1. National comparison is SIZE-MATCHED (same surgery band),
--      widening the band only if needed, else all practices.
--   2. Each metric finds its OWN local pool, widening surgery band
--      then distance (25 -> 50 -> 100 -> 150 miles), stopping as soon
--      as there are enough comparable practices. One sparse line (e.g.
--      NHS income in a private-heavy area) no longer drags every other
--      line's geography wider.
--   3. A real minimum sample (TARGET_N) is targeted; a local pool still
--      too thin at 150 miles is suppressed (the size-matched national
--      still carries the line) rather than shown as fact or relabelled.
--   4. Per-metric sanity bounds drop obvious data errors before the
--      median (e.g. a UDA rate of £1,102, a £ total under £250).
--
-- Aggregate-only. Returns medians and deltas; never sample counts,
-- pool footnotes or practice rows (client view stays clean).
-- ============================================================

-- Internal helper: median + count for one column under an arbitrary,
-- already-safe WHERE clause. Not granted to callers directly; only the
-- SECURITY DEFINER benchmark function below calls it.
create or replace function public.ddv_pool_stat(p_col text, p_where text, p_lo numeric, p_hi numeric)
returns table(n bigint, med numeric)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  -- p_lo/p_hi are per-metric sanity bounds: values outside them are
  -- treated as data errors and excluded before taking the median.
  -- percentile_cont returns double precision (numeric inputs are cast to
  -- double), so cast back to numeric to match the declared return type.
  return query execute format(
    $q$
      select
        count(%1$I)::bigint,
        (percentile_cont(0.5) within group (order by %1$I))::numeric
      from public.practices
      where (%2$s)
        and %1$I is not null
        and %1$I >= %3$s and %1$I <= %4$s
    $q$,
    p_col, p_where, p_lo, p_hi
  );
end;
$$;

revoke all on function public.ddv_pool_stat(text, text, numeric, numeric) from public;
revoke all on function public.ddv_pool_stat(text, text, numeric, numeric) from anon, authenticated;

create or replace function public.ddv_client_benchmark(payload jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  -- Tunables. Adjust after reviewing the data audit.
  TARGET_N   constant int := 20;  -- widen the pool until at least this many peers
  MIN_SHOW_N constant int := 8;   -- below this, suppress rather than show a noisy median

  v_location text := btrim(coalesce(payload->>'location', ''));
  v_surgery  int  := nullif(payload->>'surgery_count', '')::int;
  v_lat      double precision := nullif(payload->>'lat', '')::double precision;
  v_lng      double precision := nullif(payload->>'lng', '')::double precision;
  v_metrics  jsonb := coalesce(payload->'metrics', '[]'::jsonb);
  v_is_13_plus boolean;
  v_has_center boolean := false;

  v_metric jsonb;
  v_id text;
  v_col text;
  v_your numeric;

  v_lo numeric;  -- per-metric lower sanity bound
  v_hi numeric;  -- per-metric upper sanity bound
  v_where text;
  v_geo text;
  v_n bigint;
  v_med numeric;

  -- national (size-matched) result
  nat_med numeric;
  nat_n bigint;
  nat_basis text;

  -- local (per-metric) result
  loc_med numeric;
  loc_n bigint;
  loc_basis text;
  best_med numeric;
  best_n bigint;
  best_basis text;

  pct_nat numeric;
  pct_loc numeric;
  local_suppressed boolean;

  metric_results jsonb := '[]'::jsonb;
  row_obj jsonb;

  -- ladder step config
  step int;
  s_mode text;
  s_lo int;
  s_hi int;
  s_radius double precision;
  s_found boolean;
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

  v_has_center :=
    v_lat is not null and v_lng is not null
    and v_lat = v_lat and v_lng = v_lng
    and v_lat between -90 and 90 and v_lng between -180 and 180;

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

    -- Per-metric sanity bounds: drop obvious data errors before the median.
    -- UDA rate is a per-UDA rate (~£15-40), not a £ total.
    if v_id = 'uda_rate' then
      v_lo := 5; v_hi := 150;
    else
      v_lo := 250; v_hi := 25000000;
    end if;

    -- ---------------------------------------------------------
    -- NATIONAL, size-matched: widen the surgery band until we have
    -- enough, else fall back to all practices.
    -- ---------------------------------------------------------
    nat_med := null; nat_n := 0; nat_basis := null; s_found := false;
    for step in 1..4 loop
      if v_is_13_plus then
        -- 13+ is already an open band; treat as one bucket
        s_lo := greatest(1, 13 - (step - 1)); s_hi := 50;
      else
        s_lo := greatest(1, v_surgery - (step - 1));
        s_hi := least(50, v_surgery + (step - 1));
      end if;
      v_where := format('surgery_count between %s and %s', s_lo, s_hi);
      select n, med into v_n, v_med from public.ddv_pool_stat(v_col, v_where, v_lo, v_hi);
      if v_med is not null and (nat_med is null or v_n > nat_n) then
        nat_med := v_med; nat_n := v_n;
        nat_basis := case when s_lo = s_hi
          then format('%s-surgery practices nationally', s_lo)
          else format('%s-%s surgery practices nationally', s_lo, s_hi) end;
      end if;
      if v_n >= TARGET_N then s_found := true; exit; end if;
    end loop;
    if not s_found then
      -- all practices, any size
      select n, med into v_n, v_med from public.ddv_pool_stat(v_col, 'true', v_lo, v_hi);
      if v_med is not null and (nat_med is null or v_n >= nat_n) then
        nat_med := v_med; nat_n := v_n; nat_basis := 'all practices nationally';
      end if;
    end if;

    -- ---------------------------------------------------------
    -- LOCAL, per-metric ladder. Stop at the first step with enough
    -- peers; otherwise keep the widest (largest n) step seen.
    -- ---------------------------------------------------------
    -- Local stays GEOGRAPHIC: widen surgery band, then distance out to
    -- 150 miles. If it is still too thin, suppress it — the size-matched
    -- national comparison still carries the line. We never relabel a
    -- national pool as "local".
    loc_med := null; loc_n := 0; loc_basis := null;
    best_med := null; best_n := -1; best_basis := null; s_found := false;

    for step in 1..7 loop
      -- resolve step -> (mode, surgery band, radius)
      if v_is_13_plus then
        case step
          when 1 then s_mode:='place';  s_lo:=13; s_hi:=50; s_radius:=null;
          when 2 then s_mode:='place';  s_lo:=12; s_hi:=50; s_radius:=null;
          when 3 then s_mode:='place';  s_lo:=11; s_hi:=50; s_radius:=null;
          when 4 then s_mode:='radius'; s_lo:=12; s_hi:=50; s_radius:=25;
          when 5 then s_mode:='radius'; s_lo:=11; s_hi:=50; s_radius:=50;
          when 6 then s_mode:='radius'; s_lo:=11; s_hi:=50; s_radius:=100;
          else        s_mode:='radius'; s_lo:=11; s_hi:=50; s_radius:=150;
        end case;
      else
        case step
          when 1 then s_mode:='place';  s_lo:=v_surgery;                 s_hi:=v_surgery;                 s_radius:=null;
          when 2 then s_mode:='place';  s_lo:=greatest(1,v_surgery-1);   s_hi:=least(50,v_surgery+1);     s_radius:=null;
          when 3 then s_mode:='place';  s_lo:=greatest(1,v_surgery-2);   s_hi:=least(50,v_surgery+2);     s_radius:=null;
          when 4 then s_mode:='radius'; s_lo:=greatest(1,v_surgery-1);   s_hi:=least(50,v_surgery+1);     s_radius:=25;
          when 5 then s_mode:='radius'; s_lo:=greatest(1,v_surgery-2);   s_hi:=least(50,v_surgery+2);     s_radius:=50;
          when 6 then s_mode:='radius'; s_lo:=greatest(1,v_surgery-2);   s_hi:=least(50,v_surgery+2);     s_radius:=100;
          else        s_mode:='radius'; s_lo:=greatest(1,v_surgery-2);   s_hi:=least(50,v_surgery+2);     s_radius:=150;
        end case;
      end if;

      -- radius steps need a geocoded centre
      if s_mode = 'radius' and not v_has_center then
        continue;
      end if;

      if s_mode = 'place' then
        v_where := format(
          $w$ ( lower(btrim(coalesce(city,''))) = lower(%L)
                or lower(btrim(coalesce(county,''))) = lower(%L) )
              and surgery_count between %s and %s $w$,
          v_location, v_location, s_lo, s_hi
        );
      else -- 'radius'
        v_where := format(
          $w$ geog is not null
              and st_dwithin(geog, st_setsrid(st_makepoint(%s, %s), 4326)::geography, %s * 1609.344)
              and surgery_count between %s and %s $w$,
          v_lng, v_lat, s_radius, s_lo, s_hi
        );
      end if;

      select n, med into v_n, v_med from public.ddv_pool_stat(v_col, v_where, v_lo, v_hi);

      if v_med is not null and v_n > best_n then
        best_med := v_med; best_n := v_n;
        best_basis := case s_mode
          when 'place'  then format('in %s', v_location)
          else format('within %s miles of %s', s_radius::int, v_location)
        end;
      end if;

      if v_n >= TARGET_N then s_found := true; exit; end if;
    end loop;

    if best_n >= MIN_SHOW_N then
      loc_med := best_med; loc_n := best_n; loc_basis := best_basis;
      local_suppressed := false;
    else
      local_suppressed := true;
    end if;

    -- ---------------------------------------------------------
    -- deltas
    -- ---------------------------------------------------------
    pct_nat := null;
    if nat_med is not null and nat_med <> 0 then
      pct_nat := round(((v_your - nat_med) / nat_med) * 100, 1);
    end if;

    pct_loc := null;
    if not local_suppressed and loc_med is not null and loc_med <> 0 then
      pct_loc := round(((v_your - loc_med) / loc_med) * 100, 1);
    end if;

    row_obj := jsonb_build_object(
      'id', v_id,
      'your_value', v_your,
      'national', jsonb_build_object('median', nat_med, 'basis', nat_basis),
      'local', case when local_suppressed then null
                    else jsonb_build_object('median', loc_med, 'basis', loc_basis) end,
      'pct_vs_national', pct_nat,
      'pct_vs_local', pct_loc,
      'local_suppressed', local_suppressed
    );
    metric_results := metric_results || jsonb_build_array(row_obj);
  end loop;

  return jsonb_build_object(
    'cohort', jsonb_build_object(
      'location', v_location,
      'requested_surgery_count', v_surgery,
      'label', format('Compared with similar-size practices near %s', v_location)
    ),
    'metrics', metric_results
  );
end;
$$;

comment on function public.ddv_client_benchmark(jsonb) is
  'Public client benchmark v2: size-matched national + per-metric widening local pools. Aggregate-only, no sample counts exposed.';

revoke all on function public.ddv_client_benchmark(jsonb) from public;
revoke all on function public.ddv_client_benchmark(jsonb) from anon, authenticated;
grant execute on function public.ddv_client_benchmark(jsonb) to service_role;
