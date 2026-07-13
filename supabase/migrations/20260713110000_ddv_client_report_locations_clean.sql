-- Prefer locations with enough practices for a useful local peer group.
create or replace function public.ddv_client_report_locations()
returns jsonb
language sql
security definer
set search_path = public
as $$
  select coalesce(
    jsonb_agg(to_jsonb(place) order by place),
    '[]'::jsonb
  )
  from (
    select place
    from (
      select place, count(*) as n
      from (
        select btrim(city) as place from public.practices
        where city is not null and length(btrim(city)) > 0
        union all
        select btrim(county) as place from public.practices
        where county is not null and length(btrim(county)) > 0
      ) raw
      where place is not null
        and length(place) between 2 and 40
        and place !~ '[0-9]'
        and place !~ ','
        and place !~ '\s{2,}'
      group by place
    ) counted
    where n >= 3
  ) places;
$$;

revoke all on function public.ddv_client_report_locations() from public;
revoke all on function public.ddv_client_report_locations() from anon, authenticated;
grant execute on function public.ddv_client_report_locations() to service_role;
