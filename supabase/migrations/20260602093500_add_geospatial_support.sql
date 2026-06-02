-- Add geospatial support for radius queries (e.g. "30 miles around Brighton").
--
-- Uses:
-- - practices.lat / practices.lng populated from UK postcodes (centroids)
-- - practices.geog geography(Point,4326) generated for fast distance filtering
-- - postcode_geocode cache table for postcode -> (lat,lng)

create extension if not exists postgis;

-- Cache for postcode -> lat/lng. Normalized form: upper-case, no spaces.
create table if not exists public.postcode_geocode (
  postcode text primary key,
  lat double precision not null,
  lng double precision not null,
  updated_at timestamptz not null default now()
);

alter table public.practices
  add column if not exists lat double precision null,
  add column if not exists lng double precision null;

-- Generated geography point (null if lat/lng missing).
alter table public.practices
  add column if not exists geog geography(Point, 4326)
  generated always as (
    case
      when lat is null or lng is null then null
      else st_setsrid(st_makepoint(lng, lat), 4326)::geography
    end
  ) stored;

create index if not exists practices_geog_gix on public.practices using gist (geog);

