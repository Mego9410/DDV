-- Add "Visited on" date from calc sheet header.

alter table if exists public.practices
  add column if not exists visited_on date null;

create index if not exists practices_visited_on_idx on public.practices(visited_on);

