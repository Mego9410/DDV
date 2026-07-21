-- Founding-cohort waitlist applications for the Hidden Profit Programme.
create table if not exists public.profit_waitlist (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  name text not null,
  role text not null,
  email text not null,
  phone text not null,
  practice_name text not null,
  location text not null,
  surgery_count integer not null,
  practice_type text not null,
  turnover_band text not null,
  years_owned text not null,
  categories text[] not null default '{}',
  overhead_band text not null,
  invoice_access text not null,
  decision_maker text not null,
  timeline text not null,
  motivation text not null,
  consent boolean not null default false
);

create index if not exists profit_waitlist_email_idx on public.profit_waitlist (lower(email));
create index if not exists profit_waitlist_created_idx on public.profit_waitlist (created_at);

alter table public.profit_waitlist enable row level security;

revoke all on table public.profit_waitlist from anon, authenticated;
grant all on table public.profit_waitlist to service_role;

comment on table public.profit_waitlist is
  'Applications to join the founding cohort of the DDV cost-optimisation (Hidden Profit) programme.';
