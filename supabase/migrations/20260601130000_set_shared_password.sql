-- Shared site access password (bcrypt hash for plaintext: password)
create table if not exists public.app_secrets (
  key text primary key,
  value text not null
);

insert into public.app_secrets (key, value)
values (
  'shared_password_hash',
  '$2a$10$Z8uxdQ2GCBD7fn80Mc3OCuWiiWkOPFADCSgho4UFN5xSb60p.r8b6'
)
on conflict (key) do update set value = excluded.value;
