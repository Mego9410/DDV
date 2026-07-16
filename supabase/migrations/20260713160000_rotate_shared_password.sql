-- Rotate the shared site access password.
-- The plaintext lives nowhere in the repo; only this bcrypt hash is stored,
-- in app_secrets, and the /api/access/verify function reads it from there.
insert into public.app_secrets (key, value)
values (
  'shared_password_hash',
  '$2a$10$U5lMXgBhTeqbRAy4AWPgYeQsnZJ0GpojZ2nisHBtI8h9NLm07oUsy'
)
on conflict (key) do update set value = excluded.value;
