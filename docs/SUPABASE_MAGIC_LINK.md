# Supabase Auth magic links (report unlock)

Used by `POST /api/report/request-link`. Emails are sent by **Supabase Auth** (not Resend / not Vercel).

Vercel has no transactional email product. Supabase sends from its own mail domain, so you do **not** need to verify `dentaldatavault.com` for outbound mail.

## Flow

1. User submits name + email + figures
2. API stores the report in `report_leads` with a hashed unlock token
3. API calls Supabase `POST /auth/v1/otp` with `redirect_to=/report?token=…`
4. User clicks the magic link in email → Supabase verifies → redirects to the report URL
5. Frontend calls `POST /api/report/unlock` with that token

## Dashboard setup (required)

In the Supabase project → **Authentication → URL Configuration**:

1. **Site URL**: `https://www.dentaldatavault.com` (or your current production origin)
2. **Redirect URLs** — add:
   - `https://www.dentaldatavault.com/**`
   - `https://ddv-mu.vercel.app/**`
   - `http://localhost:3000/**` (local)

Optional: **Authentication → Email Templates → Magic Link** — customise copy for Dental Data Vault. Keep `{{ .ConfirmationURL }}` in the template.

## Env (Vercel)

Already used by the app:

| Name | Purpose |
|------|---------|
| `SUPABASE_URL` | Project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Server-side Auth OTP + DB |
| `REPORT_SITE_URL` | Origin embedded in unlock redirects (e.g. `https://www.dentaldatavault.com`) |

Resend env vars are unused and can be removed.

## Limits

- Auth emails have rate limits (often ~1 OTP / 60s per address).
- On some free tiers, deliverability / volume is limited — check Supabase Auth email settings if mail doesn’t arrive.
- Spammers can create Auth users via this flow (`create_user: true`); that is intentional so first-time clients can unlock.
