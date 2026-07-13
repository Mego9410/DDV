const crypto = require("crypto");
const { callRpc, insertRow, patchRows, sendAuthMagicLink } = require("../_lib/supabase");
const { resolveSiteUrl } = require("../_lib/site");
const {
  setCors,
  geocodePlaceToLatLng,
  checkRateLimit,
  parseBody,
  validateBenchmarkBody,
  enrichMetrics,
} = require("./_lib");

const TOKEN_TTL_HOURS = 24;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function validateLead(body) {
  const name = String(body?.name ?? "").trim();
  const email = String(body?.email ?? "").trim().toLowerCase();
  if (!name || name.length > 120) {
    const err = new Error("Please enter your name");
    err.statusCode = 400;
    throw err;
  }
  if (!email || email.length > 254 || !EMAIL_RE.test(email)) {
    const err = new Error("Please enter a valid email address");
    err.statusCode = 400;
    throw err;
  }
  return { name, email };
}

function maskEmail(email) {
  const [user, domain] = String(email).split("@");
  if (!domain) return "***";
  const visible = user.slice(0, Math.min(2, user.length));
  return `${visible}${"*".repeat(Math.max(user.length - visible.length, 2))}@${domain}`;
}

function hashToken(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });

  try {
    checkRateLimit(req);
    const body = parseBody(req);
    const { name, email } = validateLead(body);
    const { location, surgeryCount, metrics } = validateBenchmarkBody(body);

    const center = await geocodePlaceToLatLng(location);
    const payload = {
      location,
      surgery_count: surgeryCount,
      metrics,
    };
    if (center) {
      payload.lat = center.lat;
      payload.lng = center.lng;
    }

    const raw = await callRpc("ddv_client_benchmark", { payload });
    const report = enrichMetrics(raw || {});

    const token = crypto.randomBytes(32).toString("base64url");
    const token_hash = hashToken(token);
    const expires_at = new Date(Date.now() + TOKEN_TTL_HOURS * 60 * 60 * 1000).toISOString();

    const row = await insertRow("report_leads", {
      name,
      email,
      location,
      surgery_count: surgeryCount,
      report_json: report,
      token_hash,
      expires_at,
    });

    const siteUrl = resolveSiteUrl(req);
    const unlockUrl = `${siteUrl}/report?token=${encodeURIComponent(token)}`;

    await sendAuthMagicLink({
      email,
      redirectTo: unlockUrl,
      data: { full_name: name, report_lead_id: row?.id || null },
    });

    if (row?.id) {
      try {
        await patchRows("report_leads", { id: `eq.${row.id}` }, { email_id: "supabase_auth_otp" });
      } catch {
        /* ignore */
      }
    }

    return res.status(200).json({
      ok: true,
      emailMasked: maskEmail(email),
      expiresInHours: TOKEN_TTL_HOURS,
      provider: "supabase",
    });
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
