const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_SECRET_TABLE = process.env.SUPABASE_SECRET_TABLE || "app_secrets";

function requireEnv(name, value) {
  if (!value) {
    const err = new Error(`Missing server env: ${name}`);
    err.statusCode = 500;
    throw err;
  }
}

function supabaseHeaders() {
  requireEnv("SUPABASE_URL", SUPABASE_URL);
  requireEnv("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY);
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
  };
}

async function fetchSecret(key) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) return null;
  try {
    const url = SUPABASE_URL.replace(/\/$/, "");
    const endpoint = `${url}/rest/v1/${SUPABASE_SECRET_TABLE}`;
    const params = new URLSearchParams({ select: "value", key: `eq.${key}`, limit: "1" });
    const resp = await fetch(`${endpoint}?${params.toString()}`, { headers: supabaseHeaders() });
    if (!resp.ok) return null;
    const data = await resp.json();
    if (!Array.isArray(data) || !data.length) return null;
    const value = data[0]?.value;
    return typeof value === "string" && value.trim() ? value.trim() : null;
  } catch {
    return null;
  }
}

async function callRpc(fnName, body) {
  const url = SUPABASE_URL?.replace(/\/$/, "");
  const resp = await fetch(`${url}/rest/v1/rpc/${fnName}`, {
    method: "POST",
    headers: { ...supabaseHeaders(), "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
  });
  const text = await resp.text();
  if (!resp.ok) {
    const err = new Error(text || `Supabase RPC failed (${resp.status})`);
    err.statusCode = 500;
    throw err;
  }
  return text ? JSON.parse(text) : null;
}

async function insertRow(table, row) {
  const url = SUPABASE_URL?.replace(/\/$/, "");
  requireEnv("SUPABASE_URL", url);
  const resp = await fetch(`${url}/rest/v1/${table}`, {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      "Content-Type": "application/json",
      Prefer: "return=representation",
    },
    body: JSON.stringify(row),
  });
  const text = await resp.text();
  if (!resp.ok) {
    const err = new Error(text || `Supabase insert failed (${resp.status})`);
    err.statusCode = 500;
    throw err;
  }
  const data = text ? JSON.parse(text) : null;
  return Array.isArray(data) ? data[0] ?? null : data;
}

async function selectRows(table, { select = "*", filters = {}, limit = 1, order } = {}) {
  const url = SUPABASE_URL?.replace(/\/$/, "");
  requireEnv("SUPABASE_URL", url);
  const params = new URLSearchParams({ select: String(select), limit: String(limit) });
  for (const [key, value] of Object.entries(filters || {})) {
    params.set(key, String(value));
  }
  if (order) params.set("order", order);
  const resp = await fetch(`${url}/rest/v1/${table}?${params.toString()}`, {
    headers: supabaseHeaders(),
  });
  const text = await resp.text();
  if (!resp.ok) {
    const err = new Error(text || `Supabase select failed (${resp.status})`);
    err.statusCode = 500;
    throw err;
  }
  return text ? JSON.parse(text) : [];
}

async function countRows(table) {
  const url = SUPABASE_URL?.replace(/\/$/, "");
  requireEnv("SUPABASE_URL", url);
  const resp = await fetch(`${url}/rest/v1/${table}?select=id&limit=1`, {
    method: "HEAD",
    headers: { ...supabaseHeaders(), Prefer: "count=exact" },
  });
  if (!resp.ok) return null;
  const range = resp.headers.get("content-range") || "";
  const total = Number(range.split("/")[1]);
  return Number.isFinite(total) ? total : null;
}

async function patchRows(table, filters, patch) {
  const url = SUPABASE_URL?.replace(/\/$/, "");
  requireEnv("SUPABASE_URL", url);
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(filters || {})) {
    params.set(key, String(value));
  }
  const resp = await fetch(`${url}/rest/v1/${table}?${params.toString()}`, {
    method: "PATCH",
    headers: {
      ...supabaseHeaders(),
      "Content-Type": "application/json",
      Prefer: "return=representation",
    },
    body: JSON.stringify(patch),
  });
  const text = await resp.text();
  if (!resp.ok) {
    const err = new Error(text || `Supabase patch failed (${resp.status})`);
    err.statusCode = 500;
    throw err;
  }
  const data = text ? JSON.parse(text) : [];
  return Array.isArray(data) ? data : [];
}

/**
 * Send a Supabase Auth magic link (email from Supabase — no custom domain required).
 * redirectTo must be allow-listed under Auth → URL Configuration → Redirect URLs.
 */
async function sendAuthMagicLink({ email, redirectTo, data }) {
  const url = SUPABASE_URL?.replace(/\/$/, "");
  requireEnv("SUPABASE_URL", url);
  const params = new URLSearchParams();
  if (redirectTo) params.set("redirect_to", redirectTo);
  const qs = params.toString() ? `?${params.toString()}` : "";
  const resp = await fetch(`${url}/auth/v1/otp${qs}`, {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      email,
      create_user: true,
      data: data && typeof data === "object" ? data : undefined,
    }),
  });
  const text = await resp.text();
  if (!resp.ok) {
    let detail = text || `Supabase Auth OTP failed (${resp.status})`;
    try {
      const parsed = JSON.parse(text);
      detail = parsed?.msg || parsed?.error_description || parsed?.message || detail;
    } catch {
      /* keep text */
    }
    const err = new Error(detail);
    err.statusCode = resp.status === 429 ? 429 : 502;
    throw err;
  }
  return text ? JSON.parse(text) : { ok: true };
}

module.exports = {
  fetchSecret,
  callRpc,
  insertRow,
  selectRows,
  countRows,
  patchRows,
  sendAuthMagicLink,
};

