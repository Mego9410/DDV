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
  const url = SUPABASE_URL?.replace(/\/$/, "");
  const endpoint = `${url}/rest/v1/${SUPABASE_SECRET_TABLE}`;
  const params = new URLSearchParams({ select: "value", key: `eq.${key}`, limit: "1" });
  const resp = await fetch(`${endpoint}?${params.toString()}`, { headers: supabaseHeaders() });
  if (!resp.ok) return null;
  const data = await resp.json();
  if (!Array.isArray(data) || !data.length) return null;
  const value = data[0]?.value;
  return typeof value === "string" && value.trim() ? value.trim() : null;
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

module.exports = { fetchSecret, callRpc };

