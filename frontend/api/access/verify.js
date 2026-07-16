const bcrypt = require("bcryptjs");
const { fetchSecret } = require("../_lib/supabase");
const { mintAccessToken } = require("../_lib/token");

const ACCESS_TOKEN_SECRET = (process.env.ACCESS_TOKEN_SECRET || "").trim() || "ddv-dev-access-token-secret";
const ACCESS_TOKEN_TTL_SECONDS = Number(process.env.ACCESS_TOKEN_TTL_SECONDS || 1800);
const SUPABASE_SHARED_PASSWORD_KEY = process.env.SUPABASE_SHARED_PASSWORD_KEY || "shared_password_hash";

// The password is stored only as a bcrypt hash in Supabase app_secrets
// (key: shared_password_hash). No plaintext or hash lives in the code.
// SHARED_PASSWORD_HASH is an optional env break-glass (a bcrypt hash, not
// plaintext) for the rare case the secrets table is unreachable.
async function isPasswordValid(password) {
  const trimmed = String(password ?? "").trim();
  if (!trimmed) return false;

  const hashes = new Set();
  const envHash = (process.env.SHARED_PASSWORD_HASH || "").trim();
  if (envHash) hashes.add(envHash);
  const storedHash = await fetchSecret(SUPABASE_SHARED_PASSWORD_KEY);
  if (storedHash) hashes.add(storedHash);

  for (const hash of hashes) {
    try {
      if (bcrypt.compareSync(trimmed, hash)) return true;
    } catch {
      // ignore malformed hashes
    }
  }
  return false;
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });
  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const password = String(body?.password ?? "");
    if (!password.trim()) return res.status(400).json({ detail: "Missing password" });

    const ok = await isPasswordValid(password);
    if (!ok) return res.status(401).json({ detail: "Invalid password" });

    const access_token = mintAccessToken({
      secret: ACCESS_TOKEN_SECRET,
      ttlSeconds: ACCESS_TOKEN_TTL_SECONDS,
    });

    return res.status(200).json({
      access_token,
      token_type: "bearer",
      expires_in: ACCESS_TOKEN_TTL_SECONDS,
    });
  } catch (e) {
    return res.status(500).json({ detail: String(e?.message || e) });
  }
};
