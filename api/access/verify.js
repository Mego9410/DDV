const bcrypt = require("bcryptjs");
const { fetchSecret } = require("../_lib/supabase");
const { mintAccessToken } = require("../_lib/token");

const ACCESS_TOKEN_SECRET = process.env.ACCESS_TOKEN_SECRET || "";
const ACCESS_TOKEN_TTL_SECONDS = Number(process.env.ACCESS_TOKEN_TTL_SECONDS || 1800);
const SHARED_PASSWORD_PLAIN = process.env.SHARED_PASSWORD_PLAIN || "pass";
const SUPABASE_SHARED_PASSWORD_KEY = process.env.SUPABASE_SHARED_PASSWORD_KEY || "shared_password_hash";

module.exports = async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });
  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const password = String(body?.password ?? "");
    if (!password.trim()) return res.status(400).json({ detail: "Missing password" });

    const storedHash = await fetchSecret(SUPABASE_SHARED_PASSWORD_KEY);
    let ok = false;
    if (!storedHash) {
      ok = password === SHARED_PASSWORD_PLAIN;
    } else {
      ok = bcrypt.compareSync(password, storedHash);
    }

    if (!ok) return res.status(401).json({ detail: "Invalid password" });
    if (!ACCESS_TOKEN_SECRET) return res.status(500).json({ detail: "Missing server env: ACCESS_TOKEN_SECRET" });

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

