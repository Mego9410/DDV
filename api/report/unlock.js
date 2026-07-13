const crypto = require("crypto");
const { selectRows, patchRows } = require("../_lib/supabase");
const { setCors, checkRateLimit, parseBody } = require("./_lib");

function hashToken(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "POST" && req.method !== "GET") {
    return res.status(405).json({ detail: "Method not allowed" });
  }

  try {
    checkRateLimit(req);

    let token = "";
    if (req.method === "GET") {
      token = String(req.query?.token ?? "").trim();
    } else {
      const body = parseBody(req);
      token = String(body?.token ?? "").trim();
    }

    if (!token || token.length < 20 || token.length > 200) {
      const err = new Error("This unlock link is invalid or incomplete.");
      err.statusCode = 400;
      throw err;
    }

    const token_hash = hashToken(token);
    const rows = await selectRows("report_leads", {
      select: "id,name,email,report_json,expires_at,unlocked_at,verified_at",
      filters: { token_hash: `eq.${token_hash}` },
      limit: 1,
    });
    const row = Array.isArray(rows) ? rows[0] : null;
    if (!row) {
      const err = new Error("This unlock link is invalid or has expired.");
      err.statusCode = 404;
      throw err;
    }

    const expiresAt = new Date(row.expires_at).getTime();
    if (!Number.isFinite(expiresAt) || expiresAt < Date.now()) {
      const err = new Error("This unlock link has expired. Please generate your report again.");
      err.statusCode = 410;
      throw err;
    }

    const now = new Date().toISOString();
    const patch = { unlocked_at: row.unlocked_at || now };
    if (!row.verified_at) patch.verified_at = now;
    await patchRows("report_leads", { id: `eq.${row.id}` }, patch);

    return res.status(200).json({
      ok: true,
      name: row.name,
      report: row.report_json,
    });
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
