const crypto = require("crypto");
const { selectRows } = require("../_lib/supabase");

function hashToken(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

async function requireValidLead(token) {
  if (!token || token.length < 20 || token.length > 200) {
    const err = new Error("This unlock link is invalid or incomplete.");
    err.statusCode = 400;
    throw err;
  }

  const token_hash = hashToken(token);
  const rows = await selectRows("report_leads", {
    select: "id,name,email,location,surgery_count,report_json,expires_at,unlocked_at,verified_at",
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

  return row;
}

function inputsFromLead(row) {
  const report = row?.report_json || {};
  const metrics = Array.isArray(report?.metrics)
    ? report.metrics
        .filter((m) => m && m.id != null && Number.isFinite(Number(m.your_value)))
        .map((m) => ({ id: m.id, value: Number(m.your_value) }))
    : [];
  return {
    location: row.location || report?.cohort?.location || "",
    surgeryCount: row.surgery_count ?? null,
    metrics,
  };
}

module.exports = { hashToken, requireValidLead, inputsFromLead };
