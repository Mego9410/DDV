const { patchRows } = require("../_lib/supabase");
const { setCors, checkRateLimit, parseBody } = require("./_lib");
const { requireValidLead, inputsFromLead } = require("./lead");

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

    const row = await requireValidLead(token);

    const now = new Date().toISOString();
    const patch = { unlocked_at: row.unlocked_at || now };
    if (!row.verified_at) patch.verified_at = now;
    await patchRows("report_leads", { id: `eq.${row.id}` }, patch);

    return res.status(200).json({
      ok: true,
      name: row.name,
      report: row.report_json,
      inputs: inputsFromLead(row),
    });
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
