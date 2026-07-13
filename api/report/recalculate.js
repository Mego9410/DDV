const { callRpc, patchRows } = require("../_lib/supabase");
const { requireValidLead } = require("./lead");
const {
  setCors,
  geocodePlaceToLatLng,
  checkRateLimit,
  parseBody,
  validateBenchmarkBody,
  enrichMetrics,
} = require("./_lib");

module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });

  try {
    checkRateLimit(req);
    const body = parseBody(req);
    const token = String(body?.token ?? "").trim();
    const row = await requireValidLead(token);
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

    const now = new Date().toISOString();
    await patchRows(
      "report_leads",
      { id: `eq.${row.id}` },
      {
        location,
        surgery_count: surgeryCount,
        report_json: report,
        unlocked_at: row.unlocked_at || now,
        verified_at: row.verified_at || now,
      }
    );

    return res.status(200).json({
      ok: true,
      name: row.name,
      report,
      inputs: { location, surgeryCount, metrics },
    });
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
