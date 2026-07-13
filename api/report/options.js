const { callRpc } = require("../_lib/supabase");
const {
  METRICS,
  SURGERY_COUNTS,
  setCors,
  mergeLocations,
  checkRateLimit,
} = require("./_lib");

module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "GET") return res.status(405).json({ detail: "Method not allowed" });

  try {
    checkRateLimit(req);
    let locations = [];
    try {
      const raw = await callRpc("ddv_client_report_locations", {});
      locations = mergeLocations(raw);
    } catch {
      locations = mergeLocations([]);
    }

    return res.status(200).json({
      locations,
      surgeryCounts: SURGERY_COUNTS,
      metrics: METRICS.map(({ id, label, unit, group }) => ({ id, label, unit, group })),
    });
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
