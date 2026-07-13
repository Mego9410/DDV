const { callRpc } = require("../_lib/supabase");
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
    const { location, surgeryCount, metrics } = validateBenchmarkBody(parseBody(req));

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
    return res.status(200).json(enrichMetrics(raw || {}));
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
