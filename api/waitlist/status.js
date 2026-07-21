const { countRows } = require("../_lib/supabase");
const { setCors, checkRateLimit } = require("../report/_lib");

const FOUNDING_PLACES = 25;

module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "GET") return res.status(405).json({ detail: "Method not allowed" });

  try {
    checkRateLimit(req);
    const applications = await countRows("profit_waitlist");
    return res.status(200).json({
      ok: true,
      places: FOUNDING_PLACES,
      applications: Number.isFinite(applications) ? applications : null,
    });
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
