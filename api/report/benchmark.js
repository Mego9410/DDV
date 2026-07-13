/**
 * Direct benchmark responses are gated. Clients must use:
 *   POST /api/report/request-link  (name + email + form) → magic link emailed
 *   POST /api/report/unlock        (token) → report JSON
 */
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(204).end();
  return res.status(410).json({
    detail: "Use POST /api/report/request-link with name and email. Results unlock via magic link.",
  });
};
