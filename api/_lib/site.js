/**
 * Public site origin for magic-link redirects.
 * Prefer REPORT_SITE_URL (e.g. https://www.dentaldatavault.com).
 */
function resolveSiteUrl(req) {
  const explicit = (process.env.REPORT_SITE_URL || process.env.SITE_URL || "").trim().replace(/\/$/, "");
  if (explicit) return explicit;
  const vercel = (process.env.VERCEL_URL || "").trim().replace(/\/$/, "");
  if (vercel) return `https://${vercel}`;
  const host = req?.headers?.["x-forwarded-host"] || req?.headers?.host;
  const proto = req?.headers?.["x-forwarded-proto"] || "https";
  if (host) return `${proto}://${String(host).split(",")[0].trim()}`;
  return "http://localhost:3000";
}

module.exports = { resolveSiteUrl };
