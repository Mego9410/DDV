const METRICS = [
  { id: "turnover", label: "Turnover", unit: "gbp", column: "cert_income_gbp", group: "income" },
  { id: "net_profit", label: "Net profit", unit: "gbp", column: "cert_net_profit_gbp", group: "income" },
  { id: "nhs_income", label: "NHS income", unit: "gbp", column: "income_split_nhs_value", group: "income" },
  { id: "fpi_income", label: "Private (FPI) income", unit: "gbp", column: "income_split_fpi_value", group: "income" },
  { id: "uda_rate", label: "UDA rate", unit: "gbp", column: "uda_rate_gbp", group: "income" },
  { id: "associates", label: "Associates", unit: "gbp", column: "cert_associates_gbp", group: "costs" },
  { id: "wages", label: "Staff wages", unit: "gbp", column: "cert_wages_gbp", group: "costs" },
  { id: "hygiene", label: "Hygienist", unit: "gbp", column: "cert_hygiene_gbp", group: "costs" },
  { id: "materials", label: "Materials", unit: "gbp", column: "cert_materials_gbp", group: "costs" },
  { id: "labs", label: "Laboratory", unit: "gbp", column: "cert_labs_gbp", group: "costs" },
  { id: "associate_cost", label: "Modelled associate cost", unit: "gbp", column: "associate_cost_amount", group: "costs" },
];

const SURGERY_COUNTS = [
  ...Array.from({ length: 12 }, (_, i) => ({ value: i + 1, label: String(i + 1) })),
  { value: 13, label: "13+" },
];

/** Common UK counties always offered even if sparse in the dataset. */
const CURATED_LOCATIONS = [
  "Bedfordshire",
  "Berkshire",
  "Bristol",
  "Buckinghamshire",
  "Cambridgeshire",
  "Cheshire",
  "Cornwall",
  "Cumbria",
  "Derbyshire",
  "Devon",
  "Dorset",
  "Durham",
  "East Sussex",
  "Essex",
  "Gloucestershire",
  "Greater London",
  "Greater Manchester",
  "Hampshire",
  "Hertfordshire",
  "Kent",
  "Lancashire",
  "Leicestershire",
  "Lincolnshire",
  "London",
  "Merseyside",
  "Norfolk",
  "North Yorkshire",
  "Northamptonshire",
  "Nottinghamshire",
  "Oxfordshire",
  "Somerset",
  "South Yorkshire",
  "Staffordshire",
  "Suffolk",
  "Surrey",
  "Tyne and Wear",
  "Warwickshire",
  "West Midlands",
  "West Sussex",
  "West Yorkshire",
  "Wiltshire",
  "Worcestershire",
  "Birmingham",
  "Bradford",
  "Leeds",
  "Liverpool",
  "Manchester",
  "Newcastle",
  "Nottingham",
  "Sheffield",
];

const ALLOWED_METRIC_IDS = new Set(METRICS.map((m) => m.id));

function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

function mergeLocations(fromDb) {
  const set = new Set();
  for (const loc of CURATED_LOCATIONS) {
    if (loc && String(loc).trim()) set.add(String(loc).trim());
  }
  const list = Array.isArray(fromDb) ? fromDb : [];
  for (const loc of list) {
    const s = typeof loc === "string" ? loc.trim() : String(loc ?? "").trim();
    if (!s) continue;
    if (s.length < 2 || s.length > 40) continue;
    if (/\d/.test(s) || /,/.test(s) || /\s{2,}/.test(s)) continue;
    set.add(s);
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b, "en-GB"));
}

async function geocodePlaceToLatLng(place) {
  const raw = String(place || "").trim();
  if (!raw) return null;

  const pc = raw.replace(/\s+/g, "").toUpperCase();
  if (/^[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2}$/.test(pc)) {
    const resp = await fetch(`https://api.postcodes.io/postcodes/${encodeURIComponent(raw)}`);
    if (!resp.ok) return null;
    const data = await resp.json().catch(() => null);
    const r = data?.result;
    if (!r) return null;
    const lat = Number(r.latitude);
    const lng = Number(r.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
    return { lat, lng };
  }

  const resp = await fetch(`https://api.postcodes.io/places?q=${encodeURIComponent(raw)}`);
  if (!resp.ok) return null;
  const data = await resp.json().catch(() => null);
  const r = Array.isArray(data?.result) ? data.result[0] : null;
  const lat = Number(r?.latitude);
  const lng = Number(r?.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  return { lat, lng };
}

/** Very light in-memory throttle (best-effort on serverless). */
const rateBuckets = new Map();
const RATE_WINDOW_MS = 60_000;
const RATE_MAX = 30;

function clientIp(req) {
  const xf = req.headers?.["x-forwarded-for"] || req.headers?.["X-Forwarded-For"];
  if (typeof xf === "string" && xf.trim()) return xf.split(",")[0].trim();
  return req.socket?.remoteAddress || "unknown";
}

function checkRateLimit(req) {
  const ip = clientIp(req);
  const now = Date.now();
  let bucket = rateBuckets.get(ip);
  if (!bucket || now - bucket.start > RATE_WINDOW_MS) {
    bucket = { start: now, count: 0 };
    rateBuckets.set(ip, bucket);
  }
  bucket.count += 1;
  if (bucket.count > RATE_MAX) {
    const err = new Error("Too many requests. Please try again shortly.");
    err.statusCode = 429;
    throw err;
  }
}

function parseBody(req) {
  if (typeof req.body === "string") {
    try {
      return JSON.parse(req.body);
    } catch {
      const err = new Error("Invalid JSON body");
      err.statusCode = 400;
      throw err;
    }
  }
  return req.body || {};
}

function validateBenchmarkBody(body) {
  const location = String(body?.location ?? "").trim();
  if (!location || location.length > 120) {
    const err = new Error("location is required");
    err.statusCode = 400;
    throw err;
  }

  const surgeryCount = Number(body?.surgeryCount ?? body?.surgery_count);
  if (!Number.isInteger(surgeryCount) || surgeryCount < 1 || surgeryCount > 50) {
    const err = new Error("surgeryCount must be an integer between 1 and 50");
    err.statusCode = 400;
    throw err;
  }

  const rawMetrics = body?.metrics;
  if (!Array.isArray(rawMetrics) || rawMetrics.length === 0) {
    const err = new Error("Select at least one metric and enter your figure");
    err.statusCode = 400;
    throw err;
  }
  if (rawMetrics.length > 12) {
    const err = new Error("At most 12 metrics allowed");
    err.statusCode = 400;
    throw err;
  }

  const seen = new Set();
  const metrics = [];
  for (const m of rawMetrics) {
    const id = String(m?.id ?? "")
      .trim()
      .toLowerCase();
    if (!ALLOWED_METRIC_IDS.has(id)) {
      const err = new Error(`Unknown or disallowed metric: ${id || "(empty)"}`);
      err.statusCode = 400;
      throw err;
    }
    if (seen.has(id)) continue;
    seen.add(id);
    const value = Number(m?.value);
    if (!Number.isFinite(value)) {
      const err = new Error(`Invalid value for metric ${id}`);
      err.statusCode = 400;
      throw err;
    }
    metrics.push({ id, value });
  }

  if (!metrics.length) {
    const err = new Error("Select at least one metric and enter your figure");
    err.statusCode = 400;
    throw err;
  }

  return { location, surgeryCount, metrics };
}

function enrichMetrics(result) {
  const byId = Object.fromEntries(METRICS.map((m) => [m.id, m]));
  const metrics = Array.isArray(result?.metrics) ? result.metrics : [];
  return {
    ...result,
    metrics: metrics.map((row) => {
      const meta = byId[row.id] || {};
      return {
        ...row,
        label: meta.label || row.id,
        unit: meta.unit || "gbp",
        group: meta.group || null,
      };
    }),
  };
}

module.exports = {
  METRICS,
  SURGERY_COUNTS,
  ALLOWED_METRIC_IDS,
  setCors,
  mergeLocations,
  geocodePlaceToLatLng,
  checkRateLimit,
  parseBody,
  validateBenchmarkBody,
  enrichMetrics,
};
