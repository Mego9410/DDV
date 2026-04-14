const OpenAI = require("openai");
const { fetchSecret, callRpc } = require("./_lib/supabase");
const { verifyAccessToken } = require("./_lib/token");

const ACCESS_TOKEN_SECRET = process.env.ACCESS_TOKEN_SECRET || "";
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_PRACTICES_TABLE = process.env.SUPABASE_PRACTICES_TABLE || "practices";

function extractJsonObject(text) {
  if (!text) return "";
  let t = String(text).trim();

  // Strip markdown fences like ```json ... ``` (anywhere)
  t = t.replace(/```(?:json)?/gi, "").replace(/```/g, "").trim();

  // If there's extra text, try to take the first {...} block.
  const start = t.indexOf("{");
  const end = t.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) {
    t = t.slice(start, end + 1);
  }
  return t.trim();
}

function getBearerToken(req) {
  const h = req.headers?.authorization || req.headers?.Authorization || "";
  const s = Array.isArray(h) ? h[0] : String(h);
  if (!s.toLowerCase().startsWith("bearer ")) return null;
  return s.slice(7).trim();
}

function isCountyListQuestion(message) {
  const m = String(message || "").toLowerCase();
  if (!m.includes("county")) return false;
  return (
    m.includes("what are") ||
    m.includes("which") ||
    m.includes("list") ||
    m.includes("show") ||
    m.includes("counties") ||
    m.includes("locations")
  );
}

function isAverageUdaRateQuestion(message) {
  const m = String(message || "").toLowerCase();
  if (!m.includes("uda")) return false;
  if (!m.includes("rate")) return false;
  return m.includes("average") || m.includes("avg") || m.includes("mean");
}

function isPracticeValueQuestion(message) {
  const m = String(message || "").toLowerCase();
  // common phrasings seen in DDV usage
  if (m.includes("grand total")) return true;
  if (m.includes("practice value")) return true;
  if (m.includes("total value")) return true;
  if (m.includes("valuation")) return true;
  return false;
}

function requestedAgg(message) {
  const m = String(message || "").toLowerCase();
  if (m.includes("average") || m.includes("avg") || m.includes("mean")) return "avg";
  if (m.includes("median")) return "median";
  if (m.includes("minimum") || m.includes("min")) return "min";
  if (m.includes("maximum") || m.includes("max")) return "max";
  if (m.includes("count") || m.includes("how many")) return "count";
  if (m.includes("sum") || m.includes("total")) return "sum";
  return null;
}

function supabaseHeaders() {
  if (!SUPABASE_URL) {
    const err = new Error("Missing server env: SUPABASE_URL");
    err.statusCode = 500;
    throw err;
  }
  if (!SUPABASE_SERVICE_ROLE_KEY) {
    const err = new Error("Missing server env: SUPABASE_SERVICE_ROLE_KEY");
    err.statusCode = 500;
    throw err;
  }
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
  };
}

async function fetchDistinctCounties() {
  const url = SUPABASE_URL.replace(/\/$/, "");
  const endpoint = `${url}/rest/v1/${SUPABASE_PRACTICES_TABLE}`;
  const params = new URLSearchParams({
    select: "county",
    county: "not.is.null",
    order: "county.asc",
    limit: "10000",
  });
  const resp = await fetch(`${endpoint}?${params.toString()}`, { headers: supabaseHeaders() });
  const text = await resp.text();
  if (!resp.ok) {
    const err = new Error(text || `Supabase query failed (${resp.status})`);
    err.statusCode = 500;
    throw err;
  }
  const rows = text ? JSON.parse(text) : [];
  const out = [];
  const seen = new Set();
  for (const r of Array.isArray(rows) ? rows : []) {
    const c = typeof r?.county === "string" ? r.county.trim() : "";
    if (!c) continue;
    const k = c.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(c);
  }
  return out;
}

async function fetchAvgUdaRateGbp() {
  const url = SUPABASE_URL.replace(/\/$/, "");
  const endpoint = `${url}/rest/v1/${SUPABASE_PRACTICES_TABLE}`;
  const params = new URLSearchParams({
    select: "uda_rate_gbp",
    uda_rate_gbp: "not.is.null",
    limit: "10000",
  });
  const resp = await fetch(`${endpoint}?${params.toString()}`, { headers: supabaseHeaders() });
  const text = await resp.text();
  if (!resp.ok) {
    const err = new Error(text || `Supabase query failed (${resp.status})`);
    err.statusCode = 500;
    throw err;
  }
  const rows = text ? JSON.parse(text) : [];
  let sum = 0;
  let n = 0;
  for (const r of Array.isArray(rows) ? rows : []) {
    const v = Number(r?.uda_rate_gbp);
    if (!Number.isFinite(v)) continue;
    sum += v;
    n += 1;
  }
  return { avg: n ? sum / n : null, count: n };
}

async function fetchNumericValues(field, extraParams) {
  const url = SUPABASE_URL.replace(/\/$/, "");
  const endpoint = `${url}/rest/v1/${SUPABASE_PRACTICES_TABLE}`;
  const params = new URLSearchParams({
    select: field,
    [field]: "not.is.null",
    limit: "10000",
    ...(extraParams || {}),
  });
  const resp = await fetch(`${endpoint}?${params.toString()}`, { headers: supabaseHeaders() });
  const text = await resp.text();
  if (!resp.ok) {
    const err = new Error(text || `Supabase query failed (${resp.status})`);
    err.statusCode = 500;
    throw err;
  }
  const rows = text ? JSON.parse(text) : [];
  const values = [];
  for (const r of Array.isArray(rows) ? rows : []) {
    const v = Number(r?.[field]);
    if (!Number.isFinite(v)) continue;
    values.push(v);
  }
  return values;
}

function aggNumeric(values, agg) {
  if (!Array.isArray(values) || !values.length) return null;
  if (agg === "count") return values.length;
  if (agg === "sum") return values.reduce((a, b) => a + b, 0);
  if (agg === "min") return values.reduce((a, b) => (a < b ? a : b), values[0]);
  if (agg === "max") return values.reduce((a, b) => (a > b ? a : b), values[0]);
  if (agg === "avg") return values.reduce((a, b) => a + b, 0) / values.length;
  if (agg === "median") {
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return null;
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });
  try {
    if (!ACCESS_TOKEN_SECRET) return res.status(500).json({ detail: "Missing server env: ACCESS_TOKEN_SECRET" });

    const token = getBearerToken(req);
    if (!token) return res.status(401).json({ detail: "Missing bearer token" });
    verifyAccessToken({ token, secret: ACCESS_TOKEN_SECRET });

    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const message = String(body?.message ?? "").trim();
    const messages = Array.isArray(body?.messages) ? body.messages : [];
    if (!message) return res.status(400).json({ detail: "Missing message" });

    // Lightweight "dimension listing" support without changing the DB RPC.
    // The current ddv_query_intent only returns numeric aggregates.
    if (isCountyListQuestion(message)) {
      const t0 = Date.now();
      const counties = await fetchDistinctCounties();
      const latency_ms = Date.now() - t0;
      if (!counties.length) {
        return res.status(200).json({ answer: "No results found for that question.", value: null, intent: null, latency_ms });
      }
      const preview = counties.slice(0, 30);
      const suffix = counties.length > preview.length ? ` (and ${counties.length - preview.length} more)` : "";
      const answer = `Counties (${counties.length}): ${preview.join(", ")}${suffix}.`;
      return res.status(200).json({ answer, value: counties.length, intent: { kind: "distinct", field: "county" }, latency_ms });
    }

    if (isAverageUdaRateQuestion(message)) {
      const t0 = Date.now();
      const { avg, count } = await fetchAvgUdaRateGbp();
      const latency_ms = Date.now() - t0;
      if (avg === null) {
        return res
          .status(200)
          .json({ answer: "No results found for that question.", value: null, intent: { kind: "avg", field: "uda_rate_gbp" }, latency_ms });
      }
      const rounded = Math.round(avg * 100) / 100;
      const answer = `Average UDA rate is £${rounded} (from ${count} practices with a UDA rate).`;
      return res
        .status(200)
        .json({ answer, value: avg, intent: { kind: "avg", field: "uda_rate_gbp", count }, latency_ms });
    }

    // Practice value / valuation metrics live in practices.* columns but are not yet supported by ddv_query_intent RPC.
    // We handle the common cases here without returning raw rows.
    if (isPracticeValueQuestion(message)) {
      // Answer-first: if the user doesn't specify an aggregation, default to avg.
      const agg = requestedAgg(message) || "avg";

      const t0 = Date.now();
      const field = "grand_total";
      const values = await fetchNumericValues(field);
      const out = aggNumeric(values, agg);
      const latency_ms = Date.now() - t0;
      if (out === null) {
        return res.status(200).json({
          answer: "No results found for that question.",
          value: null,
          intent: { kind: agg, field },
          latency_ms,
        });
      }
      const rounded = Math.round(Number(out) * 100) / 100;
      const label = agg === "sum" ? "Total (sum) practice value" : agg === "avg" ? "Average practice value" : `${agg} practice value`;
      const answer = `${label} (grand_total) is £${rounded}.`;
      return res.status(200).json({
        answer,
        value: Number(out),
        intent: { kind: agg, field, count: values.length },
        follow_ups: [
          "Show the total (sum) across all practices",
          "Show the median practice value",
          "Filter by county (e.g., Kent)",
        ],
        latency_ms,
      });
    }

    const apiKey = process.env.OPENAI_API_KEY || (await fetchSecret("openai_api_key"));
    if (!apiKey) {
      return res.status(503).json({
        detail:
          "Missing OpenAI configuration. Set OPENAI_API_KEY on Vercel, or store it in Supabase (app_secrets key='openai_api_key').",
      });
    }

    const client = new OpenAI({ apiKey });

    const system = `You translate questions into a strict JSON object for querying a Postgres table named 'practices'.

Return ONLY valid JSON that matches this schema:
{
  "metric": "associate_cost_amount" | "associate_cost_pct" | "surgery_count" | "turnover_gbp" | "cert_associates_gbp" | "cert_associates_percent",
  "agg": "avg" | "median" | "min" | "max" | "count",
  "filters": [
     {"field": "county" | "city" | "postcode" | "surgery_count" | "accounts_period_end" | "visited_on", "op": "=" | "in" | ">=" | "<=" | "between", "value": <any>}
  ],
  "group_by": ["county" | "city" | "postcode" | "surgery_count" | "accounts_period_end" | "visited_on"],
  "limit": <int>
}

Rules:
- Prefer "=" for single-value filters.
- For surgery count, use an integer.
- For county and city, use title case (e.g. "Kent", "Essex", "London").
- Use limit <= 200 unless asked for more.
- If asked for an average, use agg="avg" and metric accordingly.
- If the user asks "how many practices" / "how many are there" / "count practices", set agg="count" and add the appropriate geography filters (county/city/postcode) if mentioned.
- If the user asks a question you cannot represent with the schema, still return JSON but set agg="count", metric="associate_cost_amount", and add no filters.`;

    const t0 = Date.now();
    const resp = await client.responses.create({
      model: "gpt-4o-mini",
      input: [
        { role: "system", content: system },
        // Include recent conversation for better intent disambiguation.
        // Keep it bounded to avoid token blow-ups.
        ...messages.slice(-20),
        { role: "user", content: `Question: ${message}` },
      ],
    });
    const text = resp.output_text || "";

    let intent;
    try {
      const extracted = extractJsonObject(text);
      intent = JSON.parse(extracted);
    } catch (e) {
      return res.status(400).json({ detail: `LLM returned non-JSON: ${String(e)}. Text=${text.slice(0, 300)}` });
    }

    // Execute against Supabase via RPC to avoid returning raw practice rows.
    const out = await callRpc("ddv_query_intent", { intent });
    const value = out?.value ?? null;
    const n = out?.n ?? null;
    const nullExcluded = out?.null_excluded ?? null;

    const geoFilter = Array.isArray(intent?.filters)
      ? intent.filters.find((f) => f && (f.field === "county" || f.field === "city" || f.field === "postcode"))
      : null;
    const geoLabel = geoFilter?.field ? String(geoFilter.field) : null;
    const geoValue = geoFilter?.value != null ? String(geoFilter.value).replace(/^"|"$/g, "") : null;

    function fmtNumber(x) {
      if (x == null || x === "") return "";
      const nn = Number(x);
      if (!Number.isFinite(nn)) return String(x);
      return Number.isInteger(nn) ? String(nn) : nn.toFixed(2);
    }

    function metricLabel(m) {
      if (m === "turnover_gbp") return "turnover";
      if (m === "surgery_count") return "surgery count";
      if (m === "associate_cost_amount") return "associate cost";
      if (m === "associate_cost_pct") return "associate cost (%)";
      if (m === "cert_associates_gbp") return "associate wages (certified accounts)";
      if (m === "cert_associates_percent") return "associate wages (% of income)";
      return String(m || "value");
    }

    // Deterministic, answer-first response (no follow-up questions).
    let answer;
    if (value === null) {
      answer = "No results found for that question.";
    } else if (intent?.agg === "count") {
      if (geoLabel && geoValue) {
        const loc = geoLabel === "postcode" ? `for postcode ${geoValue}` : `in ${geoValue}`;
        answer = `There are ${fmtNumber(value)} practices ${loc}.`;
      } else {
        answer = `There are ${fmtNumber(value)} practices.`;
      }
    } else {
      const loc = geoLabel && geoValue ? (geoLabel === "postcode" ? ` for postcode ${geoValue}` : ` in ${geoValue}`) : "";
      answer = `The ${intent?.agg || "avg"} ${metricLabel(intent?.metric)}${loc} is ${fmtNumber(value)}.`;
    }

    const follow_ups = [];
    if (intent?.agg === "count") {
      follow_ups.push("Break this down by surgery count");
      follow_ups.push("What is the average turnover in this area?");
      follow_ups.push("What is the median associate wage (% of income) in this area?");
    } else {
      follow_ups.push("Show the median instead");
      follow_ups.push("Filter to 2-surgery practices");
      follow_ups.push("Count practices matching these filters");
    }

    const latency_ms = Date.now() - t0;

    return res.status(200).json({ answer, value, n, null_excluded: nullExcluded, intent, follow_ups, latency_ms });
  } catch (e) {
    const status = Number(e?.statusCode || 500);
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};

