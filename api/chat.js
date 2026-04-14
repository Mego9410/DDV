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
      const agg = requestedAgg(message);
      // If user doesn't specify aggregation, ask before computing.
      if (!agg || agg === "count") {
        const latency_ms = 0;
        return res.status(200).json({
          answer: "For practice value, do you want the average per practice, or the total (sum) across all practices?",
          value: null,
          intent: { kind: "clarify", topic: "practice_value_agg" },
          needs_clarification: true,
          suggestions: ["Average grand total per practice", "Sum of grand totals across all practices"],
          latency_ms,
        });
      }

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

If the question is ambiguous or underspecified, you MUST ask a clarifying question first by returning ONLY JSON:
{
  "clarify": "<your question to the user>",
  "suggestions": ["<suggestion 1>", "<suggestion 2>", "<suggestion 3>"]
}
Do NOT include metric/agg/filters when you return "clarify", and do NOT request or reveal any raw rows.

Otherwise, return ONLY valid JSON that matches this schema:
{
  "metric": "associate_cost_amount" | "associate_cost_pct",
  "agg": "avg" | "median" | "min" | "max" | "count",
  "filters": [
     {"field": "county" | "surgery_count" | "accounts_period_end", "op": "=" | "in" | ">=" | "<=" | "between", "value": <any>}
  ],
  "group_by": ["county" | "surgery_count" | "accounts_period_end"],
  "limit": <int>
}

Rules:
- Prefer "=" for single-value filters.
- For surgery count, use an integer.
- For county, use title case (e.g. "Kent").
- Use limit <= 200 unless asked for more.
- If asked for an average, use agg="avg" and metric accordingly.
- If the user asks a question you cannot represent with the schema, return a "clarify" question instead.`;

    const t0 = Date.now();
    const resp = await client.responses.create({
      model: "gpt-4o-mini",
      input: [
        { role: "system", content: system },
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

    if (intent && typeof intent === "object" && typeof intent.clarify === "string" && intent.clarify.trim()) {
      const latency_ms = Date.now() - t0;
      const suggestions = Array.isArray(intent.suggestions) ? intent.suggestions.filter((s) => typeof s === "string" && s.trim()) : [];
      return res.status(200).json({
        answer: intent.clarify.trim(),
        value: null,
        intent: { kind: "clarify", clarify: intent.clarify.trim(), suggestions },
        needs_clarification: true,
        suggestions,
        latency_ms,
      });
    }

    // Execute against Supabase via RPC to avoid returning raw practice rows.
    const out = await callRpc("ddv_query_intent", { intent });
    const value = out?.value ?? null;

    // Natural language response (LLM) from question + computed value.
    // If the LLM fails, fall back to a simple deterministic answer.
    let answer = value === null ? "No results found for that question." : `Result: ${value}`;
    try {
      const resp2 = await client.responses.create({
        model: "gpt-4o-mini",
        input: [
          {
            role: "system",
            content:
              "You are a concise data assistant. Write a single-sentence natural language answer for the user. " +
              "Do not mention SQL, JSON, models, or internal implementation. If value is null, say no results.",
          },
          {
            role: "user",
            content: JSON.stringify({ question: message, intent, value }),
          },
        ],
      });
      const nl = String(resp2.output_text || "").trim();
      if (nl) answer = nl;
    } catch (_) {}

    const latency_ms = Date.now() - t0;

    return res.status(200).json({ answer, value, intent, latency_ms });
  } catch (e) {
    const status = Number(e?.statusCode || 500);
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};

