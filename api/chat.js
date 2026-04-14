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

function isAverageUdaRateQuestion(message) {
  const m = String(message || "").toLowerCase();
  if (!m.includes("uda")) return false;
  if (!m.includes("rate")) return false;
  return m.includes("average") || m.includes("avg") || m.includes("mean");
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
  "metric": "practice_count" | "associate_cost_amount" | "associate_cost_pct" | "surgery_count" | "turnover_gbp" | "cert_associates_gbp" | "cert_associates_percent" | "grand_total" | "goodwill" | "efandf" | "total" | "freehold",
  "agg": "avg" | "median" | "min" | "max" | "count" | "sum",
  "filters": [
     {"field": "county" | "city" | "postcode" | "surgery_count" | "accounts_period_end" | "visited_on", "op": "=" | "in" | ">=" | "<=" | "between", "value": <any>}
  ],
  "group_by": ["county" | "city" | "postcode" | "surgery_count" | "accounts_period_end" | "visited_on"],
  "order_by": {"by": "value" | "county" | "city" | "postcode" | "surgery_count" | "accounts_period_end" | "visited_on", "dir": "asc" | "desc"},
  "limit": <int>
}

Rules:
- Prefer "=" for single-value filters.
- For surgery count, use an integer.
- For county and city, use title case (e.g. "Kent", "Essex", "London").
- If the user says "in London" (or another city), treat it as a city filter (field="city"), not county.
- If the user asks about "number of surgeries" / "surgeries per practice", use metric="surgery_count".
- Use limit <= 200 unless asked for more.
- If the user asks for "most"/"top"/"highest" within a group (e.g. "Which county has the most practices?"), set group_by=["county"], agg="count", metric="practice_count", order_by={"by":"value","dir":"desc"}, and limit=1.
- If the user asks "list counties" or "what counties do we have", set group_by=["county"], agg="count", metric="practice_count", order_by={"by":"county","dir":"asc"}, and limit=1000.
- If the user asks for "practice value" / "value of a practice" / "valuation" / "grand total", use metric="grand_total" (GBP) unless they explicitly ask for goodwill/freehold/etc.
- If the user asks for "average practice value" or "average value of a practice", use agg="avg" and metric="grand_total".
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
    const rows = Array.isArray(out?.rows) ? out.rows : null;
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
      if (m === "grand_total") return "practice value";
      if (m === "goodwill") return "goodwill value";
      if (m === "efandf") return "equipment/fittings value";
      if (m === "total") return "total value";
      if (m === "freehold") return "freehold value";
      return String(m || "value");
    }

    // Deterministic, answer-first response (no follow-up questions).
    let answer;
    if (rows && rows.length) {
      const preview = rows.slice(0, 15);
      const lines = preview.map((r, i) => {
        const g = r?.group && typeof r.group === "object" ? r.group : {};
        const parts = Object.entries(g).map(([k, v]) => `${k}=${v}`);
        const gtxt = parts.length ? parts.join(", ") : "group";
        return `${i + 1}. ${gtxt}: ${fmtNumber(r?.value)}`;
      });
      answer = `Top results:\n${lines.join("\n")}`;
    } else if (value === null) {
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
      if (intent?.metric === "surgery_count" && Number(value) > 100) {
        answer += " (This looks unusually high for surgery count—check the interpreted intent/filters.)";
      }
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

    return res.status(200).json({ answer, value, rows, n, null_excluded: nullExcluded, intent, follow_ups, latency_ms });
  } catch (e) {
    const status = Number(e?.statusCode || 500);
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};

