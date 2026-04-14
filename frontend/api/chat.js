const OpenAI = require("openai");
const { fetchSecret, callRpc } = require("./_lib/supabase");
const { verifyAccessToken } = require("./_lib/token");

const ACCESS_TOKEN_SECRET = process.env.ACCESS_TOKEN_SECRET || "";

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
      const n = Number(x);
      if (!Number.isFinite(n)) return String(x);
      // integers as-is; otherwise 2dp
      return Number.isInteger(n) ? String(n) : n.toFixed(2);
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
        // "county" => "in Essex", "city" => "in London", "postcode" => "for postcode SW1A 1AA"
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
      if (geoLabel !== "county") follow_ups.push("How many practices are in Kent?");
      follow_ups.push("What is the average turnover in this area?");
    } else {
      follow_ups.push("Show the median instead");
      follow_ups.push("Filter to 2-surgery practices");
      follow_ups.push("Count practices matching these filters");
    }

    const latency_ms = Date.now() - t0;

    return res.status(200).json({
      answer,
      value,
      n,
      null_excluded: nullExcluded,
      intent,
      follow_ups,
      latency_ms,
    });
  } catch (e) {
    const status = Number(e?.statusCode || 500);
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};

