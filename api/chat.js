const fs = require("fs");
const path = require("path");
const OpenAI = require("openai");
const { fetchSecret, callRpc } = require("./_lib/supabase");
const { verifyAccessToken } = require("./_lib/token");

const ACCESS_TOKEN_SECRET = (process.env.ACCESS_TOKEN_SECRET || "").trim() || "ddv-dev-access-token-secret";
// gpt-4o-mini is too weak for analytical reasoning + SQL; default to a stronger
// model. Override with OPENAI_MODEL (e.g. "gpt-4.1" or "gpt-4o").
const MODEL = process.env.OPENAI_MODEL || "gpt-4o";
// How many tool round-trips the agent may take before it must answer.
const MAX_STEPS = Number(process.env.CHAT_MAX_STEPS || 6);
// Hard cap on how much SQL output we feed back to the model per call.
const MAX_TOOL_RESULT_CHARS = 45000;
// How many prior turns of conversation to keep for follow-up context.
const MAX_HISTORY_TURNS = 20;

// ---- Load the markdown context docs that teach the model about the data ----
// These are the source of truth the agent uses to understand the schema and
// business meaning. They are bundled with the function (see vercel.json
// includeFiles). Loaded once per cold start.
function loadDataDocs() {
  try {
    const dir = path.join(__dirname, "context");
    const files = fs
      .readdirSync(dir)
      .filter((f) => f.toLowerCase().endsWith(".md"))
      .sort();
    const parts = files.map((f) => fs.readFileSync(path.join(dir, f), "utf8"));
    const joined = parts.join("\n\n---\n\n").trim();
    if (joined) return joined;
  } catch (_) {
    // fall through to the minimal fallback below
  }
  return [
    "# DDV data (fallback context)",
    "Query the Postgres table `public.practices` (one row per practice, ~720 rows, the full population).",
    "Money columns are GBP; percent columns are 0-100. Geography (`city`, `county`) is messy free text - match case-insensitively and whitespace-tolerant (e.g. `lower(btrim(city)) = lower(btrim('Manchester'))` or `btrim(city) ILIKE '%manchester%'`).",
    "Use `grand_total` for practice value, `cert_income_gbp` for turnover, `cert_net_profit_gbp` for profit, `surgery_count` for surgeries.",
    "Always exclude NULLs from aggregates and report the sample size.",
  ].join("\n");
}

const DATA_DOCS = loadDataDocs();

function buildSystemPrompt() {
  return `You are DDV's internal data analyst assistant. You answer questions about DDV's
dental-practice dataset by querying a read-only Postgres database and explaining
what the numbers mean, in clear natural language.

You have two tools:
- run_sql(query): runs a single read-only SQL SELECT/WITH statement and returns
  rows as JSON. Use it to gather every fact you need. You may call it multiple
  times in one turn to explore, check spellings, then compute the final figures.
- geocode_place(place): resolves a UK place name or postcode to { lat, lng }.
  Use it for ANY distance/radius question, then filter with st_dwithin.

Hard rules:
- Base EVERY number you state on a query you actually ran this turn. Never guess
  or rely on memory for figures.
- Read-only: only SELECT / WITH statements. No writes of any kind.
- To "analyse all the data", aggregate over the whole table in SQL (e.g. avg,
  count, percentile_cont) - that already considers every row. Do not try to list
  all rows.
- Always exclude NULLs from numeric aggregates and report the sample size (n).
- When the user's geography or wording is fuzzy, first run a quick exploratory
  query (DISTINCT / GROUP BY / ILIKE) to see the real values, then aggregate.
  For place names, check both 'city' and 'county' (they are messy and often swapped).
- Distance / radius questions ("within X miles of Y", "the Y area", "near Y"):
  call geocode_place(Y) to get coordinates, then count/aggregate with
  st_dwithin(geog, st_setsrid(st_makepoint(lng, lat), 4326)::geography, miles * 1609.344).
  Only practices with a non-null geog can be matched: state how many were excluded
  for missing coordinates (count(*) where geog is null). If geocode_place fails,
  say you couldn't locate the place - do NOT return 0 as if it were the answer.
- Money columns: a value of 0 usually means "not provided", not a true zero. For
  "cheapest/lowest/minimum" and similar, exclude 0 (and NULL) and say so.
- Percentage columns contain dirty outliers (e.g. >100%). Sanity-bound them
  (e.g. where col between 0 and 100) for typical/average questions and mention it.
- Date columns (accounts_period_end and its _prev) contain some corrupt values;
  this table is latest-only with no real time series. Don't infer multi-year
  trends beyond the single _prev (prior-year) columns.
- If asked for data that isn't in the schema (e.g. headcount, patient numbers,
  satisfaction), say it isn't in the dataset. Never invent a column or a number.
- If a question is ambiguous, make the most defensible assumption, proceed, and
  state the assumption. Do not interrogate the user with forms.
- Use the conversation so far to resolve follow-up questions.

Answer style: lead with the direct, conclusion-first answer, then briefly show
the supporting evidence (column used, filters, n, any outliers/nulls excluded).
Format GBP like £1,234,567. Be analytical and concise.

Below is the data dictionary you must rely on.

${DATA_DOCS}`;
}

const SYSTEM_PROMPT = buildSystemPrompt();

const RUN_SQL_TOOL = {
  type: "function",
  function: {
    name: "run_sql",
    description:
      "Run a single read-only SQL SELECT/WITH query against the DDV Postgres database and return matching rows as JSON. Use Postgres syntax. The main table is public.practices.",
    parameters: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description:
            "A single read-only SQL statement (SELECT or WITH only). No semicolons-separated multiple statements, no writes.",
        },
      },
      required: ["query"],
      additionalProperties: false,
    },
  },
};

const GEOCODE_TOOL = {
  type: "function",
  function: {
    name: "geocode_place",
    description:
      "Resolve a UK place name or postcode to { lat, lng } using postcodes.io. Use this for any distance/radius question (e.g. 'within 20 miles of Chingford') BEFORE writing SQL, then filter with st_dwithin on public.practices.geog. Returns { error } if the place cannot be located.",
    parameters: {
      type: "object",
      properties: {
        place: {
          type: "string",
          description: "A UK town/city/area name or a UK postcode (e.g. 'Brighton' or 'SW1A 1AA').",
        },
      },
      required: ["place"],
      additionalProperties: false,
    },
  },
};

const CHAT_TOOLS = [RUN_SQL_TOOL, GEOCODE_TOOL];

function getBearerToken(req) {
  const h = req.headers?.authorization || req.headers?.Authorization || "";
  const s = Array.isArray(h) ? h[0] : String(h);
  if (!s.toLowerCase().startsWith("bearer ")) return null;
  return s.slice(7).trim();
}

function sanitizeHistory(messages) {
  if (!Array.isArray(messages)) return [];
  return messages
    .filter((m) => m && (m.role === "user" || m.role === "assistant") && typeof m.content === "string")
    .slice(-MAX_HISTORY_TURNS)
    .map((m) => ({ role: m.role, content: m.content }));
}

async function runSql(query) {
  const out = await callRpc("ddv_run_select", { q: query });
  let text = JSON.stringify(out ?? []);
  let truncated = false;
  if (text.length > MAX_TOOL_RESULT_CHARS) {
    text = text.slice(0, MAX_TOOL_RESULT_CHARS);
    truncated = true;
  }
  const rowCount = Array.isArray(out) ? out.length : null;
  return { text, truncated, rowCount };
}

// Default radius (miles) for vague "<place> area" / "near <place>" questions
// without an explicit distance. Matches the guidance given to the model.
const DEFAULT_AREA_RADIUS_MILES = 25;

// Words that signal the user wants a metric/filter beyond a plain practice count.
// If any of these co-occur with a radius, we DON'T use the simple count fast-path;
// instead we let the analyst model compose the full SQL (it can geocode via the
// geocode_place tool), so compound questions like
// "average turnover of 3-surgery practices within 30 miles of Bristol" work.
const NON_COUNT_SIGNAL_RE =
  /\b(turnover|revenue|income|profit|margin|valuation|value|worth|goodwill|freehold|surger|nhs|private|denplan|uda|associate|wage|material|lab|average|avg|median|mean|sum|total|highest|lowest|max|min|top|per\s+surgery|breakdown|group)\b/i;

// Trim trailing qualifier clauses from a captured place name so geocoding gets a
// clean token (e.g. "Bristol with turnover over 1m" -> "Bristol").
function cleanPlace(raw) {
  let p = String(raw || "").trim();
  // Cut at the first joining/qualifier keyword.
  p = p.split(/\s+(?:and|with|that|which|who|whose|having|where|but)\b/i)[0];
  // Cut at punctuation that introduces another clause.
  p = p.split(/[,.;:]/)[0];
  return p.replace(/['"]/g, "").trim();
}

// Parse a radius/area intent out of a natural-language question.
// Returns { radiusMiles, place, explicitRadius } or null.
function parseRadiusQuestion(q) {
  const s = String(q || "").trim();

  // 1) Explicit distance: "within 20 miles of Chingford"
  let m = s.match(/\bwithin\s+(\d+(?:\.\d+)?)\s*(?:miles?|mi)\s+(?:of|from|around)\s+(.+?)\s*\??$/i);
  if (m) return { radiusMiles: Number(m[1]), place: cleanPlace(m[2]), explicitRadius: true };

  // 2) Explicit distance: "30 miles around/near/from/of Brighton"
  m = s.match(/\b(\d+(?:\.\d+)?)\s*(?:miles?|mi)\s+(?:around|near|from|of)\s+(.+?)\s*\??$/i);
  if (m) return { radiusMiles: Number(m[1]), place: cleanPlace(m[2]), explicitRadius: true };

  // 3) Vague area, no distance: "the Manchester area", "in the Brighton area"
  m = s.match(/\b(?:in|around|near|the)\s+(.+?)\s+area\b/i) || s.match(/\b(.+?)\s+area\b/i);
  if (m) {
    const place = cleanPlace(m[1].replace(/^(?:the|in|around|near)\s+/i, ""));
    // Skip if it looks like a money/number phrase ("around the £1m mark") rather
    // than a real place name - let the analyst model handle those.
    if (place && !/[£$\d]/.test(place)) {
      return { radiusMiles: DEFAULT_AREA_RADIUS_MILES, place, explicitRadius: false };
    }
  }

  // 4) "near/around <place>" with no distance.
  m = s.match(/\b(?:near|around|close to|surrounding)\s+(.+?)\s*\??$/i);
  if (m) {
    const place = cleanPlace(m[1]);
    if (place && !/[£$\d]/.test(place)) {
      return { radiusMiles: DEFAULT_AREA_RADIUS_MILES, place, explicitRadius: false };
    }
  }

  return null;
}

// True when the question is essentially "how many practices ..." with no other
// metric/filter. Only then is the deterministic radius count fast-path safe.
function isSimplePracticeCount(q) {
  const s = String(q || "");
  if (NON_COUNT_SIGNAL_RE.test(s)) return false;
  return /\bhow\s+many\b/i.test(s) || /\b(count|number\s+of)\b/i.test(s) || /\bpractices?\b/i.test(s);
}

async function geocodePlaceToLatLng(place) {
  const raw = String(place || "").trim();
  if (!raw) return null;

  // If it looks like a UK postcode, use postcodes.io postcode endpoint
  const pc = raw.replace(/\s+/g, "").toUpperCase();
  if (/^[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2}$/.test(pc) || /^[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2}$/.test(raw.toUpperCase().replace(/\s+/g, ""))) {
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

  // Otherwise try postcodes.io places endpoint (best-effort)
  const resp = await fetch(`https://api.postcodes.io/places?q=${encodeURIComponent(raw)}`);
  if (!resp.ok) return null;
  const data = await resp.json().catch(() => null);
  const r = Array.isArray(data?.result) ? data.result[0] : null;
  const lat = Number(r?.latitude);
  const lng = Number(r?.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  return { lat, lng };
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });
  try {
    const token = getBearerToken(req);
    if (!token) return res.status(401).json({ detail: "Missing bearer token" });
    verifyAccessToken({ token, secret: ACCESS_TOKEN_SECRET });

    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const message = String(body?.message ?? "").trim();
    const history = sanitizeHistory(body?.messages);
    if (!message) return res.status(400).json({ detail: "Missing message" });

    // Fast-path: deterministic radius questions (avoids the LLM saying "we can't
    // do distance"). Only used for simple "how many practices" counts; compound
    // questions (metric/filter + radius) fall through to the analyst model, which
    // can geocode via the geocode_place tool and compose the full SQL itself.
    const radius = parseRadiusQuestion(message);
    if (
      radius &&
      Number.isFinite(radius.radiusMiles) &&
      radius.radiusMiles > 0 &&
      radius.place &&
      isSimplePracticeCount(message)
    ) {
      const t0 = Date.now();
      const center = await geocodePlaceToLatLng(radius.place);
      if (!center) {
        return res.status(200).json({
          answer: `I couldn't locate "${radius.place}" on the map, so I can't run a distance search for it. Try a more specific place name or a nearby UK postcode.`,
          sql: [],
          model: "radius-fast-path",
          latency_ms: Date.now() - t0,
        });
      }

      const intent = {
        metric: "practice_count",
        agg: "count",
        filters: [
          {
            field: "near",
            op: "within_miles",
            value: { lat: center.lat, lng: center.lng, radius_miles: radius.radiusMiles },
          },
        ],
      };
      const out = await callRpc("ddv_query_intent", { intent });

      if (out?.geo_unresolved) {
        return res.status(200).json({
          answer: `I couldn't locate "${radius.place}" precisely enough to run a distance search.`,
          sql: [],
          intent,
          model: "radius-fast-path",
          latency_ms: Date.now() - t0,
        });
      }

      const value = Number(out?.value ?? 0);
      const n = out?.n ?? value;
      const missing = Number(out?.geo_missing ?? 0);
      const phrase = radius.explicitRadius
        ? `within ${radius.radiusMiles} miles of ${radius.place}`
        : `in the ${radius.place} area (within ~${radius.radiusMiles} miles)`;
      let answer = `There ${value === 1 ? "is" : "are"} ${value} practice${value === 1 ? "" : "s"} ${phrase}.`;
      if (missing > 0) {
        answer += ` (Note: ${missing} practice${missing === 1 ? "" : "s"} without mapped coordinates couldn't be included in the distance search.)`;
      }
      return res.status(200).json({
        answer,
        sql: [],
        intent,
        n,
        model: "radius-fast-path",
        latency_ms: Date.now() - t0,
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

    const messages = [
      { role: "system", content: SYSTEM_PROMPT },
      ...history,
      { role: "user", content: message },
    ];

    const t0 = Date.now();
    const executedSql = [];
    let answer = "";

    for (let step = 0; step < MAX_STEPS; step += 1) {
      const completion = await client.chat.completions.create({
        model: MODEL,
        temperature: 0.2,
        messages,
        tools: CHAT_TOOLS,
        tool_choice: "auto",
      });

      const choice = completion.choices?.[0];
      const msg = choice?.message;
      if (!msg) break;

      const toolCalls = Array.isArray(msg.tool_calls) ? msg.tool_calls : [];

      if (!toolCalls.length) {
        answer = (msg.content || "").trim();
        break;
      }

      // Record the assistant turn (with its tool calls) before answering each tool.
      messages.push(msg);

      for (const call of toolCalls) {
        if (call?.type !== "function") {
          messages.push({ role: "tool", tool_call_id: call.id, content: "Unsupported tool." });
          continue;
        }

        let args = {};
        try {
          args = JSON.parse(call.function?.arguments || "{}");
        } catch (_) {
          messages.push({ role: "tool", tool_call_id: call.id, content: "Could not parse tool arguments as JSON." });
          continue;
        }

        if (call.function?.name === "geocode_place") {
          const place = String(args.place || "").trim();
          if (!place) {
            messages.push({ role: "tool", tool_call_id: call.id, content: JSON.stringify({ error: "Empty place." }) });
            continue;
          }
          try {
            const center = await geocodePlaceToLatLng(place);
            const content = center
              ? JSON.stringify({ place, lat: center.lat, lng: center.lng })
              : JSON.stringify({ place, error: "Could not locate that place." });
            messages.push({ role: "tool", tool_call_id: call.id, content });
          } catch (e) {
            messages.push({ role: "tool", tool_call_id: call.id, content: JSON.stringify({ place, error: String(e?.message || e) }) });
          }
          continue;
        }

        if (call.function?.name !== "run_sql") {
          messages.push({ role: "tool", tool_call_id: call.id, content: "Unsupported tool." });
          continue;
        }

        const query = String(args.query || "").trim();
        if (!query) {
          messages.push({ role: "tool", tool_call_id: call.id, content: "Empty query." });
          continue;
        }

        executedSql.push(query);
        try {
          const { text, truncated, rowCount } = await runSql(query);
          const note = truncated
            ? `\n[Truncated: result was larger than ${MAX_TOOL_RESULT_CHARS} chars. Refine with aggregates/LIMIT.]`
            : "";
          const countNote = rowCount != null ? `Rows: ${rowCount}. ` : "";
          messages.push({ role: "tool", tool_call_id: call.id, content: `${countNote}${text}${note}` });
        } catch (e) {
          // Feed the error back so the model can self-correct its SQL.
          messages.push({
            role: "tool",
            tool_call_id: call.id,
            content: `SQL error: ${String(e?.message || e)}`,
          });
        }
      }
    }

    if (!answer) {
      // Loop exhausted without a final natural-language answer: ask the model to
      // summarise from what it has gathered, with tools disabled.
      try {
        const wrapUp = await client.chat.completions.create({
          model: MODEL,
          temperature: 0.2,
          messages: [
            ...messages,
            {
              role: "user",
              content:
                "Based only on the query results above, give your best concise answer now. Do not request more queries.",
            },
          ],
        });
        answer = (wrapUp.choices?.[0]?.message?.content || "").trim();
      } catch (_) {
        /* ignore; handled below */
      }
    }

    if (!answer) {
      answer =
        "I couldn't pull together a confident answer from the data for that question. Try rephrasing or narrowing it down.";
    }

    const latency_ms = Date.now() - t0;
    return res.status(200).json({ answer, sql: executedSql, model: MODEL, latency_ms });
  } catch (e) {
    const status = Number(e?.statusCode || 500);
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
