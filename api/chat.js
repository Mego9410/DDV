const fs = require("fs");
const path = require("path");
const OpenAI = require("openai");
const { fetchSecret, callRpc } = require("./_lib/supabase");
const { verifyAccessToken } = require("./_lib/token");

const ACCESS_TOKEN_SECRET = process.env.ACCESS_TOKEN_SECRET || "";
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
    "Money columns are GBP; percent columns are 0-100. Geography (`city`, `county`) is messy free text - match with ILIKE.",
    "Use `grand_total` for practice value, `cert_income_gbp` for turnover, `cert_net_profit_gbp` for profit, `surgery_count` for surgeries.",
    "Always exclude NULLs from aggregates and report the sample size.",
  ].join("\n");
}

const DATA_DOCS = loadDataDocs();

function buildSystemPrompt() {
  return `You are DDV's internal data analyst assistant. You answer questions about DDV's
dental-practice dataset by querying a read-only Postgres database and explaining
what the numbers mean, in clear natural language.

You have one tool: run_sql(query) - it runs a single read-only SQL SELECT/WITH
statement against the database and returns rows as JSON. Use it to gather every
fact you need. You may call it multiple times in one turn to explore the data,
check how values are spelled, and then compute the final figures.

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

module.exports = async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });
  try {
    if (!ACCESS_TOKEN_SECRET) return res.status(500).json({ detail: "Missing server env: ACCESS_TOKEN_SECRET" });

    const token = getBearerToken(req);
    if (!token) return res.status(401).json({ detail: "Missing bearer token" });
    verifyAccessToken({ token, secret: ACCESS_TOKEN_SECRET });

    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const message = String(body?.message ?? "").trim();
    const history = sanitizeHistory(body?.messages);
    if (!message) return res.status(400).json({ detail: "Missing message" });

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
        tools: [RUN_SQL_TOOL],
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
        if (call?.type !== "function" || call.function?.name !== "run_sql") {
          messages.push({ role: "tool", tool_call_id: call.id, content: "Unsupported tool." });
          continue;
        }
        let query = "";
        try {
          const args = JSON.parse(call.function.arguments || "{}");
          query = String(args.query || "").trim();
        } catch (_) {
          messages.push({ role: "tool", tool_call_id: call.id, content: "Could not parse tool arguments as JSON." });
          continue;
        }

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
