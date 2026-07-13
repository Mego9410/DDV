const { insertRow } = require("./_lib/supabase");
const { verifyAccessToken } = require("./_lib/token");

const ACCESS_TOKEN_SECRET = (process.env.ACCESS_TOKEN_SECRET || "").trim() || "ddv-dev-access-token-secret";
const MAX_DESCRIPTION_CHARS = 4000;
const MAX_THREAD_TURNS = 40;
const MAX_CONTENT_CHARS = 8000;

function getBearerToken(req) {
  const h = req.headers?.authorization || req.headers?.Authorization || "";
  const s = Array.isArray(h) ? h[0] : String(h);
  if (!s.toLowerCase().startsWith("bearer ")) return null;
  return s.slice(7).trim();
}

function sanitizeThread(messages) {
  if (!Array.isArray(messages)) return [];
  return messages
    .filter((m) => m && (m.role === "user" || m.role === "assistant") && typeof m.content === "string")
    .slice(-MAX_THREAD_TURNS)
    .map((m) => ({
      role: m.role,
      content: String(m.content).slice(0, MAX_CONTENT_CHARS),
    }));
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });

  try {
    const token = getBearerToken(req);
    if (!token) return res.status(401).json({ detail: "Missing bearer token" });
    verifyAccessToken({ token, secret: ACCESS_TOKEN_SECRET });

    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const description = String(body?.description ?? "").trim();
    if (!description) return res.status(400).json({ detail: "Description is required" });
    if (description.length > MAX_DESCRIPTION_CHARS) {
      return res.status(400).json({ detail: `Description must be under ${MAX_DESCRIPTION_CHARS} characters` });
    }

    const chat_thread = sanitizeThread(body?.chat_thread);
    const user_agent = typeof body?.user_agent === "string" ? body.user_agent.slice(0, 500) : null;
    const page_url = typeof body?.page_url === "string" ? body.page_url.slice(0, 1000) : null;

    const row = await insertRow("bug_reports", {
      description,
      chat_thread,
      user_agent,
      page_url,
      status: "open",
    });

    return res.status(201).json({
      id: row?.id ?? null,
      created_at: row?.created_at ?? null,
      status: row?.status ?? "open",
    });
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
