const { insertRow, countRows } = require("../_lib/supabase");
const { setCors, checkRateLimit, parseBody } = require("../report/_lib");

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PHONE_RE = /^[0-9+()\s-]{7,20}$/;

const ROLES = ["principal", "partner", "practice_manager", "other"];
const PRACTICE_TYPES = ["nhs", "private", "mixed"];
const TURNOVER_BANDS = ["under_500k", "500k_1m", "1m_2m", "2m_5m", "over_5m"];
const YEARS_OWNED = ["under_2", "2_5", "5_10", "over_10"];
const OVERHEAD_BANDS = ["under_100k", "100k_250k", "250k_500k", "over_500k", "not_sure"];
const INVOICE_ACCESS = ["yes_myself", "yes_with_help", "not_sure"];
const DECISION_MAKER = ["sole", "shared", "not_me"];
const TIMELINES = ["at_launch", "3_months", "6_months", "curious"];
const CATEGORIES = [
  "materials",
  "labs",
  "utilities",
  "insurance",
  "card_banking",
  "telecoms_it",
  "clinical_waste",
  "equipment",
  "cleaning_facilities",
  "finance_leasing",
  "not_sure",
];

function bad(message) {
  const err = new Error(message);
  err.statusCode = 400;
  return err;
}

function requiredText(value, label, max = 160) {
  const text = String(value ?? "").trim();
  if (!text || text.length > max) throw bad(`Please provide ${label}`);
  return text;
}

function requiredChoice(value, allowed, label) {
  const text = String(value ?? "").trim();
  if (!allowed.includes(text)) throw bad(`Please select ${label}`);
  return text;
}

function validate(body) {
  const name = requiredText(body?.name, "your full name", 120);
  const role = requiredChoice(body?.role, ROLES, "your role");
  const email = String(body?.email ?? "").trim().toLowerCase();
  if (!email || email.length > 254 || !EMAIL_RE.test(email)) {
    throw bad("Please enter a valid email address");
  }
  const phone = String(body?.phone ?? "").trim();
  if (!PHONE_RE.test(phone)) throw bad("Please enter a valid phone number");

  const practice_name = requiredText(body?.practiceName, "your practice name", 160);
  const location = requiredText(body?.location, "your practice location", 120);
  const surgery_count = Number(body?.surgeryCount);
  if (!Number.isInteger(surgery_count) || surgery_count < 1 || surgery_count > 60) {
    throw bad("Please select your number of surgeries");
  }
  const practice_type = requiredChoice(body?.practiceType, PRACTICE_TYPES, "your practice type");
  const turnover_band = requiredChoice(body?.turnoverBand, TURNOVER_BANDS, "your turnover band");
  const years_owned = requiredChoice(body?.yearsOwned, YEARS_OWNED, "how long you have owned the practice");

  const rawCategories = Array.isArray(body?.categories) ? body.categories : [];
  const categories = [...new Set(rawCategories.map((c) => String(c).trim()))].filter((c) =>
    CATEGORIES.includes(c)
  );
  if (!categories.length) throw bad("Please select at least one cost category");

  const overhead_band = requiredChoice(body?.overheadBand, OVERHEAD_BANDS, "your overhead spend band");
  const invoice_access = requiredChoice(body?.invoiceAccess, INVOICE_ACCESS, "your invoice access");
  const decision_maker = requiredChoice(body?.decisionMaker, DECISION_MAKER, "who makes the decision");
  const timeline = requiredChoice(body?.timeline, TIMELINES, "your timeline");

  const motivation = String(body?.motivation ?? "").trim();
  if (motivation.length < 80 || motivation.length > 2000) {
    throw bad("Please tell us why you want a founding place (at least 80 characters)");
  }

  if (body?.consent !== true) throw bad("Please confirm you are happy to be contacted");

  return {
    name,
    role,
    email,
    phone,
    practice_name,
    location,
    surgery_count,
    practice_type,
    turnover_band,
    years_owned,
    categories,
    overhead_band,
    invoice_access,
    decision_maker,
    timeline,
    motivation,
    consent: true,
  };
}

module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "POST") return res.status(405).json({ detail: "Method not allowed" });

  try {
    checkRateLimit(req);
    const body = parseBody(req);

    // Honeypot: silently accept bot submissions without storing them.
    if (String(body?.website ?? "").trim()) {
      return res.status(200).json({ ok: true, position: null });
    }

    const row = validate(body);
    await insertRow("profit_waitlist", row);
    const position = await countRows("profit_waitlist");

    return res.status(200).json({ ok: true, position });
  } catch (e) {
    const status = e?.statusCode || 500;
    return res.status(status).json({ detail: String(e?.message || e) });
  }
};
