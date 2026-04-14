// API base:
// - Deployed on Vercel: use same-origin serverless routes under /api
// - Local dev: defaults to http://localhost:8010 (override via localStorage)
//   localStorage.setItem("DDV_API_BASE", "http://localhost:8000"); location.reload();
const OVERRIDE_API_BASE = localStorage.getItem("DDV_API_BASE");
const DEFAULT_LOCAL_API = "http://localhost:8010";
const API_BASE = ((OVERRIDE_API_BASE ?? (location.hostname.endsWith("vercel.app") ? "" : DEFAULT_LOCAL_API)) || "")
  .replace(/\/$/, "");
const TOKEN_KEY = "DDV_ACCESS_TOKEN";

const el = (id) => document.getElementById(id);

const hero = el("hero");
const chat = el("chat");
const chatScroll = el("chatScroll");
const composerForm = el("composerForm");
const composerInput = el("composerInput");
const composerCard = el("composerCard");
const chatForm = el("chatForm");
const chatInput = el("chatInput");
const btnLogout = el("btnLogout");

const passwordModal = el("passwordModal");
const passwordForm = el("passwordForm");
const passwordInput = el("passwordInput");
const passwordError = el("passwordError");

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

function setToken(token) {
  localStorage.setItem(TOKEN_KEY, token);
}

function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
}

function setLoggedInUI(loggedIn) {
  btnLogout.hidden = !loggedIn;
  if (loggedIn) {
    // Start in "small composer" mode. Chat stays collapsed until first submit.
    hero.hidden = false;
    if (composerCard) composerCard.hidden = false;
    chat.hidden = false;
    chat.classList.add("is-collapsed");
    chat.classList.remove("is-expanding", "is-expanded");
    setTimeout(() => composerInput?.focus(), 50);
  } else {
    hero.hidden = false;
    if (composerCard) composerCard.hidden = false;
    chat.hidden = true;
    setTimeout(() => composerInput?.focus(), 50);
  }
}

function expandChatFromHero() {
  // One-time transition: hero fades out, chat expands in.
  if (chat.classList.contains("is-expanded") || chat.classList.contains("is-expanding")) return;

  hero.classList.add("is-collapsing");
  chat.classList.remove("is-collapsed");
  chat.classList.add("is-expanding");

  // Allow layout to apply then mark expanded.
  requestAnimationFrame(() => {
    chat.classList.add("is-expanded");
    setTimeout(() => {
      chat.classList.remove("is-expanding");
      hero.hidden = true;
      hero.classList.remove("is-collapsing");
      setTimeout(() => chatInput?.focus(), 50);
    }, 560);
  });
}

function addBubble(kind, text, meta) {
  const wrap = document.createElement("div");
  wrap.className = `bubble ${kind}`;
  wrap.textContent = text;
  if (meta) {
    const m = document.createElement("div");
    m.className = "meta";
    m.textContent = meta;
    wrap.appendChild(m);
  }
  chatScroll.appendChild(wrap);
  chatScroll.scrollTop = chatScroll.scrollHeight;
}

async function verifyPassword(password) {
  const resp = await fetch(`${API_BASE}/api/access/verify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password }),
  });
  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(t || `Password verify failed (${resp.status})`);
  }
  return await resp.json();
}

async function sendMessage(message) {
  const token = getToken();
  const resp = await fetch(`${API_BASE}/api/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ message }),
  });
  if (!resp.ok) {
    const t = await resp.text();
    // If the token has expired (or is missing/invalid), force re-auth.
    if (resp.status === 401) {
      const err = new Error(t || "Unauthorized");
      err.authExpired = true;
      err.statusCode = 401;
      throw err;
    }
    throw new Error(t || `Chat failed (${resp.status})`);
  }
  return await resp.json();
}

async function ensureAuthed() {
  const token = getToken();
  if (token) {
    setLoggedInUI(true);
    return;
  }

  passwordError.hidden = true;
  passwordInput.value = "";
  passwordModal.showModal();
  setTimeout(() => passwordInput.focus(), 50);
}

passwordForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  passwordError.hidden = true;
  const pw = passwordInput.value || "";
  if (!pw.trim()) return;

  try {
    const out = await verifyPassword(pw);
    setToken(out.access_token);
    passwordModal.close();
    setLoggedInUI(true);
  } catch (err) {
    passwordError.hidden = false;
    const msg = String(err?.message || err || "");
    passwordError.textContent = msg.includes("Failed to fetch") || msg.includes("NetworkError")
      ? `Can't reach API at ${API_BASE}. Start the backend and retry.`
      : "Invalid password. Try again.";
    passwordInput.select();
  }
});

btnLogout.addEventListener("click", () => {
  clearToken();
  setLoggedInUI(false);
  ensureAuthed();
});

async function handleFirstQuestion(q, { _retriedAfterAuth } = {}) {
  // Once the user starts chatting, we only need the chat composer.
  if (composerCard) composerCard.hidden = true;
  expandChatFromHero();
  addBubble("user", q);
  addBubble("ai", "Thinking…");
  try {
    const out = await sendMessage(q);
    // Replace last AI bubble
    chatScroll.lastChild.textContent = out.answer;
    const suggestions =
      out?.needs_clarification && Array.isArray(out?.suggestions) && out.suggestions.length
        ? `Suggestions: ${out.suggestions.slice(0, 3).join(" · ")}`
        : "";
    const meta = suggestions ? `${suggestions}  |  Latency: ${out.latency_ms}ms` : `Latency: ${out.latency_ms}ms`;
    const m = document.createElement("div");
    m.className = "meta";
    m.textContent = meta;
    chatScroll.lastChild.appendChild(m);

    if (out?.needs_clarification) {
      // Keep the conversation going: focus input so user can answer immediately.
      setTimeout(() => chatInput?.focus(), 50);
    }
  } catch (err) {
    // Expired tokens are expected; prompt for password and retry once.
    if (err?.authExpired && !_retriedAfterAuth) {
      const msg = String(err?.message || err || "");
      // Only auto-reauth on a clear expiry signal to avoid loops.
      if (msg.includes("Token expired") || msg.includes("Missing bearer token") || msg.includes("Invalid token")) {
        clearToken();
        setLoggedInUI(false);
        await ensureAuthed();
        if (getToken()) {
          // Remove the "Thinking…" bubble before retrying.
          if (chatScroll.lastChild) chatScroll.removeChild(chatScroll.lastChild);
          await handleFirstQuestion(q, { _retriedAfterAuth: true });
          return;
        }
      }
    }
    chatScroll.lastChild.textContent = "Something went wrong. Please try again.";
    const m = document.createElement("div");
    m.className = "meta";
    m.textContent = String(err?.message || err);
    chatScroll.lastChild.appendChild(m);
  }
}

composerForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const q = composerInput.value.trim();
  if (!q) return;
  composerInput.value = "";
  await ensureAuthed();
  if (!getToken()) return;
  setLoggedInUI(true);
  await handleFirstQuestion(q);
});

chatForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const q = chatInput.value.trim();
  if (!q) return;
  chatInput.value = "";
  await handleFirstQuestion(q);
});

// boot
(async function init() {
  setLoggedInUI(Boolean(getToken()));
  await ensureAuthed();
})();

