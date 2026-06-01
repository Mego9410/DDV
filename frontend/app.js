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
const heroExamples = el("heroExamples");

const passwordModal = el("passwordModal");
const passwordForm = el("passwordForm");
const passwordInput = el("passwordInput");
const passwordError = el("passwordError");

const CHAT_THREAD_KEY = "DDV_CHAT_THREAD_V1";

// Conversation context is kept for the current browser session only.
// sessionStorage is cleared when the tab/window is closed, so follow-up
// context survives navigation/reloads within a session but does not persist
// after the window is closed (by design).
function loadThread() {
  try {
    const raw = sessionStorage.getItem(CHAT_THREAD_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function saveThread(thread) {
  try {
    sessionStorage.setItem(CHAT_THREAD_KEY, JSON.stringify(thread || []));
  } catch (_) {}
}

function clearThread() {
  try {
    sessionStorage.removeItem(CHAT_THREAD_KEY);
  } catch (_) {}
}

// In-memory thread for this session (persisted to sessionStorage)
let thread = loadThread();

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

// Animated "typing" bubble shown while the assistant is composing a reply.
// Returns the bubble element so the caller can swap in the final answer.
function addThinkingBubble() {
  const wrap = document.createElement("div");
  wrap.className = "bubble ai";
  const dots = document.createElement("div");
  dots.className = "typing";
  dots.setAttribute("aria-label", "Assistant is thinking");
  dots.innerHTML = "<span></span><span></span><span></span>";
  wrap.appendChild(dots);
  chatScroll.appendChild(wrap);
  chatScroll.scrollTop = chatScroll.scrollHeight;
  return wrap;
}

// Re-render the saved conversation (used when restoring a session).
function renderThread() {
  if (!chatScroll) return;
  chatScroll.innerHTML = "";
  for (const m of Array.isArray(thread) ? thread : []) {
    if (!m || typeof m.content !== "string") continue;
    addBubble(m.role === "user" ? "user" : "ai", m.content);
  }
}

// Show the chat expanded immediately (no entrance animation) for restored sessions.
function showChatExpanded() {
  if (composerCard) composerCard.hidden = true;
  hero.classList.add("is-collapsing");
  hero.hidden = true;
  chat.hidden = false;
  chat.classList.remove("is-collapsed", "is-expanding");
  chat.classList.add("is-expanded");
}

function formatVerifyError(message, status) {
  if (message.includes("Failed to fetch") || message.includes("NetworkError")) {
    return `Can't reach API at ${API_BASE || "(same origin)"}. Check the deployment and retry.`;
  }
  try {
    const parsed = JSON.parse(message);
    if (parsed?.detail) return String(parsed.detail);
  } catch {
    // not JSON
  }
  if (status && status >= 500) {
    const snippet = message.replace(/\s+/g, " ").trim().slice(0, 160);
    return snippet
      ? `Server error (${status}): ${snippet}`
      : `Server error (${status}). Check Vercel env vars and redeploy.`;
  }
  if (status === 401) return "Invalid password. Try again.";
  return message || "Sign-in failed. Try again.";
}

async function verifyPassword(password) {
  const resp = await fetch(`${API_BASE}/api/access/verify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password }),
  });
  if (!resp.ok) {
    const t = await resp.text();
    const err = new Error(t || `Password verify failed (${resp.status})`);
    err.statusCode = resp.status;
    throw err;
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
    body: JSON.stringify({ message, messages: thread }),
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
    passwordError.textContent = formatVerifyError(msg, err?.statusCode);
    passwordInput.select();
  }
});

btnLogout.addEventListener("click", () => {
  clearToken();
  clearThread();
  thread = [];
  setLoggedInUI(false);
  ensureAuthed();
});

async function handleFirstQuestion(q, { _retriedAfterAuth } = {}) {
  // Once the user starts chatting, we only need the chat composer.
  if (composerCard) composerCard.hidden = true;
  expandChatFromHero();
  addBubble("user", q);
  addThinkingBubble();

  // Update local thread before the request so the server sees the latest user turn.
  thread = Array.isArray(thread) ? thread : [];
  thread.push({ role: "user", content: q });
  // Keep the thread from growing unbounded in localStorage.
  if (thread.length > 40) thread = thread.slice(thread.length - 40);
  saveThread(thread);

  try {
    const out = await sendMessage(q);
    // Replace last AI bubble
    chatScroll.lastChild.textContent = out.answer;
    const followUps =
      Array.isArray(out?.follow_ups) && out.follow_ups.length ? `Next: ${out.follow_ups.slice(0, 3).join(" · ")}` : "";
    const meta = followUps ? `${followUps}  |  Latency: ${out.latency_ms}ms` : `Latency: ${out.latency_ms}ms`;
    const m = document.createElement("div");
    m.className = "meta";
    m.textContent = meta;
    chatScroll.lastChild.appendChild(m);
    chatScroll.scrollTop = chatScroll.scrollHeight;

    // Keep the conversation going: focus input so user can ask a follow-on immediately.
    setTimeout(() => chatInput?.focus(), 50);

    // Append assistant turn to thread after successful response
    thread = Array.isArray(thread) ? thread : [];
    thread.push({ role: "assistant", content: out.answer });
    if (thread.length > 40) thread = thread.slice(thread.length - 40);
    saveThread(thread);
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

    // Roll back the last user turn if the request failed.
    thread = Array.isArray(thread) ? thread : [];
    if (thread.length && thread[thread.length - 1]?.role === "user" && thread[thread.length - 1]?.content === q) {
      thread.pop();
      saveThread(thread);
    }
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

// Example prompt chips: clicking one runs it like a typed question.
if (heroExamples) {
  heroExamples.addEventListener("click", async (e) => {
    const chip = e.target.closest(".chip");
    if (!chip) return;
    const q = (chip.textContent || "").trim();
    if (!q) return;
    await ensureAuthed();
    if (!getToken()) return;
    setLoggedInUI(true);
    await handleFirstQuestion(q);
  });
}

// boot
(async function init() {
  if (getToken()) {
    setLoggedInUI(true);
    // Restore an in-progress conversation for this session (survives reload,
    // cleared when the window/tab is closed).
    if (Array.isArray(thread) && thread.length) {
      showChatExpanded();
      renderThread();
      setTimeout(() => chatInput?.focus(), 50);
    }
  } else {
    setLoggedInUI(false);
    await ensureAuthed();
  }
})();

