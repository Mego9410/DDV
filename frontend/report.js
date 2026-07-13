(() => {
  // Temporarily off while testing the form. Flip to true to restore magic-link gate.
  const EMAIL_GATE_ENABLED = false;

  const locationInput = document.getElementById("locationSelect");
  const surgeryInput = document.getElementById("surgerySelect");
  const locationTrigger = document.getElementById("locationTrigger");
  const surgeryTrigger = document.getElementById("surgeryTrigger");
  const locationList = document.getElementById("locationList");
  const surgeryList = document.getElementById("surgeryList");
  const locationSearch = document.getElementById("locationSearch");
  const locationEmpty = document.getElementById("locationEmpty");
  const metricGroups = document.getElementById("metricGroups");
  const reportForm = document.getElementById("reportForm");
  const formError = document.getElementById("formError");
  const btnGenerate = document.getElementById("btnGenerate");
  const formUnlockedHint = document.getElementById("formUnlockedHint");
  const reportGate = document.getElementById("reportGate");
  const gateForm = document.getElementById("gateForm");
  const gateName = document.getElementById("gateName");
  const gateEmail = document.getElementById("gateEmail");
  const gateError = document.getElementById("gateError");
  const gateBack = document.getElementById("gateBack");
  const gateSubmit = document.getElementById("gateSubmit");
  const reportSent = document.getElementById("reportSent");
  const reportSentCopy = document.getElementById("reportSentCopy");
  const reportOutput = document.getElementById("reportOutput");
  const reportCohort = document.getElementById("reportCohort");
  const reportRows = document.getElementById("reportRows");
  const formShell = reportForm?.closest(".report-form-shell") || reportForm?.parentElement;

  const UNLOCK_TOKEN_KEY = "ddv_report_unlock_token";

  let locationOptions = [];
  let openDropdown = null;
  let pendingBenchmark = null;
  let unlockToken = "";
  let optionsLoaded = false;
  let hasGenerated = false;

  const GROUP_LABELS = {
    income: "Income & outcomes",
    costs: "Costs",
  };

  const gbp = new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 0,
  });

  const gbpPrecise = new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 2,
  });

  function formatMoney(value, metricId) {
    if (value == null || !Number.isFinite(Number(value))) return "—";
    const n = Number(value);
    if (metricId === "uda_rate") return gbpPrecise.format(n);
    return gbp.format(n);
  }

  // Direction of a % delta, reusing the report's green/red convention.
  function benchDir(pct) {
    if (pct == null || !Number.isFinite(Number(pct))) return "is-flat";
    if (Math.abs(Number(pct)) < 0.05) return "is-flat";
    return Number(pct) > 0 ? "is-above" : "is-below";
  }

  function benchArrow(cls) {
    return cls === "is-above" ? "▲" : cls === "is-below" ? "▼" : "■";
  }

  function benchColour(cls) {
    return cls === "is-above"
      ? "var(--available-fg)"
      : cls === "is-below"
      ? "var(--danger)"
      : "var(--fg-3)";
  }

  function signedPct(pct) {
    if (pct == null || !Number.isFinite(Number(pct))) return "—";
    const n = Number(pct);
    if (Math.abs(n) < 0.05) return "In line";
    return `${n > 0 ? "+" : ""}${n.toFixed(1)}%`;
  }

  // Mixed scale:
  //  - tight cluster (UDA-style): zoom into the value range so markers separate
  //  - wide / outlier: £0-based axis with soft-cap so gaps stay representative
  function buildPositionLine(row) {
    const you = Number(row.your_value);
    const nat =
      row.national && row.national.median != null ? Number(row.national.median) : null;
    const hasLocal = !row.local_suppressed && row.local && row.local.median != null;
    const loc = hasLocal ? Number(row.local.median) : null;
    const same =
      row.national_same_size && row.national_same_size.median != null
        ? Number(row.national_same_size.median)
        : null;

    const pts = [you, nat, loc].filter((v) => v != null && Number.isFinite(v) && v >= 0);
    if (!pts.length) {
      return `<div class="bench"><p class="compare-note">No figures to plot.</p></div>`;
    }

    const rawMin = Math.min(...pts);
    const rawMax = Math.max(...pts);
    const peerRef = Math.max(nat || 0, loc || 0, same || 0);
    const mid = peerRef > 0 ? peerRef : (rawMin + rawMax) / 2 || 1;
    const rawSpan = rawMax - rawMin;
    const relSpread = rawSpan / mid;
    const outlierHigh = peerRef > 0 && you > peerRef * 1.55;
    const outlierLow = peerRef > 0 && you < peerRef * 0.65;

    let min;
    let max;
    let plotYou = you;

    if (!outlierHigh && !outlierLow && relSpread <= 0.28) {
      // Zoom: expand around the cluster so close values remain readable
      const pad = Math.max(rawSpan * 0.55, mid * 0.04, Math.abs(mid) * 0.02);
      min = Math.max(0, rawMin - pad);
      max = rawMax + pad;
      if (max - min < mid * 0.08) {
        min = Math.max(0, mid - mid * 0.04);
        max = mid + mid * 0.04;
      }
    } else if (outlierHigh || outlierLow || relSpread >= 0.55) {
      // Absolute: start at £0; soft-cap extreme highs so the chart doesn't look broken
      min = 0;
      const peak = rawMax;
      max = Math.max(peak * 1.06, peerRef > 0 ? peerRef * 2 : peak * 1.06);
      const softCap = peerRef > 0 ? peerRef * 2.15 : max;
      if (you > softCap) {
        plotYou = softCap;
        max = softCap * 1.04;
      }
    } else {
      // Balanced: peer-centred window — not full £0, not a microscopic zoom
      const half = Math.max(rawSpan * 0.75, mid * 0.35);
      min = Math.max(0, Math.min(rawMin, mid) - half * 0.25);
      max = Math.max(rawMax, mid) + half * 0.35;
    }

    const span = max - min || 1;
    const xRaw = (v) => {
      const t = ((Number(v) - min) / span) * 100;
      return Math.max(2, Math.min(98, t));
    };

    // Keep marker order but enforce a minimum visual gap so labels don't collide
    function spreadMarkers(items, minGap) {
      if (items.length < 2) return items;
      const sorted = [...items].sort((a, b) => a.x - b.x || a.priority - b.priority);
      for (let i = 1; i < sorted.length; i++) {
        const floor = sorted[i - 1].x + minGap;
        if (sorted[i].x < floor) sorted[i].x = floor;
      }
      if (sorted[sorted.length - 1].x > 98) {
        const overflow = sorted[sorted.length - 1].x - 98;
        for (const it of sorted) it.x = Math.max(2, it.x - overflow);
        for (let i = 1; i < sorted.length; i++) {
          const floor = sorted[i - 1].x + minGap;
          if (sorted[i].x < floor) sorted[i].x = Math.min(98, floor);
        }
      }
      return sorted;
    }

    const dir = benchDir(hasLocal ? row.pct_vs_local : row.pct_vs_national);
    const natDir = benchDir(row.pct_vs_national);
    const locDir = benchDir(row.pct_vs_local);

    const markers = [{ id: "you", x: xRaw(plotYou), priority: 0 }];
    if (nat != null) markers.push({ id: "nat", x: xRaw(nat), priority: 1 });
    if (hasLocal) markers.push({ id: "loc", x: xRaw(loc), priority: 2 });
    const placed = spreadMarkers(markers, relSpread <= 0.28 ? 11 : 7);
    const byId = Object.fromEntries(placed.map((m) => [m.id, m.x]));

    const xYou = byId.you;
    const natX = nat != null ? byId.nat : null;
    const locX = hasLocal ? byId.loc : null;

    const barAnchorX = hasLocal ? locX : natX;
    const barL = barAnchorX != null ? Math.min(barAnchorX, xYou) : xYou;
    const barW = barAnchorX != null ? Math.abs(xYou - barAnchorX) : 0;

    // Prefer putting the You label on the side with more free space / away from ticks
    const nearestTick = [natX, locX]
      .filter((v) => v != null)
      .reduce((best, v) => (best == null || Math.abs(v - xYou) < Math.abs(best - xYou) ? v : best), null);
    const labelRight =
      nearestTick != null ? xYou < nearestTick : xYou < 55;
    const youLab = labelRight
      ? `left:${xYou.toFixed(1)}%;transform:translateX(10px)`
      : `right:${(100 - xYou).toFixed(1)}%;text-align:right;transform:translateX(-10px)`;

    let ticks = "";
    if (nat != null && natX != null) {
      ticks += `<span class="bench-tick" style="left:${natX.toFixed(1)}%"></span>`;
      ticks += `<span class="bench-tlabel" style="left:${natX.toFixed(1)}%">National</span>`;
    }
    if (hasLocal && locX != null) {
      ticks += `<span class="bench-tick loc" style="left:${locX.toFixed(1)}%"></span>`;
      ticks += `<span class="bench-tlabel below" style="left:${locX.toFixed(1)}%">Local</span>`;
    }

    let chips = "";
    if (nat != null) {
      chips += `<span class="bench-chip ${natDir}"><span class="ar">${benchArrow(
        natDir
      )}</span>${signedPct(row.pct_vs_national)} vs national</span>`;
    }
    if (hasLocal) {
      chips += `<span class="bench-chip ${locDir}"><span class="ar">${benchArrow(
        locDir
      )}</span>${signedPct(row.pct_vs_local)} vs local</span>`;
    } else {
      chips += `<span class="bench-chip is-muted">Not enough local peers to compare</span>`;
    }

    let legend = `<span class="bench-key"><i style="background:${benchColour(
      dir
    )}"></i> You <b>${formatMoney(you, row.id)}</b></span>`;
    if (hasLocal) {
      legend += `<span class="bench-key"><i class="tick" style="background:var(--fg-1)"></i> Local median <b>${formatMoney(
        loc,
        row.id
      )}</b></span>`;
    }
    if (nat != null) {
      legend += `<span class="bench-key"><i class="tick" style="background:var(--fg-3)"></i> National median <b>${formatMoney(
        nat,
        row.id
      )}</b></span>`;
    }
    if (same != null) {
      legend += `<span>Same-size <b>${formatMoney(same, row.id)}</b></span>`;
    }

    return `
      <div class="bench">
        <div class="bench-axis">
          <span class="bench-linebg"></span>
          <span class="bench-bar ${dir}" style="left:${barL.toFixed(1)}%;width:${barW.toFixed(
      1
    )}%"></span>
          ${ticks}
          <span class="bench-you ${dir}" style="left:${xYou.toFixed(1)}%"></span>
          <span class="bench-youlab" style="${youLab}">You <strong>${formatMoney(
      you,
      row.id
    )}</strong></span>
        </div>
        <div class="bench-foot">${chips}</div>
        <div class="bench-legend">${legend}</div>
      </div>`;
  }

  function closeAllDropdowns() {
    document.querySelectorAll(".dd.is-open").forEach((dd) => {
      dd.classList.remove("is-open");
      const panel = dd.querySelector(".dd-panel");
      const trigger = dd.querySelector(".dd-trigger");
      if (panel) panel.hidden = true;
      if (trigger) trigger.setAttribute("aria-expanded", "false");
    });
    openDropdown = null;
  }

  function openDd(dd) {
    if (!dd) return;
    closeAllDropdowns();
    dd.classList.add("is-open");
    const panel = dd.querySelector(".dd-panel");
    const trigger = dd.querySelector(".dd-trigger");
    if (panel) panel.hidden = false;
    if (trigger) trigger.setAttribute("aria-expanded", "true");
    openDropdown = dd;
    const search = dd.querySelector(".dd-search");
    if (search) {
      search.value = "";
      filterLocations("");
      setTimeout(() => search.focus(), 0);
    }
  }

  function setDdValue(dd, value, label) {
    const hidden = dd.querySelector('input[type="hidden"]');
    const text = dd.querySelector(".dd-trigger-text");
    if (hidden) hidden.value = value;
    if (text) {
      text.textContent = label;
      text.classList.toggle("dd-placeholder", !value);
    }
    dd.querySelectorAll(".dd-option").forEach((opt) => {
      const selected = opt.dataset.value === String(value);
      opt.classList.toggle("is-selected", selected);
      opt.setAttribute("aria-selected", selected ? "true" : "false");
    });
  }

  function renderOptions(listEl, options, { selectedValue = "" } = {}) {
    listEl.innerHTML = "";
    for (const opt of options) {
      const li = document.createElement("li");
      li.className = "dd-option";
      li.setAttribute("role", "option");
      li.dataset.value = String(opt.value);
      li.textContent = opt.label;
      const selected = String(opt.value) === String(selectedValue);
      li.classList.toggle("is-selected", selected);
      li.setAttribute("aria-selected", selected ? "true" : "false");
      listEl.appendChild(li);
    }
  }

  function filterLocations(query) {
    const q = String(query || "").trim().toLowerCase();
    const filtered = !q
      ? locationOptions
      : locationOptions.filter((o) => o.label.toLowerCase().includes(q));
    renderOptions(locationList, filtered, { selectedValue: locationInput.value });
    locationEmpty.hidden = filtered.length > 0;
  }

  function wireDropdown(dd) {
    const trigger = dd.querySelector(".dd-trigger");
    const panel = dd.querySelector(".dd-panel");
    const list = dd.querySelector(".dd-list");
    if (!trigger || !panel || !list) return;

    trigger.addEventListener("click", (e) => {
      e.preventDefault();
      if (dd.classList.contains("is-open")) closeAllDropdowns();
      else openDd(dd);
    });

    list.addEventListener("click", (e) => {
      const opt = e.target.closest(".dd-option");
      if (!opt) return;
      setDdValue(dd, opt.dataset.value, opt.textContent);
      closeAllDropdowns();
      trigger.focus();
    });

    trigger.addEventListener("keydown", (e) => {
      if (e.key === "ArrowDown" || e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        openDd(dd);
      } else if (e.key === "Escape") {
        closeAllDropdowns();
      }
    });
  }

  document.querySelectorAll(".dd").forEach(wireDropdown);

  locationSearch?.addEventListener("input", () => {
    filterLocations(locationSearch.value);
  });

  locationSearch?.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeAllDropdowns();
      locationTrigger.focus();
    } else if (e.key === "Enter") {
      e.preventDefault();
      const first = locationList.querySelector(".dd-option");
      if (first) {
        setDdValue(locationTrigger.closest(".dd"), first.dataset.value, first.textContent);
        closeAllDropdowns();
        locationTrigger.focus();
      }
    }
  });

  document.addEventListener("click", (e) => {
    if (!openDropdown) return;
    if (!e.target.closest(".dd")) closeAllDropdowns();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && openDropdown) closeAllDropdowns();
  });

  function setError(msg) {
    if (!msg) {
      formError.hidden = true;
      formError.textContent = "";
      return;
    }
    formError.hidden = false;
    formError.textContent = msg;
  }

  function renderMetricCatalog(metrics) {
    metricGroups.innerHTML = "";
    const byGroup = { income: [], costs: [] };
    for (const m of metrics) {
      const g = m.group === "costs" ? "costs" : "income";
      byGroup[g].push(m);
    }

    for (const groupKey of ["income", "costs"]) {
      const items = byGroup[groupKey];
      if (!items.length) continue;

      const section = document.createElement("div");
      section.className = "metric-group";

      const title = document.createElement("h3");
      title.className = "metric-group-title";
      title.textContent = GROUP_LABELS[groupKey] || groupKey;
      section.appendChild(title);

      const list = document.createElement("div");
      list.className = "metric-list";

      for (const m of items) {
        const row = document.createElement("div");
        row.className = "metric-row";
        row.dataset.metricId = m.id;

        const checkLabel = document.createElement("label");
        checkLabel.className = "metric-check";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.name = `metric_${m.id}`;
        checkbox.value = m.id;
        checkbox.dataset.role = "metric-toggle";

        const nameSpan = document.createElement("span");
        nameSpan.textContent = m.label;

        checkLabel.appendChild(checkbox);
        checkLabel.appendChild(nameSpan);

        const valueInput = document.createElement("input");
        valueInput.className = "metric-value";
        valueInput.type = "number";
        valueInput.inputMode = "decimal";
        valueInput.min = "0";
        valueInput.step = "any";
        valueInput.placeholder = "Your figure (£)";
        valueInput.disabled = true;
        valueInput.dataset.role = "metric-value";
        valueInput.dataset.metricId = m.id;
        valueInput.setAttribute("aria-label", `${m.label} — your figure in pounds`);

        checkbox.addEventListener("change", () => {
          const on = checkbox.checked;
          valueInput.disabled = !on;
          row.classList.toggle("is-active", on);
          if (!on) valueInput.value = "";
          if (on) valueInput.focus();
        });

        row.appendChild(checkLabel);
        row.appendChild(valueInput);
        list.appendChild(row);
      }

      section.appendChild(list);
      metricGroups.appendChild(section);
    }
  }

  function collectMetrics() {
    const rows = metricGroups.querySelectorAll(".metric-row");
    const metrics = [];
    for (const row of rows) {
      const checkbox = row.querySelector('[data-role="metric-toggle"]');
      const valueInput = row.querySelector('[data-role="metric-value"]');
      if (!checkbox?.checked) continue;
      const id = checkbox.value;
      const value = Number(valueInput?.value);
      if (!Number.isFinite(value)) {
        const label =
          row.querySelector(".metric-check span")?.textContent?.trim() || id.replace(/_/g, " ");
        const err = new Error(`Enter your £ figure for ${label}`);
        err.field = valueInput;
        throw err;
      }
      metrics.push({ id, value });
    }
    return metrics;
  }

  function formatCohortLocation(_cohort) {
    return "Local practices in your area";
  }

  function renderReport(data) {
    const cohort = data?.cohort || {};
    // v2 backend supplies a ready cohort.label; fall back to the
    // location-based label for the pre-v2 response shape.
    reportCohort.textContent = cohort.label || formatCohortLocation(cohort);

    reportRows.innerHTML = "";
    const metrics = Array.isArray(data?.metrics) ? data.metrics : [];

    metrics.forEach((row, index) => {
      const el = document.createElement("article");
      el.className = "report-row";
      el.style.animationDelay = `${Math.min(index * 40, 240)}ms`;

      const head = document.createElement("div");
      head.className = "report-row-head";

      const label = document.createElement("div");
      label.className = "report-row-label";
      label.textContent = row.label || row.id;

      const yours = document.createElement("div");
      yours.className = "report-row-yours";
      yours.innerHTML = `Your figure <strong>${formatMoney(row.your_value, row.id)}</strong>`;

      head.appendChild(label);
      head.appendChild(yours);

      el.appendChild(head);
      el.insertAdjacentHTML("beforeend", buildPositionLine(row));

      reportRows.appendChild(el);
    });

    reportOutput.hidden = false;
    reportOutput.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function setUnlockedUi(on) {
    if (formUnlockedHint) formUnlockedHint.hidden = !on;
    const text = btnGenerate?.querySelector(".report-submit-text");
    if (text) {
      if (!EMAIL_GATE_ENABLED) text.textContent = on ? "Update report" : "Generate report";
      else text.textContent = on ? "Update report" : "Continue";
    }
  }

  function idleSubmitLabel() {
    if (!EMAIL_GATE_ENABLED) return hasGenerated ? "Update report" : "Generate report";
    return unlockToken ? "Update report" : "Continue";
  }

  function setUpdating(on) {
    if (!btnGenerate) return;
    btnGenerate.disabled = on;
    const text = btnGenerate.querySelector(".report-submit-text");
    const spinner = btnGenerate.querySelector(".report-submit-spinner");
    if (text) text.textContent = on ? (hasGenerated || unlockToken ? "Updating…" : "Generating…") : idleSubmitLabel();
    if (spinner) spinner.hidden = !on;
  }

  function persistUnlockToken(token) {
    unlockToken = String(token || "").trim();
    try {
      if (unlockToken) sessionStorage.setItem(UNLOCK_TOKEN_KEY, unlockToken);
      else sessionStorage.removeItem(UNLOCK_TOKEN_KEY);
    } catch {
      /* ignore */
    }
    setUnlockedUi(Boolean(unlockToken));
  }

  function readStoredUnlockToken() {
    try {
      return sessionStorage.getItem(UNLOCK_TOKEN_KEY) || "";
    } catch {
      return "";
    }
  }

  function applyFormValues(inputs = {}) {
    const location = String(inputs.location || "").trim();
    if (location) {
      const locDd = locationTrigger.closest(".dd");
      setDdValue(locDd, location, location);
    }

    const surgeryCount = Number(inputs.surgeryCount);
    if (Number.isInteger(surgeryCount) && surgeryCount >= 1) {
      const surgeryDd = surgeryTrigger.closest(".dd");
      const opt = [...(surgeryList?.querySelectorAll(".dd-option") || [])].find(
        (el) => el.dataset.value === String(surgeryCount)
      );
      const label = opt?.textContent?.trim() || String(surgeryCount);
      setDdValue(surgeryDd, String(surgeryCount), label);
    }

    const metrics = Array.isArray(inputs.metrics) ? inputs.metrics : [];
    const byId = Object.fromEntries(metrics.map((m) => [m.id, m.value]));
    metricGroups.querySelectorAll(".metric-row").forEach((row) => {
      const checkbox = row.querySelector('[data-role="metric-toggle"]');
      const valueInput = row.querySelector('[data-role="metric-value"]');
      if (!checkbox || !valueInput) return;
      const value = byId[checkbox.value];
      if (value == null || !Number.isFinite(Number(value))) {
        checkbox.checked = false;
        valueInput.disabled = true;
        valueInput.value = "";
        row.classList.remove("is-active");
        return;
      }
      checkbox.checked = true;
      valueInput.disabled = false;
      valueInput.value = String(value);
      row.classList.add("is-active");
    });
  }

  function collectFormPayload() {
    const location = locationInput.value.trim();
    const surgeryCount = Number(surgeryInput.value);
    if (!location) {
      const err = new Error("Choose a location.");
      err.field = locationTrigger;
      err.openDd = locationTrigger.closest(".dd");
      throw err;
    }
    if (!Number.isInteger(surgeryCount) || surgeryCount < 1) {
      const err = new Error("Choose number of surgeries.");
      err.field = surgeryTrigger;
      err.openDd = surgeryTrigger.closest(".dd");
      throw err;
    }
    const metrics = collectMetrics();
    if (!metrics.length) {
      throw new Error("Tick at least one element and enter your figure.");
    }
    return { location, surgeryCount, metrics };
  }

  function setGateSending(on) {
    gateSubmit.disabled = on;
    gateBack.disabled = on;
    const text = gateSubmit.querySelector(".gate-submit-text");
    const spinner = gateSubmit.querySelector(".report-submit-spinner");
    if (text) text.textContent = on ? "Sending…" : "Email me the link";
    if (spinner) spinner.hidden = !on;
  }

  function setGateError(msg) {
    if (!msg) {
      gateError.hidden = true;
      gateError.textContent = "";
      return;
    }
    gateError.hidden = false;
    gateError.textContent = msg;
  }

  function showView({ form = false, gate = false, sent = false, output = false } = {}) {
    if (formShell) formShell.hidden = !form;
    reportGate.hidden = !gate;
    reportSent.hidden = !sent;
    reportOutput.hidden = !output;
  }

  function getUnlockTokenFromUrl() {
    try {
      return new URLSearchParams(window.location.search).get("token") || "";
    } catch {
      return "";
    }
  }

  function clearTokenFromUrl() {
    try {
      const url = new URL(window.location.href);
      if (!url.searchParams.has("token")) return;
      url.searchParams.delete("token");
      window.history.replaceState({}, "", url.pathname + url.search + url.hash);
    } catch {
      /* ignore */
    }
  }

  async function runDirectBenchmark(payload) {
    const resp = await fetch("/api/report/benchmark", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      throw new Error(data.detail || "Could not generate report");
    }
    hasGenerated = true;
    setUnlockedUi(true);
    showView({ form: true, output: true });
    renderReport(data);
  }

  async function unlockWithToken(token) {
    showView({});
    setError("");
    const resp = await fetch("/api/report/unlock", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token }),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      throw new Error(data.detail || "Could not unlock report");
    }
    persistUnlockToken(token);
    clearTokenFromUrl();
    await ensureOptionsLoaded();
    applyFormValues(data.inputs || {});
    showView({ form: true, output: true });
    renderReport(data.report || {});
  }

  async function recalculateReport(payload) {
    const resp = await fetch("/api/report/recalculate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token: unlockToken, ...payload }),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      throw new Error(data.detail || "Could not update report");
    }
    applyFormValues(data.inputs || payload);
    showView({ form: true, output: true });
    renderReport(data.report || {});
  }

  async function ensureOptionsLoaded() {
    if (optionsLoaded) return;
    await loadOptions();
  }

  async function loadOptions() {
    const resp = await fetch("/api/report/options");
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      throw new Error(data.detail || "Could not load report options");
    }

    locationOptions = (data.locations || []).map((loc) => ({ value: loc, label: loc }));
    renderOptions(locationList, locationOptions);
    locationTrigger.querySelector(".dd-trigger-text").textContent = "Select location…";
    locationTrigger.querySelector(".dd-trigger-text").classList.add("dd-placeholder");
    locationInput.value = "";
    locationEmpty.hidden = locationOptions.length > 0;

    const surgeryOptions = (data.surgeryCounts || []).map((s) => ({
      value: String(s.value),
      label: s.label,
    }));
    renderOptions(surgeryList, surgeryOptions);
    surgeryTrigger.querySelector(".dd-trigger-text").textContent = "Select…";
    surgeryTrigger.querySelector(".dd-trigger-text").classList.add("dd-placeholder");
    surgeryInput.value = "";

    renderMetricCatalog(data.metrics || []);
    optionsLoaded = true;
  }

  reportForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    setError("");

    let payload;
    try {
      payload = collectFormPayload();
    } catch (err) {
      setError(err.message || "Enter your figures for each selected element.");
      err.field?.focus?.();
      if (err.openDd) openDd(err.openDd);
      return;
    }

    // Testing mode: skip email gate and generate immediately.
    if (!EMAIL_GATE_ENABLED) {
      setUpdating(true);
      try {
        await runDirectBenchmark(payload);
      } catch (err) {
        setError(String(err.message || err));
      } finally {
        setUpdating(false);
      }
      return;
    }

    if (unlockToken) {
      setUpdating(true);
      try {
        await recalculateReport(payload);
      } catch (err) {
        setError(String(err.message || err));
      } finally {
        setUpdating(false);
      }
      return;
    }

    pendingBenchmark = payload;
    setGateError("");
    showView({ gate: true });
    reportGate.scrollIntoView({ behavior: "smooth", block: "start" });
    gateName.focus();
  });

  gateBack.addEventListener("click", () => {
    setGateError("");
    showView({ form: true, output: Boolean(unlockToken) && !reportOutput.hidden });
    reportForm.scrollIntoView({ behavior: "smooth", block: "start" });
  });

  gateForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    setGateError("");

    if (!pendingBenchmark) {
      setGateError("Please complete the report form first.");
      showView({ form: true });
      return;
    }

    const name = gateName.value.trim();
    const email = gateEmail.value.trim();
    if (!name) {
      setGateError("Please enter your name.");
      gateName.focus();
      return;
    }
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      setGateError("Please enter a valid email address.");
      gateEmail.focus();
      return;
    }

    setGateSending(true);
    try {
      const resp = await fetch("/api/report/request-link", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          email,
          ...pendingBenchmark,
        }),
      });
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        throw new Error(data.detail || "Could not send unlock email");
      }
      const masked = data.emailMasked || email;
      reportSentCopy.textContent = `We’ve sent a private unlock link to ${masked}. Open it to view your benchmark report.`;
      showView({ sent: true });
      reportSent.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch (err) {
      setGateError(String(err.message || err));
    } finally {
      setGateSending(false);
    }
  });

  async function boot() {
    if (!EMAIL_GATE_ENABLED) {
      // Clear any leftover unlock session from earlier testing.
      persistUnlockToken("");
      showView({ form: true });
      setUnlockedUi(false);
      try {
        await loadOptions();
      } catch (err) {
        setError(String(err.message || err));
        const text = locationTrigger.querySelector(".dd-trigger-text");
        if (text) {
          text.textContent = "Unable to load locations";
          text.classList.add("dd-placeholder");
        }
        locationTrigger.disabled = true;
        surgeryTrigger.disabled = true;
      }
      return;
    }

    const token = getUnlockTokenFromUrl() || readStoredUnlockToken();
    if (token) {
      try {
        await unlockWithToken(token);
        return;
      } catch (err) {
        persistUnlockToken("");
        showView({ form: true });
        setError(String(err.message || err));
      }
    }

    showView({ form: true });
    setUnlockedUi(false);
    try {
      await loadOptions();
    } catch (err) {
      setError(String(err.message || err));
      const text = locationTrigger.querySelector(".dd-trigger-text");
      if (text) {
        text.textContent = "Unable to load locations";
        text.classList.add("dd-placeholder");
      }
      locationTrigger.disabled = true;
      surgeryTrigger.disabled = true;
    }
  }

  boot();
})();
