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

  function formatPctDelta(pct) {
    if (pct == null || !Number.isFinite(Number(pct))) return { text: "—", cls: "is-flat" };
    const n = Number(pct);
    if (Math.abs(n) < 0.05) return { text: "In line with median", cls: "is-flat" };
    const abs = Math.abs(n).toFixed(1);
    if (n > 0) return { text: `${abs}% higher than median`, cls: "is-above" };
    return { text: `${abs}% lower than median`, cls: "is-below" };
  }

  // Deviation meter: median is the centre baseline, your value sits left
  // (below) or right (above). Magnitude is clamped so extreme outliers stay
  // on-scale; the exact % is always shown in text beneath.
  const METER_CAP = 60; // percent mapped to the edge of the track
  function buildMeter(pct, cls) {
    let offset = 0; // -50..50, share of the half-track
    if (pct != null && Number.isFinite(Number(pct))) {
      const clamped = Math.max(-METER_CAP, Math.min(METER_CAP, Number(pct)));
      offset = (clamped / METER_CAP) * 50;
    }
    const you = Math.max(2, Math.min(98, 50 + offset));
    let fill;
    if (offset > 0.4) {
      fill = `left:50%;width:${(you - 50).toFixed(2)}%`;
    } else if (offset < -0.4) {
      fill = `left:${you.toFixed(2)}%;width:${(50 - you).toFixed(2)}%`;
    } else {
      fill = `left:50%;width:0%`;
    }
    return `
      <div class="bench-meter" role="img" aria-hidden="true">
        <div class="bench-track">
          <span class="bench-fill ${cls}" style="${fill}"></span>
          <span class="bench-you ${cls}" style="left:${you.toFixed(2)}%"></span>
        </div>
        <div class="bench-scale"><span>Below</span><span>Median</span><span>Above</span></div>
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

  function formatCohortLocation(cohort) {
    const place = String(cohort?.location || "").trim();
    if (!place) return "Local peer group for your selected location";
    if (cohort?.mode === "radius" && Number.isFinite(Number(cohort.radius_miles))) {
      return `Local peer group: within ${Number(cohort.radius_miles)} miles of ${place}`;
    }
    return `Local peer group: ${place}`;
  }

  function renderReport(data) {
    const cohort = data?.cohort || {};
    reportCohort.textContent = formatCohortLocation(cohort);

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

      const compare = document.createElement("div");
      compare.className = "report-compare";

      const natDelta = formatPctDelta(row.pct_vs_national);
      const natBlock = document.createElement("div");
      natBlock.className = `compare-block ${natDelta.cls}`;
      natBlock.innerHTML = `
        <div class="compare-head">
          <span class="compare-label">National median</span>
          <span class="compare-median">${formatMoney(row.national?.median, row.id)}</span>
        </div>
        ${buildMeter(row.pct_vs_national, natDelta.cls)}
        <div class="compare-delta ${natDelta.cls}">${natDelta.text}</div>
      `;

      const locBlock = document.createElement("div");
      if (row.local_suppressed || !row.local) {
        locBlock.className = "compare-block";
        locBlock.innerHTML = `
          <div class="compare-head">
            <span class="compare-label">Local peers</span>
          </div>
          <p class="compare-note">Not enough local peers with this figure to compare.</p>
        `;
      } else {
        const locDelta = formatPctDelta(row.pct_vs_local);
        locBlock.className = `compare-block ${locDelta.cls}`;
        locBlock.innerHTML = `
          <div class="compare-head">
            <span class="compare-label">Local peer median</span>
            <span class="compare-median">${formatMoney(row.local?.median, row.id)}</span>
          </div>
          ${buildMeter(row.pct_vs_local, locDelta.cls)}
          <div class="compare-delta ${locDelta.cls}">${locDelta.text}</div>
        `;
      }

      compare.appendChild(natBlock);
      compare.appendChild(locBlock);

      el.appendChild(head);
      el.appendChild(compare);

      if (row.national_same_size?.median != null) {
        const same = document.createElement("div");
        same.className = "report-same-size";
        same.innerHTML = `National same-size median: <strong>${formatMoney(
          row.national_same_size.median,
          row.id
        )}</strong>`;
        el.appendChild(same);
      }

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
