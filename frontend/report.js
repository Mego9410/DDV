(() => {
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
  const reportOutput = document.getElementById("reportOutput");
  const reportCohort = document.getElementById("reportCohort");
  const reportRows = document.getElementById("reportRows");

  let locationOptions = [];
  let openDropdown = null;

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

  // Position line: your value and the peer medians plotted on one shared
  // pounds axis. Distance is literal and uncapped, so how far above (or below)
  // a median you sit is drawn to scale. Colour keeps the green-above /
  // red-below convention, driven by the local comparison (national if local
  // is suppressed).
  function buildPositionLine(row) {
    const you = Number(row.your_value);
    const nat =
      row.national && row.national.median != null ? Number(row.national.median) : null;
    const hasLocal = !row.local_suppressed && row.local && row.local.median != null;
    const loc = hasLocal ? Number(row.local.median) : null;

    const pts = [you, nat, loc].filter((v) => v != null && Number.isFinite(v));
    let min = Math.min(...pts);
    let max = Math.max(...pts);
    let pad = (max - min) * 0.12;
    if (!(pad > 0)) pad = (Math.abs(max) || 1) * 0.1;
    min -= pad;
    max += pad;
    const span = max - min || 1;
    const x = (v) => ((v - min) / span) * 100;

    const dir = benchDir(hasLocal ? row.pct_vs_local : row.pct_vs_national);
    const natDir = benchDir(row.pct_vs_national);
    const locDir = benchDir(row.pct_vs_local);

    const xYou = x(you);
    const barAnchor = hasLocal ? loc : nat;
    const barL = barAnchor != null ? Math.min(x(barAnchor), xYou) : xYou;
    const barW = barAnchor != null ? Math.abs(xYou - x(barAnchor)) : 0;
    const youLab =
      xYou >= 55
        ? `right:${(100 - xYou).toFixed(1)}%;text-align:right`
        : `left:${xYou.toFixed(1)}%;transform:translateX(-50%)`;

    let ticks = "";
    if (nat != null) {
      ticks += `<span class="bench-tick" style="left:${x(nat)}%"></span>`;
      ticks += `<span class="bench-tlabel" style="left:${x(nat)}%">National</span>`;
    }
    if (hasLocal) {
      ticks += `<span class="bench-tick loc" style="left:${x(loc)}%"></span>`;
      ticks += `<span class="bench-tlabel below" style="left:${x(loc)}%">Local</span>`;
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
    if (row.national_same_size && row.national_same_size.median != null) {
      legend += `<span>Same-size <b>${formatMoney(
        row.national_same_size.median,
        row.id
      )}</b></span>`;
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

  function renderReport(data) {
    const cohort = data?.cohort || {};
    reportCohort.textContent = cohort.label
      ? `Local peer group: ${cohort.label}`
      : "Local peer group selected from your location and surgery count.";

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

  function setGenerating(on) {
    btnGenerate.disabled = on;
    const text = btnGenerate.querySelector(".report-submit-text");
    const spinner = btnGenerate.querySelector(".report-submit-spinner");
    if (text) text.textContent = on ? "Generating…" : "Generate report";
    if (spinner) spinner.hidden = !on;
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
  }

  reportForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    setError("");

    const location = locationInput.value.trim();
    const surgeryCount = Number(surgeryInput.value);
    if (!location) {
      setError("Choose a location.");
      locationTrigger.focus();
      openDd(locationTrigger.closest(".dd"));
      return;
    }
    if (!Number.isInteger(surgeryCount) || surgeryCount < 1) {
      setError("Choose number of surgeries.");
      surgeryTrigger.focus();
      openDd(surgeryTrigger.closest(".dd"));
      return;
    }

    let metrics;
    try {
      metrics = collectMetrics();
    } catch (err) {
      setError(err.message || "Enter your figures for each selected element.");
      err.field?.focus();
      return;
    }

    if (!metrics.length) {
      setError("Tick at least one element and enter your figure.");
      return;
    }

    setGenerating(true);

    try {
      const resp = await fetch("/api/report/benchmark", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ location, surgeryCount, metrics }),
      });
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        throw new Error(data.detail || "Could not generate report");
      }
      renderReport(data);
    } catch (err) {
      setError(String(err.message || err));
      reportOutput.hidden = true;
    } finally {
      setGenerating(false);
    }
  });

  loadOptions().catch((err) => {
    setError(String(err.message || err));
    const text = locationTrigger.querySelector(".dd-trigger-text");
    if (text) {
      text.textContent = "Unable to load locations";
      text.classList.add("dd-placeholder");
    }
    locationTrigger.disabled = true;
    surgeryTrigger.disabled = true;
  });
})();
