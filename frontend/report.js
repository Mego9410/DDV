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

  function formatPctDelta(pct) {
    if (pct == null || !Number.isFinite(Number(pct))) return { text: "—", cls: "is-flat" };
    const n = Number(pct);
    if (Math.abs(n) < 0.05) return { text: "In line with median", cls: "is-flat" };
    const abs = Math.abs(n).toFixed(1);
    if (n > 0) return { text: `${abs}% higher than median`, cls: "is-above" };
    return { text: `${abs}% lower than median`, cls: "is-below" };
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

      const compare = document.createElement("div");
      compare.className = "report-compare";

      const natDelta = formatPctDelta(row.pct_vs_national);
      const natBlock = document.createElement("div");
      natBlock.className = `compare-block ${natDelta.cls}`;
      natBlock.innerHTML = `
        <div class="compare-label">National median</div>
        <div class="compare-median">${formatMoney(row.national?.median, row.id)}</div>
        <div class="compare-delta ${natDelta.cls}">${natDelta.text}</div>
      `;

      const locBlock = document.createElement("div");
      if (row.local_suppressed || !row.local) {
        locBlock.className = "compare-block";
        locBlock.innerHTML = `
          <div class="compare-label">Local peers</div>
          <div class="compare-note">Not enough local peers with this figure to compare.</div>
        `;
      } else {
        const locDelta = formatPctDelta(row.pct_vs_local);
        locBlock.className = `compare-block ${locDelta.cls}`;
        locBlock.innerHTML = `
          <div class="compare-label">Local peer median</div>
          <div class="compare-median">${formatMoney(row.local?.median, row.id)}</div>
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
        same.textContent = `National same-size median: ${formatMoney(
          row.national_same_size.median,
          row.id
        )}`;
        el.appendChild(same);
      }

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
