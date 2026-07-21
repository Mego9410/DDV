(function () {
  "use strict";

  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const gbp = (n) => "£" + Math.round(n).toLocaleString("en-GB");

  /* ================= Live places / applications ================= */
  fetch("/api/waitlist/status")
    .then((r) => (r.ok ? r.json() : null))
    .then((data) => {
      if (!data || !Number.isFinite(data.applications)) return;
      const places = data.places || 25;
      const text = `${data.applications} application${data.applications === 1 ? "" : "s"} received — ${places} founding places`;
      const hero = document.getElementById("livePlaces");
      const apply = document.getElementById("applyPlaces");
      if (hero && data.applications > 0) hero.textContent = text;
      if (apply && data.applications > 0) apply.textContent = text;
    })
    .catch(() => {});

  /* ================= Count-up animations ================= */
  function countUp(el, target, { prefix = "", suffix = "", duration = 1100 } = {}) {
    if (reduceMotion) {
      el.textContent = prefix + target.toLocaleString("en-GB") + suffix;
      return;
    }
    const start = performance.now();
    function tick(now) {
      const t = Math.min((now - start) / duration, 1);
      const eased = 1 - Math.pow(1 - t, 3);
      el.textContent = prefix + Math.round(target * eased).toLocaleString("en-GB") + suffix;
      if (t < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
  }

  const counters = document.querySelectorAll(".hpp-count");
  const counterObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        countUp(entry.target, Number(entry.target.dataset.target));
        counterObserver.unobserve(entry.target);
      });
    },
    { threshold: 0.5 }
  );
  counters.forEach((el) => counterObserver.observe(el));

  /* ================= Hero audit panel ================= */
  const panel = document.getElementById("auditPanel");
  if (panel) {
    panel.querySelectorAll(".hpp-panel-row").forEach((row) => {
      row.style.setProperty("--pct", row.dataset.pct);
    });
    const total = document.getElementById("panelTotal");
    const panelObserver = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          panel.classList.add("is-live");
          if (total) countUp(total, Number(total.dataset.target), { prefix: "£", duration: 1400 });
          panelObserver.unobserve(panel);
        });
      },
      { threshold: 0.4 }
    );
    panelObserver.observe(panel);
  }

  /* ================= Savings calculator ================= */
  const range = document.getElementById("calcRange");
  if (range) {
    const valueOut = document.getElementById("calcValue");
    const savingsOut = document.getElementById("calcSavings");
    const termOut = document.getElementById("calcTerm");
    const exitOut = document.getElementById("calcExit");
    const SAVINGS_RATE = 0.2;
    const EXIT_MULTIPLE = 7;

    function renderCalc() {
      const overheads = Number(range.value);
      const savings = overheads * SAVINGS_RATE;
      const fill = ((overheads - Number(range.min)) / (Number(range.max) - Number(range.min))) * 100;
      range.style.setProperty("--fill", fill + "%");
      valueOut.textContent = gbp(overheads);
      savingsOut.textContent = gbp(savings);
      termOut.textContent = gbp(savings * 2);
      exitOut.textContent = gbp(savings * EXIT_MULTIPLE);
    }

    range.addEventListener("input", renderCalc);
    renderCalc();
  }

  /* ================= Application form ================= */
  const form = document.getElementById("waitlistForm");
  if (!form) return;

  const errorEl = document.getElementById("formError");
  const successEl = document.getElementById("formSuccess");
  const positionEl = document.getElementById("successPosition");
  const submitBtn = document.getElementById("submitBtn");
  const motivationCount = document.getElementById("motivationCount");
  const steps = Array.from(form.querySelectorAll(".hpp-step-panel"));
  const dots = Array.from(document.querySelectorAll("[data-step-dot]"));

  function showError(message) {
    errorEl.textContent = message;
    errorEl.hidden = !message;
  }

  function setStep(n) {
    steps.forEach((s) => s.classList.toggle("is-active", s.dataset.step === String(n)));
    dots.forEach((d) => {
      const dn = Number(d.dataset.stepDot);
      d.classList.toggle("is-active", dn === n);
      d.classList.toggle("is-done", dn < n);
    });
    showError("");
    const card = document.querySelector(".hpp-form-card");
    if (card) card.scrollIntoView({ behavior: reduceMotion ? "auto" : "smooth", block: "start" });
  }

  function validateStep(stepEl) {
    let firstInvalid = null;
    const fields = stepEl.querySelectorAll("input, select, textarea");
    fields.forEach((field) => {
      const valid = field.checkValidity();
      field.classList.toggle("is-invalid", !valid);
      if (!valid && !firstInvalid) firstInvalid = field;
    });

    const checkGroup = stepEl.querySelector('[data-group="categories"]');
    if (checkGroup) {
      const anyChecked = checkGroup.querySelectorAll("input:checked").length > 0;
      checkGroup.classList.toggle("is-invalid", !anyChecked);
      if (!anyChecked && !firstInvalid) {
        showError("Please select at least one cost category — 'Not sure' counts.");
        return false;
      }
    }

    if (firstInvalid) {
      firstInvalid.reportValidity();
      firstInvalid.focus();
      return false;
    }
    return true;
  }

  form.querySelectorAll(".hpp-next").forEach((btn) => {
    btn.addEventListener("click", () => {
      const current = btn.closest(".hpp-step-panel");
      if (validateStep(current)) setStep(Number(btn.dataset.next));
    });
  });

  form.querySelectorAll(".hpp-back").forEach((btn) => {
    btn.addEventListener("click", () => setStep(Number(btn.dataset.back)));
  });

  const motivation = form.elements.motivation;
  motivation.addEventListener("input", () => {
    motivationCount.textContent = String(motivation.value.trim().length);
  });

  form.querySelectorAll(".hpp-input").forEach((field) => {
    field.addEventListener("input", () => field.classList.remove("is-invalid"));
    field.addEventListener("change", () => field.classList.remove("is-invalid"));
  });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const lastStep = steps[steps.length - 1];
    if (!validateStep(lastStep)) return;

    const motivationText = motivation.value.trim();
    if (motivationText.length < 80) {
      motivation.classList.add("is-invalid");
      motivation.focus();
      showError("Tell us a little more — at least 80 characters on why you want a founding place.");
      return;
    }

    const payload = {
      website: form.elements.website.value,
      name: form.elements.name.value.trim(),
      role: form.elements.role.value,
      email: form.elements.email.value.trim(),
      phone: form.elements.phone.value.trim(),
      practiceName: form.elements.practiceName.value.trim(),
      location: form.elements.location.value.trim(),
      surgeryCount: Number(form.elements.surgeryCount.value),
      practiceType: form.elements.practiceType.value,
      turnoverBand: form.elements.turnoverBand.value,
      yearsOwned: form.elements.yearsOwned.value,
      categories: Array.from(form.querySelectorAll('input[name="categories"]:checked')).map((c) => c.value),
      overheadBand: form.elements.overheadBand.value,
      invoiceAccess: form.elements.invoiceAccess.value,
      decisionMaker: form.elements.decisionMaker.value,
      timeline: form.elements.timeline.value,
      motivation: motivationText,
      consent: form.elements.consent.checked,
    };

    submitBtn.disabled = true;
    submitBtn.textContent = "Submitting…";
    showError("");

    try {
      const resp = await fetch("/api/waitlist/join", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        throw new Error(data?.detail || "Something went wrong. Please try again.");
      }

      form.hidden = true;
      document.querySelector(".hpp-progress").hidden = true;
      positionEl.innerHTML =
        data.position && Number(data.position) > 0
          ? `You're application <strong>#${Number(data.position)}</strong> for the 25 founding places.`
          : "Your application is in the queue for the 25 founding places.";
      successEl.hidden = false;
      successEl.scrollIntoView({ behavior: reduceMotion ? "auto" : "smooth", block: "center" });
    } catch (err) {
      showError(err.message || "Something went wrong. Please try again.");
      submitBtn.disabled = false;
      submitBtn.textContent = "Submit application";
    }
  });
})();
