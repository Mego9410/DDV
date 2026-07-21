(function () {
  "use strict";

  const form = document.getElementById("waitlistForm");
  const errorEl = document.getElementById("formError");
  const successEl = document.getElementById("formSuccess");
  const positionEl = document.getElementById("successPosition");
  const submitBtn = document.getElementById("submitBtn");
  const motivationCount = document.getElementById("motivationCount");
  const steps = Array.from(form.querySelectorAll(".hpp-step"));
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
    if (card) card.scrollIntoView({ behavior: "smooth", block: "start" });
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
      const current = btn.closest(".hpp-step");
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
      successEl.scrollIntoView({ behavior: "smooth", block: "center" });
    } catch (err) {
      showError(err.message || "Something went wrong. Please try again.");
      submitBtn.disabled = false;
      submitBtn.textContent = "Submit application";
    }
  });
})();
