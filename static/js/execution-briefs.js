function getExecutionContractIdForRole(roleOrContractId) {
  const value = String(roleOrContractId || "").trim();
  if (!value) {
    return "workbench-builder";
  }
  if (value.startsWith("workbench-")) {
    return value;
  }
  return `workbench-${value}`;
}

async function loadExecutionBrief(contractId, options = {}) {
  const normalizedContractId = getExecutionContractIdForRole(contractId);
  const taskId = String(options.taskId || "");
  const runId = String(options.runId || "");
  if (state.executionDirty && (taskId || runId)) {
    const saved = await saveExecutionState({ silent: true });
    if (!saved) {
      return;
    }
  }
  const params = new URLSearchParams();
  if (taskId) {
    params.set("task_id", taskId);
  }
  if (runId) {
    params.set("run_id", runId);
  }
  setStatus("Loading agent brief...", `Building a focused brief for ${normalizedContractId}.`);
  const response = await fetch(`/api/agent-contracts/${encodeURIComponent(normalizedContractId)}/brief?${params.toString()}`);
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Agent brief failed", payload.detail || payload.error || "Unable to build the requested agent brief.");
    return;
  }
  state.executionBrief = {
    ...payload.brief,
    contract_id: normalizedContractId,
    loaded_at: new Date().toISOString(),
  };
  if (payload.source_of_truth) {
    state.sourceOfTruth = payload.source_of_truth;
  }
  render();
  const scope = runId || taskId || normalizedContractId;
  setStatus("Agent brief ready", `The execution brief for ${scope} is ready to copy or hand off.`);
}

function clearExecutionBrief() {
  state.executionBrief = null;
  render();
  setStatus("Agent brief cleared", "The execution panel is back to the general control-plane view.");
}

async function copyExecutionBriefPrompt() {
  const prompt = String(state.executionBrief?.prompt_markdown || state.executionBrief?.prompt_text || "");
  if (!prompt) {
    setStatus("Copy skipped", "Load an execution brief first.");
    return;
  }
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(prompt);
    } else {
      const textarea = document.createElement("textarea");
      textarea.value = prompt;
      textarea.setAttribute("readonly", "true");
      textarea.style.position = "absolute";
      textarea.style.left = "-9999px";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      textarea.remove();
    }
    setStatus("Agent brief copied", "The current execution brief is ready to paste into an agent session.");
  } catch (error) {
    setStatus("Copy failed", error instanceof Error ? error.message : "Unable to copy the brief to the clipboard.");
  }
}

function renderExecutionBriefSection() {
  const brief = state.executionBrief;
  if (!brief) {
    return "";
  }
  const task = brief.task || null;
  const run = brief.run || null;
  const contract = brief.contract || {};
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Agent assignment brief</h3>
        <div class="chip-row">
          <button type="button" data-execution-action="copy-brief">Copy brief</button>
          <button type="button" data-execution-action="clear-brief">Clear</button>
        </div>
      </div>
      <div class="column-row execution-editor-card execution-brief-card">
        <div class="chip-row">
          <span class="tag-chip">${escapeHtml(contract.id || brief.contract_id || "agent")}</span>
          ${task ? `<span class="status-pill">${escapeHtml(task.id || "")}</span>` : ""}
          ${run ? `<span class="status-pill">${escapeHtml(run.id || "")}</span>` : ""}
        </div>
        ${contract.summary ? `<div class="column-meta">${escapeHtml(contract.summary)}</div>` : ""}
        ${brief.linked_refs?.length ? `<div class="column-meta"><strong>Refs:</strong> ${escapeHtml(brief.linked_refs.join(", "))}</div>` : ""}
        <pre class="execution-brief-prompt">${escapeHtml(brief.prompt_markdown || brief.prompt_text || "")}</pre>
      </div>
    </div>
  `;
}
