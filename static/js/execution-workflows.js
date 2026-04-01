function clearExecutionWorkflow(options = {}) {
  state.executionWorkflow = null;
  if (options.clearBrief) {
    state.executionBrief = null;
  }
  render();
  if (!options.silent) {
    setStatus("Agent workflow cleared", "The execution panel is back to the general control-plane view.");
  }
}

async function loadExecutionWorkflow(contractId, options = {}) {
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
  setStatus("Loading agent workflow...", `Computing the next guided workflow for ${normalizedContractId}.`);
  const response = await fetch(`/api/agent-contracts/${encodeURIComponent(normalizedContractId)}/workflow?${params.toString()}`);
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Agent workflow failed", payload.detail || payload.error || "Unable to build the requested workflow.");
    return;
  }
  state.executionWorkflow = {
    ...payload.workflow,
    contract_id: normalizedContractId,
    loaded_at: new Date().toISOString(),
  };
  if (payload.workflow?.brief) {
    state.executionBrief = {
      ...payload.workflow.brief,
      contract_id: normalizedContractId,
      loaded_at: new Date().toISOString(),
    };
  }
  if (payload.source_of_truth) {
    state.sourceOfTruth = payload.source_of_truth;
  }
  render();
  const focus = payload.workflow?.focus?.title || normalizedContractId;
  setStatus("Agent workflow ready", `${focus} is ready for guided launch or handoff.`);
}

async function launchExecutionWorkflow(contractId, options = {}) {
  const normalizedContractId = getExecutionContractIdForRole(contractId);
  if (state.executionDirty) {
    const saved = await saveExecutionState({ silent: true });
    if (!saved) {
      return;
    }
  }
  setStatus("Launching workflow...", `Preparing a resumable ${normalizedContractId} run.`);
  const response = await fetch(`/api/agent-contracts/${encodeURIComponent(normalizedContractId)}/launch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      expected_revision: Number(state.planState?.revision || 0),
      updated_by: "user",
      task_id: String(options.taskId || ""),
      run_id: String(options.runId || ""),
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Workflow launch failed", payload.detail || payload.error || "Unable to launch the requested workflow.");
    return;
  }
  applyExecutionPayload(payload);
  state.executionWorkflow = {
    ...(payload.workflow || {}),
    contract_id: normalizedContractId,
    loaded_at: new Date().toISOString(),
  };
  if (payload.workflow?.brief) {
    state.executionBrief = {
      ...payload.workflow.brief,
      contract_id: normalizedContractId,
      loaded_at: new Date().toISOString(),
    };
  }
  render();
  const mode = payload.launch_mode === "resume_existing" ? "resumed" : "created";
  setStatus("Workflow launch ready", `${payload.agent_run?.id || normalizedContractId} ${mode} for the guided workflow.`);
}

function renderExecutionWorkflowSection() {
  const workflow = state.executionWorkflow;
  if (!workflow) {
    return "";
  }
  const focus = workflow.focus || {};
  const run = workflow.resumable_run || workflow.starter_run?.agent_run || null;
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Agent workflow</h3>
        <div class="chip-row">
          <button type="button" data-execution-action="launch-workflow" data-execution-contract-id="${escapeHtml(workflow.contract_id || workflow.contract?.id || "")}" ${run ? "" : "disabled"}>Launch guided run</button>
          <button type="button" data-execution-action="copy-brief" ${workflow.brief ? "" : "disabled"}>Copy brief</button>
          <button type="button" data-execution-action="clear-workflow">Clear</button>
        </div>
      </div>
      <div class="column-row execution-editor-card execution-brief-card">
        <div class="chip-row">
          <span class="tag-chip">${escapeHtml(workflow.contract?.id || workflow.contract_id || "agent")}</span>
          ${focus.kind ? `<span class="status-pill">${escapeHtml(focus.kind)}</span>` : ""}
          ${run ? `<span class="status-pill">${escapeHtml(run.id || run.status || "run")}</span>` : ""}
        </div>
        ${focus.title ? `<div class="column-main">${escapeHtml(focus.title)}</div>` : ""}
        ${focus.subtitle ? `<div class="column-meta">${escapeHtml(focus.subtitle)}</div>` : ""}
        ${(workflow.recommended_actions || []).length ? `
          <div class="column-list" style="margin-top: 12px;">
            ${workflow.recommended_actions.map((action) => `
              <div class="column-row">
                <div class="column-main">${escapeHtml(action.label || action.kind || "Action")}</div>
                <div class="column-meta">${escapeHtml(action.reason || "")}</div>
                <div class="column-meta">${escapeHtml(`${action.method || "GET"} ${action.route || ""}`)}</div>
              </div>
            `).join("")}
          </div>
        ` : '<p class="hint">No workflow actions are available yet.</p>'}
      </div>
    </div>
  `;
}
