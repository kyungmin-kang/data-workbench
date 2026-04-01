// Extracted execution rendering and execution-to-graph linking helpers.

function matchesExecutionSearch(item, search) {
  if (!search) {
    return true;
  }
  const haystack = JSON.stringify(item).toLowerCase();
  return haystack.includes(search);
}

function collectExecutionViewContext(planState, execution) {
  const criticalTaskIds = new Set((execution.critical_path || []).map((task) => task.id));
  const blockedTaskIds = new Set((planState.tasks || []).filter((task) => task.status === "blocked").map((task) => task.id));
  const runTaskIds = new Set((planState.agent_runs || []).flatMap((run) => run.task_ids || []));
  const evidenceCheckIds = new Set((planState.evidence || []).map((proof) => proof.check_id).filter(Boolean));
  const evidenceTaskIds = new Set(
    (planState.tasks || [])
      .filter((task) => (task.acceptance_check_ids || []).some((checkId) => evidenceCheckIds.has(checkId)) || (task.evidence_ids || []).length)
      .map((task) => task.id)
  );
  const evidenceDecisionIds = new Set(
    (planState.decisions || [])
      .filter((decision) => (decision.acceptance_check_ids || []).some((checkId) => evidenceCheckIds.has(checkId)) || (decision.evidence_ids || []).length)
      .map((decision) => decision.id)
  );
  return { criticalTaskIds, blockedTaskIds, runTaskIds, evidenceTaskIds, evidenceDecisionIds, evidenceCheckIds };
}

function filterExecutionItems(collection, items, planState, execution) {
  const search = String(state.executionSearch || "").trim().toLowerCase();
  const roleFilter = state.executionRoleFilter || "all";
  const hideCompleted = Boolean(state.executionHideCompleted);
  const view = state.executionView || "all";
  const context = collectExecutionViewContext(planState, execution);
  return (items || []).filter((item) => {
    if (!matchesExecutionSearch(item, search)) {
      return false;
    }
    if (collection === "tasks") {
      if (roleFilter !== "all" && (item.role || "builder") !== roleFilter) {
        return false;
      }
      if (hideCompleted && ["done", "cancelled"].includes(item.status)) {
        return false;
      }
      if (view === "critical" && !context.criticalTaskIds.has(item.id)) {
        return false;
      }
      if (view === "blocked" && item.status !== "blocked") {
        return false;
      }
      if (view === "evidence" && !((item.acceptance_check_ids || []).length || (item.evidence_ids || []).length || context.evidenceTaskIds.has(item.id))) {
        return false;
      }
      if (view === "runs" && !context.runTaskIds.has(item.id)) {
        return false;
      }
      return true;
    }
    if (collection === "decisions") {
      if (view === "blocked" && !(planState.tasks || []).some((task) => item.id && (task.decision_ids || []).includes(item.id) && task.status === "blocked")) {
        return false;
      }
      if (view === "critical" && !(planState.tasks || []).some((task) => item.id && (task.decision_ids || []).includes(item.id) && context.criticalTaskIds.has(task.id))) {
        return false;
      }
      if (view === "evidence" && !((item.acceptance_check_ids || []).length || (item.evidence_ids || []).length || context.evidenceDecisionIds.has(item.id))) {
        return false;
      }
      if (view === "runs" && !(planState.agent_runs || []).some((run) => (run.task_ids || []).some((taskId) => (planState.tasks || []).some((task) => task.id === taskId && (task.decision_ids || []).includes(item.id))))) {
        return false;
      }
      return true;
    }
    if (collection === "blockers") {
      if (hideCompleted && item.status === "resolved") {
        return false;
      }
      if (view === "blocked" && item.status === "resolved") {
        return false;
      }
      if (view === "critical" && !((item.task_ids || []).some((taskId) => context.criticalTaskIds.has(taskId)))) {
        return false;
      }
      if (view === "runs" && !((item.id && (planState.agent_runs || []).some((run) => (run.blocked_by || []).includes(item.id)))) ) {
        return false;
      }
      return true;
    }
    if (collection === "acceptance_checks") {
      if (view === "evidence" || view === "all") {
        return true;
      }
      return view !== "runs";
    }
    if (collection === "evidence") {
      return view === "all" || view === "evidence";
    }
    if (collection === "attachments") {
      return view === "all" || view === "evidence";
    }
    if (collection === "agent_runs") {
      if (view === "runs" || view === "all") {
        return true;
      }
      return false;
    }
    return true;
  });
}

function renderExecutionEditorSection(title, collection, items, emptyMessage, renderer, addLabel) {
  return `
    <div class="section">
      <div class="section-actions">
        <h3>${escapeHtml(title)}</h3>
        <div class="chip-row">
          <span class="hint">${formatValue(items.length)}</span>
          <button type="button" data-execution-action="add-item" data-execution-collection="${escapeHtml(collection)}">${escapeHtml(addLabel)}</button>
        </div>
      </div>
      <div class="column-list">
        ${items.length ? items.map(renderer).join("") : `<p class="hint">${escapeHtml(emptyMessage)}</p>`}
      </div>
    </div>
  `;
}

function renderExecutionDecisionItem(decision) {
  return `
    <div class="column-row execution-editor-card">
      <div class="section-actions">
        <div>
          <div class="column-main">${escapeHtml(decision.title || decision.id)}</div>
          <div class="column-meta">${escapeHtml(decision.id)}</div>
        </div>
        <div class="chip-row">
          <button type="button" data-execution-action="decision-create-task" data-execution-decision-id="${escapeHtml(decision.id)}">Add task</button>
          <button type="button" data-execution-action="load-brief" data-execution-contract-id="${escapeHtml(getExecutionContractIdForRole(decision.kind === "planning" ? "architect" : "builder"))}" data-execution-decision-id="${escapeHtml(decision.id)}">Starter brief</button>
          <button type="button" data-execution-action="remove-item" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}">Remove</button>
        </div>
      </div>
      <div class="execution-editor-grid">
        <label class="execution-editor-field">Title<input class="graph-inline-input" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="title" value="${escapeHtml(decision.title || "")}" /></label>
        <label class="execution-editor-field">Kind<input class="graph-inline-input" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="kind" value="${escapeHtml(decision.kind || "")}" /></label>
        <label class="execution-editor-field">Status
          <select class="graph-inline-select" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="status">
            ${["proposed", "accepted", "in_progress", "validated", "deprecated"].map((status) => `<option value="${status}" ${decision.status === status ? "selected" : ""}>${status}</option>`).join("")}
          </select>
        </label>
        <label class="execution-editor-field">Linked refs<input class="graph-inline-input" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="linked_refs" value="${escapeHtml(formatListValue(decision.linked_refs))}" placeholder="data:market_signals, field.market_signals.price" /></label>
        <label class="execution-editor-field">Acceptance checks<input class="graph-inline-input" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="acceptance_check_ids" value="${escapeHtml(formatListValue(decision.acceptance_check_ids))}" placeholder="check.snapshot.delivery" /></label>
        <label class="execution-editor-field">Evidence proofs<input class="graph-inline-input" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="evidence_ids" value="${escapeHtml(formatListValue(decision.evidence_ids))}" placeholder="evidence.snapshot.delivery" /></label>
        <label class="execution-editor-field">Supersedes<input class="graph-inline-input" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="supersedes_decision_id" value="${escapeHtml(decision.supersedes_decision_id || "")}" placeholder="decision.old_contract" /></label>
        <label class="execution-editor-field execution-editor-checkbox"><input type="checkbox" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="locked" ${decision.locked ? "checked" : ""} /> Locked</label>
      </div>
      <label class="execution-editor-field execution-editor-textarea">Summary<textarea class="graph-inline-input graph-inline-note" data-execution-collection="decisions" data-execution-id="${escapeHtml(decision.id)}" data-execution-field="summary" placeholder="What was agreed and why?">${escapeHtml(decision.summary || "")}</textarea></label>
    </div>
  `;
}

function renderExecutionTaskItem(task) {
  return `
    <div class="column-row execution-editor-card">
      <div class="section-actions">
        <div>
          <div class="column-main">${escapeHtml(task.title || task.id)}</div>
          <div class="column-meta">${escapeHtml(task.id)}</div>
        </div>
        <div class="chip-row">
          <button type="button" data-execution-action="task-quick-status" data-execution-task-id="${escapeHtml(task.id)}" data-execution-task-status="in_progress">Start</button>
          <button type="button" data-execution-action="task-quick-status" data-execution-task-id="${escapeHtml(task.id)}" data-execution-task-status="blocked">Block</button>
          <button type="button" data-execution-action="task-quick-status" data-execution-task-id="${escapeHtml(task.id)}" data-execution-task-status="done">Done</button>
          <button type="button" data-execution-action="task-create-blocker" data-execution-task-id="${escapeHtml(task.id)}">New blocker</button>
          <button type="button" data-execution-action="task-create-run" data-execution-task-id="${escapeHtml(task.id)}">New run</button>
          <button type="button" data-execution-action="load-brief" data-execution-contract-id="${escapeHtml(getExecutionContractIdForRole(task.role || "builder"))}" data-execution-task-id="${escapeHtml(task.id)}">Brief</button>
          <button type="button" data-execution-action="remove-item" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}">Remove</button>
        </div>
      </div>
      <div class="execution-editor-grid">
        <label class="execution-editor-field">Title<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="title" value="${escapeHtml(task.title || "")}" /></label>
        <label class="execution-editor-field">Role<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="role" value="${escapeHtml(task.role || "builder")}" /></label>
        <label class="execution-editor-field">Status
          <select class="graph-inline-select" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="status">
            ${["todo", "in_progress", "blocked", "done", "cancelled"].map((status) => `<option value="${status}" ${task.status === status ? "selected" : ""}>${status}</option>`).join("")}
          </select>
        </label>
        <label class="execution-editor-field">Decisions<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="decision_ids" value="${escapeHtml(formatListValue(task.decision_ids))}" placeholder="decision.market_snapshot" /></label>
        <label class="execution-editor-field">Linked refs<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="linked_refs" value="${escapeHtml(formatListValue(task.linked_refs))}" placeholder="contract:api.market_snapshot" /></label>
        <label class="execution-editor-field">Depends on<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="depends_on" value="${escapeHtml(formatListValue(task.depends_on))}" placeholder="task.other_step" /></label>
        <label class="execution-editor-field">Acceptance checks<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="acceptance_check_ids" value="${escapeHtml(formatListValue(task.acceptance_check_ids))}" placeholder="check.snapshot.delivery" /></label>
        <label class="execution-editor-field">Evidence proofs<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="evidence_ids" value="${escapeHtml(formatListValue(task.evidence_ids))}" placeholder="evidence.snapshot.delivery" /></label>
        <label class="execution-editor-field">Blockers<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="blocker_ids" value="${escapeHtml(formatListValue(task.blocker_ids))}" placeholder="blocker.snapshot.confirmation" /></label>
        <label class="execution-editor-field execution-editor-checkbox"><input type="checkbox" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="exploratory" ${task.exploratory ? "checked" : ""} /> Exploratory</label>
        ${task.exploratory ? `<label class="execution-editor-field">Exploration goal<input class="graph-inline-input" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="exploration_goal" value="${escapeHtml(task.exploration_goal || "")}" placeholder="Clarify unknown schema or sequencing" /></label>` : ""}
      </div>
      <label class="execution-editor-field execution-editor-textarea">Summary<textarea class="graph-inline-input graph-inline-note" data-execution-collection="tasks" data-execution-id="${escapeHtml(task.id)}" data-execution-field="summary" placeholder="One focused session for one role.">${escapeHtml(task.summary || "")}</textarea></label>
    </div>
  `;
}

function renderExecutionBlockerItem(blocker) {
  return `
    <div class="column-row execution-editor-card">
      <div class="section-actions">
        <div>
          <div class="column-main">${escapeHtml(blocker.summary || blocker.id)}</div>
          <div class="column-meta">${escapeHtml(blocker.id)}</div>
        </div>
        <div class="chip-row">
          <button type="button" data-execution-action="blocker-quick-status" data-execution-blocker-id="${escapeHtml(blocker.id)}" data-execution-blocker-status="resolved">Resolve</button>
          <button type="button" data-execution-action="focus-search" data-execution-search="${escapeHtml(blocker.id)}" data-execution-view="blocked">Focus</button>
          <button type="button" data-execution-action="remove-item" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}">Remove</button>
        </div>
      </div>
      <div class="execution-editor-grid">
        <label class="execution-editor-field">Type<input class="graph-inline-input" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}" data-execution-field="type" value="${escapeHtml(blocker.type || "")}" /></label>
        <label class="execution-editor-field">Status
          <select class="graph-inline-select" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}" data-execution-field="status">
            ${["open", "in_progress", "resolved"].map((status) => `<option value="${status}" ${blocker.status === status ? "selected" : ""}>${status}</option>`).join("")}
          </select>
        </label>
        <label class="execution-editor-field">Tasks<input class="graph-inline-input" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}" data-execution-field="task_ids" value="${escapeHtml(formatListValue(blocker.task_ids))}" placeholder="task.market_snapshot.delivery" /></label>
        <label class="execution-editor-field">Decisions<input class="graph-inline-input" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}" data-execution-field="decision_ids" value="${escapeHtml(formatListValue(blocker.decision_ids))}" placeholder="decision.market_snapshot" /></label>
        <label class="execution-editor-field">Linked refs<input class="graph-inline-input" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}" data-execution-field="linked_refs" value="${escapeHtml(formatListValue(blocker.linked_refs))}" placeholder="contract:api.market_snapshot" /></label>
        <label class="execution-editor-field">Owner<input class="graph-inline-input" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}" data-execution-field="owner" value="${escapeHtml(blocker.owner || "")}" placeholder="qa" /></label>
      </div>
      <label class="execution-editor-field execution-editor-textarea">Summary<textarea class="graph-inline-input graph-inline-note" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}" data-execution-field="summary" placeholder="What is blocked?">${escapeHtml(blocker.summary || "")}</textarea></label>
      <label class="execution-editor-field execution-editor-textarea">Suggested resolution<textarea class="graph-inline-input graph-inline-note" data-execution-collection="blockers" data-execution-id="${escapeHtml(blocker.id)}" data-execution-field="suggested_resolution" placeholder="What should unblock this?">${escapeHtml(blocker.suggested_resolution || "")}</textarea></label>
    </div>
  `;
}

function renderExecutionCheckItem(check) {
  return `
    <div class="column-row execution-editor-card">
      <div class="section-actions">
        <div>
          <div class="column-main">${escapeHtml(check.label || check.id)}</div>
          <div class="column-meta">${escapeHtml(check.id)}</div>
        </div>
        <div class="chip-row">
          <button type="button" data-execution-action="create-proof" data-execution-check-id="${escapeHtml(check.id)}">Add proof</button>
          <button type="button" data-execution-action="remove-item" data-execution-collection="acceptance_checks" data-execution-id="${escapeHtml(check.id)}">Remove</button>
        </div>
      </div>
      <div class="execution-editor-grid">
        <label class="execution-editor-field">Label<input class="graph-inline-input" data-execution-collection="acceptance_checks" data-execution-id="${escapeHtml(check.id)}" data-execution-field="label" value="${escapeHtml(check.label || "")}" /></label>
        <label class="execution-editor-field">Kind<input class="graph-inline-input" data-execution-collection="acceptance_checks" data-execution-id="${escapeHtml(check.id)}" data-execution-field="kind" value="${escapeHtml(check.kind || "")}" /></label>
        <label class="execution-editor-field">Linked refs<input class="graph-inline-input" data-execution-collection="acceptance_checks" data-execution-id="${escapeHtml(check.id)}" data-execution-field="linked_refs" value="${escapeHtml(formatListValue(check.linked_refs))}" placeholder="contract:api.market_snapshot" /></label>
        <label class="execution-editor-field execution-editor-checkbox"><input type="checkbox" data-execution-collection="acceptance_checks" data-execution-id="${escapeHtml(check.id)}" data-execution-field="required" ${check.required !== false ? "checked" : ""} /> Required</label>
      </div>
    </div>
  `;
}

function renderExecutionEvidenceItem(proof) {
  return `
    <div class="column-row execution-editor-card">
      <div class="section-actions">
        <div>
          <div class="column-main">${escapeHtml(proof.id)}</div>
          <div class="column-meta">${escapeHtml(proof.check_id || "No linked acceptance check yet")}</div>
        </div>
        <div class="chip-row">
          <button type="button" data-execution-action="evidence-quick-status" data-execution-evidence-id="${escapeHtml(proof.id)}" data-execution-evidence-status="verified">Verify</button>
          <button type="button" data-execution-action="evidence-quick-status" data-execution-evidence-id="${escapeHtml(proof.id)}" data-execution-evidence-status="rejected">Reject</button>
          <button type="button" data-execution-action="remove-item" data-execution-collection="evidence" data-execution-id="${escapeHtml(proof.id)}">Remove</button>
        </div>
      </div>
      <div class="execution-editor-grid">
        <label class="execution-editor-field">Check id<input class="graph-inline-input" data-execution-collection="evidence" data-execution-id="${escapeHtml(proof.id)}" data-execution-field="check_id" value="${escapeHtml(proof.check_id || "")}" placeholder="check.snapshot.delivery" /></label>
        <label class="execution-editor-field">Status
          <select class="graph-inline-select" data-execution-collection="evidence" data-execution-id="${escapeHtml(proof.id)}" data-execution-field="status">
            ${["recorded", "verified", "rejected"].map((status) => `<option value="${status}" ${proof.status === status ? "selected" : ""}>${status}</option>`).join("")}
          </select>
        </label>
        <label class="execution-editor-field">Attachments<input class="graph-inline-input" data-execution-collection="evidence" data-execution-id="${escapeHtml(proof.id)}" data-execution-field="attachment_ids" value="${escapeHtml(formatListValue(proof.attachment_ids))}" placeholder="attachment.snapshot.note" /></label>
        <label class="execution-editor-field">Recorded by<input class="graph-inline-input" data-execution-collection="evidence" data-execution-id="${escapeHtml(proof.id)}" data-execution-field="recorded_by" value="${escapeHtml(proof.recorded_by || "")}" placeholder="qa" /></label>
      </div>
      <label class="execution-editor-field execution-editor-textarea">Proof summary<textarea class="graph-inline-input graph-inline-note" data-execution-collection="evidence" data-execution-id="${escapeHtml(proof.id)}" data-execution-field="summary" placeholder="How does this satisfy the check?">${escapeHtml(proof.summary || "")}</textarea></label>
    </div>
  `;
}

function renderExecutionAttachmentItem(attachment) {
  return `
    <div class="column-row execution-editor-card">
      <div class="section-actions">
        <div>
          <div class="column-main">${escapeHtml(attachment.label || attachment.id)}</div>
          <div class="column-meta">${escapeHtml(attachment.id)}</div>
        </div>
        <button type="button" data-execution-action="remove-item" data-execution-collection="attachments" data-execution-id="${escapeHtml(attachment.id)}">Remove</button>
      </div>
      <div class="execution-editor-grid">
        <label class="execution-editor-field">Kind<input class="graph-inline-input" data-execution-collection="attachments" data-execution-id="${escapeHtml(attachment.id)}" data-execution-field="kind" value="${escapeHtml(attachment.kind || "")}" /></label>
        <label class="execution-editor-field">Label<input class="graph-inline-input" data-execution-collection="attachments" data-execution-id="${escapeHtml(attachment.id)}" data-execution-field="label" value="${escapeHtml(attachment.label || "")}" /></label>
        <label class="execution-editor-field">Path or URL<input class="graph-inline-input" data-execution-collection="attachments" data-execution-id="${escapeHtml(attachment.id)}" data-execution-field="path_or_url" value="${escapeHtml(attachment.path_or_url || "")}" placeholder="/absolute/path/or/url" /></label>
        <label class="execution-editor-field">Decisions<input class="graph-inline-input" data-execution-collection="attachments" data-execution-id="${escapeHtml(attachment.id)}" data-execution-field="linked_decision_ids" value="${escapeHtml(formatListValue(attachment.linked_decision_ids))}" placeholder="decision.market_snapshot" /></label>
        <label class="execution-editor-field">Tasks<input class="graph-inline-input" data-execution-collection="attachments" data-execution-id="${escapeHtml(attachment.id)}" data-execution-field="linked_task_ids" value="${escapeHtml(formatListValue(attachment.linked_task_ids))}" placeholder="task.market_snapshot.delivery" /></label>
      </div>
    </div>
  `;
}

function renderExecutionRunItem(run) {
  return `
    <div class="column-row execution-editor-card">
      <div class="section-actions">
        <div>
          <div class="column-main">${escapeHtml(run.objective || run.id)}</div>
          <div class="column-meta">${escapeHtml(run.id)}</div>
        </div>
        <div class="chip-row">
          <button type="button" data-execution-action="run-quick-status" data-execution-run-id="${escapeHtml(run.id)}" data-execution-run-status="running">Resume</button>
          <button type="button" data-execution-action="run-quick-status" data-execution-run-id="${escapeHtml(run.id)}" data-execution-run-status="waiting">Wait</button>
          <button type="button" data-execution-action="run-quick-status" data-execution-run-id="${escapeHtml(run.id)}" data-execution-run-status="blocked">Block</button>
          <button type="button" data-execution-action="run-quick-status" data-execution-run-id="${escapeHtml(run.id)}" data-execution-run-status="completed">Complete</button>
          <button type="button" data-execution-action="run-handoff" data-execution-run-id="${escapeHtml(run.id)}">Handoff</button>
          <button type="button" data-execution-action="load-brief" data-execution-contract-id="${escapeHtml(getExecutionContractIdForRole(run.role || "builder"))}" data-execution-run-id="${escapeHtml(run.id)}">Brief</button>
          <button type="button" data-execution-action="remove-item" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}">Remove</button>
        </div>
      </div>
      <div class="execution-editor-grid">
        <label class="execution-editor-field">Role<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="role" value="${escapeHtml(run.role || "")}" /></label>
        <label class="execution-editor-field">Status
          <select class="graph-inline-select" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="status">
            ${["planned", "running", "waiting", "blocked", "completed", "failed", "cancelled"].map((status) => `<option value="${status}" ${run.status === status ? "selected" : ""}>${status}</option>`).join("")}
          </select>
        </label>
        <label class="execution-editor-field">Tasks<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="task_ids" value="${escapeHtml(formatListValue(run.task_ids))}" placeholder="task.market_snapshot.delivery" /></label>
        <label class="execution-editor-field">Blocked by<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="blocked_by" value="${escapeHtml(formatListValue(run.blocked_by))}" placeholder="blocker.market_snapshot.confirmation" /></label>
        <label class="execution-editor-field">Objective<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="objective" value="${escapeHtml(run.objective || "")}" /></label>
        <label class="execution-editor-field">Bundle id<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="bundle_id" value="${escapeHtml(run.bundle_id || "")}" /></label>
        <label class="execution-editor-field">Branch<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="branch" value="${escapeHtml(run.branch || "")}" placeholder="codex/..." /></label>
        <label class="execution-editor-field">Next action<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="next_action_hint" value="${escapeHtml(run.next_action_hint || "")}" placeholder="What should happen next?" /></label>
        <label class="execution-editor-field">Status reason<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="status_reason" value="${escapeHtml(run.status_reason || "")}" placeholder="Why is this state current?" /></label>
        <label class="execution-editor-field">Outputs<input class="graph-inline-input" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="outputs" value="${escapeHtml(formatListValue(run.outputs))}" placeholder="docs/summary.md, PR #12" /></label>
      </div>
      <label class="execution-editor-field execution-editor-textarea">Last summary<textarea class="graph-inline-input graph-inline-note" data-execution-collection="agent_runs" data-execution-id="${escapeHtml(run.id)}" data-execution-field="last_summary" placeholder="Most recent useful update">${escapeHtml(run.last_summary || "")}</textarea></label>
      ${(run.events || []).length ? `
        <div class="execution-editor-events">
          <strong>Events</strong>
          <ul>
            ${(run.events || []).map((event) => `<li>${escapeHtml(event.kind || "note")}: ${escapeHtml(event.summary || "")}</li>`).join("")}
          </ul>
        </div>
      ` : ""}
    </div>
  `;
}

function renderExecutionAgreementLog(entries) {
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Agreement history</h3>
        <span class="hint">${formatValue(entries.length)}</span>
      </div>
      <div class="column-list">
        ${entries.length ? entries.slice().reverse().slice(0, 8).map((entry) => `
          <div class="column-row">
            <div class="column-main">${escapeHtml(entry.summary || entry.id)}</div>
            <div class="column-meta">${escapeHtml(entry.actor || "system")} · ${escapeHtml(entry.at || "pending timestamp")}</div>
          </div>
        `).join("") : '<p class="hint">Agreement history will appear here as reviews, validation, and merges happen.</p>'}
      </div>
    </div>
  `;
}

function renderExecutionContracts(contracts) {
  if (!contracts.length) {
    return "";
  }
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Agent contracts</h3>
        <span class="hint">Standalone-first, agent-enhanced rules</span>
      </div>
      <div class="column-list">
        ${contracts.map((contract) => `
          <div class="column-row execution-editor-card">
            <div class="column-main">${escapeHtml(contract.id || contract.role || "agent")}</div>
            <div class="column-meta">${escapeHtml(contract.summary || "")}</div>
            ${contract.mission ? `<div class="column-meta"><strong>Mission:</strong> ${escapeHtml(contract.mission)}</div>` : ""}
            <div class="chip-row">
              <span class="tag-chip">${escapeHtml(contract.role || "agent")}</span>
              <span class="status-pill">${escapeHtml(`Reads ${formatValue((contract.reads || []).length)}`)}</span>
              <span class="status-pill">${escapeHtml(`Writes ${formatValue((contract.writes || []).length)}`)}</span>
              <button type="button" data-execution-action="load-workflow" data-execution-contract-id="${escapeHtml(contract.id || "")}">Workflow</button>
              <button type="button" data-execution-action="launch-workflow" data-execution-contract-id="${escapeHtml(contract.id || "")}">Launch run</button>
              <button type="button" data-execution-action="load-brief" data-execution-contract-id="${escapeHtml(contract.id || "")}">Starter brief</button>
            </div>
            ${(contract.operating_loop || []).length ? `
              <details>
                <summary>Playbook</summary>
                <ul class="warning-list">${(contract.operating_loop || []).map((step) => `<li>${escapeHtml(step)}</li>`).join("")}</ul>
                ${contract.starter_prompt ? `<p class="hint"><strong>Starter prompt:</strong> ${escapeHtml(contract.starter_prompt)}</p>` : ""}
                ${contract.playbook_path ? `<p class="hint">Playbook path: ${escapeHtml(contract.playbook_path)}</p>` : ""}
              </details>
            ` : ""}
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function renderExecutionRoleLanes(roleLanes) {
  if (!roleLanes.length) {
    return "";
  }
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Role lanes</h3>
        <span class="hint">Top open work grouped by owner role</span>
      </div>
      <div class="execution-lane-grid">
        ${roleLanes.map((lane) => `
          <div class="column-row execution-editor-card execution-lane-card">
            <div class="section-actions">
              <div>
                <div class="column-main">${escapeHtml(lane.role || "builder")}</div>
                <div class="column-meta">${escapeHtml(`${formatValue(lane.open_count || 0)} open task${lane.open_count === 1 ? "" : "s"}`)}</div>
              </div>
              <button type="button" data-execution-action="focus-role" data-execution-role="${escapeHtml(lane.role || "builder")}">Focus role</button>
            </div>
            <div class="column-list">
              ${(lane.tasks || []).length ? lane.tasks.map((task) => `
                <div class="column-row">
                  <div class="column-main">${escapeHtml(task.title || task.id)}</div>
                  <div class="chip-row">
                    <span class="status-pill ${escapeHtml(task.status === "blocked" ? "broken" : task.status === "in_progress" ? "warning" : "")}">${escapeHtml(task.status || "todo")}</span>
                    <button type="button" data-execution-action="load-brief" data-execution-contract-id="${escapeHtml(getExecutionContractIdForRole(task.role || lane.role || "builder"))}" data-execution-task-id="${escapeHtml(task.id)}">Brief</button>
                  </div>
                </div>
              `).join("") : '<p class="hint">No open tasks in this lane.</p>'}
            </div>
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function renderExecutionHandoffQueue(queue) {
  if (!queue.length) {
    return "";
  }
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Handoff queue</h3>
        <span class="hint">${formatValue(queue.length)} run${queue.length === 1 ? "" : "s"} waiting for attention</span>
      </div>
      <div class="column-list">
        ${queue.map((run) => `
          <div class="column-row execution-editor-card">
            <div class="section-actions">
              <div>
                <div class="column-main">${escapeHtml(run.objective || run.id)}</div>
                <div class="column-meta">${escapeHtml(`${run.role || "agent"} · ${run.status || "waiting"}`)}</div>
              </div>
              <div class="chip-row">
                <button type="button" data-execution-action="load-brief" data-execution-contract-id="${escapeHtml(getExecutionContractIdForRole(run.role || "builder"))}" data-execution-run-id="${escapeHtml(run.id)}">Brief</button>
                <button type="button" data-execution-action="focus-search" data-execution-search="${escapeHtml(run.id)}" data-execution-view="runs">Focus</button>
              </div>
            </div>
            ${run.status_reason ? `<div class="column-meta"><strong>Reason:</strong> ${escapeHtml(run.status_reason)}</div>` : ""}
            ${run.next_action_hint ? `<div class="column-meta"><strong>Next:</strong> ${escapeHtml(run.next_action_hint)}</div>` : ""}
            ${run.task_titles?.length ? `<div class="column-meta"><strong>Tasks:</strong> ${escapeHtml(run.task_titles.join(", "))}</div>` : ""}
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function renderExecutionSummary() {
  if (!executionSummary) {
    return;
  }
  const planState = ensureEditablePlanState();
  const execution = getExecutionData();
  const counts = execution.counts || {};
  const topTasks = execution.top_open_tasks || [];
  const topBlocker = execution.top_blocker || null;
  const topDecision = execution.highest_risk_decision || null;
  const criticalPath = execution.critical_path || [];
  const blockedWork = execution.blocked_work || [];
  const recentlyCompleted = execution.recently_completed || [];
  const roleLanes = execution.role_lanes || [];
  const handoffQueue = execution.handoff_queue || [];
  const resumableRuns = execution.resumable_runs || [];
  const localIssues = buildLocalExecutionIssues(planState);
  const filteredDecisions = filterExecutionItems("decisions", planState.decisions || [], planState, execution);
  const filteredTasks = filterExecutionItems("tasks", planState.tasks || [], planState, execution);
  const filteredBlockers = filterExecutionItems("blockers", planState.blockers || [], planState, execution);
  const filteredChecks = filterExecutionItems("acceptance_checks", planState.acceptance_checks || [], planState, execution);
  const filteredEvidence = filterExecutionItems("evidence", planState.evidence || [], planState, execution);
  const filteredAttachments = filterExecutionItems("attachments", planState.attachments || [], planState, execution);
  const filteredRuns = filterExecutionItems("agent_runs", planState.agent_runs || [], planState, execution);
  executionSummary.innerHTML = `
    <div class="compact-summary-stack">
      <div class="section-actions">
        <h3>Execution control plane</h3>
        <div class="chip-row">
          <button type="button" data-execution-action="refresh">Refresh</button>
          <button type="button" data-execution-action="derive-tasks">Derive tasks</button>
          <button type="button" data-execution-action="save">${state.executionDirty ? "Save execution draft" : "Save execution state"}</button>
        </div>
      </div>
      ${state.executionDirty ? '<div class="status-pill warning">Unsaved execution edits are not yet reflected in derived priorities.</div>' : ""}
      ${renderExecutionWorkflowSection()}
      ${renderExecutionBriefSection()}
      <div class="section execution-toolbar">
        <div class="chip-row">
          ${[
            ["all", "All"],
            ["critical", "Critical path"],
            ["blocked", "Blocked"],
            ["evidence", "Evidence"],
            ["runs", "Runs"],
          ].map(([view, label]) => `<button type="button" class="${state.executionView === view ? "primary" : ""}" data-execution-action="set-view" data-execution-view="${view}">${label}</button>`).join("")}
        </div>
        <div class="execution-editor-grid">
          <label class="execution-editor-field">Search<input class="graph-inline-input" data-execution-control="search" value="${escapeHtml(state.executionSearch || "")}" placeholder="Search decisions, tasks, blockers, or runs" /></label>
          <label class="execution-editor-field">Role filter
            <select class="graph-inline-select" data-execution-control="role_filter">
              ${["all", "architect", "scout", "builder", "qa"].map((role) => `<option value="${role}" ${state.executionRoleFilter === role ? "selected" : ""}>${role === "all" ? "all roles" : role}</option>`).join("")}
            </select>
          </label>
          <label class="execution-editor-field execution-editor-checkbox"><input type="checkbox" data-execution-control="hide_completed" ${state.executionHideCompleted ? "checked" : ""} /> Hide completed</label>
        </div>
      </div>
      <div class="meta-grid compact-summary">
        <div><strong>Revision</strong><br>${formatValue(execution.revision || 0)}</div>
        <div><strong>Updated by</strong><br>${escapeHtml(execution.updated_by || "bridge")}</div>
        <div><strong>Open tasks</strong><br>${formatValue(counts.open_tasks || 0)}</div>
        <div><strong>Blocked</strong><br>${formatValue(counts.blocked_tasks || 0)}</div>
      </div>
      <div class="chip-row">
        <span class="status-pill">${escapeHtml(`Decisions: ${formatValue(counts.decisions || 0)}`)}</span>
        <span class="status-pill">${escapeHtml(`Tasks: ${formatValue(counts.tasks || 0)}`)}</span>
        <span class="status-pill">${escapeHtml(`Blockers: ${formatValue(counts.blockers || 0)}`)}</span>
        <span class="status-pill">${escapeHtml(`Runs: ${formatValue(counts.agent_runs || 0)}`)}</span>
        ${(execution.open_tasks_by_role || []).map((item) => `<span class="tag-chip">${escapeHtml(`${item.role}: ${formatValue(item.count)}`)}</span>`).join("")}
      </div>
      ${localIssues.length ? `
        <div class="section">
          <div class="section-actions">
            <h3>Draft validation hints</h3>
            <span class="hint">${formatValue(localIssues.length)} issue${localIssues.length === 1 ? "" : "s"}</span>
          </div>
          <ul class="warning-list">
            ${localIssues.slice(0, 8).map((issue) => `<li class="${escapeHtml(issue.severity === "error" ? "broken" : "")}">${escapeHtml(issue.message)}</li>`).join("")}
          </ul>
        </div>
      ` : '<div class="success-chip">✓ Draft execution state currently passes local guardrails</div>'}
      ${topTasks.length ? `
        <div class="section">
          <div class="section-actions">
            <h3>Top open tasks</h3>
            <span class="hint">${formatValue(topTasks.length)} surfaced for priority</span>
          </div>
          <div class="column-list">
            ${topTasks.slice(0, 3).map((task) => `
              <div class="column-row">
                <div class="column-main">${escapeHtml(task.title || task.id)}</div>
                <div class="chip-row">
                  <span class="status-pill ${escapeHtml(task.status === "blocked" ? "broken" : task.status === "in_progress" ? "warning" : "")}">${escapeHtml(task.status || "todo")}</span>
                  <span class="tag-chip">${escapeHtml(task.role || "builder")}</span>
                </div>
                ${task.summary ? `<div class="column-meta">${escapeHtml(task.summary)}</div>` : ""}
              </div>
            `).join("")}
          </div>
        </div>
      ` : '<div class="success-chip">✓ No open execution tasks yet</div>'}
      ${(topBlocker || topDecision) ? `
        <div class="meta-grid compact-summary">
          <div><strong>Top blocker</strong><br>${escapeHtml(topBlocker?.summary || "None")}</div>
          <div><strong>Highest-risk decision</strong><br>${escapeHtml(topDecision?.title || "None")}</div>
        </div>
      ` : ""}
      ${criticalPath.length ? `
        <div class="section">
          <div class="section-actions">
            <h3>Critical path</h3>
            <span class="hint">${formatValue(criticalPath.length)} ordered step${criticalPath.length === 1 ? "" : "s"}</span>
          </div>
          <div class="column-list">
            ${criticalPath.slice(0, 4).map((task) => `
              <div class="column-row">
                <div class="column-main">${escapeHtml(task.title || task.id)}</div>
                <div class="column-meta">${escapeHtml(task.role || "builder")} · ${escapeHtml(task.status || "todo")}</div>
              </div>
            `).join("")}
          </div>
        </div>
      ` : ""}
      ${blockedWork.length ? `
        <div class="section">
          <div class="section-actions">
            <h3>Blocked work</h3>
            <span class="hint">${formatValue(blockedWork.length)} blocking task${blockedWork.length === 1 ? "" : "s"}</span>
          </div>
          <div class="column-list">
            ${blockedWork.slice(0, 4).map((task) => `
              <div class="column-row">
                <div class="column-main">${escapeHtml(task.title || task.id)}</div>
                <div class="column-meta">${escapeHtml((task.blockers || []).join(" | ") || task.summary || "Blocked")}</div>
              </div>
            `).join("")}
          </div>
        </div>
      ` : ""}
      ${recentlyCompleted.length ? `
        <div class="section">
          <div class="section-actions">
            <h3>Recently completed</h3>
            <span class="hint">${formatValue(recentlyCompleted.length)}</span>
          </div>
          <div class="column-list">
            ${recentlyCompleted.slice(0, 4).map((task) => `
              <div class="column-row">
                <div class="column-main">${escapeHtml(task.title || task.id)}</div>
                <div class="column-meta">${escapeHtml(task.completed_at || task.updated_at || "completed")}</div>
              </div>
            `).join("")}
          </div>
        </div>
      ` : ""}
      ${renderExecutionRoleLanes(roleLanes)}
      ${renderExecutionHandoffQueue(handoffQueue)}
      ${resumableRuns.length ? `
        <div class="section">
          <div class="section-actions">
            <h3>Resumable runs</h3>
            <span class="hint">${formatValue(resumableRuns.length)}</span>
          </div>
          <div class="column-list">
            ${resumableRuns.slice(0, 4).map((run) => `
              <div class="column-row">
                <div class="column-main">${escapeHtml(run.objective || run.id)}</div>
                <div class="column-meta">${escapeHtml(run.status_reason || run.last_summary || run.status || "")}</div>
              </div>
            `).join("")}
          </div>
        </div>
      ` : ""}
      ${execution.missing_proof_decisions?.length ? `
        <div class="section">
          <div class="section-actions">
            <h3>Missing proof</h3>
            <span class="hint">${formatValue(execution.missing_proof_decisions.length)} decision${execution.missing_proof_decisions.length === 1 ? "" : "s"} need evidence</span>
          </div>
          <div class="column-list">
            ${execution.missing_proof_decisions.slice(0, 4).map((decision) => `
              <div class="column-row">
                <div class="column-main">${escapeHtml(decision.title || decision.id)}</div>
                <div class="column-meta">${escapeHtml((decision.missing_required_checks || []).join(", ") || "Missing required proof")}</div>
              </div>
            `).join("")}
          </div>
        </div>
      ` : ""}
      ${renderExecutionContracts(execution.agent_contracts || [])}
      ${renderExecutionEditorSection("Decisions", "decisions", filteredDecisions, "Capture agreed intent before execution drifts.", renderExecutionDecisionItem, "Add decision")}
      ${renderExecutionEditorSection("Tasks", "tasks", filteredTasks, "Tasks should stay small enough for one focused session.", renderExecutionTaskItem, "Add task")}
      ${renderExecutionEditorSection("Blockers", "blockers", filteredBlockers, "Structured blockers drive triage and handoff.", renderExecutionBlockerItem, "Add blocker")}
      ${renderExecutionEditorSection("Acceptance checks", "acceptance_checks", filteredChecks, "Checks define what counts as done.", renderExecutionCheckItem, "Add check")}
      ${renderExecutionEditorSection("Evidence", "evidence", filteredEvidence, "Evidence should prove a specific acceptance check.", renderExecutionEvidenceItem, "Add evidence")}
      ${renderExecutionEditorSection("Attachments", "attachments", filteredAttachments, "Attach concise context to decisions and tasks.", renderExecutionAttachmentItem, "Add attachment")}
      ${renderExecutionEditorSection("Agent runs", "agent_runs", filteredRuns, "Runs should stay inspectable and resumable.", renderExecutionRunItem, "Add run")}
      ${renderExecutionAgreementLog(planState.agreement_log || [])}
    </div>
  `;
}

function getExecutionRefsForNode(node) {
  return new Set([
    node.id,
    ...(node.columns || []).map((column) => column.id).filter(Boolean),
    ...((node.contract?.fields || []).map((field) => field.id).filter(Boolean)),
  ]);
}

function getNodeExecutionSummary(nodeId) {
  const node = getNodeById(nodeId);
  if (!node || !state.planState) {
    return null;
  }
  const refs = getExecutionRefsForNode(node);
  const linkedDecisions = (state.planState.decisions || []).filter((decision) => (decision.linked_refs || []).some((ref) => refs.has(ref)));
  const linkedTasks = (state.planState.tasks || []).filter((task) => (task.linked_refs || []).some((ref) => refs.has(ref)));
  const linkedBlockers = (state.planState.blockers || []).filter((blocker) => (
    (blocker.linked_refs || []).some((ref) => refs.has(ref))
      || (blocker.task_ids || []).some((taskId) => linkedTasks.some((task) => task.id === taskId))
      || (blocker.decision_ids || []).some((decisionId) => linkedDecisions.some((decision) => decision.id === decisionId))
  ));
  if (!linkedDecisions.length && !linkedTasks.length && !linkedBlockers.length) {
    return null;
  }
  return {
    decisions: linkedDecisions,
    tasks: linkedTasks,
    blockers: linkedBlockers,
    openTaskCount: linkedTasks.filter((task) => !["done", "cancelled"].includes(task.status)).length,
    blockedTaskCount: linkedTasks.filter((task) => task.status === "blocked").length,
  };
}

function renderGraphNodeExecutionBar(node) {
  const summary = getNodeExecutionSummary(node.id);
  if (!summary) {
    return "";
  }
  const chips = [
    `<span class="graph-node-review-chip">${escapeHtml(`${formatValue(summary.decisions.length)} decision${summary.decisions.length === 1 ? "" : "s"}`)}</span>`,
    `<span class="graph-node-review-chip">${escapeHtml(`${formatValue(summary.openTaskCount)} open task${summary.openTaskCount === 1 ? "" : "s"}`)}</span>`,
  ];
  if (summary.blockedTaskCount) {
    chips.push(`<span class="graph-node-review-chip broken">${escapeHtml(`${formatValue(summary.blockedTaskCount)} blocked`)}</span>`);
  }
  if (summary.blockers.length) {
    chips.push(`<span class="graph-node-review-chip warning">${escapeHtml(`${formatValue(summary.blockers.length)} blocker${summary.blockers.length === 1 ? "" : "s"}`)}</span>`);
  }
  chips.push(
    `<button class="ghost-button small" type="button" data-focus-execution-node="${escapeHtml(node.id)}" data-focus-execution-view="all">Execution</button>`
  );
  if (summary.blockedTaskCount) {
    chips.push(
      `<button class="ghost-button small" type="button" data-focus-execution-node="${escapeHtml(node.id)}" data-focus-execution-view="blocked">Blocked</button>`
    );
  }
  return `<div class="graph-node-review-bar execution"><div class="chip-row">${chips.join("")}</div></div>`;
}

function renderNodeExecution(node) {
  const summary = getNodeExecutionSummary(node.id);
  if (!summary) {
    return '<p class="hint">No linked execution state yet. The graph still remains the structural source of truth.</p>';
  }
  return `
    <div class="section-actions">
      <h3>Execution links</h3>
      <div class="chip-row">
        <button type="button" data-focus-execution-node="${escapeHtml(node.id)}" data-focus-execution-view="all">Show in execution</button>
        ${summary.blockedTaskCount ? `<button type="button" data-focus-execution-node="${escapeHtml(node.id)}" data-focus-execution-view="blocked">Show blocked</button>` : ""}
      </div>
    </div>
    <div class="meta-grid compact-summary">
      <div><strong>Decisions</strong><br>${formatValue(summary.decisions.length)}</div>
      <div><strong>Open tasks</strong><br>${formatValue(summary.openTaskCount)}</div>
      <div><strong>Blocked tasks</strong><br>${formatValue(summary.blockedTaskCount)}</div>
      <div><strong>Blockers</strong><br>${formatValue(summary.blockers.length)}</div>
    </div>
    <div class="section">
      <h3>Linked decisions</h3>
      <div class="column-list">
        ${summary.decisions.length ? summary.decisions.slice(0, 3).map((decision) => `
          <div class="column-row">
            <div class="column-main">${escapeHtml(decision.title || decision.id)}</div>
            <div class="chip-row">
              <span class="status-pill ${escapeHtml(decision.status === "validated" ? "healthy" : decision.status === "in_progress" ? "warning" : "")}">${escapeHtml(decision.status || "proposed")}</span>
            </div>
            ${decision.summary ? `<div class="column-meta">${escapeHtml(decision.summary)}</div>` : ""}
          </div>
        `).join("") : '<div class="success-chip">✓ No linked decisions</div>'}
      </div>
    </div>
    <div class="section">
      <h3>Linked tasks</h3>
      <div class="column-list">
        ${summary.tasks.length ? summary.tasks.slice(0, 4).map((task) => `
          <div class="column-row">
            <div class="column-main">${escapeHtml(task.title || task.id)}</div>
            <div class="chip-row">
              <span class="status-pill ${escapeHtml(task.status === "blocked" ? "broken" : task.status === "in_progress" ? "warning" : task.status === "done" ? "healthy" : "")}">${escapeHtml(task.status || "todo")}</span>
              <span class="tag-chip">${escapeHtml(task.role || "builder")}</span>
            </div>
            ${task.summary ? `<div class="column-meta">${escapeHtml(task.summary)}</div>` : ""}
          </div>
        `).join("") : '<div class="success-chip">✓ No linked tasks</div>'}
      </div>
    </div>
  `;
}
