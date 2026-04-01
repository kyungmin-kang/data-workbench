// Extracted execution control-plane logic from app.js so the app shell stays maintainable.

function createEmptyPlanState() {
  return {
    revision: 0,
    updated_at: "",
    updated_by: "",
    decisions: [],
    tasks: [],
    blockers: [],
    acceptance_checks: [],
    evidence: [],
    attachments: [],
    agent_runs: [],
    agreement_log: [],
  };
}

function clonePlanStateData(planState = null) {
  return JSON.parse(JSON.stringify(planState || createEmptyPlanState()));
}

function ensureEditablePlanState() {
  if (!state.planState) {
    state.planState = createEmptyPlanState();
  }
  for (const key of ["decisions", "tasks", "blockers", "acceptance_checks", "evidence", "attachments", "agent_runs", "agreement_log"]) {
    state.planState[key] = Array.isArray(state.planState[key]) ? state.planState[key] : [];
  }
  if (typeof state.planState.revision !== "number") {
    state.planState.revision = Number(state.planState.revision || 0);
  }
  state.planState.updated_at = state.planState.updated_at || "";
  state.planState.updated_by = state.planState.updated_by || "";
  return state.planState;
}

function applyExecutionPayload(payload) {
  if (payload.plan_state) {
    state.planState = clonePlanStateData(payload.plan_state);
  }
  if (payload.source_of_truth) {
    state.sourceOfTruth = payload.source_of_truth;
  }
  if (payload.agent_contracts?.length) {
    state.sourceOfTruth = state.sourceOfTruth || {};
    state.sourceOfTruth.agent_contracts = payload.agent_contracts;
  }
  state.executionDirty = false;
  updateToolbarState();
}

function markExecutionDirty() {
  state.executionDirty = true;
  updateToolbarState();
}

function formatListValue(value) {
  return Array.isArray(value) ? value.join(", ") : "";
}

function nextExecutionEntityId(collection, prefix) {
  const items = ensureEditablePlanState()[collection] || [];
  const ids = new Set(items.map((item) => item.id));
  let index = items.length + 1;
  let candidate = `${prefix}.${index}`;
  while (ids.has(candidate)) {
    index += 1;
    candidate = `${prefix}.${index}`;
  }
  return candidate;
}

function createExecutionEntity(collection) {
  switch (collection) {
    case "decisions":
      return {
        id: nextExecutionEntityId("decisions", "decision.custom"),
        title: "New decision",
        kind: "planning",
        status: "proposed",
        linked_refs: [],
        acceptance_check_ids: [],
        evidence_ids: [],
        supersedes_decision_id: "",
        locked: false,
        summary: "",
        created_at: "",
        updated_at: "",
        updated_by: "",
      };
    case "tasks":
      return {
        id: nextExecutionEntityId("tasks", "task.custom"),
        title: "New execution task",
        role: "builder",
        status: "todo",
        exploratory: false,
        exploration_goal: "",
        decision_ids: [],
        linked_refs: [],
        depends_on: [],
        acceptance_check_ids: [],
        evidence_ids: [],
        blocker_ids: [],
        summary: "",
        created_at: "",
        updated_at: "",
        completed_at: "",
      };
    case "blockers":
      return {
        id: nextExecutionEntityId("blockers", "blocker.custom"),
        type: "dependency",
        status: "open",
        task_ids: [],
        decision_ids: [],
        linked_refs: [],
        summary: "",
        suggested_resolution: "",
        owner: "",
        created_at: "",
        updated_at: "",
        resolved_at: "",
      };
    case "acceptance_checks":
      return {
        id: nextExecutionEntityId("acceptance_checks", "check.custom"),
        label: "New acceptance check",
        kind: "validation",
        linked_refs: [],
        required: true,
      };
    case "evidence":
      return {
        id: nextExecutionEntityId("evidence", "evidence.custom"),
        check_id: "",
        summary: "",
        attachment_ids: [],
        status: "recorded",
        recorded_by: "",
        recorded_at: "",
      };
    case "attachments":
      return {
        id: nextExecutionEntityId("attachments", "attachment.custom"),
        kind: "note",
        label: "New attachment",
        path_or_url: "",
        linked_decision_ids: [],
        linked_task_ids: [],
      };
    case "agent_runs":
      return {
        id: nextExecutionEntityId("agent_runs", "run.custom"),
        role: "builder",
        status: "planned",
        status_reason: "",
        next_action_hint: "",
        blocked_by: [],
        task_ids: [],
        objective: "",
        bundle_id: "",
        branch: "",
        started_at: "",
        updated_at: "",
        last_summary: "",
        outputs: [],
        events: [],
      };
    default:
      return null;
  }
}

function getExecutionData() {
  const planState = ensureEditablePlanState();
  const counts = {
    decisions: (planState.decisions || []).length,
    tasks: (planState.tasks || []).length,
    open_tasks: (planState.tasks || []).filter((task) => !["done", "cancelled"].includes(task.status)).length,
    blocked_tasks: (planState.tasks || []).filter((task) => task.status === "blocked").length,
    blockers: (planState.blockers || []).filter((blocker) => blocker.status !== "resolved").length,
    agent_runs: (planState.agent_runs || []).length,
    resumable_runs: (planState.agent_runs || []).filter((run) => ["waiting", "blocked", "failed", "running"].includes(run.status)).length,
  };
  return {
    revision: planState.revision || 0,
    updated_at: planState.updated_at || "",
    updated_by: planState.updated_by || "",
    counts,
    top_open_tasks: state.sourceOfTruth?.top_open_tasks || [],
    top_blocker: state.sourceOfTruth?.top_blocker || null,
    highest_risk_decision: state.sourceOfTruth?.highest_risk_decision || null,
    critical_path: state.sourceOfTruth?.critical_path || [],
    blocked_work: state.sourceOfTruth?.blocked_work || [],
    recently_completed: state.sourceOfTruth?.recently_completed || [],
    role_lanes: state.sourceOfTruth?.role_lanes || state.sourceOfTruth?.plan_state?.role_lanes || [],
    handoff_queue: state.sourceOfTruth?.handoff_queue || [],
    open_tasks_by_role: state.sourceOfTruth?.plan_state?.open_tasks_by_role || [],
    missing_proof_decisions: state.sourceOfTruth?.plan_state?.missing_proof_decisions || [],
    resumable_runs: state.sourceOfTruth?.plan_state?.resumable_runs || [],
    recent_agent_activity: state.sourceOfTruth?.recent_agent_activity || [],
    agent_contracts: state.sourceOfTruth?.agent_contracts || [],
  };
}

function buildLocalExecutionIssues(planState) {
  const issues = [];
  const taskIds = new Set((planState.tasks || []).map((task) => task.id));
  const blockerIds = new Set((planState.blockers || []).map((blocker) => blocker.id));
  const decisionIds = new Set((planState.decisions || []).map((decision) => decision.id));
  for (const task of planState.tasks || []) {
    if (!task.exploratory && !(task.decision_ids || []).length) {
      issues.push({ severity: "error", message: `${task.title || task.id} is missing a linked decision.` });
    }
    if (task.exploratory && !String(task.exploration_goal || "").trim()) {
      issues.push({ severity: "warning", message: `${task.title || task.id} is exploratory but has no exploration goal.` });
    }
    if (task.status === "blocked" && !(task.blocker_ids || []).length) {
      issues.push({ severity: "error", message: `${task.title || task.id} is blocked but has no structured blocker.` });
    }
    if ((task.linked_refs || []).length > 4 || (task.acceptance_check_ids || []).length > 3) {
      issues.push({ severity: "warning", message: `${task.title || task.id} may be too large for one focused session.` });
    }
    for (const dependencyId of task.depends_on || []) {
      if (!taskIds.has(dependencyId)) {
        issues.push({ severity: "warning", message: `${task.title || task.id} depends on missing task ${dependencyId}.` });
      }
    }
  }
  for (const decision of planState.decisions || []) {
    if (["accepted", "in_progress", "validated"].includes(decision.status)) {
      const linkedTasks = (planState.tasks || []).filter((task) => (task.decision_ids || []).includes(decision.id) && task.status !== "cancelled");
      if (!linkedTasks.length) {
        issues.push({ severity: "error", message: `${decision.title || decision.id} needs at least one linked active task.` });
      }
    }
  }
  for (const blocker of planState.blockers || []) {
    if (!(blocker.task_ids || []).length && !(blocker.decision_ids || []).length) {
      issues.push({ severity: "error", message: `${blocker.id} is orphaned and needs a linked task or decision.` });
    }
    if (!(blocker.linked_refs || []).length) {
      issues.push({ severity: "warning", message: `${blocker.id} is missing linked refs for easier triage.` });
    }
  }
  for (const proof of planState.evidence || []) {
    if (!String(proof.check_id || "").trim()) {
      issues.push({ severity: "error", message: `${proof.id} is missing the acceptance check it proves.` });
    }
    if (!String(proof.summary || "").trim()) {
      issues.push({ severity: "error", message: `${proof.id} needs a concise proof summary.` });
    }
  }
  for (const run of planState.agent_runs || []) {
    if (["waiting", "blocked", "failed"].includes(run.status)) {
      if (!String(run.status_reason || "").trim()) {
        issues.push({ severity: "error", message: `${run.id} needs a status reason before handoff.` });
      }
      if (!String(run.next_action_hint || "").trim()) {
        issues.push({ severity: "error", message: `${run.id} needs a next action hint before handoff.` });
      }
    }
    for (const blockerId of run.blocked_by || []) {
      if (!blockerIds.has(blockerId)) {
        issues.push({ severity: "warning", message: `${run.id} references missing blocker ${blockerId}.` });
      }
    }
  }
  for (const attachment of planState.attachments || []) {
    for (const decisionId of attachment.linked_decision_ids || []) {
      if (!decisionIds.has(decisionId)) {
        issues.push({ severity: "warning", message: `${attachment.id} references missing decision ${decisionId}.` });
      }
    }
    for (const taskId of attachment.linked_task_ids || []) {
      if (!taskIds.has(taskId)) {
        issues.push({ severity: "warning", message: `${attachment.id} references missing task ${taskId}.` });
      }
    }
  }
  return issues;
}

async function maybePersistExecutionQuickEdit(hadDirty, successTitle, successDetail, draftDetail) {
  if (hadDirty) {
    setStatus(successTitle, draftDetail);
    return false;
  }
  const saved = await saveExecutionState({ silent: true });
  if (saved) {
    setStatus(successTitle, successDetail);
  }
  return saved;
}

function inferExecutionRoleFromRefs(linkedRefs, fallback = "builder") {
  for (const ref of linkedRefs || []) {
    if (String(ref).startsWith("source:")) {
      return "scout";
    }
    if (String(ref).startsWith("contract:") || String(ref).startsWith("data:") || String(ref).startsWith("compute:")) {
      return "builder";
    }
  }
  return fallback || "builder";
}

function createTaskFromDecision(decisionId) {
  const planState = ensureEditablePlanState();
  const decision = getExecutionEntity("decisions", decisionId);
  if (!decision) {
    return;
  }
  const hadDirty = state.executionDirty;
  const task = createExecutionEntity("tasks");
  const seed = String(decision.id || "decision").replace(/[^a-zA-Z0-9_.-]/g, "_");
  task.id = nextExecutionEntityId("tasks", `task.${seed}`);
  task.title = decision.title || "New execution task";
  task.role = inferExecutionRoleFromRefs(decision.linked_refs || [], decision.kind === "planning" ? "architect" : "builder");
  task.decision_ids = [decision.id];
  task.linked_refs = [...(decision.linked_refs || [])];
  task.acceptance_check_ids = [...(decision.acceptance_check_ids || [])];
  task.summary = decision.summary ? `Advance: ${decision.summary}` : `Focused execution task for ${decision.title || decision.id}.`;
  planState.tasks.push(task);
  if (decision.status === "proposed") {
    decision.status = "accepted";
  }
  markExecutionDirty();
  render();
  void maybePersistExecutionQuickEdit(
    hadDirty,
    "Execution task created",
    `${task.title || task.id} is saved and linked to ${decision.title || decision.id}.`,
    `${task.title || task.id} was added to the local execution draft. Save when ready.`
  );
}

function createBlockerFromTask(taskId, options = {}) {
  const planState = ensureEditablePlanState();
  const task = getExecutionEntity("tasks", taskId);
  if (!task) {
    return null;
  }
  const hadDirty = state.executionDirty;
  const blocker = createExecutionEntity("blockers");
  const seed = String(task.id || "task").replace(/[^a-zA-Z0-9_.-]/g, "_");
  blocker.id = nextExecutionEntityId("blockers", `blocker.${seed}`);
  blocker.type = "dependency";
  blocker.status = "open";
  blocker.task_ids = [task.id];
  blocker.decision_ids = [...(task.decision_ids || [])];
  blocker.linked_refs = [...(task.linked_refs || [])];
  blocker.summary = options.summary || `Unblock ${task.title || task.id}`;
  blocker.suggested_resolution = options.suggestedResolution || `Resolve the dependency holding ${task.title || task.id}.`;
  blocker.owner = task.role || "builder";
  planState.blockers.push(blocker);
  task.blocker_ids = [...new Set([...(task.blocker_ids || []), blocker.id])];
  task.status = "blocked";
  task.completed_at = "";
  markExecutionDirty();
  render();
  if (!options.silent) {
    void maybePersistExecutionQuickEdit(
      hadDirty,
      "Blocker created",
      `${blocker.summary} is now linked to ${task.title || task.id}.`,
      `${blocker.summary} was added to the local execution draft. Save when ready.`
    );
  }
  return blocker;
}

function updateTaskQuickStatus(taskId, nextStatus) {
  const task = getExecutionEntity("tasks", taskId);
  if (!task) {
    return;
  }
  const hadDirty = state.executionDirty;
  if (nextStatus === "blocked" && !(task.blocker_ids || []).length) {
    createBlockerFromTask(taskId, { silent: true });
  }
  task.status = nextStatus;
  if (nextStatus === "done") {
    task.completed_at = task.completed_at || new Date().toISOString();
    for (const blockerId of task.blocker_ids || []) {
      const blocker = getExecutionEntity("blockers", blockerId);
      if (blocker) {
        blocker.status = "resolved";
        blocker.resolved_at = blocker.resolved_at || new Date().toISOString();
      }
    }
    task.blocker_ids = [];
  } else {
    task.completed_at = "";
  }
  markExecutionDirty();
  render();
  void maybePersistExecutionQuickEdit(
    hadDirty,
    "Task status updated",
    `${task.title || task.id} is now ${nextStatus}.`,
    `${task.title || task.id} is now ${nextStatus} in the local execution draft. Save when ready.`
  );
}

function updateBlockerQuickStatus(blockerId, nextStatus) {
  const blocker = getExecutionEntity("blockers", blockerId);
  if (!blocker) {
    return;
  }
  const hadDirty = state.executionDirty;
  blocker.status = nextStatus;
  blocker.updated_at = new Date().toISOString();
  blocker.resolved_at = nextStatus === "resolved" ? (blocker.resolved_at || blocker.updated_at) : "";
  if (nextStatus === "resolved") {
    for (const taskId of blocker.task_ids || []) {
      const task = getExecutionEntity("tasks", taskId);
      if (!task) {
        continue;
      }
      task.blocker_ids = (task.blocker_ids || []).filter((entry) => entry !== blocker.id);
      if (task.status === "blocked" && !(task.blocker_ids || []).length) {
        task.status = "todo";
      }
    }
  }
  markExecutionDirty();
  render();
  void maybePersistExecutionQuickEdit(
    hadDirty,
    "Blocker updated",
    `${blocker.summary || blocker.id} is now ${nextStatus}.`,
    `${blocker.summary || blocker.id} is now ${nextStatus} in the local execution draft. Save when ready.`
  );
}

function updateEvidenceQuickStatus(evidenceId, nextStatus) {
  const proof = getExecutionEntity("evidence", evidenceId);
  if (!proof) {
    return;
  }
  const hadDirty = state.executionDirty;
  proof.status = nextStatus;
  proof.recorded_at = proof.recorded_at || new Date().toISOString();
  proof.recorded_by = proof.recorded_by || getCurrentStructureReviewerIdentity();
  markExecutionDirty();
  render();
  void maybePersistExecutionQuickEdit(
    hadDirty,
    "Proof updated",
    `${proof.id} is now ${nextStatus}.`,
    `${proof.id} is now ${nextStatus} in the local execution draft. Save when ready.`
  );
}

async function createRunFromTask(taskId) {
  if (state.executionDirty) {
    const saved = await saveExecutionState({ silent: true });
    if (!saved) {
      return;
    }
  }
  const task = getExecutionEntity("tasks", taskId);
  if (!task) {
    setStatus("Agent run failed", `Task ${taskId} is no longer available.`);
    return;
  }
  const current = ensureEditablePlanState();
  const run = createExecutionEntity("agent_runs");
  run.role = task.role || inferExecutionRoleFromRefs(task.linked_refs || []);
  run.status = task.status === "blocked" || (task.blocker_ids || []).length ? "blocked" : "planned";
  run.status_reason = run.status === "blocked" ? "Blocked by the linked execution blockers." : "";
  run.next_action_hint = task.summary || `Advance ${task.title || task.id} from source-of-truth.`;
  run.blocked_by = [...(task.blocker_ids || [])];
  run.task_ids = [task.id];
  run.objective = task.title || task.id;
  run.last_summary = task.summary || "";
  const response = await fetch("/api/agent-runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      agent_run: run,
      expected_revision: current.revision || 0,
      updated_by: getCurrentStructureReviewerIdentity(),
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Agent run failed", payload.detail || payload.error || "Unable to create the agent run.");
    return;
  }
  applyExecutionPayload(payload);
  render();
  setStatus("Agent run created", `${payload.agent_run?.objective || payload.agent_run?.id || "New run"} is now tracked in execution state.`);
}

function getExecutionEntity(collection, id) {
  return (ensureEditablePlanState()[collection] || []).find((item) => item.id === id) || null;
}

function pruneExecutionReferences(planState) {
  planState.blockers = (planState.blockers || []).filter((blocker) => (blocker.task_ids || []).length || (blocker.decision_ids || []).length);
  const activeBlockerIds = new Set((planState.blockers || []).map((blocker) => blocker.id));
  for (const task of planState.tasks || []) {
    task.blocker_ids = (task.blocker_ids || []).filter((blockerId) => activeBlockerIds.has(blockerId));
    if (task.status === "blocked" && !(task.blocker_ids || []).length) {
      task.status = "todo";
    }
  }
  for (const entry of planState.agreement_log || []) {
    if (entry.decision_id && !(planState.decisions || []).some((decision) => decision.id === entry.decision_id)) {
      entry.decision_id = "";
    }
    if (entry.task_id && !(planState.tasks || []).some((task) => task.id === entry.task_id)) {
      entry.task_id = "";
    }
  }
}

function removeExecutionItem(collection, id) {
  const planState = ensureEditablePlanState();
  planState[collection] = (planState[collection] || []).filter((item) => item.id !== id);
  if (collection === "decisions") {
    for (const task of planState.tasks || []) {
      task.decision_ids = (task.decision_ids || []).filter((decisionId) => decisionId !== id);
      if (!(task.decision_ids || []).length) {
        task.exploratory = true;
        task.exploration_goal = task.exploration_goal || "Reconnect this task to a decision before marking it complete.";
        if (task.status === "done") {
          task.status = "cancelled";
        }
      }
    }
    for (const blocker of planState.blockers || []) {
      blocker.decision_ids = (blocker.decision_ids || []).filter((decisionId) => decisionId !== id);
    }
    for (const attachment of planState.attachments || []) {
      attachment.linked_decision_ids = (attachment.linked_decision_ids || []).filter((decisionId) => decisionId !== id);
    }
  } else if (collection === "tasks") {
    for (const blocker of planState.blockers || []) {
      blocker.task_ids = (blocker.task_ids || []).filter((taskId) => taskId !== id);
    }
    for (const task of planState.tasks || []) {
      task.depends_on = (task.depends_on || []).filter((dependencyId) => dependencyId !== id);
    }
    for (const attachment of planState.attachments || []) {
      attachment.linked_task_ids = (attachment.linked_task_ids || []).filter((taskId) => taskId !== id);
    }
    for (const run of planState.agent_runs || []) {
      run.task_ids = (run.task_ids || []).filter((taskId) => taskId !== id);
    }
  } else if (collection === "blockers") {
    for (const task of planState.tasks || []) {
      task.blocker_ids = (task.blocker_ids || []).filter((blockerId) => blockerId !== id);
    }
    for (const run of planState.agent_runs || []) {
      run.blocked_by = (run.blocked_by || []).filter((blockerId) => blockerId !== id);
    }
  } else if (collection === "acceptance_checks") {
    planState.evidence = (planState.evidence || []).filter((proof) => proof.check_id !== id);
    const remainingEvidenceIds = new Set((planState.evidence || []).map((proof) => proof.id));
    for (const decision of planState.decisions || []) {
      decision.acceptance_check_ids = (decision.acceptance_check_ids || []).filter((checkId) => checkId !== id);
      decision.evidence_ids = (decision.evidence_ids || []).filter((proofId) => remainingEvidenceIds.has(proofId));
    }
    for (const task of planState.tasks || []) {
      task.acceptance_check_ids = (task.acceptance_check_ids || []).filter((checkId) => checkId !== id);
      task.evidence_ids = (task.evidence_ids || []).filter((proofId) => remainingEvidenceIds.has(proofId));
    }
  } else if (collection === "evidence") {
    for (const decision of planState.decisions || []) {
      decision.evidence_ids = (decision.evidence_ids || []).filter((proofId) => proofId !== id);
    }
    for (const task of planState.tasks || []) {
      task.evidence_ids = (task.evidence_ids || []).filter((proofId) => proofId !== id);
    }
  } else if (collection === "attachments") {
    for (const proof of planState.evidence || []) {
      proof.attachment_ids = (proof.attachment_ids || []).filter((attachmentId) => attachmentId !== id);
    }
  }
  pruneExecutionReferences(planState);
}

async function refreshExecutionState() {
  if (state.executionDirty && !window.confirm("You have unsaved execution edits. Refreshing will discard them. Continue?")) {
    return;
  }
  setStatus("Refreshing execution state...", "Reloading the shared execution source of truth.");
  const response = await fetch("/api/plan-state");
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Execution refresh failed", payload.detail || payload.error || "Unable to refresh execution state.");
    return;
  }
  applyExecutionPayload(payload);
  render();
  setStatus("Execution refreshed", "The latest execution state and priority signals are back in sync.");
}

async function saveExecutionState(options = {}) {
  const planState = ensureEditablePlanState();
  const response = await fetch("/api/plan-state", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      plan_state: planState,
      expected_revision: planState.revision || 0,
      updated_by: getCurrentStructureReviewerIdentity(),
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Execution save failed", payload.detail || payload.error || "Unable to save execution state.");
    return false;
  }
  applyExecutionPayload(payload);
  render();
  if (!options.silent) {
    setStatus("Execution state saved", "Decisions, tasks, blockers, and runs were persisted with revision checks.");
  }
  return true;
}

async function deriveExecutionTasks() {
  if (state.executionDirty) {
    const saved = await saveExecutionState({ silent: true });
    if (!saved) {
      return;
    }
  }
  const current = ensureEditablePlanState();
  const response = await fetch("/api/plan-state/derive-tasks", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      expected_revision: current.revision || 0,
      updated_by: getCurrentStructureReviewerIdentity(),
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Task derivation failed", payload.detail || payload.error || "Unable to derive execution tasks.");
    return;
  }
  applyExecutionPayload(payload);
  render();
  setStatus("Execution tasks derived", "Proposed decisions without active work were converted into focused tasks.");
}

function createExecutionProofFromCheck(checkId) {
  const planState = ensureEditablePlanState();
  const check = (planState.acceptance_checks || []).find((item) => item.id === checkId);
  if (!check) {
    return;
  }
  const proof = createExecutionEntity("evidence");
  proof.id = nextExecutionEntityId("evidence", `evidence.${checkId.replace(/[^a-zA-Z0-9_.-]/g, "_")}`);
  proof.check_id = checkId;
  proof.summary = `Proof draft for ${check.label || check.id}`;
  proof.recorded_by = getCurrentStructureReviewerIdentity();
  planState.evidence.push(proof);
  for (const decision of planState.decisions || []) {
    if ((decision.acceptance_check_ids || []).includes(checkId) && !(decision.evidence_ids || []).includes(proof.id)) {
      decision.evidence_ids = [...(decision.evidence_ids || []), proof.id];
    }
  }
  for (const task of planState.tasks || []) {
    if ((task.acceptance_check_ids || []).includes(checkId) && !(task.evidence_ids || []).includes(proof.id)) {
      task.evidence_ids = [...(task.evidence_ids || []), proof.id];
    }
  }
  markExecutionDirty();
  render();
  setStatus("Proof draft created", `${check.label || check.id} now has a structured evidence draft ready to fill in.`);
}

async function updateExecutionRunQuickState(runId, nextStatus) {
  const run = getExecutionEntity("agent_runs", runId);
  if (!run) {
    return;
  }
  const hadDirty = state.executionDirty;
  const timestamp = new Date().toISOString();
  run.status = nextStatus;
  run.updated_at = timestamp;
  if (nextStatus === "running") {
    run.status_reason = "";
    run.next_action_hint = run.next_action_hint || "Continue the assigned execution task.";
    run.events = [...(run.events || []), { id: `${run.id}.resume.${Date.now()}`, kind: "resume", summary: "Run resumed from the execution panel.", created_at: timestamp, created_by: getCurrentStructureReviewerIdentity() }];
  } else if (nextStatus === "waiting") {
    run.status_reason = run.status_reason || "Waiting for follow-up input or another dependency to clear.";
    run.next_action_hint = run.next_action_hint || "Review blockers and resume when the next input arrives.";
  } else if (nextStatus === "blocked") {
    run.status_reason = run.status_reason || "Blocked pending an explicit blocker resolution.";
    run.next_action_hint = run.next_action_hint || "Check the linked blockers and hand off if needed.";
  } else if (nextStatus === "completed") {
    run.status_reason = "";
    run.next_action_hint = "";
    run.events = [...(run.events || []), { id: `${run.id}.complete.${Date.now()}`, kind: "complete", summary: "Run marked completed from the execution panel.", created_at: timestamp, created_by: getCurrentStructureReviewerIdentity() }];
  }
  markExecutionDirty();
  render();
  await maybePersistExecutionQuickEdit(
    hadDirty,
    "Run updated",
    `${run.objective || run.id} is now ${nextStatus}.`,
    `${run.objective || run.id} is now ${nextStatus} in the local execution draft. Save when ready.`
  );
}

async function handoffExecutionRun(runId) {
  const run = getExecutionEntity("agent_runs", runId);
  if (!run) {
    return;
  }
  const hadDirty = state.executionDirty;
  const timestamp = new Date().toISOString();
  run.status = "waiting";
  run.updated_at = timestamp;
  run.status_reason = run.status_reason || "Paused for handoff.";
  run.next_action_hint = run.next_action_hint || run.last_summary || run.objective || "Pick up the next agreed task from source-of-truth.";
  run.events = [
    ...(run.events || []),
    {
      id: `${run.id}.handoff.${Date.now()}`,
      kind: "handoff",
      summary: `Handoff ready: ${run.next_action_hint}`,
      created_at: timestamp,
      created_by: getCurrentStructureReviewerIdentity(),
    },
  ];
  markExecutionDirty();
  render();
  await maybePersistExecutionQuickEdit(
    hadDirty,
    "Run handed off",
    `${run.objective || run.id} now has a saved resumable handoff trail.`,
    `${run.objective || run.id} now has a resumable handoff trail in the local draft. Save when ready.`
  );
}

function handleExecutionMutation(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  if (target.dataset.executionControl === "search") {
    state.executionSearch = String(target.value || "");
    render();
    return;
  }
  if (target.dataset.executionControl === "role_filter") {
    state.executionRoleFilter = String(target.value || "all");
    render();
    return;
  }
  if (target.dataset.executionControl === "hide_completed") {
    state.executionHideCompleted = target instanceof HTMLInputElement ? target.checked : Boolean(target.value);
    render();
    return;
  }
  const collection = target.dataset.executionCollection;
  const itemId = target.dataset.executionId;
  const field = target.dataset.executionField;
  if (!collection || !itemId || !field) {
    return;
  }
  const item = getExecutionEntity(collection, itemId);
  if (!item) {
    return;
  }
  const arrayFields = new Set([
    "linked_refs",
    "acceptance_check_ids",
    "evidence_ids",
    "decision_ids",
    "depends_on",
    "blocker_ids",
    "task_ids",
    "attachment_ids",
    "linked_decision_ids",
    "linked_task_ids",
    "blocked_by",
    "outputs",
  ]);
  if (arrayFields.has(field)) {
    item[field] = parseReferenceList(target.value);
  } else if (target instanceof HTMLInputElement && target.type === "checkbox") {
    item[field] = target.checked;
    if (collection === "tasks" && field === "exploratory" && !target.checked) {
      item.exploration_goal = "";
    }
  } else {
    item[field] = target.value;
  }
  if (collection === "tasks" && field === "status") {
    item.completed_at = item.status === "done" ? (item.completed_at || new Date().toISOString()) : "";
  }
  markExecutionDirty();
  if (event.type === "change") {
    pruneExecutionReferences(ensureEditablePlanState());
  }
  if ((target instanceof HTMLInputElement && target.type === "checkbox") || target instanceof HTMLSelectElement) {
    render();
  }
}

function handleExecutionClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const actionTarget = target.closest("[data-execution-action]");
  if (!(actionTarget instanceof HTMLElement)) {
    return;
  }
  const action = actionTarget.dataset.executionAction;
  if (action === "refresh") {
    void refreshExecutionState();
    return;
  }
  if (action === "save") {
    void saveExecutionState();
    return;
  }
  if (action === "derive-tasks") {
    void deriveExecutionTasks();
    return;
  }
  if (action === "set-view") {
    state.executionView = actionTarget.dataset.executionView || "all";
    render();
    return;
  }
  if (action === "focus-search") {
    focusExecutionSearch(actionTarget.dataset.executionSearch || "", {
      view: actionTarget.dataset.executionView || "all",
      hideCompleted: actionTarget.dataset.executionHideCompleted === "true",
    });
    return;
  }
  if (action === "create-proof") {
    createExecutionProofFromCheck(actionTarget.dataset.executionCheckId || "");
    return;
  }
  if (action === "decision-create-task") {
    createTaskFromDecision(actionTarget.dataset.executionDecisionId || "");
    return;
  }
  if (action === "task-quick-status") {
    updateTaskQuickStatus(
      actionTarget.dataset.executionTaskId || "",
      actionTarget.dataset.executionTaskStatus || "todo",
    );
    return;
  }
  if (action === "task-create-blocker") {
    createBlockerFromTask(actionTarget.dataset.executionTaskId || "");
    return;
  }
  if (action === "task-create-run") {
    void createRunFromTask(actionTarget.dataset.executionTaskId || "");
    return;
  }
  if (action === "blocker-quick-status") {
    updateBlockerQuickStatus(
      actionTarget.dataset.executionBlockerId || "",
      actionTarget.dataset.executionBlockerStatus || "open",
    );
    return;
  }
  if (action === "evidence-quick-status") {
    updateEvidenceQuickStatus(
      actionTarget.dataset.executionEvidenceId || "",
      actionTarget.dataset.executionEvidenceStatus || "recorded",
    );
    return;
  }
  if (action === "load-brief") {
    void loadExecutionBrief(actionTarget.dataset.executionContractId || "", {
      taskId: actionTarget.dataset.executionTaskId || "",
      runId: actionTarget.dataset.executionRunId || "",
    });
    return;
  }
  if (action === "load-workflow") {
    void loadExecutionWorkflow(actionTarget.dataset.executionContractId || "", {
      taskId: actionTarget.dataset.executionTaskId || "",
      runId: actionTarget.dataset.executionRunId || "",
    });
    return;
  }
  if (action === "launch-workflow") {
    void launchExecutionWorkflow(actionTarget.dataset.executionContractId || "", {
      taskId: actionTarget.dataset.executionTaskId || "",
      runId: actionTarget.dataset.executionRunId || "",
    });
    return;
  }
  if (action === "copy-brief") {
    void copyExecutionBriefPrompt();
    return;
  }
  if (action === "clear-brief") {
    clearExecutionBrief();
    return;
  }
  if (action === "clear-workflow") {
    clearExecutionWorkflow({ clearBrief: false });
    return;
  }
  if (action === "focus-role") {
    state.executionView = "all";
    state.executionRoleFilter = actionTarget.dataset.executionRole || "all";
    render();
    return;
  }
  if (action === "run-quick-status") {
    void updateExecutionRunQuickState(
      actionTarget.dataset.executionRunId || "",
      actionTarget.dataset.executionRunStatus || "planned",
    );
    return;
  }
  if (action === "run-handoff") {
    void handoffExecutionRun(actionTarget.dataset.executionRunId || "");
    return;
  }
  if (action === "add-item") {
    const collection = actionTarget.dataset.executionCollection || "";
    const item = createExecutionEntity(collection);
    if (!item) {
      return;
    }
    ensureEditablePlanState()[collection].push(item);
    markExecutionDirty();
    render();
    return;
  }
  if (action === "remove-item") {
    const collection = actionTarget.dataset.executionCollection || "";
    const itemId = actionTarget.dataset.executionId || "";
    if (!collection || !itemId) {
      return;
    }
    removeExecutionItem(collection, itemId);
    markExecutionDirty();
    render();
  }
}

function focusExecutionSearch(search, options = {}) {
  state.executionView = options.view || "all";
  state.executionSearch = String(search || "");
  state.executionRoleFilter = options.role || "all";
  state.executionHideCompleted = Boolean(options.hideCompleted);
  render();
  executionSummary?.scrollIntoView?.({ behavior: "smooth", block: "start" });
  setStatus(
    "Execution focus updated",
    state.executionSearch
      ? `Showing execution state related to ${state.executionSearch}.`
      : "Showing the full execution state."
  );
}

function focusExecutionForNode(nodeId, options = {}) {
  if (!nodeId) {
    return;
  }
  focusExecutionSearch(nodeId, options);
}
