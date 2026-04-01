// Extracted structure review rendering and bundle overlay helpers from app.js.

function structureBundleHasHighImpact(bundle) {
  return Boolean(
    (bundle.high_severity_contradiction_count || 0)
    || (bundle.downstream_breakage_count || 0)
    || (bundle.binding_mismatch_count || 0)
  );
}

function structureBundleNeedsAttention(bundle, currentVersion) {
  const stats = getStructureBundleReviewStats(bundle, currentVersion);
  return stats.stale || stats.pendingCount > 0 || stats.reviewRequiredCount > 0;
}

function getCurrentStructureReviewerIdentity() {
  return (state.structureReviewerIdentity || "user").trim() || "user";
}

function bundleMatchesAssignmentFilter(bundle, filterKey) {
  const assignedReviewer = String(bundle.assigned_reviewer || "").trim();
  const bundleOwner = String(bundle.bundle_owner || "").trim();
  const currentReviewer = getCurrentStructureReviewerIdentity();
  if (filterKey === "mine") {
    return assignedReviewer === currentReviewer || bundleOwner === currentReviewer;
  }
  if (filterKey === "unassigned") {
    return !assignedReviewer;
  }
  if (filterKey === "blocked") {
    return bundle.triage_state === "blocked";
  }
  return true;
}

function bundleMatchesInboxFilter(bundle, filter, currentVersion) {
  const stats = getStructureBundleReviewStats(bundle, currentVersion);
  if (filter === "all") {
    return true;
  }
  if (filter === "needs_attention") {
    return structureBundleNeedsAttention(bundle, currentVersion);
  }
  if (filter === "contradictions") {
    return stats.contradictionCount > 0;
  }
  if (filter === "high_impact") {
    return structureBundleHasHighImpact(bundle);
  }
  if (filter === "stale") {
    return stats.stale;
  }
  if (filter === "ready") {
    return Boolean(bundle.ready_to_merge || (stats.acceptedCount > 0 && stats.pendingCount === 0 && !stats.stale && stats.reviewRequiredCount === 0));
  }
  if (filter === "deferred") {
    return stats.deferredCount > 0;
  }
  return true;
}

function buildStructureAssignmentFilterCounts(bundles) {
  const counts = {
    all: bundles.length,
    mine: 0,
    unassigned: 0,
    blocked: 0,
  };
  bundles.forEach((bundle) => {
    if (bundleMatchesAssignmentFilter(bundle, "mine")) counts.mine += 1;
    if (bundleMatchesAssignmentFilter(bundle, "unassigned")) counts.unassigned += 1;
    if (bundleMatchesAssignmentFilter(bundle, "blocked")) counts.blocked += 1;
  });
  return counts;
}

function renderStructureAssignmentFilters(bundles) {
  const counts = buildStructureAssignmentFilterCounts(bundles);
  const items = [
    { key: "all", label: "All assignments" },
    { key: "mine", label: "Mine" },
    { key: "unassigned", label: "Unassigned" },
    { key: "blocked", label: "Blocked" },
  ];
  return `
    <div class="structure-review-filter-bar">
      ${items.map((item) => `
        <button
          class="ghost-button structure-queue-chip ${state.structureAssignmentFilter === item.key ? "active" : ""}"
          type="button"
          data-structure-action="set-assignment-filter"
          data-assignment-filter="${escapeHtml(item.key)}"
        >
          <span>${escapeHtml(item.label)}</span>
          <span class="status-pill">${formatValue(counts[item.key] || 0)}</span>
        </button>
      `).join("")}
    </div>
  `;
}

function renderStructureReviewPresetControls() {
  const presets = getStructureReviewPresetOptions();
  return `
    <div class="structure-review-toolbar">
      <div class="form-grid compact">
        <label class="form-field">
          Acting reviewer
          <input data-structure-pref="reviewer_identity" value="${escapeHtml(getCurrentStructureReviewerIdentity())}" />
        </label>
        <label class="form-field">
          Saved preset
          <select data-structure-pref="selected_preset_id">
            <option value="">No preset selected</option>
            ${presets.map((preset) => `<option value="${escapeHtml(preset.id)}" ${state.selectedStructureReviewPresetId === preset.id ? "selected" : ""}>${escapeHtml(preset.name)}</option>`).join("")}
          </select>
        </label>
        <label class="form-field">
          Save current as
          <input data-structure-pref="preset_draft_name" value="${escapeHtml(state.structureReviewPresetDraftName || "")}" placeholder="Team handoff queue" />
        </label>
      </div>
      <div class="structure-review-toggles">
        <button class="ghost-button" type="button" data-structure-action="apply-review-preset" ${state.selectedStructureReviewPresetId ? "" : "disabled"}>Apply preset</button>
        <button class="ghost-button" type="button" data-structure-action="save-review-preset">Save current preset</button>
        <button class="ghost-button danger-soft" type="button" data-structure-action="delete-review-preset" ${state.selectedStructureReviewPresetId && !String(state.selectedStructureReviewPresetId).startsWith("builtin:") ? "" : "disabled"}>Delete preset</button>
      </div>
    </div>
  `;
}

function renderStructureReviewAnalytics(bundles) {
  const currentReviewer = getCurrentStructureReviewerIdentity();
  const pendingReviewCount = bundles.reduce((total, bundle) => total + Number(bundle.pending_count || 0), 0);
  const unresolvedContradictions = bundles.reduce((total, bundle) => total + Number(bundle.review_required_count || 0), 0);
  const deferredCount = bundles.reduce((total, bundle) => total + Number(bundle.deferred_count || 0), 0);
  const mineCount = bundles.filter((bundle) => bundleMatchesAssignmentFilter(bundle, "mine")).length;
  const blockedCount = bundles.filter((bundle) => bundle.triage_state === "blocked").length;
  const hottestBundle = bundles.reduce((current, bundle) => (
    !current || Number(bundle.contradiction_count || 0) > Number(current.contradiction_count || 0) ? bundle : current
  ), null);
  const items = [
    { label: `${currentReviewer} queue`, value: mineCount, detail: "assigned reviewer or owner matches current reviewer" },
    { label: "Review debt", value: pendingReviewCount + unresolvedContradictions, detail: `${pendingReviewCount} pending patches + ${unresolvedContradictions} unresolved contradictions` },
    { label: "Blocked bundles", value: blockedCount, detail: "triage state marked blocked" },
    { label: "Deferred work", value: deferredCount, detail: "deferred patches still waiting on follow-up" },
    { label: "Hot contradiction bundle", value: hottestBundle ? hottestBundle.bundle_id : "none", detail: hottestBundle ? `${formatValue(hottestBundle.contradiction_count || 0)} contradictions` : "no contradiction hotspots" },
  ];
  return `
    <div class="structure-bundle-summary-grid">
      ${items.map((item) => `
        <div class="meta-card">
          <strong>${escapeHtml(item.label)}</strong>
          <div>${escapeHtml(formatValue(item.value))}</div>
          <div class="hint">${escapeHtml(item.detail)}</div>
        </div>
      `).join("")}
    </div>
  `;
}

function buildStructureBundleInboxCounts(bundles, currentVersion) {
  const counts = {
    needs_attention: 0,
    contradictions: 0,
    high_impact: 0,
    stale: 0,
    ready: 0,
    deferred: 0,
    all: bundles.length,
  };
  bundles.forEach((bundle) => {
    if (bundleMatchesInboxFilter(bundle, "needs_attention", currentVersion)) counts.needs_attention += 1;
    if (bundleMatchesInboxFilter(bundle, "contradictions", currentVersion)) counts.contradictions += 1;
    if (bundleMatchesInboxFilter(bundle, "high_impact", currentVersion)) counts.high_impact += 1;
    if (bundleMatchesInboxFilter(bundle, "stale", currentVersion)) counts.stale += 1;
    if (bundleMatchesInboxFilter(bundle, "ready", currentVersion)) counts.ready += 1;
    if (bundleMatchesInboxFilter(bundle, "deferred", currentVersion)) counts.deferred += 1;
  });
  return counts;
}

function structureBundlePriority(bundle, currentVersion) {
  const stats = getStructureBundleReviewStats(bundle, currentVersion);
  if (stats.stale) {
    return 0;
  }
  if (bundle.triage_state === "blocked") {
    return 1;
  }
  if (stats.reviewRequiredCount) {
    return 2;
  }
  if (structureBundleHasHighImpact(bundle)) {
    return 3;
  }
  if (stats.pendingCount) {
    return 4;
  }
  if (stats.deferredCount) {
    return 5;
  }
  if (bundle.ready_to_merge || (stats.acceptedCount > 0 && stats.pendingCount === 0 && stats.reviewRequiredCount === 0)) {
    return 6;
  }
  return 7;
}

function renderStructureBundleInboxQueue(bundles, currentVersion) {
  const counts = buildStructureBundleInboxCounts(bundles, currentVersion);
  const items = [
    { key: "needs_attention", label: "Review required" },
    { key: "contradictions", label: "Contradictions" },
    { key: "high_impact", label: "High impact" },
    { key: "stale", label: "Stale" },
    { key: "ready", label: "Ready to merge" },
    { key: "deferred", label: "Deferred" },
    { key: "all", label: "All bundles" },
  ];
  return `
    <div class="structure-queue-bar">
      ${items.map((item) => `
        <button
          class="ghost-button structure-queue-chip ${state.structureBundleInboxFilter === item.key ? "active" : ""}"
          type="button"
          data-structure-action="set-bundle-inbox-filter"
          data-bundle-inbox-filter="${escapeHtml(item.key)}"
        >
          <span>${escapeHtml(item.label)}</span>
          <span class="status-pill">${formatValue(counts[item.key] || 0)}</span>
        </button>
      `).join("")}
    </div>
  `;
}

function renderStructureMemory() {
  if (!structureSummary) {
    return;
  }
  const structure = state.structure;
  if (!structure) {
    structureSummary.innerHTML = "<p>No structure summary yet.</p>";
    return;
  }

  const readiness = structure.readiness || {};
  const readinessSummary = readiness.summary || {};
  const { currentVersion, bundles, filteredBundles } = getStructureBundleLists(structure);
  const bundleCounts = buildStructureBundleInboxCounts(bundles, currentVersion);
  const previewBundles = filteredBundles.slice(0, 3);

  structureSummary.innerHTML = `
    <div class="compact-summary-stack">
      <div class="meta-grid compact-summary">
        <div><strong>Canonical</strong><br>${escapeHtml(structure.canonical_spec_path || "specs/structure/spec.yaml")}</div>
        <div><strong>Version</strong><br>${formatValue(structure.structure_version || 1)}</div>
        <div><strong>Updated by</strong><br>${escapeHtml(structure.updated_by || "user")}</div>
        <div><strong>Readiness</strong><br><span class="status-pill ${escapeHtml(readinessClass(readiness.status))}">${escapeHtml(readiness.status || "Not Ready")}</span></div>
      </div>
      <div class="chip-row">
        <span class="status-pill">${escapeHtml(`Tier 1: ${formatValue(readinessSummary.tier_1_issues || 0)}`)}</span>
        <span class="status-pill">${escapeHtml(`Warnings: ${formatValue(readinessSummary.warnings || 0)}`)}</span>
        <span class="status-pill">${escapeHtml(`Required bindings: ${formatValue(readinessSummary.required_binding_failures || 0)}`)}</span>
      </div>
      <div class="section-actions">
        <button class="ghost-button" type="button" data-structure-action="open-review-inbox">Open review inbox</button>
        <button class="ghost-button" type="button" data-structure-action="export-yaml">Export YAML</button>
        <button class="ghost-button" type="button" data-structure-action="scan-quick" data-structure-role="scout">Scout scan</button>
        <button class="ghost-button" type="button" data-structure-action="scan-quick" data-structure-role="recorder">Recorder scan</button>
      </div>
      <div class="meta-grid compact-summary">
        <div><strong>Needs attention</strong><br>${formatValue(bundleCounts.needs_attention || 0)}</div>
        <div><strong>Contradictions</strong><br>${formatValue(bundleCounts.contradictions || 0)}</div>
        <div><strong>Ready to merge</strong><br>${formatValue(bundleCounts.ready || 0)}</div>
        <div><strong>All bundles</strong><br>${formatValue(bundles.length)}</div>
      </div>
      ${(readiness.issues || []).length ? `
        <div class="section">
          <div class="section-actions">
            <h3>Top readiness issues</h3>
            <button class="ghost-button" type="button" data-structure-action="open-review-inbox">Review structure</button>
          </div>
          <ul class="warning-list">
            ${(readiness.issues || []).slice(0, 3).map((issue) => `
              <li>
                <div>${escapeHtml(issue.message || "")}</div>
                ${issue.why_this_matters ? `<div class="hint">${escapeHtml(issue.why_this_matters)}</div>` : ""}
              </li>
            `).join("")}
          </ul>
        </div>
      ` : '<div class="success-chip">✓ No issues</div>'}
      <div class="section structure-inline-preview">
        <div class="section-actions">
          <h3>Bundle preview</h3>
          <span class="hint">${formatValue(filteredBundles.length)} in current queue</span>
        </div>
        ${renderStructureBundleInboxQueue(bundles, currentVersion)}
        <div class="column-list">
          ${previewBundles.length
            ? previewBundles.map((bundle) => renderStructureBundleListItem(bundle, { currentVersion, selectedBundleId: state.selectedStructureBundleId })).join("")
            : "<p>No review bundles yet. Run a scout or recorder scan to inspect repo or docs into proposed structure patches.</p>"}
        </div>
      </div>
    </div>
  `;
}

function getStructureBundleLists(structure) {
  const currentVersion = structure?.structure_version || 1;
  const bundles = structure?.bundles || [];
  const filteredBundles = bundles
    .filter((bundle) => bundleMatchesInboxFilter(bundle, state.structureBundleInboxFilter || "needs_attention", currentVersion))
    .filter((bundle) => bundleMatchesAssignmentFilter(bundle, state.structureAssignmentFilter || "all"))
    .sort((left, right) => {
      const leftPriority = structureBundlePriority(left, currentVersion);
      const rightPriority = structureBundlePriority(right, currentVersion);
      return leftPriority - rightPriority
        || String(right.created_at || "").localeCompare(String(left.created_at || ""))
        || String(left.bundle_id || "").localeCompare(String(right.bundle_id || ""));
    });
  return { currentVersion, bundles, filteredBundles };
}

async function toggleReviewDrawer(open) {
  state.reviewDrawerOpen = open;
  if (open && state.structure) {
    const { bundles, filteredBundles } = getStructureBundleLists(state.structure);
    const preferredBundle = filteredBundles[0] || bundles[0] || null;
    if (preferredBundle && (!state.selectedStructureBundle || state.selectedStructureBundleId !== preferredBundle.bundle_id)) {
      await loadStructureBundle(preferredBundle.bundle_id, { silent: true });
    }
  }
  render();
}

function renderStructureReviewDrawer() {
  if (!reviewDrawerContent) {
    return;
  }
  const structure = state.structure;
  if (!structure) {
    reviewDrawerContent.innerHTML = "<p>No structure memory loaded yet.</p>";
    return;
  }
  const { currentVersion, bundles, filteredBundles } = getStructureBundleLists(structure);
  const readiness = structure.readiness || {};
  const readinessSummary = readiness.summary || {};
  const bundleCounts = buildStructureBundleInboxCounts(bundles, currentVersion);
  const selectedBundle = state.selectedStructureBundle;
  const execution = getExecutionData();
  reviewDrawerContent.innerHTML = `
    <div class="review-drawer-shell">
      <div class="structure-review-banner ${escapeHtml(readinessClass(readiness.status))}">
        ${escapeHtml(
          readiness.status === "Ready to Build"
            ? "Canonical YAML is structurally ready. Focus on reviewing new observed changes before merging."
            : `Canonical YAML is ${readiness.status || "Not Ready"}. Review structure changes and readiness blockers together from here.`
        )}
      </div>
      <div class="structure-bundle-summary-grid">
        <div class="meta-card"><strong>Canonical version</strong>${formatValue(structure.structure_version || 1)}</div>
        <div class="meta-card"><strong>Needs attention</strong>${formatValue(bundleCounts.needs_attention || 0)}</div>
        <div class="meta-card"><strong>Contradictions</strong>${formatValue(bundleCounts.contradictions || 0)}</div>
        <div class="meta-card"><strong>Ready to merge</strong>${formatValue(bundleCounts.ready || 0)}</div>
        <div class="meta-card"><strong>Tier 1</strong>${formatValue(readinessSummary.tier_1_issues || 0)}</div>
        <div class="meta-card"><strong>Required binding failures</strong>${formatValue(readinessSummary.required_binding_failures || 0)}</div>
      </div>
      <div class="section">
        <div class="review-drawer-section-head">
          <h3>Execution state</h3>
          <span class="hint">Shared source of truth for humans and agents</span>
        </div>
        <div class="structure-bundle-summary-grid">
          <div class="meta-card"><strong>Open tasks</strong>${formatValue(execution.counts?.open_tasks || 0)}</div>
          <div class="meta-card"><strong>Blocked tasks</strong>${formatValue(execution.counts?.blocked_tasks || 0)}</div>
          <div class="meta-card"><strong>Blockers</strong>${formatValue(execution.counts?.blockers || 0)}</div>
          <div class="meta-card"><strong>Resumable runs</strong>${formatValue(execution.counts?.resumable_runs || 0)}</div>
        </div>
        ${(execution.top_blocker || execution.highest_risk_decision)
          ? `<div class="meta-grid compact-summary">
              <div><strong>Top blocker</strong><br>${escapeHtml(execution.top_blocker?.summary || "None")}</div>
              <div><strong>Highest-risk decision</strong><br>${escapeHtml(execution.highest_risk_decision?.title || "None")}</div>
            </div>`
          : '<div class="success-chip">✓ No execution hotspots surfaced yet</div>'}
      </div>
      <div class="section">
        <div class="review-drawer-section-head">
          <h3>Bundle inbox</h3>
          <div class="row-actions">
            <button class="ghost-button" type="button" data-structure-action="scan-quick" data-structure-role="scout">Scout scan</button>
            <button class="ghost-button" type="button" data-structure-action="scan-quick" data-structure-role="recorder">Recorder scan</button>
            <button class="ghost-button" type="button" data-structure-action="refresh-bundles">Refresh bundles</button>
          </div>
        </div>
        ${renderStructureBundleInboxQueue(bundles, currentVersion)}
        ${renderStructureAssignmentFilters(bundles)}
        ${renderStructureReviewPresetControls()}
        ${renderStructureReviewAnalytics(bundles)}
        <div class="review-drawer-list column-list">
          ${filteredBundles.length
            ? filteredBundles.map((bundle) => renderStructureBundleListItem(bundle, { currentVersion, selectedBundleId: state.selectedStructureBundleId })).join("")
            : '<div class="success-chip">✓ No bundles match the current inbox filter</div>'}
        </div>
      </div>
      ${selectedBundle
        ? renderStructureBundleDetail(selectedBundle, { currentVersion })
        : '<div class="section"><p>Select or open a bundle to inspect grouped review units, contradictions, and merge readiness.</p></div>'}
    </div>
  `;
}

function renderStructureBundleListItem(bundle, options = {}) {
  const reviewState = getStructureBundleReviewStats(bundle, options.currentVersion || 1);
  const selectedBundleId = options.selectedBundleId || "";
  const statusLabel = reviewState.stale
    ? "Needs rebase / regenerate"
    : reviewState.mergedAt
      ? "Merged"
      : reviewState.pendingCount
        ? "Review in progress"
        : reviewState.acceptedCount
          ? "Accepted and ready"
          : "Awaiting review";
  const statusClass = reviewState.stale
    ? "broken"
    : reviewState.mergedAt
      ? "healthy"
      : reviewState.pendingCount
        ? "warning"
        : "healthy";
  const detailText = reviewState.stale
    ? `Base v${formatValue(reviewState.baseVersion)} is behind canonical v${formatValue(reviewState.currentVersion)}.`
    : reviewState.mergedAt
      ? `Merged by ${escapeHtml(reviewState.mergedBy || "user")} at ${escapeHtml(reviewState.mergedAt)}.`
      : `reconciliation: planned missing ${formatValue(bundle.planned_missing_count || 0)} | untracked ${formatValue(bundle.observed_untracked_count || 0)} | divergent ${formatValue(bundle.implemented_differently_count || 0)}`;
  return `
    <div class="column-row structure-bundle-row ${selectedBundleId === bundle.bundle_id ? "selected" : ""} ${reviewState.stale ? "broken" : ""}">
      <div class="column-head">
        <div>
          <div class="column-main">${escapeHtml(bundle.bundle_id)}</div>
          <div class="chip-row">
            <span class="tag-chip">${escapeHtml(bundle.role || "scout")}</span>
            <span class="tag-chip">${escapeHtml(bundle.scope || "full")}</span>
            <span class="status-pill ${escapeHtml(statusClass)}">${escapeHtml(statusLabel)}</span>
            ${reviewState.reviewRequiredCount ? `<span class="status-pill broken">${escapeHtml(`${formatValue(reviewState.reviewRequiredCount)} review-required`)}</span>` : ""}
            ${bundle.ready_to_merge ? '<span class="status-pill healthy">ready to merge</span>' : ""}
            <span class="tag-chip">${escapeHtml(`triage: ${bundle.triage_state || "new"}`)}</span>
          </div>
        </div>
        <div class="row-actions">
          <button class="ghost-button" type="button" data-structure-action="open-bundle" data-bundle-id="${escapeHtml(bundle.bundle_id)}">${selectedBundleId === bundle.bundle_id ? "Loaded" : "Open"}</button>
        </div>
      </div>
      <div class="column-meta">pending ${formatValue(reviewState.pendingCount)} | accepted ${formatValue(reviewState.acceptedCount)} | deferred ${formatValue(reviewState.deferredCount)} | rejected ${formatValue(reviewState.rejectedCount)}</div>
      <div class="column-meta">contradictions ${formatValue(reviewState.contradictionCount)} | patches ${formatValue(reviewState.patchCount)} | readiness ${escapeHtml(bundle.readiness_status || "Not Ready")}</div>
      <div class="column-meta">binding mismatches ${formatValue(bundle.binding_mismatch_count || 0)} | column mismatches ${formatValue(bundle.column_mismatch_count || 0)} | downstream breakage ${formatValue(bundle.downstream_breakage_count || 0)}</div>
      <div class="column-meta">owner ${escapeHtml(bundle.bundle_owner || "unassigned")} | reviewer ${escapeHtml(bundle.assigned_reviewer || "unassigned")}</div>
      <div class="column-meta">${detailText}</div>
      <div class="column-meta">last review ${escapeHtml(formatStructureTimestamp(bundle.last_reviewed_at || ""))} ${bundle.last_reviewed_by ? `| by ${escapeHtml(bundle.last_reviewed_by)}` : ""}</div>
    </div>
  `;
}

function getStructureBundleReviewStats(bundle, currentVersion = 1) {
  const patches = bundle.patches || [];
  const readPatchCount = (stateValue) => (
    typeof bundle[`${stateValue}_count`] === "number"
      ? bundle[`${stateValue}_count`]
      : patches.filter((patch) => patch.review_state === stateValue).length
  );
  const baseVersion = bundle.base_structure_version || 1;
  const contradictionCount = typeof bundle.contradiction_count === "number" ? bundle.contradiction_count : (bundle.contradictions || []).length;
  const reviewRequiredCount = typeof bundle.review_required_count === "number"
    ? bundle.review_required_count
    : (bundle.contradictions || []).filter((item) => item.review_required !== false).length;
  return {
    patchCount: typeof bundle.patch_count === "number" ? bundle.patch_count : patches.length,
    pendingCount: readPatchCount("pending"),
    acceptedCount: readPatchCount("accepted"),
    rejectedCount: readPatchCount("rejected"),
    deferredCount: readPatchCount("deferred"),
    contradictionCount,
    reviewRequiredCount,
    mergedAt: bundle.merged_at || bundle.review?.merged_at || "",
    mergedBy: bundle.merged_by || bundle.review?.merged_by || "",
    baseVersion,
    currentVersion,
    stale: currentVersion > baseVersion,
  };
}

function formatStructureTimestamp(value) {
  if (!value) {
    return "Not reviewed yet";
  }
  return String(value).replace("T", " ").replace("Z", " UTC");
}

function humanizeStructureDecision(decision) {
  if (decision === "accepted") {
    return "Accept";
  }
  if (decision === "rejected") {
    return "Reject";
  }
  if (decision === "deferred") {
    return "Defer";
  }
  return "Review";
}

function humanizeStructureContradictionDecision(decision) {
  if (decision === "accepted") {
    return "Adopt observed";
  }
  if (decision === "rejected") {
    return "Keep canonical";
  }
  if (decision === "deferred") {
    return "Defer for evidence";
  }
  return "Review required";
}

function summarizeStructureContradictionTitle(contradiction, context) {
  const targetMeta = getStructureTargetMeta(context, contradiction.target_id || "", contradiction.field_id || "");
  if (targetMeta.field) {
    return `${targetMeta.nodeLabel}.${targetMeta.field.name}`;
  }
  return targetMeta.nodeLabel || contradiction.target_id || contradiction.id || "Contradiction";
}

function humanizeStructureTriageState(value) {
  if (value === "in_review") {
    return "In review";
  }
  if (value === "blocked") {
    return "Blocked";
  }
  if (value === "resolved") {
    return "Resolved";
  }
  return "New";
}

function structureTriageTone(value) {
  if (value === "blocked") {
    return "broken";
  }
  if (value === "resolved") {
    return "healthy";
  }
  if (value === "in_review") {
    return "warning";
  }
  return "";
}

function getStructureWorkflowState(bundle) {
  const review = bundle?.review || {};
  return {
    bundleOwner: review.bundle_owner || bundle?.bundle_owner || "",
    assignedReviewer: review.assigned_reviewer || bundle?.assigned_reviewer || "",
    triageState: review.triage_state || bundle?.triage_state || "new",
    triageNote: review.triage_note || bundle?.triage_note || "",
    workflowHistory: review.workflow_history || [],
  };
}

function formatStructureWorkflowChanges(changes) {
  const items = [];
  if (changes.bundle_owner !== undefined) {
    items.push(`owner -> ${changes.bundle_owner || "unassigned"}`);
  }
  if (changes.assigned_reviewer !== undefined) {
    items.push(`reviewer -> ${changes.assigned_reviewer || "unassigned"}`);
  }
  if (changes.triage_state !== undefined) {
    items.push(`triage -> ${humanizeStructureTriageState(changes.triage_state)}`);
  }
  if (changes.triage_note !== undefined) {
    items.push(`triage note ${changes.triage_note ? "updated" : "cleared"}`);
  }
  return items.join(" | ") || "Workflow updated";
}

function renderStructureWorkflowHistory(workflowHistory) {
  const history = (workflowHistory || []).slice().sort((left, right) => (
    String(right.updated_at || "").localeCompare(String(left.updated_at || ""))
  ));
  if (!history.length) {
    return '<div class="column-meta">No assignment or triage changes recorded yet.</div>';
  }
  return `
    <div class="structure-review-audit">
      <div class="group-heading">Workflow history</div>
      <div class="structure-review-audit-list">
        ${history.slice(0, 4).map((event) => `
          <div class="structure-review-audit-entry">
            <strong>${escapeHtml(`${event.updated_by || "user"} updated workflow`)}</strong>
            <div class="hint">${escapeHtml(formatStructureTimestamp(event.updated_at || ""))}</div>
            <div>${escapeHtml(formatStructureWorkflowChanges(event.changes || {}))}</div>
            ${event.note ? `<div class="hint">${escapeHtml(event.note)}</div>` : ""}
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function renderStructureWorkflowSection(bundle) {
  const workflow = getStructureWorkflowState(bundle);
  const draft = state.structureWorkflowDraft || {};
  const actingReviewer = getCurrentStructureReviewerIdentity();
  const triageState = draft.triageState || workflow.triageState || "new";
  const triageOptions = [
    { key: "new", label: "New" },
    { key: "in_review", label: "In review" },
    { key: "blocked", label: "Blocked" },
    { key: "resolved", label: "Resolved" },
  ];
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Review workflow</h3>
        <div class="row-actions">
          <span class="status-pill ${escapeHtml(structureTriageTone(triageState))}">${escapeHtml(humanizeStructureTriageState(triageState))}</span>
          <button class="ghost-button" type="button" data-structure-action="assign-bundle-to-me" data-bundle-id="${escapeHtml(bundle.bundle_id)}">${escapeHtml((draft.assignedReviewer || workflow.assignedReviewer) === actingReviewer ? "Assigned to me" : "Assign to me")}</button>
          <button class="ghost-button" type="button" data-structure-action="save-bundle-workflow" data-bundle-id="${escapeHtml(bundle.bundle_id)}">Save workflow</button>
        </div>
      </div>
      <div class="column-meta">Ownership and triage stay separate from accept / reject / defer so reviewers can coordinate noisy bundles without mutating review decisions.</div>
      <div class="structure-workflow-grid">
        <label class="form-field">
          Bundle owner
          <input data-structure-workflow-field="bundleOwner" value="${escapeHtml(draft.bundleOwner || workflow.bundleOwner || "")}" placeholder="platform-data" />
        </label>
        <label class="form-field">
          Assigned reviewer
          <input data-structure-workflow-field="assignedReviewer" value="${escapeHtml(draft.assignedReviewer || workflow.assignedReviewer || "")}" placeholder="review-manager" />
        </label>
        <label class="form-field form-field-full">
          Triage note
          <textarea data-structure-workflow-field="triageNote" placeholder="Blocked on schema clarification from API owners">${escapeHtml(draft.triageNote || workflow.triageNote || "")}</textarea>
        </label>
      </div>
      <div class="structure-triage-bar">
        ${triageOptions.map((option) => `
          <button
            class="ghost-button structure-queue-chip ${triageState === option.key ? "active" : ""}"
            type="button"
            data-structure-action="set-triage-state"
            data-bundle-id="${escapeHtml(bundle.bundle_id)}"
            data-triage-state="${escapeHtml(option.key)}"
            aria-pressed="${triageState === option.key ? "true" : "false"}"
          >
            ${escapeHtml(option.label)}
          </button>
        `).join("")}
      </div>
      ${renderStructureWorkflowHistory(workflow.workflowHistory)}
    </div>
  `;
}

function renderStructureMergeReadinessChecklist(bundle, reviewState, patchGroups) {
  const fieldGroups = patchGroups.flatMap((group) => group.fieldGroups || []);
  const pendingReviewUnits = fieldGroups.filter((fieldGroup) => fieldGroup.pendingIds.length || fieldGroup.relatedContradictions.some((item) => item.review_required !== false)).length;
  const deferredHighImpactUnits = fieldGroups.filter((fieldGroup) => fieldGroup.highImpact && fieldGroup.reviewCounts.deferred > 0).length;
  const blockers = bundle.review?.merge_blockers || [];
  const items = [
    {
      label: "Base version aligned",
      done: !reviewState.stale,
      detail: reviewState.stale
        ? `Bundle base v${formatValue(reviewState.baseVersion)} trails canonical v${formatValue(reviewState.currentVersion)}.`
        : "Canonical YAML and bundle base version match.",
    },
    {
      label: "Review-required contradictions resolved",
      done: reviewState.reviewRequiredCount === 0,
      detail: reviewState.reviewRequiredCount
        ? `${formatValue(reviewState.reviewRequiredCount)} contradiction${reviewState.reviewRequiredCount === 1 ? "" : "s"} still need explicit review.`
        : "No contradictions are still marked review-required.",
    },
    {
      label: "Pending review units cleared",
      done: pendingReviewUnits === 0,
      detail: pendingReviewUnits
        ? `${formatValue(pendingReviewUnits)} review unit${pendingReviewUnits === 1 ? "" : "s"} still have pending patches or unresolved contradictions.`
        : "Every visible review unit has been resolved or intentionally deferred.",
    },
    {
      label: "High-impact deferred items revisited",
      done: deferredHighImpactUnits === 0,
      detail: deferredHighImpactUnits
        ? `${formatValue(deferredHighImpactUnits)} high-impact unit${deferredHighImpactUnits === 1 ? "" : "s"} are still deferred.`
        : "No high-impact units are sitting in deferred limbo.",
    },
    {
      label: "Deterministic merge blockers clear",
      done: blockers.length === 0,
      detail: blockers.length ? (blockers[0].reason || blockers[0].type || "Merge is blocked.") : "No merge blockers are recorded on this bundle.",
    },
  ];
  const readyToMerge = items.every((item) => item.done) && reviewState.acceptedCount > 0;
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Merge readiness</h3>
        <span class="status-pill ${escapeHtml(readyToMerge ? "healthy" : "warning")}">${escapeHtml(readyToMerge ? "Ready to merge" : "Needs review")}</span>
      </div>
      <div class="column-list structure-checklist">
        ${items.map((item) => `
          <div class="structure-checklist-row ${item.done ? "done" : "blocked"}">
            <div>
              <div class="column-main">${escapeHtml(item.label)}</div>
              <div class="column-meta">${escapeHtml(item.detail)}</div>
            </div>
            <span class="status-pill ${escapeHtml(item.done ? "healthy" : "warning")}">${escapeHtml(item.done ? "Clear" : "Needs action")}</span>
          </div>
        `).join("")}
        <div class="structure-checklist-row ${readyToMerge ? "done" : "blocked"}">
          <div>
            <div class="column-main">Accepted patch set queued</div>
            <div class="column-meta">${reviewState.acceptedCount ? `${formatValue(reviewState.acceptedCount)} accepted patch${reviewState.acceptedCount === 1 ? "" : "es"} ready for merge.` : "Merge stays disabled until at least one accepted patch exists."}</div>
          </div>
          <span class="status-pill ${escapeHtml(reviewState.acceptedCount ? "healthy" : "warning")}">${escapeHtml(reviewState.acceptedCount ? "Queued" : "Missing")}</span>
        </div>
      </div>
    </div>
  `;
}

function renderStructureKeyboardHelp(filteredPatchGroups) {
  const visibleUnits = collectVisibleStructureReviewUnits(filteredPatchGroups);
  if (!visibleUnits.length) {
    return "";
  }
  const activeIndex = Math.max(0, visibleUnits.findIndex((item) => item.reviewUnitKey === state.structureActiveReviewUnitKey));
  return `
    <div class="structure-keyboard-help" role="note">
      Keyboard review: <kbd>J</kbd>/<kbd>K</kbd> move between units, <kbd>A</kbd> accept, <kbd>D</kbd> defer, <kbd>R</kbd> reject.
      <span class="hint">${escapeHtml(`Active unit ${formatValue(activeIndex + 1)} of ${formatValue(visibleUnits.length)}`)}</span>
    </div>
  `;
}

function renderStructureRebaseReviewUnitList(title, items, tone = "") {
  if (!(items || []).length) {
    return "";
  }
  return `
    <div class="structure-review-unit-section">
      <div class="group-heading">${escapeHtml(title)}</div>
      <ul class="warning-list compact-list ${escapeHtml(tone)}">
        ${(items || []).map((item) => `
          <li>
            <div>${escapeHtml(item.label || item.target_id || item.key || "Review unit")}</div>
            <div class="hint">${escapeHtml(`${humanizeStructureDecision(item.decision || "pending")} | ${item.patch_type || item.kind || "structure"}`)}</div>
          </li>
        `).join("")}
      </ul>
    </div>
  `;
}

function renderStructureBundleDetail(bundle, options = {}) {
  const reviewState = getStructureBundleReviewStats(bundle, options.currentVersion || 1);
  const context = buildStructureBundleContext(bundle);
  const patchGroups = groupStructureBundlePatches(bundle, context);
  const filteredPatchGroups = filterStructurePatchGroups(patchGroups);
  syncActiveStructureReviewUnit(filteredPatchGroups);
  const hiddenLowConfidencePendingIds = patchGroups.flatMap((group) => group.hiddenLowConfidencePendingIds);
  const hiddenNonMaterialPendingIds = filteredPatchGroups.hiddenNonMaterialPendingIds || [];
  const lastReviewNote = bundle.review?.last_review_note || "";
  return `
    <div class="section structure-bundle-detail">
      <div class="section-actions">
        <h3>Bundle detail</h3>
        <div class="row-actions">
          <span class="status-pill ${escapeHtml(reviewState.stale ? "broken" : reviewState.mergedAt ? "healthy" : reviewState.pendingCount ? "warning" : "healthy")}">
            ${escapeHtml(
              reviewState.stale
                ? "Needs rebase / regenerate"
                : reviewState.mergedAt
                  ? "Merged"
                  : reviewState.acceptedCount && !reviewState.pendingCount && !reviewState.reviewRequiredCount
                    ? "Ready for merge"
                    : "Review in progress"
            )}
          </span>
          ${reviewState.stale ? `<button class="ghost-button" type="button" data-structure-action="preview-rebase" data-bundle-id="${escapeHtml(bundle.bundle_id)}">Preview rebase</button>` : ""}
          ${reviewState.stale ? `<button class="ghost-button" type="button" data-structure-action="rebase-bundle" data-bundle-id="${escapeHtml(bundle.bundle_id)}">Rebase bundle</button>` : ""}
          <button class="ghost-button" type="button" data-structure-action="merge-bundle" data-bundle-id="${escapeHtml(bundle.bundle_id)}" ${bundle.ready_to_merge || (reviewState.acceptedCount && !reviewState.pendingCount && !reviewState.stale && !reviewState.reviewRequiredCount) ? "" : "disabled"}>Merge accepted</button>
        </div>
      </div>
      <div class="structure-review-banner ${reviewState.stale ? "broken" : reviewState.mergedAt ? "healthy" : "warning"}">
        ${reviewState.stale
          ? `Base version v${formatValue(reviewState.baseVersion)} is behind canonical YAML v${formatValue(reviewState.currentVersion)}. Preview the rebase first so reviewers can see which decisions transfer and which items need fresh review.`
          : reviewState.mergedAt
            ? `This bundle already merged into canonical YAML at ${escapeHtml(formatStructureTimestamp(reviewState.mergedAt))}${reviewState.mergedBy ? ` by ${escapeHtml(reviewState.mergedBy)}` : ""}.`
            : reviewState.reviewRequiredCount
              ? `${formatValue(reviewState.reviewRequiredCount)} contradiction${reviewState.reviewRequiredCount === 1 ? "" : "s"} still need explicit review before merge.`
              : reviewState.acceptedCount
                ? `${formatValue(reviewState.acceptedCount)} accepted patch${reviewState.acceptedCount === 1 ? "" : "es"} are queued. Finish the remaining review units, then merge.`
                : "Inspect contradictions first, then move through the grouped review units."}
      </div>
      ${renderStructureMergeBlockers(bundle.review?.merge_blockers || [])}
      <div class="structure-bundle-summary-grid">
        <div class="meta-card"><strong>Base version</strong>${escapeHtml(formatValue(reviewState.baseVersion))}</div>
        <div class="meta-card"><strong>Canonical now</strong>${escapeHtml(formatValue(reviewState.currentVersion))}</div>
        <div class="meta-card"><strong>Role / scope</strong>${escapeHtml(`${bundle.scan?.role || "scout"} / ${bundle.scan?.scope || "full"}`)}</div>
        <div class="meta-card"><strong>Created</strong>${escapeHtml(formatStructureTimestamp(bundle.scan?.created_at || ""))}</div>
        <div class="meta-card"><strong>Last reviewed</strong>${escapeHtml(formatStructureTimestamp(bundle.review?.last_reviewed_at || ""))}</div>
        <div class="meta-card"><strong>Reviewer</strong>${escapeHtml(bundle.review?.last_reviewed_by || "Not reviewed yet")}</div>
        <div class="meta-card"><strong>Merge state</strong>${escapeHtml(bundle.review?.merge_status || (reviewState.stale ? "stale" : reviewState.mergedAt ? "merged" : "open"))}</div>
        <div class="meta-card"><strong>Fingerprint</strong>${escapeHtml((bundle.scan?.fingerprint || "").slice(0, 12) || "unknown")}</div>
      </div>
      ${renderStructureWorkflowSection(bundle)}
      ${renderStructureMergeReadinessChecklist(bundle, reviewState, patchGroups)}
      ${lastReviewNote ? `<div class="structure-inline-impact note">Last reviewer note: ${escapeHtml(lastReviewNote)}</div>` : ""}
      <div class="chip-row">
        <span class="status-pill warning">${escapeHtml(`Pending: ${formatValue(reviewState.pendingCount)}`)}</span>
        <span class="status-pill healthy">${escapeHtml(`Accepted: ${formatValue(reviewState.acceptedCount)}`)}</span>
        <span class="status-pill">${escapeHtml(`Deferred: ${formatValue(reviewState.deferredCount)}`)}</span>
        <span class="status-pill">${escapeHtml(`Rejected: ${formatValue(reviewState.rejectedCount)}`)}</span>
        <span class="status-pill broken">${escapeHtml(`Contradictions: ${formatValue(reviewState.contradictionCount)}`)}</span>
        ${hiddenLowConfidencePendingIds.length ? `<span class="status-pill">${escapeHtml(`Low-confidence hidden: ${formatValue(hiddenLowConfidencePendingIds.length)}`)}</span>` : ""}
        ${hiddenNonMaterialPendingIds.length && !state.structureShowNonMaterial ? `<span class="status-pill">${escapeHtml(`Non-material hidden: ${formatValue(hiddenNonMaterialPendingIds.length)}`)}</span>` : ""}
      </div>
      ${renderStructureBundleRebasePreview(bundle, reviewState)}
      ${renderStructureWhyThisMatters(bundle, context)}
      ${renderStructureContradictionReview(bundle, context)}
      <div class="section">
        <div class="section-actions">
          <h3>Patch review inbox</h3>
          <div class="row-actions">
            <span class="hint">${formatValue(filteredPatchGroups.visiblePendingIds.length)} visible pending patch${filteredPatchGroups.visiblePendingIds.length === 1 ? "" : "es"} grouped by node, then field</span>
            ${renderStructureReviewActions(bundle.bundle_id, filteredPatchGroups.visiblePendingIds, {
              acceptLabel: "Accept visible",
              deferLabel: "Defer visible",
              rejectLabel: "Reject visible",
            })}
          </div>
        </div>
        <div class="structure-review-toolbar">
          ${renderStructureReviewUnitFilters(patchGroups)}
          <div class="structure-review-toggles">
            <button class="ghost-button ${state.structureShowLowConfidence ? "active" : ""}" type="button" data-structure-action="toggle-low-confidence">${state.structureShowLowConfidence ? "Hide low-confidence" : "Show low-confidence"}</button>
            <button class="ghost-button ${state.structureShowMinorImpacts ? "active" : ""}" type="button" data-structure-action="toggle-minor-impacts">${state.structureShowMinorImpacts ? "Hide minor impacts" : "Show minor impacts"}</button>
            <button class="ghost-button ${state.structureShowNonMaterial ? "active" : ""}" type="button" data-structure-action="toggle-non-material">${state.structureShowNonMaterial ? "Hide non-material" : "Show non-material"}</button>
          </div>
        </div>
        ${renderStructureKeyboardHelp(filteredPatchGroups)}
        <div class="column-meta">Review units keep canonical state, proposed state, contradictions, evidence, and consumer impact together so large bundles stay reviewable.</div>
        ${hiddenLowConfidencePendingIds.length && !state.structureShowLowConfidence ? '<div class="column-meta">Low-confidence-only suggestions stay collapsed by default so inspection stays focused on higher-signal changes.</div>' : ""}
        ${hiddenNonMaterialPendingIds.length && !state.structureShowNonMaterial ? '<div class="column-meta">Only material changes stay in the main queue by default; confidence-only churn and no-impact reconciliations remain tucked away unless you ask for them.</div>' : ""}
        <div class="column-list structure-review-node-list">
          ${filteredPatchGroups.groups.length
            ? filteredPatchGroups.groups.map((group) => renderStructurePatchNodeGroup(bundle.bundle_id, group, context)).join("")
            : '<div class="success-chip">✓ No review units match the current filter</div>'}
        </div>
      </div>
      ${renderStructureReconciliation(bundle.reconciliation || {})}
    </div>
  `;
}

function renderStructureBundleRebasePreview(bundle, reviewState) {
  const preview = state.structureRebasePreviews[bundle.bundle_id] || bundle.review?.last_rebase_summary || null;
  if (!reviewState.stale && !preview) {
    return "";
  }
  const preservedStates = preview?.preserved_review_states || {};
  const droppedStates = preview?.dropped_review_states || {};
  const changedTargets = preview?.changed_targets || [];
  return `
    <div class="structure-rebase-card ${reviewState.stale ? "broken" : "warning"}">
      <div class="section-actions">
        <h3>Base version / rebase preview</h3>
        ${reviewState.stale ? `<button class="ghost-button" type="button" data-structure-action="preview-rebase" data-bundle-id="${escapeHtml(bundle.bundle_id)}">Refresh preview</button>` : ""}
      </div>
      <div class="column-meta">
        ${reviewState.stale
          ? `This bundle was generated against v${formatValue(reviewState.baseVersion)} and now trails canonical v${formatValue(reviewState.currentVersion)}.`
          : "Latest rebase summary is recorded here so reviewers can see what changed and what was preserved."}
      </div>
      ${preview ? `
        <div class="structure-rebase-grid">
          <div class="meta-card"><strong>Transferred reviews</strong>${escapeHtml(formatValue(preview.transferred_review_count || 0))}</div>
          <div class="meta-card"><strong>Dropped reviews</strong>${escapeHtml(formatValue(preview.dropped_review_count || 0))}</div>
          <div class="meta-card"><strong>Pending after rebase</strong>${escapeHtml(formatValue(preview.pending_patch_count || 0))}</div>
          <div class="meta-card"><strong>Review-required contradictions</strong>${escapeHtml(formatValue(preview.review_required_count || 0))}</div>
        </div>
        <div class="chip-row">
          <span class="status-pill healthy">${escapeHtml(`Accepted kept: ${formatValue(preservedStates.accepted || 0)}`)}</span>
          <span class="status-pill">${escapeHtml(`Deferred kept: ${formatValue(preservedStates.deferred || 0)}`)}</span>
          <span class="status-pill broken">${escapeHtml(`Dropped accepted: ${formatValue(droppedStates.accepted || 0)}`)}</span>
          <span class="status-pill broken">${escapeHtml(`Dropped deferred: ${formatValue(droppedStates.deferred || 0)}`)}</span>
        </div>
        ${renderStructureRebaseReviewUnitList("Preserved review units", preview.preserved_review_units || [], "healthy")}
        ${renderStructureRebaseReviewUnitList("Dropped review units", preview.dropped_review_units || [], "warning")}
        ${changedTargets.length ? `
          <div class="structure-review-required-note">
            <div class="group-heading">Needs fresh review after rebase</div>
            <ul class="warning-list compact-list structure-preview-list">
              ${changedTargets.map((item) => `<li>${escapeHtml(`${item.kind}: ${item.label} - ${item.message}`)}</li>`).join("")}
            </ul>
          </div>
        ` : '<div class="success-chip">✓ No extra re-review targets surfaced in the latest preview</div>'}
        <div class="column-meta">Workflow ownership and triage state transfer with preserved review units so stale bundles can be rebased without losing queue context.</div>
      ` : `
        <div class="structure-review-required-note">
          Preview the rebase before you regenerate this stale bundle so reviewers can see which decisions transfer cleanly and which units need another pass.
        </div>
      `}
    </div>
  `;
}

function renderStructureWhyThisMatters(bundle, context) {
  const groups = buildStructureBundleImpactGroups(bundle, context);
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Why this matters</h3>
        <span class="hint">${state.structureShowMinorImpacts ? "Showing significant and minor impacts" : "Showing significant impacts only"}</span>
      </div>
      ${groups.length ? `
        <div class="structure-impact-grid">
          ${groups.map((group) => renderStructureImpactGroup(group)).join("")}
        </div>
      ` : '<div class="success-chip">✓ No significant downstream impact detected</div>'}
    </div>
  `;
}

function buildStructureBundleImpactGroups(bundle, context) {
  const grouped = new Map();
  const order = ["ui_breakage", "contract_breakage", "consumer_risk", "unknown"];
  collectStructureBundleImpactItems(bundle, context, { includeMinor: state.structureShowMinorImpacts }).forEach((item) => {
    const key = classifyStructureImpactItem(item, context);
    const message = item.message || item.label || item.target_id || "";
    if (!message) {
      return;
    }
    if (!grouped.has(key)) {
      grouped.set(key, { key, items: [], seen: new Set(), severity: "low" });
    }
    const group = grouped.get(key);
    if (group.seen.has(message)) {
      return;
    }
    group.seen.add(message);
    group.items.push(item);
    if (structureImpactSeverityRank(item.severity) < structureImpactSeverityRank(group.severity)) {
      group.severity = item.severity;
    }
  });
  return order
    .filter((key) => grouped.has(key))
    .map((key) => {
      const group = grouped.get(key);
      return {
        ...group,
        title: key === "ui_breakage"
          ? "UI breakage"
          : key === "contract_breakage"
            ? "Contract breakage"
            : key === "consumer_risk"
              ? "Consumer risk"
              : "Unknown impact",
      };
    });
}

function structureImpactSeverityRank(value) {
  if (value === "high") {
    return 0;
  }
  if (value === "medium") {
    return 1;
  }
  return 2;
}

function inferStructureImpactSeverity(item, context) {
  if (item.severity === "high" || item.severity === "medium" || item.severity === "low") {
    return item.severity;
  }
  if (item.significant === false) {
    return "low";
  }
  const message = String(item.message || "").toLowerCase();
  if (message.includes("will not render") || message.includes("break") || message.includes("missing required")) {
    return "high";
  }
  const targetMeta = getStructureTargetMeta(context, item.target_id || "", item.field_id || "");
  if (targetMeta.node?.kind === "contract") {
    return "high";
  }
  if (targetMeta.node?.kind === "data" || targetMeta.node?.kind === "compute") {
    return "medium";
  }
  return "low";
}

function collectStructureBundleImpactItems(bundle, context, options = {}) {
  const includeMinor = Boolean(options.includeMinor);
  const items = [];
  const pushItem = (item, source) => {
    if (!item?.message) {
      return;
    }
    const normalized = {
      ...item,
      source: item.source || source,
    };
    normalized.severity = inferStructureImpactSeverity(normalized, context);
    if (!includeMinor && normalized.severity === "low") {
      return;
    }
    items.push(normalized);
  };
  (bundle.impacts || []).forEach((item) => {
    pushItem(item, "impact");
  });
  ["planned_missing", "implemented_differently", "observed_untracked"].forEach((section) => {
    (bundle.reconciliation?.[section] || []).forEach((item) => {
      pushItem(item, section);
    });
  });
  (bundle.contradictions || []).forEach((contradiction) => {
    (contradiction.downstream_impacts || []).forEach((message) => {
      pushItem(
        {
          message,
          target_id: contradiction.field_id || contradiction.target_id || contradiction.id,
          field_id: contradiction.field_id || "",
          node_id: contradiction.node_id || contradiction.target_id || "",
          severity: contradiction.severity || "",
          significant: contradiction.severity !== "low",
          why_this_matters: contradiction.why_this_matters || "",
        },
        "contradiction"
      );
    });
  });
  return items;
}

function classifyStructureImpactItem(item, context) {
  const message = String(item.message || "").toLowerCase();
  if (message.includes("render")) {
    return "ui_breakage";
  }
  if (message.includes("contract") || message.includes("api")) {
    return "contract_breakage";
  }
  if (message.includes("will not render")) {
    return "ui_breakage";
  }
  const targetMeta = getStructureTargetMeta(context, item.target_id || "", item.field_id || "");
  if (targetMeta.node?.kind === "contract" && targetMeta.node?.extension_type === "ui") {
    return "ui_breakage";
  }
  if (targetMeta.node?.kind === "contract") {
    return "contract_breakage";
  }
  if (targetMeta.node?.kind === "data" || targetMeta.node?.kind === "compute") {
    return "consumer_risk";
  }
  return "unknown";
}

function collectStructureImpactItemsForTargets(bundle, targetIds, context, options = {}) {
  const normalizedTargets = new Set((targetIds || []).filter(Boolean));
  if (!normalizedTargets.size) {
    return [];
  }
  const items = [];
  const seen = new Set();
  collectStructureBundleImpactItems(bundle, context, { includeMinor: options.includeMinor }).forEach((item) => {
    const relatedTargets = [
      item.field_id,
      item.target_id,
      item.node_id,
      ...(item.affected_refs || []),
    ].filter(Boolean);
    if (!relatedTargets.some((targetId) => normalizedTargets.has(targetId))) {
      return;
    }
    const dedupeKey = `${item.message}|${item.target_id || ""}|${item.source || ""}`;
    if (seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);
    items.push(item);
  });
  return items.sort((left, right) => (
    structureImpactSeverityRank(left.severity) - structureImpactSeverityRank(right.severity)
    || String(left.message || "").localeCompare(String(right.message || ""))
  ));
}

function renderStructureImpactGroup(group) {
  return `
    <article class="structure-impact-card ${escapeHtml(group.severity || "low")}">
      <div class="column-head">
        <div class="column-main">${escapeHtml(group.title)}</div>
        <span class="status-pill ${escapeHtml(group.severity === "high" ? "broken" : group.severity === "medium" ? "warning" : "")}">${escapeHtml(`${formatValue(group.items.length)} ${group.severity || "low"}`)}</span>
      </div>
      <ul class="warning-list compact-list">
        ${group.items.slice(0, 4).map((item) => `
          <li>
            <div>${escapeHtml(item.message || item.label || item.target_id || "")}</div>
            ${item.why_this_matters && item.why_this_matters !== item.message ? `<div class="hint">${escapeHtml(item.why_this_matters)}</div>` : ""}
          </li>
        `).join("")}
      </ul>
      ${group.items.length > 4 ? `<div class="column-meta">${escapeHtml(`${formatValue(group.items.length - 4)} more impact signal${group.items.length - 4 === 1 ? "" : "s"} hidden in this bucket.`)}</div>` : ""}
    </article>
  `;
}

function renderStructureContradictionReview(bundle, context) {
  const contradictions = bundle.contradictions || [];
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Contradiction review</h3>
        <span class="hint">${formatValue(contradictions.length)} contradiction${contradictions.length === 1 ? "" : "s"}${contradictions.length ? " require explicit review" : ""}</span>
      </div>
      ${contradictions.length ? `
        <div class="column-list">
          ${contradictions.map((item) => renderStructureContradictionCard(bundle, item, context)).join("")}
        </div>
      ` : '<div class="success-chip">✓ No contradictions detected in this bundle</div>'}
    </div>
  `;
}

function renderStructureContradictionCard(bundle, contradiction, context) {
  const relatedPatches = findStructureRelatedPatches(bundle, contradiction);
  const pendingRelatedIds = relatedPatches.filter((patch) => patch.review_state === "pending").map((patch) => patch.id);
  const title = summarizeStructureContradictionTitle(contradiction, context);
  const evidenceLabels = Array.from(new Set([...(contradiction.evidence_sources || []), ...relatedPatches.flatMap((patch) => patch.evidence || [])]));
  const reviewLabel = contradiction.review_required === false && contradiction.review_state !== "pending"
    ? humanizeStructureContradictionDecision(contradiction.review_state)
    : contradiction.review_required === false
      ? "Reviewed"
      : "Review required";
  return `
    <div class="column-row structure-contradiction-card">
      <div class="column-head">
        <div>
          <div class="column-main">${escapeHtml(title)}</div>
          <div class="column-meta">${escapeHtml(contradiction.message || contradiction.target_id || "Observed structure conflicts with canonical memory.")}</div>
        </div>
        <div class="row-actions">
          <span class="status-pill ${escapeHtml(contradiction.review_required === false ? "healthy" : "broken")}">${escapeHtml(reviewLabel)}</span>
          ${contradiction.confidence_delta ? `<span class="tag-chip">${escapeHtml(`confidence ${contradiction.confidence_delta}`)}</span>` : ""}
        </div>
      </div>
      ${contradiction.why_this_matters ? `<div class="structure-inline-impact">${escapeHtml(contradiction.why_this_matters)}</div>` : ""}
      <div class="structure-review-required-note">Choose one explicit resolution path so the contradiction stops floating as ambiguous structure memory.</div>
      <div class="structure-evidence-grid">
        <div class="structure-evidence-card">
          <div class="group-heading">Canonical memory</div>
          ${renderStructureEvidenceEntries(contradiction.existing_belief || {}, context)}
        </div>
        <div class="structure-evidence-card">
          <div class="group-heading">Latest scan</div>
          ${renderStructureEvidenceEntries(contradiction.new_evidence || {}, context)}
        </div>
      </div>
      ${evidenceLabels.length ? `
        <div class="chip-row">
          ${evidenceLabels.map((label) => `<span class="tag-chip">${escapeHtml(label)}</span>`).join("")}
        </div>
      ` : ""}
      ${contradiction.downstream_impacts?.length ? `
        <div class="structure-impact-list">
          <div class="group-heading">Direct downstream impact</div>
          <ul class="warning-list compact-list">
            ${contradiction.downstream_impacts.map((impact) => `<li>${escapeHtml(impact)}</li>`).join("")}
          </ul>
        </div>
      ` : ""}
      ${contradiction.affected_refs?.length ? `<div class="column-meta">Affected refs: ${escapeHtml(contradiction.affected_refs.map((value) => formatStructureReference(context, value)).join(" | "))}</div>` : ""}
      ${renderStructurePatchStateSummary(summarizeStructurePatchStates(relatedPatches))}
      <div class="structure-contradiction-actions">
        <button class="ghost-button" type="button" data-structure-action="review-contradiction" data-bundle-id="${escapeHtml(bundle.bundle_id)}" data-contradiction-id="${escapeHtml(contradiction.id)}" data-decision="accepted" ${contradiction.review_state === "accepted" ? "disabled" : ""}>Adopt observed</button>
        <button class="ghost-button" type="button" data-structure-action="review-contradiction" data-bundle-id="${escapeHtml(bundle.bundle_id)}" data-contradiction-id="${escapeHtml(contradiction.id)}" data-decision="deferred" ${contradiction.review_state === "deferred" ? "disabled" : ""}>Defer for evidence</button>
        <button class="ghost-button danger-soft" type="button" data-structure-action="review-contradiction" data-bundle-id="${escapeHtml(bundle.bundle_id)}" data-contradiction-id="${escapeHtml(contradiction.id)}" data-decision="rejected" ${contradiction.review_state === "rejected" ? "disabled" : ""}>Keep canonical</button>
      </div>
      ${pendingRelatedIds.length ? `<div class="column-meta">${escapeHtml(`${formatValue(pendingRelatedIds.length)} related pending patch${pendingRelatedIds.length === 1 ? "" : "es"} will move with this contradiction decision.`)}</div>` : ""}
      ${renderStructureReviewAudit([contradiction], { title: "Contradiction audit trail", decisionLabelFn: humanizeStructureContradictionDecision })}
    </div>
  `;
}

function renderStructureEvidenceEntries(entries, context) {
  const items = Object.entries(entries || {}).filter(([, value]) => value !== "" && value !== null && value !== false);
  if (!items.length) {
    return '<div class="hint">No explicit evidence captured.</div>';
  }
  return items.map(([key, value]) => `
    <div class="structure-evidence-entry">
      <span class="structure-evidence-label">${escapeHtml(humanizeStructureEvidenceKey(key))}</span>
      <span class="structure-evidence-value">${escapeHtml(formatStructureEvidenceValue(context, key, value))}</span>
    </div>
  `).join("");
}

function humanizeStructureEvidenceKey(value) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function formatStructureEvidenceValue(context, key, value) {
  if (key === "missing_in_scan" && value) {
    return "Not observed in this scan";
  }
  if (Array.isArray(value)) {
    return value.map((item) => formatStructureEvidenceValue(context, key, item)).join(", ");
  }
  if (typeof value === "string" && (key.includes("binding") || key.includes("ref"))) {
    return formatStructureReference(context, value);
  }
  if (typeof value === "object" && value) {
    return JSON.stringify(value);
  }
  return String(value);
}

function renderStructureReconciliation(reconciliation) {
  const summary = reconciliation.summary || {};
  const comparison = reconciliation.comparison || {};
  const downstreamBreakage = reconciliation.downstream_breakage || {};
  const sections = [
    { key: "planned_missing", label: "Planned missing", tone: "contract_breaks" },
    { key: "observed_untracked", label: "Observed untracked", tone: "warnings" },
    { key: "implemented_differently", label: "Implemented differently", tone: "warnings" },
    { key: "uncertain_matches", label: "Uncertain matches", tone: "informational" },
  ];
  const hasItems = sections.some((section) => (reconciliation[section.key] || []).length);
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Reconciliation</h3>
        <span class="hint">planned vs observed structure</span>
      </div>
      <div class="chip-row">
        <span class="issue-chip broken">planned missing ${formatValue(summary.planned_missing || 0)}</span>
        <span class="issue-chip warning">untracked ${formatValue(summary.observed_untracked || 0)}</span>
        <span class="issue-chip warning">divergent ${formatValue(summary.implemented_differently || 0)}</span>
        <span class="issue-chip">uncertain ${formatValue(summary.uncertain_matches || 0)}</span>
        <span class="issue-chip broken">binding mismatches ${formatValue(comparison.binding_mismatches || 0)}</span>
        <span class="issue-chip warning">column mismatches ${formatValue(comparison.column_mismatches || 0)}</span>
        <span class="issue-chip">${escapeHtml(`downstream breakage ${formatValue(downstreamBreakage.count || 0)}`)}</span>
      </div>
      ${renderStructureComparisonSummary(comparison, downstreamBreakage)}
      ${hasItems ? `
        <div class="severity-grid">
          ${sections.map((section) => renderStructureReconciliationSection(section, reconciliation[section.key] || [])).join("")}
        </div>
      ` : '<div class="success-chip">✓ No issues</div>'}
    </div>
  `;
}

function renderStructureReconciliationSection(section, items) {
  const visibleItems = items.filter((item) => item.significant !== false || section.key === "uncertain_matches");
  const renderedItems = (visibleItems.length ? visibleItems : items).slice(0, 6);
  return `
    <details class="severity-tile ${escapeHtml(section.tone)}" ${section.key !== "uncertain_matches" && renderedItems.length ? "open" : ""}>
      <summary>${escapeHtml(section.label)} (${formatValue(items.length)})</summary>
      <div class="severity-body">
        ${renderedItems.length ? `
          <ul class="warning-list">
            ${renderedItems.map((item) => `
              <li>
                <div>${escapeHtml(item.message || item.label || item.target_id || "")}</div>
                ${item.why_this_matters ? `<div class="hint">${escapeHtml(item.why_this_matters)}</div>` : ""}
                ${item.source ? `<div class="column-meta">source: ${escapeHtml(item.source)}</div>` : ""}
              </li>
            `).join("")}
          </ul>
        ` : '<div class="success-chip">✓ No issues</div>'}
      </div>
    </details>
  `;
}

function renderStructureMergeBlockers(blockers) {
  if (!blockers.length) {
    return "";
  }
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Merge blockers</h3>
        <span class="hint">${formatValue(blockers.length)} blocking issue${blockers.length === 1 ? "" : "s"}</span>
      </div>
      <ul class="warning-list compact-list">
        ${blockers.map((blocker) => `<li>${escapeHtml(blocker.reason || blocker.type || "Blocked")}</li>`).join("")}
      </ul>
    </div>
  `;
}

function renderStructureComparisonSummary(comparison, downstreamBreakage) {
  const items = [
    { label: "Plan docs", value: comparison.plan_candidates || 0, tone: "" },
    { label: "Planned fields", value: comparison.planned_fields || 0, tone: "" },
    { label: "Matched fields", value: comparison.matched_fields || 0, tone: "healthy" },
    { label: "Missing fields", value: comparison.missing_fields || 0, tone: "broken" },
    { label: "Unplanned fields", value: comparison.unplanned_fields || 0, tone: "warning" },
    { label: "Binding mismatches", value: comparison.binding_mismatches || 0, tone: "broken" },
    { label: "Column mismatches", value: comparison.column_mismatches || 0, tone: "warning" },
    { label: "Breakage signals", value: downstreamBreakage.count || 0, tone: "broken" },
  ];
  return `
    <div class="structure-impact-grid">
      ${items.map((item) => `
        <article class="structure-impact-card">
          <div class="column-head">
            <div class="column-main">${escapeHtml(item.label)}</div>
            <span class="status-pill ${escapeHtml(item.tone)}">${formatValue(item.value)}</span>
          </div>
        </article>
      `).join("")}
      ${downstreamBreakage.count ? `
        <article class="structure-impact-card">
          <div class="column-head">
            <div class="column-main">Direct downstream breakage</div>
            <span class="status-pill broken">${formatValue(downstreamBreakage.count || 0)}</span>
          </div>
          <ul class="warning-list compact-list">
            ${(downstreamBreakage.items || []).slice(0, 4).map((item) => `<li>${escapeHtml(item.message || item.target_id || "")}</li>`).join("")}
          </ul>
        </article>
      ` : ""}
    </div>
  `;
}

function buildStructureBundleContext(bundle) {
  const nodeById = Object.fromEntries((bundle.observed?.nodes || []).map((node) => [node.id, node]));
  const edgeById = Object.fromEntries((bundle.observed?.edges || []).map((edge) => [edge.id, edge]));
  const fieldById = {};
  const impactByTarget = new Map();
  (bundle.impacts || []).forEach((impact) => {
    if (impact.target_id && impact.message && !impactByTarget.has(impact.target_id)) {
      impactByTarget.set(impact.target_id, impact.message);
    }
  });
  (bundle.contradictions || []).forEach((contradiction) => {
    const targetId = contradiction.field_id || contradiction.target_id || contradiction.id;
    if (targetId && contradiction.downstream_impacts?.[0] && !impactByTarget.has(targetId)) {
      impactByTarget.set(targetId, contradiction.downstream_impacts[0]);
    }
  });
  Object.values(nodeById).forEach((node) => {
    [...(node.columns || []), ...(node.contract?.fields || [])].forEach((field) => {
      fieldById[field.id] = {
        field,
        node,
        nodeLabel: node.label || node.id,
      };
    });
  });
  return {
    nodeById,
    edgeById,
    fieldById,
    impactByTarget,
  };
}

function getActiveStructureBundleForGraph() {
  if (state.selectedStructureBundle?.bundle_id) {
    return state.selectedStructureBundle;
  }
  if (!state.selectedStructureBundleId) {
    return null;
  }
  return (state.structure?.bundles || []).find((bundle) => bundle.bundle_id === state.selectedStructureBundleId) || null;
}

function resolveGraphReviewStatus(summary) {
  if (!summary) {
    return "";
  }
  if (summary.reviewRequired || summary.reviewRequiredCount || summary.contradictionCount) {
    return "review-required";
  }
  if (summary.impactCount || summary.pendingCount || summary.deferredCount) {
    return "warning";
  }
  if (summary.acceptedCount || summary.rejectedCount) {
    return "reviewed";
  }
  return "";
}

function buildStructureGraphReviewOverlay(bundle) {
  if (!bundle) {
    return null;
  }
  const context = buildStructureBundleContext(bundle);
  const patchGroups = groupStructureBundlePatches(bundle, context);
  const nodeSummaries = new Map();
  const fieldSummaries = new Map();
  const reviewUnitsByKey = new Map();

  function ensureNodeSummary(nodeId, nodeLabel = "", nodeKind = "") {
    if (!nodeId || !getNodeById(nodeId)) {
      return null;
    }
    if (!nodeSummaries.has(nodeId)) {
      nodeSummaries.set(nodeId, {
        nodeId,
        label: nodeLabel || getNodeById(nodeId)?.label || nodeId,
        nodeKind: nodeKind || getNodeById(nodeId)?.kind || "structure",
        reviewUnitCount: 0,
        pendingCount: 0,
        reviewRequiredCount: 0,
        contradictionCount: 0,
        impactCount: 0,
        acceptedCount: 0,
        deferredCount: 0,
        rejectedCount: 0,
        reviewUnitKeys: [],
        contradictionIds: new Set(),
        impactKeys: new Set(),
        status: "",
      });
    }
    return nodeSummaries.get(nodeId);
  }

  function applyFieldContradictionsToSummary(summary, contradictions = []) {
    (contradictions || []).forEach((contradiction) => {
      if (!contradiction?.id) {
        return;
      }
      summary.contradictionIds.add(contradiction.id);
      if (contradiction.review_required !== false) {
        summary.reviewRequired = true;
      }
      (contradiction.downstream_impacts || []).forEach((impact, index) => {
        summary.impactKeys.add(`${contradiction.id}:${index}:${impact}`);
      });
    });
  }

  patchGroups.forEach((group) => {
    const nodeSummary = ensureNodeSummary(group.key, group.label, group.nodeKind);
    if (!nodeSummary) {
      return;
    }
    nodeSummary.reviewUnitCount = group.fieldGroups.length;
    group.fieldGroups.forEach((fieldGroup) => {
      const fieldSummary = {
        nodeId: group.key,
        nodeLabel: group.label,
        fieldId: fieldGroup.key,
        label: fieldGroup.label,
        reviewUnitKey: fieldGroup.reviewUnitKey,
        pendingCount: fieldGroup.pendingIds.length,
        reviewRequired: fieldGroup.reviewRequired,
        contradictionCount: fieldGroup.relatedContradictions.length,
        impactCount: (fieldGroup.impactItems || []).filter((item) => item.severity !== "low").length,
        acceptedCount: fieldGroup.reviewCounts.accepted,
        deferredCount: fieldGroup.reviewCounts.deferred,
        rejectedCount: fieldGroup.reviewCounts.rejected,
        lowConfidenceOnly: fieldGroup.lowConfidenceOnly,
        highImpact: fieldGroup.highImpact,
        contradictionIds: new Set(),
        impactKeys: new Set(),
        status: "",
      };
      applyFieldContradictionsToSummary(fieldSummary, fieldGroup.relatedContradictions);
      (fieldGroup.impactItems || []).filter((item) => item.severity !== "low").forEach((item) => {
        fieldSummary.impactKeys.add(`${item.message || item.target_id || item.field_id || "impact"}|${item.severity || "low"}`);
      });
      fieldSummary.contradictionCount = fieldSummary.contradictionIds.size;
      fieldSummary.impactCount = fieldSummary.impactKeys.size;
      fieldSummary.status = resolveGraphReviewStatus(fieldSummary);
      fieldSummaries.set(fieldGroup.key, fieldSummary);
      if (fieldGroup.reviewUnitKey) {
        reviewUnitsByKey.set(fieldGroup.reviewUnitKey, fieldSummary);
      }

      nodeSummary.pendingCount += fieldGroup.pendingIds.length;
      nodeSummary.reviewRequiredCount += fieldGroup.reviewRequired ? 1 : 0;
      nodeSummary.acceptedCount += fieldGroup.reviewCounts.accepted;
      nodeSummary.deferredCount += fieldGroup.reviewCounts.deferred;
      nodeSummary.rejectedCount += fieldGroup.reviewCounts.rejected;
      nodeSummary.reviewUnitKeys.push(fieldGroup.reviewUnitKey);
      applyFieldContradictionsToSummary(nodeSummary, fieldGroup.relatedContradictions);
      (fieldGroup.impactItems || []).filter((item) => item.severity !== "low").forEach((item) => {
        nodeSummary.impactKeys.add(`${item.message || item.target_id || item.field_id || "impact"}|${item.severity || "low"}`);
      });
    });
  });

  (bundle.contradictions || []).forEach((contradiction) => {
    const targetMeta = getStructureTargetMeta(context, contradiction.target_id || "", contradiction.field_id || "");
    const nodeId = targetMeta.node?.id || contradiction.node_id || "";
    const nodeSummary = ensureNodeSummary(nodeId, targetMeta.nodeLabel, targetMeta.node?.kind || "structure");
    if (!nodeSummary) {
      return;
    }
    applyFieldContradictionsToSummary(nodeSummary, [contradiction]);
    if (targetMeta.field?.id) {
      const existingFieldSummary = fieldSummaries.get(targetMeta.field.id) || {
        nodeId,
        nodeLabel: targetMeta.nodeLabel,
        fieldId: targetMeta.field.id,
        label: targetMeta.field.name,
        reviewUnitKey: "",
        pendingCount: 0,
        reviewRequired: false,
        contradictionCount: 0,
        impactCount: 0,
        acceptedCount: 0,
        deferredCount: 0,
        rejectedCount: 0,
        lowConfidenceOnly: false,
        highImpact: false,
        contradictionIds: new Set(),
        impactKeys: new Set(),
        status: "",
      };
      applyFieldContradictionsToSummary(existingFieldSummary, [contradiction]);
      existingFieldSummary.contradictionCount = existingFieldSummary.contradictionIds.size;
      existingFieldSummary.impactCount = existingFieldSummary.impactKeys.size;
      existingFieldSummary.status = resolveGraphReviewStatus(existingFieldSummary);
      fieldSummaries.set(targetMeta.field.id, existingFieldSummary);
    }
  });

  nodeSummaries.forEach((summary) => {
    summary.contradictionCount = summary.contradictionIds.size;
    summary.impactCount = summary.impactKeys.size;
    summary.status = resolveGraphReviewStatus(summary);
  });

  return {
    bundleId: bundle.bundle_id,
    context,
    patchGroups,
    nodeSummaries,
    fieldSummaries,
    reviewUnitsByKey,
  };
}

function getGraphNodeReviewSummary(nodeId, overlay) {
  return overlay?.nodeSummaries?.get(nodeId) || null;
}

function getGraphFieldReviewSummary(fieldId, overlay) {
  if (!fieldId) {
    return null;
  }
  return overlay?.fieldSummaries?.get(fieldId) || null;
}

function summarizeGraphReviewSummary(summary) {
  if (!summary) {
    return "";
  }
  if (summary.reviewRequired || summary.reviewRequiredCount) {
    return "Review required";
  }
  if (summary.pendingCount) {
    return "Pending review";
  }
  if (summary.impactCount) {
    return "Downstream impact";
  }
  if (summary.acceptedCount) {
    return "Reviewed";
  }
  return "Open review";
}

function ensureStructureReviewUnitVisible(reviewUnitKey, bundle) {
  if (!reviewUnitKey || !bundle) {
    return;
  }
  const context = buildStructureBundleContext(bundle);
  const patchGroups = groupStructureBundlePatches(bundle, context);
  const filtered = filterStructurePatchGroups(patchGroups);
  const visibleUnits = collectVisibleStructureReviewUnits(filtered);
  if (visibleUnits.some((unit) => unit.reviewUnitKey === reviewUnitKey)) {
    return;
  }
  state.structureReviewUnitFilter = "all";
  state.structureShowLowConfidence = true;
  state.structureShowNonMaterial = true;
}

async function openGraphReviewTarget(options = {}) {
  if (options.nodeId) {
    state.selectionMode = "node";
    state.selectedNodeId = options.nodeId;
    state.selectedEdgeId = null;
  }
  await toggleReviewDrawer(true);
  const bundle = getActiveStructureBundleForGraph();
  if (!bundle) {
    render();
    setStatus("Review unavailable", "Run a structure scan or open a bundle before jumping from the graph into review.");
    return;
  }
  const overlay = buildStructureGraphReviewOverlay(bundle);
  const directUnit = options.reviewUnitKey
    ? overlay?.reviewUnitsByKey?.get(options.reviewUnitKey)
    : options.fieldId
      ? overlay?.fieldSummaries?.get(options.fieldId)
      : null;
  const nodeSummary = options.nodeId ? overlay?.nodeSummaries?.get(options.nodeId) : null;
  const target = directUnit || nodeSummary || null;
  const reviewUnitKey = target?.reviewUnitKey || target?.reviewUnitKeys?.find(Boolean) || "";
  if (reviewUnitKey) {
    ensureStructureReviewUnitVisible(reviewUnitKey, bundle);
    setActiveStructureReviewUnit(reviewUnitKey);
    return;
  }
  render();
  if (target) {
    setStatus("Review inbox opened", `${target.label || target.nodeLabel || options.nodeId} has structure review context in the inbox.`);
  } else {
    setStatus("Review inbox opened", "The active bundle is open. This node does not map to a visible review unit in the current bundle.");
  }
}

function getStructureTargetMeta(context, targetId, fieldId = "") {
  const resolvedFieldId = fieldId && context.fieldById[fieldId]
    ? fieldId
    : targetId && context.fieldById[targetId]
      ? targetId
      : "";
  if (resolvedFieldId) {
    const meta = context.fieldById[resolvedFieldId];
    return {
      node: meta.node,
      field: meta.field,
      nodeLabel: meta.nodeLabel,
    };
  }
  if (targetId && context.nodeById[targetId]) {
    return {
      node: context.nodeById[targetId],
      field: null,
      nodeLabel: context.nodeById[targetId].label || targetId,
    };
  }
  return { node: null, field: null, nodeLabel: targetId };
}

function formatStructureReference(context, value) {
  if (!value) {
    return "Not set";
  }
  if (context.fieldById[value]) {
    const meta = context.fieldById[value];
    return `${meta.nodeLabel}.${meta.field.name}`;
  }
  if (context.nodeById[value]) {
    return context.nodeById[value].label || value;
  }
  return String(value);
}

function buildStructureConnectionLabel(edge, context) {
  if (!edge) {
    return "Connection change";
  }
  const sourceLabel = context.nodeById[edge.source]?.label || edge.source || "Unknown source";
  const targetLabel = context.nodeById[edge.target]?.label || edge.target || "Unknown target";
  return `${sourceLabel} -> ${targetLabel}`;
}

function findStructureRelatedContradictions(bundle, targetIds) {
  const normalizedTargets = new Set((targetIds || []).filter(Boolean));
  if (!normalizedTargets.size) {
    return [];
  }
  return (bundle.contradictions || []).filter((contradiction) => {
    const relatedTargets = [
      contradiction.field_id,
      contradiction.target_id,
      contradiction.node_id,
      ...(contradiction.affected_refs || []),
    ].filter(Boolean);
    return relatedTargets.some((targetId) => normalizedTargets.has(targetId));
  });
}

function buildStructureReviewEventList(records) {
  const events = [];
  (records || []).forEach((record) => {
    const history = (record.review_history || []).length
      ? (record.review_history || [])
      : record.reviewed_at
        ? [{
          decision: record.review_state || "pending",
          reviewed_by: record.reviewed_by || "user",
          reviewed_at: record.reviewed_at,
          note: record.review_note || "",
        }]
        : [];
    history.forEach((event) => {
      events.push({
        decision: event.decision || record.review_state || "pending",
        reviewed_by: event.reviewed_by || record.reviewed_by || "user",
        reviewed_at: event.reviewed_at || record.reviewed_at || "",
        note: event.note || "",
        relatedPatchCount: (event.related_patch_ids || []).length,
      });
    });
  });
  return events.sort((left, right) => String(right.reviewed_at || "").localeCompare(String(left.reviewed_at || "")));
}

function renderStructureReviewAudit(records, options = {}) {
  const events = buildStructureReviewEventList(records);
  if (!events.length) {
    return "";
  }
  const decisionLabelFn = options.decisionLabelFn || humanizeStructureDecision;
  return `
    <div class="structure-review-audit">
      <div class="group-heading">${escapeHtml(options.title || "Review notes / audit trail")}</div>
      <div class="structure-review-audit-list">
        ${events.slice(0, 3).map((event) => `
          <div class="structure-review-audit-entry">
            <strong>${escapeHtml(`${decisionLabelFn(event.decision)} by ${event.reviewed_by || "user"}`)}</strong>
            <div class="hint">${escapeHtml(formatStructureTimestamp(event.reviewed_at || ""))}</div>
            ${event.relatedPatchCount ? `<div class="hint">${escapeHtml(`${formatValue(event.relatedPatchCount)} related patch${event.relatedPatchCount === 1 ? "" : "es"}`)}</div>` : ""}
            ${event.note ? `<div>${escapeHtml(event.note)}</div>` : ""}
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function summarizeStructureEvidence(entries, context) {
  const items = Object.entries(entries || {}).filter(([, value]) => value !== "" && value !== null && value !== false);
  if (!items.length) {
    return "No explicit evidence captured";
  }
  return items
    .slice(0, 2)
    .map(([key, value]) => `${humanizeStructureEvidenceKey(key)}: ${formatStructureEvidenceValue(context, key, value)}`)
    .join(" | ");
}

function summarizeStructureFieldGroupCanonicalState(fieldGroup, context) {
  const contradiction = fieldGroup.relatedContradictions[0];
  if (contradiction?.existing_belief) {
    return summarizeStructureEvidence(contradiction.existing_belief, context);
  }
  const patch = fieldGroup.patches[0];
  const payload = patch?.payload || {};
  if (patch?.type === "add_field" || patch?.type === "add_node" || patch?.type === "add_edge") {
    return "Not present in canonical memory";
  }
  if (patch?.type === "change_binding" || patch?.type === "add_binding") {
    return `Binding: ${formatStructureReference(context, payload.previous_binding || "")}`;
  }
  if (patch?.type === "remove_field" || patch?.type === "remove_node" || patch?.type === "remove_edge") {
    return "Present in canonical memory";
  }
  if (patch?.type === "confidence_change") {
    return `Confidence: ${payload.previous_confidence || patch.confidence || "unknown"}`;
  }
  return "Canonical state unchanged";
}

function summarizeStructureFieldGroupProposedState(fieldGroup, context) {
  const contradiction = fieldGroup.relatedContradictions[0];
  if (contradiction?.new_evidence) {
    return summarizeStructureEvidence(contradiction.new_evidence, context);
  }
  const patch = fieldGroup.patches[0];
  const payload = patch?.payload || {};
  if (patch?.type === "add_field" || patch?.type === "remove_field") {
    return payload.field?.name || fieldGroup.label || "Field update";
  }
  if (patch?.type === "add_node" || patch?.type === "remove_node") {
    return payload.node?.label || patch.target_id || "Node update";
  }
  if (patch?.type === "change_binding" || patch?.type === "add_binding") {
    return `Binding: ${formatStructureReference(context, payload.new_binding || payload.primary_binding || "")}`;
  }
  if (patch?.type === "confidence_change") {
    return `Confidence: ${payload.new_confidence || payload.confidence || patch.confidence || "unknown"}`;
  }
  if (patch?.type === "add_edge" || patch?.type === "remove_edge") {
    return buildStructureConnectionLabel(payload.edge || context.edgeById[patch.edge_id || patch.target_id || ""], context);
  }
  return summarizeStructurePatch(patch, context);
}

function summarizeStructureFieldGroupReason(fieldGroup) {
  if (fieldGroup.relatedContradictions.some((item) => item.review_required !== false)) {
    return "Contradiction needs an explicit resolution";
  }
  if (fieldGroup.highImpact) {
    return "Consumer-facing breakage is attached to this unit";
  }
  if (fieldGroup.lowConfidenceOnly) {
    return "Only low-confidence evidence supports this suggestion";
  }
  if (fieldGroup.pendingIds.length) {
    return "Pending patch review";
  }
  return "Reviewed";
}

function summarizeStructureFieldGroupState(fieldGroup) {
  if (fieldGroup.reviewRequired) {
    return "Review required";
  }
  if (fieldGroup.pendingIds.length) {
    return "Pending";
  }
  if (fieldGroup.reviewCounts.accepted) {
    return "Accepted";
  }
  if (fieldGroup.reviewCounts.deferred) {
    return "Deferred";
  }
  if (fieldGroup.reviewCounts.rejected) {
    return "Rejected";
  }
  return "No action";
}

function structureFieldGroupIsMaterial(fieldGroup) {
  if (fieldGroup.relatedContradictions.length || fieldGroup.highImpact) {
    return true;
  }
  if ((fieldGroup.patches || []).some((patch) => ["change_binding", "add_binding", "remove_binding", "remove_field", "remove_node", "remove_edge"].includes(patch.type))) {
    return true;
  }
  if ((fieldGroup.patches || []).some((patch) => patch.review_state !== "pending" && patch.type !== "confidence_change")) {
    return true;
  }
  return !fieldGroup.lowConfidenceOnly && (fieldGroup.patches || []).some((patch) => patch.type !== "confidence_change");
}

function describeStructureFieldGroupMateriality(fieldGroup) {
  if (fieldGroup.relatedContradictions.length) {
    return "Contradiction or conflicting evidence keeps this review unit material.";
  }
  if (fieldGroup.highImpact) {
    return "Downstream contract or UI impact keeps this unit in the primary review queue.";
  }
  if ((fieldGroup.patches || []).some((patch) => ["change_binding", "add_binding", "remove_binding"].includes(patch.type))) {
    return "Binding changes are always treated as material.";
  }
  if ((fieldGroup.patches || []).some((patch) => ["remove_field", "remove_node", "remove_edge"].includes(patch.type))) {
    return "Destructive structure changes stay visible as material review units.";
  }
  if ((fieldGroup.patches || []).every((patch) => patch.type === "confidence_change")) {
    return "Confidence-only churn with no contradictions or downstream impact is hidden by default.";
  }
  if (fieldGroup.lowConfidenceOnly) {
    return "Low-confidence, no-impact suggestions are hidden by default unless explicitly requested.";
  }
  return "No downstream or contradiction signal promoted this unit into the primary review queue.";
}

function structureFieldGroupMatchesFilter(fieldGroup, filterKey) {
  if (filterKey === "all") {
    return true;
  }
  if (filterKey === "review_required") {
    return fieldGroup.reviewRequired;
  }
  if (filterKey === "contradictions") {
    return Boolean(fieldGroup.relatedContradictions.length);
  }
  if (filterKey === "high_impact") {
    return fieldGroup.highImpact;
  }
  if (filterKey === "pending") {
    return Boolean(fieldGroup.pendingIds.length);
  }
  if (filterKey === "deferred") {
    return fieldGroup.reviewCounts.deferred > 0;
  }
  if (filterKey === "reviewed") {
    return !fieldGroup.pendingIds.length && (
      fieldGroup.reviewCounts.accepted > 0
      || fieldGroup.reviewCounts.rejected > 0
      || fieldGroup.reviewCounts.deferred > 0
      || fieldGroup.relatedContradictions.some((item) => item.review_required === false)
    );
  }
  return true;
}

function buildStructureReviewUnitCounts(patchGroups) {
  const fieldGroups = patchGroups.flatMap((group) => group.fieldGroups || []);
  const counts = {
    review_required: 0,
    contradictions: 0,
    high_impact: 0,
    pending: 0,
    deferred: 0,
    reviewed: 0,
    all: fieldGroups.length,
  };
  fieldGroups.forEach((fieldGroup) => {
    if (structureFieldGroupMatchesFilter(fieldGroup, "review_required")) counts.review_required += 1;
    if (structureFieldGroupMatchesFilter(fieldGroup, "contradictions")) counts.contradictions += 1;
    if (structureFieldGroupMatchesFilter(fieldGroup, "high_impact")) counts.high_impact += 1;
    if (structureFieldGroupMatchesFilter(fieldGroup, "pending")) counts.pending += 1;
    if (structureFieldGroupMatchesFilter(fieldGroup, "deferred")) counts.deferred += 1;
    if (structureFieldGroupMatchesFilter(fieldGroup, "reviewed")) counts.reviewed += 1;
  });
  return counts;
}

function renderStructureReviewUnitFilters(patchGroups) {
  const counts = buildStructureReviewUnitCounts(patchGroups);
  const items = [
    { key: "review_required", label: "Review required" },
    { key: "contradictions", label: "Contradictions" },
    { key: "high_impact", label: "High impact" },
    { key: "pending", label: "Pending" },
    { key: "deferred", label: "Deferred" },
    { key: "reviewed", label: "Reviewed" },
    { key: "all", label: "All units" },
  ];
  return `
    <div class="structure-review-filter-bar">
      ${items.map((item) => `
        <button
          class="ghost-button structure-queue-chip ${state.structureReviewUnitFilter === item.key ? "active" : ""}"
          type="button"
          data-structure-action="set-review-filter"
          data-review-filter="${escapeHtml(item.key)}"
        >
          <span>${escapeHtml(item.label)}</span>
          <span class="status-pill">${formatValue(counts[item.key] || 0)}</span>
        </button>
      `).join("")}
    </div>
  `;
}

function filterStructurePatchGroups(patchGroups) {
  const filterKey = state.structureReviewUnitFilter || "review_required";
  const mappedGroups = patchGroups
    .map((group) => {
      const filteredFieldGroups = (group.fieldGroups || []).filter((fieldGroup) => structureFieldGroupMatchesFilter(fieldGroup, filterKey));
      const materialFieldGroups = state.structureShowNonMaterial
        ? filteredFieldGroups
        : filteredFieldGroups.filter((fieldGroup) => fieldGroup.isMaterial);
      const matchingVisible = materialFieldGroups.filter((fieldGroup) => !fieldGroup.lowConfidenceOnly);
      const matchingLow = materialFieldGroups.filter((fieldGroup) => fieldGroup.lowConfidenceOnly);
      const displayedFieldGroups = state.structureShowLowConfidence ? materialFieldGroups : matchingVisible;
      const hiddenNonMaterialFieldGroups = state.structureShowNonMaterial
        ? []
        : filteredFieldGroups.filter((fieldGroup) => !fieldGroup.isMaterial);
      return {
        ...group,
        filteredVisibleFieldGroups: displayedFieldGroups,
        filteredLowConfidenceFieldGroups: state.structureShowLowConfidence ? [] : matchingLow,
        visiblePendingIds: displayedFieldGroups.flatMap((fieldGroup) => fieldGroup.pendingIds),
        hiddenLowConfidencePendingIds: state.structureShowLowConfidence ? [] : matchingLow.flatMap((fieldGroup) => fieldGroup.pendingIds),
        hiddenNonMaterialPendingIds: hiddenNonMaterialFieldGroups.flatMap((fieldGroup) => fieldGroup.pendingIds),
      };
    });
  const groups = mappedGroups.filter((group) => group.filteredVisibleFieldGroups.length || group.filteredLowConfidenceFieldGroups.length);
  return {
    groups,
    visiblePendingIds: groups.flatMap((group) => group.visiblePendingIds),
    hiddenNonMaterialPendingIds: mappedGroups.flatMap((group) => group.hiddenNonMaterialPendingIds || []),
  };
}

function collectVisibleStructureReviewUnits(filteredPatchGroups) {
  return (filteredPatchGroups?.groups || []).flatMap((group) => group.filteredVisibleFieldGroups || []);
}

function syncActiveStructureReviewUnit(filteredPatchGroups) {
  const visibleUnits = collectVisibleStructureReviewUnits(filteredPatchGroups);
  if (!visibleUnits.length) {
    state.structureActiveReviewUnitKey = "";
    return "";
  }
  if (visibleUnits.some((unit) => unit.reviewUnitKey === state.structureActiveReviewUnitKey)) {
    return state.structureActiveReviewUnitKey;
  }
  state.structureActiveReviewUnitKey = visibleUnits[0].reviewUnitKey || "";
  return state.structureActiveReviewUnitKey;
}

function scrollActiveStructureReviewUnitIntoView() {
  requestAnimationFrame(() => {
    const cards = Array.from(document.querySelectorAll("[data-structure-review-unit-key]"));
    const activeCard = cards.find((element) => element instanceof HTMLElement && element.dataset.structureReviewUnitKey === state.structureActiveReviewUnitKey);
    if (!(activeCard instanceof HTMLElement)) {
      return;
    }
    activeCard.focus({ preventScroll: true });
    activeCard.scrollIntoView({ block: "nearest", behavior: "smooth" });
  });
}

function setActiveStructureReviewUnit(reviewUnitKey, options = {}) {
  if (!reviewUnitKey) {
    return;
  }
  if (state.structureActiveReviewUnitKey === reviewUnitKey) {
    if (options.scroll !== false) {
      scrollActiveStructureReviewUnitIntoView();
    }
    return;
  }
  state.structureActiveReviewUnitKey = reviewUnitKey;
  render();
  if (options.scroll !== false) {
    scrollActiveStructureReviewUnitIntoView();
  }
}

function groupStructureBundlePatches(bundle, context) {
  const groups = new Map();
  (bundle.patches || []).forEach((patch) => {
    const patchMeta = resolveStructurePatchGrouping(patch, context);
    if (!groups.has(patchMeta.nodeKey)) {
      groups.set(patchMeta.nodeKey, {
        key: patchMeta.nodeKey,
        label: patchMeta.nodeLabel,
        nodeKind: patchMeta.nodeKind,
        fieldGroups: new Map(),
      });
    }
    const nodeGroup = groups.get(patchMeta.nodeKey);
    if (!nodeGroup.fieldGroups.has(patchMeta.fieldKey)) {
      nodeGroup.fieldGroups.set(patchMeta.fieldKey, {
        key: patchMeta.fieldKey,
        label: patchMeta.fieldLabel,
        nodeLabel: patchMeta.nodeLabel,
        nodeKind: patchMeta.nodeKind,
        patches: [],
      });
    }
    nodeGroup.fieldGroups.get(patchMeta.fieldKey).patches.push(patch);
  });
  return Array.from(groups.values())
    .map((group) => ({
      ...group,
      fieldGroups: Array.from(group.fieldGroups.values())
        .map((fieldGroup) => {
          const pendingIds = fieldGroup.patches.filter((patch) => patch.review_state === "pending").map((patch) => patch.id);
          const lowConfidenceOnly = fieldGroup.patches.every((patch) => patch.confidence === "low");
          const reviewCounts = {
            accepted: fieldGroup.patches.filter((patch) => patch.review_state === "accepted").length,
            rejected: fieldGroup.patches.filter((patch) => patch.review_state === "rejected").length,
            deferred: fieldGroup.patches.filter((patch) => patch.review_state === "deferred").length,
          };
          const targetIds = Array.from(new Set(fieldGroup.patches.flatMap((patch) => [patch.field_id, patch.target_id, patch.node_id]).filter(Boolean)));
          const relatedContradictions = findStructureRelatedContradictions(bundle, targetIds);
          const impactItems = collectStructureImpactItemsForTargets(
            bundle,
            [
              ...targetIds,
              ...relatedContradictions.flatMap((item) => [item.field_id, item.target_id, item.node_id]),
            ],
            context,
            { includeMinor: true }
          );
          return {
            ...fieldGroup,
            patches: fieldGroup.patches.slice().sort((left, right) => {
              const leftPending = left.review_state === "pending" ? 0 : 1;
              const rightPending = right.review_state === "pending" ? 0 : 1;
              return leftPending - rightPending || String(left.type || "").localeCompare(String(right.type || ""));
            }),
            reviewUnitKey: `${group.key}::${fieldGroup.key}`,
            pendingIds,
            lowConfidenceOnly,
            reviewCounts,
            relatedContradictions,
            impactItems,
            reviewRequired: Boolean(pendingIds.length) || relatedContradictions.some((item) => item.review_required !== false),
            highImpact: impactItems.some((item) => item.severity !== "low"),
            isMaterial: false,
            materialityReason: "",
          };
        })
        .map((fieldGroup) => ({
          ...fieldGroup,
          isMaterial: structureFieldGroupIsMaterial(fieldGroup),
          materialityReason: describeStructureFieldGroupMateriality(fieldGroup),
        }))
        .sort((left, right) => {
          const leftPriority = left.reviewRequired ? 0 : left.highImpact ? 1 : left.pendingIds.length ? 2 : left.lowConfidenceOnly ? 3 : 4;
          const rightPriority = right.reviewRequired ? 0 : right.highImpact ? 1 : right.pendingIds.length ? 2 : right.lowConfidenceOnly ? 3 : 4;
          return leftPriority - rightPriority || String(left.label || "").localeCompare(String(right.label || ""));
        }),
    }))
    .map((group) => {
      const visibleFieldGroups = group.fieldGroups.filter((fieldGroup) => !fieldGroup.lowConfidenceOnly);
      const lowConfidenceFieldGroups = group.fieldGroups.filter((fieldGroup) => fieldGroup.lowConfidenceOnly);
      return {
        ...group,
        visibleFieldGroups,
        lowConfidenceFieldGroups,
        visiblePendingIds: visibleFieldGroups.flatMap((fieldGroup) => fieldGroup.pendingIds),
        hiddenLowConfidencePendingIds: lowConfidenceFieldGroups.flatMap((fieldGroup) => fieldGroup.pendingIds),
        pendingCount: group.fieldGroups.reduce((total, fieldGroup) => total + fieldGroup.pendingIds.length, 0),
      };
    })
    .sort((left, right) => {
      const leftPriority = left.visiblePendingIds.length ? 0 : left.hiddenLowConfidencePendingIds.length ? 1 : 2;
      const rightPriority = right.visiblePendingIds.length ? 0 : right.hiddenLowConfidencePendingIds.length ? 1 : 2;
      return leftPriority - rightPriority || String(left.label || "").localeCompare(String(right.label || ""));
    });
}

function resolveStructurePatchGrouping(patch, context) {
  const targetMeta = getStructureTargetMeta(context, patch.target_id || "", patch.field_id || "");
  const edge = patch.payload?.edge || context.edgeById[patch.edge_id || patch.target_id || ""];
  const node = targetMeta.node
    || (patch.node_id ? context.nodeById[patch.node_id] : null)
    || (edge?.source ? context.nodeById[edge.source] : null)
    || patch.payload?.node
    || null;
  const nodeKey = node?.id || edge?.source || patch.node_id || patch.target_id || patch.id;
  const nodeLabel = node?.label || node?.id || (edge ? buildStructureConnectionLabel(edge, context) : patch.target_id || patch.id);
  const nodeKind = node?.kind || (edge ? "connection" : "structure");
  if (targetMeta.field) {
    return {
      nodeKey,
      nodeLabel,
      nodeKind,
      fieldKey: targetMeta.field.id,
      fieldLabel: targetMeta.field.name,
    };
  }
  if (patch.payload?.field?.name) {
    return {
      nodeKey,
      nodeLabel,
      nodeKind,
      fieldKey: patch.field_id || `field:${nodeKey}:${patch.payload.field.name}`,
      fieldLabel: patch.payload.field.name,
    };
  }
  if (edge) {
    return {
      nodeKey,
      nodeLabel,
      nodeKind,
      fieldKey: edge.id || patch.edge_id || patch.id,
      fieldLabel: buildStructureConnectionLabel(edge, context),
    };
  }
  return {
    nodeKey,
    nodeLabel,
    nodeKind,
    fieldKey: patch.target_id || patch.id,
    fieldLabel: patch.payload?.node?.label || targetMeta.nodeLabel || patch.target_id || patch.id,
  };
}

function renderStructurePatchNodeGroup(bundleId, group, context) {
  const visibleFieldGroups = group.filteredVisibleFieldGroups || group.visibleFieldGroups;
  const lowConfidenceFieldGroups = group.filteredLowConfidenceFieldGroups || [];
  const shouldOpen = Boolean(group.visiblePendingIds.length || visibleFieldGroups.some((fieldGroup) => fieldGroup.reviewRequired));
  return `
    <details class="detail-accordion structure-node-group" ${shouldOpen ? "open" : ""}>
      <summary>
        <div class="structure-node-summary">
          <div>
            <div class="column-main">${escapeHtml(group.label || group.key || "Structure changes")}</div>
            <div class="column-meta">${escapeHtml(group.nodeKind || "structure")} | ${formatValue(visibleFieldGroups.length + lowConfidenceFieldGroups.length)} review unit${visibleFieldGroups.length + lowConfidenceFieldGroups.length === 1 ? "" : "s"}</div>
          </div>
          <div class="row-actions">
            ${group.hiddenLowConfidencePendingIds.length ? `<span class="tag-chip">${escapeHtml(`${formatValue(group.hiddenLowConfidencePendingIds.length)} low-confidence hidden`)}</span>` : ""}
            ${group.hiddenNonMaterialPendingIds?.length && !state.structureShowNonMaterial ? `<span class="tag-chip muted">${escapeHtml(`${formatValue(group.hiddenNonMaterialPendingIds.length)} non-material hidden`)}</span>` : ""}
            ${group.visiblePendingIds.length ? `<span class="status-pill warning">${escapeHtml(`${formatValue(group.visiblePendingIds.length)} pending`)}</span>` : ""}
          </div>
        </div>
      </summary>
      <div class="detail-accordion-content">
        <div class="section-actions structure-inbox-actions">
          <span class="hint">${formatValue(visibleFieldGroups.length + lowConfidenceFieldGroups.length)} field group${visibleFieldGroups.length + lowConfidenceFieldGroups.length === 1 ? "" : "s"} in this node</span>
          ${renderStructureReviewActions(bundleId, group.visiblePendingIds, {
            acceptLabel: "Accept node",
            deferLabel: "Defer node",
            rejectLabel: "Reject node",
          })}
        </div>
        <div class="column-list">
          ${visibleFieldGroups.map((fieldGroup) => renderStructurePatchFieldGroup(bundleId, fieldGroup, context)).join("")}
        </div>
        ${lowConfidenceFieldGroups.length ? `
          <details class="detail-accordion structure-low-confidence">
            <summary>Low-confidence suggestions (${formatValue(lowConfidenceFieldGroups.length)})</summary>
            <div class="detail-accordion-content">
              <div class="section-actions structure-inbox-actions">
                <span class="hint">Collapsed by default to keep noisy suggestions out of the main review path.</span>
                ${renderStructureReviewActions(bundleId, group.hiddenLowConfidencePendingIds, {
                  acceptLabel: "Accept low-confidence",
                  deferLabel: "Defer low-confidence",
                  rejectLabel: "Reject low-confidence",
                })}
              </div>
              <div class="column-list">
                ${lowConfidenceFieldGroups.map((fieldGroup) => renderStructurePatchFieldGroup(bundleId, fieldGroup, context)).join("")}
              </div>
            </div>
          </details>
        ` : ""}
      </div>
    </details>
  `;
}

function renderStructurePatchFieldGroup(bundleId, fieldGroup, context) {
  const visibleImpacts = (fieldGroup.impactItems || []).filter((item) => state.structureShowMinorImpacts || item.severity !== "low");
  const unitClasses = [
    "structure-review-unit-card",
    fieldGroup.reviewRequired ? "review-required" : "",
    fieldGroup.highImpact ? "high-impact" : "",
    fieldGroup.lowConfidenceOnly ? "low-confidence-only" : "",
    fieldGroup.reviewUnitKey === state.structureActiveReviewUnitKey ? "active-review-unit" : "",
  ].filter(Boolean).join(" ");
  return `
    <div
      class="${escapeHtml(unitClasses)}"
      data-structure-review-unit-key="${escapeHtml(fieldGroup.reviewUnitKey || "")}"
      tabindex="0"
      aria-current="${fieldGroup.reviewUnitKey === state.structureActiveReviewUnitKey ? "true" : "false"}"
    >
      <div class="column-head">
        <div>
          <div class="column-main">${escapeHtml(fieldGroup.label || fieldGroup.key || "Field change")}</div>
          <div class="column-meta">${escapeHtml(fieldGroup.nodeLabel || fieldGroup.nodeKind || "structure")} | ${formatValue(fieldGroup.patches.length)} patch${fieldGroup.patches.length === 1 ? "" : "es"} on this field</div>
        </div>
        <div class="row-actions">
          ${!fieldGroup.isMaterial ? '<span class="tag-chip muted">non-material</span>' : ""}
          ${fieldGroup.lowConfidenceOnly ? '<span class="tag-chip">low-confidence</span>' : ""}
          ${fieldGroup.relatedContradictions.length ? `<span class="status-pill broken">${escapeHtml(`${formatValue(fieldGroup.relatedContradictions.length)} contradiction${fieldGroup.relatedContradictions.length === 1 ? "" : "s"}`)}</span>` : ""}
          ${visibleImpacts.length ? `<span class="status-pill warning">${escapeHtml(`${formatValue(visibleImpacts.length)} impact${visibleImpacts.length === 1 ? "" : "s"}`)}</span>` : ""}
          ${renderStructureReviewActions(bundleId, fieldGroup.pendingIds, {
            acceptLabel: "Accept field",
            deferLabel: "Defer field",
            rejectLabel: "Reject field",
          })}
        </div>
      </div>
      <div class="structure-review-unit-summary">
        <span class="status-pill ${escapeHtml(fieldGroup.reviewRequired ? "broken" : fieldGroup.reviewCounts.accepted ? "healthy" : fieldGroup.reviewCounts.deferred ? "warning" : "")}">${escapeHtml(summarizeStructureFieldGroupState(fieldGroup))}</span>
        <span class="tag-chip">${escapeHtml(summarizeStructureFieldGroupReason(fieldGroup))}</span>
      </div>
      ${!fieldGroup.isMaterial ? `<div class="column-meta">${escapeHtml(fieldGroup.materialityReason || "Hidden by default unless non-material changes are shown.")}</div>` : ""}
      <div class="structure-review-unit-body">
        <div class="structure-review-unit-grid">
          <div class="meta-card"><strong>Canonical now</strong>${escapeHtml(summarizeStructureFieldGroupCanonicalState(fieldGroup, context))}</div>
          <div class="meta-card"><strong>Proposed</strong>${escapeHtml(summarizeStructureFieldGroupProposedState(fieldGroup, context))}</div>
          <div class="meta-card"><strong>Why review</strong>${escapeHtml(summarizeStructureFieldGroupReason(fieldGroup))}</div>
          <div class="meta-card"><strong>Review state</strong>${escapeHtml(summarizeStructureFieldGroupState(fieldGroup))}</div>
        </div>
        ${visibleImpacts.length ? `
          <div class="structure-review-unit-section">
            <div class="group-heading">Consumer-facing impact</div>
            <ul class="warning-list compact-list">
              ${visibleImpacts.slice(0, 3).map((item) => `<li>${escapeHtml(item.message || item.target_id || "")}</li>`).join("")}
            </ul>
          </div>
        ` : ""}
        ${fieldGroup.relatedContradictions.length ? `
          <div class="structure-review-unit-section">
            <div class="group-heading">Related contradictions</div>
            <ul class="warning-list compact-list">
              ${fieldGroup.relatedContradictions.slice(0, 3).map((item) => `<li>${escapeHtml(item.message || summarizeStructureContradictionTitle(item, context))}</li>`).join("")}
            </ul>
          </div>
        ` : ""}
        <details class="detail-accordion">
          <summary>Patch details (${formatValue(fieldGroup.patches.length)})</summary>
          <div class="detail-accordion-content">
            <div class="column-list structure-patch-list">
              ${fieldGroup.patches.map((patch) => renderStructurePatchCard(bundleId, patch, context)).join("")}
            </div>
          </div>
        </details>
        ${renderStructureReviewAudit([...fieldGroup.patches, ...fieldGroup.relatedContradictions], { title: "Review notes / audit trail" })}
      </div>
    </div>
  `;
}

function renderStructurePatchCard(bundleId, patch, context) {
  const reviewState = patch.review_state || "pending";
  const impact = context.impactByTarget.get(patch.target_id || patch.field_id || patch.node_id || "");
  return `
    <div class="structure-patch-card ${escapeHtml(reviewState)}">
      <div class="column-head">
        <div class="column-main">${escapeHtml(humanizeStructurePatchType(patch.type))}</div>
        <div class="row-actions">
          <span class="tag-chip ${escapeHtml(patch.confidence === "low" ? "low" : "")}">${escapeHtml(`${patch.confidence || "medium"} confidence`)}</span>
          <span class="status-pill ${escapeHtml(reviewState === "accepted" ? "healthy" : reviewState === "rejected" ? "broken" : reviewState === "deferred" ? "warning" : "")}">${escapeHtml(reviewState)}</span>
        </div>
      </div>
      <div class="column-meta">${escapeHtml(summarizeStructurePatch(patch, context))}</div>
      ${impact ? `<div class="structure-inline-impact subtle">${escapeHtml(impact)}</div>` : ""}
      ${(patch.evidence || []).length ? `
        <div class="chip-row">
          ${(patch.evidence || []).map((entry) => `<span class="tag-chip">${escapeHtml(entry)}</span>`).join("")}
        </div>
      ` : ""}
      ${patch.review_note ? `<div class="column-meta">${escapeHtml(`Note: ${patch.review_note}`)}</div>` : ""}
      ${renderStructureReviewActions(bundleId, [patch.id], {
        acceptLabel: "Accept",
        deferLabel: "Defer",
        rejectLabel: "Reject",
        currentDecision: reviewState,
      })}
    </div>
  `;
}

function renderStructureReviewActions(bundleId, patchIds, labels = {}) {
  const normalizedIds = Array.from(new Set((patchIds || []).filter(Boolean)));
  if (!normalizedIds.length) {
    return "";
  }
  const singlePatchId = normalizedIds.length === 1 ? normalizedIds[0] : "";
  const action = singlePatchId ? "review-patch" : "review-patch-batch";
  const sharedAttrs = singlePatchId
    ? `data-patch-id="${escapeHtml(singlePatchId)}"`
    : `data-patch-ids="${escapeHtml(normalizedIds.join(","))}"`;
  const disableIfCurrent = (decision) => singlePatchId && labels.currentDecision === decision ? "disabled" : "";
  return `
    <div class="row-actions">
      <button class="ghost-button" type="button" data-structure-action="${action}" data-bundle-id="${escapeHtml(bundleId)}" ${sharedAttrs} data-decision="accepted" ${disableIfCurrent("accepted")}>${escapeHtml(labels.acceptLabel || "Accept")}</button>
      <button class="ghost-button" type="button" data-structure-action="${action}" data-bundle-id="${escapeHtml(bundleId)}" ${sharedAttrs} data-decision="deferred" ${disableIfCurrent("deferred")}>${escapeHtml(labels.deferLabel || "Defer")}</button>
      <button class="ghost-button danger-soft" type="button" data-structure-action="${action}" data-bundle-id="${escapeHtml(bundleId)}" ${sharedAttrs} data-decision="rejected" ${disableIfCurrent("rejected")}>${escapeHtml(labels.rejectLabel || "Reject")}</button>
    </div>
  `;
}

function findStructureRelatedPatches(bundle, contradiction) {
  const contradictionTarget = contradiction.field_id || contradiction.target_id || "";
  return (bundle.patches || []).filter((patch) => (
    contradictionTarget
    && [patch.field_id, patch.target_id, patch.node_id].includes(contradictionTarget)
  ) || (
    contradiction.target_id
    && [patch.field_id, patch.target_id, patch.node_id].includes(contradiction.target_id)
  ));
}

function summarizeStructurePatchStates(patches) {
  const counts = new Map();
  patches.forEach((patch) => {
    const key = patch.review_state || "pending";
    counts.set(key, (counts.get(key) || 0) + 1);
  });
  return ["pending", "accepted", "deferred", "rejected"]
    .filter((key) => counts.has(key))
    .map((key) => ({ key, count: counts.get(key) || 0 }));
}

function renderStructurePatchStateSummary(summary) {
  if (!summary.length) {
    return "";
  }
  return `
    <div class="chip-row">
      ${summary.map((item) => `<span class="status-pill ${escapeHtml(item.key === "accepted" ? "healthy" : item.key === "rejected" ? "broken" : item.key === "deferred" ? "warning" : "")}">${escapeHtml(`${item.key}: ${formatValue(item.count)}`)}</span>`).join("")}
    </div>
  `;
}

function humanizeStructurePatchType(type) {
  return String(type || "patch")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function summarizeStructurePatch(patch, context) {
  const payload = patch.payload || {};
  if (patch.type === "add_binding" || patch.type === "change_binding") {
    const previous = payload.previous_binding || "unbound";
    const next = payload.new_binding || payload.primary_binding || "unbound";
    return `${formatStructureReference(context, previous)} -> ${formatStructureReference(context, next)}`;
  }
  if (patch.type === "add_field" || patch.type === "remove_field") {
    return payload.field?.name || formatStructureReference(context, patch.field_id || patch.target_id || "");
  }
  if (patch.type === "add_node" || patch.type === "remove_node") {
    const node = payload.node || {};
    const kind = node.kind ? ` (${node.kind})` : "";
    return `${node.label || patch.target_id || ""}${kind}`;
  }
  if (patch.type === "add_edge" || patch.type === "remove_edge") {
    return buildStructureConnectionLabel(payload.edge || context.edgeById[patch.edge_id || patch.target_id || ""], context);
  }
  if (patch.type === "confidence_change") {
    return `${payload.previous_confidence || "unknown"} -> ${payload.new_confidence || payload.confidence || "unknown"}`;
  }
  return payload.message || payload.reason || patch.target_id || patch.type;
}
