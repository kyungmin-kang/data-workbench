// Runtime support, parsing, formatting, and authoring helpers extracted from app.js

function setStatus(title, subtitle) {
  statusText.textContent = title;
  substatusText.textContent = subtitle;
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function truncateMiddle(value, maxLength = 84) {
  const text = String(value || "");
  if (text.length <= maxLength) {
    return text;
  }
  const lead = Math.max(12, Math.floor((maxLength - 1) / 2));
  const tail = Math.max(8, maxLength - lead - 1);
  return `${text.slice(0, lead)}…${text.slice(-tail)}`;
}

function ensureToastHost() {
  if (toastHost) {
    return toastHost;
  }
  toastHost = document.createElement("div");
  toastHost.className = "toast-stack";
  document.body.appendChild(toastHost);
  return toastHost;
}

async function primeCompletionNotifications() {
  if (!("Notification" in window) || Notification.permission !== "default") {
    return;
  }
  try {
    await Notification.requestPermission();
  } catch (error) {
    // Ignore browser notification permission failures and keep the in-app toast flow.
  }
}

function showToastNotification(title, subtitle, options = {}) {
  const host = ensureToastHost();
  const toast = document.createElement("div");
  toast.className = "toast-card";
  toast.innerHTML = `
    <div class="toast-title">${escapeHtml(options.emoji ? `${options.emoji} ${title}` : title)}</div>
    <div class="toast-subtitle">${escapeHtml(subtitle || "")}</div>
  `;
  host.appendChild(toast);
  window.setTimeout(() => {
    toast.classList.add("exiting");
    window.setTimeout(() => toast.remove(), 220);
  }, options.durationMs || 4200);
}

function notifyTaskCompletion(title, subtitle, options = {}) {
  showToastNotification(title, subtitle, options);
  if ("Notification" in window && Notification.permission === "granted" && document.hidden) {
    try {
      const emojiPrefix = options.emoji ? `${options.emoji} ` : "";
      const notification = new Notification(`${emojiPrefix}${title}`, {
        body: subtitle || "",
        tag: options.tag || title,
      });
      window.setTimeout(() => notification.close(), options.durationMs || 4200);
    } catch (error) {
      // Browser notifications are best-effort only.
    }
  }
}

function isProjectProfileJobActive(job) {
  return Boolean(job && (job.status === "queued" || job.status === "running"));
}

function getProjectDiscoveryPhaseEmoji(phase) {
  switch (phase) {
    case "queued":
      return "⏳";
    case "starting":
      return "🚀";
    case "loading_cached_profile":
      return "⚡";
    case "walking_files":
      return "🔎";
    case "profiling_assets":
      return "📦";
    case "indexing_python":
      return "🐍";
    case "summarizing_code":
      return "🧠";
    case "planning_hints":
      return "🗺️";
    case "completed":
      return "✅";
    case "failed":
      return "⚠️";
    default:
      return "🔎";
  }
}

function describeProjectProfileJob(job) {
  const progress = job?.progress || {};
  const details = [];
  if (progress.files_scanned) {
    details.push(`${formatValue(progress.files_scanned)} files walked`);
  }
  if (typeof progress.code_files === "number" && progress.code_files) {
    details.push(`${formatValue(progress.code_files)} code`);
  }
  if (typeof progress.docs === "number" && progress.docs) {
    details.push(`${formatValue(progress.docs)} docs`);
  }
  if (typeof progress.data_files === "number" && progress.data_files) {
    details.push(`${formatValue(progress.data_files)} data candidates`);
  }
  if (typeof progress.data_assets_processed === "number" && typeof progress.data_assets_total === "number" && progress.data_assets_total) {
    details.push(`${formatValue(progress.data_assets_processed)}/${formatValue(progress.data_assets_total)} assets profiled`);
  }
  if (typeof progress.code_hint_files_processed === "number" && typeof progress.code_hint_files_total === "number" && progress.code_hint_files_total) {
    details.push(`${formatValue(progress.code_hint_files_processed)}/${formatValue(progress.code_hint_files_total)} hint files`);
  }
  if (typeof progress.skipped_heavy_hint_files === "number" && progress.skipped_heavy_hint_files) {
    details.push(`${formatValue(progress.skipped_heavy_hint_files)} heavy files skipped`);
  }
  const currentPath = progress.current_path ? `Current: ${truncateMiddle(progress.current_path)}` : "";
  return [progress.message || "", details.join(" · "), currentPath].filter(Boolean).join(" ");
}

function renderProjectDiscoveryProgress(job) {
  if (!isProjectProfileJobActive(job)) {
    return "";
  }
  const emoji = getProjectDiscoveryPhaseEmoji(job.progress?.phase);
  const description = describeProjectProfileJob(job);
  return `
    <div class="discovery-progress-card">
      <div class="discovery-progress-header">
        <span class="loading-emoji" aria-hidden="true">${emoji}</span>
        <div>
          <strong>Discovery in progress</strong>
          <div class="hint">${escapeHtml(description || "Inspecting the selected project root in the background.")}</div>
        </div>
      </div>
    </div>
  `;
}

function cssEscapeValue(value) {
  if (window.CSS && typeof window.CSS.escape === "function") {
    return window.CSS.escape(String(value));
  }
  return String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function setValueAtPath(target, path, value) {
  const parts = path.split(".");
  let current = target;
  while (parts.length > 1) {
    const nextKey = parts.shift();
    if (!(nextKey in current) || current[nextKey] == null) {
      current[nextKey] = {};
    }
    current = current[nextKey];
  }
  current[parts[0]] = value;
}

function coerceValue(element) {
  if (element.dataset.coerce === "boolean") {
    return element.value === "true";
  }
  if (element.type === "checkbox") {
    return element.checked;
  }
  return element.value;
}

function parseSourceRefs(text) {
  return text
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => {
      const parsed = splitColumnRef(value);
      if (!parsed) {
        return { node_id: value };
      }
      const upstreamNode = getNodeById(parsed.nodeId);
      if (upstreamNode && upstreamNode.kind === "contract") {
        return { node_id: parsed.nodeId, field: parsed.columnName };
      }
      return { node_id: parsed.nodeId, column: parsed.columnName };
    });
}

function parseLabelList(text) {
  return text
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

function parseReferenceList(text) {
  return String(text || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

function parseNullableInteger(text) {
  if (text == null || text === "") {
    return null;
  }
  const numeric = Number.parseInt(text, 10);
  return Number.isNaN(numeric) ? null : numeric;
}

function getActiveTracedColumnRefs() {
  if (state.selectedColumnRefs?.length) {
    return [...state.selectedColumnRefs];
  }
  return state.selectedColumnRef ? [state.selectedColumnRef] : [];
}

function getPickedColumnRefs() {
  return [...(state.linkSelectionRefs || [])];
}

function isColumnPicked(ref) {
  return getPickedColumnRefs().includes(ref);
}

function togglePickedColumn(ref) {
  if (!ref) return;
  const picked = new Set(getPickedColumnRefs());
  if (picked.has(ref)) {
    picked.delete(ref);
  } else {
    picked.add(ref);
  }
  state.linkSelectionRefs = [...picked];
}

function clearPickedColumns() {
  state.linkSelectionRefs = [];
}

function setTracedColumns(refs) {
  const uniqueRefs = [...new Set((refs || []).filter(Boolean))];
  state.selectedColumnRefs = uniqueRefs;
  state.selectedColumnRef = uniqueRefs[0] || null;
}

function replaceTracedColumnRef(previousRef, nextRef) {
  const refs = getActiveTracedColumnRefs().map((ref) => (ref === previousRef ? nextRef : ref));
  setTracedColumns(refs);
}

function removeTracedColumnRef(ref) {
  if (!ref) {
    return;
  }
  setTracedColumns(getActiveTracedColumnRefs().filter((current) => current !== ref));
}

function toggleTracedColumn(ref) {
  if (!ref) return;
  const current = new Set(getActiveTracedColumnRefs());
  if (current.has(ref)) {
    current.delete(ref);
  } else {
    current.add(ref);
  }
  setTracedColumns([...current]);
}

const WORK_STATUS_ORDER = ["todo", "in_progress", "done"];
const WORK_STATUS_LABELS = {
  todo: "To Do",
  in_progress: "In Progress",
  done: "Done",
};

function getNodeWorkStatus(node) {
  const executionSummary = getNodeExecutionSummary(node?.id);
  if (executionSummary?.tasks?.length) {
    if (executionSummary.tasks.some((task) => task.status === "blocked" || task.status === "in_progress")) {
      return "in_progress";
    }
    if (executionSummary.tasks.every((task) => task.status === "done")) {
      return "done";
    }
    return "todo";
  }
  return node?.work_status || "todo";
}

function cycleNodeWorkStatus(node) {
  if (!node) return;
  const current = getNodeWorkStatus(node);
  const index = WORK_STATUS_ORDER.indexOf(current);
  node.work_status = WORK_STATUS_ORDER[(index + 1) % WORK_STATUS_ORDER.length];
}

function getNodeWorkItems(node) {
  if (!node) return [];
  if (!Array.isArray(node.work_items)) {
    node.work_items = [];
  }
  return node.work_items;
}

function isColumnTraced(ref) {
  return getActiveTracedColumnRefs().includes(ref);
}

function hasActiveTrace() {
  return getActiveTracedColumnRefs().length > 0;
}

function getLineageDirectionForView() {
  return state.currentView === "impact" ? state.impactDirection : "both";
}

function getCombinedLineageRefs(direction) {
  const tracedRefs = getActiveTracedColumnRefs();
  const combined = new Set();
  tracedRefs.forEach((ref) => {
    getColumnLineageClosure(ref, direction).forEach((lineageRef) => combined.add(lineageRef));
  });
  return combined;
}

function getActiveLineageRefs() {
  return getCombinedLineageRefs(getLineageDirectionForView());
}

function isColumnHighlightedRef(ref) {
  if (!ref) return false;
  return hasActiveTrace() ? getActiveLineageRefs().has(ref) : false;
}

function splitColumnRef(ref) {
  const index = ref.lastIndexOf(".");
  if (index <= 0) return null;
  return {
    nodeId: ref.slice(0, index),
    columnName: ref.slice(index + 1),
  };
}

function splitDataTailPair(value) {
  const text = String(value || "");
  const index = text.lastIndexOf(":");
  if (index <= 0) {
    return ["", ""];
  }
  return [text.slice(0, index), text.slice(index + 1)];
}

function edgeTouchesColumnRefs(edge, refs) {
  const touchesMappings = (edge.column_mappings || []).some((mapping) => {
    return refs.has(`${edge.source}.${mapping.source_column}`) || refs.has(`${edge.target}.${mapping.target_column}`);
  });
  if (touchesMappings) {
    return true;
  }
  let hasSourceRef = false;
  let hasTargetRef = false;
  refs.forEach((ref) => {
    if (!hasSourceRef && ref.startsWith(`${edge.source}.`)) {
      hasSourceRef = true;
    }
    if (!hasTargetRef && ref.startsWith(`${edge.target}.`)) {
      hasTargetRef = true;
    }
  });
  return hasSourceRef && hasTargetRef;
}

function isMappingFocused(edge, mapping) {
  const lineageRefs = getActiveLineageRefs();
  if (!lineageRefs.size) return false;
  return lineageRefs.has(`${edge.source}.${mapping.source_column}`)
    || lineageRefs.has(`${edge.target}.${mapping.target_column}`);
}

function getNodeRefreshMeta(node) {
  if (!node || !state.graph) {
    return null;
  }
  let value = "";
  if (node.kind === "source") {
    value = (node.source?.refresh || "").trim();
  } else if (node.kind === "data") {
    value = (node.data?.update_frequency || "").trim();
    if (!value) {
      const incomingSource = state.graph.edges
        .filter((edge) => edge.target === node.id)
        .map((edge) => getNodeById(edge.source))
        .find((candidate) => candidate?.kind === "source" && candidate.source?.refresh);
      value = (incomingSource?.source?.refresh || "").trim();
    }
  }
  if (!value) {
    return null;
  }
  return {
    value,
    label: value,
    badge: `↻ ${value}`,
  };
}

function buildNodeMeta(node) {
  const workCount = getNodeWorkItems(node).length;
  const refreshMeta = getNodeRefreshMeta(node);
  if (node.kind === "data") {
    const rowCount = node.data?.row_count != null ? `${node.data.row_count} obs` : "row count missing";
    return `${rowCount}${refreshMeta ? ` | ${refreshMeta.badge}` : ""} | ${workCount} notes`;
  }
  if (node.kind === "source") {
    const dictionaryCount = (node.source?.data_dictionaries || []).length;
    const rawAssetCount = (node.source?.raw_assets || []).length;
    return `${node.source?.provider || "provider missing"}${refreshMeta ? ` | ${refreshMeta.badge}` : ""} | ${rawAssetCount} raw assets | ${dictionaryCount} dictionaries | ${workCount} notes`;
  }
  if (node.kind === "compute") {
    const selectedCount = (node.compute?.feature_selection || []).filter((feature) => feature.status === "selected").length;
    return `${node.compute?.runtime || "runtime missing"} | ${selectedCount}/${(node.compute?.feature_selection || []).length} selected | ${workCount} notes`;
  }
  return `${(node.contract?.fields || []).length} fields | ${workCount} notes`;
}

function createAuthoringNode(options = {}) {
  const draft = state.authoring.node;
  if (!draft.label.trim()) {
    setStatus("Create object failed", "Node label is required.");
    return;
  }
  recordGraphHistory();
  const node = buildEmptyNode(draft.kind, draft.extensionType, draft.label.trim(), draft.description.trim());
  node.owner = draft.owner.trim();
  if (node.kind === "data") {
    node.data.persistence = draft.persistence;
    node.data.persisted = draft.persisted;
    node.data.update_frequency = draft.updateFrequency.trim();
    node.data.profile_target = draft.referenceValue.trim();
    if (draft.referenceKind === "disk_path") {
      node.data.local_path = draft.referenceValue.trim();
    }
    node.columns = parseAuthoringNodeColumns(draft.schemaText);
  }
  if (node.kind === "source") {
    node.source.provider = draft.sourceProvider.trim();
    node.source.refresh = draft.sourceRefresh.trim();
    node.source.series_id = draft.sourceSeriesId.trim();
    node.source.origin.kind = draft.referenceKind.trim();
    node.source.origin.value = draft.referenceValue.trim();
    if (draft.dataDictionaryLabel.trim() || draft.dataDictionaryValue.trim()) {
      node.source.data_dictionaries.push({
        label: draft.dataDictionaryLabel.trim(),
        kind: draft.dataDictionaryKind,
        value: draft.dataDictionaryValue.trim(),
      });
    }
    if (draft.rawAssetValue.trim()) {
      node.source.raw_assets.push({
        label: draft.rawAssetLabel.trim(),
        kind: draft.rawAssetKind,
        format: draft.rawAssetFormat,
        value: draft.rawAssetValue.trim(),
        profile_ready: true,
      });
    }
  }
  if (node.kind === "compute") {
    node.compute.runtime = draft.runtime.trim();
  }
  if (node.kind === "contract") {
    if (node.extension_type === "api") {
      node.contract.route = draft.route.trim();
    } else {
      node.contract.component = draft.component.trim();
      node.contract.ui_role = draft.uiRole;
    }
  }
  state.graph.nodes.push(node);
  state.selectionMode = "node";
  state.selectedNodeId = node.id;
  state.selectedEdgeId = null;
  clearTracedColumns();
  state.authoring.node = buildDefaultAuthoringState().node;
  if (options.fromGraphPanel) {
    state.showGraphAddPanel = false;
  }
  if (options.attachPickedColumns) {
    attachPickedColumnsToNode(node.id, { recordHistory: false });
    return;
  }
  state.needsAutoLayout = true;
  markDirty(false);
  syncAuthoringState();
  render();
  setStatus("Object created", `${node.id} was added locally. Save -> Plan when ready.`);
}

function createAuthoringEdge() {
  const draft = state.authoring.edge;
  if (!draft.source || !draft.target) {
    setStatus("Create edge failed", "Choose both a source and a target node.");
    return;
  }
  if (draft.source === draft.target) {
    setStatus("Create edge failed", "Source and target must be different.");
    return;
  }
  const duplicate = state.graph.edges.some((edge) => edge.source === draft.source && edge.target === draft.target && edge.type === draft.type);
  if (duplicate) {
    setStatus("Create edge skipped", "That edge already exists.");
    return;
  }
  recordGraphHistory();
  const edge = {
    id: makeUniqueEdgeId(draft.type, draft.source, draft.target),
    type: draft.type,
    source: draft.source,
    target: draft.target,
    label: "",
    column_mappings: [],
    notes: "",
  };
  state.graph.edges.push(edge);
  state.selectionMode = "edge";
  state.selectedEdgeId = edge.id;
  clearTracedColumns();
  markDirty(false);
  render();
  setStatus("Edge created", `${edge.id} was added locally.`);
}

function deleteNodeById(nodeId) {
  const node = getNodeById(nodeId);
  if (!node) {
    return;
  }
  queueDestructiveConfirm({
    title: "Remove object",
    message: `Remove ${node.label} and its attached connections from the graph?`,
    onConfirm: () => {
      recordGraphHistory();
      state.graph.nodes = state.graph.nodes.filter((candidate) => candidate.id !== node.id);
      state.graph.edges = state.graph.edges.filter((edge) => edge.source !== node.id && edge.target !== node.id);
      state.selectedNodeId = state.graph.nodes[0]?.id || null;
      state.selectedEdgeId = null;
      clearTracedColumns();
      clearPickedColumns();
      state.selectionMode = "node";
      state.needsAutoLayout = true;
      markDirty(false);
      syncAuthoringState();
      render();
      setStatus("Object removed", `${node.id} and its attached edges were removed locally.`);
    },
  });
}

function deleteCurrentSelection() {
  if (state.selectionMode === "edge" && state.selectedEdgeId) {
    const edge = getEdgeById(state.selectedEdgeId);
    if (!edge) return;
    queueDestructiveConfirm({
      title: "Remove connection",
      message: `Remove ${edge.type} between ${getNodeById(edge.source)?.label || edge.source} and ${getNodeById(edge.target)?.label || edge.target}?`,
      onConfirm: () => {
        recordGraphHistory();
        state.graph.edges = state.graph.edges.filter((candidate) => candidate.id !== edge.id);
        state.selectedEdgeId = null;
        state.selectionMode = "node";
        markDirty(false);
        syncAuthoringState();
        render();
        setStatus("Connection removed", `${edge.id} and its mappings were removed locally.`);
      },
    });
    return;
  }

  if (!state.selectedNodeId) {
    setStatus("Delete skipped", "Select a node or edge first.");
    return;
  }

  deleteNodeById(state.selectedNodeId);
}

async function importAssetFromAuthoring() {
  const importSpec = buildAuthoringImportSpec();
  if (!importSpec) {
    setStatus("Import failed", "Source label, data label, and raw asset value are required.");
    return;
  }
  await runImportAsset(importSpec, { resetDraft: true });
}

function buildAuthoringImportSpec() {
  const draft = state.authoring.import;
  if (!draft.sourceLabel.trim() || !draft.dataLabel.trim() || !draft.rawAssetValue.trim()) {
    return null;
  }

  return {
    source_label: draft.sourceLabel.trim(),
    source_extension_type: draft.sourceExtensionType,
    source_description: draft.sourceDescription.trim(),
    source_provider: draft.sourceProvider.trim(),
    source_refresh: draft.sourceRefresh.trim(),
    source_origin_kind: draft.sourceOriginKind.trim(),
    source_origin_value: draft.sourceOriginValue.trim(),
    source_series_id: draft.sourceSeriesId.trim(),
    raw_asset_label: draft.rawAssetLabel.trim(),
    raw_asset_kind: draft.rawAssetKind,
    raw_asset_format: draft.rawAssetFormat,
    raw_asset_value: draft.rawAssetValue.trim(),
    profile_ready: draft.profileReady,
    data_label: draft.dataLabel.trim(),
    data_extension_type: draft.dataExtensionType,
    data_description: draft.dataDescription.trim(),
    update_frequency: draft.updateFrequency.trim(),
    persistence: draft.persistence,
    persisted: draft.persisted,
    schema_columns: parseSchemaColumns(draft.schemaText),
  };
}

async function runImportAsset(importSpec, options = {}) {
  setStatus("Importing asset...", "Creating source/data objects and profiling the asset when it is accessible.");
  const previousGraph = cloneGraph();
  const response = await fetch("/api/import/asset", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      graph: state.graph,
      import_spec: importSpec,
      root_path: state.projectProfileOptions.rootPath || "",
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Import failed", payload.detail || payload.error || "Unable to import the asset.");
    return;
  }

  state.graph = payload.graph;
  recordGraphHistory(previousGraph);
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.structure = payload.structure || state.structure;
  state.selectionMode = "node";
  state.selectedNodeId = payload.imported?.data_node_id || state.selectedNodeId;
  state.selectedEdgeId = null;
  clearTracedColumns();
  state.needsAutoLayout = true;
  if (options.resetDraft) {
    resetAuthoringImportDraft();
  }
  markDirty(false);
  syncAuthoringState();
  render();
  setStatus(
    "Asset imported",
    `${payload.imported?.source_node_id || "source"} and ${payload.imported?.data_node_id || "data"} were added locally.`
  );
}

async function importSelectedProjectSuggestions() {
  const selectedPaths = state.selectedProjectImports || [];
  if (!selectedPaths.length) {
    setStatus("Bulk import skipped", "Select one or more suggested assets first.");
    return;
  }
  const importSpecs = selectedPaths
    .map((path) => getProjectImportSuggestion(path))
    .filter(Boolean);
  if (!importSpecs.length) {
    setStatus("Bulk import failed", "The selected assets do not have import metadata.");
    return;
  }

  setStatus("Importing selected assets...", "Creating source/data objects and profiling accessible assets in one pass.");
  const previousGraph = cloneGraph();
  const response = await fetch("/api/import/assets/bulk", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      graph: state.graph,
      import_specs: importSpecs,
      root_path: state.projectProfileOptions.rootPath || "",
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Bulk import failed", payload.detail || payload.error || "Unable to import the selected assets.");
    return;
  }

  state.graph = payload.graph;
  recordGraphHistory(previousGraph);
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.structure = payload.structure || state.structure;
  state.selectionMode = "node";
  state.selectedNodeId = payload.imported?.[0]?.data_node_id || state.selectedNodeId;
  state.selectedEdgeId = null;
  clearTracedColumns();
  state.selectedProjectImports = [];
  state.needsAutoLayout = true;
  markDirty(false);
  syncAuthoringState();
  render();
  setStatus(
    "Bulk import complete",
    `${(payload.imported || []).length} asset${(payload.imported || []).length === 1 ? "" : "s"} imported, ${(payload.skipped || []).length} skipped as existing.`
  );
}

async function importProjectBootstrap() {
  if (!state.projectProfile) {
    setStatus("Bootstrap unavailable", "Run project discovery first.");
    return;
  }

  const selectedPaths = state.selectedProjectImports || [];
  const selectedApiHints = state.selectedProjectApiHints || [];
  const selectedUiHints = state.selectedProjectUiHints || [];
  const apiHintIds = selectedApiHints;
  const uiHintIds = selectedUiHints;
  const importAssets = state.projectBootstrapOptions.assets
    && (selectedPaths.length > 0 || (state.projectProfile.data_assets || []).some((asset) => asset.suggested_import));
  const importApiHints = state.projectBootstrapOptions.apiHints
    && (selectedApiHints.length > 0 || (state.projectProfile.api_contract_hints || []).length > 0);
  const importUiHints = state.projectBootstrapOptions.uiHints
    && (selectedUiHints.length > 0 || (state.projectProfile.ui_contract_hints || []).length > 0);

  if (!importAssets && !importApiHints && !importUiHints) {
    setStatus("Bootstrap skipped", "The current discovery does not have importable assets or code hints.");
    return;
  }

  setStatus("Bootstrapping graph...", "Importing discovered data assets and code hints into the current graph.");
  const previousGraph = cloneGraph();
  const response = await fetch("/api/import/project-bootstrap", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      graph: state.graph,
      include_tests: state.projectProfileOptions.includeTests,
      include_internal: state.projectProfileOptions.includeInternal,
      profile_token: state.projectProfile?.cache?.token || "",
      root_path: state.projectProfileOptions.rootPath || "",
      asset_paths: selectedPaths,
      api_hint_ids: apiHintIds,
      ui_hint_ids: uiHintIds,
      import_assets: importAssets,
      import_api_hints: importApiHints,
      import_ui_hints: importUiHints,
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Bootstrap failed", payload.detail || payload.error || "Unable to import discovered findings.");
    return;
  }

  state.graph = payload.graph;
  recordGraphHistory(previousGraph);
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.structure = payload.structure || state.structure;
  state.projectProfile = payload.project_profile || state.projectProfile;
  state.selectedProjectImports = [];
  state.selectedProjectApiHints = [];
  state.selectedProjectUiHints = [];
  state.selectionMode = "node";
  state.selectedNodeId = payload.imported?.asset_imported?.[0]?.data_node_id
    || payload.imported?.ui_created?.[0]
    || payload.imported?.ui_updated?.[0]
    || payload.imported?.api_created?.[0]
    || payload.imported?.api_updated?.[0]
    || state.selectedNodeId;
  state.selectedEdgeId = null;
  clearTracedColumns();
  state.needsAutoLayout = true;
  markDirty(false);
  syncAuthoringState();
  render();
  setStatus(
    "Graph bootstrap complete",
    `${(payload.imported?.asset_imported || []).length} assets imported, ${(payload.imported?.asset_skipped || []).length} skipped, ${(payload.imported?.api_created || []).length + (payload.imported?.api_updated || []).length} API contracts touched, ${(payload.imported?.ui_created || []).length + (payload.imported?.ui_updated || []).length} UI contracts touched.`
  );
}

function toggleSelectionList(currentValues, value, checked) {
  const selected = new Set(currentValues || []);
  if (checked) {
    selected.add(value);
  } else {
    selected.delete(value);
  }
  return [...selected];
}

async function requestContractFieldSuggestions(fieldIndex) {
  const node = getNodeById(state.selectedNodeId);
  if (!node || node.kind !== "contract") {
    return;
  }
  const field = node.contract.fields[fieldIndex];
  if (!field) {
    return;
  }

  setStatus("Loading binding suggestions...", `Looking for upstream matches for ${field.name}.`);
  const response = await fetch("/api/contract/suggestions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      graph: state.graph,
      node_id: node.id,
      field_name: field.name,
      limit: 8,
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Suggestion lookup failed", payload.detail || payload.error || "Unable to load field suggestions.");
    return;
  }
  state.bindingSuggestions[getContractFieldSuggestionKey(node.id, fieldIndex)] = payload;
  renderSelectionDetail();
  setStatus(
    "Binding suggestions ready",
    `${(payload.suggestions || []).length} suggestion${(payload.suggestions || []).length === 1 ? "" : "s"} found for ${field.name}.`
  );
}

function applyContractFieldAutoSuggestion(fieldIndex) {
  const node = getNodeById(state.selectedNodeId);
  if (!node || node.kind !== "contract") {
    return;
  }
  const suggestionState = state.bindingSuggestions[getContractFieldSuggestionKey(node.id, fieldIndex)];
  if (!suggestionState?.auto_suggestion) {
    setStatus("No auto-suggestion", "Try loading field suggestions first.");
    return;
  }
  applyContractFieldSource(fieldIndex, suggestionState.auto_suggestion.source);
}

function applyContractFieldSuggestion(fieldIndex, suggestionIndex) {
  const node = getNodeById(state.selectedNodeId);
  if (!node || node.kind !== "contract") {
    return;
  }
  const suggestionState = state.bindingSuggestions[getContractFieldSuggestionKey(node.id, fieldIndex)];
  const suggestion = suggestionState?.suggestions?.[suggestionIndex];
  if (!suggestion) {
    return;
  }
  applyContractFieldSource(fieldIndex, suggestion.source);
}

function applyContractFieldSource(fieldIndex, source) {
  const node = getNodeById(state.selectedNodeId);
  if (!node || node.kind !== "contract") {
    return;
  }
  const field = node.contract.fields[fieldIndex];
  if (!field) {
    return;
  }
  field.sources = [source];
  markDirty(false);
  renderSelectionDetail();
  setStatus("Binding applied", `${field.name} now points to ${formatSources([source])}.`);
}

async function createApiContractFromHint(hintId) {
  await importProjectHint("api", hintId);
}

async function createUiContractFromHint(hintId) {
  await importProjectHint("ui", hintId);
}

function getProjectApiHint(hintId) {
  return (state.projectProfile?.api_contract_hints || []).find((hint) => hint.id === hintId) || null;
}

function getProjectUiHint(hintId) {
  return (state.projectProfile?.ui_contract_hints || []).find((hint) => hint.id === hintId) || null;
}

function getContractFieldSuggestionKey(nodeId, fieldIndex) {
  return `${nodeId}::${fieldIndex}`;
}

async function importProjectHint(hintKind, hintId) {
  setStatus("Importing project hint...", "Creating or selecting contract nodes from project discovery.");
  const previousGraph = cloneGraph();
  const response = await fetch("/api/import/project-hint", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      graph: state.graph,
      hint_kind: hintKind,
      hint_id: hintId,
      profile_token: state.projectProfile?.cache?.token || "",
      root_path: state.projectProfileOptions.rootPath || "",
      include_tests: state.projectProfileOptions.includeTests,
      include_internal: state.projectProfileOptions.includeInternal,
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Hint import failed", payload.detail || payload.error || "Unable to import the selected hint.");
    return;
  }

  state.graph = payload.graph;
  recordGraphHistory(previousGraph);
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.structure = payload.structure || state.structure;
  state.selectionMode = "node";
  state.selectedNodeId = payload.imported?.node_id || state.selectedNodeId;
  state.selectedEdgeId = null;
  clearTracedColumns();
  state.needsAutoLayout = true;
  markDirty(false);
  syncAuthoringState();
  render();
  const detail = hintKind === "api"
    ? `${payload.imported?.node_id || "contract"} ${payload.imported?.created ? "was created" : "was updated"}, ${(payload.imported?.created_field_names || []).length} field(s) added, ${(payload.imported?.binding_summary?.applied || []).length} binding(s) auto-applied.`
    : `${payload.imported?.node_id || "contract"} linked to ${(payload.imported?.bound_api_node_ids || []).length} API contract(s), created ${(payload.imported?.created_field_names || []).length} field(s), and auto-applied ${(payload.imported?.binding_summary?.applied || []).length} binding(s).`;
  setStatus(hintKind === "api" ? "API contract ready" : "UI contract ready", detail);
}

function loadProjectImportSuggestion(assetPath) {
  const suggestion = getProjectImportSuggestion(assetPath);
  if (!suggestion) {
    setStatus("Load skipped", "Suggested import metadata was not available for that asset.");
    return;
  }
  state.authoring.import = normalizeImportSuggestionForState(suggestion);
  syncAuthoringState();
  renderAuthoringPanel();
  setStatus("Import draft loaded", `${suggestion.source_label} -> ${suggestion.data_label} is ready to review in Add / Import.`);
}

async function importProjectImportSuggestion(assetPath) {
  const suggestion = getProjectImportSuggestion(assetPath);
  if (!suggestion) {
    setStatus("Import skipped", "Suggested import metadata was not available for that asset.");
    return;
  }
  await runImportAsset(suggestion, { resetDraft: false });
}

function getProjectImportSuggestion(assetPath) {
  const asset = (state.projectProfile?.data_assets || []).find((candidate) => candidate.path === assetPath);
  return asset?.suggested_import || null;
}

function normalizeImportSuggestionForState(suggestion) {
  return {
    sourceLabel: suggestion.source_label || "",
    sourceExtensionType: suggestion.source_extension_type || "object",
    sourceDescription: suggestion.source_description || "",
    sourceProvider: suggestion.source_provider || "",
    sourceRefresh: suggestion.source_refresh || "",
    sourceOriginKind: suggestion.source_origin_kind || "",
    sourceOriginValue: suggestion.source_origin_value || "",
    sourceSeriesId: suggestion.source_series_id || "",
    rawAssetLabel: suggestion.raw_asset_label || "",
    rawAssetKind: suggestion.raw_asset_kind || "file",
    rawAssetFormat: suggestion.raw_asset_format || "unknown",
    rawAssetValue: suggestion.raw_asset_value || "",
    profileReady: suggestion.profile_ready !== false,
    dataLabel: suggestion.data_label || "",
    dataExtensionType: suggestion.data_extension_type || "raw_dataset",
    dataDescription: suggestion.data_description || "",
    updateFrequency: suggestion.update_frequency || "",
    persistence: suggestion.persistence || "cold",
    persisted: suggestion.persisted === true,
    schemaText: (suggestion.schema_columns || [])
      .map((column) => `${column.name}:${column.data_type || "unknown"}`)
      .join("\n"),
  };
}

function resetAuthoringImportDraft() {
  state.authoring.import = buildDefaultAuthoringState().import;
}

async function importOpenAPIFromAuthoring() {
  const importSpec = buildOpenAPIImportSpec();
  if (!importSpec) {
    setStatus("OpenAPI import failed", "Provide either a spec path or pasted OpenAPI text.");
    return;
  }
  await runOpenAPIImport(importSpec, { resetDraft: true });
}

function buildOpenAPIImportSpec() {
  const draft = state.authoring.openapi;
  if (!draft.specPath.trim() && !draft.specText.trim()) {
    return null;
  }
  return {
    spec_path: draft.specPath.trim(),
    spec_text: draft.specText,
    owner: draft.owner.trim(),
  };
}

async function runOpenAPIImport(importSpec, options = {}) {
  setStatus("Importing OpenAPI...", "Creating API contract nodes from the supplied spec.");
  const previousGraph = cloneGraph();
  const response = await fetch("/api/import/openapi", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      graph: state.graph,
      import_spec: importSpec,
      root_path: state.projectProfileOptions.rootPath || "",
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("OpenAPI import failed", payload.detail || payload.error || "Unable to import API contracts.");
    return;
  }

  state.graph = payload.graph;
  recordGraphHistory(previousGraph);
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.structure = payload.structure || state.structure;
  state.selectionMode = "node";
  state.selectedNodeId = payload.imported?.contract_node_ids?.[0]
    || payload.imported?.updated_contract_node_ids?.[0]
    || state.selectedNodeId;
  state.selectedEdgeId = null;
  clearTracedColumns();
  state.needsAutoLayout = true;
  if (options.resetDraft) {
    resetAuthoringOpenAPIDraft();
  }
  markDirty(false);
  syncAuthoringState();
  render();
  setStatus(
    "OpenAPI imported",
    `${(payload.imported?.contract_node_ids || []).length} added, ${(payload.imported?.updated_contract_node_ids || []).length} updated, ${(payload.imported?.binding_summary?.applied || []).length} field bindings applied automatically, ${(payload.imported?.binding_summary?.unresolved || []).length} still unresolved.`
  );
}

function resetAuthoringOpenAPIDraft() {
  state.authoring.openapi = buildDefaultAuthoringState().openapi;
}

function getExtensionOptions(kind) {
  if (kind === "source") {
    return ["provider", "group", "object", "url", "disk_path", "api_endpoint", "bucket_path"];
  }
  if (kind === "data") {
    return ["raw_dataset", "table", "view", "materialized_view", "feature_set"];
  }
  if (kind === "compute") {
    return ["transform", "model"];
  }
  return ["api", "ui"];
}

function buildEmptyNode(kind, extensionType, label, description) {
  return {
    id: makeUniqueNodeId(kind, label),
    kind,
    extension_type: extensionType,
    label,
    description,
    tags: [],
    owner: "",
    sensitivity: "internal",
    status: "active",
    work_status: "todo",
    profile_status: "unknown",
    notes: "",
    work_items: [],
    position: getNextNodePosition(kind),
    columns: [],
    source: {
      provider: "",
      origin: { kind: "", value: "" },
      refresh: "",
      shared_config: {},
      series_id: "",
      data_dictionaries: [],
      raw_assets: [],
    },
    data: {
      persistence: "",
      local_path: "",
      update_frequency: "",
      persisted: false,
      row_count: null,
      sampled: null,
      profile_target: "",
    },
    compute: {
      runtime: "",
      inputs: [],
      outputs: [],
      notes: "",
      feature_selection: [],
      column_mappings: [],
    },
    contract: {
      route: "",
      component: "",
      ui_role: extensionType === "ui" ? "component" : "",
      fields: [],
    },
  };
}

function getNextNodePosition(kind) {
  const xPositions = { source: 20, data: 330, compute: 640, contract: 960 };
  const sameKind = state.graph.nodes.filter((node) => node.kind === kind);
  const nextY = sameKind.length ? Math.max(...sameKind.map((node) => node.position.y)) + 120 : 40;
  return { x: xPositions[kind] || 20, y: nextY };
}

function makeUniqueNodeId(kind, label) {
  const base = `${kind}:${slugifyText(label) || kind}`;
  const existing = new Set(state.graph.nodes.map((node) => node.id));
  if (!existing.has(base)) {
    return base;
  }
  let counter = 2;
  while (existing.has(`${base}-${counter}`)) {
    counter += 1;
  }
  return `${base}-${counter}`;
}

function makeUniqueEdgeId(type, source, target) {
  const base = `edge:${type}:${slugifyText(source)}:${slugifyText(target)}`;
  const existing = new Set(state.graph.edges.map((edge) => edge.id));
  if (!existing.has(base)) {
    return base;
  }
  let counter = 2;
  while (existing.has(`${base}-${counter}`)) {
    counter += 1;
  }
  return `${base}-${counter}`;
}

function slugifyText(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function parseSchemaColumns(text) {
  return String(text || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [namePart, typePart = "unknown"] = line.split(":");
      return {
        name: namePart.trim(),
        data_type: typePart.trim() || "unknown",
      };
    })
    .filter((column) => column.name);
}

function parseAuthoringNodeColumns(text) {
  return String(text || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const parts = line.includes(":") ? line.split(":") : line.split(",");
      const [namePart = "", typePart = "unknown", categoryPart = "", labelsPart = ""] = parts;
      return {
        name: namePart.trim(),
        data_type: (typePart || "unknown").trim() || "unknown",
        description: namePart.trim(),
        nullable: true,
        null_pct: null,
        stats: {},
        notes: "",
        category: categoryPart.trim(),
        labels: parseLabelList(labelsPart),
      };
    })
    .filter((column) => column.name);
}

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "missing";
  }
  return String(value);
}

function formatColumnMappings(edge, mappings) {
  return mappings.map((mapping) => `${edge.source}.${mapping.source_column} -> ${edge.target}.${mapping.target_column}`).join(", ");
}

function formatSources(sources) {
  return sources.map((source) => {
    if (source.column) {
      return `${source.node_id}.${source.column}`;
    }
    if (source.field) {
      return `${source.node_id}.${source.field}`;
    }
    return source.node_id || "unknown";
  }).join(", ");
}

function renderWarningList(title, items) {
  return `
    <strong>${escapeHtml(title)}</strong>
    <ul class="warning-list">${renderItems(items || [])}</ul>
  `;
}

function renderFeatureSelectionList(node) {
  const featureEntries = getSortedFeatureSelections(node);
  if (!featureEntries.length) {
    return "";
  }

  return featureEntries.map((entry) => {
    if (entry.type === "group") {
      return `<div class="group-heading">${escapeHtml(entry.label)}</div>`;
    }
    const { feature, index } = entry;
    return `
      <div class="column-row ${isColumnHighlightedRef(feature.column_ref) ? "focused" : ""}">
        <div class="column-head">
          <div>
            <div class="column-main">${escapeHtml(feature.column_ref || "unnamed feature")}</div>
            <div class="chip-row">
              <span class="tag-chip">${escapeHtml(feature.category || "uncategorized")}</span>
              ${renderLabelChips(feature.labels || [])}
            </div>
          </div>
          <div class="row-actions">
            <button class="text-button" type="button" data-focus-column="${escapeHtml(feature.column_ref || "")}">${isColumnTraced(feature.column_ref || "") ? "Untrace" : "Trace lineage"}</button>
            <button class="inline-button danger" type="button" data-feature-remove="${index}">Remove</button>
          </div>
        </div>
        <div class="column-meta">
          stage: ${escapeHtml(feature.stage || "unspecified")} | persisted: ${feature.persisted ? "yes" : "no"} | order: ${formatValue(feature.order)}
        </div>
        <div class="form-grid compact">
          <label class="form-field">
            Status
            <select data-feature-index="${index}">
              ${["selected", "candidate", "rejected", "deferred"].map((status) => `
                <option value="${status}" ${feature.status === status ? "selected" : ""}>${status}</option>
              `).join("")}
            </select>
          </label>
          <label class="form-field">
            Category
            <input data-feature-index="${index}" data-feature-field="category" value="${escapeHtml(feature.category || "")}" />
          </label>
          <label class="form-field">
            Order
            <input data-feature-index="${index}" data-feature-field="order" value="${feature.order ?? ""}" />
          </label>
          <label class="form-field">
            Stage
            <input data-feature-index="${index}" data-feature-field="stage" value="${escapeHtml(feature.stage || "")}" />
          </label>
          <label class="form-field form-field-full">
            Labels
            <input data-feature-index="${index}" data-feature-field="labels" value="${escapeHtml((feature.labels || []).join(", "))}" />
          </label>
        </div>
      </div>
    `;
  }).join("");
}

function getSortedFeatureSelections(node) {
  const entries = (node.compute?.feature_selection || []).map((feature, index) => ({ feature, index }));
  if (state.featureSortMode === "original") {
    return entries;
  }

  if (state.featureSortMode === "status") {
    const rank = { selected: 0, candidate: 1, deferred: 2, rejected: 3 };
    const sorted = [...entries].sort((left, right) => {
      const leftRank = rank[left.feature.status] ?? 99;
      const rightRank = rank[right.feature.status] ?? 99;
      if (leftRank !== rightRank) {
        return leftRank - rightRank;
      }
      return compareFeatureEntries(left, right);
    });
    return withGroupHeadings(sorted, (entry) => capitalize(entry.feature.status || "unknown"));
  }

  const sorted = [...entries].sort(compareFeatureEntries);
  return withGroupHeadings(sorted, (entry) => entry.feature.category || "uncategorized");
}

function compareFeatureEntries(left, right) {
  const leftCategory = left.feature.category || "uncategorized";
  const rightCategory = right.feature.category || "uncategorized";
  if (leftCategory !== rightCategory) {
    return leftCategory.localeCompare(rightCategory);
  }
  const leftOrder = left.feature.order ?? Number.MAX_SAFE_INTEGER;
  const rightOrder = right.feature.order ?? Number.MAX_SAFE_INTEGER;
  if (leftOrder !== rightOrder) {
    return leftOrder - rightOrder;
  }
  return (left.feature.column_ref || "").localeCompare(right.feature.column_ref || "");
}

function withGroupHeadings(entries, groupLabelFn) {
  const grouped = [];
  let previousLabel = null;
  entries.forEach((entry) => {
    const label = groupLabelFn(entry);
    if (label !== previousLabel) {
      grouped.push({ type: "group", label });
      previousLabel = label;
    }
    grouped.push({ type: "item", ...entry });
  });
  return grouped;
}

function renderLabelChips(labels) {
  if (!labels.length) {
    return '<span class="tag-chip muted">no labels</span>';
  }
  return labels.map((label) => `<span class="tag-chip">${escapeHtml(label)}</span>`).join("");
}

function capitalize(value) {
  return value ? `${value[0].toUpperCase()}${value.slice(1)}` : "";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

