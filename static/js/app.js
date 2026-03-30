const state = {
  graph: null,
  diagnostics: {},
  validationReport: null,
  currentView: "data",
  impactDirection: "both",
  selectedNodeId: null,
  selectedEdgeId: null,
  selectionMode: "node",
  selectedColumnRef: null,
  selectedColumnRefs: [],
  latestPlan: null,
  lastArtifacts: null,
  structure: null,
  selectedStructureBundleId: "",
  selectedStructureBundle: null,
  structureDraft: {
    yamlText: "",
    role: "scout",
    scope: "changed",
    docPathsText: "",
    selectedPathsText: "",
  },
  projectProfile: null,
  selectedProjectImports: [],
  selectedProjectApiHints: [],
  selectedProjectUiHints: [],
  onboardingPresets: [],
  selectedProjectPresetId: "",
  projectPresetDraft: {
    name: "",
    description: "",
  },
  projectWizardStep: 1,
  bindingSuggestions: {},
  projectProfileOptions: {
    includeTests: false,
    includeInternal: true,
    rootPath: "",
  },
  projectBootstrapOptions: {
    assets: true,
    apiHints: true,
    uiHints: true,
  },
  searchTerm: "",
  kindFilter: "all",
  featureSortMode: "category",
  interactionMode: "inspect",
  zoom: 1,
  hasManualZoom: false,
  graphFullscreen: false,
  legendCollapsed: false,
  legendPosition: null,
  inspectorSections: {
    overview: true,
    dependencies: false,
    editing: false,
  },
  lastInspectorSelectionKey: "",
  authoringDrawerOpen: false,
  expandedGraphNodes: {},
  showGraphDetails: false,
  graphDetailSnapshot: null,
  graphEditNodes: {},
  graphNodeDetailOverrides: {},
  columnDependencyToggles: {},
  graphTablePages: {},
  pendingColumnLinkRef: null,
  pendingBindingTarget: null,
  linkSelectionRefs: [],
  showGraphAddPanel: false,
  historyPast: [],
  historyFuture: [],
  needsAutoLayout: true,
  contextMenu: null,
  destructiveConfirm: null,
  destructiveWarningSilencedUntil: 0,
  directoryPicker: {
    open: false,
    query: "",
    results: [],
    loading: false,
  },
  authoring: buildDefaultAuthoringState(),
  dirty: false,
};

const shell = document.querySelector(".shell");
const canvasPanel = document.getElementById("canvas-panel");
const graphCanvas = document.getElementById("graph-canvas");
const graphEdges = document.getElementById("graph-edges");
const graphScroller = document.getElementById("graph-scroller");
const graphViewport = document.getElementById("graph-viewport");
const graphLegendPanel = document.getElementById("graph-legend-panel");
const graphLegendBody = document.getElementById("graph-legend-body");
const graphLegendToggle = document.getElementById("graph-legend-toggle");
const graphContextMenu = document.getElementById("graph-context-menu");
const nodeDetail = document.getElementById("node-detail");
const structureSummary = document.getElementById("structure-summary");
const planSummary = document.getElementById("plan-summary");
const validationSummary = document.getElementById("validation-summary");
const authoringPanel = document.getElementById("authoring-panel");
const authoringDrawer = document.getElementById("authoring-drawer");
const authoringDrawerToggle = document.getElementById("authoring-drawer-toggle");
const authoringDrawerClose = document.getElementById("authoring-drawer-close");
const detailHeading = document.getElementById("detail-heading");
const inspectorPanel = document.getElementById("inspector-panel");
const inspectorModePill = document.getElementById("inspector-mode-pill");
const statusText = document.getElementById("status-text");
const substatusText = document.getElementById("substatus-text");
const projectProfileSummary = document.getElementById("project-profile-summary");
const viewSelect = document.getElementById("view-select");
const impactDirectionSelect = document.getElementById("impact-direction-select");
const searchInput = document.getElementById("search-input");
const kindFilterSelect = document.getElementById("kind-filter-select");
const visibleCount = document.getElementById("visible-count");
const dirtyIndicator = document.getElementById("dirty-indicator");
const projectProfileButton = document.getElementById("project-profile-button");
const validateButton = document.getElementById("validate-button");
const saveButton = document.getElementById("save-button");
const refreshButton = document.getElementById("refresh-button");
const undoButton = document.getElementById("undo-button");
const redoButton = document.getElementById("redo-button");
const centerSelectionButton = document.getElementById("center-selection-button");
const clearFiltersButton = document.getElementById("clear-filters-button");
const zoomInButton = document.getElementById("zoom-in-button");
const zoomOutButton = document.getElementById("zoom-out-button");
const zoomResetButton = document.getElementById("zoom-reset-button");
const zoomInput = document.getElementById("zoom-input");
const graphDetailToggle = document.getElementById("graph-detail-toggle");
const fullscreenButton = document.getElementById("fullscreen-button");
const removeSelectionButton = document.getElementById("remove-selection-button");
const inspectModeButton = document.getElementById("inspect-mode-button");
const editModeButton = document.getElementById("edit-mode-button");
const confirmModal = document.getElementById("confirm-modal");
const confirmModalTitle = document.getElementById("confirm-modal-title");
const confirmModalMessage = document.getElementById("confirm-modal-message");
const confirmModalSnoozeCheck = document.getElementById("confirm-modal-snooze-check");
const confirmModalSnoozeMinutes = document.getElementById("confirm-modal-snooze-minutes");
const confirmModalCancel = document.getElementById("confirm-modal-cancel");
const confirmModalOk = document.getElementById("confirm-modal-ok");
const directoryPickerModal = document.getElementById("directory-picker-modal");
const directoryPickerClose = document.getElementById("directory-picker-close");
const directoryPickerQuery = document.getElementById("directory-picker-query");
const directoryPickerSearch = document.getElementById("directory-picker-search");
const directoryPickerUseCurrent = document.getElementById("directory-picker-use-current");
const directoryPickerResults = document.getElementById("directory-picker-results");
const activeTraceLegend = document.getElementById("active-trace-legend");
const graphAddObjectButton = document.getElementById("graph-add-object-button");
const graphAddPanel = document.getElementById("graph-add-panel");
const graphAddPanelClose = document.getElementById("graph-add-panel-close");
const graphAddKindSelect = document.getElementById("graph-add-kind-select");
const graphAddTypeSelect = document.getElementById("graph-add-type-select");
const graphAddLabelInput = document.getElementById("graph-add-label-input");
const graphAddDescriptionInput = document.getElementById("graph-add-description-input");
const graphAddSubmitButton = document.getElementById("graph-add-submit-button");
const graphLinkSelectionSummary = document.getElementById("graph-link-selection-summary");
const graphLinkSelectionClear = document.getElementById("graph-link-selection-clear");

let dragState = null;
let pinchState = null;
let legendDragState = null;

const GRAPH_CARD_BASE = { width: 360, height: 118 };
const GRAPH_CARD_EXPANDED = { width: 460, height: 252 };
const GRAPH_LAYOUT_GAP_X = 84;
const GRAPH_LAYOUT_GAP_Y = 46;
const GRAPH_MIN_ZOOM = 0.25;
const GRAPH_MAX_ZOOM = 2.25;
const DEFAULT_COLUMN_TYPES = ["string", "integer", "float", "boolean", "date", "datetime", "json", "array"];
const TRACE_PALETTE = ["#0b7285", "#c92a2a", "#5f3dc4", "#e67700", "#1c7ed6", "#2b8a3e", "#a61e4d", "#495057"];

async function boot() {
  bindEvents();
  await loadGraph();
  await loadOnboardingPresets({ silent: true });
  renderProjectProfile();
}

async function loadGraph() {
  setStatus("Loading graph...", "Pulling the canonical spec and the latest diagnostics.");
  const response = await fetch("/api/graph");
  const payload = await response.json();
  state.graph = payload.graph;
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.latestPlan = payload.latest_plan || null;
  state.structure = payload.structure || null;
  state.lastArtifacts = null;
  state.bindingSuggestions = {};
  state.selectedNodeId = null;
  state.selectedEdgeId = null;
  state.selectionMode = "node";
  state.dirty = false;
  state.needsAutoLayout = true;
  state.hasManualZoom = false;
  syncAuthoringState();
  render();
  fitGraphToViewport();
  setStatus(
    `${state.graph.nodes.length} nodes loaded`,
    "Search, filter, trace columns, edit metadata, and save to generate the tiered plan."
  );
}

function bindEvents() {
  viewSelect.addEventListener("change", () => {
    state.currentView = viewSelect.value;
    state.needsAutoLayout = true;
    render();
    if (!state.hasManualZoom) {
      fitGraphToViewport();
    }
  });

  impactDirectionSelect.addEventListener("change", () => {
    state.impactDirection = impactDirectionSelect.value;
    render();
  });

  searchInput.addEventListener("input", () => {
    state.searchTerm = searchInput.value.trim().toLowerCase();
    render();
  });

  kindFilterSelect.addEventListener("change", () => {
    state.kindFilter = kindFilterSelect.value;
    state.needsAutoLayout = true;
    render();
    if (!state.hasManualZoom) {
      fitGraphToViewport();
    }
  });

  centerSelectionButton.addEventListener("click", () => centerSelection());
  removeSelectionButton.addEventListener("click", deleteCurrentSelection);
  clearFiltersButton.addEventListener("click", clearFilters);
  undoButton.addEventListener("click", undoGraphChange);
  redoButton.addEventListener("click", redoGraphChange);
  inspectModeButton.addEventListener("click", () => {
    state.interactionMode = "inspect";
    closeGraphContextMenu();
    render();
  });
  editModeButton.addEventListener("click", () => {
    state.interactionMode = "edit";
    closeGraphContextMenu();
    render();
  });
  zoomInButton.addEventListener("click", () => adjustZoom(0.05));
  zoomOutButton.addEventListener("click", () => adjustZoom(-0.05));
  zoomResetButton.addEventListener("click", resetZoom);
  zoomInput.addEventListener("change", handleZoomInputChange);
  zoomInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      handleZoomInputChange(event);
    }
  });
  fullscreenButton.addEventListener("click", toggleGraphFullscreen);
  authoringDrawerToggle.addEventListener("click", () => {
    state.authoringDrawerOpen = !state.authoringDrawerOpen;
    updateToolbarState();
  });
  authoringDrawerClose.addEventListener("click", () => {
    state.authoringDrawerOpen = false;
    updateToolbarState();
  });
  graphDetailToggle.addEventListener("change", () => toggleGraphDetails(graphDetailToggle.checked));
  graphAddObjectButton.addEventListener("click", () => {
    state.showGraphAddPanel = !state.showGraphAddPanel;
    updateToolbarState();
  });
  graphAddPanelClose.addEventListener("click", () => {
    state.showGraphAddPanel = false;
    updateToolbarState();
  });
  graphAddKindSelect.addEventListener("change", () => {
    state.authoring.node.kind = graphAddKindSelect.value;
    const options = getExtensionOptions(state.authoring.node.kind);
    state.authoring.node.extensionType = options[0];
    syncGraphAddPanel();
  });
  graphAddTypeSelect.addEventListener("change", () => {
    state.authoring.node.extensionType = graphAddTypeSelect.value;
  });
  graphAddLabelInput.addEventListener("input", () => {
    state.authoring.node.label = graphAddLabelInput.value;
  });
  graphAddDescriptionInput.addEventListener("input", () => {
    state.authoring.node.description = graphAddDescriptionInput.value;
  });
  graphAddSubmitButton.addEventListener("click", () => {
    createAuthoringNode({ fromGraphPanel: true, attachPickedColumns: true });
  });
  graphLinkSelectionClear.addEventListener("click", () => {
    clearPickedColumns();
    render();
  });
  graphScroller.addEventListener("wheel", handleGraphWheel, { passive: false });
  graphScroller.addEventListener("dblclick", handleGraphDoubleClick);
  graphScroller.addEventListener("touchstart", handleGraphTouchStart, { passive: false });
  graphScroller.addEventListener("touchmove", handleGraphTouchMove, { passive: false });
  graphScroller.addEventListener("touchend", handleGraphTouchEnd, { passive: false });
  graphCanvas.addEventListener("input", handleGraphCanvasMutation);
  graphCanvas.addEventListener("change", handleGraphCanvasMutation);
  graphCanvas.addEventListener("click", handleGraphCanvasClick);
  graphCanvas.addEventListener("contextmenu", handleGraphCanvasContextMenu);
  graphCanvas.addEventListener("dragstart", handleGraphCanvasDragStart);
  graphCanvas.addEventListener("dragover", handleGraphCanvasDragOver);
  graphCanvas.addEventListener("drop", handleGraphCanvasDrop);
  graphLegendToggle.addEventListener("click", toggleLegendPanel);
  graphLegendPanel.addEventListener("mousedown", beginLegendDrag);
  nodeDetail.addEventListener("toggle", handleInspectorToggle, true);
  document.addEventListener("click", () => closeGraphContextMenu());
  confirmModalCancel.addEventListener("click", cancelDestructiveConfirm);
  confirmModalOk.addEventListener("click", confirmDestructiveAction);
  confirmModal.addEventListener("click", handleModalBackdropClick);
  directoryPickerClose.addEventListener("click", closeDirectoryPicker);
  directoryPickerSearch.addEventListener("click", () => searchProjectDirectories());
  directoryPickerUseCurrent.addEventListener("click", () => {
    state.projectProfileOptions.rootPath = "";
    closeDirectoryPicker();
    renderProjectProfile();
  });
  directoryPickerModal.addEventListener("click", handleModalBackdropClick);
  directoryPickerResults.addEventListener("click", handleDirectoryPickerClick);
  directoryPickerQuery.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      searchProjectDirectories();
    }
  });

  projectProfileButton.addEventListener("click", async () => {
    await loadProjectProfile();
  });

  validateButton.addEventListener("click", async () => {
    validateButton.disabled = true;
    await validateCurrentGraph({ updateStatus: true });
    validateButton.disabled = false;
  });

  saveButton.addEventListener("click", async () => {
    saveButton.disabled = true;
    const validation = await validateCurrentGraph({ updateStatus: false });
    if (!validation || (validation.errors || []).length) {
      saveButton.disabled = false;
      const errorCount = validation?.summary?.errors || 0;
      setStatus(
        "Save blocked",
        errorCount ? `${errorCount} blocking validation issue${errorCount === 1 ? "" : "s"} need attention before saving.` : "Validation failed."
      );
      render();
      return;
    }
    setStatus("Saving graph...", "Computing deterministic diff and tiered impact plan.");
    const response = await fetch("/api/graph/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ graph: state.graph }),
    });
    const payload = await response.json();
    saveButton.disabled = false;
    if (!response.ok) {
      setStatus("Save failed", payload.detail || payload.error || "Validation failed.");
      return;
    }
    state.graph = payload.graph;
    state.diagnostics = payload.diagnostics || {};
    state.validationReport = payload.validation || validation;
    state.structure = payload.structure || state.structure;
    state.latestPlan = payload.plan;
    state.lastArtifacts = payload.artifacts || null;
    state.dirty = false;
    render();
    setStatus("Plan generated", "Tiered impact report written without applying code changes.");
  });

  refreshButton.addEventListener("click", async () => {
    if (state.dirty && !window.confirm("You have unsaved edits. Refreshing profiles will discard local changes. Continue?")) {
      return;
    }
    refreshButton.disabled = true;
    setStatus("Refreshing profiles...", "Profiling accessible datasets and merging summary stats.");
    const response = await fetch("/api/profile/refresh", { method: "POST" });
    const payload = await response.json();
    refreshButton.disabled = false;
    if (!response.ok) {
      setStatus("Profile refresh failed", payload.detail || payload.error || "Unable to refresh profiles.");
      return;
    }
    state.graph = payload.graph;
    state.diagnostics = payload.diagnostics || {};
    state.validationReport = payload.validation || null;
    state.structure = payload.structure || state.structure;
    state.latestPlan = payload.latest_plan || state.latestPlan;
    state.lastArtifacts = null;
    state.dirty = false;
    render();
    setStatus("Profiles refreshed", "Quick stats updated where data was accessible.");
  });

  nodeDetail.addEventListener("input", handleDetailMutation);
  nodeDetail.addEventListener("change", handleDetailMutation);
  nodeDetail.addEventListener("click", handleDetailClick);
  validationSummary.addEventListener("click", handleDetailClick);
  if (structureSummary) {
    structureSummary.addEventListener("input", handleStructureMutation);
    structureSummary.addEventListener("change", handleStructureMutation);
    structureSummary.addEventListener("click", handleStructureClick);
  }
  authoringPanel.addEventListener("input", handleAuthoringMutation);
  authoringPanel.addEventListener("change", handleAuthoringMutation);
  authoringPanel.addEventListener("click", handleAuthoringClick);
  authoringPanel.addEventListener("input", handleStructureMutation);
  authoringPanel.addEventListener("change", handleStructureMutation);
  authoringPanel.addEventListener("click", handleStructureClick);
  projectProfileSummary.addEventListener("change", handleProjectProfileMutation);
  projectProfileSummary.addEventListener("click", handleProjectProfileClick);

  document.addEventListener("mousemove", onMouseMove);
  document.addEventListener("mouseup", onMouseUp);
  document.addEventListener("keydown", handleGlobalKeyDown);
  window.addEventListener("resize", handleWindowResize);

  window.addEventListener("beforeunload", (event) => {
    if (!state.dirty) return;
    event.preventDefault();
    event.returnValue = "";
  });

  const tooltip = document.getElementById("node-peek-tooltip");
  document.addEventListener("mouseover", (event) => {
    const nodeEl = event.target.closest(".graph-node");
    if (nodeEl && nodeEl.dataset.nodeId) {
      const node = getNodeById(nodeEl.dataset.nodeId);
      if (node && tooltip) {
        tooltip.innerHTML = `<strong>${escapeHtml(node.label || node.id)}</strong><br><span style="color: var(--muted)">${escapeHtml(node.kind)} | ${node.columns?.length || 0} fields</span>`;
        tooltip.classList.add("visible");
      }
    }
  });
  document.addEventListener("mousemove", (event) => {
    if (tooltip && tooltip.classList.contains("visible")) {
      tooltip.style.left = `${event.clientX + 15}px`;
      tooltip.style.top = `${event.clientY + 15}px`;
    }
  });
  document.addEventListener("mouseout", (event) => {
    if (event.target.closest(".graph-node") && tooltip) {
      tooltip.classList.remove("visible");
    }
  });

  const cmdPalette = document.getElementById("command-palette");
  const cmdInput = document.getElementById("command-palette-input");
  const cmdResults = document.getElementById("command-palette-results");
  document.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === "k") {
      event.preventDefault();
      if (cmdPalette.open) {
        cmdPalette.close();
      } else {
        cmdPalette.showModal();
        cmdInput.value = "";
        cmdResults.innerHTML = state.graph.nodes.slice(0, 10).map(n => `<div class="command-palette-item" data-select-node="${n.id}"><div class="command-palette-item-title">${escapeHtml(n.label || n.id)}</div><div class="command-palette-item-meta">${escapeHtml(n.kind)}</div></div>`).join("");
        cmdInput.focus();
      }
    }
  });
  if (cmdInput && cmdResults) {
    cmdInput.addEventListener("input", (e) => {
      const q = e.target.value.toLowerCase();
      const matches = state.graph.nodes.filter(n => (n.label || n.id).toLowerCase().includes(q) || n.kind.toLowerCase().includes(q)).slice(0, 10);
      cmdResults.innerHTML = matches.map(n => `<div class="command-palette-item" data-select-node="${n.id}"><div class="command-palette-item-title">${escapeHtml(n.label || n.id)}</div><div class="command-palette-item-meta">${escapeHtml(n.kind)}</div></div>`).join("");
    });
    cmdResults.addEventListener("click", (e) => {
      const item = e.target.closest(".command-palette-item");
      if (item && item.dataset.selectNode) {
        state.selectedNodeId = item.dataset.selectNode;
        cmdPalette.close();
        render();
      }
    });
  }
  if (cmdPalette) {
    cmdPalette.addEventListener("click", (event) => {
      if (event.target === cmdPalette) {
        cmdPalette.close();
      }
    });
  }
}

function render() {
  updateToolbarState();
  syncGraphAddPanel();
  renderActiveTraceLegend();
  renderGraph();
  renderSelectionDetail();
  renderPlan();
  renderValidation();
  renderStructureMemory();
  renderAuthoringPanel();
  renderProjectProfile();
  renderGraphContextMenu();
  renderConfirmModal();
  renderDirectoryPicker();
}

function renderActiveTraceLegend() {
  if (!activeTraceLegend) {
    return;
  }
  const refs = getActiveTracedColumnRefs();
  if (!refs.length) {
    activeTraceLegend.innerHTML = "";
    return;
  }
  activeTraceLegend.innerHTML = refs.map((ref) => `
    <span class="legend-chip trace-seed" style="--trace-color: ${escapeHtml(getTraceSeedColor(ref))}">
      ${escapeHtml(ref)}
    </span>
  `).join("");
}

function toggleLegendPanel(event) {
  event.preventDefault();
  event.stopPropagation();
  state.legendCollapsed = !state.legendCollapsed;
  updateToolbarState();
}

function positionLegendPanel() {
  if (!graphLegendPanel || !graphScroller) {
    return;
  }
  if (!state.legendPosition) {
    const width = graphLegendPanel.offsetWidth || 244;
    state.legendPosition = {
      x: Math.max(12, graphScroller.clientWidth - width - 16),
      y: 14,
    };
  }
  graphLegendPanel.style.left = `${state.legendPosition.x}px`;
  graphLegendPanel.style.top = `${state.legendPosition.y}px`;
}

async function validateCurrentGraph(options = {}) {
  const response = await fetch("/api/graph/validate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ graph: state.graph }),
  });
  const payload = await response.json();
  if (!response.ok) {
    state.validationReport = {
      errors: [{ message: payload.detail || payload.error || "Validation failed." }],
      warnings: [],
      summary: { errors: 1, warnings: 0 },
    };
    if (options.updateStatus) {
      setStatus("Validation failed", payload.detail || payload.error || "Unable to validate the graph.");
    }
    renderValidation();
    return state.validationReport;
  }
  state.graph = payload.graph;
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.structure = payload.structure || state.structure;
  if (options.updateStatus) {
    const summary = state.validationReport?.summary || {};
    setStatus(
      summary.errors ? "Validation found issues" : "Validation passed",
      `${formatValue(summary.errors)} blocking issue${summary.errors === 1 ? "" : "s"}, ${formatValue(summary.warnings)} warning${summary.warnings === 1 ? "" : "s"}.`
    );
  }
  render();
  return state.validationReport;
}

function updateToolbarState() {
  viewSelect.value = state.currentView;
  impactDirectionSelect.value = state.impactDirection;
  searchInput.value = state.searchTerm;
  kindFilterSelect.value = state.kindFilter;
  dirtyIndicator.textContent = state.dirty ? "Unsaved local edits" : "All changes saved";
  dirtyIndicator.classList.toggle("dirty", state.dirty);
  zoomInput.value = String(Math.round(state.zoom * 100));
  graphDetailToggle.checked = state.showGraphDetails;
  fullscreenButton.textContent = state.graphFullscreen ? "Exit full screen" : "Full screen";
  canvasPanel.classList.toggle("fullscreen", state.graphFullscreen);
  shell.classList.toggle("graph-fullscreen", state.graphFullscreen);
  graphLegendPanel.classList.toggle("collapsed", state.legendCollapsed);
  graphLegendToggle.textContent = state.legendCollapsed ? "+" : "−";
  if (state.interactionMode !== "edit") {
    state.showGraphAddPanel = false;
  }
  graphAddPanel.hidden = !state.showGraphAddPanel;
  graphAddObjectButton.classList.toggle("primary", state.showGraphAddPanel);
  graphAddObjectButton.disabled = state.interactionMode !== "edit";
  inspectModeButton.classList.toggle("active", state.interactionMode === "inspect");
  editModeButton.classList.toggle("active", state.interactionMode === "edit");
  const hasSelection = Boolean((state.selectionMode === "edge" && state.selectedEdgeId) || state.selectedNodeId);
  removeSelectionButton.disabled = state.interactionMode !== "edit" || !hasSelection;
  removeSelectionButton.hidden = state.interactionMode !== "edit";
  if (state.selectionMode === "edge" && state.selectedEdgeId) {
    removeSelectionButton.textContent = "Remove connection";
  } else {
    removeSelectionButton.textContent = "Remove selected";
  }
  inspectorModePill.textContent = state.interactionMode === "edit" ? "Edit" : "Inspect";
  authoringDrawer.hidden = !state.authoringDrawerOpen;
  authoringDrawerToggle.classList.toggle("primary", state.authoringDrawerOpen);
  canvasPanel.classList.toggle("edit-mode", state.interactionMode === "edit");
  positionLegendPanel();
  undoButton.disabled = !state.historyPast.length;
  redoButton.disabled = !state.historyFuture.length;
}

function syncGraphAddPanel() {
  if (!graphAddPanel) {
    return;
  }
  const nodeDraft = state.authoring.node;
  const typeOptions = getExtensionOptions(nodeDraft.kind);
  if (!typeOptions.includes(nodeDraft.extensionType)) {
    nodeDraft.extensionType = typeOptions[0];
  }
  graphAddKindSelect.value = nodeDraft.kind;
  graphAddTypeSelect.innerHTML = typeOptions
    .map((value) => `<option value="${escapeHtml(value)}" ${value === nodeDraft.extensionType ? "selected" : ""}>${escapeHtml(value)}</option>`)
    .join("");
  graphAddLabelInput.value = nodeDraft.label;
  graphAddDescriptionInput.value = nodeDraft.description;
  const pickedRefs = getPickedColumnRefs();
  graphLinkSelectionSummary.textContent = pickedRefs.length
    ? `${pickedRefs.length} picked column${pickedRefs.length === 1 ? "" : "s"}`
    : "No columns picked";
  graphLinkSelectionClear.disabled = !pickedRefs.length;
  graphAddSubmitButton.textContent = pickedRefs.length ? "Create + link picked" : "Create object";
}

function renderGraph() {
  const projected = getViewGraph();
  const highlight = getHighlightContext(projected);
  if (state.needsAutoLayout && !dragState) {
    applyLaneLayout(projected.nodes);
    state.needsAutoLayout = false;
  }
  const routingContext = buildRoutingContext(projected.nodes);
  const dimensions = getCanvasDimensions(projected.nodes);

  graphCanvas.innerHTML = "";
  graphEdges.innerHTML = "";
  graphViewport.style.minWidth = `${dimensions.width}px`;
  graphViewport.style.minHeight = `${dimensions.height}px`;
  graphViewport.style.transform = `scale(${state.zoom})`;
  graphCanvas.style.minWidth = `${dimensions.width}px`;
  graphCanvas.style.minHeight = `${dimensions.height}px`;
  graphEdges.setAttribute("width", dimensions.width);
  graphEdges.setAttribute("height", dimensions.height);
  visibleCount.textContent = `${projected.nodes.length} nodes / ${projected.edges.length} edges`;
  renderGraphLayerBands(projected.nodes, dimensions);

  const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
  graphEdges.appendChild(defs);
  const markerIds = new Map();

  projected.edges.forEach((edge) => {
    const source = getNodeById(edge.source);
    const target = getNodeById(edge.target);
    if (!source || !target) return;
    const routed = getRoutedEdgeGeometry(source, target, routingContext);
    const traceColors = getEdgeTraceColors(edge);
    const edgeStateClasses = getEdgeVisualStateClasses(edge.id, highlight.edgeIds).join(" ");
    const edgeColor = getEdgeBaseColor(edge);
    const markerId = ensureEdgeMarker(defs, markerIds, edgeColor);

    if (traceColors.length) {
      const outline = document.createElementNS("http://www.w3.org/2000/svg", "path");
      outline.setAttribute("d", routed.path);
      outline.setAttribute("data-edge-id", edge.id);
      outline.setAttribute("class", `edge-path edge-trace-outline ${edgeStateClasses}`.trim());
      outline.style.stroke = "rgba(31, 26, 23, 0.22)";
      graphEdges.appendChild(outline);

      const offsets = traceColors.map((_, index) => (index - ((traceColors.length - 1) / 2)) * 2.2);
      traceColors.forEach((color, index) => {
        const lane = document.createElementNS("http://www.w3.org/2000/svg", "path");
        lane.setAttribute("d", buildOffsetRoutedPath(routed, offsets[index]));
        lane.setAttribute("data-edge-id", edge.id);
        lane.setAttribute("class", `edge-path edge-trace-lane ${edgeStateClasses}`.trim());
        lane.style.stroke = color;
        const laneMarkerId = ensureEdgeMarker(defs, markerIds, color);
        lane.setAttribute("marker-end", `url(#${laneMarkerId})`);
        graphEdges.appendChild(lane);
      });
    } else {
      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", routed.path);
      path.setAttribute("data-edge-id", edge.id);
      path.setAttribute("class", buildEdgeClass(edge.id, highlight.edgeIds).replace("edge-line", "edge-path"));
      path.style.stroke = edgeColor;
      path.setAttribute("marker-end", `url(#${markerId})`);
      graphEdges.appendChild(path);
    }

    const hit = document.createElementNS("http://www.w3.org/2000/svg", "path");
    hit.setAttribute("d", routed.path);
    hit.setAttribute("data-edge-id", edge.id);
    hit.setAttribute("class", "edge-hit");
    hit.addEventListener("click", (event) => {
      event.stopPropagation();
      state.selectionMode = "edge";
      state.selectedEdgeId = edge.id;
      render();
    });
    graphEdges.appendChild(hit);

    const mappingLabel = summarizeEdgeMappings(edge);
    if (mappingLabel) {
      const labelColor = traceColors[0] || edgeColor;
      const labelWidth = Math.max(72, Math.min(220, mappingLabel.length * 6.2));
      const labelBg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      labelBg.setAttribute("x", String(routed.labelX - labelWidth / 2));
      labelBg.setAttribute("y", String(routed.labelY - 12));
      labelBg.setAttribute("width", String(labelWidth));
      labelBg.setAttribute("height", "20");
      labelBg.setAttribute("rx", "8");
      labelBg.setAttribute("ry", "8");
      labelBg.setAttribute("class", "edge-label-bg");
      labelBg.setAttribute("stroke", labelColor);
      graphEdges.appendChild(labelBg);

      const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
      label.setAttribute("x", String(routed.labelX));
      label.setAttribute("y", String(routed.labelY + 2));
      label.setAttribute("text-anchor", "middle");
      label.setAttribute("class", "edge-label-text");
      label.setAttribute("fill", labelColor);
      label.textContent = mappingLabel;
      graphEdges.appendChild(label);
    }
  });

  projected.nodes.forEach((node) => {
    const size = getGraphNodeSize(node);
    const element = document.createElement("article");
    element.className = buildNodeClass(node.id, highlight.nodeIds);
    element.style.left = `${node.position.x}px`;
    element.style.top = `${node.position.y}px`;
    element.style.width = `${size.width}px`;
    element.style.minHeight = `${size.height}px`;
    element.style.zIndex = String(getGraphNodeZIndex(node.id));
    element.style.setProperty("--node-extension-accent", getNodeExtensionColor(node));
    element.dataset.storageKind = getNodeStorageKind(node);
    element.dataset.extensionType = node.extension_type;
    element.dataset.nodeId = node.id;
    if (isGraphNodeExpanded(node.id)) {
      element.classList.add("expanded");
    }
    element.innerHTML = renderGraphNodeCard(node);
    element.querySelectorAll("button, input, select, textarea, label, summary").forEach((interactive) => {
      interactive.addEventListener("pointerdown", stopGraphInteractiveGesture);
      interactive.addEventListener("mousedown", stopGraphInteractiveGesture);
      interactive.addEventListener("touchstart", stopGraphInteractiveGesture, { passive: false });
    });
    element.addEventListener("mousedown", (event) => {
      if (event.target instanceof HTMLElement && event.target.closest("button, input, select, textarea, label, summary, details, [data-category-chip]")) {
        return;
      }
      beginDrag(event, node.id);
    });
    element.addEventListener("click", (event) => {
      if (event.target instanceof HTMLElement && event.target.closest("button, input, select, textarea, label, [data-category-chip]")) {
        return;
      }
      state.selectionMode = "node";
      state.selectedNodeId = node.id;
      state.selectedEdgeId = null;
      render();
    });
    const expandButton = element.querySelector("[data-graph-toggle-expand]");
    if (expandButton) {
      expandButton.addEventListener("mousedown", (event) => event.stopPropagation());
      expandButton.addEventListener("click", (event) => {
        event.stopPropagation();
        state.expandedGraphNodes[node.id] = !isGraphNodeExpanded(node.id);
        state.needsAutoLayout = true;
        if (!state.hasManualZoom) {
          fitGraphToViewport();
        } else {
          renderGraph();
        }
      });
    }
    element.querySelectorAll("[data-graph-table-page]").forEach((button) => {
      button.addEventListener("mousedown", (event) => event.stopPropagation());
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        const [nodeId, direction] = splitDataTailPair(button.dataset.graphTablePage || "");
        if (!nodeId || !direction) return;
        shiftGraphTablePage(nodeId, direction);
      });
    });
    element.querySelectorAll("[data-graph-focus-column]").forEach((button) => {
      button.addEventListener("mousedown", (event) => event.stopPropagation());
      button.addEventListener("click", (event) => {
        event.stopPropagation();
        const ref = button.dataset.graphFocusColumn;
        if (!ref) return;
        state.selectionMode = "node";
        state.selectedNodeId = node.id;
        state.selectedEdgeId = null;
        toggleTracedColumn(ref);
        render();
      });
    });
    element.querySelectorAll("[data-graph-column-add]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        addGraphColumnRow(button.dataset.graphColumnAdd);
      });
    });
    element.querySelectorAll("[data-graph-column-commit]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        const [nodeId, indexText] = splitDataTailPair(button.dataset.graphColumnCommit || "");
        commitGraphColumnRow(nodeId, indexText);
      });
    });
    element.querySelectorAll("[data-graph-column-cancel]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        const [nodeId, indexText] = splitDataTailPair(button.dataset.graphColumnCancel || "");
        cancelGraphColumnRow(nodeId, indexText);
      });
    });
    element.querySelectorAll("[data-graph-column-remove]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        const [nodeId, indexText] = splitDataTailPair(button.dataset.graphColumnRemove || "");
        removeGraphColumnRow(nodeId, indexText);
      });
    });
    element.querySelectorAll("[data-graph-work-item-add]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        addGraphWorkItem(button.dataset.graphWorkItemAdd);
      });
    });
    element.querySelectorAll("[data-graph-work-item-remove]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        const [nodeId, indexText] = splitDataTailPair(button.dataset.graphWorkItemRemove || "");
        removeGraphWorkItem(nodeId, indexText);
      });
    });
    element.querySelectorAll("[data-graph-remove-node]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        deleteNodeById(button.dataset.graphRemoveNode);
      });
    });
    graphCanvas.appendChild(element);
  });
}

function stopGraphInteractiveGesture(event) {
  event.stopPropagation();
}

function renderSelectionDetail() {
  const selection = getCurrentSelection();
  const selectionKey = selection
    ? selection.type === "edge"
      ? `edge:${selection.edge.id}`
      : `node:${selection.node.id}`
    : "";
  if (selectionKey && selectionKey !== state.lastInspectorSelectionKey) {
    state.inspectorSections = {
      overview: true,
      dependencies: false,
      editing: false,
    };
    state.lastInspectorSelectionKey = selectionKey;
  }
  if (!selection || selection.type !== "node") {
    inspectorPanel.classList.remove("open");
    detailHeading.textContent = "Selection Details";
    nodeDetail.innerHTML = "<p>Select a node to inspect dependencies and health.</p>";
    return;
  }

  const node = selection.node;
  inspectorPanel.classList.add("open");
  detailHeading.textContent = "Node Details";
  nodeDetail.innerHTML = renderNodeDetail(node);
}

function renderNodeDetail(node) {
  const diagnostics = getNodeDiagnostics(node.id);
  const tracedRefs = getActiveTracedColumnRefs();
  const selectionPill = tracedRefs.length
    ? `<span class="selection-pill">Tracking ${escapeHtml(String(tracedRefs.length))}</span>`
    : `<span class="selection-pill">${escapeHtml(node.kind)} / ${escapeHtml(node.extension_type)}</span>`;

  return `
    <div class="detail-header">
      <div>
        <h3>${escapeHtml(node.label)}</h3>
        <p class="hint">${escapeHtml(node.id)}</p>
      </div>
      ${selectionPill}
    </div>
    ${renderInspectorSection("overview", "Overview", renderNodeOverview(node, diagnostics), true)}
    ${renderInspectorSection("dependencies", "Dependencies", renderNodeDependencies(node, diagnostics), false)}
    ${renderInspectorSection("editing", "Editing", renderNodeEditing(node), false, {
      disabled: state.interactionMode !== "edit",
      hidden: false,
    })}
  `;
}

function renderEdgeDetail(edge) {
  const source = getNodeById(edge.source);
  const target = getNodeById(edge.target);
  return `
    <div class="detail-header">
      <div>
        <h3>${escapeHtml(edge.type)} edge</h3>
        <p class="hint">${escapeHtml(edge.id)}</p>
      </div>
      <span class="selection-pill">${escapeHtml(edge.source)} -> ${escapeHtml(edge.target)}</span>
    </div>
    ${renderInspectorSection("overview", "Overview", `
      <div class="meta-grid compact-summary">
        <div><strong>Source</strong><br>${escapeHtml(source ? source.label : edge.source)}</div>
        <div><strong>Target</strong><br>${escapeHtml(target ? target.label : edge.target)}</div>
        <div><strong>Type</strong><br>${escapeHtml(edge.type)}</div>
        <div><strong>Mappings</strong><br>${formatValue((edge.column_mappings || []).length)}</div>
      </div>
      <div class="section-actions">
        <button class="ghost-button" type="button" data-center-edge="${escapeHtml(edge.id)}">Center edge</button>
        <button class="ghost-button" type="button" data-select-node="${escapeHtml(edge.source)}">Open source</button>
        <button class="ghost-button" type="button" data-select-node="${escapeHtml(edge.target)}">Open target</button>
        ${state.interactionMode === "edit" ? `<button class="inline-button danger" type="button" data-delete-selection="true">Remove edge</button>` : ""}
      </div>
    `, true)}
    ${renderInspectorSection("dependencies", "Dependencies", renderMappingSection("Column mappings", [edge]), false)}
    ${renderInspectorSection("editing", "Editing", renderEdgeEditing(edge), false, {
      disabled: state.interactionMode !== "edit",
      hidden: false,
    })}
  `;
}

function renderNodeOverview(node, diagnostics) {
  const health = diagnostics.health || "healthy";
  const uiRole = diagnostics.ui_role ? ` / ${diagnostics.ui_role}` : "";
  const refreshMeta = getNodeRefreshMeta(node);
  const dependencyToggle = (node.kind === "data" || node.kind === "contract")
    ? `
      <label class="inline-checkbox">
        <input type="checkbox" data-column-dependency-toggle="${escapeHtml(node.id)}" ${state.columnDependencyToggles[node.id] ? "checked" : ""} />
        <span>Show column-level dependencies</span>
      </label>
    `
    : "";
  return `
    <div class="meta-grid compact-summary">
      <div><strong>Type</strong><br>${escapeHtml(node.kind)} / ${escapeHtml(node.extension_type)}${escapeHtml(uiRole)}</div>
      <div><strong>Owner</strong><br>${escapeHtml(node.owner || "unassigned")}</div>
      <div><strong>Profile</strong><br>${escapeHtml(node.profile_status || "unknown")}</div>
      <div><strong>Status</strong><br>${escapeHtml(node.status || "active")}</div>
      ${refreshMeta ? `<div><strong>Refresh</strong><br>${escapeHtml(refreshMeta.label)}</div>` : ""}
    </div>
    <div class="dependency-summary-pill ${escapeHtml(health)}">
      <span>↑ ${escapeHtml(String(diagnostics.upstream_count || 0))}</span>
      <span>↓ ${escapeHtml(String(diagnostics.downstream_count || 0))}</span>
      <span>${getHealthIcon(health)} ${escapeHtml(health)}</span>
    </div>
    ${diagnostics.node_issues?.length ? `
      <div class="issue-stack">
        ${diagnostics.node_issues.map((issue) => `
          <div class="issue-chip ${escapeHtml(issue.severity || "warning")}">${escapeHtml(issue.message || "")}</div>
        `).join("")}
      </div>
    ` : '<div class="success-chip">✓ No issues</div>'}
    <div class="section-actions">
      <button class="ghost-button" type="button" data-center-node="${escapeHtml(node.id)}">Center node</button>
      ${getActiveTracedColumnRefs().length ? '<button class="ghost-button" type="button" data-clear-column-focus="true">Clear traces</button>' : ""}
    </div>
    ${dependencyToggle}
  `;
}

function renderNodeDependencies(node, diagnostics) {
  const incoming = state.graph.edges.filter((edge) => edge.target === node.id);
  const outgoing = state.graph.edges.filter((edge) => edge.source === node.id);
  const dependencySummary = [];
  if (incoming.length) {
    dependencySummary.push(`<div class="dependency-block"><strong>Incoming</strong><ul>${incoming.map((edge) => `<li>${escapeHtml(getNodeById(edge.source)?.label || edge.source)}</li>`).join("")}</ul></div>`);
  }
  if (outgoing.length) {
    dependencySummary.push(`<div class="dependency-block"><strong>Outgoing</strong><ul>${outgoing.map((edge) => `<li>${escapeHtml(getNodeById(edge.target)?.label || edge.target)}</li>`).join("")}</ul></div>`);
  }
  const issues = collectNodeDependencyIssues(node, diagnostics);
  const bindings = renderInspectBindings(node, diagnostics);
  return `
    <div class="dependency-columns">
      ${dependencySummary.join("") || '<div class="success-chip">✓ No issues</div>'}
    </div>
    ${issues.length ? `
      <div class="section">
        <h3>Why this matters</h3>
        <ul class="warning-list">
          ${issues.map((issue) => `<li>${escapeHtml(issue)}</li>`).join("")}
        </ul>
      </div>
    ` : ""}
    ${bindings}
  `;
}

function renderNodeEditing(node) {
  if (state.interactionMode !== "edit") {
    return '<p class="hint">Switch to Edit mode to change bindings, metadata, notes, and structure.</p>';
  }
  if (node.kind === "data") {
    return `
      <div class="section">
        <h3>Metadata</h3>
        <div class="form-grid">
          ${renderNodeField("Label", "label", node.label, { liveRender: true, full: true })}
          ${renderNodeTextarea("Description", "description", node.description || "", { liveRender: true, full: true })}
          ${renderNodeField("Owner", "owner", node.owner || "")}
          ${renderNodeField("Sensitivity", "sensitivity", node.sensitivity || "")}
          ${renderNodeSelect("Status", "status", node.status || "active", ["active", "draft", "deprecated", "archived"])}
          ${renderNodeSelect("Profile status", "profile_status", node.profile_status || "unknown", ["unknown", "schema_only", "profiled", "sampled_profile"])}
          ${renderNodeTextarea("Notes", "notes", node.notes || "", { full: true })}
        </div>
      </div>
      ${renderDataSection(node)}
    `;
  }
  if (node.kind === "compute") {
    return renderComputeSection(node);
  }
  if (node.kind === "contract") {
    return renderContractSection(node, getContractDiagnostics(node.id));
  }
  return renderSourceSection(node);
}

function renderEdgeEditing(edge) {
  if (state.interactionMode !== "edit") {
    return '<p class="hint">Switch to Edit mode to change edge metadata or mappings.</p>';
  }
  const mappings = (edge.column_mappings || []).map((mapping, index) => `
    <div class="mapping-item ${isMappingFocused(edge, mapping) ? "focused" : ""}">
      <div class="column-head">
        <div class="column-main">${escapeHtml(edge.type)} mapping ${index + 1}</div>
        <div class="row-actions">
          <button class="text-button" type="button" data-focus-column="${escapeHtml(edge.source)}.${escapeHtml(mapping.source_column)}">Trace source</button>
          <button class="text-button" type="button" data-focus-column="${escapeHtml(edge.target)}.${escapeHtml(mapping.target_column)}">Trace target</button>
          <button class="inline-button danger" type="button" data-edge-remove-mapping="${index}">Remove</button>
        </div>
      </div>
      <div class="form-grid compact">
        <label class="form-field">
          Source column
          <input data-edge-mapping-index="${index}" data-edge-mapping-side="source_column" value="${escapeHtml(mapping.source_column || "")}" />
        </label>
        <label class="form-field">
          Target column
          <input data-edge-mapping-index="${index}" data-edge-mapping-side="target_column" value="${escapeHtml(mapping.target_column || "")}" />
        </label>
      </div>
    </div>
  `).join("");
  return `
    <div class="form-grid">
      <label class="form-field form-field-full">
        Label
        <input data-edge-path="label" data-live-render="graph" value="${escapeHtml(edge.label || "")}" />
      </label>
      <label class="form-field form-field-full">
        Notes
        <textarea data-edge-path="notes">${escapeHtml(edge.notes || "")}</textarea>
      </label>
    </div>
    <div class="section-actions">
      <button class="ghost-button" type="button" data-edge-add-mapping="true">Add mapping</button>
    </div>
    <div class="mapping-list">${mappings || '<p class="hint">No mappings yet.</p>'}</div>
  `;
}

function getNodeDiagnostics(nodeId) {
  return state.diagnostics?.nodes?.[nodeId] || {
    upstream_count: 0,
    downstream_count: 0,
    health: "healthy",
    node_issues: [],
    bindings: {},
    ui_role: "",
  };
}

function getContractDiagnostics(nodeId) {
  return state.diagnostics?.contracts?.[nodeId] || state.diagnostics?.[nodeId] || {
    missing_dependencies: [],
    unused_fields: [],
    breakage_warnings: [],
    bindings: {},
    node_issues: [],
    health: "healthy",
  };
}

function renderInspectorSection(sectionKey, title, content, defaultOpen = false, options = {}) {
  if (options.hidden) {
    return "";
  }
  const isOpen = state.inspectorSections[sectionKey] ?? defaultOpen;
  const disabled = options.disabled ? "data-disabled=\"true\"" : "";
  const disabledClass = options.disabled ? "disabled" : "";
  return `
    <details class="detail-accordion inspector-section ${disabledClass}" data-inspector-section="${escapeHtml(sectionKey)}" ${isOpen ? "open" : ""} ${disabled}>
      <summary>${escapeHtml(title)}</summary>
      <div class="detail-accordion-content">${content}</div>
    </details>
  `;
}

function handleInspectorToggle(event) {
  const target = event.target;
  if (!(target instanceof HTMLDetailsElement)) {
    return;
  }
  const section = target.dataset.inspectorSection;
  if (!section) {
    return;
  }
  if (target.dataset.disabled === "true" && target.open) {
    target.open = false;
    return;
  }
  state.inspectorSections[section] = target.open;
}

function getHealthIcon(health) {
  if (health === "broken") {
    return "⚠";
  }
  if (health === "warning") {
    return "△";
  }
  return "✓";
}

function collectNodeDependencyIssues(node, diagnostics) {
  const messages = [];
  (diagnostics.node_issues || []).forEach((issue) => {
    if (issue.message) {
      messages.push(issue.message);
    }
  });
  if (node.kind === "contract") {
    Object.values(getContractDiagnostics(node.id).bindings || {}).forEach((binding) => {
      if (binding.why_this_matters) {
        messages.push(binding.why_this_matters);
      }
    });
  }
  return [...new Set(messages)];
}

function renderInspectBindings(node, diagnostics) {
  if (node.kind !== "contract" && node.kind !== "data") {
    return "";
  }
  const showColumnDependencies = Boolean(state.columnDependencyToggles[node.id]);
  const bindings = Object.values(diagnostics.bindings || {});
  if (!bindings.length) {
    return '<div class="success-chip">✓ No issues</div>';
  }
  const rows = bindings.map((binding) => {
    const bindingRef = `${node.id}.${binding.name}`;
    const rebindLabel = isPendingBindingTarget(node.id, binding.name)
      ? "Cancel target"
      : state.pendingColumnLinkRef
        ? "Connect here"
        : "Rebind";
    const hoverActions = `
      <div class="binding-row-actions">
        <button class="text-button" type="button" data-focus-column="${escapeHtml(bindingRef)}">${isColumnTraced(bindingRef) ? "Untrace" : "Trace"}</button>
        ${state.interactionMode === "edit" && node.kind === "contract"
          ? `<button class="text-button" type="button" data-graph-contract-link="${escapeHtml(node.id)}:${escapeHtml(binding.name)}">${rebindLabel}</button>
             <button class="text-button danger-link" type="button" data-graph-contract-field-remove-name="${escapeHtml(node.id)}:${escapeHtml(binding.name)}">Remove</button>`
          : ""}
      </div>
    `;
    const dependencySummary = showColumnDependencies
      ? `
        <span class="binding-row-health ${escapeHtml(binding.health || "healthy")}">
          ↑ ${escapeHtml(String(binding.upstream_count || 0))}
          ↓ ${escapeHtml(String(binding.downstream_count || 0))}
          ${getHealthIcon(binding.health || "healthy")} ${escapeHtml(binding.health || "healthy")}
        </span>
      `
      : "";
    if (node.kind === "contract") {
      const sourceText = binding.primary_binding
        ? `${binding.name} -> ${binding.primary_binding}`
        : `${binding.name} -> broken`;
      const alternatives = binding.alternative_count
        ? `<details class="binding-alt-sources"><summary>+${binding.alternative_count} alternatives</summary><ul>${(binding.alternative_bindings || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul></details>`
        : "";
      return `
        <div class="binding-row ${escapeHtml(binding.health || "healthy")}" data-binding-ref="${escapeHtml(bindingRef)}" ${getLineageStyleAttribute(bindingRef)}>
          <div class="binding-row-main">
            <div>
              <div class="binding-row-title">${escapeHtml(sourceText)}</div>
              ${binding.why_this_matters ? `<div class="binding-row-impact">${escapeHtml(binding.why_this_matters)}</div>` : ""}
            </div>
            ${dependencySummary}
          </div>
          ${hoverActions}
          ${alternatives}
        </div>
      `;
    }
    return `
      <div class="binding-row ${escapeHtml(binding.health || "healthy")}" data-binding-ref="${escapeHtml(bindingRef)}" ${getLineageStyleAttribute(bindingRef)}>
        <div class="binding-row-main">
          <div class="binding-row-title">${escapeHtml(binding.name)}</div>
          ${dependencySummary}
        </div>
        ${hoverActions}
      </div>
    `;
  }).join("");
  return `
    <div class="section">
      <h3>${node.kind === "contract" ? "Contract bindings" : "Column health"}</h3>
      <div class="binding-list">${rows}</div>
    </div>
  `;
}

function renderDataSection(node) {
  const dataMeta = node.data || {};
  const columns = (node.columns || []).map((column, index) => {
    const ref = `${node.id}.${column.name}`;
    const stats = Object.entries(column.stats || {})
      .map(([key, value]) => `${key}: ${Array.isArray(value) ? JSON.stringify(value) : value}`)
      .join(", ");
    const keyParts = [];
    if (column.primary_key) keyParts.push("PK");
    if (column.foreign_key) keyParts.push(`FK ${column.foreign_key}`);
    if (column.indexed) keyParts.push("indexed");
    return `
      <div class="column-row ${isColumnHighlightedRef(ref) ? "focused" : ""}">
        <div class="column-head">
          <div class="column-main">${escapeHtml(column.name)}</div>
          <div class="row-actions">
            <span class="pill">${escapeHtml(column.data_type)}</span>
            <button class="text-button" type="button" data-focus-column="${escapeHtml(ref)}">${isColumnTraced(ref) ? "Untrace" : "Trace lineage"}</button>
            <button class="inline-button danger" type="button" data-column-remove="${index}">Remove</button>
          </div>
        </div>
        <div class="form-grid compact">
          <label class="form-field">
            Type
            <input data-column-index="${index}" data-column-field="data_type" value="${escapeHtml(column.data_type || "")}" />
          </label>
          <label class="form-field">
            Nullable
            <select data-column-index="${index}" data-column-field="nullable" data-coerce="boolean">
              <option value="true" ${column.nullable ? "selected" : ""}>true</option>
              <option value="false" ${!column.nullable ? "selected" : ""}>false</option>
            </select>
          </label>
          <label class="form-field form-field-full">
            Description
            <input data-column-index="${index}" data-column-field="description" value="${escapeHtml(column.description || "")}" />
          </label>
          <label class="form-field form-field-full">
            Notes
            <input data-column-index="${index}" data-column-field="notes" value="${escapeHtml(column.notes || "")}" />
          </label>
        </div>
        <div class="column-meta">null %: ${formatValue(column.null_pct)} | stats: ${escapeHtml(stats || "missing")}</div>
        <div class="column-meta">${escapeHtml(keyParts.join(" | ") || "no primary/foreign key or index metadata recorded")}</div>
      </div>
    `;
  }).join("");

  return `
    <div class="section">
      <h3>Dataset metadata</h3>
      <div class="form-grid">
        <label class="form-field">
          Persistence
          <select data-node-path="data.persistence">
            ${["cold", "warm", "hot", "transient", ""].map((value) => `<option value="${value}" ${dataMeta.persistence === value ? "selected" : ""}>${value || "missing"}</option>`).join("")}
          </select>
        </label>
        <label class="form-field">
          Update cadence
          <input data-node-path="data.update_frequency" value="${escapeHtml(dataMeta.update_frequency || "")}" />
        </label>
        <label class="form-field">
          Local path
          <input data-node-path="data.local_path" value="${escapeHtml(dataMeta.local_path || "")}" />
        </label>
        <label class="form-field">
          Persisted
          <select data-node-path="data.persisted" data-coerce="boolean">
            <option value="true" ${dataMeta.persisted ? "selected" : ""}>true</option>
            <option value="false" ${!dataMeta.persisted ? "selected" : ""}>false</option>
          </select>
        </label>
      </div>
      <div class="meta-grid">
        <div><strong>Observations</strong><br>${formatValue(dataMeta.row_count)}</div>
        <div><strong>Profile target</strong><br>${formatValue(dataMeta.profile_target)}</div>
        <div><strong>Sampled profile</strong><br>${dataMeta.sampled ? "Yes" : "No"}</div>
        <div><strong>Status</strong><br>${escapeHtml(node.profile_status)}</div>
        <div><strong>Indices</strong><br>${escapeHtml((dataMeta.indices || []).join(", ") || "none recorded")}</div>
      </div>
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Columns</h3>
        <button class="ghost-button" type="button" data-column-add="true">Add column</button>
      </div>
      <div class="column-list">${columns || "<p>No columns recorded.</p>"}</div>
    </div>
  `;
}

function renderComputeSection(node) {
  const compute = node.compute || {};
  const featureSelection = renderFeatureSelectionList(node);
  const selectedCount = (compute.feature_selection || []).filter((feature) => feature.status === "selected").length;

  return `
    <div class="section">
      <h3>Compute metadata</h3>
      <div class="form-grid">
        <label class="form-field">
          Runtime
          <input data-node-path="compute.runtime" value="${escapeHtml(compute.runtime || "")}" />
        </label>
        <label class="form-field">
          Type
          <input value="${escapeHtml(node.extension_type)}" disabled />
        </label>
        <label class="form-field form-field-full">
          Compute notes
          <textarea data-node-path="compute.notes">${escapeHtml(compute.notes || "")}</textarea>
        </label>
      </div>
      <div class="meta-grid">
        <div><strong>Inputs</strong><br>${formatValue((compute.inputs || []).length)}</div>
        <div><strong>Outputs</strong><br>${formatValue((compute.outputs || []).length)}</div>
        <div><strong>Selected features</strong><br>${selectedCount}</div>
        <div><strong>Total features</strong><br>${formatValue((compute.feature_selection || []).length)}</div>
      </div>
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Feature selection</h3>
        <label class="form-field">
          View mode
          <select data-feature-sort="true">
            <option value="category" ${state.featureSortMode === "category" ? "selected" : ""}>Category / order</option>
            <option value="status" ${state.featureSortMode === "status" ? "selected" : ""}>Selected first</option>
            <option value="original" ${state.featureSortMode === "original" ? "selected" : ""}>Spec order</option>
          </select>
        </label>
        <button class="ghost-button" type="button" data-feature-add="true">Add feature</button>
      </div>
      <div class="column-list">${featureSelection || "<p>No feature selections defined.</p>"}</div>
    </div>
  `;
}

function renderContractSection(node, diagnostics) {
  const fields = (node.contract.fields || []).map((field, index) => {
    const ref = `${node.id}.${field.name}`;
    const sources = field.sources || [];
    const suggestionState = state.bindingSuggestions[getContractFieldSuggestionKey(node.id, index)] || null;
    return `
      <div class="column-row ${isColumnHighlightedRef(ref) ? "focused" : ""}">
        <div class="column-head">
          <div class="column-main">${escapeHtml(field.name)}</div>
          <div class="row-actions">
            <button class="text-button" type="button" data-focus-column="${escapeHtml(ref)}">${isColumnTraced(ref) ? "Untrace" : "Trace lineage"}</button>
            <button class="text-button" type="button" data-contract-field-suggest="${index}">Suggest bindings</button>
            <button class="inline-button danger" type="button" data-contract-field-remove="${index}">Remove</button>
          </div>
        </div>
        <div class="form-grid compact">
          <label class="form-field">
            Field name
            <input data-contract-field-index="${index}" data-contract-field-prop="name" value="${escapeHtml(field.name)}" />
          </label>
          <label class="form-field form-field-full">
            Sources
            <input data-contract-field-index="${index}" data-contract-field-prop="sources" value="${escapeHtml(formatSources(field.sources || []))}" />
          </label>
        </div>
        ${renderContractSourceChips(field, index)}
        ${renderContractFieldSuggestionSection(node, index, suggestionState)}
      </div>
    `;
  }).join("");

  return `
    <div class="section">
      <h3>Contract metadata</h3>
      <div class="form-grid">
        <label class="form-field">
          Contract type
          <input value="${escapeHtml(node.extension_type)}" disabled />
        </label>
        <label class="form-field">
          ${node.extension_type === "api" ? "Route" : "Component"}
          <input data-node-path="${node.extension_type === "api" ? "contract.route" : "contract.component"}" value="${escapeHtml(node.extension_type === "api" ? (node.contract.route || "") : (node.contract.component || ""))}" />
        </label>
        ${node.extension_type === "ui" ? `
          <label class="form-field">
            UI role
            <select data-node-path="contract.ui_role">
              ${["screen", "container", "component"].map((value) => `<option value="${value}" ${node.contract.ui_role === value ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </label>
        ` : ""}
      </div>
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Contract fields</h3>
        <button class="ghost-button" type="button" data-contract-field-add="true">Add field</button>
      </div>
      <div class="column-list">${fields || "<p>No fields defined.</p>"}</div>
    </div>
    <div class="section">
      <h3>Diagnostics</h3>
      ${renderWarningList("Missing dependencies", diagnostics.missing_dependencies)}
      ${renderWarningList("Unused fields", diagnostics.unused_fields)}
      ${renderWarningList("Breakage warnings", diagnostics.breakage_warnings)}
    </div>
  `;
}

function renderContractSourceChips(field, index) {
  const sources = field.sources || [];
  if (!sources.length) {
    return `<div class="column-meta">No bound sources yet.</div>`;
  }
  return `
    <div class="chip-row">
      ${sources.map((source, sourceIndex) => `
        <span class="tag-chip">
          ${escapeHtml(formatSources([source]))}
          <button class="inline-button danger" type="button" data-contract-source-remove="${index}:${sourceIndex}">x</button>
        </span>
      `).join("")}
      <button class="text-button" type="button" data-contract-field-clear-sources="${index}">Clear all</button>
    </div>
  `;
}

function renderContractFieldSuggestionSection(node, index, suggestionState) {
  if (!suggestionState) {
    return "";
  }
  const autoSuggestion = suggestionState.auto_suggestion;
  const suggestions = suggestionState.suggestions || [];
  return `
    <div class="section compact">
      <div class="section-actions">
        <strong>Binding suggestions</strong>
        ${autoSuggestion ? `<button class="ghost-button" type="button" data-contract-field-apply-auto="${index}">Use best match</button>` : ""}
      </div>
      ${suggestions.length ? `
        <div class="mapping-list">
          ${suggestions.map((suggestion, suggestionIndex) => `
            <div class="mapping-item">
              <div class="column-head">
                <div class="column-main">${escapeHtml(suggestion.ref)}</div>
                <div class="row-actions">
                  <span class="pill">score ${escapeHtml(String(suggestion.score))}</span>
                  <button class="text-button" type="button" data-contract-field-apply-suggestion="${index}:${suggestionIndex}">Use</button>
                </div>
              </div>
              <div class="column-meta">${escapeHtml(suggestion.reason || "Suggested from current graph")}</div>
            </div>
          `).join("")}
        </div>
      ` : "<p>No suggestions were found for this field.</p>"}
    </div>
  `;
}

function renderSourceSection(node) {
  const source = node.source || {};
  const dictionaries = (source.data_dictionaries || []).map((dictionary, index) => `
    <div class="column-row">
      <div class="column-head">
        <div class="column-main">${escapeHtml(dictionary.label || `Dictionary ${index + 1}`)}</div>
        <div class="row-actions">
          <button class="inline-button danger" type="button" data-source-dictionary-remove="${index}">Remove</button>
        </div>
      </div>
      <div class="form-grid compact">
        <label class="form-field">
          Label
          <input data-source-dictionary-index="${index}" data-source-dictionary-prop="label" value="${escapeHtml(dictionary.label || "")}" />
        </label>
        <label class="form-field">
          Kind
          <select data-source-dictionary-index="${index}" data-source-dictionary-prop="kind">
            <option value="link" ${dictionary.kind === "link" ? "selected" : ""}>link</option>
            <option value="file" ${dictionary.kind === "file" ? "selected" : ""}>file</option>
          </select>
        </label>
        <label class="form-field form-field-full">
          Value
          <input data-source-dictionary-index="${index}" data-source-dictionary-prop="value" value="${escapeHtml(dictionary.value || "")}" />
        </label>
      </div>
    </div>
  `).join("");
  const rawAssets = (source.raw_assets || []).map((asset, index) => `
    <div class="column-row">
      <div class="column-head">
        <div class="column-main">${escapeHtml(asset.label || `Raw asset ${index + 1}`)}</div>
        <div class="row-actions">
          <span class="pill">${escapeHtml(asset.kind || "file")} / ${escapeHtml(asset.format || "unknown")}</span>
          <button class="inline-button danger" type="button" data-source-raw-asset-remove="${index}">Remove</button>
        </div>
      </div>
      <div class="form-grid compact">
        <label class="form-field">
          Label
          <input data-source-raw-asset-index="${index}" data-source-raw-asset-prop="label" value="${escapeHtml(asset.label || "")}" />
        </label>
        <label class="form-field">
          Kind
          <select data-source-raw-asset-index="${index}" data-source-raw-asset-prop="kind">
            ${["file", "object_storage", "glob", "directory"].map((kind) => `
              <option value="${kind}" ${asset.kind === kind ? "selected" : ""}>${kind}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Format
          <select data-source-raw-asset-index="${index}" data-source-raw-asset-prop="format">
            ${["csv", "csv_gz", "parquet", "parquet_collection", "zip_csv", "unknown"].map((format) => `
              <option value="${format}" ${asset.format === format ? "selected" : ""}>${format}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Profile ready
          <select data-source-raw-asset-index="${index}" data-source-raw-asset-prop="profile_ready" data-coerce="boolean">
            <option value="true" ${asset.profile_ready !== false ? "selected" : ""}>true</option>
            <option value="false" ${asset.profile_ready === false ? "selected" : ""}>false</option>
          </select>
        </label>
        <label class="form-field form-field-full">
          Value
          <input data-source-raw-asset-index="${index}" data-source-raw-asset-prop="value" value="${escapeHtml(asset.value || "")}" />
        </label>
      </div>
    </div>
  `).join("");

  return `
    <div class="section">
      <h3>Source metadata</h3>
      <div class="form-grid">
        <label class="form-field">
          Provider
          <input data-node-path="source.provider" value="${escapeHtml(source.provider || "")}" />
        </label>
        <label class="form-field">
          Refresh cadence
          <input data-node-path="source.refresh" value="${escapeHtml(source.refresh || "")}" />
        </label>
        <label class="form-field">
          Series ID
          <input data-node-path="source.series_id" value="${escapeHtml(source.series_id || "")}" />
        </label>
        <label class="form-field">
          Origin kind
          <input data-node-path="source.origin.kind" value="${escapeHtml(source.origin?.kind || "")}" />
        </label>
        <label class="form-field form-field-full">
          Origin value
          <input data-node-path="source.origin.value" value="${escapeHtml(source.origin?.value || "")}" />
        </label>
      </div>
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Raw profile assets</h3>
        <button class="ghost-button" type="button" data-source-raw-asset-add="true">Add raw asset</button>
      </div>
      <p class="hint">Keep the authoritative upstream source in origin, then attach local or object-storage raw assets used for bootstrap profiling and landing-file inspection.</p>
      <div class="column-list">${rawAssets || "<p>No raw profile assets attached.</p>"}</div>
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Data dictionaries</h3>
        <button class="ghost-button" type="button" data-source-dictionary-add="true">Add dictionary</button>
      </div>
      <p class="hint">Attach a local file path or an external link for source-specific field definitions.</p>
      <div class="column-list">${dictionaries || "<p>No data dictionaries attached.</p>"}</div>
    </div>
  `;
}

function renderMappingSection(title, edges) {
  const items = edges.map((edge) => `
    <div class="mapping-item ${state.selectionMode === "edge" && state.selectedEdgeId === edge.id ? "focused" : ""}">
      <div class="column-head">
        <div class="column-main">${escapeHtml(edge.type)}: ${escapeHtml(edge.source)} -> ${escapeHtml(edge.target)}</div>
        <div class="row-actions">
          <button class="text-button" type="button" data-select-edge="${escapeHtml(edge.id)}">Edit edge</button>
        </div>
      </div>
      <div class="column-meta">${edge.column_mappings && edge.column_mappings.length ? escapeHtml(formatColumnMappings(edge, edge.column_mappings)) : "No column mappings"}</div>
    </div>
  `).join("");
  return `
    <div class="section">
      <h3>${escapeHtml(title)}</h3>
      <div class="mapping-list">${items || "<p>No edges in this direction.</p>"}</div>
    </div>
  `;
}

function renderPlan() {
  if (!state.latestPlan) {
    planSummary.innerHTML = '<div class="success-chip">✓ No issues</div><p class="hint">Save the graph to compute a deterministic diff.</p>';
    return;
  }
  const tiers = state.latestPlan.tiers;
  const tiles = [
    buildPlanSeverityTile("contract_breaks", "🚨", "Contract Breaks", [
      ...((tiers.tier_1?.breaking_changes || []).map((item) => ({ group: "Breaking changes", message: item }))),
      ...((tiers.tier_1?.removed_columns || []).map((item) => ({ group: "Removed columns", message: item }))),
      ...((tiers.tier_1?.contract_violations || []).map((item) => ({ group: "Contract violations", message: item }))),
    ]),
    buildPlanSeverityTile("warnings", "⚠️", "Warnings", [
      ...((tiers.tier_2?.impacted_datasets || []).map((item) => ({ group: "Impacted datasets", message: item }))),
      ...((tiers.tier_2?.impacted_apis || []).map((item) => ({ group: "Impacted APIs", message: item }))),
    ]),
    buildPlanSeverityTile("informational", "ℹ️", "Informational", [
      ...((tiers.tier_3?.upstream_ripple_effects || []).map((item) => ({ group: "Upstream ripple effects", message: item }))),
      ...((tiers.tier_3?.potential_file_targets || []).map((item) => ({ group: "Potential file targets", message: item }))),
    ]),
  ];
  planSummary.innerHTML = `
    <div class="severity-grid">${tiles.map(renderSeverityTile).join("")}</div>
    ${state.lastArtifacts ? `
      <div class="artifacts">
        <strong>Artifacts</strong>
        <div>${escapeHtml(state.lastArtifacts.latest_json)}</div>
        <div>${escapeHtml(state.lastArtifacts.latest_markdown)}</div>
      </div>
    ` : ""}
    <div class="plan-tier compact-tier">
      <h3>Metadata tracked</h3>
      ${renderCompactPlanList("Feature-selection updates", state.latestPlan.diff?.changed_feature_selection || [])}
      ${renderCompactPlanList("Source dictionary updates", state.latestPlan.diff?.changed_source_dictionaries || [])}
      ${renderCompactPlanList("Source raw asset updates", state.latestPlan.diff?.changed_source_raw_assets || [])}
    </div>
  `;
}

function renderValidation() {
  if (!validationSummary) {
    return;
  }
  if (!state.validationReport) {
    validationSummary.innerHTML = "<p>No validation report yet.</p>";
    return;
  }

  const diagnosticTiles = state.diagnostics?.severity_tiles || {};
  const warningRows = (state.validationReport.warnings || []).map((issue) => ({
    group: issue.node_id || issue.edge_id || "Graph warning",
    message: issue.message || "",
  }));
  validationSummary.innerHTML = `
    <div class="severity-grid">
      ${renderSeverityTile(normalizeDiagnosticTile(diagnosticTiles.contract_breaks, "contract_breaks", "🚨", "Contract Breaks"))}
      ${renderSeverityTile(buildPlanSeverityTile("warnings", "⚠️", "Warnings", warningRows))}
      ${renderSeverityTile(normalizeDiagnosticTile(diagnosticTiles.informational, "informational", "ℹ️", "Informational"))}
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
  const bundles = structure.bundles || [];
  const selectedBundle = state.selectedStructureBundle;
  const selectedBundleId = state.selectedStructureBundleId;

  structureSummary.innerHTML = `
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
      <button class="ghost-button" type="button" data-structure-action="export-yaml">Export YAML</button>
      <button class="ghost-button" type="button" data-structure-action="scan-quick" data-structure-role="scout">Scout scan</button>
      <button class="ghost-button" type="button" data-structure-action="scan-quick" data-structure-role="recorder">Recorder scan</button>
      <button class="ghost-button" type="button" data-structure-action="refresh-bundles">Refresh bundles</button>
    </div>
    ${(readiness.issues || []).length ? `
      <div class="section">
        <h3>Readiness issues</h3>
        <ul class="warning-list">
          ${(readiness.issues || []).slice(0, 6).map((issue) => `
            <li>
              <div>${escapeHtml(issue.message || "")}</div>
              ${issue.why_this_matters ? `<div class="hint">${escapeHtml(issue.why_this_matters)}</div>` : ""}
            </li>
          `).join("")}
        </ul>
      </div>
    ` : '<div class="success-chip">✓ No issues</div>'}
    <div class="section">
      <div class="section-actions">
        <h3>Review bundles</h3>
        <span class="hint">${formatValue(bundles.length)} bundle${bundles.length === 1 ? "" : "s"}</span>
      </div>
      <div class="column-list">
        ${bundles.length ? bundles.map((bundle) => `
          <div class="column-row ${selectedBundleId === bundle.bundle_id ? "selected" : ""}">
            <div class="column-head">
              <div class="column-main">${escapeHtml(bundle.bundle_id)}</div>
              <div class="row-actions">
                <span class="pill">${escapeHtml(bundle.role || "scout")}</span>
                <span class="pill">${escapeHtml(bundle.scope || "full")}</span>
                <button class="ghost-button" type="button" data-structure-action="open-bundle" data-bundle-id="${escapeHtml(bundle.bundle_id)}">${selectedBundleId === bundle.bundle_id ? "Loaded" : "Open"}</button>
              </div>
            </div>
            <div class="column-meta">patches: ${formatValue(bundle.patch_count || 0)} | pending: ${formatValue(bundle.pending_count || 0)} | contradictions: ${formatValue(bundle.contradiction_count || 0)}</div>
            <div class="column-meta">readiness: ${escapeHtml(bundle.readiness_status || "Not Ready")}</div>
            <div class="column-meta">reconciliation: planned missing ${formatValue(bundle.planned_missing_count || 0)} | untracked ${formatValue(bundle.observed_untracked_count || 0)} | divergent ${formatValue(bundle.implemented_differently_count || 0)}</div>
          </div>
        `).join("") : "<p>No review bundles yet. Run a scout or recorder scan to inspect repo or docs into proposed structure patches.</p>"}
      </div>
    </div>
    ${selectedBundle ? renderStructureBundleDetail(selectedBundle) : ""}
  `;
}

function renderStructureBundleDetail(bundle) {
  const review = bundle.review || {};
  const acceptedCount = (review.accepted_patch_ids || []).length;
  const contradictions = bundle.contradictions || [];
  const impacts = bundle.impacts || [];
  const pendingPatches = (bundle.patches || []).filter((patch) => patch.review_state === "pending");
  const patchGroups = groupStructureBundlePatches(bundle);
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Bundle detail</h3>
        <div class="row-actions">
          <span class="hint">base v${formatValue(bundle.base_structure_version || 1)}</span>
          <button class="ghost-button" type="button" data-structure-action="merge-bundle" data-bundle-id="${escapeHtml(bundle.bundle_id)}" ${acceptedCount ? "" : "disabled"}>Merge accepted</button>
        </div>
      </div>
      <div class="meta-grid compact-summary">
        <div><strong>Role</strong><br>${escapeHtml(bundle.scan?.role || "scout")}</div>
        <div><strong>Scope</strong><br>${escapeHtml(bundle.scan?.scope || "full")}</div>
        <div><strong>Fingerprint</strong><br>${escapeHtml((bundle.scan?.fingerprint || "").slice(0, 12) || "unknown")}</div>
        <div><strong>Created</strong><br>${escapeHtml(bundle.scan?.created_at || "")}</div>
      </div>
      ${renderStructureReconciliation(bundle.reconciliation || {})}
      ${contradictions.length ? `
        <div class="section">
          <h3>Contradictions</h3>
          <ul class="warning-list">
            ${contradictions.slice(0, 6).map((item) => `
              <li>
                <div>${escapeHtml(item.message || item.title || item.target_id || "Contradiction")}</div>
                ${item.why_this_matters ? `<div class="hint">${escapeHtml(item.why_this_matters)}</div>` : ""}
              </li>
            `).join("")}
          </ul>
        </div>
      ` : ""}
      ${impacts.length ? `
        <div class="section">
          <h3>Significant impacts</h3>
          <ul class="warning-list">
            ${impacts.slice(0, 6).map((item) => `<li>${escapeHtml(item.message || item.why_this_matters || item.target_id || "")}</li>`).join("")}
          </ul>
        </div>
      ` : ""}
      <div class="section">
        <div class="section-actions">
          <h3>Patch review</h3>
          <div class="row-actions">
            <span class="hint">${formatValue(pendingPatches.length)} pending patch${pendingPatches.length === 1 ? "" : "es"}</span>
            <button class="ghost-button" type="button" data-structure-action="review-patch-batch" data-bundle-id="${escapeHtml(bundle.bundle_id)}" data-decision="accepted" data-patch-ids="${escapeHtml(pendingPatches.map((patch) => patch.id).join(","))}" ${pendingPatches.length ? "" : "disabled"}>Accept pending</button>
            <button class="ghost-button" type="button" data-structure-action="review-patch-batch" data-bundle-id="${escapeHtml(bundle.bundle_id)}" data-decision="deferred" data-patch-ids="${escapeHtml(pendingPatches.map((patch) => patch.id).join(","))}" ${pendingPatches.length ? "" : "disabled"}>Defer pending</button>
            <button class="ghost-button danger-soft" type="button" data-structure-action="review-patch-batch" data-bundle-id="${escapeHtml(bundle.bundle_id)}" data-decision="rejected" data-patch-ids="${escapeHtml(pendingPatches.map((patch) => patch.id).join(","))}" ${pendingPatches.length ? "" : "disabled"}>Reject pending</button>
          </div>
        </div>
        <div class="column-list">
          ${patchGroups.map((group) => renderStructurePatchGroup(bundle.bundle_id, group)).join("") || '<div class="success-chip">✓ No issues</div>'}
        </div>
      </div>
    </div>
  `;
}

function renderStructureReconciliation(reconciliation) {
  const summary = reconciliation.summary || {};
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
      </div>
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

function groupStructureBundlePatches(bundle) {
  const observedNodes = Object.fromEntries((bundle.observed?.nodes || []).map((node) => [node.id, node]));
  const groups = new Map();
  (bundle.patches || []).forEach((patch) => {
    const key = patch.node_id || patch.target_id || patch.edge_id || patch.id;
    const node = observedNodes[patch.node_id] || observedNodes[patch.target_id];
    const label = node?.label || patch.node_id || patch.target_id || patch.id;
    if (!groups.has(key)) {
      groups.set(key, { key, label, patches: [] });
    }
    groups.get(key).patches.push(patch);
  });
  return Array.from(groups.values())
    .map((group) => ({
      ...group,
      patches: group.patches.slice().sort((left, right) => {
        const leftPending = left.review_state === "pending" ? 0 : 1;
        const rightPending = right.review_state === "pending" ? 0 : 1;
        return leftPending - rightPending || String(left.type || "").localeCompare(String(right.type || ""));
      }),
    }))
    .sort((left, right) => String(left.label || "").localeCompare(String(right.label || "")));
}

function renderStructurePatchGroup(bundleId, group) {
  const pendingIds = group.patches.filter((patch) => patch.review_state === "pending").map((patch) => patch.id);
  return `
    <div class="section">
      <div class="section-actions">
        <h3>${escapeHtml(group.label || group.key || "Patch group")}</h3>
        <div class="row-actions">
          <span class="hint">${formatValue(group.patches.length)} patch${group.patches.length === 1 ? "" : "es"}</span>
          <button class="ghost-button" type="button" data-structure-action="review-patch-batch" data-bundle-id="${escapeHtml(bundleId)}" data-decision="accepted" data-patch-ids="${escapeHtml(pendingIds.join(","))}" ${pendingIds.length ? "" : "disabled"}>Accept group</button>
          <button class="ghost-button" type="button" data-structure-action="review-patch-batch" data-bundle-id="${escapeHtml(bundleId)}" data-decision="deferred" data-patch-ids="${escapeHtml(pendingIds.join(","))}" ${pendingIds.length ? "" : "disabled"}>Defer group</button>
        </div>
      </div>
      <div class="column-list">
        ${group.patches.map((patch) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(patch.type)} · ${escapeHtml(patch.target_id || "")}</div>
              <div class="row-actions">
                <span class="pill">${escapeHtml(patch.confidence || "medium")}</span>
                <span class="pill">${escapeHtml(patch.review_state || "pending")}</span>
              </div>
            </div>
            <div class="column-meta">${escapeHtml(summarizeStructurePatch(patch))}</div>
            <div class="row-actions">
              <button class="ghost-button" type="button" data-structure-action="review-patch" data-bundle-id="${escapeHtml(bundleId)}" data-patch-id="${escapeHtml(patch.id)}" data-decision="accepted">Accept</button>
              <button class="ghost-button" type="button" data-structure-action="review-patch" data-bundle-id="${escapeHtml(bundleId)}" data-patch-id="${escapeHtml(patch.id)}" data-decision="deferred">Defer</button>
              <button class="ghost-button danger-soft" type="button" data-structure-action="review-patch" data-bundle-id="${escapeHtml(bundleId)}" data-patch-id="${escapeHtml(patch.id)}" data-decision="rejected">Reject</button>
            </div>
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function summarizeStructurePatch(patch) {
  const payload = patch.payload || {};
  if (patch.type === "add_binding" || patch.type === "change_binding") {
    return `${payload.previous_binding || "unbound"} → ${payload.new_binding || "unbound"}`;
  }
  if (patch.type === "add_field" || patch.type === "remove_field") {
    return payload.field_id || payload.name || patch.target_id || "";
  }
  if (patch.type === "add_node" || patch.type === "remove_node") {
    return payload.label || patch.target_id || "";
  }
  if (patch.type === "confidence_change") {
    return `${payload.previous_confidence || "unknown"} → ${payload.new_confidence || "unknown"}`;
  }
  return payload.message || payload.reason || patch.target_id || patch.type;
}

function readinessClass(status) {
  if (status === "Ready to Build") {
    return "healthy";
  }
  if (status === "Partially Ready") {
    return "warning";
  }
  return "broken";
}

function buildPlanSeverityTile(key, icon, label, rows) {
  return {
    key,
    icon,
    label,
    count: rows.length,
    rows: groupFlatRows(rows),
  };
}

function normalizeDiagnosticTile(tile, key, icon, label) {
  return {
    key,
    icon,
    label,
    count: tile?.count || 0,
    rows: tile?.rows || [],
  };
}

function renderSeverityTile(tile) {
  const normalized = tile;
  const count = normalized.count || 0;
  const summaryClass = normalized.key || "informational";
  const empty = count === 0 ? '<div class="success-chip">✓ No issues</div>' : "";
  return `
    <details class="severity-tile ${escapeHtml(summaryClass)}" ${count ? "" : ""}>
      <summary><span>${normalized.icon} ${escapeHtml(normalized.label)} (${escapeHtml(String(count))})</span></summary>
      <div class="severity-body">
        ${empty}
        ${count ? renderSeverityGroups(normalized.rows || []) : ""}
      </div>
    </details>
  `;
}

function groupFlatRows(rows) {
  const grouped = new Map();
  rows.forEach((row) => {
    const groupKey = row.group || row.node_id || "Issue";
    if (!grouped.has(groupKey)) {
      grouped.set(groupKey, {
        node_id: row.node_id || "",
        node_label: row.node_label || groupKey,
        group: groupKey,
        issues: [],
      });
    }
    grouped.get(groupKey).issues.push({
      binding_name: row.binding_name || "",
      message: row.message || "",
      why_this_matters: row.why_this_matters || "",
    });
  });
  return [...grouped.values()];
}

function renderSeverityGroups(groups) {
  return groups.map((group) => `
    <div class="severity-group">
      <strong>[${escapeHtml(group.node_id || group.group || group.node_label || "Issue")}]</strong>
      <ul class="warning-list">
        ${(group.issues || []).map((issue) => `
          <li>
            <div>${escapeHtml(issue.binding_name ? `${issue.binding_name}: ${issue.message}` : issue.message || "")}</div>
            ${issue.why_this_matters ? `<div class="hint">${escapeHtml(issue.why_this_matters)}</div>` : ""}
          </li>
        `).join("") || `<li>${escapeHtml(group.message || "")}</li>`}
      </ul>
    </div>
  `).join("");
}

function renderCompactPlanList(title, items) {
  if (!items.length) {
    return "";
  }
  return `
    <div class="compact-plan-list">
      <strong>${escapeHtml(title)}</strong>
      <ul>${items.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>
    </div>
  `;
}

function renderItems(items) {
  if (!items.length) {
    return "";
  }
  return items.map((item) => `<li>${escapeHtml(item)}</li>`).join("");
}

function renderHintFieldSummary(fields) {
  if (!fields?.length) {
    return "fields: none inferred";
  }
  return `fields: ${escapeHtml(fields.join(", "))}`;
}

function describeBootstrapScope({
  selectableCount,
  selectedAssetCount,
  apiHintCount,
  selectedApiHintCount,
  uiHintCount,
  selectedUiHintCount,
}) {
  const parts = [];
  if (state.projectBootstrapOptions.assets) {
    parts.push(
      selectedAssetCount
        ? `${selectedAssetCount} selected asset${selectedAssetCount === 1 ? "" : "s"}`
        : `${selectableCount} suggested asset${selectableCount === 1 ? "" : "s"}`
    );
  }
  if (state.projectBootstrapOptions.apiHints) {
    parts.push(
      selectedApiHintCount
        ? `${selectedApiHintCount} selected API hint${selectedApiHintCount === 1 ? "" : "s"}`
        : `${apiHintCount} visible API hint${apiHintCount === 1 ? "" : "s"}`
    );
  }
  if (state.projectBootstrapOptions.uiHints) {
    parts.push(
      selectedUiHintCount
        ? `${selectedUiHintCount} selected UI hint${selectedUiHintCount === 1 ? "" : "s"}`
        : `${uiHintCount} visible UI hint${uiHintCount === 1 ? "" : "s"}`
    );
  }
  if (!parts.length) {
    return "Bootstrap is disabled for all discovery categories.";
  }
  return `Bootstrap will import ${parts.join(", ")}.`;
}

function renderIssueItems(items, options = {}) {
  if (!items.length) {
    return options.empty ? `<li>${escapeHtml(options.empty)}</li>` : "";
  }
  return items.map((item) => {
    const targetRef = item.column_ref || "";
    const targetNode = item.node_id || "";
    const focusButton = targetRef
      ? `<button class="text-button" type="button" data-focus-column="${escapeHtml(targetRef)}">${isColumnTraced(targetRef) ? "Untrace" : "Trace"}</button>`
      : targetNode
        ? `<button class="text-button" type="button" data-select-node="${escapeHtml(targetNode)}">Open</button>`
        : "";
    return `
      <li>
        <div class="column-head">
          <div class="column-main">${escapeHtml(item.message || "")}</div>
          <div class="row-actions">${focusButton}</div>
        </div>
      </li>
    `;
  }).join("");
}

function renderNodeField(label, path, value, options = {}) {
  return `
    <label class="form-field ${options.full ? "form-field-full" : ""}">
      ${escapeHtml(label)}
      <input data-node-path="${escapeHtml(path)}" ${options.liveRender ? 'data-live-render="graph"' : ""} value="${escapeHtml(value || "")}" />
    </label>
  `;
}

function renderNodeTextarea(label, path, value, options = {}) {
  return `
    <label class="form-field ${options.full ? "form-field-full" : ""}">
      ${escapeHtml(label)}
      <textarea data-node-path="${escapeHtml(path)}" ${options.liveRender ? 'data-live-render="graph"' : ""}>${escapeHtml(value || "")}</textarea>
    </label>
  `;
}

function renderNodeSelect(label, path, value, values) {
  return `
    <label class="form-field">
      ${escapeHtml(label)}
      <select data-node-path="${escapeHtml(path)}">
        ${values.map((option) => `<option value="${escapeHtml(option)}" ${option === value ? "selected" : ""}>${escapeHtml(option)}</option>`).join("")}
      </select>
    </label>
  `;
}

function renderDetailAccordion(title, content, open = false) {
  return `
    <details class="detail-accordion" ${open ? "open" : ""}>
      <summary>${escapeHtml(title)}</summary>
      <div class="detail-accordion-content">${content}</div>
    </details>
  `;
}

function handleDetailMutation(event) {
  const target = event.target;
  const node = state.selectionMode === "node" ? getNodeById(state.selectedNodeId) : null;
  const edge = state.selectionMode === "edge" ? getEdgeById(state.selectedEdgeId) : null;

  if (node && target.matches("[data-node-path]")) {
    setValueAtPath(node, target.dataset.nodePath, coerceValue(target));
    markDirty(target.dataset.liveRender === "graph");
    return;
  }

  if (node && target.matches("[data-column-index][data-column-field]")) {
    const column = node.columns[Number(target.dataset.columnIndex)];
    if (!column) return;
    column[target.dataset.columnField] = coerceValue(target);
    markDirty(false);
    return;
  }

  if (node && target.matches("[data-feature-index]")) {
    const feature = node.compute.feature_selection[Number(target.dataset.featureIndex)];
    if (!feature) return;
    const fieldName = target.dataset.featureField || "status";
    if (fieldName === "labels") {
      feature[fieldName] = parseLabelList(target.value);
    } else if (fieldName === "order") {
      feature[fieldName] = parseNullableInteger(target.value);
    } else {
      feature[fieldName] = coerceValue(target);
    }
    markDirty(false);
    if (fieldName === "status") {
      render();
    } else if ((fieldName === "category" || fieldName === "order") && event.type === "change") {
      renderSelectionDetail();
    }
    return;
  }

  if (node && target.matches("[data-contract-field-index][data-contract-field-prop='name']")) {
    const field = node.contract.fields[Number(target.dataset.contractFieldIndex)];
    if (!field) return;
    const previousRef = `${node.id}.${field.name}`;
    field.name = target.value;
    replaceTracedColumnRef(previousRef, `${node.id}.${field.name}`);
    markDirty(false);
    return;
  }

  if (node && target.matches("[data-contract-field-index][data-contract-field-prop='sources']")) {
    if (event.type !== "change") return;
    const field = node.contract.fields[Number(target.dataset.contractFieldIndex)];
    if (!field) return;
    field.sources = parseSourceRefs(target.value);
    markDirty(false);
    return;
  }

  if (node && target.matches("[data-source-dictionary-index][data-source-dictionary-prop]")) {
    const dictionary = node.source.data_dictionaries[Number(target.dataset.sourceDictionaryIndex)];
    if (!dictionary) return;
    dictionary[target.dataset.sourceDictionaryProp] = coerceValue(target);
    markDirty(false);
    if (target.dataset.sourceDictionaryProp === "label" && event.type === "change") {
      renderSelectionDetail();
    }
    return;
  }

  if (node && target.matches("[data-source-raw-asset-index][data-source-raw-asset-prop]")) {
    const rawAsset = node.source.raw_assets[Number(target.dataset.sourceRawAssetIndex)];
    if (!rawAsset) return;
    rawAsset[target.dataset.sourceRawAssetProp] = coerceValue(target);
    markDirty(false);
    if (target.dataset.sourceRawAssetProp === "label" && event.type === "change") {
      renderSelectionDetail();
    }
    return;
  }

  if (target.matches("[data-feature-sort]")) {
    state.featureSortMode = target.value;
    renderSelectionDetail();
    return;
  }

  if (edge && target.matches("[data-edge-path]")) {
    setValueAtPath(edge, target.dataset.edgePath, coerceValue(target));
    markDirty(target.dataset.liveRender === "graph");
    return;
  }

  if (edge && target.matches("[data-edge-mapping-index][data-edge-mapping-side]")) {
    const mapping = edge.column_mappings[Number(target.dataset.edgeMappingIndex)];
    if (!mapping) return;
    mapping[target.dataset.edgeMappingSide] = target.value;
    markDirty(false);
  }
}

function handleDetailClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.dataset.focusColumn) {
    state.selectionMode = "node";
    const ref = target.dataset.focusColumn;
    toggleTracedColumn(ref);
    const parsed = splitColumnRef(ref);
    state.selectedNodeId = parsed ? parsed.nodeId : state.selectedNodeId;
    state.selectedEdgeId = null;
    render();
    return;
  }

  if (target.dataset.clearColumnFocus) {
    clearTracedColumns();
    render();
    return;
  }

  if (target.dataset.selectEdge) {
    state.selectionMode = "edge";
    state.selectedEdgeId = target.dataset.selectEdge;
    render();
    return;
  }

  if (target.dataset.centerNode) {
    centerNode(target.dataset.centerNode);
    return;
  }

  if (target.dataset.centerEdge) {
    centerEdge(target.dataset.centerEdge);
    return;
  }

  if (target.dataset.selectNode) {
    state.selectionMode = "node";
    state.selectedNodeId = target.dataset.selectNode;
    state.selectedEdgeId = null;
    render();
    return;
  }

  if (target.dataset.columnDependencyToggle) {
    const nodeId = target.dataset.columnDependencyToggle;
    state.columnDependencyToggles[nodeId] = !state.columnDependencyToggles[nodeId];
    renderSelectionDetail();
    return;
  }

  if (target.dataset.contractFieldSuggest) {
    requestContractFieldSuggestions(Number(target.dataset.contractFieldSuggest));
    return;
  }

  if (target.dataset.contractFieldApplyAuto) {
    applyContractFieldAutoSuggestion(Number(target.dataset.contractFieldApplyAuto));
    return;
  }

  if (target.dataset.contractFieldApplySuggestion) {
    const [fieldIndex, suggestionIndex] = target.dataset.contractFieldApplySuggestion.split(":").map(Number);
    applyContractFieldSuggestion(fieldIndex, suggestionIndex);
    return;
  }

  if (target.dataset.contractFieldClearSources) {
    const node = getNodeById(state.selectedNodeId);
    if (!node || node.kind !== "contract") return;
    const field = node.contract.fields[Number(target.dataset.contractFieldClearSources)];
    if (!field) return;
    queueDestructiveConfirm({
      title: "Remove field bindings",
      message: `Clear all bindings from ${field.name} in ${node.label}?`,
      onConfirm: () => {
        recordGraphHistory();
        field.sources = [];
        markDirty(false);
        renderSelectionDetail();
      },
    });
    return;
  }

  if (target.dataset.contractSourceRemove) {
    const [fieldIndex, sourceIndex] = target.dataset.contractSourceRemove.split(":").map(Number);
    const node = getNodeById(state.selectedNodeId);
    if (!node || node.kind !== "contract") return;
    const field = node.contract.fields[fieldIndex];
    if (!field) return;
    queueDestructiveConfirm({
      title: "Remove connection",
      message: `Remove one binding from ${field.name} in ${node.label}?`,
      onConfirm: () => {
        recordGraphHistory();
        field.sources.splice(sourceIndex, 1);
        markDirty(false);
        renderSelectionDetail();
      },
    });
    return;
  }

  if (target.dataset.graphContractLink) {
    const [nodeId, fieldName] = splitDataTailPair(target.dataset.graphContractLink || "");
    if (!nodeId || !fieldName) return;
    if (isPendingBindingTarget(nodeId, fieldName)) {
      clearPendingBindingState();
      render();
      setStatus("Rebind cleared", "Choose another field or source to start again.");
      return;
    }
    if (!state.pendingColumnLinkRef) {
      state.pendingBindingTarget = { nodeId, fieldName };
      render();
      setStatus("Binding target selected", `Click Connect on a source column to bind ${fieldName} here.`);
      return;
    }
    createContractFieldBinding(state.pendingColumnLinkRef, nodeId, fieldName);
    render();
    return;
  }

  if (target.dataset.graphContractFieldRemoveName) {
    const [nodeId, fieldName] = splitDataTailPair(target.dataset.graphContractFieldRemoveName || "");
    removeContractFieldByName(nodeId, fieldName);
    return;
  }

  if (target.dataset.edgeAddMapping) {
    const edge = getEdgeById(state.selectedEdgeId);
    if (!edge) return;
    edge.column_mappings.push({ source_column: "", target_column: "" });
    markDirty(false);
    render();
    return;
  }

  if (target.dataset.edgeRemoveMapping) {
    const edge = getEdgeById(state.selectedEdgeId);
    if (!edge) return;
    const mappingIndex = Number(target.dataset.edgeRemoveMapping);
    const mapping = edge.column_mappings[mappingIndex];
    if (!mapping) return;
    queueDestructiveConfirm({
      title: "Remove connection",
      message: `Remove the ${mapping.source_column || "source"} -> ${mapping.target_column || "target"} mapping?`,
      onConfirm: () => {
        recordGraphHistory();
        edge.column_mappings.splice(mappingIndex, 1);
        markDirty(false);
        render();
      },
    });
    return;
  }

  if (target.dataset.sourceDictionaryAdd) {
    const node = getNodeById(state.selectedNodeId);
    if (!node) return;
    node.source.data_dictionaries.push({ label: "", kind: "link", value: "" });
    state.needsAutoLayout = true;
    markDirty(false);
    render();
    return;
  }

  if (target.dataset.sourceDictionaryRemove) {
    const node = getNodeById(state.selectedNodeId);
    if (!node) return;
    const index = Number(target.dataset.sourceDictionaryRemove);
    const dictionary = node.source.data_dictionaries[index];
    if (!dictionary) return;
    queueDestructiveConfirm({
      title: "Remove data dictionary",
      message: `Remove ${dictionary.label || "this data dictionary"} from ${node.label}?`,
      onConfirm: () => {
        recordGraphHistory();
        node.source.data_dictionaries.splice(index, 1);
        state.needsAutoLayout = true;
        markDirty(false);
        render();
      },
    });
    return;
  }

  if (target.dataset.sourceRawAssetAdd) {
    const node = getNodeById(state.selectedNodeId);
    if (!node) return;
    node.source.raw_assets.push({
      label: "",
      kind: "file",
      format: "csv",
      value: "",
      profile_ready: true,
    });
    state.needsAutoLayout = true;
    markDirty(false);
    render();
    return;
  }

  if (target.dataset.sourceRawAssetRemove) {
    const node = getNodeById(state.selectedNodeId);
    if (!node) return;
    const index = Number(target.dataset.sourceRawAssetRemove);
    const asset = node.source.raw_assets[index];
    if (!asset) return;
    queueDestructiveConfirm({
      title: "Remove raw asset",
      message: `Remove ${asset.label || "this raw asset"} from ${node.label}?`,
      onConfirm: () => {
        recordGraphHistory();
        node.source.raw_assets.splice(index, 1);
        state.needsAutoLayout = true;
        markDirty(false);
        render();
      },
    });
    return;
  }

  if (target.dataset.columnAdd) {
    const node = getNodeById(state.selectedNodeId);
    if (!node || node.kind !== "data") return;
    node.columns.push({
      name: `new_column_${node.columns.length + 1}`,
      data_type: "string",
      description: "",
      nullable: true,
      null_pct: null,
      stats: {},
      notes: "",
    });
    state.needsAutoLayout = true;
    markDirty(false);
    render();
    return;
  }

  if (target.dataset.columnRemove) {
    const node = getNodeById(state.selectedNodeId);
    if (!node || node.kind !== "data") return;
    removeGraphColumnRow(node.id, target.dataset.columnRemove);
    return;
  }

  if (target.dataset.contractFieldAdd) {
    const node = getNodeById(state.selectedNodeId);
    if (!node || node.kind !== "contract") return;
    node.contract.fields.push({ name: `field_${node.contract.fields.length + 1}`, sources: [] });
    state.needsAutoLayout = true;
    markDirty(false);
    render();
    return;
  }

  if (target.dataset.contractFieldRemove) {
    const node = getNodeById(state.selectedNodeId);
    if (!node || node.kind !== "contract") return;
    const field = node.contract.fields[Number(target.dataset.contractFieldRemove)];
    if (!field) return;
    removeContractFieldByName(node.id, field.name);
    return;
  }

  if (target.dataset.deleteSelection) {
    deleteCurrentSelection();
    return;
  }

  if (target.dataset.featureAdd) {
    const node = getNodeById(state.selectedNodeId);
    if (!node || node.kind !== "compute") return;
    node.compute.feature_selection.push({
      column_ref: "",
      status: "candidate",
      persisted: false,
      stage: "",
      category: "",
      labels: [],
      order: null,
    });
    state.needsAutoLayout = true;
    markDirty(false);
    render();
    return;
  }

  if (target.dataset.featureRemove) {
    const node = getNodeById(state.selectedNodeId);
    if (!node || node.kind !== "compute") return;
    node.compute.feature_selection.splice(Number(target.dataset.featureRemove), 1);
    state.needsAutoLayout = true;
    markDirty(false);
    render();
  }
}

function handleGraphCanvasMutation(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.matches("[data-graph-work-item-index][data-graph-work-item-field][data-graph-node-id]")) {
    const node = getNodeById(target.dataset.graphNodeId);
    if (!node) return;
    const workItems = getNodeWorkItems(node);
    const item = workItems[Number(target.dataset.graphWorkItemIndex)];
    if (!item) return;
    if (event.type === "change") {
      recordGraphHistory();
    }
    item[target.dataset.graphWorkItemField] = target.value;
    markDirty(false);
    return;
  }

  if (target.matches("[data-graph-column-index][data-graph-column-field][data-graph-node-id]")) {
    const node = getNodeById(target.dataset.graphNodeId);
    if (!node || node.kind !== "data") return;
    const column = node.columns[Number(target.dataset.graphColumnIndex)];
    if (!column) return;
    const field = target.dataset.graphColumnField;
    if (event.type === "change" || field === "data_type_select") {
      recordGraphHistory();
    }
    if (field === "labels") {
      column.labels = parseLabelList(target.value);
    } else if (field === "data_type_select") {
      column.data_type = target.value === "other" ? "" : target.value;
      markDirty(true);
      render();
      return;
    } else {
      const previousName = field === "name" ? column.name : "";
      column[field] = target.value;
      if (field === "name" && previousName && previousName !== column.name) {
        renameColumnReferences(node.id, previousName, column.name);
      }
    }
    if (field === "name" && !column.description) {
      column.description = column.name;
    }
    markDirty(false);
  }
}

function createPendingColumnSpec() {
  return {
    name: "",
    data_type: "string",
    description: "",
    nullable: true,
    null_pct: null,
    stats: {},
    notes: "",
    category: "",
    labels: [],
    __isNew: true,
  };
}

function addGraphColumnRow(nodeId) {
  const node = getNodeById(nodeId);
  if (!node || node.kind !== "data" || state.interactionMode !== "edit") {
    return;
  }
  if ((node.columns || []).some((column) => column.__isNew)) {
    return;
  }
  recordGraphHistory();
  node.columns.push(createPendingColumnSpec());
  state.graphEditNodes[node.id] = true;
  state.needsAutoLayout = true;
  markDirty(true);
  render();
}

function commitGraphColumnRow(nodeId, indexText) {
  const node = getNodeById(nodeId);
  if (!node || node.kind !== "data" || state.interactionMode !== "edit") {
    return;
  }
  syncGraphColumnRowFromDom(nodeId, indexText);
  const column = node.columns[Number(indexText)];
  if (!column) return;
  if (!column.name?.trim()) {
    setStatus("Variable needs a name", "Give the new row a column name before adding it.");
    return;
  }
  delete column.__isNew;
  if (!column.description) {
    column.description = column.name;
  }
  markDirty(true);
  render();
}

function syncGraphColumnRowFromDom(nodeId, indexText) {
  const node = getNodeById(nodeId);
  if (!node || node.kind !== "data") {
    return;
  }
  const columnIndex = Number(indexText);
  const column = node.columns[columnIndex];
  if (!column) {
    return;
  }
  const nodeElement = graphCanvas?.querySelector(`[data-node-id="${cssEscapeValue(nodeId)}"]`);
  if (!(nodeElement instanceof HTMLElement)) {
    return;
  }
  nodeElement.querySelectorAll(`[data-graph-node-id="${cssEscapeValue(nodeId)}"][data-graph-column-index="${columnIndex}"]`).forEach((field) => {
    if (!(field instanceof HTMLInputElement || field instanceof HTMLSelectElement || field instanceof HTMLTextAreaElement)) {
      return;
    }
    const fieldName = field.dataset.graphColumnField;
    if (!fieldName) {
      return;
    }
    if (fieldName === "labels") {
      column.labels = parseLabelList(field.value);
      return;
    }
    if (fieldName === "data_type_select") {
      if (field.value !== "other") {
        column.data_type = field.value;
      }
      return;
    }
    column[fieldName] = field.value;
  });
}

function cancelGraphColumnRow(nodeId, indexText) {
  const node = getNodeById(nodeId);
  if (!node || node.kind !== "data" || state.interactionMode !== "edit") {
    return;
  }
  recordGraphHistory();
  node.columns.splice(Number(indexText), 1);
  markDirty(true);
  render();
}

function removeGraphColumnRow(nodeId, indexText) {
  const node = getNodeById(nodeId);
  const index = Number(indexText);
  if (!node || node.kind !== "data" || state.interactionMode !== "edit") {
    return;
  }
  const column = node.columns[index];
  if (!column) {
    return;
  }
  queueDestructiveConfirm({
    title: "Remove column",
    message: `Remove ${column.name || `column ${index + 1}`} from ${node.label}?`,
    onConfirm: () => {
      recordGraphHistory();
      const removed = node.columns.splice(index, 1)[0];
      if (removed?.name) {
        removeTracedColumnRef(`${node.id}.${removed.name}`);
        state.linkSelectionRefs = (state.linkSelectionRefs || []).filter((ref) => ref !== `${node.id}.${removed.name}`);
        if (state.pendingColumnLinkRef === `${node.id}.${removed.name}` || isPendingBindingTarget(node.id, removed.name)) {
          clearPendingBindingState();
        }
      }
      state.needsAutoLayout = true;
      markDirty(true);
      render();
      setStatus("Column removed", `${removed?.name || "Column"} was removed locally.`);
    },
  });
}

function addGraphWorkItem(nodeId) {
  const node = getNodeById(nodeId);
  if (!node || state.interactionMode !== "edit") {
    return;
  }
  recordGraphHistory();
  getNodeWorkItems(node).push({ kind: "task", text: "" });
  state.needsAutoLayout = true;
  markDirty(true);
  render();
}

function removeGraphWorkItem(nodeId, indexText) {
  const node = getNodeById(nodeId);
  if (!node || state.interactionMode !== "edit") {
    return;
  }
  const items = getNodeWorkItems(node);
  const index = Number(indexText);
  const item = items[index];
  if (!item) {
    return;
  }
  queueDestructiveConfirm({
    title: "Remove note",
    message: `Remove this ${item.kind || "note"} from ${node.label}?`,
    onConfirm: () => {
      recordGraphHistory();
      items.splice(index, 1);
      state.needsAutoLayout = true;
      markDirty(true);
      render();
      setStatus("Note removed", `${node.label} was updated locally.`);
    },
  });
}

function removePickedColumnsFromNode(nodeId) {
  const node = getNodeById(nodeId);
  if (!node || node.kind !== "data" || state.interactionMode !== "edit") {
    return;
  }
  const pickedRefs = getPickedColumnRefs().filter((ref) => ref.startsWith(`${node.id}.`));
  if (!pickedRefs.length) {
    return;
  }
  queueDestructiveConfirm({
    title: "Remove picked columns",
    message: `Remove ${pickedRefs.length} picked column${pickedRefs.length === 1 ? "" : "s"} from ${node.label}?`,
    onConfirm: () => {
      recordGraphHistory();
      const removeNames = new Set(pickedRefs.map((ref) => splitColumnRef(ref)?.columnName).filter(Boolean));
      node.columns = node.columns.filter((column) => !removeNames.has(column.name));
      state.selectedColumnRefs = (state.selectedColumnRefs || []).filter((ref) => !pickedRefs.includes(ref));
      if (state.selectedColumnRef && pickedRefs.includes(state.selectedColumnRef)) {
        state.selectedColumnRef = null;
      }
      if (state.pendingColumnLinkRef && pickedRefs.includes(state.pendingColumnLinkRef)) {
        clearPendingBindingState();
      }
      state.linkSelectionRefs = (state.linkSelectionRefs || []).filter((ref) => !pickedRefs.includes(ref));
      state.needsAutoLayout = true;
      markDirty(true);
      render();
      setStatus("Picked columns removed", `${pickedRefs.length} column${pickedRefs.length === 1 ? "" : "s"} were removed locally.`);
    },
  });
}

function handleGraphCanvasClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const cycleButton = target.closest("[data-graph-cycle-work-status]");
  if (cycleButton instanceof HTMLElement) {
    event.stopPropagation();
    const node = getNodeById(cycleButton.dataset.graphCycleWorkStatus);
    if (!node) return;
    recordGraphHistory();
    cycleNodeWorkStatus(node);
    markDirty(true);
    render();
    return;
  }

  const detailsButton = target.closest("[data-graph-node-details]");
  if (detailsButton instanceof HTMLElement) {
    event.stopPropagation();
    const nodeId = detailsButton.dataset.graphNodeDetails;
    state.graphNodeDetailOverrides[nodeId] = !shouldShowGraphExtraDetails(nodeId);
    renderGraph();
    return;
  }

  const editButton = target.closest("[data-graph-node-edit]");
  if (editButton instanceof HTMLElement) {
    event.stopPropagation();
    const nodeId = editButton.dataset.graphNodeEdit;
    state.graphEditNodes[nodeId] = !isGraphNodeEditMode(nodeId);
    state.needsAutoLayout = true;
    render();
    return;
  }

  const addColumnButton = target.closest("[data-graph-column-add]");
  if (addColumnButton instanceof HTMLElement) {
    event.stopPropagation();
    addGraphColumnRow(addColumnButton.dataset.graphColumnAdd);
    return;
  }

  const commitColumnButton = target.closest("[data-graph-column-commit]");
  if (commitColumnButton instanceof HTMLElement) {
    event.stopPropagation();
    const [nodeId, indexText] = splitDataTailPair(commitColumnButton.dataset.graphColumnCommit || "");
    commitGraphColumnRow(nodeId, indexText);
    return;
  }

  const cancelColumnButton = target.closest("[data-graph-column-cancel]");
  if (cancelColumnButton instanceof HTMLElement) {
    event.stopPropagation();
    const [nodeId, indexText] = splitDataTailPair(cancelColumnButton.dataset.graphColumnCancel || "");
    cancelGraphColumnRow(nodeId, indexText);
    return;
  }

  const connectColumnButton = target.closest("[data-graph-column-link]");
  if (connectColumnButton instanceof HTMLElement) {
    event.stopPropagation();
    togglePendingColumnLink(connectColumnButton.dataset.graphColumnLink);
    render();
    return;
  }

  const removeColumnButton = target.closest("[data-graph-column-remove]");
  if (removeColumnButton instanceof HTMLElement) {
    event.stopPropagation();
    const [nodeId, indexText] = splitDataTailPair(removeColumnButton.dataset.graphColumnRemove || "");
    removeGraphColumnRow(nodeId, indexText);
    return;
  }

  const pickColumnButton = target.closest("[data-graph-pick-column]");
  if (pickColumnButton instanceof HTMLElement) {
    event.stopPropagation();
    togglePickedColumn(pickColumnButton.dataset.graphPickColumn);
    render();
    return;
  }

  const attachPickedButton = target.closest("[data-graph-attach-picked]");
  if (attachPickedButton instanceof HTMLElement) {
    event.stopPropagation();
    attachPickedColumnsToNode(attachPickedButton.dataset.graphAttachPicked);
    return;
  }

  const removePickedButton = target.closest("[data-graph-remove-picked]");
  if (removePickedButton instanceof HTMLElement) {
    event.stopPropagation();
    removePickedColumnsFromNode(removePickedButton.dataset.graphRemovePicked);
    return;
  }

  const addWorkItemButton = target.closest("[data-graph-work-item-add]");
  if (addWorkItemButton instanceof HTMLElement) {
    event.stopPropagation();
    addGraphWorkItem(addWorkItemButton.dataset.graphWorkItemAdd);
    return;
  }

  const removeWorkItemButton = target.closest("[data-graph-work-item-remove]");
  if (removeWorkItemButton instanceof HTMLElement) {
    event.stopPropagation();
    const [nodeId, indexText] = splitDataTailPair(removeWorkItemButton.dataset.graphWorkItemRemove || "");
    removeGraphWorkItem(nodeId, indexText);
    return;
  }

  const removeNodeButton = target.closest("[data-graph-remove-node]");
  if (removeNodeButton instanceof HTMLElement) {
    event.stopPropagation();
    deleteNodeById(removeNodeButton.dataset.graphRemoveNode);
    return;
  }

  const connectContractButton = target.closest("[data-graph-contract-link]");
  if (connectContractButton instanceof HTMLElement) {
    event.stopPropagation();
    const [nodeId, fieldName] = splitDataTailPair(connectContractButton.dataset.graphContractLink || "");
    if (!nodeId || !fieldName) return;
    if (isPendingBindingTarget(nodeId, fieldName)) {
      clearPendingBindingState();
      render();
      setStatus("Rebind cleared", "Choose another field or source to start again.");
      return;
    }
    if (!state.pendingColumnLinkRef) {
      state.pendingBindingTarget = { nodeId, fieldName };
      render();
      setStatus("Binding target selected", `Click Connect on a source column to bind ${fieldName} here.`);
      return;
    }
    createContractFieldBinding(state.pendingColumnLinkRef, nodeId, fieldName);
    render();
    return;
  }

  const removeContractFieldButton = target.closest("[data-graph-contract-field-remove-name]");
  if (removeContractFieldButton instanceof HTMLElement) {
    event.stopPropagation();
    const [nodeId, fieldName] = splitDataTailPair(removeContractFieldButton.dataset.graphContractFieldRemoveName || "");
    const node = getNodeById(nodeId);
    if (!node || node.kind !== "contract") return;
    const fieldIndex = (node.contract?.fields || []).findIndex((field) => field.name === fieldName);
    if (fieldIndex < 0) return;
    recordGraphHistory();
    node.contract.fields.splice(fieldIndex, 1);
    markDirty(true);
    render();
    return;
  }
}

function handleGraphCanvasContextMenu(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const row = target.closest("[data-binding-ref]");
  if (!(row instanceof HTMLElement)) {
    closeGraphContextMenu();
    return;
  }
  event.preventDefault();
  const ref = row.dataset.bindingRef;
  if (!ref) {
    return;
  }
  state.contextMenu = {
    x: event.clientX,
    y: event.clientY,
    ref,
  };
  renderGraphContextMenu();
}

function closeGraphContextMenu() {
  state.contextMenu = null;
  renderGraphContextMenu();
}

function renderGraphContextMenu() {
  if (!graphContextMenu) {
    return;
  }
  if (!state.contextMenu) {
    graphContextMenu.hidden = true;
    graphContextMenu.innerHTML = "";
    return;
  }
  const parsed = splitColumnRef(state.contextMenu.ref);
  const node = parsed ? getNodeById(parsed.nodeId) : null;
  const canRemoveContractField = node?.kind === "contract" && state.interactionMode === "edit";
  const connectLabel = state.pendingColumnLinkRef
    ? "Connect here"
    : node?.kind === "contract"
      ? "Select target"
      : "Start connect";
  graphContextMenu.hidden = false;
  graphContextMenu.style.left = `${state.contextMenu.x}px`;
  graphContextMenu.style.top = `${state.contextMenu.y}px`;
  graphContextMenu.innerHTML = `
    <button class="context-menu-item" type="button" data-context-action="trace" data-context-ref="${escapeHtml(state.contextMenu.ref)}">${isColumnTraced(state.contextMenu.ref) ? "Untrace" : "Trace lineage"}</button>
    <button class="context-menu-item" type="button" data-context-action="pick" data-context-ref="${escapeHtml(state.contextMenu.ref)}">${isColumnPicked(state.contextMenu.ref) ? "Unpick" : "Pick"}</button>
    <button class="context-menu-item" type="button" data-context-action="connect" data-context-ref="${escapeHtml(state.contextMenu.ref)}">${connectLabel}</button>
    ${canRemoveContractField ? `<button class="context-menu-item danger" type="button" data-context-action="remove-contract-field" data-context-ref="${escapeHtml(state.contextMenu.ref)}">Remove field</button>` : ""}
  `;
  graphContextMenu.querySelectorAll("[data-context-action]").forEach((button) => {
    button.addEventListener("click", (clickEvent) => {
      clickEvent.stopPropagation();
      const action = button.dataset.contextAction;
      const ref = button.dataset.contextRef;
      if (!ref) {
        return;
      }
      if (action === "trace") {
        toggleTracedColumn(ref);
      } else if (action === "pick") {
        togglePickedColumn(ref);
      } else if (action === "connect") {
        if (state.pendingColumnLinkRef && parsed && node?.kind === "contract") {
          createContractFieldBinding(state.pendingColumnLinkRef, parsed.nodeId, parsed.columnName);
        } else if (parsed && node?.kind === "contract") {
          if (isPendingBindingTarget(parsed.nodeId, parsed.columnName)) {
            clearPendingBindingState();
          } else {
            state.pendingBindingTarget = { nodeId: parsed.nodeId, fieldName: parsed.columnName };
          }
        } else {
          togglePendingColumnLink(ref);
        }
      } else if (action === "remove-contract-field" && parsed && node?.kind === "contract") {
        removeContractFieldByName(parsed.nodeId, parsed.columnName);
      }
      closeGraphContextMenu();
      render();
    });
  });
}

function renderConfirmModal() {
  if (!confirmModal) {
    return;
  }
  const confirmState = state.destructiveConfirm;
  confirmModal.hidden = !confirmState;
  if (!confirmState) {
    confirmModalSnoozeCheck.checked = false;
    confirmModalSnoozeMinutes.value = "1";
    return;
  }
  confirmModalTitle.textContent = confirmState.title || "Confirm removal";
  confirmModalMessage.textContent = confirmState.message || "This action cannot be undone from the graph.";
}

function removeContractFieldByName(nodeId, fieldName) {
  const node = getNodeById(nodeId);
  if (!node || node.kind !== "contract" || state.interactionMode !== "edit") {
    return;
  }
  const fieldIndex = (node.contract?.fields || []).findIndex((field) => field.name === fieldName);
  if (fieldIndex < 0) {
    return;
  }
  queueDestructiveConfirm({
    title: "Remove contract field",
    message: `Remove ${fieldName} from ${node.label}?`,
    onConfirm: () => {
      recordGraphHistory();
      node.contract.fields.splice(fieldIndex, 1);
      removeTracedColumnRef(`${nodeId}.${fieldName}`);
      state.linkSelectionRefs = (state.linkSelectionRefs || []).filter((ref) => ref !== `${nodeId}.${fieldName}`);
      if (state.pendingColumnLinkRef === `${nodeId}.${fieldName}` || isPendingBindingTarget(nodeId, fieldName)) {
        clearPendingBindingState();
      }
      state.needsAutoLayout = true;
      markDirty(true);
      render();
      setStatus("Field removed", `${fieldName} was removed locally.`);
    },
  });
}

function queueDestructiveConfirm({ title, message, onConfirm }) {
  if (Date.now() < state.destructiveWarningSilencedUntil) {
    onConfirm();
    return;
  }
  state.destructiveConfirm = { title, message, onConfirm };
  renderConfirmModal();
}

function cancelDestructiveConfirm() {
  state.destructiveConfirm = null;
  renderConfirmModal();
}

function confirmDestructiveAction() {
  if (!state.destructiveConfirm) {
    return;
  }
  const pending = state.destructiveConfirm;
  state.destructiveConfirm = null;
  if (confirmModalSnoozeCheck.checked) {
    const minutes = Number(confirmModalSnoozeMinutes.value || 1);
    state.destructiveWarningSilencedUntil = Date.now() + (minutes * 60 * 1000);
  }
  renderConfirmModal();
  pending.onConfirm();
}

function handleModalBackdropClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  if (target.dataset.modalClose === "confirm") {
    cancelDestructiveConfirm();
    return;
  }
  if (target.dataset.modalClose === "directory") {
    closeDirectoryPicker();
  }
}

function renderDirectoryPicker() {
  if (!directoryPickerModal) {
    return;
  }
  directoryPickerModal.hidden = !state.directoryPicker.open;
  if (!state.directoryPicker.open) {
    return;
  }
  directoryPickerQuery.value = state.directoryPicker.query;
  directoryPickerResults.innerHTML = state.directoryPicker.loading
    ? "<p class=\"hint\">Searching directories...</p>"
    : state.directoryPicker.results.length
      ? state.directoryPicker.results.map((item) => `
        <button class="column-row button-reset" type="button" data-directory-pick="${escapeHtml(item.path)}">
          <div class="column-head">
            <div class="column-main">${escapeHtml(item.name || item.path)}</div>
            <div class="row-actions"><span class="pill">${escapeHtml(item.path)}</span></div>
          </div>
        </button>
      `).join("")
      : "<p class=\"hint\">No matching directories found yet.</p>";
}

function openDirectoryPicker() {
  state.directoryPicker.open = true;
  state.directoryPicker.query = state.projectProfileOptions.rootPath || state.projectProfile?.root || "";
  state.directoryPicker.results = [];
  renderDirectoryPicker();
  searchProjectDirectories();
}

function closeDirectoryPicker() {
  state.directoryPicker.open = false;
  renderDirectoryPicker();
}

async function searchProjectDirectories() {
  state.directoryPicker.loading = true;
  state.directoryPicker.query = directoryPickerQuery?.value || state.directoryPicker.query;
  renderDirectoryPicker();
  const params = new URLSearchParams();
  if (state.directoryPicker.query) {
    params.set("query", state.directoryPicker.query);
  }
  const basePath = state.projectProfile?.root || "";
  if (basePath) {
    params.set("base_path", basePath);
  }
  const response = await fetch(`/api/project/directories?${params.toString()}`);
  const payload = await response.json();
  state.directoryPicker.loading = false;
  if (!response.ok) {
    state.directoryPicker.results = [];
    setStatus("Directory search failed", payload.detail || payload.error || "Unable to search for project roots.");
    renderDirectoryPicker();
    return;
  }
  state.directoryPicker.results = payload.directories || [];
  renderDirectoryPicker();
}

function handleDirectoryPickerClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest("[data-directory-pick]");
  if (!(button instanceof HTMLElement)) {
    return;
  }
  state.projectProfileOptions.rootPath = button.dataset.directoryPick || "";
  closeDirectoryPicker();
  renderProjectProfile();
}

function isPendingBindingTarget(nodeId, fieldName) {
  return state.pendingBindingTarget?.nodeId === nodeId && state.pendingBindingTarget?.fieldName === fieldName;
}

function clearPendingBindingState() {
  state.pendingColumnLinkRef = null;
  state.pendingBindingTarget = null;
}

function handleGraphCanvasDragStart(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  if (!target.dataset.categoryChip) return;
  event.dataTransfer?.setData("text/plain", target.dataset.categoryChip);
  event.dataTransfer.effectAllowed = "copy";
}

function handleGraphCanvasDragOver(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  if (!target.dataset.categoryDrop) return;
  event.preventDefault();
  event.dataTransfer.dropEffect = "copy";
}

function handleGraphCanvasDrop(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  if (!target.dataset.categoryDrop) return;
  event.preventDefault();
  const category = event.dataTransfer?.getData("text/plain");
  const [nodeId, indexText] = splitDataTailPair(target.dataset.categoryDrop);
  const node = getNodeById(nodeId);
  if (!node || node.kind !== "data" || !category) return;
  const column = node.columns[Number(indexText)];
  if (!column) return;
  recordGraphHistory();
  column.category = category;
  markDirty(true);
  render();
}

function togglePendingColumnLink(ref) {
  if (!ref) return;
  if (state.pendingBindingTarget) {
    const target = state.pendingBindingTarget;
    clearPendingBindingState();
    createContractFieldBinding(ref, target.nodeId, target.fieldName);
    render();
    return;
  }
  if (state.pendingColumnLinkRef === ref) {
    clearPendingBindingState();
    setStatus("Lineage link cleared", "Pick another source column to start a lineage mapping.");
    return;
  }
  if (!state.pendingColumnLinkRef) {
    state.pendingColumnLinkRef = ref;
    setStatus("Lineage source selected", `${ref} is ready to connect to a downstream column.`);
    return;
  }
  const sourceRef = state.pendingColumnLinkRef;
  clearPendingBindingState();
  if (sourceRef === ref) {
    return;
  }
  createColumnLineageMapping(sourceRef, ref);
}

function renameColumnReferences(nodeId, previousName, nextName) {
  const previousRef = `${nodeId}.${previousName}`;
  const nextRef = `${nodeId}.${nextName}`;
  replaceTracedColumnRef(previousRef, nextRef);
  state.graph.edges.forEach((edge) => {
    (edge.column_mappings || []).forEach((mapping) => {
      if (edge.source === nodeId && mapping.source_column === previousName) {
        mapping.source_column = nextName;
      }
      if (edge.target === nodeId && mapping.target_column === previousName) {
        mapping.target_column = nextName;
      }
    });
  });
  state.graph.nodes.forEach((node) => {
    if (node.kind === "compute") {
      (node.compute?.feature_selection || []).forEach((feature) => {
        if (feature.column_ref === previousRef) {
          feature.column_ref = nextRef;
        }
      });
    }
    if (node.kind === "contract") {
      (node.contract?.fields || []).forEach((field) => {
        (field.sources || []).forEach((source) => {
          if (source.node_id === nodeId && source.column === previousName) {
            source.column = nextName;
          }
        });
      });
    }
  });
}

function createColumnLineageMapping(sourceRef, targetRef) {
  const sourceParsed = splitColumnRef(sourceRef);
  const targetParsed = splitColumnRef(targetRef);
  if (!sourceParsed || !targetParsed) return;
  if (sourceParsed.nodeId === targetParsed.nodeId) {
    setStatus("Lineage link skipped", "Choose a column on another node for lineage mapping.");
    return;
  }
  recordGraphHistory();
  const edge = ensureLineageEdge(sourceParsed.nodeId, targetParsed.nodeId);
  const duplicate = (edge.column_mappings || []).some((mapping) => mapping.source_column === sourceParsed.columnName && mapping.target_column === targetParsed.columnName);
  if (!duplicate) {
    edge.column_mappings.push({
      source_column: sourceParsed.columnName,
      target_column: targetParsed.columnName,
    });
  }
  state.selectionMode = "edge";
  state.selectedEdgeId = edge.id;
  state.selectedNodeId = targetParsed.nodeId;
  markDirty(true);
  render();
  setStatus("Lineage mapping created", `${sourceRef} now maps to ${targetRef}.`);
}

function createContractFieldBinding(sourceRef, contractNodeId, fieldName) {
  const parsed = splitColumnRef(sourceRef);
  const node = getNodeById(contractNodeId);
  if (!parsed || !node || node.kind !== "contract") {
    return;
  }
  const sourceNode = getNodeById(parsed.nodeId);
  if (!sourceNode) {
    return;
  }
  const field = (node.contract?.fields || []).find((entry) => entry.name === fieldName);
  if (!field) {
    return;
  }
  clearPendingBindingState();
  recordGraphHistory();
  const sourceRefValue = makeContractSourceRef(sourceNode, parsed.columnName);
  field.sources = [sourceRefValue, ...(field.sources || []).filter((candidate) => (
    candidate.node_id !== sourceRefValue.node_id
      || candidate.column !== sourceRefValue.column
      || candidate.field !== sourceRefValue.field
  ))];
  const edge = ensureLineageEdge(parsed.nodeId, contractNodeId);
  if (!(edge.column_mappings || []).some((mapping) => mapping.source_column === parsed.columnName && mapping.target_column === fieldName)) {
    edge.column_mappings.push({
      source_column: parsed.columnName,
      target_column: fieldName,
    });
  }
  state.selectionMode = "node";
  state.selectedNodeId = contractNodeId;
  state.selectedEdgeId = edge.id;
  markDirty(true);
  setStatus("Binding connected", `${sourceRef} now feeds ${contractNodeId}.${fieldName}.`);
}

function ensureLineageEdge(sourceNodeId, targetNodeId) {
  let edge = state.graph.edges.find((candidate) => candidate.source === sourceNodeId && candidate.target === targetNodeId);
  if (edge) return edge;
  const sourceNode = getNodeById(sourceNodeId);
  const targetNode = getNodeById(targetNodeId);
  const type = inferEdgeTypeForNodes(sourceNode, targetNode);
  edge = {
    id: makeUniqueEdgeId(type, sourceNodeId, targetNodeId),
    type,
    source: sourceNodeId,
    target: targetNodeId,
    label: "",
    column_mappings: [],
    notes: "",
  };
  state.graph.edges.push(edge);
  return edge;
}

function inferEdgeTypeForNodes(sourceNode, targetNode) {
  if (!sourceNode || !targetNode) return "depends_on";
  if (sourceNode.kind === "source" && targetNode.kind === "data") return "ingests";
  if (sourceNode.kind === "compute" && targetNode.kind === "data") return "produces";
  if (sourceNode.kind === "data" && targetNode.kind === "compute") return "depends_on";
  if (sourceNode.kind === "data" && targetNode.kind === "data") return "derives";
  if (sourceNode.kind === "contract" && targetNode.kind === "contract") return "binds";
  if ((sourceNode.kind === "data" || sourceNode.kind === "compute") && targetNode.kind === "contract") return "serves";
  return "depends_on";
}

function attachPickedColumnsToNode(targetNodeId, options = {}) {
  const targetNode = getNodeById(targetNodeId);
  const pickedRefs = getPickedColumnRefs();
  if (!targetNode) {
    return;
  }
  if (!pickedRefs.length) {
    setStatus("No columns picked", "Pick one or more columns first, then attach them to a data, compute, or contract object.");
    return;
  }
  if (targetNode.kind === "source") {
    setStatus("Attach skipped", "Picked columns can be attached to data, compute, or contract objects.");
    return;
  }

  if (options.recordHistory !== false) {
    recordGraphHistory();
  }
  const sourceGroups = new Map();
  let createdBindings = 0;
  pickedRefs.forEach((ref) => {
    const parsed = splitColumnRef(ref);
    if (!parsed || parsed.nodeId === targetNodeId) {
      return;
    }
    const sourceNode = getNodeById(parsed.nodeId);
    if (!sourceNode) {
      return;
    }
    const bindingName = attachRefToTargetNode(sourceNode, parsed.columnName, targetNode, ref);
    if (!bindingName) {
      return;
    }
    createdBindings += 1;
    if (!sourceGroups.has(parsed.nodeId)) {
      sourceGroups.set(parsed.nodeId, []);
    }
    sourceGroups.get(parsed.nodeId).push({
      source_column: parsed.columnName,
      target_column: bindingName,
    });
  });

  sourceGroups.forEach((mappings, sourceNodeId) => {
    const edge = ensureLineageEdge(sourceNodeId, targetNodeId);
    mappings.forEach((mapping) => {
      const duplicate = (edge.column_mappings || []).some((candidate) => (
        candidate.source_column === mapping.source_column
          && candidate.target_column === mapping.target_column
      ));
      if (!duplicate) {
        edge.column_mappings.push(mapping);
      }
    });
  });

  state.selectionMode = "node";
  state.selectedNodeId = targetNodeId;
  state.selectedEdgeId = null;
  clearPickedColumns();
  state.needsAutoLayout = true;
  markDirty(true);
  render();
  setStatus("Picked columns attached", `${createdBindings} picked column${createdBindings === 1 ? "" : "s"} were attached to ${targetNode.label}.`);
}

function attachRefToTargetNode(sourceNode, sourceBindingName, targetNode, fullRef) {
  const sourceColumn = sourceNode.kind === "data"
    ? sourceNode.columns.find((column) => column.name === sourceBindingName)
    : null;
  if (targetNode.kind === "data") {
    const existing = targetNode.columns.find((column) => column.name === sourceBindingName);
    if (!existing) {
      targetNode.columns.push({
        name: sourceBindingName,
        data_type: sourceColumn?.data_type || inferBindingTypeFromNode(sourceNode, sourceBindingName),
        description: sourceColumn?.description || sourceBindingName,
        nullable: sourceColumn?.nullable ?? true,
        null_pct: sourceColumn?.null_pct ?? null,
        stats: {},
        notes: sourceColumn?.notes || "",
        category: sourceColumn?.category || "",
        labels: [...(sourceColumn?.labels || [])],
      });
    }
    return sourceBindingName;
  }
  if (targetNode.kind === "contract") {
    let field = (targetNode.contract.fields || []).find((entry) => entry.name === sourceBindingName);
    if (!field) {
      field = { name: sourceBindingName, sources: [] };
      targetNode.contract.fields.push(field);
    }
    const sourceRef = makeContractSourceRef(sourceNode, sourceBindingName);
    const duplicateSource = (field.sources || []).some((candidate) => (
      candidate.node_id === sourceRef.node_id
        && candidate.column === sourceRef.column
        && candidate.field === sourceRef.field
    ));
    if (!duplicateSource) {
      field.sources.push(sourceRef);
    }
    return field.name;
  }
  if (targetNode.kind === "compute") {
    targetNode.compute.inputs = [...new Set([...(targetNode.compute.inputs || []), sourceNode.id])];
    if (targetNode.extension_type === "model" || targetNode.extension_type === "transform") {
      const existingFeature = (targetNode.compute.feature_selection || []).find((feature) => feature.column_ref === fullRef);
      if (!existingFeature) {
        targetNode.compute.feature_selection.push({
          column_ref: fullRef,
          status: "candidate",
          persisted: false,
          stage: targetNode.extension_type === "model" ? "train" : "transform",
          category: sourceColumn?.category || "",
          labels: [...(sourceColumn?.labels || [])],
          order: null,
        });
      }
    }
    const existingMapping = (targetNode.compute.column_mappings || []).find((mapping) => mapping.source === fullRef && mapping.target === sourceBindingName);
    if (!existingMapping) {
      targetNode.compute.column_mappings.push({
        source: fullRef,
        target: sourceBindingName,
      });
    }
    return sourceBindingName;
  }
  return "";
}

function makeContractSourceRef(node, bindingName) {
  if (node.kind === "contract") {
    return { node_id: node.id, field: bindingName };
  }
  return { node_id: node.id, column: bindingName };
}

function inferBindingTypeFromNode(node, bindingName) {
  if (node.kind === "data") {
    return node.columns.find((column) => column.name === bindingName)?.data_type || "string";
  }
  return node.kind === "contract" ? "string" : "derived";
}

function markDirty(rerenderGraphOnly) {
  state.dirty = true;
  updateToolbarState();
  if (rerenderGraphOnly) {
    renderGraph();
  }
}

function clearFilters() {
  state.searchTerm = "";
  state.kindFilter = "all";
  setTracedColumns([]);
  clearPickedColumns();
  clearPendingBindingState();
  searchInput.value = "";
  kindFilterSelect.value = "all";
  render();
  fitGraphToViewport();
}

function clearTracedColumns() {
  setTracedColumns([]);
}

function cloneGraph(graph = state.graph) {
  return JSON.parse(JSON.stringify(graph));
}

function recordGraphHistory(snapshot = null) {
  if (!state.graph) return;
  state.historyPast.push(snapshot || cloneGraph());
  if (state.historyPast.length > 80) {
    state.historyPast.shift();
  }
  state.historyFuture = [];
}

function restoreGraphSnapshot(snapshot) {
  if (!snapshot) return;
  state.graph = cloneGraph(snapshot);
  clearPendingBindingState();
  clearPickedColumns();
  state.selectionMode = "node";
  if (!getNodeById(state.selectedNodeId)) {
    state.selectedNodeId = state.graph.nodes[0]?.id || null;
  }
  if (!getEdgeById(state.selectedEdgeId)) {
    state.selectedEdgeId = null;
  }
  syncAuthoringState();
  state.needsAutoLayout = true;
  state.dirty = true;
  render();
}

function undoGraphChange() {
  if (!state.historyPast.length || !state.graph) return;
  state.historyFuture.push(cloneGraph());
  const previous = state.historyPast.pop();
  restoreGraphSnapshot(previous);
  setStatus("Undo applied", "Reverted the latest graph edit.");
}

function redoGraphChange() {
  if (!state.historyFuture.length || !state.graph) return;
  state.historyPast.push(cloneGraph());
  const next = state.historyFuture.pop();
  restoreGraphSnapshot(next);
  setStatus("Redo applied", "Reapplied the latest reverted graph edit.");
}

function adjustZoom(delta, anchor = null) {
  setZoom(state.zoom + delta, anchor, { manual: true });
}

function handleZoomInputChange(event) {
  const nextValue = Number(event.target.value);
  if (!Number.isFinite(nextValue)) {
    updateToolbarState();
    return;
  }
  setZoom(nextValue / 100, null, { manual: true });
}

function setZoom(nextZoom, anchor = null, options = {}) {
  const clamped = Math.min(GRAPH_MAX_ZOOM, Math.max(GRAPH_MIN_ZOOM, Number(nextZoom.toFixed(2))));
  if (clamped === state.zoom) {
    return;
  }
  const anchorPoint = anchor || {
    x: graphScroller.clientWidth / 2,
    y: graphScroller.clientHeight / 2,
  };
  const contentX = (graphScroller.scrollLeft + anchorPoint.x) / state.zoom;
  const contentY = (graphScroller.scrollTop + anchorPoint.y) / state.zoom;
  state.zoom = clamped;
  state.hasManualZoom = options.manual !== false;
  renderGraph();
  updateToolbarState();
  graphScroller.scrollTo({
    left: Math.max(0, contentX * state.zoom - anchorPoint.x),
    top: Math.max(0, contentY * state.zoom - anchorPoint.y),
  });
}

function resetZoom() {
  state.hasManualZoom = true;
  setZoom(1, null, { manual: true });
}

function toggleGraphFullscreen() {
  state.graphFullscreen = !state.graphFullscreen;
  updateToolbarState();
  if (!state.hasManualZoom) {
    requestAnimationFrame(() => {
      fitGraphToViewport();
    });
  }
}

function handleWindowResize() {
  if (state.legendPosition) {
    const maxX = Math.max(12, graphScroller.clientWidth - (graphLegendPanel?.offsetWidth || 244) - 12);
    const maxY = Math.max(12, graphScroller.clientHeight - (graphLegendPanel?.offsetHeight || 140) - 12);
    state.legendPosition = {
      x: Math.min(state.legendPosition.x, maxX),
      y: Math.min(state.legendPosition.y, maxY),
    };
    positionLegendPanel();
  }
  if (!state.graph || state.hasManualZoom) {
    return;
  }
  fitGraphToViewport();
}

function fitGraphToViewport() {
  const projected = getViewGraph();
  if (!projected.nodes.length) {
    return;
  }
  if (state.needsAutoLayout && !dragState) {
    applyLaneLayout(projected.nodes);
    state.needsAutoLayout = false;
  }
  const dimensions = getCanvasDimensions(projected.nodes);
  const horizontalZoom = graphScroller.clientWidth / dimensions.width;
  const verticalZoom = graphScroller.clientHeight / dimensions.height;
  state.zoom = Math.min(GRAPH_MAX_ZOOM, Math.max(GRAPH_MIN_ZOOM, Number(Math.min(horizontalZoom, verticalZoom, 1).toFixed(2))));
  renderGraph();
  graphScroller.scrollTo({ left: 0, top: 0 });
  updateToolbarState();
}

function toggleGraphDetails(nextValue = null) {
  const next = nextValue == null ? !state.showGraphDetails : Boolean(nextValue);
  if (next === state.showGraphDetails) {
    updateToolbarState();
    return;
  }
  if (next) {
    state.graphDetailSnapshot = { ...state.expandedGraphNodes };
  } else {
    state.expandedGraphNodes = state.graphDetailSnapshot ? { ...state.graphDetailSnapshot } : {};
    state.graphDetailSnapshot = null;
  }
  state.showGraphDetails = next;
  state.needsAutoLayout = true;
  render();
  if (!state.hasManualZoom) {
    fitGraphToViewport();
  }
}

function handleGraphWheel(event) {
  if (!(event.ctrlKey || event.metaKey)) {
    event.preventDefault();
    graphScroller.scrollLeft += event.deltaX;
    graphScroller.scrollTop += event.deltaY;
    return;
  }
  event.preventDefault();
  const rect = graphScroller.getBoundingClientRect();
  adjustZoom(event.deltaY < 0 ? 0.04 : -0.04, {
    x: event.clientX - rect.left,
    y: event.clientY - rect.top,
  });
}

function handleGraphDoubleClick(event) {
  const rect = graphScroller.getBoundingClientRect();
  adjustZoom(event.shiftKey ? -0.12 : 0.12, {
    x: event.clientX - rect.left,
    y: event.clientY - rect.top,
  });
}

function getTouchDistance(touches) {
  return Math.hypot(
    touches[0].clientX - touches[1].clientX,
    touches[0].clientY - touches[1].clientY,
  );
}

function handleGraphTouchStart(event) {
  if (event.touches.length !== 2) {
    return;
  }
  pinchState = {
    distance: getTouchDistance(event.touches),
    zoom: state.zoom,
  };
}

function handleGraphTouchMove(event) {
  if (event.touches.length !== 2 || !pinchState) {
    return;
  }
  event.preventDefault();
  const nextDistance = getTouchDistance(event.touches);
  const midpoint = {
    x: ((event.touches[0].clientX + event.touches[1].clientX) / 2) - graphScroller.getBoundingClientRect().left,
    y: ((event.touches[0].clientY + event.touches[1].clientY) / 2) - graphScroller.getBoundingClientRect().top,
  };
  const ratio = nextDistance / pinchState.distance;
  setZoom(pinchState.zoom * ratio, midpoint, { manual: true });
}

function handleGlobalKeyDown(event) {
  if (event.key === "Escape" && state.graphFullscreen) {
    state.graphFullscreen = false;
    updateToolbarState();
    if (!state.hasManualZoom) {
      requestAnimationFrame(() => fitGraphToViewport());
    }
    return;
  }
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "z" && !event.shiftKey) {
    if (event.target instanceof HTMLElement && ["INPUT", "TEXTAREA", "SELECT"].includes(event.target.tagName)) {
      return;
    }
    event.preventDefault();
    undoGraphChange();
    return;
  }
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "z" && event.shiftKey) {
    if (event.target instanceof HTMLElement && ["INPUT", "TEXTAREA", "SELECT"].includes(event.target.tagName)) {
      return;
    }
    event.preventDefault();
    redoGraphChange();
  }
}

function handleGraphTouchEnd(event) {
  if (pinchState && pinchState.zoom !== state.zoom) {
    state.hasManualZoom = true;
  }
  if (!event.touches || event.touches.length < 2) {
    pinchState = null;
  }
}

function getGraphPointerPosition(event) {
  const bounds = graphScroller.getBoundingClientRect();
  return {
    x: (event.clientX - bounds.left + graphScroller.scrollLeft) / state.zoom,
    y: (event.clientY - bounds.top + graphScroller.scrollTop) / state.zoom,
  };
}

function beginDrag(event, nodeId) {
  if (event.target.closest("select, textarea, input, button")) {
    return;
  }
  const node = getNodeById(nodeId);
  if (!node) return;
  const pointer = getGraphPointerPosition(event);
  dragState = {
    nodeId,
    offsetX: pointer.x - node.position.x,
    offsetY: pointer.y - node.position.y,
  };
}

function beginLegendDrag(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement) || !target.closest("[data-legend-drag]")) {
    return;
  }
  if (target.closest("button")) {
    return;
  }
  positionLegendPanel();
  const bounds = graphScroller.getBoundingClientRect();
  legendDragState = {
    offsetX: event.clientX - bounds.left - (state.legendPosition?.x || 0),
    offsetY: event.clientY - bounds.top - (state.legendPosition?.y || 0),
  };
  event.preventDefault();
}

function onMouseMove(event) {
  if (legendDragState) {
    const bounds = graphScroller.getBoundingClientRect();
    const width = graphLegendPanel?.offsetWidth || 244;
    const height = graphLegendPanel?.offsetHeight || 140;
    state.legendPosition = {
      x: Math.max(12, Math.min(graphScroller.clientWidth - width - 12, event.clientX - bounds.left - legendDragState.offsetX)),
      y: Math.max(12, Math.min(graphScroller.clientHeight - height - 12, event.clientY - bounds.top - legendDragState.offsetY)),
    };
    positionLegendPanel();
    return;
  }
  if (!dragState) return;
  const node = getNodeById(dragState.nodeId);
  if (!node) return;
  const pointer = getGraphPointerPosition(event);
  node.position.x = Math.max(16, pointer.x - dragState.offsetX);
  node.position.y = Math.max(16, pointer.y - dragState.offsetY);
  state.dirty = true;
  renderGraph();
  updateToolbarState();
}

function onMouseUp() {
  dragState = null;
  legendDragState = null;
}

function getViewGraph() {
  const graph = state.graph;
  if (!graph) return { nodes: [], edges: [] };

  let visibleNodeIds = new Set(graph.nodes.map((node) => node.id));
  if (state.currentView === "contract") {
    visibleNodeIds = new Set(graph.nodes.filter((node) => {
      return ["source", "data", "compute"].includes(node.kind)
        || (node.kind === "contract" && node.extension_type === "api");
    }).map((node) => node.id));
  } else if (state.currentView === "ui") {
    visibleNodeIds = new Set(graph.nodes.filter((node) => {
      return ["data", "compute"].includes(node.kind)
        || (node.kind === "contract" && ["api", "ui"].includes(node.extension_type));
    }).map((node) => node.id));
  }

  if (state.currentView === "impact") {
    const seeds = getImpactSeedNodeIds();
    if (seeds.size) {
      visibleNodeIds = new Set([...visibleNodeIds].filter((nodeId) => getNodeLineageClosure(seeds, state.impactDirection).has(nodeId)));
    }
  }

  if (state.kindFilter !== "all") {
    visibleNodeIds = new Set([...visibleNodeIds].filter((nodeId) => getNodeById(nodeId)?.kind === state.kindFilter));
  }

  if (state.searchTerm) {
    visibleNodeIds = new Set([...visibleNodeIds].filter((nodeId) => matchesSearch(getNodeById(nodeId))));
  }

  return {
    nodes: state.graph.nodes.filter((node) => visibleNodeIds.has(node.id)),
    edges: state.graph.edges.filter((edge) => visibleNodeIds.has(edge.source) && visibleNodeIds.has(edge.target)),
  };
}

function getImpactSeedNodeIds() {
  const tracedRefs = getActiveTracedColumnRefs();
  if (tracedRefs.length) {
    const refs = getCombinedLineageRefs(state.impactDirection);
    return new Set([...refs].map((ref) => splitColumnRef(ref)?.nodeId).filter(Boolean));
  }
  if (state.selectionMode === "edge" && state.selectedEdgeId) {
    const edge = getEdgeById(state.selectedEdgeId);
    return edge ? new Set([edge.source, edge.target]) : new Set();
  }
  return state.selectedNodeId ? new Set([state.selectedNodeId]) : new Set();
}

function getHighlightContext(projected) {
  const visibleNodeIds = new Set(projected.nodes.map((node) => node.id));
  const visibleEdgeIds = new Set(projected.edges.map((edge) => edge.id));

  if (!state.graph) {
    return { nodeIds: visibleNodeIds, edgeIds: visibleEdgeIds };
  }

  const tracedRefs = getActiveTracedColumnRefs();
  if (tracedRefs.length) {
    const refs = getActiveLineageRefs();
    const nodeIds = new Set([...refs].map((ref) => splitColumnRef(ref)?.nodeId).filter((nodeId) => visibleNodeIds.has(nodeId)));
    const edgeIds = new Set(projected.edges.filter((edge) => edgeTouchesColumnRefs(edge, refs)).map((edge) => edge.id));
    return { nodeIds, edgeIds };
  }

  if (state.selectionMode === "edge" && state.selectedEdgeId) {
    const edge = getEdgeById(state.selectedEdgeId);
    if (!edge) return { nodeIds: visibleNodeIds, edgeIds: visibleEdgeIds };
    const direction = state.currentView === "impact" ? state.impactDirection : "both";
    const nodeIds = new Set([...getNodeLineageClosure(new Set([edge.source, edge.target]), direction)].filter((nodeId) => visibleNodeIds.has(nodeId)));
    const edgeIds = new Set(projected.edges.filter((candidate) => nodeIds.has(candidate.source) && nodeIds.has(candidate.target)).map((candidate) => candidate.id));
    edgeIds.add(edge.id);
    return { nodeIds, edgeIds };
  }

  if (state.selectedNodeId) {
    const direction = state.currentView === "impact" ? state.impactDirection : "both";
    const nodeIds = new Set([...getNodeLineageClosure(new Set([state.selectedNodeId]), direction)].filter((nodeId) => visibleNodeIds.has(nodeId)));
    const edgeIds = new Set(projected.edges.filter((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target)).map((edge) => edge.id));
    return { nodeIds, edgeIds };
  }

  return { nodeIds: visibleNodeIds, edgeIds: visibleEdgeIds };
}

function getNodeLineageClosure(seedIds, direction) {
  const visited = new Set(seedIds);
  const queue = [...seedIds];
  while (queue.length) {
    const current = queue.shift();
    state.graph.edges.forEach((edge) => {
      if ((direction === "both" || direction === "downstream") && edge.source === current && !visited.has(edge.target)) {
        visited.add(edge.target);
        queue.push(edge.target);
      }
      if ((direction === "both" || direction === "upstream") && edge.target === current && !visited.has(edge.source)) {
        visited.add(edge.source);
        queue.push(edge.source);
      }
    });
  }
  return visited;
}

function getColumnLineageClosure(seedRef, direction) {
  const graph = buildColumnGraph();
  const visited = new Set([seedRef]);
  const queue = [seedRef];
  while (queue.length) {
    const current = queue.shift();
    if (direction === "both" || direction === "downstream") {
      (graph.downstream.get(current) || []).forEach((nextRef) => {
        if (!visited.has(nextRef)) {
          visited.add(nextRef);
          queue.push(nextRef);
        }
      });
    }
    if (direction === "both" || direction === "upstream") {
      (graph.upstream.get(current) || []).forEach((nextRef) => {
        if (!visited.has(nextRef)) {
          visited.add(nextRef);
          queue.push(nextRef);
        }
      });
    }
  }
  return visited;
}

function buildColumnGraph() {
  const downstream = new Map();
  const upstream = new Map();
  const addLink = (from, to) => {
    if (!from || !to) return;
    if (!downstream.has(from)) downstream.set(from, []);
    if (!upstream.has(to)) upstream.set(to, []);
    downstream.get(from).push(to);
    upstream.get(to).push(from);
  };

  state.graph.edges.forEach((edge) => {
    (edge.column_mappings || []).forEach((mapping) => {
      addLink(`${edge.source}.${mapping.source_column}`, `${edge.target}.${mapping.target_column}`);
    });
  });

  state.graph.nodes.forEach((node) => {
    if (node.kind !== "contract") return;
    (node.contract.fields || []).forEach((field) => {
      (field.sources || []).forEach((source) => {
        const upstreamRef = source.column
          ? `${source.node_id}.${source.column}`
          : source.field
            ? `${source.node_id}.${source.field}`
            : "";
        addLink(upstreamRef, `${node.id}.${field.name}`);
      });
    });
  });

  return { downstream, upstream };
}

function buildEdgeClass(edgeId, highlightEdgeIds) {
  const classes = ["edge-line"];
  if (state.selectionMode === "edge" && state.selectedEdgeId === edgeId) {
    classes.push("selected");
  }
  if (highlightEdgeIds.has(edgeId)) {
    classes.push("highlight");
  } else if ((state.selectedNodeId || state.selectedEdgeId || hasActiveTrace()) && highlightEdgeIds.size) {
    classes.push("faded");
  }
  return classes.join(" ");
}

function ensureEdgeMarker(defs, markerIds, color) {
  if (markerIds.has(color)) {
    return markerIds.get(color);
  }
  const markerId = `edge-arrow-${color.replace(/[^a-z0-9]/gi, "").toLowerCase()}`;
  const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
  marker.setAttribute("id", markerId);
  marker.setAttribute("markerWidth", "6");
  marker.setAttribute("markerHeight", "6");
  marker.setAttribute("refX", "5.4");
  marker.setAttribute("refY", "3");
  marker.setAttribute("orient", "auto");
  marker.setAttribute("markerUnits", "strokeWidth");
  const markerPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
  markerPath.setAttribute("d", "M 0.7 0.9 L 5.1 3 L 0.7 5.1");
  markerPath.setAttribute("fill", "none");
  markerPath.setAttribute("stroke", color);
  markerPath.setAttribute("stroke-width", "1");
  markerPath.setAttribute("stroke-linecap", "round");
  markerPath.setAttribute("stroke-linejoin", "round");
  marker.appendChild(markerPath);
  defs.appendChild(marker);
  markerIds.set(color, markerId);
  return markerId;
}

function getEdgeVisualStateClasses(edgeId, highlightEdgeIds) {
  const classes = [];
  if (state.selectionMode === "edge" && state.selectedEdgeId === edgeId) {
    classes.push("selected");
  }
  if ((state.selectedNodeId || state.selectedEdgeId || hasActiveTrace()) && highlightEdgeIds.size && !highlightEdgeIds.has(edgeId)) {
    classes.push("faded");
  }
  return classes;
}

function buildNodeClass(nodeId, highlightNodeIds) {
  const node = getNodeById(nodeId);
  const classes = ["graph-node", node.kind];
  const health = getNodeDiagnostics(nodeId).health;
  if (health && health !== "healthy") {
    classes.push(`health-${health}`);
  }
  if (state.selectionMode === "node" && state.selectedNodeId === nodeId) {
    classes.push("selected");
  }
  if (getActiveTracedColumnRefs().some((ref) => ref.startsWith(`${nodeId}.`))) {
    classes.push("column-focused");
  }
  if ((state.selectedNodeId || state.selectedEdgeId || hasActiveTrace()) && highlightNodeIds.size && !highlightNodeIds.has(nodeId)) {
    classes.push("faded");
  }
  return classes.join(" ");
}

function isGraphNodeExpanded(nodeId) {
  return Boolean(state.showGraphDetails || state.expandedGraphNodes[nodeId]);
}

function getGraphNodeZIndex(nodeId) {
  if (state.selectionMode === "node" && state.selectedNodeId === nodeId) {
    return 6;
  }
  if (isGraphNodeExpanded(nodeId)) {
    return 4;
  }
  return 1;
}

function shouldShowGraphExtraDetails(nodeId) {
  return Boolean(state.showGraphDetails || state.graphNodeDetailOverrides[nodeId]);
}

function isGraphNodeEditMode(nodeId) {
  if (state.interactionMode !== "edit") {
    return false;
  }
  const node = getNodeById(nodeId);
  if (node?.kind === "data") {
    return true;
  }
  return Boolean(state.graphEditNodes[nodeId]);
}

function getNodeColumnCategories(node) {
  return [...new Set((node.columns || []).map((column) => column.category).filter(Boolean))].sort((left, right) => left.localeCompare(right));
}

function renderExpandedNodePills(node) {
  const pills = [];
  if (node.kind === "source") {
    pills.push(node.source?.provider || "source");
    if (node.source?.origin?.kind) {
      pills.push(node.source.origin.kind);
    }
  } else if (node.kind === "data") {
    if (node.data?.persistence) pills.push(node.data.persistence);
    pills.push(node.data?.persisted ? "persisted" : "not persisted");
  } else if (node.kind === "compute") {
    pills.push(node.compute?.runtime || "runtime missing");
    pills.push(node.extension_type);
  } else if (node.kind === "contract") {
    pills.push(node.extension_type);
    if (node.contract?.route) pills.push(node.contract.route);
    if (node.contract?.component) pills.push(node.contract.component);
  }
  return pills.map((pill) => `<span class="pill">${escapeHtml(pill)}</span>`).join("");
}

function summarizeColumnKeys(column) {
  const parts = [];
  if (column.primary_key) parts.push("primary key");
  if (column.composite_primary_key) parts.push("composite primary key");
  if (column.foreign_key) parts.push(`foreign key ${column.foreign_key}`);
  if (column.indexed) parts.push("index");
  return parts.join(" | ");
}

function getColumnNonNullCountEstimate(node, column) {
  const totalRows = Number(node.data?.row_count || 0);
  if (!totalRows) return 0;
  const nullPct = Number(column.null_pct || 0);
  return Math.max(0, totalRows * (1 - (nullPct / 100)));
}

function getNodeExtensionColor(node) {
  const extensionColors = {
    provider: "#b3882d",
    group: "#c9a541",
    object: "#d6b24c",
    url: "#b8a16b",
    disk_path: "#9b7b2f",
    api_endpoint: "#1e5f74",
    bucket_path: "#846a36",
    raw_dataset: "#2f6c5e",
    table: "#1f7f74",
    view: "#4c8e7f",
    materialized_view: "#215d59",
    feature_set: "#6b8d4b",
    transform: "#b95c3f",
    model: "#9e4f36",
    api: "#385a9b",
    ui: "#667ab0",
  };
  return extensionColors[node.extension_type] || "rgba(31, 26, 23, 0.12)";
}

function getNodeStorageKind(node) {
  if (node.kind === "source") {
    const originKind = node.source?.origin?.kind;
    if (originKind === "api_endpoint" || originKind === "url") return "api";
    if (originKind === "bucket_path") return "object";
    if (originKind === "disk_path") return "file";
    const rawAssetKind = node.source?.raw_assets?.[0]?.kind;
    if (rawAssetKind === "object_storage") return "object";
    if (rawAssetKind === "file" || rawAssetKind === "glob" || rawAssetKind === "directory") return "file";
    return "api";
  }
  if (node.kind === "data") {
    if (node.data?.storage_kind) return node.data.storage_kind;
    if (node.data?.persisted || ["warm", "hot"].includes(node.data?.persistence)) return "db";
    if (node.data?.local_path || node.data?.profile_target) return "file";
    return "file";
  }
  if (node.kind === "compute") {
    return "db";
  }
  return "api";
}

function getNodeStorageMeta(node) {
  const kind = getNodeStorageKind(node);
  if (node.kind === "data") {
    if (kind === "db") {
      return { kind: "db", label: node.data?.storage_name || storageNameFromPersistence(node.data?.persistence) };
    }
    if (kind === "file") {
      return { kind: "file", label: "raw file" };
    }
  }
  if (node.kind === "source") {
    if (kind === "api") return { kind: "api", label: node.source?.provider || "API" };
    if (kind === "object") return { kind: "object", label: "object store" };
    if (kind === "file") return { kind: "file", label: "landing file" };
  }
  if (node.kind === "compute") {
    return { kind: "db", label: node.compute?.runtime || node.extension_type };
  }
  if (node.kind === "contract") {
    return { kind: "api", label: node.extension_type };
  }
  return null;
}

function storageNameFromPersistence(persistence) {
  if (persistence === "hot") return "serving DB";
  if (persistence === "warm") return "analytics DB";
  if (persistence === "cold") return "raw store";
  return "DB";
}

function getNodeExtensionSortRank(node) {
  const ranks = {
    provider: 0,
    group: 1,
    object: 2,
    disk_path: 3,
    api_endpoint: 4,
    bucket_path: 5,
    url: 6,
    transform: 0,
    model: 1,
    raw_dataset: 0,
    feature_set: 1,
    table: 2,
    view: 3,
    materialized_view: 4,
    api: 0,
    ui: 1,
  };
  return ranks[node.extension_type] ?? 99;
}

function compareNodesForLayout(left, right) {
  const leftGroup = getSourceGroupKey(left);
  const rightGroup = getSourceGroupKey(right);
  if (left.kind === "source" && right.kind === "source" && leftGroup !== rightGroup) {
    return leftGroup.localeCompare(rightGroup);
  }
  if (left.position.y !== right.position.y) {
    return left.position.y - right.position.y;
  }
  if (left.position.x !== right.position.x) {
    return left.position.x - right.position.x;
  }
  const extensionCompare = getNodeExtensionSortRank(left) - getNodeExtensionSortRank(right);
  if (extensionCompare !== 0) {
    return extensionCompare;
  }
  return left.id.localeCompare(right.id);
}

function getDataSublane(node) {
  if (node.kind !== "data") {
    return "default";
  }
  if (node.extension_type === "raw_dataset") {
    return "raw";
  }
  if (node.extension_type === "feature_set" || node.data?.persisted === false) {
    return "feature";
  }
  return "serving";
}

function getSourceGroupKey(node) {
  if (node.kind !== "source") {
    return "";
  }
  return node.source?.provider || node.source?.group || node.label || "Source";
}

function getLaneColumns(nodes) {
  const columns = [];
  const sourceNodes = nodes.filter((node) => node.kind === "source").sort(compareNodesForLayout);
  if (sourceNodes.length) {
    columns.push({ id: "source", kind: "source", label: "Source layer", sublane: "source", nodes: sourceNodes });
  }
  const computeNodes = nodes.filter((node) => node.kind === "compute").sort(compareNodesForLayout);
  if (computeNodes.length) {
    columns.push({ id: "compute", kind: "compute", label: "Compute layer", sublane: "compute", nodes: computeNodes });
  }
  const dataDefinitions = [
    { key: "raw", label: "Raw data" },
    { key: "feature", label: "Feature layer" },
    { key: "serving", label: "Serving layer" },
  ];
  dataDefinitions.forEach((definition) => {
    const groupedNodes = nodes
      .filter((node) => node.kind === "data" && getDataSublane(node) === definition.key)
      .sort(compareNodesForLayout);
    if (groupedNodes.length) {
      columns.push({
        id: `data:${definition.key}`,
        kind: "data",
        label: definition.label,
        sublane: definition.key,
        nodes: groupedNodes,
      });
    }
  });
  const contractDefinitions = [
    { key: "api", label: "API layer" },
    { key: "ui", label: "UI layer" },
  ];
  contractDefinitions.forEach((definition) => {
    const groupedNodes = nodes
      .filter((node) => node.kind === "contract" && node.extension_type === definition.key)
      .sort(compareNodesForLayout);
    if (groupedNodes.length) {
      columns.push({
        id: `contract:${definition.key}`,
        kind: "contract",
        label: definition.label,
        sublane: definition.key,
        nodes: groupedNodes,
      });
    }
  });
  return columns;
}

function getBandColorFromKey(key) {
  const source = Array.from(key || "group").reduce((total, char) => total + char.charCodeAt(0), 0);
  const hue = source % 360;
  return `hsla(${hue}, 58%, 72%, 0.14)`;
}

function renderGraphLayerBands(nodes, dimensions) {
  const columns = getLaneColumns(nodes);
  const laneOrder = ["source", "compute", "data", "contract"];
  laneOrder.forEach((lane) => {
    const laneColumns = columns.filter((column) => column.kind === lane);
    if (!laneColumns.length) return;
    const laneNodes = laneColumns.flatMap((column) => column.nodes);
    const minX = Math.min(...laneNodes.map((node) => node.position.x)) - 20;
    const maxX = Math.max(...laneNodes.map((node) => node.position.x + getGraphNodeSize(node).width)) + 20;
    const band = document.createElement("div");
    band.className = `graph-layer-band ${lane}`;
    band.dataset.label = `${lane} layer`;
    band.style.left = `${Math.max(12, minX)}px`;
    band.style.width = `${Math.min(dimensions.width - 24, maxX - minX)}px`;
    graphCanvas.appendChild(band);

    if (lane === "source") {
      const grouped = new Map();
      laneNodes.forEach((node) => {
        const key = getSourceGroupKey(node);
        if (!grouped.has(key)) {
          grouped.set(key, []);
        }
        grouped.get(key).push(node);
      });
      grouped.forEach((groupNodes, key) => {
        const top = Math.min(...groupNodes.map((node) => node.position.y)) - 10;
        const bottom = Math.max(...groupNodes.map((node) => node.position.y + getGraphNodeSize(node).height)) + 10;
        const subgroup = document.createElement("div");
        subgroup.className = "graph-layer-band subgroup";
        subgroup.dataset.label = key;
        subgroup.style.left = `${Math.max(18, minX + 8)}px`;
        subgroup.style.width = `${Math.max(120, maxX - minX - 16)}px`;
        subgroup.style.top = `${Math.max(42, top)}px`;
        subgroup.style.height = `${Math.max(76, bottom - top)}px`;
        subgroup.style.bottom = "auto";
        subgroup.style.setProperty("--group-band-color", getBandColorFromKey(key));
        graphCanvas.appendChild(subgroup);
      });
    }

    if (lane === "data") {
      laneColumns.forEach((column) => {
        const sublaneNodes = column.nodes;
        if (!sublaneNodes.length) return;
        const subMinX = Math.min(...sublaneNodes.map((node) => node.position.x)) - 10;
        const subMaxX = Math.max(...sublaneNodes.map((node) => node.position.x + getGraphNodeSize(node).width)) + 10;
        const subBand = document.createElement("div");
        subBand.className = `graph-layer-band sublayer ${column.sublane}`;
        subBand.dataset.label = column.label;
        subBand.style.left = `${Math.max(18, subMinX)}px`;
        subBand.style.width = `${Math.max(120, subMaxX - subMinX)}px`;
        graphCanvas.appendChild(subBand);
      });
    }

    if (lane === "contract") {
      laneColumns.forEach((column) => {
        const sublaneNodes = column.nodes;
        if (!sublaneNodes.length) return;
        const subMinX = Math.min(...sublaneNodes.map((node) => node.position.x)) - 10;
        const subMaxX = Math.max(...sublaneNodes.map((node) => node.position.x + getGraphNodeSize(node).width)) + 10;
        const subBand = document.createElement("div");
        subBand.className = `graph-layer-band sublayer ${column.sublane}`;
        subBand.dataset.label = column.label;
        subBand.style.left = `${Math.max(18, subMinX)}px`;
        subBand.style.width = `${Math.max(120, subMaxX - subMinX)}px`;
        graphCanvas.appendChild(subBand);
      });
    }
  });
}

function getGraphNodeSize(node) {
  const workSectionHeight = getGraphWorkSectionHeight(node);
  if (isGraphNodeExpanded(node.id)) {
    if (node.kind === "data") {
      const pageState = getGraphTablePageState(node);
      const showExtraDetails = shouldShowGraphExtraDetails(node.id);
      const editMode = isGraphNodeEditMode(node.id);
      return {
        width: showExtraDetails ? 620 : 500,
        height: Math.max(
          296,
          178
            + pageState.items.length * 42
            + (pageState.pendingNew ? 52 : 0)
            + (editMode ? 48 : 0)
            + (pageState.totalPages > 1 ? 46 : 0)
            + workSectionHeight
        ),
      };
    }
    const itemCount = getGraphNodeExpandedItems(node).length;
    const expandedHeightByKind = {
      contract: 238 + itemCount * 70 + workSectionHeight,
      compute: 212 + itemCount * 50 + workSectionHeight,
      source: 208 + itemCount * 50 + workSectionHeight,
    };
    return {
      width: node.kind === "contract" ? 430 : GRAPH_CARD_EXPANDED.width,
      height: Math.max(GRAPH_CARD_EXPANDED.height, expandedHeightByKind[node.kind] || (180 + itemCount * 42)),
    };
  }
  return { ...GRAPH_CARD_BASE };
}

function getGraphWorkSectionHeight(node) {
  const items = getNodeWorkItems(node);
  return 74 + Math.max(1, items.length) * 42;
}

function getGraphNodeExpandedItems(node) {
  if (node.kind === "data") {
    return getGraphTablePageState(node).items.map((entry) => entry.column);
  }
  if (node.kind === "contract") {
    return (node.contract?.fields || []).slice(0, 8);
  }
  if (node.kind === "compute") {
    return (node.compute?.feature_selection || []).slice(0, 6);
  }
  return (node.source?.raw_assets || []).slice(0, 6);
}

function renderGraphNodeCard(node) {
  const summary = buildNodeMeta(node);
  const expanded = isGraphNodeExpanded(node.id);
  const description = node.description || "No description.";
  const storageMeta = getNodeStorageMeta(node);
  const refreshMeta = getNodeRefreshMeta(node);
  const workStatus = getNodeWorkStatus(node);
  const diagnostics = getNodeDiagnostics(node.id);
  const editMode = state.interactionMode === "edit";
  return `
    <span class="graph-node-port inbound" aria-hidden="true"></span>
    <span class="graph-node-port outbound" aria-hidden="true"></span>
    <div class="graph-node-header">
      <div>
        <div class="graph-node-topline">
          <strong>${escapeHtml(node.label)}</strong>
          <div class="graph-node-topline-badges">
            ${storageMeta ? `<span class="graph-node-storage ${escapeHtml(storageMeta.kind)}">${escapeHtml(storageMeta.label)}</span>` : ""}
            ${refreshMeta ? `<span class="graph-node-refresh">${escapeHtml(refreshMeta.badge)}</span>` : ""}
          </div>
        </div>
        <p>${escapeHtml(description)}</p>
      </div>
      <div class="graph-node-actions">
        <button class="graph-work-status ${escapeHtml(workStatus)}" type="button" data-graph-cycle-work-status="${escapeHtml(node.id)}">${escapeHtml(WORK_STATUS_LABELS[workStatus])}</button>
        ${editMode ? `<button class="text-button danger-link" type="button" data-graph-remove-node="${escapeHtml(node.id)}">Remove</button>` : ""}
        <button class="text-button" type="button" data-graph-toggle-expand="${escapeHtml(node.id)}" ${state.showGraphDetails ? "disabled" : ""}>${expanded ? "Hide Details" : "Show Details"}</button>
      </div>
    </div>
    <div class="graph-node-health-summary ${escapeHtml(diagnostics.health || "healthy")}">↑ ${escapeHtml(String(diagnostics.upstream_count || 0))} &nbsp; ↓ ${escapeHtml(String(diagnostics.downstream_count || 0))} &nbsp; ${getHealthIcon(diagnostics.health || "healthy")} ${escapeHtml(diagnostics.health || "healthy")}</div>
    ${expanded ? `
      <div class="graph-node-kind">
        <span class="pill">${node.kind} / ${node.extension_type}</span>
      </div>
    ` : ""}
    <div class="graph-node-summary">${escapeHtml(summary)}</div>
    ${expanded ? `<div class="graph-node-subsummary">${renderExpandedNodePills(node)}</div>` : ""}
    ${expanded ? renderExpandedGraphNodeBody(node) : ""}
  `;
}

function renderExpandedGraphNodeBody(node) {
  if (node.kind === "data") {
    return renderExpandedGraphDataTable(node);
  }
  const items = getGraphNodeExpandedItems(node);
  if (node.kind === "contract") {
    const editMode = state.interactionMode === "edit";
    const diagnostics = getContractDiagnostics(node.id);
    return `
      <div class="graph-node-body">
        ${renderGraphPickedToolbar(node)}
        <div class="graph-node-columns">
          ${items.length ? items.map((field) => `
            <div
              class="graph-node-column ${getGraphColumnLineageClass(`${node.id}.${field.name}`)} ${getLineageSeedColors(`${node.id}.${field.name}`).length ? "lineage-active" : ""} ${isColumnPicked(`${node.id}.${field.name}`) ? "picked" : ""}"
              data-binding-ref="${escapeHtml(`${node.id}.${field.name}`)}"
              ${getLineageStyleAttribute(`${node.id}.${field.name}`)}
            >
              <div class="graph-node-column-head">
                <strong>${escapeHtml(field.name)}</strong>
                <div class="row-actions">
                  <button class="text-button" type="button" data-graph-focus-column="${escapeHtml(`${node.id}.${field.name}`)}">${isColumnTraced(`${node.id}.${field.name}`) ? "Stop" : "Track"}</button>
                  ${editMode ? `<button class="text-button" type="button" data-graph-contract-link="${escapeHtml(node.id)}:${escapeHtml(field.name)}">${isPendingBindingTarget(node.id, field.name) ? "Cancel target" : state.pendingColumnLinkRef ? "Connect here" : "Rebind"}</button>` : ""}
                  ${editMode ? `<button class="text-button" type="button" data-graph-pick-column="${escapeHtml(`${node.id}.${field.name}`)}">${isColumnPicked(`${node.id}.${field.name}`) ? "Unpick" : "Pick"}</button>` : ""}
                </div>
              </div>
              <div class="graph-node-column-meta">${escapeHtml(diagnostics.bindings?.[field.name]?.primary_binding || formatSources(field.sources || []) || "broken")}</div>
              ${diagnostics.bindings?.[field.name]?.why_this_matters ? `<div class="graph-node-column-meta issue">${escapeHtml(diagnostics.bindings[field.name].why_this_matters)}</div>` : ""}
            </div>
          `).join("") : "<div class=\"graph-node-column\">No fields recorded.</div>"}
        </div>
        ${renderGraphWorkSection(node)}
      </div>
    `;
  }
  if (node.kind === "compute") {
    return `
      <div class="graph-node-body">
        ${renderGraphPickedToolbar(node)}
        <div class="graph-node-summary">inputs: ${formatValue((node.compute?.inputs || []).length)} | outputs: ${formatValue((node.compute?.outputs || []).length)}</div>
        <div class="graph-node-columns">
          ${items.length ? items.map((feature, index) => `
            <div class="graph-node-column ${getGraphColumnLineageClass(feature.column_ref || "")} ${getLineageSeedColors(feature.column_ref || "").length ? "lineage-active" : ""} ${isColumnPicked(feature.column_ref || "") ? "picked" : ""}" ${getLineageStyleAttribute(feature.column_ref || "")}>
              <div class="graph-node-column-head">
                <strong>${escapeHtml(feature.column_ref || "feature")}</strong>
                <div class="row-actions">
                  <span class="pill">${escapeHtml(feature.status || "candidate")}</span>
                  ${feature.column_ref ? `<button class="text-button" type="button" data-graph-pick-column="${escapeHtml(feature.column_ref)}">${isColumnPicked(feature.column_ref) ? "Unpick" : "Pick"}</button>` : ""}
                </div>
              </div>
              <div class="graph-node-column-meta">${escapeHtml((feature.category || "uncategorized"))} | ${escapeHtml((feature.labels || []).join(", ") || "no labels")}</div>
            </div>
          `).join("") : "<div class=\"graph-node-column\">No features recorded.</div>"}
        </div>
        ${renderGraphWorkSection(node)}
      </div>
    `;
  }
  return `
    <div class="graph-node-body">
      ${renderGraphPickedToolbar(node)}
      <div class="graph-node-summary">origin: ${escapeHtml(node.source?.origin?.kind || "source")} | ${escapeHtml(node.source?.origin?.value || "missing")}</div>
      <div class="graph-node-columns">
        ${(node.source?.data_dictionaries || []).map((dictionary) => `
          <div class="graph-node-column">
            <div class="graph-node-column-head">
              <strong>${escapeHtml(dictionary.label || "dictionary")}</strong>
              <span class="pill">${escapeHtml(dictionary.kind || "link")}</span>
            </div>
            <div class="graph-node-column-meta">${escapeHtml(dictionary.value || "missing")}</div>
          </div>
        `).join("")}
        ${items.length ? items.map((asset) => `
          <div class="graph-node-column">
            <div class="graph-node-column-head">
              <strong>${escapeHtml(asset.label || "raw asset")}</strong>
              <span class="pill">${escapeHtml(asset.format || "unknown")}</span>
            </div>
            <div class="graph-node-column-meta">${escapeHtml(asset.value || "missing path")}</div>
          </div>
        `).join("") : "<div class=\"graph-node-column\">No raw assets recorded.</div>"}
      </div>
      ${renderGraphWorkSection(node)}
    </div>
  `;
}

function renderExpandedGraphDataTable(node) {
  const pageState = getGraphTablePageState(node);
  const showExtraDetails = shouldShowGraphExtraDetails(node.id);
  const editMode = isGraphNodeEditMode(node.id);
  const categoryOptions = getNodeColumnCategories(node);
  const pickedRefs = getPickedColumnRefs();
  const pickedOwnRefs = pickedRefs.filter((ref) => ref.startsWith(`${node.id}.`));
  const refreshMeta = getNodeRefreshMeta(node);
  return `
    <div class="graph-node-body">
      <div class="graph-node-inline-toolbar">
        <div class="graph-node-summary">
          obs: ${formatValue(node.data?.row_count)}${refreshMeta ? ` | ${escapeHtml(refreshMeta.badge)}` : ""}
        </div>
        <div class="row-actions">
          ${pickedRefs.length ? `<button class="text-button" type="button" data-graph-attach-picked="${escapeHtml(node.id)}">Use ${pickedRefs.length} picked</button>` : ""}
          ${editMode && pickedOwnRefs.length ? `<button class="text-button danger-link" type="button" data-graph-remove-picked="${escapeHtml(node.id)}">Remove ${pickedOwnRefs.length} picked</button>` : ""}
          <span class="pill">${pageState.totalCount} columns</span>
          <button class="text-button" type="button" data-graph-node-details="${escapeHtml(node.id)}">${showExtraDetails ? "Hide metrics" : "Show metrics"}</button>
        </div>
      </div>
      ${editMode && categoryOptions.length ? `
        <div class="graph-inline-tags">
          ${categoryOptions.map((category) => `<span class="graph-category-chip" draggable="true" data-category-chip="${escapeHtml(category)}">${escapeHtml(category)}</span>`).join("")}
        </div>
      ` : ""}
      <div class="graph-node-table-wrap">
        ${pageState.items.length ? `
          <table class="graph-node-table">
            <thead>
              <tr>
                <th>Column</th>
                <th>Type</th>
                <th>Category</th>
                ${showExtraDetails ? "<th>Keys / index</th><th>Summary</th>" : ""}
                <th>Trace</th>
              </tr>
            </thead>
            <tbody>
              ${pageState.items.map(({ column, index }) => renderExpandedDataColumn(node, column, index, { showExtraDetails, editMode })).join("")}
              ${editMode ? renderPendingNewDataColumnRow(node, pageState, { showExtraDetails }) : ""}
            </tbody>
          </table>
        ` : editMode ? `
          <table class="graph-node-table">
            <thead>
              <tr>
                <th>Column</th>
                <th>Type</th>
                <th>Category</th>
                ${showExtraDetails ? "<th>Keys / index</th><th>Summary</th>" : ""}
                <th>Trace</th>
              </tr>
            </thead>
            <tbody>
              ${renderPendingNewDataColumnRow(node, pageState, { showExtraDetails })}
            </tbody>
          </table>
        ` : "<div class=\"graph-table-empty\">No columns recorded.</div>"}
        ${renderGraphTablePager(node.id, pageState)}
      </div>
      ${renderGraphWorkSection(node)}
    </div>
  `;
}

function renderDataTypeControl(nodeId, index, currentType = "") {
  const normalizedType = (currentType || "").toLowerCase();
  const useCustomType = normalizedType && !DEFAULT_COLUMN_TYPES.includes(normalizedType);
  const selectedType = useCustomType ? "other" : normalizedType;
  return `
    <div class="graph-type-editor">
      <select class="graph-inline-select" data-graph-column-index="${index}" data-graph-column-field="data_type_select" data-graph-node-id="${escapeHtml(nodeId)}">
        <option value="" ${!selectedType ? "selected" : ""}>type</option>
        ${DEFAULT_COLUMN_TYPES.map((type) => `<option value="${type}" ${selectedType === type ? "selected" : ""}>${type}</option>`).join("")}
        <option value="other" ${selectedType === "other" ? "selected" : ""}>other</option>
      </select>
      ${selectedType === "other"
        ? `<input class="graph-inline-input" data-graph-column-index="${index}" data-graph-column-field="data_type" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(currentType || "")}" placeholder="custom type" />`
        : ""}
    </div>
  `;
}

function renderExpandedDataColumn(node, column, index, options = {}) {
  const columnRef = `${node.id}.${column.name}`;
  const statsSummary = summarizeColumnStats(node, column.stats || {}, column);
  const keySummary = summarizeColumnKeys(column);
  const labelValue = (column.labels || []).join(", ");
  const connectLabel = state.pendingColumnLinkRef === columnRef
    ? "Cancel"
    : state.pendingColumnLinkRef
      ? "Connect here"
      : "Connect";
  return `
    <tr class="graph-table-row ${getGraphColumnLineageClass(columnRef)} ${getLineageSeedColors(columnRef).length ? "lineage-active" : ""} ${state.pendingColumnLinkRef === columnRef ? "graph-link-pending" : ""} ${isColumnPicked(columnRef) ? "picked" : ""}" data-binding-ref="${escapeHtml(columnRef)}" ${getLineageStyleAttribute(columnRef)}>
      <td class="graph-table-column">
        ${options.editMode
          ? `<input class="graph-inline-input" data-graph-column-index="${index}" data-graph-column-field="name" data-graph-node-id="${escapeHtml(node.id)}" value="${escapeHtml(column.name || "")}" placeholder="column_name" />`
          : escapeHtml(column.name)}
      </td>
      <td>
        ${options.editMode
          ? renderDataTypeControl(node.id, index, column.data_type || "")
          : escapeHtml(column.data_type || "unknown")}
      </td>
      <td class="graph-category-cell">
        ${options.editMode
          ? `
            <input class="graph-inline-input" data-graph-column-index="${index}" data-graph-column-field="category" data-graph-node-id="${escapeHtml(node.id)}" data-category-drop="${escapeHtml(node.id)}:${index}" value="${escapeHtml(column.category || "")}" placeholder="category" />
            <input class="graph-inline-input" data-graph-column-index="${index}" data-graph-column-field="labels" data-graph-node-id="${escapeHtml(node.id)}" value="${escapeHtml(labelValue)}" placeholder="labels" />
          `
          : escapeHtml(column.category || "Uncategorized")}
      </td>
      ${options.showExtraDetails ? `
        <td>${escapeHtml(keySummary)}</td>
        <td>${escapeHtml(statsSummary || "")}</td>
      ` : ""}
      <td>
        <div class="graph-action-stack">
          <button class="text-button" type="button" data-graph-focus-column="${escapeHtml(columnRef)}">${isColumnTraced(columnRef) ? "Stop" : "Track"}</button>
          <button class="text-button" type="button" data-graph-pick-column="${escapeHtml(columnRef)}">${isColumnPicked(columnRef) ? "Unpick" : "Pick"}</button>
          <button class="text-button graph-connect-button ${state.pendingColumnLinkRef === columnRef ? "pending" : ""}" type="button" data-graph-column-link="${escapeHtml(columnRef)}">${connectLabel}</button>
          ${options.editMode ? `<button class="text-button danger-link" type="button" data-graph-column-remove="${escapeHtml(node.id)}:${index}">Remove</button>` : ""}
        </div>
      </td>
    </tr>
  `;
}

function renderPendingNewDataColumnRow(node, pageState, options = {}) {
  const pending = pageState.pendingNew;
  const editMode = isGraphNodeEditMode(node.id);
  if (!editMode) {
    return "";
  }
  if (!pending) {
    return `
      <tr class="graph-table-row graph-table-add-row">
        <td colspan="${options.showExtraDetails ? 6 : 4}">
          <button class="text-button" type="button" data-graph-column-add="${escapeHtml(node.id)}">+ Add variable</button>
        </td>
      </tr>
    `;
  }
  const column = pending.column;
  return `
    <tr class="graph-table-row graph-table-add-row">
      <td><input class="graph-inline-input" data-graph-column-index="${pending.index}" data-graph-column-field="name" data-graph-node-id="${escapeHtml(node.id)}" value="${escapeHtml(column.name || "")}" placeholder="column_name" /></td>
      <td>${renderDataTypeControl(node.id, pending.index, column.data_type || "")}</td>
      <td class="graph-category-cell">
        <input class="graph-inline-input" data-graph-column-index="${pending.index}" data-graph-column-field="category" data-graph-node-id="${escapeHtml(node.id)}" data-category-drop="${escapeHtml(node.id)}:${pending.index}" value="${escapeHtml(column.category || "")}" placeholder="category" />
        <input class="graph-inline-input" data-graph-column-index="${pending.index}" data-graph-column-field="labels" data-graph-node-id="${escapeHtml(node.id)}" value="${escapeHtml((column.labels || []).join(", "))}" placeholder="labels" />
      </td>
      ${options.showExtraDetails ? "<td></td><td></td>" : ""}
      <td>
        <div class="graph-inline-row-actions">
          <button class="text-button" type="button" data-graph-column-commit="${escapeHtml(node.id)}:${pending.index}">Add</button>
          <button class="text-button" type="button" data-graph-column-cancel="${escapeHtml(node.id)}:${pending.index}">Cancel</button>
        </div>
      </td>
    </tr>
  `;
}

function renderGraphPickedToolbar(node) {
  const pickedRefs = getPickedColumnRefs();
  if (!pickedRefs.length || node.kind === "source") {
    return "";
  }
  return `
    <div class="graph-node-inline-toolbar">
      <span class="graph-picked-pill">${pickedRefs.length} picked</span>
      <div class="row-actions">
        <button class="text-button" type="button" data-graph-attach-picked="${escapeHtml(node.id)}">Use picked here</button>
      </div>
    </div>
  `;
}

function renderGraphWorkSection(node) {
  const items = getNodeWorkItems(node);
  const editMode = state.interactionMode === "edit";
  return `
    <div class="graph-work-section">
      <div class="graph-node-inline-toolbar">
        <div class="graph-node-summary">notes: ${items.length}</div>
        <div class="row-actions">
          ${editMode ? `<button class="text-button" type="button" data-graph-work-item-add="${escapeHtml(node.id)}">Add note</button>` : ""}
        </div>
      </div>
      <div class="graph-work-list">
        ${items.length ? items.map((item, index) => renderGraphWorkItem(node.id, item, index)).join("") : "<div class=\"graph-node-column\">No stories, tasks, or bugs yet.</div>"}
      </div>
    </div>
  `;
}

function renderGraphWorkItem(nodeId, item, index) {
  const editMode = state.interactionMode === "edit";
  return `
    <div class="graph-work-item">
      <select class="graph-inline-select" data-graph-work-item-index="${index}" data-graph-work-item-field="kind" data-graph-node-id="${escapeHtml(nodeId)}" ${editMode ? "" : "disabled"}>
        ${["task", "story", "bug", "note"].map((value) => `<option value="${value}" ${value === (item.kind || "task") ? "selected" : ""}>${value}</option>`).join("")}
      </select>
      <textarea class="graph-inline-input graph-inline-note" data-graph-work-item-index="${index}" data-graph-work-item-field="text" data-graph-node-id="${escapeHtml(nodeId)}" placeholder="Add a quick graph note" ${editMode ? "" : "readonly"}>${escapeHtml(item.text || "")}</textarea>
      ${editMode ? `<button class="inline-button danger" type="button" data-graph-work-item-remove="${escapeHtml(nodeId)}:${index}">Remove</button>` : ""}
    </div>
  `;
}

function getTraceSeedColor(ref) {
  const refs = getActiveTracedColumnRefs();
  const index = refs.indexOf(ref);
  return TRACE_PALETTE[index >= 0 ? index % TRACE_PALETTE.length : 0];
}

function getLineageSeedColors(ref) {
  if (!ref || !hasActiveTrace()) {
    return [];
  }
  const direction = getLineageDirectionForView();
  return getActiveTracedColumnRefs()
    .filter((seedRef) => getColumnLineageClosure(seedRef, direction).has(ref))
    .map((seedRef) => getTraceSeedColor(seedRef));
}

function getLineageStyleAttribute(ref) {
  const colors = getLineageSeedColors(ref);
  if (!colors.length) {
    return "";
  }
  const segmentWidth = 100 / colors.length;
  const stops = colors.map((color, index) => {
    const start = Number((index * segmentWidth).toFixed(2));
    const end = Number(((index + 1) * segmentWidth).toFixed(2));
    return `${color} ${start}% ${end}%`;
  }).join(", ");
  return `style="--lineage-stripes: linear-gradient(90deg, ${stops}); --trace-accent: ${colors[0]};"`;
}

function getEdgeTraceColors(edge) {
  if (!hasActiveTrace()) {
    return [];
  }
  const direction = getLineageDirectionForView();
  return getActiveTracedColumnRefs()
    .filter((seedRef) => edgeTouchesColumnRefs(edge, getColumnLineageClosure(seedRef, direction)))
    .map((seedRef) => getTraceSeedColor(seedRef));
}

function summarizeColumnStats(node, stats, column) {
  const entries = Object.entries(stats || {});
  if (!entries.length) {
    return "";
  }
  if (typeof stats.mean === "number") {
    const std = typeof stats.std === "number" ? stats.std : null;
    return `${stats.mean}${std != null ? ` (${std})` : ""}`;
  }
  if (stats.mode) {
    const nonNullEstimate = getColumnNonNullCountEstimate(node, column);
    const modeCount = Number(stats.mode_count || stats.top_values?.[0]?.[1] || 0);
    const pct = nonNullEstimate ? Math.round((modeCount / nonNullEstimate) * 1000) / 10 : null;
    return `${stats.mode}${pct != null ? ` (${pct}%)` : ""}`;
  }
  return entries.slice(0, 2).map(([key, value]) => `${key}: ${Array.isArray(value) ? JSON.stringify(value) : value}`).join(" | ");
}

function renderGraphTablePager(nodeId, pageState) {
  if (pageState.totalPages <= 1) {
    return "";
  }
  return `
    <div class="graph-table-pagination">
      <span class="hint">Page ${pageState.page + 1} of ${pageState.totalPages}</span>
      <div class="row-actions">
        <button class="ghost-button" type="button" data-graph-table-page="${escapeHtml(nodeId)}:prev" ${pageState.page === 0 ? "disabled" : ""}>Prev</button>
        <button class="ghost-button" type="button" data-graph-table-page="${escapeHtml(nodeId)}:next" ${pageState.page >= pageState.totalPages - 1 ? "disabled" : ""}>Next</button>
      </div>
    </div>
  `;
}

function getGraphColumnLineageClass(ref) {
  if (!ref) return "";
  if (isColumnTraced(ref)) {
    return "tracked";
  }
  if (isColumnHighlightedRef(ref)) {
    return "lineage";
  }
  return "";
}

function getGraphTablePageState(node) {
  const pageSize = 10;
  const allColumns = (node.columns || []).map((column, index) => ({ column, index }));
  const pendingNew = allColumns.find((entry) => entry.column.__isNew);
  const columns = allColumns.filter((entry) => !entry.column.__isNew);
  const tracedRefs = new Set(getActiveTracedColumnRefs());
  const lineageRefs = getActiveLineageRefs();
  const ordered = [...columns].sort((left, right) => {
    const leftRef = `${node.id}.${left.column.name}`;
    const rightRef = `${node.id}.${right.column.name}`;
    const leftRank = tracedRefs.has(leftRef) ? 0 : lineageRefs.has(leftRef) ? 1 : 2;
    const rightRank = tracedRefs.has(rightRef) ? 0 : lineageRefs.has(rightRef) ? 1 : 2;
    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    return left.index - right.index;
  });
  const totalPages = Math.max(1, Math.ceil(ordered.length / pageSize));
  const rawPage = Number(state.graphTablePages[node.id] || 0);
  const page = Math.max(0, Math.min(totalPages - 1, rawPage));
  state.graphTablePages[node.id] = page;
  return {
    page,
    pageSize,
    totalPages,
    totalCount: ordered.length,
    items: ordered.slice(page * pageSize, page * pageSize + pageSize),
    pendingNew,
  };
}

function shiftGraphTablePage(nodeId, direction) {
  const node = getNodeById(nodeId);
  if (!node || node.kind !== "data") {
    return;
  }
  const pageState = getGraphTablePageState(node);
  const nextPage = direction === "next" ? pageState.page + 1 : pageState.page - 1;
  state.graphTablePages[nodeId] = Math.max(0, Math.min(pageState.totalPages - 1, nextPage));
  renderGraph();
}

function applyLaneLayout(nodes) {
  if (!nodes.length) return;
  if (typeof dagre !== "undefined") {
    const g = new dagre.graphlib.Graph();
    g.setGraph({ rankdir: "LR", align: "UL", marginx: 40, marginy: 92, nodesep: 40, edgesep: 20, ranksep: 120 });
    g.setDefaultEdgeLabel(() => ({}));
    nodes.forEach(node => {
      const size = getGraphNodeSize(node);
      g.setNode(node.id, { width: size.width, height: size.height });
    });
    state.graph.edges.forEach(edge => {
      if (edge.source && edge.target) {
        g.setEdge(edge.source, edge.target);
      }
    });
    dagre.layout(g);
    nodes.forEach(node => {
      const p = g.node(node.id);
      if (p) {
        node.position.x = p.x - p.width / 2;
        node.position.y = p.y - p.height / 2;
      }
    });
    return;
  }

  const columns = getLaneColumns(nodes);
  if (!columns.length) return;
  const columnWidths = new Map(columns.map((column) => [
    column.id,
    column.nodes.length ? Math.max(...column.nodes.map((node) => getGraphNodeSize(node).width)) : GRAPH_CARD_BASE.width,
  ]));
  let cursorX = 36;
  let previousKind = "";
  columns.forEach((column) => {
    const intraLaneGap = previousKind && previousKind === column.kind ? 28 : 0;
    cursorX += intraLaneGap;
    column.x = cursorX;
    cursorX += columnWidths.get(column.id) + GRAPH_LAYOUT_GAP_X;
    previousKind = column.kind;
  });
  const rowCount = Math.max(0, ...columns.map((column) => column.nodes.length));
  const rowHeights = new Array(rowCount).fill(GRAPH_CARD_BASE.height);
  for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    columns.forEach((column) => {
      const node = column.nodes[rowIndex];
      if (!node) return;
      rowHeights[rowIndex] = Math.max(rowHeights[rowIndex], getGraphNodeSize(node).height);
    });
  }
  let cursorY = 92;
  for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    columns.forEach((column) => {
      const node = column.nodes[rowIndex];
      if (!node) return;
      node.position.x = column.x;
      node.position.y = cursorY;
    });
    cursorY += rowHeights[rowIndex] + GRAPH_LAYOUT_GAP_Y;
  }
}

function getNodePortPosition(node, side) {
  const size = getGraphNodeSize(node);
  return {
    x: side === "outbound" ? node.position.x + size.width : node.position.x,
    y: node.position.y + Math.min(size.height / 2, 70),
  };
}

function buildOffsetRoutedPath(route, offset = 0) {
  if (!route.points?.length) {
    return route.path || "";
  }
  const shifted = route.points.map((point, index, points) => {
    const previous = points[index - 1] || point;
    const next = points[index + 1] || point;
    const previousNormal = getSegmentNormal(previous, point, offset);
    const nextNormal = getSegmentNormal(point, next, offset);
    return {
      x: point.x + ((previousNormal.x + nextNormal.x) || previousNormal.x || nextNormal.x || 0),
      y: point.y + ((previousNormal.y + nextNormal.y) || previousNormal.y || nextNormal.y || 0),
    };
  });
  return buildPolylinePath(shifted);
}

function getSegmentNormal(from, to, offset) {
  if (!offset || !from || !to) {
    return { x: 0, y: 0 };
  }
  if (Math.abs(from.x - to.x) > Math.abs(from.y - to.y)) {
    return { x: 0, y: offset };
  }
  if (Math.abs(from.y - to.y) > Math.abs(from.x - to.x)) {
    return { x: offset, y: 0 };
  }
  return { x: 0, y: 0 };
}

function getEdgeBaseColor(edge) {
  const seed = `${edge.source}:${edge.target}:${edge.type}`;
  const palette = ["#7c5a0a", "#0d6b63", "#9a3412", "#1d4ed8", "#7e22ce", "#b91c1c", "#047857", "#9f1239"];
  const total = Array.from(seed).reduce((sum, char) => sum + char.charCodeAt(0), 0);
  return palette[total % palette.length];
}

function getEdgeRouteOffset(edge) {
  const seed = Array.from(edge.id || `${edge.source}:${edge.target}`).reduce((sum, char) => sum + char.charCodeAt(0), 0);
  return (seed % 3) * 10;
}

function getNodeBounds(node) {
  const size = getGraphNodeSize(node);
  return {
    left: node.position.x,
    right: node.position.x + size.width,
    top: node.position.y,
    bottom: node.position.y + size.height,
    width: size.width,
    height: size.height,
  };
}

function getRoutedEdgeGeometry(source, target, routingContext = null) {
  const context = routingContext || buildRoutingContext(getViewGraph().nodes);
  const start = getNodePortPosition(source, "outbound");
  const end = getNodePortPosition(target, "inbound");
  const sourceBounds = getNodeBounds(source);
  const targetBounds = getNodeBounds(target);
  const clearance = 22;
  const startExit = { x: sourceBounds.right + clearance, y: start.y };
  const endEntry = { x: targetBounds.left - clearance, y: end.y };
  const closeColumnObstacles = getRoutingObstacles(context.nodes, source.id, target.id, 32);
  if ((targetBounds.left - sourceBounds.right) < 120) {
    const detourPoints = buildSideDetourRoute(start, end, startExit, endEntry, sourceBounds, targetBounds, closeColumnObstacles, context.dimensions);
    if (detourPoints && validateOrthogonalRoute(detourPoints, closeColumnObstacles)) {
      const labelPoint = getPolylineLabelPoint(detourPoints);
      return {
        start,
        end,
        points: detourPoints,
        path: buildPolylinePath(detourPoints),
        labelX: labelPoint.x,
        labelY: labelPoint.y - 8,
      };
    }
  }
  const paddings = [36, 58, 86];
  for (const padding of paddings) {
    const obstacles = getRoutingObstacles(context.nodes, source.id, target.id, padding);
    const routePoints = findObstacleAwareRoute(startExit, endEntry, obstacles, context.dimensions);
    const points = routePoints
      ? compressOrthogonalPoints([start, startExit, ...routePoints, endEntry, end])
      : null;
    if (points && validateOrthogonalRoute(points, obstacles)) {
      const labelPoint = getPolylineLabelPoint(points);
      return {
        start,
        end,
        points,
        path: buildPolylinePath(points),
        labelX: labelPoint.x,
        labelY: labelPoint.y - 8,
      };
    }
  }

  const expandedObstacles = getRoutingObstacles(context.nodes, source.id, target.id, 68);
  const fallbackRoute = findFallbackOrthogonalRoute(start, startExit, endEntry, end, expandedObstacles, context.dimensions);
  if (fallbackRoute && validateOrthogonalRoute(fallbackRoute, expandedObstacles)) {
    const labelPoint = getPolylineLabelPoint(fallbackRoute);
    return {
      start,
      end,
      points: fallbackRoute,
      path: buildPolylinePath(fallbackRoute),
      labelX: labelPoint.x,
      labelY: labelPoint.y - 8,
    };
  }

  const legacy = getLegacyRoutedEdgeGeometry(source, target);
  if (validateOrthogonalRoute(legacy.points || [], expandedObstacles)) {
    return legacy;
  }

  const topFallback = buildBoundaryRoute(start, end, expandedObstacles, "top");
  const bottomFallback = buildBoundaryRoute(start, end, expandedObstacles, "bottom");
  const lastResort = compressOrthogonalPoints([
    start,
    { x: start.x, y: 12 },
    { x: end.x, y: 12 },
    end,
  ]);
  const safePoints = topFallback || bottomFallback || lastResort;
  const labelPoint = getPolylineLabelPoint(safePoints);
  return {
    start,
    end,
    points: safePoints,
    path: buildPolylinePath(safePoints),
    labelX: labelPoint.x,
    labelY: labelPoint.y - 8,
  };
}

function buildSideDetourRoute(start, end, startExit, endEntry, sourceBounds, targetBounds, obstacles, dimensions) {
  const rightDetourX = Math.min(
    Math.max(sourceBounds.right, targetBounds.right, ...obstacles.map((obstacle) => obstacle.right), 0) + 28,
    Math.max(40, dimensions.width - 28),
  );
  const rightRoute = compressOrthogonalPoints([
    start,
    startExit,
    { x: rightDetourX, y: start.y },
    { x: rightDetourX, y: end.y },
    endEntry,
    end,
  ]);
  return validateOrthogonalRoute(rightRoute, obstacles) ? rightRoute : null;
}

function getLegacyRoutedEdgeGeometry(source, target) {
  const start = getNodePortPosition(source, "outbound");
  const end = getNodePortPosition(target, "inbound");
  const sourceBounds = getNodeBounds(source);
  const targetBounds = getNodeBounds(target);
  const routeOffset = getEdgeRouteOffset({ id: `${source.id}:${target.id}`, source: source.id, target: target.id });
  const forwardFlow = start.x <= end.x;
  const sourceClearX = start.x + 34;
  const targetClearX = end.x - 34;
  let routeY;
  let middleX;
  let points;

  if (forwardFlow) {
    if (Math.abs(start.y - end.y) < 18) {
      routeY = Math.max(18, Math.min(sourceBounds.top, targetBounds.top) - 28 - routeOffset);
    } else if (start.y < end.y) {
      const gap = targetBounds.top - sourceBounds.bottom;
      routeY = gap > 18 ? sourceBounds.bottom + gap / 2 : Math.max(18, Math.min(sourceBounds.top, targetBounds.top) - 28 - routeOffset);
    } else {
      const gap = sourceBounds.top - targetBounds.bottom;
      routeY = gap > 18 ? targetBounds.bottom + gap / 2 : Math.max(18, Math.min(sourceBounds.top, targetBounds.top) - 28 - routeOffset);
    }
    middleX = (sourceClearX + targetClearX) / 2;
    points = [
      start,
      { x: sourceClearX, y: start.y },
      { x: sourceClearX, y: routeY },
      { x: targetClearX, y: routeY },
      { x: targetClearX, y: end.y },
      end,
    ];
  } else {
    const loopX = Math.max(sourceBounds.right, targetBounds.right) + 52 + routeOffset;
    routeY = Math.max(18, Math.min(sourceBounds.top, targetBounds.top) - 30 - routeOffset);
    middleX = loopX;
    points = [
      start,
      { x: loopX, y: start.y },
      { x: loopX, y: routeY },
      { x: targetClearX, y: routeY },
      { x: targetClearX, y: end.y },
      end,
    ];
  }
  return {
    start,
    end,
    points,
    path: buildPolylinePath(points),
    labelX: middleX,
    labelY: routeY - 6,
  };
}

function buildRoutingContext(nodes) {
  return {
    nodes,
    dimensions: getCanvasDimensions(nodes),
  };
}

function getRoutingObstacles(nodes, sourceId, targetId, padding = 16) {
  return nodes
    .filter((node) => node.id !== sourceId && node.id !== targetId)
    .map((node) => inflateBounds(getNodeBounds(node), padding));
}

function inflateBounds(bounds, padding) {
  return {
    left: bounds.left - padding,
    right: bounds.right + padding,
    top: bounds.top - padding,
    bottom: bounds.bottom + padding,
  };
}

function findObstacleAwareRoute(start, end, obstacles, dimensions) {
  const xValues = new Set([24, Math.round(start.x), Math.round(end.x), Math.max(24, Math.round(dimensions.width - 24))]);
  const yValues = new Set([24, Math.round(start.y), Math.round(end.y), Math.max(24, Math.round(dimensions.height - 24))]);
  obstacles.forEach((obstacle) => {
    xValues.add(Math.round(obstacle.left));
    xValues.add(Math.round(obstacle.right));
    yValues.add(Math.round(obstacle.top));
    yValues.add(Math.round(obstacle.bottom));
  });

  const points = [];
  [...xValues].forEach((x) => {
    [...yValues].forEach((y) => {
      const point = { x, y };
      if (!isPointBlocked(point, obstacles, start, end)) {
        points.push(point);
      }
    });
  });
  points.push(start, end);

  const uniquePoints = dedupePoints(points);
  const graph = buildRoutingGraph(uniquePoints, obstacles);
  const path = shortestOrthogonalPath(graph, start, end);
  return path && path.length ? path.slice(1, -1) : null;
}

function findFallbackOrthogonalRoute(start, startExit, endEntry, end, obstacles, dimensions) {
  const yCandidates = dedupeNumberList([
    28,
    Math.max(28, Math.min(start.y, end.y) - 34),
    Math.min(dimensions.height - 28, Math.max(start.y, end.y) + 34),
    ...obstacles.flatMap((obstacle) => [Math.max(28, obstacle.top - 18), Math.min(dimensions.height - 28, obstacle.bottom + 18)]),
  ]);
  const xCandidates = dedupeNumberList([
    28,
    Math.max(28, Math.min(start.x, end.x) - 34),
    Math.min(dimensions.width - 28, Math.max(start.x, end.x) + 34),
    ...obstacles.flatMap((obstacle) => [Math.max(28, obstacle.left - 18), Math.min(dimensions.width - 28, obstacle.right + 18)]),
  ]);

  const candidates = [];
  yCandidates.forEach((y) => {
    candidates.push([start, { x: startExit.x, y: start.y }, { x: startExit.x, y }, { x: endEntry.x, y }, { x: endEntry.x, y: end.y }, end]);
  });
  xCandidates.forEach((x) => {
    candidates.push([start, { x, y: start.y }, { x, y: end.y }, end]);
    candidates.push([start, { x: startExit.x, y: start.y }, { x, y: start.y }, { x, y: end.y }, { x: endEntry.x, y: end.y }, end]);
  });

  const valid = candidates
    .map((points) => compressOrthogonalPoints(points))
    .filter((points) => validateOrthogonalRoute(points, obstacles));
  if (!valid.length) {
    return null;
  }
  valid.sort((left, right) => getRouteLength(left) - getRouteLength(right));
  return valid[0];
}

function buildBoundaryRoute(start, end, obstacles, direction) {
  const boundaryY = direction === "bottom"
    ? Math.max(start.y, end.y, ...obstacles.map((obstacle) => obstacle.bottom)) + 22
    : Math.max(18, Math.min(start.y, end.y, ...obstacles.map((obstacle) => obstacle.top)) - 22);
  const points = compressOrthogonalPoints([
    start,
    { x: start.x, y: boundaryY },
    { x: end.x, y: boundaryY },
    end,
  ]);
  return validateOrthogonalRoute(points, obstacles) ? points : null;
}

function dedupeNumberList(values) {
  return [...new Set(values.map((value) => Math.round(value)))].sort((left, right) => left - right);
}

function getRouteLength(points) {
  let total = 0;
  for (let index = 0; index < points.length - 1; index += 1) {
    total += Math.abs(points[index + 1].x - points[index].x) + Math.abs(points[index + 1].y - points[index].y);
  }
  return total;
}

function isPointBlocked(point, obstacles, start, end) {
  if ((point.x === start.x && point.y === start.y) || (point.x === end.x && point.y === end.y)) {
    return false;
  }
  return obstacles.some((obstacle) => (
    point.x >= obstacle.left && point.x <= obstacle.right
      && point.y >= obstacle.top && point.y <= obstacle.bottom
  ));
}

function dedupePoints(points) {
  const seen = new Set();
  return points.filter((point) => {
    const key = `${Math.round(point.x)}:${Math.round(point.y)}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function buildRoutingGraph(points, obstacles) {
  const graph = new Map(points.map((point) => [pointKey(point), []]));
  const byX = new Map();
  const byY = new Map();
  points.forEach((point) => {
    const roundedX = Math.round(point.x);
    const roundedY = Math.round(point.y);
    if (!byX.has(roundedX)) byX.set(roundedX, []);
    if (!byY.has(roundedY)) byY.set(roundedY, []);
    byX.get(roundedX).push(point);
    byY.get(roundedY).push(point);
  });

  byX.forEach((columnPoints) => {
    const sorted = [...columnPoints].sort((left, right) => left.y - right.y);
    for (let index = 0; index < sorted.length - 1; index += 1) {
      linkRoutingNeighbors(graph, sorted[index], sorted[index + 1], obstacles);
    }
  });

  byY.forEach((rowPoints) => {
    const sorted = [...rowPoints].sort((left, right) => left.x - right.x);
    for (let index = 0; index < sorted.length - 1; index += 1) {
      linkRoutingNeighbors(graph, sorted[index], sorted[index + 1], obstacles);
    }
  });

  return graph;
}

function linkRoutingNeighbors(graph, left, right, obstacles) {
  if (!isSegmentClear(left, right, obstacles)) {
    return;
  }
  const cost = Math.abs(left.x - right.x) + Math.abs(left.y - right.y);
  graph.get(pointKey(left)).push({ point: right, cost });
  graph.get(pointKey(right)).push({ point: left, cost });
}

function isSegmentClear(start, end, obstacles) {
  return obstacles.every((obstacle) => !segmentHitsObstacle(start, end, obstacle));
}

function segmentHitsObstacle(start, end, obstacle) {
  if (Math.abs(start.x - end.x) < 0.5) {
    const x = start.x;
    const top = Math.min(start.y, end.y);
    const bottom = Math.max(start.y, end.y);
    return x >= obstacle.left && x <= obstacle.right
      && bottom >= obstacle.top && top <= obstacle.bottom;
  }
  if (Math.abs(start.y - end.y) < 0.5) {
    const y = start.y;
    const left = Math.min(start.x, end.x);
    const right = Math.max(start.x, end.x);
    return y >= obstacle.top && y <= obstacle.bottom
      && right >= obstacle.left && left <= obstacle.right;
  }
  return true;
}

function shortestOrthogonalPath(graph, start, end) {
  const startKey = pointKey(start);
  const endKey = pointKey(end);
  const frontier = [{ key: startKey, point: start, cost: 0, priority: 0, direction: "" }];
  const best = new Map([[`${startKey}:`, 0]]);
  const previous = new Map();

  while (frontier.length) {
    frontier.sort((left, right) => left.priority - right.priority);
    const current = frontier.shift();
    if (current.key === endKey) {
      return reconstructRoutingPath(previous, current.key, current.direction, end);
    }
    const neighbors = graph.get(current.key) || [];
    neighbors.forEach(({ point, cost }) => {
      const direction = Math.abs(point.x - current.point.x) > Math.abs(point.y - current.point.y) ? "h" : "v";
      const turnPenalty = current.direction && current.direction !== direction ? 12 : 0;
      const nextCost = current.cost + cost + turnPenalty;
      const stateKey = `${pointKey(point)}:${direction}`;
      if (best.has(stateKey) && best.get(stateKey) <= nextCost) {
        return;
      }
      best.set(stateKey, nextCost);
      previous.set(stateKey, { key: current.key, direction: current.direction, point: current.point });
      frontier.push({
        key: pointKey(point),
        point,
        cost: nextCost,
        direction,
        priority: nextCost + Math.abs(end.x - point.x) + Math.abs(end.y - point.y),
      });
    });
  }
  return null;
}

function reconstructRoutingPath(previous, endKey, endDirection, endPoint) {
  const points = [endPoint];
  let cursorKey = `${endKey}:${endDirection}`;
  while (previous.has(cursorKey)) {
    const step = previous.get(cursorKey);
    points.push(step.point);
    cursorKey = `${step.key}:${step.direction}`;
  }
  return points.reverse();
}

function pointKey(point) {
  return `${Math.round(point.x)}:${Math.round(point.y)}`;
}

function compressOrthogonalPoints(points) {
  if (!points.length) {
    return [];
  }
  const compressed = [points[0]];
  for (let index = 1; index < points.length - 1; index += 1) {
    const previous = compressed[compressed.length - 1];
    const current = points[index];
    const next = points[index + 1];
    const sameX = Math.abs(previous.x - current.x) < 0.5 && Math.abs(current.x - next.x) < 0.5;
    const sameY = Math.abs(previous.y - current.y) < 0.5 && Math.abs(current.y - next.y) < 0.5;
    if (!sameX && !sameY) {
      compressed.push(current);
    }
  }
  compressed.push(points[points.length - 1]);
  return compressed;
}

function buildPolylinePath(points) {
  if (!points.length) {
    return "";
  }
  return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
}

function validateOrthogonalRoute(points, obstacles) {
  if (!points.length) {
    return false;
  }
  for (let index = 0; index < points.length - 1; index += 1) {
    const start = points[index];
    const end = points[index + 1];
    const orthogonal = Math.abs(start.x - end.x) < 0.5 || Math.abs(start.y - end.y) < 0.5;
    if (!orthogonal || !isSegmentClear(start, end, obstacles)) {
      return false;
    }
  }
  return true;
}

function getPolylineLabelPoint(points) {
  if (points.length < 2) {
    return points[0] || { x: 0, y: 0 };
  }
  const segmentIndex = Math.max(0, Math.floor((points.length - 2) / 2));
  const start = points[segmentIndex];
  const end = points[segmentIndex + 1];
  return {
    x: (start.x + end.x) / 2,
    y: (start.y + end.y) / 2,
  };
}

function summarizeEdgeMappings(edge) {
  const mappings = edge.column_mappings || [];
  if (!mappings.length) {
    return "";
  }
  const previews = mappings.slice(0, 2).map((mapping) => `${mapping.source_column} -> ${mapping.target_column}`);
  return `${previews.join(" | ")}${mappings.length > 2 ? ` +${mappings.length - 2}` : ""}`;
}

function matchesSearch(node) {
  if (!node) return false;
  const haystack = [
    node.id,
    node.label,
    node.description,
    node.owner,
    ...(node.tags || []),
  ].join(" ").toLowerCase();
  return haystack.includes(state.searchTerm);
}

function getCanvasDimensions(nodes) {
  const maxX = nodes.length ? Math.max(...nodes.map((node) => node.position.x + getGraphNodeSize(node).width)) : 0;
  const maxY = nodes.length ? Math.max(...nodes.map((node) => node.position.y + getGraphNodeSize(node).height)) : 0;
  return {
    width: Math.max(960, maxX + 220),
    height: Math.max(640, maxY + 180),
  };
}

function centerSelection() {
  if (state.selectionMode === "edge" && state.selectedEdgeId) {
    centerEdge(state.selectedEdgeId);
    return;
  }
  if (state.selectedNodeId) {
    centerNode(state.selectedNodeId);
  }
}

function centerNode(nodeId) {
  const node = getNodeById(nodeId);
  if (!node) return;
  const size = getGraphNodeSize(node);
  graphScroller.scrollTo({
    left: Math.max(0, (node.position.x + size.width / 2) * state.zoom - graphScroller.clientWidth / 2),
    top: Math.max(0, (node.position.y + size.height / 2) * state.zoom - graphScroller.clientHeight / 2),
    behavior: "smooth",
  });
}

function centerEdge(edgeId) {
  const edge = getEdgeById(edgeId);
  if (!edge) return;
  const source = getNodeById(edge.source);
  const target = getNodeById(edge.target);
  if (!source || !target) return;
  const routed = getRoutedEdgeGeometry(source, target, buildRoutingContext(getViewGraph().nodes));
  graphScroller.scrollTo({
    left: Math.max(0, routed.labelX * state.zoom - graphScroller.clientWidth / 2),
    top: Math.max(0, routed.labelY * state.zoom - graphScroller.clientHeight / 2),
    behavior: "smooth",
  });
}

function getCurrentSelection() {
  if (!state.graph) return null;
  if (state.selectionMode === "edge" && state.selectedEdgeId) {
    const edge = getEdgeById(state.selectedEdgeId);
    if (edge) return { type: "edge", edge };
  }
  if (state.selectedNodeId) {
    const node = getNodeById(state.selectedNodeId);
    if (node) return { type: "node", node };
  }
  return null;
}

function getNodeById(nodeId) {
  return state.graph?.nodes.find((node) => node.id === nodeId) || null;
}

function getEdgeById(edgeId) {
  return state.graph?.edges.find((edge) => edge.id === edgeId) || null;
}

function setStatus(title, subtitle) {
  statusText.textContent = title;
  substatusText.textContent = subtitle;
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

function renderProjectProfile() {
  if (!projectProfileSummary) {
    return;
  }
  const profile = state.projectProfile;
  const summary = profile?.summary || {};
  const dataAssets = profile?.data_assets || [];
  const apiHints = profile?.api_contract_hints || [];
  const uiHints = profile?.ui_contract_hints || [];
  const sqlHints = profile?.sql_structure_hints || [];
  const ormHints = profile?.orm_structure_hints || [];
  const selectedPaths = new Set(state.selectedProjectImports || []);
  const selectedApiHints = new Set(state.selectedProjectApiHints || []);
  const selectedUiHints = new Set(state.selectedProjectUiHints || []);
  const selectableCount = dataAssets.filter((asset) => asset.suggested_import).length;
  const bootstrapCount = (
    (state.projectBootstrapOptions.assets ? selectableCount : 0)
    + (state.projectBootstrapOptions.apiHints ? apiHints.length : 0)
    + (state.projectBootstrapOptions.uiHints ? uiHints.length : 0)
  );
  const bootstrapSummary = describeBootstrapScope({
    selectableCount,
    selectedAssetCount: selectedPaths.size,
    apiHintCount: apiHints.length,
    selectedApiHintCount: selectedApiHints.size,
    uiHintCount: uiHints.length,
    selectedUiHintCount: selectedUiHints.size,
  });
  projectProfileSummary.innerHTML = `
    ${renderProjectWizardNav()}
    ${state.projectWizardStep === 1 ? renderProjectWizardScopeStep(profile, summary, bootstrapSummary, bootstrapCount) : ""}
    ${state.projectWizardStep === 2 ? renderProjectWizardAssetsStep(dataAssets, selectedPaths, selectableCount) : ""}
    ${state.projectWizardStep === 3 ? renderProjectWizardContractsStep(apiHints, uiHints, sqlHints, ormHints, selectedApiHints, selectedUiHints) : ""}
    ${state.projectWizardStep === 4 ? renderProjectWizardReviewStep(profile, summary, bootstrapSummary, bootstrapCount, sqlHints, ormHints) : ""}
    ${renderProjectWizardFooter(Boolean(profile))}
  `;
}

function renderProjectWizardNav() {
  const steps = [
    { index: 1, label: "Scope" },
    { index: 2, label: "Assets" },
    { index: 3, label: "Contracts" },
    { index: 4, label: "Review" },
  ];
  return `
    <div class="wizard-nav">
      ${steps.map((step) => `
        <button
          class="wizard-step ${state.projectWizardStep === step.index ? "active" : ""}"
          type="button"
          data-project-wizard-step="${step.index}"
        >
          <span class="wizard-step-index">${step.index}</span>
          <span>${escapeHtml(step.label)}</span>
        </button>
      `).join("")}
    </div>
  `;
}

function renderProjectWizardScopeStep(profile, summary, bootstrapSummary, bootstrapCount) {
  const presets = state.onboardingPresets || [];
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Step 1. Discovery Scope</h3>
        <button class="ghost-button" type="button" data-project-rescan="true">${profile ? "Rescan project" : "Run discovery"}</button>
      </div>
      <div class="form-grid">
        <label class="form-field form-field-full">
          Project root
          <input data-project-profile-root="true" value="${escapeHtml(state.projectProfileOptions.rootPath || "")}" placeholder="${escapeHtml(state.projectProfile?.root || "") || "/path/to/project"}" />
        </label>
        <div class="form-field">
          Browse
          <button class="ghost-button" type="button" data-project-root-picker="true">Search directories</button>
        </div>
      </div>
      ${state.projectProfileOptions.rootPath ? `<p class="hint">Current root: ${escapeHtml(state.projectProfileOptions.rootPath)}</p>` : ""}
      <div class="chip-row">
        <label class="hint"><input type="checkbox" data-project-profile-option="includeTests" ${state.projectProfileOptions.includeTests ? "checked" : ""} /> include tests</label>
        <label class="hint"><input type="checkbox" data-project-profile-option="includeInternal" ${state.projectProfileOptions.includeInternal ? "checked" : ""} /> include workbench internals</label>
      </div>
      <div class="chip-row">
        <label class="hint"><input type="checkbox" data-project-bootstrap-option="assets" ${state.projectBootstrapOptions.assets ? "checked" : ""} /> bootstrap assets</label>
        <label class="hint"><input type="checkbox" data-project-bootstrap-option="apiHints" ${state.projectBootstrapOptions.apiHints ? "checked" : ""} /> bootstrap API hints</label>
        <label class="hint"><input type="checkbox" data-project-bootstrap-option="uiHints" ${state.projectBootstrapOptions.uiHints ? "checked" : ""} /> bootstrap UI hints</label>
      </div>
      <p class="hint">${escapeHtml(bootstrapSummary)}</p>
      ${profile ? `
        <div class="meta-grid">
          <div><strong>Manifests</strong><br>${formatValue(summary.manifests)}</div>
          <div><strong>Code files</strong><br>${formatValue(summary.code_files)}</div>
          <div><strong>Docs</strong><br>${formatValue(summary.docs)}</div>
          <div><strong>SQL hints</strong><br>${formatValue(summary.sql_structure_hints || 0)}</div>
          <div><strong>ORM hints</strong><br>${formatValue(summary.orm_structure_hints || 0)}</div>
          <div><strong>Importable items</strong><br>${formatValue(bootstrapCount)}</div>
        </div>
        <div class="section">
          <h3>Project root</h3>
          <p>${escapeHtml(profile.root || "unknown")}</p>
        </div>
      ` : "<p class=\"hint\">Run discovery to inspect manifests, code, docs, and accessible data assets before stepping through onboarding.</p>"}
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Saved Presets</h3>
        <div class="row-actions">
          <button class="ghost-button" type="button" data-project-preset-apply="true" ${state.selectedProjectPresetId ? "" : "disabled"}>Apply preset</button>
          <button class="ghost-button" type="button" data-project-preset-delete="true" ${state.selectedProjectPresetId ? "" : "disabled"}>Delete preset</button>
        </div>
      </div>
      <div class="form-grid">
        <label class="form-field form-field-full">
          Choose preset
          <select data-project-preset-select="true">
            <option value="">No preset selected</option>
            ${presets.map((preset) => `
              <option value="${escapeHtml(preset.id)}" ${state.selectedProjectPresetId === preset.id ? "selected" : ""}>${escapeHtml(preset.name)}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Preset name
          <input data-project-preset-field="name" value="${escapeHtml(state.projectPresetDraft.name || "")}" placeholder="NYC housing onboarding" />
        </label>
        <label class="form-field form-field-full">
          Description
          <input data-project-preset-field="description" value="${escapeHtml(state.projectPresetDraft.description || "")}" placeholder="Optional notes about survey scope and selected imports" />
        </label>
      </div>
      <div class="section-actions">
        <button class="ghost-button" type="button" data-project-preset-save="true">Save current selections as preset</button>
      </div>
    </div>
  `;
}

function renderProjectWizardAssetsStep(dataAssets, selectedPaths, selectableCount) {
  const PAGE_SIZE = 50;
  state.wizardAssetsPage = state.wizardAssetsPage || 1;
  const totalPages = Math.max(1, Math.ceil(dataAssets.length / PAGE_SIZE));
  const page = Math.min(Math.max(1, state.wizardAssetsPage), totalPages);
  const offset = (page - 1) * PAGE_SIZE;
  const pagedAssets = dataAssets.slice(offset, offset + PAGE_SIZE);
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Step 2. Data Assets <span class="hint">(${dataAssets.length} total)</span></h3>
        <div class="row-actions">
          <span class="hint">${formatValue(selectedPaths.size)} selected / ${formatValue(selectableCount)} suggested</span>
          <button class="ghost-button" type="button" data-project-select-all="true">Select all suggested</button>
          <button class="ghost-button" type="button" data-project-clear-selection="true">Clear selection</button>
          <button class="ghost-button" type="button" data-project-import-selected="true" ${selectedPaths.size ? "" : "disabled"}>Import selected now</button>
        </div>
      </div>
      <div class="column-list">
        ${pagedAssets.length ? pagedAssets.map((asset) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(asset.path || "unknown")}</div>
              <div class="row-actions">
                ${asset.suggested_import ? `<label class="hint"><input type="checkbox" data-project-select="${escapeHtml(asset.path)}" ${selectedPaths.has(asset.path) ? "checked" : ""} /> select</label>` : ""}
                <span class="pill">${escapeHtml(asset.format || "unknown")}</span>
                <span class="pill">${escapeHtml(asset.profile_status || "schema_only")}</span>
                ${asset.suggested_import ? `<button class="text-button" type="button" data-project-import-load="${escapeHtml(asset.path)}">Load to import</button>` : ""}
                ${asset.suggested_import ? `<button class="ghost-button" type="button" data-project-import-run="${escapeHtml(asset.path)}">Import now</button>` : ""}
              </div>
            </div>
            <div class="column-meta">
              rows: ${formatValue(asset.row_count)} | columns: ${asset.columns?.map((column) => `${column.name} (${column.data_type})`).join(", ") || "missing"}
            </div>
            ${asset.suggested_import ? `<div class="column-meta">suggested: ${escapeHtml(asset.suggested_import.source_label)} -> ${escapeHtml(asset.suggested_import.data_label)}</div>` : ""}
          </div>
        `).join("") : "<p>No data assets were detected. Run discovery again or widen the scope filters.</p>"}
      </div>
      ${totalPages > 1 ? `
      <div class="section-actions" style="margin-top: 14px; justify-content: center;">
        <button class="ghost-button" type="button" data-assets-page="prev" ${page <= 1 ? "disabled" : ""}>Prev Page</button>
        <span class="hint" style="margin: 0 16px;">Page ${page} of ${totalPages}</span>
        <button class="ghost-button" type="button" data-assets-page="next" ${page >= totalPages ? "disabled" : ""}>Next Page</button>
      </div>
      ` : ""}
    </div>
  `;
}

function renderProjectWizardContractsStep(apiHints, uiHints, sqlHints, ormHints, selectedApiHints, selectedUiHints) {
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Step 3. API Contracts</h3>
        <div class="row-actions">
          <span class="hint">${formatValue(selectedApiHints.size)} selected / ${formatValue(apiHints.length)}</span>
          <button class="ghost-button" type="button" data-project-api-select-all="true">Select all</button>
          <button class="ghost-button" type="button" data-project-api-clear="true">Clear</button>
        </div>
      </div>
      <div class="column-list">
        ${apiHints.length ? apiHints.map((hint) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(hint.route || hint.label || "unknown route")}</div>
              <div class="row-actions">
                <label class="hint"><input type="checkbox" data-project-api-select="${escapeHtml(hint.id)}" ${selectedApiHints.has(hint.id) ? "checked" : ""} /> select</label>
                <button class="ghost-button" type="button" data-project-api-create="${escapeHtml(hint.id)}">Create or update</button>
              </div>
            </div>
            <div class="column-meta">${escapeHtml(hint.file || "unknown file")} | ${escapeHtml(hint.detected_from || "code scan")}</div>
            <div class="column-meta">${renderHintFieldSummary(hint.response_fields || [])}</div>
          </div>
        `).join("") : "<p>No API contract hints detected.</p>"}
      </div>
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Step 3. UI Consumers</h3>
        <div class="row-actions">
          <span class="hint">${formatValue(selectedUiHints.size)} selected / ${formatValue(uiHints.length)}</span>
          <button class="ghost-button" type="button" data-project-ui-select-all="true">Select all</button>
          <button class="ghost-button" type="button" data-project-ui-clear="true">Clear</button>
        </div>
      </div>
      <div class="column-list">
        ${uiHints.length ? uiHints.map((hint) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(hint.component || hint.label || "unknown component")}</div>
              <div class="row-actions">
                <label class="hint"><input type="checkbox" data-project-ui-select="${escapeHtml(hint.id)}" ${selectedUiHints.has(hint.id) ? "checked" : ""} /> select</label>
                <button class="ghost-button" type="button" data-project-ui-create="${escapeHtml(hint.id)}">Create or update</button>
              </div>
            </div>
            <div class="column-meta">${escapeHtml(hint.file || "unknown file")} | routes: ${escapeHtml((hint.api_routes || []).join(", ") || "none detected")}</div>
            <div class="column-meta">${renderHintFieldSummary(hint.used_fields || [])}</div>
          </div>
        `).join("") : "<p>No UI consumer hints detected.</p>"}
      </div>
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Step 3. SQL Structure</h3>
        <span class="hint">${formatValue(sqlHints.length)} inferred relation${sqlHints.length === 1 ? "" : "s"}</span>
      </div>
      <div class="column-list">
        ${sqlHints.length ? sqlHints.map((hint) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(hint.relation || hint.label || "unknown relation")}</div>
              <div class="row-actions">
                <span class="pill">${escapeHtml(hint.object_type || "table")}</span>
              </div>
            </div>
            <div class="column-meta">${escapeHtml(hint.file || "unknown file")} | ${escapeHtml(hint.detected_from || "sql scan")}</div>
            <div class="column-meta">${renderHintFieldSummary((hint.fields || []).map((field) => field.name))}</div>
            ${hint.upstream_relations?.length ? `<div class="column-meta">upstream: ${escapeHtml(hint.upstream_relations.join(", "))}</div>` : ""}
          </div>
        `).join("") : "<p>No SQL structure hints detected.</p>"}
      </div>
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Step 3. ORM Structure</h3>
        <span class="hint">${formatValue(ormHints.length)} inferred model${ormHints.length === 1 ? "" : "s"}</span>
      </div>
      <div class="column-list">
        ${ormHints.length ? ormHints.map((hint) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(hint.relation || hint.label || "unknown model")}</div>
              <div class="row-actions">
                <span class="pill">${escapeHtml(hint.object_type || "table")}</span>
              </div>
            </div>
            <div class="column-meta">${escapeHtml(hint.file || "unknown file")} | ${escapeHtml(hint.detected_from || "orm scan")}</div>
            <div class="column-meta">${renderHintFieldSummary((hint.fields || []).map((field) => field.name))}</div>
            ${hint.upstream_relations?.length ? `<div class="column-meta">upstream: ${escapeHtml(hint.upstream_relations.join(", "))}</div>` : ""}
          </div>
        `).join("") : "<p>No ORM structure hints detected.</p>"}
      </div>
    </div>
  `;
}

function renderProjectWizardReviewStep(profile, summary, bootstrapSummary, bootstrapCount, sqlHints, ormHints) {
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Step 4. Review and Bootstrap</h3>
        <button class="ghost-button" type="button" data-project-bootstrap="true" ${bootstrapCount ? "" : "disabled"}>Bootstrap graph</button>
      </div>
      ${profile ? `
        <div class="meta-grid">
          <div><strong>Data assets</strong><br>${formatValue(summary.data_assets)}</div>
          <div><strong>API hints</strong><br>${formatValue(summary.api_contract_hints)}</div>
          <div><strong>UI hints</strong><br>${formatValue(summary.ui_contract_hints)}</div>
          <div><strong>SQL hints</strong><br>${formatValue(summary.sql_structure_hints || 0)}</div>
          <div><strong>ORM hints</strong><br>${formatValue(summary.orm_structure_hints || 0)}</div>
          <div><strong>Importable items</strong><br>${formatValue(bootstrapCount)}</div>
        </div>
        <p class="hint">${escapeHtml(bootstrapSummary)}</p>
        <div class="section">
          <h3>Detected manifests</h3>
          <ul class="warning-list">${renderItems(profile.manifests || [])}</ul>
        </div>
        <div class="section">
          <h3>Code sample</h3>
          <ul class="warning-list">${renderItems(profile.code_files_sample || [])}</ul>
        </div>
        <div class="section">
          <h3>Docs sample</h3>
          <ul class="warning-list">${renderItems(profile.docs_sample || [])}</ul>
        </div>
        <div class="section">
          <h3>SQL structure sample</h3>
          <ul class="warning-list">${renderItems((sqlHints || []).slice(0, 8).map((hint) => `${hint.object_type || "table"}: ${hint.relation || hint.label || "unknown"}`))}</ul>
        </div>
        <div class="section">
          <h3>ORM structure sample</h3>
          <ul class="warning-list">${renderItems((ormHints || []).slice(0, 8).map((hint) => `${hint.object_type || "table"}: ${hint.relation || hint.label || "unknown"}`))}</ul>
        </div>
      ` : "<p class=\"hint\">Run discovery in Step 1 before reviewing and bootstrapping.</p>"}
    </div>
  `;
}

function renderProjectWizardFooter(hasProfile) {
  const canGoBack = state.projectWizardStep > 1;
  const canGoNext = state.projectWizardStep < 4;
  const nextDisabled = !canGoNext || (!hasProfile && state.projectWizardStep >= 1);
  return `
    <div class="section-actions wizard-actions">
      <button class="ghost-button" type="button" data-project-wizard-move="back" ${canGoBack ? "" : "disabled"}>Back</button>
      <button class="ghost-button" type="button" data-project-wizard-move="next" ${nextDisabled ? "disabled" : ""}>Next</button>
    </div>
  `;
}

function buildDefaultAuthoringState() {
  return {
    node: {
      kind: "data",
      extensionType: "table",
      label: "",
      description: "",
      owner: "",
      runtime: "",
      route: "",
      component: "",
      uiRole: "component",
      persistence: "hot",
      persisted: true,
      updateFrequency: "",
      referenceKind: "",
      referenceValue: "",
      sourceProvider: "",
      sourceRefresh: "",
      sourceSeriesId: "",
      dataDictionaryLabel: "",
      dataDictionaryKind: "link",
      dataDictionaryValue: "",
      rawAssetLabel: "",
      rawAssetKind: "file",
      rawAssetFormat: "unknown",
      rawAssetValue: "",
      schemaText: "",
    },
    edge: {
      source: "",
      target: "",
      type: "depends_on",
    },
    import: {
      sourceLabel: "",
      sourceExtensionType: "object",
      sourceDescription: "",
      sourceProvider: "",
      sourceRefresh: "",
      sourceOriginKind: "",
      sourceOriginValue: "",
      sourceSeriesId: "",
      rawAssetLabel: "",
      rawAssetKind: "file",
      rawAssetFormat: "unknown",
      rawAssetValue: "",
      profileReady: true,
      dataLabel: "",
      dataExtensionType: "raw_dataset",
      dataDescription: "",
      updateFrequency: "",
      persistence: "cold",
      persisted: false,
      schemaText: "",
    },
    openapi: {
      specPath: "",
      specText: "",
      owner: "",
    },
  };
}

function syncAuthoringState() {
  const nodeOptions = getExtensionOptions(state.authoring.node.kind);
  if (!nodeOptions.includes(state.authoring.node.extensionType)) {
    state.authoring.node.extensionType = nodeOptions[0];
  }

  const sourceOptions = getExtensionOptions("source");
  if (!sourceOptions.includes(state.authoring.import.sourceExtensionType)) {
    state.authoring.import.sourceExtensionType = sourceOptions[0];
  }

  const dataOptions = getExtensionOptions("data");
  if (!dataOptions.includes(state.authoring.import.dataExtensionType)) {
    state.authoring.import.dataExtensionType = dataOptions[0];
  }

  const nodeIds = (state.graph?.nodes || []).map((node) => node.id);
  if (!nodeIds.length) {
    state.authoring.edge.source = "";
    state.authoring.edge.target = "";
    return;
  }
  if (!nodeIds.includes(state.authoring.edge.source)) {
    state.authoring.edge.source = nodeIds[0];
  }
  if (!nodeIds.includes(state.authoring.edge.target) || !state.authoring.edge.target) {
    state.authoring.edge.target = nodeIds[Math.min(1, nodeIds.length - 1)];
  }
}

function renderAuthoringPanel() {
  if (!authoringPanel) {
    return;
  }
  if (!state.graph) {
    authoringPanel.innerHTML = "<p>Graph not loaded yet.</p>";
    return;
  }

  syncAuthoringState();
  const nodeDraft = state.authoring.node;
  const edgeDraft = state.authoring.edge;
  const importDraft = state.authoring.import;
  const nodeOptions = getExtensionOptions(nodeDraft.kind);
  const importSourceOptions = getExtensionOptions("source");
  const importDataOptions = getExtensionOptions("data");
  const showNodeSourceFields = nodeDraft.kind === "source";
  const showNodeDataFields = nodeDraft.kind === "data";
  const showNodeComputeFields = nodeDraft.kind === "compute";
  const showNodeContractFields = nodeDraft.kind === "contract";
  const graphNodes = [...state.graph.nodes].sort((left, right) => left.id.localeCompare(right.id));
  const deleteLabel = state.selectionMode === "edge" && state.selectedEdgeId
    ? `Delete edge ${state.selectedEdgeId}`
    : state.selectedNodeId
      ? `Delete node ${state.selectedNodeId}`
      : "Delete selection";

  authoringPanel.innerHTML = `
    <div class="section">
      <h3>Create object</h3>
      <p class="hint">Use this when you want to seed a node directly. For a source + data pair with profiling, use Import asset below.</p>
      <div class="form-grid">
        <label class="form-field">
          Kind
          <select data-authoring-path="node.kind">
            ${["source", "data", "compute", "contract"].map((kind) => `
              <option value="${kind}" ${nodeDraft.kind === kind ? "selected" : ""}>${kind}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Extension type
          <select data-authoring-path="node.extensionType">
            ${nodeOptions.map((value) => `
              <option value="${value}" ${nodeDraft.extensionType === value ? "selected" : ""}>${value}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Label
          <input data-authoring-path="node.label" value="${escapeHtml(nodeDraft.label)}" />
        </label>
        <label class="form-field">
          Description
          <input data-authoring-path="node.description" value="${escapeHtml(nodeDraft.description)}" />
        </label>
        <label class="form-field">
          Owner
          <input data-authoring-path="node.owner" value="${escapeHtml(nodeDraft.owner)}" />
        </label>
        ${showNodeComputeFields ? `
          <label class="form-field">
            Runtime
            <input data-authoring-path="node.runtime" value="${escapeHtml(nodeDraft.runtime)}" placeholder="polars, python..." />
          </label>
        ` : ""}
        ${showNodeContractFields ? `
          <label class="form-field">
            ${nodeDraft.extensionType === "api" ? "Route" : "Component"}
            <input data-authoring-path="node.${nodeDraft.extensionType === "api" ? "route" : "component"}" value="${escapeHtml(nodeDraft.extensionType === "api" ? nodeDraft.route : nodeDraft.component)}" />
          </label>
          ${nodeDraft.extensionType === "ui" ? `
            <label class="form-field">
              UI role
              <select data-authoring-path="node.uiRole">
                ${["screen", "container", "component"].map((value) => `<option value="${value}" ${nodeDraft.uiRole === value ? "selected" : ""}>${value}</option>`).join("")}
              </select>
            </label>
          ` : ""}
        ` : ""}
        ${(showNodeSourceFields || showNodeDataFields) ? `
          <label class="form-field">
            Reference kind
            <select data-authoring-path="node.referenceKind">
              ${["", "api_endpoint", "url", "disk_path", "bucket_path"].map((value) => `<option value="${value}" ${nodeDraft.referenceKind === value ? "selected" : ""}>${value || "optional"}</option>`).join("")}
            </select>
          </label>
          <label class="form-field form-field-full">
            Reference value
            <input data-authoring-path="node.referenceValue" value="${escapeHtml(nodeDraft.referenceValue)}" placeholder="https://..., data/raw/file.csv, s3://bucket/key" />
          </label>
        ` : ""}
        ${showNodeSourceFields ? `
          <label class="form-field">
            Provider
            <input data-authoring-path="node.sourceProvider" value="${escapeHtml(nodeDraft.sourceProvider)}" placeholder="FRED" />
          </label>
          <label class="form-field">
            Refresh
            <input data-authoring-path="node.sourceRefresh" value="${escapeHtml(nodeDraft.sourceRefresh)}" placeholder="daily" />
          </label>
          <label class="form-field">
            Series ID
            <input data-authoring-path="node.sourceSeriesId" value="${escapeHtml(nodeDraft.sourceSeriesId)}" />
          </label>
          <label class="form-field">
            Dictionary label
            <input data-authoring-path="node.dataDictionaryLabel" value="${escapeHtml(nodeDraft.dataDictionaryLabel)}" placeholder="data dictionary" />
          </label>
          <label class="form-field">
            Dictionary kind
            <select data-authoring-path="node.dataDictionaryKind">
              ${["link", "file"].map((value) => `<option value="${value}" ${nodeDraft.dataDictionaryKind === value ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </label>
          <label class="form-field form-field-full">
            Dictionary value
            <input data-authoring-path="node.dataDictionaryValue" value="${escapeHtml(nodeDraft.dataDictionaryValue)}" placeholder="https://... or docs/file.md" />
          </label>
          <label class="form-field">
            Raw asset label
            <input data-authoring-path="node.rawAssetLabel" value="${escapeHtml(nodeDraft.rawAssetLabel)}" placeholder="landing file" />
          </label>
          <label class="form-field">
            Raw asset kind
            <select data-authoring-path="node.rawAssetKind">
              ${["file", "object_storage", "glob", "directory"].map((value) => `<option value="${value}" ${nodeDraft.rawAssetKind === value ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </label>
          <label class="form-field">
            Raw asset format
            <select data-authoring-path="node.rawAssetFormat">
              ${["unknown", "csv", "csv_gz", "zip_csv", "parquet", "parquet_collection"].map((value) => `<option value="${value}" ${nodeDraft.rawAssetFormat === value ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </label>
          <label class="form-field form-field-full">
            Raw asset value
            <input data-authoring-path="node.rawAssetValue" value="${escapeHtml(nodeDraft.rawAssetValue)}" placeholder="data/raw/file.csv or data/parts/*.parquet" />
          </label>
        ` : ""}
        ${showNodeDataFields ? `
          <label class="form-field">
            Persistence
            <select data-authoring-path="node.persistence">
              ${["cold", "warm", "hot", "transient"].map((value) => `<option value="${value}" ${nodeDraft.persistence === value ? "selected" : ""}>${value}</option>`).join("")}
            </select>
          </label>
          <label class="form-field">
            Persisted
            <select data-authoring-path="node.persisted" data-coerce="boolean">
              <option value="true" ${nodeDraft.persisted ? "selected" : ""}>true</option>
              <option value="false" ${!nodeDraft.persisted ? "selected" : ""}>false</option>
            </select>
          </label>
          <label class="form-field">
            Update frequency
            <input data-authoring-path="node.updateFrequency" value="${escapeHtml(nodeDraft.updateFrequency)}" placeholder="daily" />
          </label>
          <label class="form-field form-field-full">
            Variables / schema seed
            <textarea data-authoring-path="node.schemaText" placeholder="One per line: column_name:type:category:label1,label2">${escapeHtml(nodeDraft.schemaText)}</textarea>
          </label>
        ` : ""}
      </div>
      <div class="section-actions">
        <button class="ghost-button" type="button" data-authoring-action="create-node">Create object</button>
      </div>
    </div>
    <div class="section">
      <h3>Create edge</h3>
      <div class="form-grid">
        <label class="form-field">
          Source
          <select data-authoring-path="edge.source">
            ${graphNodes.map((node) => `
              <option value="${escapeHtml(node.id)}" ${edgeDraft.source === node.id ? "selected" : ""}>${escapeHtml(node.id)}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Target
          <select data-authoring-path="edge.target">
            ${graphNodes.map((node) => `
              <option value="${escapeHtml(node.id)}" ${edgeDraft.target === node.id ? "selected" : ""}>${escapeHtml(node.id)}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Edge type
          <select data-authoring-path="edge.type">
            ${["contains", "ingests", "produces", "derives", "serves", "binds", "depends_on"].map((value) => `
              <option value="${value}" ${edgeDraft.type === value ? "selected" : ""}>${value}</option>
            `).join("")}
          </select>
        </label>
      </div>
      <div class="section-actions">
        <button class="ghost-button" type="button" data-authoring-action="create-edge">Create edge</button>
        <button class="inline-button danger" type="button" data-authoring-action="delete-selection">${escapeHtml(deleteLabel)}</button>
      </div>
    </div>
    <div class="section">
      <h3>Import asset</h3>
      <p class="hint">Create a source + data pair from a local file, glob, directory, or object-storage pointer. If the asset is accessible, quick profiling fills the dataset automatically.</p>
      <div class="form-grid">
        <label class="form-field">
          Source label
          <input data-authoring-path="import.sourceLabel" value="${escapeHtml(importDraft.sourceLabel)}" />
        </label>
        <label class="form-field">
          Source type
          <select data-authoring-path="import.sourceExtensionType">
            ${importSourceOptions.map((value) => `
              <option value="${value}" ${importDraft.sourceExtensionType === value ? "selected" : ""}>${value}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Source provider
          <input data-authoring-path="import.sourceProvider" value="${escapeHtml(importDraft.sourceProvider)}" />
        </label>
        <label class="form-field">
          Refresh
          <input data-authoring-path="import.sourceRefresh" value="${escapeHtml(importDraft.sourceRefresh)}" />
        </label>
        <label class="form-field">
          Origin kind
          <input data-authoring-path="import.sourceOriginKind" value="${escapeHtml(importDraft.sourceOriginKind)}" placeholder="api_endpoint, url, disk_path..." />
        </label>
        <label class="form-field">
          Origin value
          <input data-authoring-path="import.sourceOriginValue" value="${escapeHtml(importDraft.sourceOriginValue)}" />
        </label>
        <label class="form-field">
          Series ID
          <input data-authoring-path="import.sourceSeriesId" value="${escapeHtml(importDraft.sourceSeriesId)}" />
        </label>
        <label class="form-field">
          Raw asset label
          <input data-authoring-path="import.rawAssetLabel" value="${escapeHtml(importDraft.rawAssetLabel)}" />
        </label>
        <label class="form-field">
          Raw asset kind
          <select data-authoring-path="import.rawAssetKind">
            ${["file", "object_storage", "glob", "directory"].map((value) => `
              <option value="${value}" ${importDraft.rawAssetKind === value ? "selected" : ""}>${value}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Raw asset format
          <select data-authoring-path="import.rawAssetFormat">
            ${["unknown", "csv", "csv_gz", "zip_csv", "parquet", "parquet_collection"].map((value) => `
              <option value="${value}" ${importDraft.rawAssetFormat === value ? "selected" : ""}>${value}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field form-field-full">
          Raw asset value
          <input data-authoring-path="import.rawAssetValue" value="${escapeHtml(importDraft.rawAssetValue)}" placeholder="data/raw/file.csv or data/parts/*.parquet" />
        </label>
        <label class="form-field">
          Profile ready
          <select data-authoring-path="import.profileReady" data-coerce="boolean">
            <option value="true" ${importDraft.profileReady ? "selected" : ""}>true</option>
            <option value="false" ${!importDraft.profileReady ? "selected" : ""}>false</option>
          </select>
        </label>
        <label class="form-field">
          Data label
          <input data-authoring-path="import.dataLabel" value="${escapeHtml(importDraft.dataLabel)}" />
        </label>
        <label class="form-field">
          Data type
          <select data-authoring-path="import.dataExtensionType">
            ${importDataOptions.map((value) => `
              <option value="${value}" ${importDraft.dataExtensionType === value ? "selected" : ""}>${value}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Persistence
          <select data-authoring-path="import.persistence">
            ${["cold", "warm", "hot", "transient"].map((value) => `
              <option value="${value}" ${importDraft.persistence === value ? "selected" : ""}>${value}</option>
            `).join("")}
          </select>
        </label>
        <label class="form-field">
          Update frequency
          <input data-authoring-path="import.updateFrequency" value="${escapeHtml(importDraft.updateFrequency)}" />
        </label>
        <label class="form-field">
          Persisted
          <select data-authoring-path="import.persisted" data-coerce="boolean">
            <option value="true" ${importDraft.persisted ? "selected" : ""}>true</option>
            <option value="false" ${!importDraft.persisted ? "selected" : ""}>false</option>
          </select>
        </label>
        <label class="form-field form-field-full">
          Source description
          <input data-authoring-path="import.sourceDescription" value="${escapeHtml(importDraft.sourceDescription)}" />
        </label>
        <label class="form-field form-field-full">
          Data description
          <input data-authoring-path="import.dataDescription" value="${escapeHtml(importDraft.dataDescription)}" />
        </label>
        <label class="form-field form-field-full">
          Schema seed
          <textarea data-authoring-path="import.schemaText" placeholder="Optional schema-only lines: column_name:data_type">${escapeHtml(importDraft.schemaText)}</textarea>
        </label>
      </div>
      <div class="section-actions">
        <button class="ghost-button" type="button" data-authoring-action="import-asset">Import asset into graph</button>
      </div>
    </div>
    <div class="section">
      <h3>Import OpenAPI</h3>
      <p class="hint">Import API contracts from an OpenAPI JSON or YAML file path, or paste the spec directly. Existing routes are updated in place, and exact column-name matches to data/compute nodes are auto-bound when the match is unambiguous.</p>
      <div class="form-grid">
        <label class="form-field">
          Spec path
          <input data-authoring-path="openapi.specPath" value="${escapeHtml(state.authoring.openapi.specPath)}" placeholder="specs/openapi.yaml" />
        </label>
        <label class="form-field">
          Owner override
          <input data-authoring-path="openapi.owner" value="${escapeHtml(state.authoring.openapi.owner)}" placeholder="backend" />
        </label>
        <label class="form-field form-field-full">
          Spec text
          <textarea data-authoring-path="openapi.specText" placeholder="Paste OpenAPI JSON or YAML here">${escapeHtml(state.authoring.openapi.specText)}</textarea>
        </label>
      </div>
      <div class="section-actions">
        <button class="ghost-button" type="button" data-authoring-action="import-openapi">Import API contracts</button>
      </div>
    </div>
    <div class="section">
      <h3>Structure YAML</h3>
      <p class="hint">YAML is the canonical structure memory. Export it, refine it directly, or import a reviewed YAML draft back into the workbench.</p>
      <div class="form-grid">
        <label class="form-field">
          Scan role
          <select data-structure-path="role">
            <option value="scout" ${state.structureDraft.role === "scout" ? "selected" : ""}>scout</option>
            <option value="recorder" ${state.structureDraft.role === "recorder" ? "selected" : ""}>recorder</option>
          </select>
        </label>
        <label class="form-field">
          Scan scope
          <select data-structure-path="scope">
            <option value="changed" ${state.structureDraft.scope === "changed" ? "selected" : ""}>changed</option>
            <option value="paths" ${state.structureDraft.scope === "paths" ? "selected" : ""}>paths</option>
            <option value="full" ${state.structureDraft.scope === "full" ? "selected" : ""}>full</option>
          </select>
        </label>
        <label class="form-field form-field-full">
          Doc paths
          <textarea data-structure-path="docPathsText" placeholder="docs/plan.md&#10;docs/data_contracts.md">${escapeHtml(state.structureDraft.docPathsText || "")}</textarea>
        </label>
        <label class="form-field form-field-full">
          Selected paths
          <textarea data-structure-path="selectedPathsText" placeholder="src/backend&#10;src/services/market.py">${escapeHtml(state.structureDraft.selectedPathsText || "")}</textarea>
        </label>
        <label class="form-field form-field-full">
          YAML draft
          <textarea data-structure-path="yamlText" placeholder="version: '1.0'&#10;metadata: ...">${escapeHtml(state.structureDraft.yamlText || "")}</textarea>
        </label>
      </div>
      <div class="section-actions">
        <button class="ghost-button" type="button" data-structure-action="load-current-yaml">Load current YAML</button>
        <button class="ghost-button" type="button" data-structure-action="import-yaml">Import YAML</button>
        <button class="ghost-button" type="button" data-structure-action="scan-configured">Run ${escapeHtml(state.structureDraft.role)}</button>
      </div>
    </div>
  `;
}

function handleAuthoringMutation(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement) || !target.matches("[data-authoring-path]")) {
    return;
  }
  setValueAtPath(state.authoring, target.dataset.authoringPath, coerceValue(target));
  syncAuthoringState();
  if (["node.kind", "node.extensionType", "import.sourceExtensionType", "import.dataExtensionType"].includes(target.dataset.authoringPath)) {
    renderAuthoringPanel();
  }
}

function handleAuthoringClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement) || !target.dataset.authoringAction) {
    return;
  }
  if (target.dataset.authoringAction === "create-node") {
    createAuthoringNode();
    return;
  }
  if (target.dataset.authoringAction === "create-edge") {
    createAuthoringEdge();
    return;
  }
  if (target.dataset.authoringAction === "delete-selection") {
    deleteCurrentSelection();
    return;
  }
  if (target.dataset.authoringAction === "import-asset") {
    importAssetFromAuthoring();
    return;
  }
  if (target.dataset.authoringAction === "import-openapi") {
    importOpenAPIFromAuthoring();
  }
}

function handleStructureMutation(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement) || !target.dataset.structurePath) {
    return;
  }
  state.structureDraft[target.dataset.structurePath] = coerceValue(target);
}

function handleStructureClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement) || !target.dataset.structureAction) {
    return;
  }
  const action = target.dataset.structureAction;
  if (action === "export-yaml" || action === "load-current-yaml") {
    loadCurrentStructureYaml();
    return;
  }
  if (action === "import-yaml") {
    importStructureYaml();
    return;
  }
  if (action === "scan-quick") {
    state.structureDraft.role = target.dataset.structureRole || "scout";
    runStructureScan();
    return;
  }
  if (action === "scan-configured") {
    runStructureScan();
    return;
  }
  if (action === "refresh-bundles") {
    refreshStructureBundles({ updateStatus: true });
    return;
  }
  if (action === "open-bundle") {
    loadStructureBundle(target.dataset.bundleId || "");
    return;
  }
  if (action === "review-patch") {
    reviewStructurePatch(target.dataset.bundleId || "", target.dataset.patchId || "", target.dataset.decision || "deferred");
    return;
  }
  if (action === "review-patch-batch") {
    reviewStructurePatches(
      target.dataset.bundleId || "",
      parsePatchIdList(target.dataset.patchIds || ""),
      target.dataset.decision || "deferred"
    );
    return;
  }
  if (action === "merge-bundle") {
    mergeStructureBundle(target.dataset.bundleId || "");
  }
}

async function loadCurrentStructureYaml() {
  setStatus("Loading YAML...", "Fetching the canonical structure spec.");
  const response = await fetch("/api/structure/export");
  const text = await response.text();
  if (!response.ok) {
    setStatus("YAML load failed", text || "Unable to export the canonical structure YAML.");
    return;
  }
  state.structureDraft.yamlText = text;
  state.authoringDrawerOpen = true;
  render();
  setStatus("YAML loaded", "The canonical structure YAML is ready to inspect or edit in Add / Import.");
}

async function importStructureYaml() {
  if (!state.structureDraft.yamlText.trim()) {
    setStatus("Import failed", "Paste or load YAML before importing.");
    return;
  }
  setStatus("Importing YAML...", "Validating the YAML draft and updating the canonical structure memory.");
  const response = await fetch("/api/structure/import", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ yaml_text: state.structureDraft.yamlText, updated_by: "user" }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Import failed", payload.detail || payload.error || "Unable to import the YAML structure spec.");
    return;
  }
  state.graph = payload.graph;
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.structure = payload.structure || state.structure;
  state.dirty = false;
  state.needsAutoLayout = true;
  render();
  if (!state.hasManualZoom) {
    fitGraphToViewport();
  }
  setStatus("YAML imported", "The canonical YAML spec now drives the current workbench graph.");
}

async function refreshStructureBundles(options = {}) {
  const response = await fetch("/api/structure/bundles");
  const payload = await response.json();
  if (!response.ok) {
    if (options.updateStatus) {
      setStatus("Bundle refresh failed", payload.detail || payload.error || "Unable to refresh structure review bundles.");
    }
    return;
  }
  state.structure = {
    ...(state.structure || {}),
    bundles: payload.bundles || [],
  };
  if (
    state.selectedStructureBundleId
    && !(payload.bundles || []).some((bundle) => bundle.bundle_id === state.selectedStructureBundleId)
  ) {
    state.selectedStructureBundleId = "";
    state.selectedStructureBundle = null;
  }
  renderStructureMemory();
  if (options.updateStatus) {
    setStatus("Bundles refreshed", `${formatValue((payload.bundles || []).length)} review bundle${(payload.bundles || []).length === 1 ? "" : "s"} available.`);
  }
}

async function loadStructureBundle(bundleId) {
  if (!bundleId) {
    return;
  }
  const response = await fetch(`/api/structure/bundles/${encodeURIComponent(bundleId)}`);
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Bundle load failed", payload.detail || payload.error || "Unable to load the selected review bundle.");
    return;
  }
  state.selectedStructureBundleId = bundleId;
  state.selectedStructureBundle = payload.bundle || null;
  renderStructureMemory();
  setStatus("Bundle loaded", `${bundleId} is ready for review.`);
}

async function runStructureScan() {
  setStatus("Scanning structure...", `Running a ${state.structureDraft.role} pass across the current root, docs, and selected paths.`);
  const response = await fetch("/api/structure/scan", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      role: state.structureDraft.role || "scout",
      scope: state.structureDraft.scope || "changed",
      include_tests: state.projectProfileOptions.includeTests,
      include_internal: state.projectProfileOptions.includeInternal,
      root_path: state.projectProfileOptions.rootPath || "",
      doc_paths: parseMultilineList(state.structureDraft.docPathsText),
      selected_paths: parseMultilineList(state.structureDraft.selectedPathsText),
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Scan failed", payload.detail || payload.error || "Unable to inspect repo or docs into a structure review bundle.");
    return;
  }
  state.structure = payload.structure || state.structure;
  state.projectProfile = payload.project_profile || state.projectProfile;
  state.selectedStructureBundle = payload.bundle || null;
  state.selectedStructureBundleId = payload.bundle?.bundle_id || "";
  render();
  setStatus("Bundle created", `${payload.bundle?.bundle_id || "Review bundle"} is ready for accept / reject / defer review.`);
}

async function reviewStructurePatch(bundleId, patchId, decision) {
  return reviewStructurePatches(bundleId, patchId ? [patchId] : [], decision);
}

async function reviewStructurePatches(bundleId, patchIds, decision) {
  const normalizedIds = Array.from(new Set((patchIds || []).filter(Boolean)));
  if (!bundleId || !normalizedIds.length) {
    return;
  }
  const endpoint = normalizedIds.length === 1
    ? `/api/structure/bundles/${encodeURIComponent(bundleId)}/review`
    : `/api/structure/bundles/${encodeURIComponent(bundleId)}/review-batch`;
  const body = normalizedIds.length === 1
    ? { patch_id: normalizedIds[0], decision }
    : { patch_ids: normalizedIds, decision };
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Patch review failed", payload.detail || payload.error || "Unable to update the review decision for that patch.");
    return;
  }
  state.selectedStructureBundle = payload.bundle || null;
  state.selectedStructureBundleId = payload.bundle?.bundle_id || bundleId;
  await refreshStructureBundles();
  renderStructureMemory();
  const targetLabel = normalizedIds.length === 1 ? normalizedIds[0] : `${formatValue(normalizedIds.length)} patches`;
  setStatus("Patch review updated", `${targetLabel} ${normalizedIds.length === 1 ? "is" : "are"} now ${decision}.`);
}

function parsePatchIdList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

async function mergeStructureBundle(bundleId) {
  if (!bundleId) {
    return;
  }
  setStatus("Merging bundle...", "Applying accepted patches into the canonical YAML structure memory.");
  const response = await fetch(`/api/structure/bundles/${encodeURIComponent(bundleId)}/merge`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ merged_by: "user" }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Merge failed", payload.detail || payload.error || "Unable to merge the selected review bundle.");
    return;
  }
  state.graph = payload.graph;
  state.diagnostics = payload.diagnostics || {};
  state.validationReport = payload.validation || null;
  state.structure = payload.structure || state.structure;
  state.selectedStructureBundle = payload.bundle || null;
  state.selectedStructureBundleId = payload.bundle?.bundle_id || bundleId;
  state.dirty = false;
  state.needsAutoLayout = true;
  render();
  if (!state.hasManualZoom) {
    fitGraphToViewport();
  }
  setStatus("Bundle merged", `${bundleId} updated the canonical YAML and refreshed the active graph.`);
}

function parseMultilineList(value) {
  return (value || "")
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function handleProjectProfileClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  if (target.dataset.projectWizardStep) {
    state.projectWizardStep = Number.parseInt(target.dataset.projectWizardStep, 10) || 1;
    renderProjectProfile();
    return;
  }
  if (target.dataset.projectWizardMove === "back") {
    state.projectWizardStep = Math.max(1, state.projectWizardStep - 1);
    renderProjectProfile();
    return;
  }
  if (target.dataset.projectWizardMove === "next") {
    state.projectWizardStep = Math.min(4, state.projectWizardStep + 1);
    renderProjectProfile();
    return;
  }
  if (target.dataset.assetsPage === "prev") {
    state.wizardAssetsPage = Math.max(1, (state.wizardAssetsPage || 1) - 1);
    renderProjectProfile();
    return;
  }
  if (target.dataset.assetsPage === "next") {
    state.wizardAssetsPage = (state.wizardAssetsPage || 1) + 1;
    renderProjectProfile();
    return;
  }
  if (target.dataset.projectSelectAll) {
    state.selectedProjectImports = (state.projectProfile?.data_assets || [])
      .filter((asset) => asset.suggested_import)
      .map((asset) => asset.path);
    renderProjectProfile();
    setStatus("Suggested imports selected", `${state.selectedProjectImports.length} project assets are ready for bulk import.`);
    return;
  }
  if (target.dataset.projectClearSelection) {
    state.selectedProjectImports = [];
    renderProjectProfile();
    setStatus("Selection cleared", "No discovered assets are selected.");
    return;
  }
  if (target.dataset.projectApiSelectAll) {
    state.selectedProjectApiHints = (state.projectProfile?.api_contract_hints || []).map((hint) => hint.id);
    renderProjectProfile();
    setStatus("API hints selected", `${state.selectedProjectApiHints.length} API hint${state.selectedProjectApiHints.length === 1 ? "" : "s"} selected for bootstrap.`);
    return;
  }
  if (target.dataset.projectApiClear) {
    state.selectedProjectApiHints = [];
    renderProjectProfile();
    setStatus("API selection cleared", "No API hints are selected.");
    return;
  }
  if (target.dataset.projectUiSelectAll) {
    state.selectedProjectUiHints = (state.projectProfile?.ui_contract_hints || []).map((hint) => hint.id);
    renderProjectProfile();
    setStatus("UI hints selected", `${state.selectedProjectUiHints.length} UI hint${state.selectedProjectUiHints.length === 1 ? "" : "s"} selected for bootstrap.`);
    return;
  }
  if (target.dataset.projectUiClear) {
    state.selectedProjectUiHints = [];
    renderProjectProfile();
    setStatus("UI selection cleared", "No UI hints are selected.");
    return;
  }
  if (target.dataset.projectImportSelected) {
    importSelectedProjectSuggestions();
    return;
  }
  if (target.dataset.projectBootstrap) {
    importProjectBootstrap();
    return;
  }
  if (target.dataset.projectPresetApply) {
    applySelectedProjectPreset();
    return;
  }
  if (target.dataset.projectPresetSave) {
    saveCurrentProjectPreset();
    return;
  }
  if (target.dataset.projectPresetDelete) {
    deleteSelectedProjectPreset();
    return;
  }
  if (target.dataset.projectRescan) {
    loadProjectProfile();
    return;
  }
  if (target.dataset.projectRootPicker) {
    openDirectoryPicker();
    return;
  }
  if (target.dataset.projectApiCreate) {
    createApiContractFromHint(target.dataset.projectApiCreate);
    return;
  }
  if (target.dataset.projectUiCreate) {
    createUiContractFromHint(target.dataset.projectUiCreate);
    return;
  }
  if (target.dataset.projectImportLoad) {
    loadProjectImportSuggestion(target.dataset.projectImportLoad);
    return;
  }
  if (target.dataset.projectImportRun) {
    importProjectImportSuggestion(target.dataset.projectImportRun);
  }
}

function handleProjectProfileMutation(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  if (target instanceof HTMLSelectElement && target.dataset.projectPresetSelect) {
    state.selectedProjectPresetId = target.value;
    syncPresetDraftFromSelection();
    renderProjectProfile();
    return;
  }
  if (target instanceof HTMLInputElement && target.dataset.projectPresetField) {
    state.projectPresetDraft[target.dataset.projectPresetField] = target.value;
    return;
  }
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  if (target.dataset.projectProfileRoot) {
    state.projectProfileOptions.rootPath = target.value;
    return;
  }
  if (target.dataset.projectProfileOption) {
    state.projectProfileOptions[target.dataset.projectProfileOption] = target.checked;
    renderProjectProfile();
    return;
  }
  if (target.dataset.projectBootstrapOption) {
    state.projectBootstrapOptions[target.dataset.projectBootstrapOption] = target.checked;
    renderProjectProfile();
    return;
  }
  if (!target.dataset.projectSelect) {
    if (target.dataset.projectApiSelect) {
      state.selectedProjectApiHints = toggleSelectionList(state.selectedProjectApiHints, target.dataset.projectApiSelect, target.checked);
      renderProjectProfile();
      return;
    }
    if (target.dataset.projectUiSelect) {
      state.selectedProjectUiHints = toggleSelectionList(state.selectedProjectUiHints, target.dataset.projectUiSelect, target.checked);
      renderProjectProfile();
    }
    return;
  }
  const path = target.dataset.projectSelect;
  state.selectedProjectImports = toggleSelectionList(state.selectedProjectImports, path, target.checked);
  renderProjectProfile();
}

async function loadProjectProfile() {
  return loadProjectProfileWithOptions();
}

async function loadProjectProfileWithOptions(options = {}) {
  projectProfileButton.disabled = true;
  setStatus("Discovering project...", "Inspecting manifests, code, docs, and accessible data assets.");
  const includeTests = options.includeTests ?? state.projectProfileOptions.includeTests;
  const includeInternal = options.includeInternal ?? state.projectProfileOptions.includeInternal;
  const rootPath = options.rootPath ?? state.projectProfileOptions.rootPath;
  const params = new URLSearchParams({
    include_tests: includeTests ? "true" : "false",
    include_internal: includeInternal ? "true" : "false",
  });
  if (rootPath) {
    params.set("root_path", rootPath);
  }
  const response = await fetch(`/api/project/profile?${params.toString()}`);
  const payload = await response.json();
  projectProfileButton.disabled = false;
  if (!response.ok) {
    setStatus("Project discovery failed", payload.detail || payload.error || "Unable to inspect this project.");
    return;
  }
  state.projectProfile = payload.project_profile || null;
  state.projectProfileOptions.includeTests = includeTests;
  state.projectProfileOptions.includeInternal = includeInternal;
  state.projectProfileOptions.rootPath = state.projectProfile?.root || rootPath || "";
  if (options.preset) {
    applyProjectPresetSelections(options.preset);
  } else {
    state.selectedProjectImports = [];
    state.selectedProjectApiHints = [];
    state.selectedProjectUiHints = [];
  }
  renderProjectProfile();
  const summary = state.projectProfile?.summary || {};
  setStatus(
    "Project discovery ready",
    `${formatValue(summary.manifests)} manifests, ${formatValue(summary.code_files)} code files, ${formatValue(summary.data_assets)} data assets scanned.`
  );
}

async function loadOnboardingPresets(options = {}) {
  const response = await fetch("/api/onboarding/presets");
  const payload = await response.json();
  if (!response.ok) {
    if (!options.silent) {
      setStatus("Preset load failed", payload.detail || payload.error || "Unable to load onboarding presets.");
    }
    return;
  }
  state.onboardingPresets = payload.presets || [];
  if (state.selectedProjectPresetId && !state.onboardingPresets.some((preset) => preset.id === state.selectedProjectPresetId)) {
    state.selectedProjectPresetId = "";
  }
  syncPresetDraftFromSelection();
}

function syncPresetDraftFromSelection() {
  const preset = getSelectedProjectPreset();
  if (!preset) {
    state.projectPresetDraft = { name: "", description: "" };
    return;
  }
  state.projectPresetDraft.name = preset.name || "";
  state.projectPresetDraft.description = preset.description || "";
}

function getSelectedProjectPreset() {
  return (state.onboardingPresets || []).find((preset) => preset.id === state.selectedProjectPresetId) || null;
}

async function applySelectedProjectPreset() {
  const preset = getSelectedProjectPreset();
  if (!preset) {
    setStatus("Preset apply failed", "Choose a saved preset first.");
    return;
  }
  state.projectProfileOptions.includeTests = preset.include_tests;
  state.projectProfileOptions.includeInternal = preset.include_internal;
  state.projectProfileOptions.rootPath = preset.root || state.projectProfileOptions.rootPath;
  state.projectBootstrapOptions = {
    assets: preset.bootstrap_options?.assets !== false,
    apiHints: preset.bootstrap_options?.apiHints !== false,
    uiHints: preset.bootstrap_options?.uiHints !== false,
  };
  state.projectWizardStep = 2;
  await loadProjectProfileWithOptions({
    includeTests: preset.include_tests,
    includeInternal: preset.include_internal,
    rootPath: preset.root || state.projectProfileOptions.rootPath,
    preset,
  });
  setStatus("Preset applied", `${preset.name} is now loaded into the onboarding wizard.`);
}

function applyProjectPresetSelections(preset) {
  const assetPaths = new Set(
    (state.projectProfile?.data_assets || [])
      .filter((asset) => asset.suggested_import)
      .map((asset) => asset.path)
  );
  const apiHintIds = new Set((state.projectProfile?.api_contract_hints || []).map((hint) => hint.id));
  const uiHintIds = new Set((state.projectProfile?.ui_contract_hints || []).map((hint) => hint.id));
  state.selectedProjectImports = (preset.selected_asset_paths || []).filter((path) => assetPaths.has(path));
  state.selectedProjectApiHints = (preset.selected_api_hint_ids || []).filter((hintId) => apiHintIds.has(hintId));
  state.selectedProjectUiHints = (preset.selected_ui_hint_ids || []).filter((hintId) => uiHintIds.has(hintId));
}

async function saveCurrentProjectPreset() {
  if (!state.projectPresetDraft.name.trim()) {
    setStatus("Preset save failed", "Preset name is required.");
    return;
  }
  const preset = {
    id: state.selectedProjectPresetId || "",
    name: state.projectPresetDraft.name.trim(),
    description: state.projectPresetDraft.description.trim(),
    root: state.projectProfileOptions.rootPath || state.projectProfile?.root || "",
    include_tests: state.projectProfileOptions.includeTests,
    include_internal: state.projectProfileOptions.includeInternal,
    bootstrap_options: { ...state.projectBootstrapOptions },
    selected_asset_paths: [...(state.selectedProjectImports || [])],
    selected_api_hint_ids: [...(state.selectedProjectApiHints || [])],
    selected_ui_hint_ids: [...(state.selectedProjectUiHints || [])],
  };
  const response = await fetch("/api/onboarding/presets", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ preset }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Preset save failed", payload.detail || payload.error || "Unable to save the onboarding preset.");
    return;
  }
  state.onboardingPresets = payload.presets || [];
  state.selectedProjectPresetId = payload.saved?.id || state.selectedProjectPresetId;
  syncPresetDraftFromSelection();
  renderProjectProfile();
  setStatus("Preset saved", `${payload.saved?.name || "Preset"} is available for future onboarding runs.`);
}

async function deleteSelectedProjectPreset() {
  const preset = getSelectedProjectPreset();
  if (!preset) {
    setStatus("Preset delete failed", "Choose a saved preset first.");
    return;
  }
  const response = await fetch(`/api/onboarding/presets/${encodeURIComponent(preset.id)}`, {
    method: "DELETE",
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Preset delete failed", payload.detail || payload.error || "Unable to delete the onboarding preset.");
    return;
  }
  state.onboardingPresets = payload.presets || [];
  state.selectedProjectPresetId = "";
  state.projectPresetDraft = { name: "", description: "" };
  renderProjectProfile();
  setStatus("Preset deleted", `${preset.name} was removed from saved onboarding presets.`);
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

boot();
