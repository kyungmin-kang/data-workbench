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
  artifactStorage: null,
  structure: null,
  planState: null,
  sourceOfTruth: null,
  executionBrief: null,
  executionWorkflow: null,
  executionView: "all",
  executionSearch: "",
  executionRoleFilter: "all",
  executionHideCompleted: false,
  selectedStructureBundleId: "",
  selectedStructureBundle: null,
  structureBundleInboxFilter: "needs_attention",
  structureAssignmentFilter: "all",
  structureReviewUnitFilter: "review_required",
  structureShowLowConfidence: false,
  structureShowMinorImpacts: false,
  structureShowNonMaterial: false,
  structureRebasePreviews: {},
  structureReviewerIdentity: "user",
  structureReviewPresets: [],
  selectedStructureReviewPresetId: "",
  structureReviewPresetDraftName: "",
  structureActiveReviewUnitKey: "",
  structureWorkflowDraft: {
    bundleOwner: "",
    assignedReviewer: "",
    triageState: "new",
    triageNote: "",
  },
  structureDraft: {
    yamlText: "",
    role: "scout",
    scope: "changed",
    docPathsText: "",
    selectedPathsText: "",
  },
  projectProfile: null,
  projectProfileJob: null,
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
  projectProfileFilters: {
    assetsQuery: "",
    assetsStatus: "all",
    contractsQuery: "",
  },
  projectProfilePages: {
    assets: 1,
    api: 1,
    ui: 1,
    sql: 1,
    orm: 1,
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
    execution: true,
    dependencies: false,
    editing: false,
  },
  lastInspectorSelectionKey: "",
  reviewDrawerOpen: false,
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
  reviewActionConfirm: null,
  destructiveWarningSilencedUntil: 0,
  directoryPicker: {
    open: false,
    query: "",
    results: [],
    loading: false,
  },
  authoring: buildDefaultAuthoringState(),
  dirty: false,
  executionDirty: false,
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
const executionSummary = document.getElementById("execution-summary");
const planSummary = document.getElementById("plan-summary");
const validationSummary = document.getElementById("validation-summary");
const authoringPanel = document.getElementById("authoring-panel");
const authoringDrawer = document.getElementById("authoring-drawer");
const authoringDrawerToggle = document.getElementById("authoring-drawer-toggle");
const authoringDrawerClose = document.getElementById("authoring-drawer-close");
const reviewDrawer = document.getElementById("review-drawer");
const reviewDrawerToggle = document.getElementById("review-drawer-toggle");
const reviewDrawerClose = document.getElementById("review-drawer-close");
const reviewDrawerContent = document.getElementById("review-drawer-content");
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
const reviewActionModal = document.getElementById("review-action-modal");
const reviewActionModalTitle = document.getElementById("review-action-modal-title");
const reviewActionModalMessage = document.getElementById("review-action-modal-message");
const reviewActionModalSummary = document.getElementById("review-action-modal-summary");
const reviewActionModalNote = document.getElementById("review-action-modal-note");
const reviewActionModalCancel = document.getElementById("review-action-modal-cancel");
const reviewActionModalOk = document.getElementById("review-action-modal-ok");
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
let toastHost = null;

const GRAPH_CARD_BASE = { width: 360, height: 118 };
const GRAPH_CARD_EXPANDED = { width: 460, height: 252 };
const GRAPH_LAYOUT_GAP_X = 84;
const GRAPH_LAYOUT_GAP_Y = 46;
const GRAPH_MIN_ZOOM = 0.25;
const GRAPH_MAX_ZOOM = 2.25;
const DEFAULT_COLUMN_TYPES = ["string", "integer", "float", "boolean", "date", "datetime", "json", "array"];
const TRACE_PALETTE = ["#0b7285", "#c92a2a", "#5f3dc4", "#e67700", "#1c7ed6", "#2b8a3e", "#a61e4d", "#495057"];
const STRUCTURE_REVIEW_PREFS_STORAGE_KEY = "workbench.structureReviewPrefs.v1";
const STRUCTURE_REVIEW_PRESETS_STORAGE_KEY = "workbench.structureReviewPresets.v1";

function readLocalStorageJson(key, fallback) {
  try {
    const rawValue = window.localStorage.getItem(key);
    return rawValue ? JSON.parse(rawValue) : fallback;
  } catch (error) {
    return fallback;
  }
}

function writeLocalStorageJson(key, value) {
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    // Ignore storage failures so review UX still works in restricted browsers.
  }
}

async function boot() {
  loadStructureReviewPreferences();
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
  state.lastArtifacts = payload.latest_artifacts || null;
  state.artifactStorage = payload.artifact_storage || null;
  state.planState = payload.plan_state || null;
  state.sourceOfTruth = payload.source_of_truth || null;
  state.executionBrief = null;
  state.executionDirty = false;
  state.bindingSuggestions = {};
  state.selectedNodeId = null;
  state.selectedEdgeId = null;
  state.selectedStructureBundleId = "";
  state.selectedStructureBundle = null;
  syncStructureWorkflowDraft(null);
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
  reviewDrawerToggle.addEventListener("click", async () => {
    await toggleReviewDrawer(!state.reviewDrawerOpen);
  });
  reviewDrawerClose.addEventListener("click", () => {
    void toggleReviewDrawer(false);
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
  reviewActionModalCancel.addEventListener("click", cancelReviewActionConfirm);
  reviewActionModalOk.addEventListener("click", () => {
    void confirmReviewAction();
  });
  reviewActionModal.addEventListener("click", handleModalBackdropClick);
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
    state.artifactStorage = payload.artifact_storage || state.artifactStorage;
    applyExecutionPayload(payload);
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
    state.lastArtifacts = payload.latest_artifacts || state.lastArtifacts;
    state.artifactStorage = payload.artifact_storage || state.artifactStorage;
    applyExecutionPayload(payload);
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
  if (executionSummary) {
    executionSummary.addEventListener("input", handleExecutionMutation);
    executionSummary.addEventListener("change", handleExecutionMutation);
    executionSummary.addEventListener("click", handleExecutionClick);
  }
  if (reviewDrawerContent) {
    reviewDrawerContent.addEventListener("input", handleStructureMutation);
    reviewDrawerContent.addEventListener("change", handleStructureMutation);
    reviewDrawerContent.addEventListener("click", handleStructureClick);
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
    if (!state.dirty && !state.executionDirty) return;
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
  renderExecutionSummary();
  renderStructureReviewDrawer();
  renderAuthoringPanel();
  renderProjectProfile();
  renderGraphContextMenu();
  renderConfirmModal();
  renderReviewActionModal();
  renderDirectoryPicker();
}

boot();
