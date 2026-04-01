// Graph shell rendering, navigation, interaction, and layout helpers extracted from app.js

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
  applyExecutionPayload(payload);
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
  const hasGraphEdits = Boolean(state.dirty);
  const hasExecutionEdits = Boolean(state.executionDirty);
  dirtyIndicator.textContent = hasGraphEdits && hasExecutionEdits
    ? "Unsaved graph and execution edits"
    : hasGraphEdits
      ? "Unsaved graph edits"
      : hasExecutionEdits
        ? "Unsaved execution edits"
        : "All changes saved";
  dirtyIndicator.classList.toggle("dirty", hasGraphEdits || hasExecutionEdits);
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
  if (reviewDrawer) {
    reviewDrawer.hidden = !state.reviewDrawerOpen;
  }
  if (reviewDrawerToggle) {
    reviewDrawerToggle.classList.toggle("primary", state.reviewDrawerOpen);
    const structure = state.structure;
    const currentVersion = structure?.structure_version || 1;
    const inboxCount = structure ? buildStructureBundleInboxCounts(structure.bundles || [], currentVersion).needs_attention : 0;
    reviewDrawerToggle.textContent = inboxCount ? `Review Inbox (${formatValue(inboxCount)})` : "Review Inbox";
  }
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
  const structureOverlay = buildStructureGraphReviewOverlay(getActiveStructureBundleForGraph());
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
    element.className = buildNodeClass(node.id, highlight.nodeIds, structureOverlay);
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
    element.innerHTML = renderGraphNodeCard(node, structureOverlay);
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
    element.querySelectorAll("[data-graph-open-review-node]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        void openGraphReviewTarget({ nodeId: button.dataset.graphOpenReviewNode || "" });
      });
    });
    element.querySelectorAll("[data-graph-open-review-field]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        void openGraphReviewTarget({
          nodeId: button.dataset.graphNodeId || "",
          fieldId: button.dataset.graphOpenReviewField || "",
          reviewUnitKey: button.dataset.reviewUnitKey || "",
        });
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
      execution: true,
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
    ${renderInspectorSection("execution", "Execution", renderNodeExecution(node), true)}
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

function isEditableTarget(target) {
  return target instanceof HTMLElement && (
    ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName)
    || target.isContentEditable
  );
}

function handleGlobalKeyDown(event) {
  if (event.key === "Escape" && state.reviewActionConfirm) {
    cancelReviewActionConfirm();
    return;
  }
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
    return;
  }
  if (event.metaKey || event.ctrlKey || event.altKey || state.reviewActionConfirm || isEditableTarget(event.target)) {
    return;
  }
  const lowerKey = String(event.key || "").toLowerCase();
  if (lowerKey === "j" || event.key === "ArrowDown") {
    event.preventDefault();
    moveStructureActiveReviewUnit(1);
    return;
  }
  if (lowerKey === "k" || event.key === "ArrowUp") {
    event.preventDefault();
    moveStructureActiveReviewUnit(-1);
    return;
  }
  if (lowerKey === "a") {
    event.preventDefault();
    triggerStructureKeyboardReview("accepted");
    return;
  }
  if (lowerKey === "d") {
    event.preventDefault();
    triggerStructureKeyboardReview("deferred");
    return;
  }
  if (lowerKey === "r") {
    event.preventDefault();
    triggerStructureKeyboardReview("rejected");
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

function buildNodeClass(nodeId, highlightNodeIds, structureOverlay = null) {
  const node = getNodeById(nodeId);
  const classes = ["graph-node", node.kind];
  const health = getNodeDiagnostics(nodeId).health;
  const reviewSummary = getGraphNodeReviewSummary(nodeId, structureOverlay);
  if (health && health !== "healthy") {
    classes.push(`health-${health}`);
  }
  if (reviewSummary?.status) {
    classes.push(`structure-${reviewSummary.status}`);
  }
  if (reviewSummary?.reviewUnitKeys?.includes(state.structureActiveReviewUnitKey)) {
    classes.push("structure-review-active");
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
  return 88 + Math.max(1, items.length) * 186;
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

function renderGraphNodeCard(node, structureOverlay = null) {
  const summary = buildNodeMeta(node);
  const expanded = isGraphNodeExpanded(node.id);
  const description = node.description || "No description.";
  const storageMeta = getNodeStorageMeta(node);
  const refreshMeta = getNodeRefreshMeta(node);
  const workStatus = getNodeWorkStatus(node);
  const executionSummary = getNodeExecutionSummary(node.id);
  const diagnostics = getNodeDiagnostics(node.id);
  const reviewSummary = getGraphNodeReviewSummary(node.id, structureOverlay);
  const editMode = state.interactionMode === "edit";
  const nodeEditLabel = isGraphNodeEditMode(node.id)
    ? "Done"
    : node.kind === "data"
      ? "Edit table"
      : node.kind === "contract"
        ? "Edit fields"
        : "Edit node";
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
        ${executionSummary
          ? `<span class="graph-work-status ${escapeHtml(workStatus)}">${escapeHtml(WORK_STATUS_LABELS[workStatus])}</span>`
          : `<button class="graph-work-status ${escapeHtml(workStatus)}" type="button" data-graph-cycle-work-status="${escapeHtml(node.id)}">${escapeHtml(WORK_STATUS_LABELS[workStatus])}</button>`}
        ${editMode && expanded ? `<button class="text-button" type="button" data-graph-node-edit="${escapeHtml(node.id)}">${escapeHtml(nodeEditLabel)}</button>` : ""}
        ${editMode ? `<button class="text-button danger-link" type="button" data-graph-remove-node="${escapeHtml(node.id)}">Remove</button>` : ""}
        ${reviewSummary ? `<button class="text-button graph-review-link" type="button" data-graph-open-review-node="${escapeHtml(node.id)}">${escapeHtml(reviewSummary.reviewRequiredCount ? "Review now" : "Open review")}</button>` : ""}
        <button class="text-button" type="button" data-graph-toggle-expand="${escapeHtml(node.id)}" ${state.showGraphDetails ? "disabled" : ""}>${expanded ? "Hide Details" : "Show Details"}</button>
      </div>
    </div>
    <div class="graph-node-health-summary ${escapeHtml(diagnostics.health || "healthy")}">↑ ${escapeHtml(String(diagnostics.upstream_count || 0))} &nbsp; ↓ ${escapeHtml(String(diagnostics.downstream_count || 0))} &nbsp; ${getHealthIcon(diagnostics.health || "healthy")} ${escapeHtml(diagnostics.health || "healthy")}</div>
    ${renderGraphNodeExecutionBar(node)}
    ${renderGraphNodeReviewBar(node.id, reviewSummary)}
    ${expanded ? `
      <div class="graph-node-kind">
        <span class="pill">${node.kind} / ${node.extension_type}</span>
      </div>
    ` : ""}
    <div class="graph-node-summary">${escapeHtml(summary)}</div>
    ${expanded ? `<div class="graph-node-subsummary">${renderExpandedNodePills(node)}</div>` : ""}
    ${expanded ? renderExpandedGraphNodeBody(node, structureOverlay) : ""}
  `;
}

function renderGraphNodeReviewBar(nodeId, reviewSummary) {
  if (!reviewSummary) {
    return "";
  }
  const chips = [];
  if (reviewSummary.reviewRequiredCount) {
    chips.push(`<span class="graph-node-review-chip broken">${escapeHtml(`${formatValue(reviewSummary.reviewRequiredCount)} review required`)}</span>`);
  } else if (reviewSummary.pendingCount) {
    chips.push(`<span class="graph-node-review-chip warning">${escapeHtml(`${formatValue(reviewSummary.pendingCount)} pending`)}</span>`);
  } else {
    chips.push(`<span class="graph-node-review-chip healthy">${escapeHtml(summarizeGraphReviewSummary(reviewSummary))}</span>`);
  }
  if (reviewSummary.contradictionCount) {
    chips.push(`<span class="graph-node-review-chip broken">${escapeHtml(`${formatValue(reviewSummary.contradictionCount)} contradiction${reviewSummary.contradictionCount === 1 ? "" : "s"}`)}</span>`);
  }
  if (reviewSummary.impactCount) {
    chips.push(`<span class="graph-node-review-chip warning">${escapeHtml(`${formatValue(reviewSummary.impactCount)} impact${reviewSummary.impactCount === 1 ? "" : "s"}`)}</span>`);
  }
  return `
    <div class="graph-node-review-bar ${escapeHtml(reviewSummary.status || "reviewed")}">
      <div class="chip-row">${chips.join("")}</div>
      <button class="text-button graph-review-link" type="button" data-graph-open-review-node="${escapeHtml(nodeId)}">Open in review inbox</button>
    </div>
  `;
}

function renderExpandedGraphNodeBody(node, structureOverlay = null) {
  if (node.kind === "data") {
    return renderExpandedGraphDataTable(node, structureOverlay);
  }
  const items = getGraphNodeExpandedItems(node);
  if (node.kind === "contract") {
    const editMode = state.interactionMode === "edit";
    const diagnostics = getContractDiagnostics(node.id);
    return `
      <div class="graph-node-body">
        ${renderGraphPickedToolbar(node)}
        <div class="graph-node-columns">
          ${items.length ? items.map((field) => {
            const fieldReview = getGraphFieldReviewSummary(field.id, structureOverlay);
            return `
            <div
              class="graph-node-column ${getGraphColumnLineageClass(`${node.id}.${field.name}`)} ${getLineageSeedColors(`${node.id}.${field.name}`).length ? "lineage-active" : ""} ${isColumnPicked(`${node.id}.${field.name}`) ? "picked" : ""} ${fieldReview?.status === "review-required" ? "review-required" : fieldReview?.status === "warning" ? "review-warning" : fieldReview?.status === "reviewed" ? "reviewed" : ""} ${fieldReview?.reviewUnitKey === state.structureActiveReviewUnitKey ? "active-review-target" : ""}"
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
              ${renderGraphFieldReviewHint(node.id, field.id, fieldReview)}
            </div>
          `;
          }).join("") : "<div class=\"graph-node-column\">No fields recorded.</div>"}
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

function renderGraphFieldReviewHint(nodeId, fieldId, reviewSummary) {
  if (!reviewSummary) {
    return "";
  }
  const statusText = summarizeGraphReviewSummary(reviewSummary);
  const details = [
    reviewSummary.contradictionCount ? `${formatValue(reviewSummary.contradictionCount)} contradiction${reviewSummary.contradictionCount === 1 ? "" : "s"}` : "",
    reviewSummary.impactCount ? `${formatValue(reviewSummary.impactCount)} impact${reviewSummary.impactCount === 1 ? "" : "s"}` : "",
    reviewSummary.pendingCount ? `${formatValue(reviewSummary.pendingCount)} pending` : "",
  ].filter(Boolean).join(" | ");
  return `
    <div class="graph-column-review-summary ${escapeHtml(reviewSummary.status || "reviewed")}">
      <span>${escapeHtml(statusText)}</span>
      ${details ? `<span class="hint">${escapeHtml(details)}</span>` : ""}
      <button
        class="text-button graph-review-inline-link"
        type="button"
        data-graph-open-review-field="${escapeHtml(fieldId || "")}"
        data-graph-node-id="${escapeHtml(nodeId)}"
        data-review-unit-key="${escapeHtml(reviewSummary.reviewUnitKey || "")}"
      >Open</button>
    </div>
  `;
}

function renderExpandedGraphDataTable(node, structureOverlay = null) {
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
              ${pageState.items.map(({ column, index }) => renderExpandedDataColumn(node, column, index, { showExtraDetails, editMode, structureOverlay })).join("")}
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
  const fieldReview = getGraphFieldReviewSummary(column.id, options.structureOverlay);
  const connectLabel = state.pendingColumnLinkRef === columnRef
    ? "Cancel"
    : state.pendingColumnLinkRef
      ? "Connect here"
      : "Connect";
  return `
    <tr class="graph-table-row ${getGraphColumnLineageClass(columnRef)} ${getLineageSeedColors(columnRef).length ? "lineage-active" : ""} ${state.pendingColumnLinkRef === columnRef ? "graph-link-pending" : ""} ${isColumnPicked(columnRef) ? "picked" : ""} ${fieldReview?.status === "review-required" ? "review-required" : fieldReview?.status === "warning" ? "review-warning" : fieldReview?.status === "reviewed" ? "reviewed" : ""} ${fieldReview?.reviewUnitKey === state.structureActiveReviewUnitKey ? "active-review-target" : ""}" data-binding-ref="${escapeHtml(columnRef)}" ${getLineageStyleAttribute(columnRef)}>
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
        ${renderGraphFieldReviewHint(node.id, column.id, fieldReview)}
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
        <div class="graph-node-summary">work items: ${items.length}</div>
        <div class="row-actions">
          ${editMode ? `<button class="text-button" type="button" data-graph-work-item-add="${escapeHtml(node.id)}">Add task</button>` : ""}
        </div>
      </div>
      <div class="graph-work-list">
        ${items.length ? items.map((item, index) => renderGraphWorkItem(node.id, item, index)).join("") : "<div class=\"graph-node-column\">No execution tasks or notes yet.</div>"}
      </div>
    </div>
  `;
}

function renderGraphWorkItem(nodeId, item, index) {
  const editMode = state.interactionMode === "edit";
  const listValue = (value) => (value || []).join(", ");
  if (!editMode) {
    return `
      <div class="graph-work-item">
        <div class="column-head">
          <div class="column-main">${escapeHtml(item.title || `Task ${index + 1}`)}</div>
          <div class="chip-row">
            <span class="status-pill ${escapeHtml(item.status === "blocked" ? "broken" : item.status === "in_progress" ? "warning" : item.status === "done" ? "healthy" : "")}">${escapeHtml(item.status || "todo")}</span>
            <span class="tag-chip">${escapeHtml(item.role || "builder")}</span>
            ${item.exploratory ? '<span class="tag-chip">exploratory</span>' : ""}
          </div>
        </div>
        ${item.summary ? `<div class="column-meta">${escapeHtml(item.summary)}</div>` : ""}
      </div>
    `;
  }
  return `
    <div class="graph-work-item">
      <div class="form-grid compact">
        <label class="form-field form-field-full">
          Title
          <input class="graph-inline-input" data-graph-work-item-index="${index}" data-graph-work-item-field="title" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(item.title || "")}" placeholder="Implement one focused task" />
        </label>
        <label class="form-field">
          Role
          <input class="graph-inline-input" data-graph-work-item-index="${index}" data-graph-work-item-field="role" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(item.role || "builder")}" placeholder="builder" />
        </label>
        <label class="form-field">
          Status
          <select class="graph-inline-select" data-graph-work-item-index="${index}" data-graph-work-item-field="status" data-graph-node-id="${escapeHtml(nodeId)}">
            ${["todo", "in_progress", "blocked", "done", "cancelled"].map((value) => `<option value="${value}" ${value === (item.status || "todo") ? "selected" : ""}>${value}</option>`).join("")}
          </select>
        </label>
        <label class="form-field form-field-full">
          Linked refs
          <input class="graph-inline-input" data-graph-work-item-index="${index}" data-graph-work-item-field="linked_refs" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(listValue(item.linked_refs))}" placeholder="data:market_signals, field.analytics_market_signals.pricing_score" />
        </label>
        <label class="form-field form-field-full">
          Decision ids
          <input class="graph-inline-input" data-graph-work-item-index="${index}" data-graph-work-item-field="decision_ids" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(listValue(item.decision_ids))}" placeholder="decision.market_snapshot" />
        </label>
        <label class="form-field">
          Depends on
          <input class="graph-inline-input" data-graph-work-item-index="${index}" data-graph-work-item-field="depends_on" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(listValue(item.depends_on))}" placeholder="task.other" />
        </label>
        <label class="form-field">
          Checks
          <input class="graph-inline-input" data-graph-work-item-index="${index}" data-graph-work-item-field="acceptance_check_ids" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(listValue(item.acceptance_check_ids))}" placeholder="check.snapshot.delivery" />
        </label>
        <label class="form-field">
          Blockers
          <input class="graph-inline-input" data-graph-work-item-index="${index}" data-graph-work-item-field="blocker_ids" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(listValue(item.blocker_ids))}" placeholder="blocker.snapshot.confirmation" />
        </label>
        <label class="form-field">
          Exploratory
          <input type="checkbox" data-graph-work-item-index="${index}" data-graph-work-item-field="exploratory" data-graph-node-id="${escapeHtml(nodeId)}" ${item.exploratory ? "checked" : ""} />
        </label>
        ${item.exploratory ? `
          <label class="form-field form-field-full">
            Exploration goal
            <input class="graph-inline-input" data-graph-work-item-index="${index}" data-graph-work-item-field="exploration_goal" data-graph-node-id="${escapeHtml(nodeId)}" value="${escapeHtml(item.exploration_goal || "")}" placeholder="Clarify unknown schema or dependency" />
          </label>
        ` : ""}
        <label class="form-field form-field-full">
          Summary
          <textarea class="graph-inline-input graph-inline-note" data-graph-work-item-index="${index}" data-graph-work-item-field="text" data-graph-node-id="${escapeHtml(nodeId)}" placeholder="Concise execution summary">${escapeHtml(item.summary || "")}</textarea>
        </label>
      </div>
      <button class="inline-button danger" type="button" data-graph-work-item-remove="${escapeHtml(nodeId)}:${index}">Remove</button>
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

