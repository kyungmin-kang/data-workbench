// Extracted graph editing, binding, and in-canvas authoring helpers.

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

  if (target.dataset.focusExecutionNode) {
    focusExecutionForNode(target.dataset.focusExecutionNode, {
      view: target.dataset.focusExecutionView || "all",
      hideCompleted: target.dataset.focusExecutionHideCompleted === "true",
    });
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
    const field = target.dataset.graphWorkItemField;
    const normalizedField = field === "text" ? "summary" : field;
    if (["linked_refs", "decision_ids", "depends_on", "acceptance_check_ids", "blocker_ids"].includes(normalizedField)) {
      item[normalizedField] = parseReferenceList(target.value);
    } else if (target instanceof HTMLInputElement && target.type === "checkbox") {
      item[normalizedField] = target.checked;
      if (!target.checked && normalizedField === "exploratory") {
        item.exploration_goal = "";
      }
      render();
    } else {
      item[normalizedField] = target.value;
    }
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
  getNodeWorkItems(node).push({
    title: "",
    role: "builder",
    status: node.work_status || "todo",
    linked_refs: [node.id],
    decision_ids: [],
    depends_on: [],
    acceptance_check_ids: [],
    blocker_ids: [],
    summary: "",
    exploratory: false,
    exploration_goal: "",
  });
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
    title: "Remove task",
    message: `Remove this work item from ${node.label}?`,
    onConfirm: () => {
      recordGraphHistory();
      items.splice(index, 1);
      state.needsAutoLayout = true;
      markDirty(true);
      render();
      setStatus("Task removed", `${node.label} was updated locally.`);
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

  const focusExecutionButton = target.closest("[data-focus-execution-node]");
  if (focusExecutionButton instanceof HTMLElement) {
    event.stopPropagation();
    focusExecutionForNode(focusExecutionButton.dataset.focusExecutionNode || "", {
      view: focusExecutionButton.dataset.focusExecutionView || "all",
      hideCompleted: focusExecutionButton.dataset.focusExecutionHideCompleted === "true",
    });
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

function renderReviewActionModal() {
  if (!reviewActionModal) {
    return;
  }
  const confirmState = state.reviewActionConfirm;
  reviewActionModal.hidden = !confirmState;
  if (!confirmState) {
    reviewActionModalNote.value = "";
    reviewActionModalSummary.innerHTML = "";
    return;
  }
  reviewActionModalTitle.textContent = confirmState.title || "Review changes";
  reviewActionModalMessage.textContent = confirmState.requireNote
    ? `${confirmState.message || "Confirm this review decision."} Add a short reviewer note before confirming.`
    : (confirmState.message || "Confirm this review decision.");
  reviewActionModalSummary.innerHTML = (confirmState.summary || [])
    .map((item) => `<div class="review-action-summary-row"><strong>${escapeHtml(item.label || "")}</strong><span>${escapeHtml(item.value || "")}</span></div>`)
    .join("");
  reviewActionModalNote.value = confirmState.note || "";
  reviewActionModalNote.placeholder = confirmState.notePlaceholder || "Optional context for future reviewers";
  reviewActionModalNote.dataset.required = confirmState.requireNote ? "true" : "false";
  reviewActionModalOk.textContent = confirmState.confirmLabel || "Confirm";
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

function queueReviewActionConfirm({
  title,
  message,
  summary = [],
  confirmLabel = "Confirm",
  note = "",
  notePlaceholder = "",
  requireNote = false,
  noteMinLength = 12,
  noteRequirementMessage = "",
  onConfirm,
}) {
  state.reviewActionConfirm = {
    title,
    message,
    summary,
    confirmLabel,
    note,
    notePlaceholder,
    requireNote,
    noteMinLength,
    noteRequirementMessage,
    onConfirm,
  };
  renderReviewActionModal();
}

function cancelReviewActionConfirm() {
  state.reviewActionConfirm = null;
  renderReviewActionModal();
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

async function confirmReviewAction() {
  if (!state.reviewActionConfirm) {
    return;
  }
  const pending = state.reviewActionConfirm;
  const note = String(reviewActionModalNote.value || "").trim();
  if (pending.requireNote && note.length < (pending.noteMinLength || 0)) {
    setStatus(
      "Reviewer note required",
      pending.noteRequirementMessage || `Add at least ${formatValue(pending.noteMinLength || 0)} characters so future reviewers understand this decision.`
    );
    reviewActionModalNote.focus();
    return;
  }
  state.reviewActionConfirm = null;
  renderReviewActionModal();
  await pending.onConfirm(note);
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
  if (target.dataset.modalClose === "review-action") {
    cancelReviewActionConfirm();
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

