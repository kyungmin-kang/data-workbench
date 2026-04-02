// Extracted project discovery/profile rendering and onboarding workflow helpers.

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
  const filteredDataAssets = filterProjectAssets(dataAssets);
  const filteredApiHints = filterProjectHints(apiHints, (hint) => [
    hint.id,
    hint.label,
    hint.route,
    hint.file,
    hint.detected_from,
    ...(hint.response_fields || []),
  ]);
  const filteredUiHints = filterProjectHints(uiHints, (hint) => [
    hint.id,
    hint.label,
    hint.component,
    hint.file,
    hint.detected_from,
    ...(hint.api_routes || []),
    ...(hint.used_fields || []),
  ]);
  const filteredSqlHints = filterProjectHints(sqlHints, (hint) => [
    hint.id,
    hint.label,
    hint.relation,
    hint.object_type,
    hint.file,
    hint.detected_from,
    ...((hint.fields || []).map((field) => field.name)),
    ...(hint.upstream_relations || []),
  ]);
  const filteredOrmHints = filterProjectHints(ormHints, (hint) => [
    hint.id,
    hint.label,
    hint.relation,
    hint.object_type,
    hint.file,
    hint.detected_from,
    ...((hint.fields || []).map((field) => field.name)),
    ...(hint.upstream_relations || []),
  ]);
  const selectedPaths = new Set(state.selectedProjectImports || []);
  const selectedApiHints = new Set(state.selectedProjectApiHints || []);
  const selectedUiHints = new Set(state.selectedProjectUiHints || []);
  const selectedSqlHints = new Set(state.selectedProjectSqlHints || []);
  const selectedOrmHints = new Set(state.selectedProjectOrmHints || []);
  const selectableCount = dataAssets.filter((asset) => asset.suggested_import).length;
  const bootstrapCount = (
    (state.projectBootstrapOptions.assets ? selectedPaths.size : 0)
    + (state.projectBootstrapOptions.apiHints ? (selectedApiHints.size || apiHints.length) : 0)
    + (state.projectBootstrapOptions.uiHints ? (selectedUiHints.size || uiHints.length) : 0)
    + (state.projectBootstrapOptions.sqlHints ? (selectedSqlHints.size || sqlHints.length) : 0)
    + (state.projectBootstrapOptions.ormHints ? (selectedOrmHints.size || ormHints.length) : 0)
  );
  const bootstrapSummary = describeBootstrapScope({
    selectableCount,
    selectedAssetCount: selectedPaths.size,
    apiHintCount: apiHints.length,
    selectedApiHintCount: selectedApiHints.size,
    uiHintCount: uiHints.length,
    selectedUiHintCount: selectedUiHints.size,
    sqlHintCount: sqlHints.length,
    selectedSqlHintCount: selectedSqlHints.size,
    ormHintCount: ormHints.length,
    selectedOrmHintCount: selectedOrmHints.size,
  });
  projectProfileSummary.innerHTML = `
    ${renderProjectWizardNav()}
    ${state.projectWizardStep === 1 ? renderProjectWizardScopeStep(profile, summary, bootstrapSummary, bootstrapCount) : ""}
    ${state.projectWizardStep === 2 ? renderProjectWizardAssetsStep(dataAssets, filteredDataAssets, selectedPaths, selectableCount) : ""}
    ${state.projectWizardStep === 3 ? renderProjectWizardContractsStep(apiHints, uiHints, sqlHints, ormHints, filteredApiHints, filteredUiHints, filteredSqlHints, filteredOrmHints, selectedApiHints, selectedUiHints, selectedSqlHints, selectedOrmHints) : ""}
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

function getEffectiveProjectRoot() {
  return state.projectProfileOptions.rootPath || state.projectProfile?.root || "";
}

function renderProjectRootStatus(profile, rootInspection) {
  const effectiveRoot = getEffectiveProjectRoot();
  const loadedRoot = profile?.root || "";
  const loadedCache = profile?.cache || {};
  const inspectionCache = rootInspection?.cache || {};
  if (effectiveRoot && loadedRoot && effectiveRoot !== loadedRoot) {
    return `
      <div class="project-root-status-card warning">
        <strong>Loaded results are for a different root</strong>
        <div class="hint">Current root input: ${escapeHtml(effectiveRoot)}</div>
        <div class="hint">Loaded discovery: ${escapeHtml(loadedRoot)}</div>
        <div class="hint">Use <em>Load saved discovery</em> or <em>Run fresh discovery</em> to switch the panel to the current root.</div>
      </div>
    `;
  }
  if (loadedRoot) {
    return `
      <div class="project-root-status-card success">
        <strong>Loaded discovery snapshot</strong>
        <div class="hint">${escapeHtml(loadedRoot)}</div>
        <div class="hint">${escapeHtml(loadedCache.cached ? "Loaded from cache." : "Freshly generated.")}${loadedCache.generated_at ? ` Generated at ${escapeHtml(loadedCache.generated_at)}.` : ""}</div>
        ${inspectionCache.available ? `<div class="hint">Saved discovery confirmed for this root.</div>` : ""}
        ${loadedCache.path ? `<div class="hint">Cache file: ${escapeHtml(loadedCache.path)}</div>` : ""}
      </div>
    `;
  }
  if (rootInspection?.exists && rootInspection?.is_directory) {
    const resolved = rootInspection.using_workspace_default
      ? "Current workspace"
      : rootInspection.resolved_path;
    return `
      <div class="project-root-status-card success">
        <strong>Project root ready</strong>
        <div class="hint">${escapeHtml(resolved)}</div>
        <div class="hint">${escapeHtml(inspectionCache.available
          ? `Saved discovery available${inspectionCache.generated_at ? ` from ${inspectionCache.generated_at}` : ""}.`
          : "No saved discovery exists yet for this exact scope; run discovery to create one.")}</div>
        ${inspectionCache.path ? `<div class="hint">Cache file: ${escapeHtml(inspectionCache.path)}</div>` : ""}
      </div>
    `;
  }
  if (effectiveRoot && rootInspection && (!rootInspection.exists || !rootInspection.is_directory)) {
    return `
      <div class="project-root-status-card warning">
        <strong>Project root not confirmed</strong>
        <div class="hint">${escapeHtml(effectiveRoot)} is not available as a directory from this workbench session.</div>
      </div>
    `;
  }
  return `
    <div class="project-root-status-card">
      <strong>No discovery loaded yet</strong>
      <div class="hint">Paste a project path, check that it resolves, then load saved discovery or run a fresh scan.</div>
    </div>
  `;
}

function renderProjectWizardScopeStep(profile, summary, bootstrapSummary, bootstrapCount) {
  const presets = state.onboardingPresets || [];
  const cacheMeta = profile?.cache || {};
  const discoveryJob = state.projectProfileJob;
  const discoveryRunning = isProjectProfileJobActive(discoveryJob);
  const discoveryEmoji = getProjectDiscoveryPhaseEmoji(discoveryJob?.progress?.phase);
  const effectiveRoot = getEffectiveProjectRoot();
  const rootInspection = state.projectRootInspection;
  const cacheSummary = cacheMeta.generated_at
    ? `${cacheMeta.cached ? "Loaded cached discovery" : "Fresh discovery"} from ${cacheMeta.generated_at}`
    : "";
  const skippedHeavyHintFiles = summary.skipped_heavy_hint_files || 0;
  const notes = Array.isArray(profile?.notes) ? profile.notes : [];
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Step 1. Discovery Scope</h3>
        <div class="row-actions">
          <button class="ghost-button" type="button" data-project-load-cached="true" ${discoveryRunning ? "disabled" : ""}>${discoveryRunning ? `${discoveryEmoji} Discovering...` : "Load saved discovery"}</button>
          <button class="ghost-button" type="button" data-project-rescan="true" ${discoveryRunning ? "disabled" : ""}>Run fresh discovery</button>
        </div>
      </div>
      ${renderProjectDiscoveryProgress(discoveryJob)}
      ${renderProjectRootStatus(profile, rootInspection)}
      <div class="form-grid">
        <label class="form-field form-field-full">
          Project root
          <input data-project-profile-root="true" value="${escapeHtml(state.projectProfileOptions.rootPath || "")}" placeholder="${escapeHtml(state.projectProfile?.root || "") || "/path/to/project"}" />
        </label>
        <div class="form-field">
          Root actions
          <div class="row-actions wrap">
            <button class="ghost-button" type="button" data-project-root-check="true">Check root</button>
            <button class="ghost-button" type="button" data-project-root-picker="true">Search directories</button>
          </div>
        </div>
      </div>
      ${effectiveRoot ? `<p class="hint">Current root input: ${escapeHtml(effectiveRoot)}</p>` : ""}
      <div class="chip-row">
        <label class="hint"><input type="checkbox" data-project-profile-option="includeTests" ${state.projectProfileOptions.includeTests ? "checked" : ""} /> include tests</label>
        <label class="hint"><input type="checkbox" data-project-profile-option="includeInternal" ${state.projectProfileOptions.includeInternal ? "checked" : ""} /> include workbench internals</label>
        <label class="hint"><input type="checkbox" data-project-profile-option="agentEnrichAfterScan" ${state.projectProfileOptions.agentEnrichAfterScan ? "checked" : ""} /> agent enrich after scan</label>
      </div>
      <div class="form-grid compact">
        <label class="form-field">
          Asset discovery mode
          <select data-project-profile-select="profilingMode">
            <option value="metadata_only" ${state.projectProfileOptions.profilingMode === "metadata_only" ? "selected" : ""}>metadata only</option>
            <option value="profile_assets" ${state.projectProfileOptions.profilingMode === "profile_assets" ? "selected" : ""}>profile assets eagerly</option>
          </select>
        </label>
        <label class="form-field form-field-full">
          Exclude paths
          <textarea data-project-profile-text="excludePathsText" placeholder="raw&#10;warehouse&#10;tmp">${escapeHtml(state.projectProfileOptions.excludePathsText || "")}</textarea>
        </label>
        <label class="form-field form-field-full">
          Target asset roots
          <textarea data-project-profile-text="assetRootsText" placeholder="data/raw&#10;landing&#10;exports/monthly">${escapeHtml(state.projectProfileOptions.assetRootsText || "")}</textarea>
        </label>
      </div>
      <div class="chip-row">
        <label class="hint"><input type="checkbox" data-project-bootstrap-option="assets" ${state.projectBootstrapOptions.assets ? "checked" : ""} /> bootstrap assets</label>
        <label class="hint"><input type="checkbox" data-project-bootstrap-option="apiHints" ${state.projectBootstrapOptions.apiHints ? "checked" : ""} /> bootstrap API hints</label>
        <label class="hint"><input type="checkbox" data-project-bootstrap-option="uiHints" ${state.projectBootstrapOptions.uiHints ? "checked" : ""} /> bootstrap UI hints</label>
        <label class="hint"><input type="checkbox" data-project-bootstrap-option="sqlHints" ${state.projectBootstrapOptions.sqlHints ? "checked" : ""} /> bootstrap SQL hints</label>
        <label class="hint"><input type="checkbox" data-project-bootstrap-option="ormHints" ${state.projectBootstrapOptions.ormHints ? "checked" : ""} /> bootstrap ORM hints</label>
      </div>
      <p class="hint">${escapeHtml(bootstrapSummary)}</p>
      <p class="hint">Default huge-repo path: run metadata-only discovery first, keep the main scan narrow, add targeted asset roots for raw files, then profile only selected assets.</p>
      ${cacheSummary ? `<p class="hint">${escapeHtml(cacheSummary)}</p>` : ""}
      ${skippedHeavyHintFiles ? `<p class="hint">Skipped ${formatValue(skippedHeavyHintFiles)} oversized or generated files during hint discovery to keep large-repo scans responsive.</p>` : ""}
      ${notes.map((note) => `<p class="hint">${escapeHtml(note)}</p>`).join("")}
      ${profile ? `
        <div class="meta-grid">
          <div><strong>Files walked</strong><br>${formatValue(summary.files_scanned || 0)}</div>
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

function renderProjectWizardAssetsStep(dataAssets, filteredDataAssets, selectedPaths, selectableCount) {
  const PAGE_SIZE = 50;
  const visibleSelectableCount = filteredDataAssets.filter((asset) => asset.suggested_import).length;
  const assetPage = paginateProjectDiscovery(filteredDataAssets, "assets", PAGE_SIZE);
  const assetProfileJob = state.projectAssetProfileJob;
  const assetProfileRunning = isProjectProfileJobActive(assetProfileJob);
  return `
    <div class="section">
      <div class="section-actions">
        <h3>Step 2. Data Assets <span class="hint">(${filteredDataAssets.length} shown / ${dataAssets.length} total)</span></h3>
        <div class="row-actions">
          <span class="hint">${formatValue(selectedPaths.size)} selected / ${formatValue(selectableCount)} suggested</span>
          <button class="ghost-button" type="button" data-project-select-all="true">Select visible suggested</button>
          <button class="ghost-button" type="button" data-project-clear-selection="true">Clear selection</button>
          <button class="ghost-button" type="button" data-project-profile-assets="selected" ${selectedPaths.size ? "" : "disabled"}>${assetProfileRunning ? "Profiling..." : "Profile selected now"}</button>
          <button class="ghost-button" type="button" data-project-import-selected="true" ${selectedPaths.size ? "" : "disabled"}>Import selected now</button>
        </div>
      </div>
      ${renderProjectAssetProfileProgress(assetProfileJob)}
      <div class="form-grid compact">
        <label class="form-field">
          Filter assets
          <input data-project-filter="assetsQuery" value="${escapeHtml(state.projectProfileFilters.assetsQuery || "")}" placeholder="path, column, source label" />
        </label>
        <label class="form-field">
          Status
          <select data-project-filter="assetsStatus">
            <option value="all" ${state.projectProfileFilters.assetsStatus === "all" ? "selected" : ""}>All assets</option>
            <option value="suggested" ${state.projectProfileFilters.assetsStatus === "suggested" ? "selected" : ""}>Suggested only</option>
            <option value="profiled" ${state.projectProfileFilters.assetsStatus === "profiled" ? "selected" : ""}>Profiled</option>
            <option value="sampled_profile" ${state.projectProfileFilters.assetsStatus === "sampled_profile" ? "selected" : ""}>Sampled</option>
            <option value="schema_only" ${state.projectProfileFilters.assetsStatus === "schema_only" ? "selected" : ""}>Schema only</option>
            <option value="unknown" ${state.projectProfileFilters.assetsStatus === "unknown" ? "selected" : ""}>Unknown</option>
          </select>
        </label>
      </div>
      ${(state.projectProfileFilters.assetsQuery || state.projectProfileFilters.assetsStatus !== "all")
        ? `<p class="hint">Showing ${formatValue(filteredDataAssets.length)} asset${filteredDataAssets.length === 1 ? "" : "s"} from ${formatValue(dataAssets.length)} total. ${formatValue(visibleSelectableCount)} visible asset${visibleSelectableCount === 1 ? "" : "s"} can be imported.</p>`
        : ""}
      <div class="column-list">
        ${assetPage.items.length ? assetPage.items.map((asset) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(asset.path || "unknown")}</div>
              <div class="row-actions">
                ${asset.suggested_import ? `<label class="hint"><input type="checkbox" data-project-select="${escapeHtml(asset.path)}" ${selectedPaths.has(asset.path) ? "checked" : ""} /> select</label>` : ""}
                <span class="pill">${escapeHtml(asset.format || "unknown")}</span>
                <span class="pill">${escapeHtml(asset.profile_status || "schema_only")}</span>
                ${(asset.member_count || 0) > 1 ? `<span class="pill">${formatValue(asset.member_count)} files</span>` : ""}
                ${asset.suggested_import ? `<button class="text-button" type="button" data-project-import-load="${escapeHtml(asset.path)}">Load to import</button>` : ""}
                <button class="ghost-button" type="button" data-project-profile-assets="${escapeHtml(asset.path)}">Profile now</button>
                ${asset.suggested_import ? `<button class="ghost-button" type="button" data-project-import-run="${escapeHtml(asset.path)}">Import now</button>` : ""}
              </div>
            </div>
            <div class="column-meta">
              rows: ${formatValue(asset.row_count)} | columns: ${asset.columns?.map((column) => `${column.name} (${column.data_type})`).join(", ") || "missing"}
            </div>
            ${asset.profiling_skipped_reason ? `<div class="column-meta">profiling: ${escapeHtml(asset.profiling_skipped_reason)}</div>` : ""}
            ${asset.group_reason ? `<div class="column-meta">collection: ${escapeHtml(asset.group_reason)}${asset.collection_key ? ` · ${escapeHtml(asset.collection_key)}` : ""}</div>` : ""}
            ${asset.suggested_import ? `<div class="column-meta">suggested: ${escapeHtml(asset.suggested_import.source_label)} -> ${escapeHtml(asset.suggested_import.data_label)}</div>` : ""}
          </div>
        `).join("") : "<p>No data assets were detected. Run discovery again or widen the scope filters.</p>"}
      </div>
      ${renderProjectDiscoveryPager("assets", assetPage.page, assetPage.totalPages)}
    </div>
  `;
}

function renderProjectWizardContractsStep(
  apiHints,
  uiHints,
  sqlHints,
  ormHints,
  filteredApiHints,
  filteredUiHints,
  filteredSqlHints,
  filteredOrmHints,
  selectedApiHints,
  selectedUiHints,
  selectedSqlHints,
  selectedOrmHints,
) {
  const PAGE_SIZE = 25;
  const apiPage = paginateProjectDiscovery(filteredApiHints, "api", PAGE_SIZE);
  const uiPage = paginateProjectDiscovery(filteredUiHints, "ui", PAGE_SIZE);
  const sqlPage = paginateProjectDiscovery(filteredSqlHints, "sql", PAGE_SIZE);
  const ormPage = paginateProjectDiscovery(filteredOrmHints, "orm", PAGE_SIZE);
  return `
    <div class="section">
      <div class="form-grid compact">
        <label class="form-field form-field-full">
          Filter contracts and structure hints
          <input data-project-filter="contractsQuery" value="${escapeHtml(state.projectProfileFilters.contractsQuery || "")}" placeholder="route, component, relation, field, file" />
        </label>
      </div>
      ${state.projectProfileFilters.contractsQuery
        ? `<p class="hint">Showing ${formatValue(filteredApiHints.length)} API, ${formatValue(filteredUiHints.length)} UI, ${formatValue(filteredSqlHints.length)} SQL, and ${formatValue(filteredOrmHints.length)} ORM hints that match the current filter.</p>`
        : ""}
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Step 3. API Contracts <span class="hint">(${filteredApiHints.length} shown / ${apiHints.length} total)</span></h3>
        <div class="row-actions">
          <span class="hint">${formatValue(selectedApiHints.size)} selected / ${formatValue(apiHints.length)}</span>
          <button class="ghost-button" type="button" data-project-api-select-all="true">Select visible</button>
          <button class="ghost-button" type="button" data-project-api-clear="true">Clear</button>
        </div>
      </div>
      <div class="column-list">
        ${apiPage.items.length ? apiPage.items.map((hint) => `
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
      ${renderProjectDiscoveryPager("api", apiPage.page, apiPage.totalPages)}
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Step 3. UI Consumers <span class="hint">(${filteredUiHints.length} shown / ${uiHints.length} total)</span></h3>
        <div class="row-actions">
          <span class="hint">${formatValue(selectedUiHints.size)} selected / ${formatValue(uiHints.length)}</span>
          <button class="ghost-button" type="button" data-project-ui-select-all="true">Select visible</button>
          <button class="ghost-button" type="button" data-project-ui-clear="true">Clear</button>
        </div>
      </div>
      <div class="column-list">
        ${uiPage.items.length ? uiPage.items.map((hint) => `
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
      ${renderProjectDiscoveryPager("ui", uiPage.page, uiPage.totalPages)}
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Step 3. SQL Structure <span class="hint">(${filteredSqlHints.length} shown / ${sqlHints.length} total)</span></h3>
        <div class="row-actions">
          <span class="hint">${formatValue(selectedSqlHints.size)} selected / ${formatValue(sqlHints.length)}</span>
          <button class="ghost-button" type="button" data-project-sql-select-all="true">Select visible</button>
          <button class="ghost-button" type="button" data-project-sql-clear="true">Clear</button>
        </div>
      </div>
      <div class="column-list">
        ${sqlPage.items.length ? sqlPage.items.map((hint) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(hint.relation || hint.label || "unknown relation")}</div>
              <div class="row-actions">
                <label class="hint"><input type="checkbox" data-project-sql-select="${escapeHtml(hint.id)}" ${selectedSqlHints.has(hint.id) ? "checked" : ""} /> select</label>
                <span class="pill">${escapeHtml(hint.object_type || "table")}</span>
                <button class="ghost-button" type="button" data-project-sql-create="${escapeHtml(hint.id)}">Create or update</button>
              </div>
            </div>
            <div class="column-meta">${escapeHtml(hint.file || "unknown file")} | ${escapeHtml(hint.detected_from || "sql scan")}</div>
            <div class="column-meta">${renderHintFieldSummary((hint.fields || []).map((field) => field.name))}</div>
            ${hint.upstream_relations?.length ? `<div class="column-meta">upstream: ${escapeHtml(hint.upstream_relations.join(", "))}</div>` : ""}
          </div>
        `).join("") : "<p>No SQL structure hints detected.</p>"}
      </div>
      ${renderProjectDiscoveryPager("sql", sqlPage.page, sqlPage.totalPages)}
    </div>
    <div class="section">
      <div class="section-actions">
        <h3>Step 3. ORM Structure <span class="hint">(${filteredOrmHints.length} shown / ${ormHints.length} total)</span></h3>
        <div class="row-actions">
          <span class="hint">${formatValue(selectedOrmHints.size)} selected / ${formatValue(ormHints.length)}</span>
          <button class="ghost-button" type="button" data-project-orm-select-all="true">Select visible</button>
          <button class="ghost-button" type="button" data-project-orm-clear="true">Clear</button>
        </div>
      </div>
      <div class="column-list">
        ${ormPage.items.length ? ormPage.items.map((hint) => `
          <div class="column-row">
            <div class="column-head">
              <div class="column-main">${escapeHtml(hint.relation || hint.label || "unknown model")}</div>
              <div class="row-actions">
                <label class="hint"><input type="checkbox" data-project-orm-select="${escapeHtml(hint.id)}" ${selectedOrmHints.has(hint.id) ? "checked" : ""} /> select</label>
                <span class="pill">${escapeHtml(hint.object_type || "table")}</span>
                <button class="ghost-button" type="button" data-project-orm-create="${escapeHtml(hint.id)}">Create or update</button>
              </div>
            </div>
            <div class="column-meta">${escapeHtml(hint.file || "unknown file")} | ${escapeHtml(hint.detected_from || "orm scan")}</div>
            <div class="column-meta">${renderHintFieldSummary((hint.fields || []).map((field) => field.name))}</div>
            ${hint.upstream_relations?.length ? `<div class="column-meta">upstream: ${escapeHtml(hint.upstream_relations.join(", "))}</div>` : ""}
          </div>
        `).join("") : "<p>No ORM structure hints detected.</p>"}
      </div>
      ${renderProjectDiscoveryPager("orm", ormPage.page, ormPage.totalPages)}
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
  if (!(target instanceof HTMLElement)) {
    return;
  }
  if (target.dataset.structurePath) {
    state.structureDraft[target.dataset.structurePath] = coerceValue(target);
    return;
  }
  if (target.dataset.structurePref === "reviewer_identity") {
    state.structureReviewerIdentity = String(coerceValue(target) || "user").trim() || "user";
    persistStructureReviewPreferences();
    render();
    return;
  }
  if (target.dataset.structurePref === "selected_preset_id") {
    state.selectedStructureReviewPresetId = String(coerceValue(target) || "");
    persistStructureReviewPreferences();
    return;
  }
  if (target.dataset.structurePref === "preset_draft_name") {
    state.structureReviewPresetDraftName = String(coerceValue(target) || "");
    return;
  }
  if (target.dataset.structureWorkflowField) {
    state.structureWorkflowDraft[target.dataset.structureWorkflowField] = String(coerceValue(target) || "");
  }
}

function handleStructureClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const actionTarget = target.closest("[data-structure-action]");
  if (!(actionTarget instanceof HTMLElement)) {
    const reviewUnitTarget = target.closest("[data-structure-review-unit-key]");
    if (reviewUnitTarget instanceof HTMLElement && reviewUnitTarget.dataset.structureReviewUnitKey) {
      setActiveStructureReviewUnit(reviewUnitTarget.dataset.structureReviewUnitKey, { scroll: false });
    }
    return;
  }
  const reviewUnitTarget = actionTarget.closest("[data-structure-review-unit-key]");
  if (reviewUnitTarget instanceof HTMLElement && reviewUnitTarget.dataset.structureReviewUnitKey) {
    state.structureActiveReviewUnitKey = reviewUnitTarget.dataset.structureReviewUnitKey;
  }
  const action = actionTarget.dataset.structureAction;
  if (action === "export-yaml" || action === "load-current-yaml") {
    loadCurrentStructureYaml();
    return;
  }
  if (action === "import-yaml") {
    importStructureYaml();
    return;
  }
  if (action === "scan-quick") {
    state.structureDraft.role = actionTarget.dataset.structureRole || "scout";
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
  if (action === "open-review-inbox") {
    void toggleReviewDrawer(true);
    return;
  }
  if (action === "set-bundle-inbox-filter") {
    state.structureBundleInboxFilter = actionTarget.dataset.bundleInboxFilter || "needs_attention";
    persistStructureReviewPreferences();
    render();
    return;
  }
  if (action === "set-assignment-filter") {
    state.structureAssignmentFilter = actionTarget.dataset.assignmentFilter || "all";
    persistStructureReviewPreferences();
    render();
    return;
  }
  if (action === "set-review-filter") {
    state.structureReviewUnitFilter = actionTarget.dataset.reviewFilter || "review_required";
    persistStructureReviewPreferences();
    render();
    return;
  }
  if (action === "toggle-low-confidence") {
    state.structureShowLowConfidence = !state.structureShowLowConfidence;
    persistStructureReviewPreferences();
    render();
    return;
  }
  if (action === "toggle-minor-impacts") {
    state.structureShowMinorImpacts = !state.structureShowMinorImpacts;
    persistStructureReviewPreferences();
    render();
    return;
  }
  if (action === "toggle-non-material") {
    state.structureShowNonMaterial = !state.structureShowNonMaterial;
    persistStructureReviewPreferences();
    render();
    return;
  }
  if (action === "open-bundle") {
    loadStructureBundle(actionTarget.dataset.bundleId || "");
    return;
  }
  if (action === "review-patch") {
    openStructurePatchReviewConfirm(
      actionTarget.dataset.bundleId || "",
      actionTarget.dataset.patchId ? [actionTarget.dataset.patchId] : [],
      actionTarget.dataset.decision || "deferred"
    );
    return;
  }
  if (action === "review-patch-batch") {
    openStructurePatchReviewConfirm(
      actionTarget.dataset.bundleId || "",
      parsePatchIdList(actionTarget.dataset.patchIds || ""),
      actionTarget.dataset.decision || "deferred"
    );
    return;
  }
  if (action === "review-contradiction") {
    openStructureContradictionReviewConfirm(
      actionTarget.dataset.bundleId || "",
      actionTarget.dataset.contradictionId || "",
      actionTarget.dataset.decision || "deferred"
    );
    return;
  }
  if (action === "preview-rebase") {
    previewStructureBundleRebase(actionTarget.dataset.bundleId || "");
    return;
  }
  if (action === "apply-review-preset") {
    const preset = getSelectedStructureReviewPreset();
    if (!preset) {
      setStatus("Preset apply failed", "Choose a saved review preset first.");
      return;
    }
    applyStructureReviewPreset(preset);
    setStatus("Review preset applied", `${preset.name} is now active in the review inbox.`);
    return;
  }
  if (action === "save-review-preset") {
    saveStructureReviewPreset();
    return;
  }
  if (action === "delete-review-preset") {
    deleteStructureReviewPreset();
    return;
  }
  if (action === "save-bundle-workflow") {
    updateStructureBundleWorkflow(actionTarget.dataset.bundleId || "");
    return;
  }
  if (action === "assign-bundle-to-me") {
    const reviewer = getCurrentStructureReviewerIdentity();
    state.structureWorkflowDraft.assignedReviewer = reviewer;
    updateStructureBundleWorkflow(actionTarget.dataset.bundleId || "", {
      assignedReviewer: reviewer,
      note: `Assigned to ${reviewer}.`,
    });
    return;
  }
  if (action === "set-triage-state") {
    const triageState = actionTarget.dataset.triageState || "new";
    state.structureWorkflowDraft.triageState = triageState;
    updateStructureBundleWorkflow(actionTarget.dataset.bundleId || "", {
      triageState,
      triageNote: state.structureWorkflowDraft.triageNote || "",
      note: state.structureWorkflowDraft.triageNote || `Marked ${triageState.replace(/_/g, " ")}.`,
    });
    return;
  }
  if (action === "merge-bundle") {
    mergeStructureBundle(actionTarget.dataset.bundleId || "");
    return;
  }
  if (action === "rebase-bundle") {
    rebaseStructureBundle(actionTarget.dataset.bundleId || "");
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
  render();
  if (options.updateStatus) {
    setStatus("Bundles refreshed", `${formatValue((payload.bundles || []).length)} review bundle${(payload.bundles || []).length === 1 ? "" : "s"} available.`);
  }
}

async function updateStructureBundleWorkflow(bundleId, options = {}) {
  if (!bundleId) {
    return;
  }
  const reviewer = options.updatedBy || getCurrentStructureReviewerIdentity();
  const body = {
    bundle_owner: options.bundleOwner ?? state.structureWorkflowDraft.bundleOwner ?? "",
    assigned_reviewer: options.assignedReviewer ?? state.structureWorkflowDraft.assignedReviewer ?? "",
    triage_state: options.triageState ?? state.structureWorkflowDraft.triageState ?? "new",
    triage_note: options.triageNote ?? state.structureWorkflowDraft.triageNote ?? "",
    updated_by: reviewer,
    note: options.note || "",
  };
  const response = await fetch(`/api/structure/bundles/${encodeURIComponent(bundleId)}/workflow`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Workflow update failed", payload.detail || payload.error || "Unable to update bundle ownership or triage.");
    return;
  }
  state.selectedStructureBundle = payload.bundle || null;
  state.selectedStructureBundleId = payload.bundle?.bundle_id || bundleId;
  syncStructureWorkflowDraft(payload.bundle || null);
  await refreshStructureBundles();
  if ((payload.updated_fields || []).length) {
    setStatus("Workflow updated", `${formatValue(payload.updated_fields.length)} workflow field${payload.updated_fields.length === 1 ? "" : "s"} updated for ${bundleId}.`);
  } else {
    setStatus("Workflow already current", "Ownership and triage already matched the current draft.");
  }
}

async function loadStructureBundle(bundleId, options = {}) {
  if (!bundleId) {
    return;
  }
  const response = await fetch(`/api/structure/bundles/${encodeURIComponent(bundleId)}`);
  const payload = await response.json();
  if (!response.ok) {
    if (!options.silent) {
      setStatus("Bundle load failed", payload.detail || payload.error || "Unable to load the selected review bundle.");
    }
    return;
  }
  state.selectedStructureBundleId = bundleId;
  state.selectedStructureBundle = payload.bundle || null;
  state.structureActiveReviewUnitKey = "";
  syncStructureWorkflowDraft(payload.bundle || null);
  render();
  if (!options.silent) {
    setStatus("Bundle loaded", `${bundleId} is ready for review.`);
  }
}

async function runStructureScan() {
  const profileToken = state.projectProfile?.cache?.token || null;
  const docPaths = parseMultilineList(state.structureDraft.docPathsText);
  const selectedPaths = parseMultilineList(state.structureDraft.selectedPathsText);
  void primeCompletionNotifications();
  setStatus(
    "Scanning structure...",
    profileToken
      ? `Running a ${state.structureDraft.role} pass across the current root, docs, and selected paths using the current discovery snapshot.`
      : `Running a ${state.structureDraft.role} pass across the current root, docs, and selected paths.`
  );
  const response = await fetch("/api/structure/scan", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      role: state.structureDraft.role || "scout",
      scope: state.structureDraft.scope || "changed",
      include_tests: state.projectProfileOptions.includeTests,
      include_internal: state.projectProfileOptions.includeInternal,
      profile_token: profileToken,
      root_path: state.projectProfileOptions.rootPath || "",
      profiling_mode: state.projectProfileOptions.profilingMode || "metadata_only",
      exclude_paths: parseMultilineList(state.projectProfileOptions.excludePathsText),
      doc_paths: docPaths,
      selected_paths: selectedPaths,
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
  state.structureRebasePreviews = {};
  state.structureActiveReviewUnitKey = "";
  syncStructureWorkflowDraft(payload.bundle || null);
  render();
  const detail = `${payload.bundle?.bundle_id || "Review bundle"} is ready for accept / reject / defer review.`;
  setStatus("Bundle created", detail);
  notifyTaskCompletion("Structure scan ready", detail, {
    emoji: "🧪",
    tag: "structure-scan",
  });
  if (state.projectProfileOptions.agentEnrichAfterScan && payload.bundle?.bundle_id) {
    await loadExecutionWorkflow("workbench-scout", {
      bundleId: payload.bundle.bundle_id,
      rootPath: state.projectProfileOptions.rootPath || "",
      docPaths,
      selectedPaths,
    });
  }
}

async function reviewStructurePatch(bundleId, patchId, decision) {
  return reviewStructurePatches(bundleId, patchId ? [patchId] : [], decision);
}

async function reviewStructurePatches(bundleId, patchIds, decision, options = {}) {
  const normalizedIds = Array.from(new Set((patchIds || []).filter(Boolean)));
  if (!bundleId || !normalizedIds.length) {
    return;
  }
  const endpoint = normalizedIds.length === 1
    ? `/api/structure/bundles/${encodeURIComponent(bundleId)}/review`
    : `/api/structure/bundles/${encodeURIComponent(bundleId)}/review-batch`;
  const body = normalizedIds.length === 1
    ? { patch_id: normalizedIds[0], decision, reviewed_by: options.reviewedBy || getCurrentStructureReviewerIdentity(), note: options.note || "" }
    : { patch_ids: normalizedIds, decision, reviewed_by: options.reviewedBy || getCurrentStructureReviewerIdentity(), note: options.note || "" };
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
  state.structureActiveReviewUnitKey = "";
  syncStructureWorkflowDraft(payload.bundle || null);
  applyExecutionPayload(payload);
  await refreshStructureBundles();
  const targetLabel = normalizedIds.length === 1 ? normalizedIds[0] : `${formatValue(normalizedIds.length)} patches`;
  setStatus("Patch review updated", `${targetLabel} ${normalizedIds.length === 1 ? "is" : "are"} now ${decision}.`);
}

async function reviewStructureContradiction(bundleId, contradictionId, decision, options = {}) {
  if (!bundleId || !contradictionId) {
    return;
  }
  const response = await fetch(`/api/structure/bundles/${encodeURIComponent(bundleId)}/review-contradiction`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contradiction_id: contradictionId,
      decision,
      reviewed_by: options.reviewedBy || getCurrentStructureReviewerIdentity(),
      note: options.note || "",
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Contradiction review failed", payload.detail || payload.error || "Unable to update contradiction review state.");
    return;
  }
  state.selectedStructureBundle = payload.bundle || null;
  state.selectedStructureBundleId = payload.bundle?.bundle_id || bundleId;
  state.structureActiveReviewUnitKey = "";
  syncStructureWorkflowDraft(payload.bundle || null);
  applyExecutionPayload(payload);
  await refreshStructureBundles();
  setStatus("Contradiction reviewed", `${contradictionId} is now ${decision}.`);
}

async function previewStructureBundleRebase(bundleId) {
  if (!bundleId) {
    return;
  }
  setStatus("Previewing rebase...", "Comparing the stale bundle against the latest canonical YAML without mutating review state.");
  const response = await fetch(`/api/structure/bundles/${encodeURIComponent(bundleId)}/rebase-preview?preserve_reviews=true`);
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Rebase preview failed", payload.detail || payload.error || "Unable to preview the latest rebase impact for this bundle.");
    return;
  }
  state.structureRebasePreviews[bundleId] = payload.preview || null;
  render();
  setStatus("Rebase preview ready", `${bundleId} now shows transfer and re-review impact before you rebase.`);
}

function parsePatchIdList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildStructureReviewDecisionSummary(bundle, patchIds, decision) {
  const context = buildStructureBundleContext(bundle);
  const patches = (bundle?.patches || []).filter((patch) => patchIds.includes(patch.id));
  const unitLabels = new Set();
  const nodeLabels = new Set();
  const impactMessages = new Set();
  patches.forEach((patch) => {
    const grouping = resolveStructurePatchGrouping(patch, context);
    unitLabels.add(grouping.fieldLabel || grouping.fieldKey || patch.id);
    nodeLabels.add(grouping.nodeLabel || grouping.nodeKey || patch.id);
    collectStructureImpactItemsForTargets(bundle, [patch.field_id, patch.target_id, patch.node_id], context).forEach((item) => {
      if (item.message) {
        impactMessages.add(item.message);
      }
    });
  });
  return [
    { label: "Decision", value: humanizeStructureDecision(decision) },
    { label: "Patches", value: formatValue(patches.length) },
    { label: "Review units", value: formatValue(unitLabels.size) },
    { label: "Nodes touched", value: formatValue(nodeLabels.size) },
    { label: "Consumer impacts", value: formatValue(impactMessages.size) },
  ];
}

function openStructurePatchReviewConfirm(bundleId, patchIds, decision) {
  const normalizedIds = Array.from(new Set((patchIds || []).filter(Boolean)));
  if (!bundleId || !normalizedIds.length) {
    return;
  }
  const bundle = state.selectedStructureBundle;
  if (!bundle || bundle.bundle_id !== bundleId) {
    void reviewStructurePatches(bundleId, normalizedIds, decision);
    return;
  }
  queueReviewActionConfirm({
    title: `${humanizeStructureDecision(decision)} review batch`,
    message: "Confirm this batched review decision before the inbox updates. Add an optional note if future reviewers will need context.",
    summary: buildStructureReviewDecisionSummary(bundle, normalizedIds, decision),
    confirmLabel: humanizeStructureDecision(decision),
    notePlaceholder: decision === "deferred" ? "What follow-up evidence is needed before this unit can move forward?" : "Optional context for future reviewers",
    onConfirm: async (note) => {
      await reviewStructurePatches(bundleId, normalizedIds, decision, { note, reviewedBy: getCurrentStructureReviewerIdentity() });
    },
  });
}

function buildStructureContradictionDecisionSummary(bundle, contradiction) {
  const relatedPatchIds = findStructureRelatedPatches(bundle, contradiction).map((patch) => patch.id);
  const context = buildStructureBundleContext(bundle);
  const impacts = collectStructureImpactItemsForTargets(
    bundle,
    [contradiction.field_id, contradiction.target_id, contradiction.node_id, ...relatedPatchIds],
    context,
  );
  return [
    { label: "Target", value: summarizeStructureContradictionTitle(contradiction, context) },
    { label: "Related patches", value: formatValue(relatedPatchIds.length) },
    { label: "Direct impacts", value: formatValue((contradiction.downstream_impacts || []).length) },
    { label: "Other consumer impacts", value: formatValue(impacts.length) },
  ];
}

function openStructureContradictionReviewConfirm(bundleId, contradictionId, decision) {
  const bundle = state.selectedStructureBundle;
  if (!bundle || bundle.bundle_id !== bundleId) {
    return;
  }
  const contradiction = (bundle.contradictions || []).find((item) => item.id === contradictionId);
  if (!contradiction) {
    return;
  }
  const requireNote = decision === "deferred" || (
    decision === "rejected"
    && (
      (contradiction.downstream_impacts || []).length > 0
      || contradiction.severity === "high"
      || contradiction.severity === "medium"
    )
  );
  queueReviewActionConfirm({
    title: `${humanizeStructureContradictionDecision(decision)} contradiction`,
    message: "This contradiction decision also updates the related patch review state so the bundle stays internally consistent.",
    summary: buildStructureContradictionDecisionSummary(bundle, contradiction),
    confirmLabel: humanizeStructureContradictionDecision(decision),
    requireNote,
    noteMinLength: 12,
    notePlaceholder: requireNote
      ? "Why is this contradiction being deferred or why should canonical stay in place?"
      : "Optional context for future reviewers",
    noteRequirementMessage: decision === "rejected"
      ? "Add a short reason before keeping canonical on a contradiction with downstream impact."
      : "Add a short reason before deferring this contradiction so the next reviewer knows what evidence is missing.",
    onConfirm: async (note) => {
      await reviewStructureContradiction(bundleId, contradictionId, decision, { note, reviewedBy: getCurrentStructureReviewerIdentity() });
    },
  });
}

async function mergeStructureBundle(bundleId) {
  if (!bundleId) {
    return;
  }
  setStatus("Merging bundle...", "Applying accepted patches into the canonical YAML structure memory.");
  const response = await fetch(`/api/structure/bundles/${encodeURIComponent(bundleId)}/merge`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ merged_by: getCurrentStructureReviewerIdentity() }),
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
  state.structureActiveReviewUnitKey = "";
  syncStructureWorkflowDraft(payload.bundle || null);
  applyExecutionPayload(payload);
  state.dirty = false;
  state.needsAutoLayout = true;
  render();
  if (!state.hasManualZoom) {
    fitGraphToViewport();
  }
  setStatus("Bundle merged", `${bundleId} updated the canonical YAML and refreshed the active graph.`);
}

async function rebaseStructureBundle(bundleId) {
  if (!bundleId) {
    return;
  }
  setStatus("Rebasing bundle...", "Re-running the saved scan against the latest canonical YAML and preserving matching review decisions.");
  const response = await fetch(`/api/structure/bundles/${encodeURIComponent(bundleId)}/rebase`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ preserve_reviews: true, rebased_by: getCurrentStructureReviewerIdentity() }),
  });
  const payload = await response.json();
  if (!response.ok) {
    setStatus("Rebase failed", payload.detail || payload.error || "Unable to rebase the selected review bundle.");
    return;
  }
  state.structure = payload.structure || state.structure;
  state.projectProfile = payload.project_profile || state.projectProfile;
  state.selectedStructureBundle = payload.bundle || null;
  state.selectedStructureBundleId = payload.bundle?.bundle_id || "";
  state.structureActiveReviewUnitKey = "";
  applyExecutionPayload(payload);
  syncStructureWorkflowDraft(payload.bundle || null);
  render();
  setStatus(
    "Bundle rebased",
    `${payload.rebased_from_bundle_id || bundleId} -> ${payload.bundle?.bundle_id || "rebased bundle"} with ${formatValue(payload.transferred_review_count || 0)} preserved review decision${(payload.transferred_review_count || 0) === 1 ? "" : "s"}.`
  );
}

function parseMultilineList(value) {
  return (value || "")
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

async function inspectCurrentProjectRoot() {
  const rootPath = getEffectiveProjectRoot();
  try {
    const inspection = await inspectProjectRootPath(rootPath);
    state.projectRootInspection = inspection;
    renderProjectProfile();
    if (inspection?.exists && inspection?.is_directory) {
      setStatus(
        "Project root ready",
        inspection.cache?.available
          ? `Saved discovery found${inspection.cache.generated_at ? ` from ${inspection.cache.generated_at}` : ""}.`
          : "The root exists. No saved discovery exists yet for this scope.",
      );
      return;
    }
    setStatus("Project root not found", "The selected root does not exist or is not a directory.");
  } catch (error) {
    state.projectRootInspection = null;
    renderProjectProfile();
    setStatus("Project root check failed", error.message || "Unable to inspect the selected project root.");
  }
}

function handleProjectProfileClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const wizardStepButton = target.closest("[data-project-wizard-step]");
  if (wizardStepButton instanceof HTMLElement) {
    state.projectWizardStep = Number.parseInt(wizardStepButton.dataset.projectWizardStep || "", 10) || 1;
    renderProjectProfile();
    return;
  }
  const wizardMoveButton = target.closest("[data-project-wizard-move]");
  if (wizardMoveButton instanceof HTMLElement && wizardMoveButton.dataset.projectWizardMove === "back") {
    state.projectWizardStep = Math.max(1, state.projectWizardStep - 1);
    renderProjectProfile();
    return;
  }
  if (wizardMoveButton instanceof HTMLElement && wizardMoveButton.dataset.projectWizardMove === "next") {
    state.projectWizardStep = Math.min(4, state.projectWizardStep + 1);
    renderProjectProfile();
    return;
  }
  const pageButton = target.closest("[data-project-page-key]");
  if (pageButton instanceof HTMLElement && pageButton.dataset.projectPageMove) {
    const pageKey = pageButton.dataset.projectPageKey;
    const currentPage = getProjectProfilePage(pageKey);
    const nextPage = pageButton.dataset.projectPageMove === "prev" ? currentPage - 1 : currentPage + 1;
    setProjectProfilePage(pageKey, nextPage);
    renderProjectProfile();
    return;
  }
  if (target.dataset.projectSelectAll) {
    state.selectedProjectImports = filterProjectAssets(state.projectProfile?.data_assets || [])
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
    state.selectedProjectApiHints = filterProjectHints(
      state.projectProfile?.api_contract_hints || [],
      (hint) => [hint.id, hint.label, hint.route, hint.file, ...(hint.response_fields || [])],
    ).map((hint) => hint.id);
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
    state.selectedProjectUiHints = filterProjectHints(
      state.projectProfile?.ui_contract_hints || [],
      (hint) => [hint.id, hint.label, hint.component, hint.file, ...(hint.api_routes || []), ...(hint.used_fields || [])],
    ).map((hint) => hint.id);
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
  if (target.dataset.projectSqlSelectAll) {
    state.selectedProjectSqlHints = filterProjectHints(
      state.projectProfile?.sql_structure_hints || [],
      (hint) => [hint.id, hint.label, hint.relation, hint.object_type, hint.file, ...(hint.upstream_relations || [])],
    ).map((hint) => hint.id);
    renderProjectProfile();
    setStatus("SQL hints selected", `${state.selectedProjectSqlHints.length} SQL hint${state.selectedProjectSqlHints.length === 1 ? "" : "s"} selected for bootstrap.`);
    return;
  }
  if (target.dataset.projectSqlClear) {
    state.selectedProjectSqlHints = [];
    renderProjectProfile();
    setStatus("SQL selection cleared", "No SQL hints are selected.");
    return;
  }
  if (target.dataset.projectOrmSelectAll) {
    state.selectedProjectOrmHints = filterProjectHints(
      state.projectProfile?.orm_structure_hints || [],
      (hint) => [hint.id, hint.label, hint.relation, hint.object_type, hint.file, ...(hint.upstream_relations || [])],
    ).map((hint) => hint.id);
    renderProjectProfile();
    setStatus("ORM hints selected", `${state.selectedProjectOrmHints.length} ORM hint${state.selectedProjectOrmHints.length === 1 ? "" : "s"} selected for bootstrap.`);
    return;
  }
  if (target.dataset.projectOrmClear) {
    state.selectedProjectOrmHints = [];
    renderProjectProfile();
    setStatus("ORM selection cleared", "No ORM hints are selected.");
    return;
  }
  if (target.dataset.projectImportSelected) {
    importSelectedProjectSuggestions();
    return;
  }
  if (target.dataset.projectProfileAssets) {
    const selection = target.dataset.projectProfileAssets === "selected"
      ? [...(state.selectedProjectImports || [])]
      : [target.dataset.projectProfileAssets];
    startProjectAssetProfileJob(selection);
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
    loadProjectProfileWithOptions({ forceRefresh: true });
    return;
  }
  if (target.dataset.projectLoadCached) {
    loadProjectProfileWithOptions({ forceRefresh: false });
    return;
  }
  if (target.dataset.projectRootCheck) {
    inspectCurrentProjectRoot();
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
  if (target.dataset.projectSqlCreate) {
    createSqlStructureFromHint(target.dataset.projectSqlCreate);
    return;
  }
  if (target.dataset.projectOrmCreate) {
    createOrmStructureFromHint(target.dataset.projectOrmCreate);
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
  if ((target instanceof HTMLInputElement || target instanceof HTMLSelectElement) && target.dataset.projectFilter) {
    state.projectProfileFilters[target.dataset.projectFilter] = target.value;
    resetProjectProfilePages();
    renderProjectProfile();
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
  if (target instanceof HTMLSelectElement && target.dataset.projectProfileSelect) {
    state.projectProfileOptions[target.dataset.projectProfileSelect] = target.value;
    state.projectRootInspection = null;
    renderProjectProfile();
    return;
  }
  if (target instanceof HTMLTextAreaElement && target.dataset.projectProfileText) {
    state.projectProfileOptions[target.dataset.projectProfileText] = target.value;
    state.projectRootInspection = null;
    return;
  }
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  if (target.dataset.projectProfileRoot) {
    state.projectProfileOptions.rootPath = target.value;
    state.projectRootInspection = null;
    return;
  }
  if (target.dataset.projectProfileOption) {
    state.projectProfileOptions[target.dataset.projectProfileOption] = target.checked;
    state.projectRootInspection = null;
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
      return;
    }
    if (target.dataset.projectSqlSelect) {
      state.selectedProjectSqlHints = toggleSelectionList(state.selectedProjectSqlHints, target.dataset.projectSqlSelect, target.checked);
      renderProjectProfile();
      return;
    }
    if (target.dataset.projectOrmSelect) {
      state.selectedProjectOrmHints = toggleSelectionList(state.selectedProjectOrmHints, target.dataset.projectOrmSelect, target.checked);
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

function mergeProfiledAssetsIntoProjectProfile(assetProfiles) {
  if (!state.projectProfile || !Array.isArray(assetProfiles) || !assetProfiles.length) {
    return;
  }
  const byId = new Map(assetProfiles.map((asset) => [asset.id, asset]));
  const byPath = new Map(assetProfiles.map((asset) => [asset.path, asset]));
  state.projectProfile.data_assets = (state.projectProfile.data_assets || []).map((asset) => (
    byId.get(asset.id) || byPath.get(asset.path) || asset
  ));
}

async function pollProjectAssetProfileJobUntilComplete(jobId) {
  while (jobId) {
    let response;
    let payload;
    try {
      response = await fetch(`/api/project/profile/assets/jobs/${encodeURIComponent(jobId)}`);
      payload = await response.json();
    } catch (error) {
      state.projectAssetProfileJob = null;
      renderProjectProfile();
      setStatus("Asset profiling failed", "The browser lost contact with the selected-asset profiling job.");
      return;
    }
    if (!response.ok) {
      state.projectAssetProfileJob = null;
      renderProjectProfile();
      setStatus("Asset profiling failed", payload.detail || payload.error || "Unable to profile the selected assets.");
      return;
    }
    const job = payload.job || null;
    state.projectAssetProfileJob = job;
    renderProjectProfile();
    if (isProjectProfileJobActive(job)) {
      setStatus("🧪 Profiling selected assets...", describeProjectAssetProfileJob(job));
      await sleep(700);
      continue;
    }
    state.projectAssetProfileJob = null;
    if (!job || job.status === "failed") {
      renderProjectProfile();
      setStatus("Asset profiling failed", job?.error || job?.progress?.message || "Unable to profile the selected assets.");
      return;
    }
    mergeProfiledAssetsIntoProjectProfile(job.asset_profiles || []);
    renderProjectProfile();
    setStatus("Selected asset profiling ready", `${formatValue((job.asset_profiles || []).length)} asset${(job.asset_profiles || []).length === 1 ? "" : "s"} updated with explicit profile results.`);
    return;
  }
}

async function startProjectAssetProfileJob(assetPaths) {
  if (!state.projectProfile) {
    setStatus("Asset profiling unavailable", "Run project discovery first.");
    return;
  }
  const selected = Array.from(new Set((assetPaths || []).filter(Boolean)));
  if (!selected.length) {
    setStatus("Asset profiling skipped", "Choose one or more discovered assets first.");
    return;
  }
  if (isProjectProfileJobActive(state.projectAssetProfileJob)) {
    setStatus("Asset profiling already running", describeProjectAssetProfileJob(state.projectAssetProfileJob));
    return;
  }
  state.projectAssetProfileJob = {
    status: "queued",
    progress: {
      phase: "queued",
      message: "Queued explicit asset profiling.",
      assets_processed: 0,
      assets_total: selected.length,
    },
  };
  renderProjectProfile();
  setStatus("🧪 Profiling selected assets...", "Running isolated profiling for the selected assets.");
  let response;
  let payload;
  try {
    response = await fetch("/api/project/profile/assets/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        root_path: state.projectProfileOptions.rootPath || "",
        include_tests: state.projectProfileOptions.includeTests,
        include_internal: state.projectProfileOptions.includeInternal,
        profile_token: state.projectProfile?.cache?.token || "",
        asset_paths: selected,
        exclude_paths: parseMultilineList(state.projectProfileOptions.excludePathsText),
        asset_roots: parseMultilineList(state.projectProfileOptions.assetRootsText),
      }),
    });
    payload = await response.json();
  } catch (error) {
    state.projectAssetProfileJob = null;
    renderProjectProfile();
    setStatus("Asset profiling failed", "The browser could not start the selected-asset profiling job.");
    return;
  }
  if (!response.ok) {
    state.projectAssetProfileJob = null;
    renderProjectProfile();
    setStatus("Asset profiling failed", payload.detail || payload.error || "Unable to profile the selected assets.");
    return;
  }
  state.projectAssetProfileJob = payload.job || state.projectAssetProfileJob;
  renderProjectProfile();
  await pollProjectAssetProfileJobUntilComplete(state.projectAssetProfileJob?.job_id || "");
}

async function pollProjectProfileJobUntilComplete(jobId, context = {}) {
  while (jobId) {
    let response;
    let payload;
    try {
      response = await fetch(`/api/project/profile/jobs/${encodeURIComponent(jobId)}`);
      payload = await response.json();
    } catch (error) {
      state.projectProfileJob = null;
      projectProfileButton.disabled = false;
      renderProjectProfile();
      setStatus("Project discovery failed", "The browser lost contact with the discovery job while it was running.");
      return;
    }
    if (!response.ok) {
      state.projectProfileJob = null;
      projectProfileButton.disabled = false;
      renderProjectProfile();
      setStatus("Project discovery failed", payload.detail || payload.error || "Unable to inspect this project.");
      return;
    }
    const job = payload.job || null;
    state.projectProfileJob = job;
    renderProjectProfile();
    if (isProjectProfileJobActive(job)) {
      setStatus(`${getProjectDiscoveryPhaseEmoji(job.progress?.phase)} Discovery running...`, describeProjectProfileJob(job));
      await sleep(700);
      continue;
    }
    projectProfileButton.disabled = false;
    state.projectProfileJob = null;
    if (!job || job.status === "failed") {
      renderProjectProfile();
      setStatus("Project discovery failed", job?.error || job?.progress?.message || "Unable to inspect this project.");
      return;
    }
    state.projectProfile = job.project_profile || null;
    state.projectRootInspection = state.projectProfile
      ? {
          requested_path: context.rootPath || state.projectProfile.root || "",
          resolved_path: state.projectProfile.root || context.rootPath || "",
          exists: Boolean(state.projectProfile.root),
          is_directory: true,
          using_workspace_default: !(context.rootPath || state.projectProfile.root),
          cache: state.projectProfile.cache || {},
        }
      : state.projectRootInspection;
    state.projectProfileOptions.includeTests = context.includeTests ?? state.projectProfileOptions.includeTests;
    state.projectProfileOptions.includeInternal = context.includeInternal ?? state.projectProfileOptions.includeInternal;
    state.projectProfileOptions.rootPath = state.projectProfile?.root || context.rootPath || "";
    state.projectProfileOptions.profilingMode = context.profilingMode || state.projectProfileOptions.profilingMode;
    state.projectProfileOptions.excludePathsText = context.excludePathsText ?? state.projectProfileOptions.excludePathsText;
    state.projectProfileOptions.assetRootsText = context.assetRootsText ?? state.projectProfileOptions.assetRootsText;
    resetProjectProfilePages();
    if (context.preset) {
      applyProjectPresetSelections(context.preset);
    } else {
      state.selectedProjectImports = [];
      state.selectedProjectApiHints = [];
      state.selectedProjectUiHints = [];
      state.selectedProjectSqlHints = [];
      state.selectedProjectOrmHints = [];
    }
    renderProjectProfile();
    const summary = state.projectProfile?.summary || {};
    const completionLead = state.projectProfile?.cache?.cached ? "Loaded saved discovery." : "Discovery completed.";
    const completionDetail = `${formatValue(summary.files_scanned || 0)} files walked, ${formatValue(summary.code_files)} code files, ${formatValue(summary.data_assets)} data assets scanned.`;
    setStatus("✅ Project discovery ready", `${completionLead} ${completionDetail}`);
    notifyTaskCompletion("Project discovery ready", completionDetail, {
      emoji: "✅",
      tag: "project-discovery",
    });
    return;
  }
}

async function loadProjectProfileWithOptions(options = {}) {
  if (isProjectProfileJobActive(state.projectProfileJob)) {
    setStatus("Discovery already running", describeProjectProfileJob(state.projectProfileJob));
    return;
  }
  projectProfileButton.disabled = true;
  const includeTests = options.includeTests ?? state.projectProfileOptions.includeTests;
  const includeInternal = options.includeInternal ?? state.projectProfileOptions.includeInternal;
  const rootPath = options.rootPath ?? state.projectProfileOptions.rootPath;
  const profilingMode = options.profilingMode ?? state.projectProfileOptions.profilingMode;
  const excludePaths = parseMultilineList(options.excludePathsText ?? state.projectProfileOptions.excludePathsText);
  const assetRoots = parseMultilineList(options.assetRootsText ?? state.projectProfileOptions.assetRootsText);
  state.projectProfileJob = {
    status: "queued",
    progress: {
      phase: "queued",
      message: "Queued project discovery.",
      files_scanned: 0,
    },
  };
  renderProjectProfile();
  setStatus("🔎 Discovering project...", "Starting background project inspection so the UI stays responsive on large repos.");
  void primeCompletionNotifications();
  let response;
  let payload;
  try {
    response = await fetch("/api/project/profile/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        include_tests: includeTests,
        include_internal: includeInternal,
        force_refresh: Boolean(options.forceRefresh),
        root_path: rootPath || "",
        profile_token: state.projectProfile?.cache?.token || "",
        profiling_mode: profilingMode || "metadata_only",
        exclude_paths: excludePaths,
        asset_roots: assetRoots,
      }),
    });
    payload = await response.json();
  } catch (error) {
    state.projectProfileJob = null;
    projectProfileButton.disabled = false;
    renderProjectProfile();
    setStatus("Project discovery failed", "The browser could not start the discovery job.");
    return;
  }
  if (!response.ok) {
    state.projectProfileJob = null;
    projectProfileButton.disabled = false;
    renderProjectProfile();
    setStatus("Project discovery failed", payload.detail || payload.error || "Unable to inspect this project.");
    return;
  }
  state.projectProfileJob = payload.job || state.projectProfileJob;
  renderProjectProfile();
  await pollProjectProfileJobUntilComplete(state.projectProfileJob?.job_id || "", {
    includeTests,
    includeInternal,
    rootPath,
    profilingMode,
    excludePathsText: options.excludePathsText ?? state.projectProfileOptions.excludePathsText,
    assetRootsText: options.assetRootsText ?? state.projectProfileOptions.assetRootsText,
    preset: options.preset,
  });
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
  state.projectProfileOptions.profilingMode = preset.profiling_mode || "metadata_only";
  state.projectProfileOptions.excludePathsText = (preset.exclude_paths || []).join("\n");
  state.projectProfileOptions.assetRootsText = (preset.asset_roots || []).join("\n");
  state.projectProfileOptions.agentEnrichAfterScan = preset.agent_enrich_after_scan === true;
  state.projectBootstrapOptions = {
    assets: preset.bootstrap_options?.assets !== false,
    apiHints: preset.bootstrap_options?.apiHints !== false,
    uiHints: preset.bootstrap_options?.uiHints !== false,
    sqlHints: preset.bootstrap_options?.sqlHints !== false,
    ormHints: preset.bootstrap_options?.ormHints !== false,
  };
  state.projectWizardStep = 2;
  await loadProjectProfileWithOptions({
    includeTests: preset.include_tests,
    includeInternal: preset.include_internal,
    rootPath: preset.root || state.projectProfileOptions.rootPath,
    profilingMode: preset.profiling_mode || "metadata_only",
    excludePathsText: (preset.exclude_paths || []).join("\n"),
    assetRootsText: (preset.asset_roots || []).join("\n"),
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
  const sqlHintIds = new Set((state.projectProfile?.sql_structure_hints || []).map((hint) => hint.id));
  const ormHintIds = new Set((state.projectProfile?.orm_structure_hints || []).map((hint) => hint.id));
  state.selectedProjectImports = (preset.selected_asset_paths || []).filter((path) => assetPaths.has(path));
  state.selectedProjectApiHints = (preset.selected_api_hint_ids || []).filter((hintId) => apiHintIds.has(hintId));
  state.selectedProjectUiHints = (preset.selected_ui_hint_ids || []).filter((hintId) => uiHintIds.has(hintId));
  state.selectedProjectSqlHints = (preset.selected_sql_hint_ids || []).filter((hintId) => sqlHintIds.has(hintId));
  state.selectedProjectOrmHints = (preset.selected_orm_hint_ids || []).filter((hintId) => ormHintIds.has(hintId));
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
    exclude_paths: parseMultilineList(state.projectProfileOptions.excludePathsText),
    asset_roots: parseMultilineList(state.projectProfileOptions.assetRootsText),
    profiling_mode: state.projectProfileOptions.profilingMode || "metadata_only",
    agent_enrich_after_scan: state.projectProfileOptions.agentEnrichAfterScan === true,
    bootstrap_options: { ...state.projectBootstrapOptions },
    selected_asset_paths: [...(state.selectedProjectImports || [])],
    selected_api_hint_ids: [...(state.selectedProjectApiHints || [])],
    selected_ui_hint_ids: [...(state.selectedProjectUiHints || [])],
    selected_sql_hint_ids: [...(state.selectedProjectSqlHints || [])],
    selected_orm_hint_ids: [...(state.selectedProjectOrmHints || [])],
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
