// Plan, validation, and project-discovery support helpers extracted from app.js

function getPlanArtifactRows(artifacts) {
  if (!artifacts || typeof artifacts !== "object") {
    return [];
  }
  const rows = [
    ["Latest JSON", artifacts.latest_json],
    ["Latest Markdown", artifacts.latest_markdown],
    ["Timestamped JSON", artifacts.timestamped_json],
    ["Timestamped Markdown", artifacts.timestamped_markdown],
    ["Remote latest JSON", artifacts.remote_latest_json],
    ["Remote latest Markdown", artifacts.remote_latest_markdown],
    ["Remote timestamped JSON", artifacts.remote_timestamped_json],
    ["Remote timestamped Markdown", artifacts.remote_timestamped_markdown],
  ];
  return rows.filter(([, value]) => Boolean(value));
}

function renderArtifactStorageGuidance(storageInfo, artifacts) {
  const objectStore = storageInfo?.object_store || {};
  const hasRemoteArtifacts = Boolean(
    artifacts?.remote_latest_json
    || artifacts?.remote_latest_markdown
    || artifacts?.remote_timestamped_json
    || artifacts?.remote_timestamped_markdown
  );
  if (!hasRemoteArtifacts && !objectStore.enabled) {
    return "";
  }
  const currentTarget = objectStore.enabled
    ? [
        objectStore.backend ? `Backend: ${objectStore.backend}` : "",
        objectStore.endpoint ? `Endpoint: ${objectStore.endpoint}` : "",
        objectStore.bucket ? `Bucket: ${objectStore.bucket}` : "",
        objectStore.prefix ? `Prefix: ${objectStore.prefix}` : "",
      ].filter(Boolean).join(" · ")
    : "Remote artifact storage is currently disabled.";
  return `
    <div class="artifact-help">
      <strong>Remote Artifact Storage</strong>
      <p class="hint">${escapeHtml(currentTarget)}</p>
      <p class="hint">${escapeHtml(objectStore.update_hint || "")}</p>
      <div class="chip-row">
        ${((objectStore.config_env_vars || []).map((name) => `<span class="tag-chip">${escapeHtml(name)}</span>`).join(""))}
      </div>
      <div class="chip-row">
        ${((objectStore.credential_env_vars || []).map((name) => `<span class="tag-chip muted">${escapeHtml(name)}</span>`).join(""))}
      </div>
    </div>
  `;
}

function renderPlanArtifacts(artifacts, storageInfo) {
  const rows = getPlanArtifactRows(artifacts);
  const guidance = renderArtifactStorageGuidance(storageInfo, artifacts);
  if (!rows.length && !guidance) {
    return "";
  }
  return `
    <div class="artifacts">
      <strong>Artifacts</strong>
      ${rows.length ? `
        <div class="artifact-list">
          ${rows.map(([label, value]) => `
            <div class="artifact-item">
              <span class="artifact-label">${escapeHtml(label)}</span>
              <code>${escapeHtml(value)}</code>
            </div>
          `).join("")}
        </div>
      ` : ""}
      ${guidance}
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
    ${renderPlanArtifacts(state.lastArtifacts, state.artifactStorage)}
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

function normalizeProjectDiscoveryQuery(value) {
  return (value || "").trim().toLowerCase();
}

function matchesProjectDiscoveryQuery(values, query) {
  if (!query) {
    return true;
  }
  return values.some((value) => String(value || "").toLowerCase().includes(query));
}

function filterProjectAssets(dataAssets) {
  const query = normalizeProjectDiscoveryQuery(state.projectProfileFilters.assetsQuery);
  const status = state.projectProfileFilters.assetsStatus || "all";
  return dataAssets.filter((asset) => {
    if (status === "suggested" && !asset.suggested_import) {
      return false;
    }
    if (status !== "all" && status !== "suggested" && (asset.profile_status || "unknown") !== status) {
      return false;
    }
    return matchesProjectDiscoveryQuery(
      [
        asset.path,
        asset.format,
        asset.profile_status,
        asset.suggested_import?.source_label,
        asset.suggested_import?.data_label,
        ...(asset.columns || []).map((column) => `${column.name} ${column.data_type}`),
      ],
      query,
    );
  });
}

function filterProjectHints(hints, valuesForHint) {
  const query = normalizeProjectDiscoveryQuery(state.projectProfileFilters.contractsQuery);
  return hints.filter((hint) => matchesProjectDiscoveryQuery(valuesForHint(hint), query));
}

function getProjectProfilePage(pageKey) {
  return Math.max(1, state.projectProfilePages?.[pageKey] || 1);
}

function setProjectProfilePage(pageKey, nextPage) {
  state.projectProfilePages = {
    ...(state.projectProfilePages || {}),
    [pageKey]: Math.max(1, nextPage),
  };
}

function resetProjectProfilePages() {
  state.projectProfilePages = {
    assets: 1,
    api: 1,
    ui: 1,
    sql: 1,
    orm: 1,
  };
}

function paginateProjectDiscovery(items, pageKey, pageSize) {
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  const page = Math.min(getProjectProfilePage(pageKey), totalPages);
  setProjectProfilePage(pageKey, page);
  const offset = (page - 1) * pageSize;
  return {
    page,
    totalPages,
    items: items.slice(offset, offset + pageSize),
  };
}

function renderProjectDiscoveryPager(pageKey, page, totalPages) {
  if (totalPages <= 1) {
    return "";
  }
  return `
    <div class="section-actions" style="margin-top: 14px; justify-content: center;">
      <button class="ghost-button" type="button" data-project-page-key="${escapeHtml(pageKey)}" data-project-page-move="prev" ${page <= 1 ? "disabled" : ""}>Prev Page</button>
      <span class="hint" style="margin: 0 16px;">Page ${page} of ${totalPages}</span>
      <button class="ghost-button" type="button" data-project-page-key="${escapeHtml(pageKey)}" data-project-page-move="next" ${page >= totalPages ? "disabled" : ""}>Next Page</button>
    </div>
  `;
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
        : `${apiHintCount} available API hint${apiHintCount === 1 ? "" : "s"}`
    );
  }
  if (state.projectBootstrapOptions.uiHints) {
    parts.push(
      selectedUiHintCount
        ? `${selectedUiHintCount} selected UI hint${selectedUiHintCount === 1 ? "" : "s"}`
        : `${uiHintCount} available UI hint${uiHintCount === 1 ? "" : "s"}`
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

