// Extracted structure review preference/state helpers from app.js.

function getBuiltInStructureReviewPresets() {
  return [
    {
      id: "builtin:high-signal",
      name: "High Signal",
      builtin: true,
      bundleInboxFilter: "needs_attention",
      assignmentFilter: "all",
      reviewUnitFilter: "review_required",
      showLowConfidence: false,
      showMinorImpacts: false,
      showNonMaterial: false,
    },
    {
      id: "builtin:contradictions",
      name: "Contradictions Only",
      builtin: true,
      bundleInboxFilter: "contradictions",
      assignmentFilter: "all",
      reviewUnitFilter: "contradictions",
      showLowConfidence: false,
      showMinorImpacts: false,
      showNonMaterial: false,
    },
    {
      id: "builtin:ready",
      name: "Ready To Merge",
      builtin: true,
      bundleInboxFilter: "ready",
      assignmentFilter: "all",
      reviewUnitFilter: "reviewed",
      showLowConfidence: false,
      showMinorImpacts: false,
      showNonMaterial: true,
    },
    {
      id: "builtin:mine",
      name: "Mine",
      builtin: true,
      bundleInboxFilter: "needs_attention",
      assignmentFilter: "mine",
      reviewUnitFilter: "review_required",
      showLowConfidence: false,
      showMinorImpacts: false,
      showNonMaterial: false,
    },
  ];
}

function getStructureReviewPresetOptions() {
  return [...getBuiltInStructureReviewPresets(), ...(state.structureReviewPresets || [])];
}

function persistStructureReviewPreferences() {
  writeLocalStorageJson(STRUCTURE_REVIEW_PREFS_STORAGE_KEY, {
    reviewerIdentity: state.structureReviewerIdentity || "user",
    bundleInboxFilter: state.structureBundleInboxFilter || "needs_attention",
    assignmentFilter: state.structureAssignmentFilter || "all",
    reviewUnitFilter: state.structureReviewUnitFilter || "review_required",
    showLowConfidence: Boolean(state.structureShowLowConfidence),
    showMinorImpacts: Boolean(state.structureShowMinorImpacts),
    showNonMaterial: Boolean(state.structureShowNonMaterial),
    selectedPresetId: state.selectedStructureReviewPresetId || "",
  });
}

function loadStructureReviewPreferences() {
  const prefs = readLocalStorageJson(STRUCTURE_REVIEW_PREFS_STORAGE_KEY, {});
  const presetList = readLocalStorageJson(STRUCTURE_REVIEW_PRESETS_STORAGE_KEY, []);
  state.structureReviewerIdentity = prefs.reviewerIdentity || "user";
  state.structureBundleInboxFilter = prefs.bundleInboxFilter || "needs_attention";
  state.structureAssignmentFilter = prefs.assignmentFilter || "all";
  state.structureReviewUnitFilter = prefs.reviewUnitFilter || "review_required";
  state.structureShowLowConfidence = Boolean(prefs.showLowConfidence);
  state.structureShowMinorImpacts = Boolean(prefs.showMinorImpacts);
  state.structureShowNonMaterial = Boolean(prefs.showNonMaterial);
  state.structureReviewPresets = Array.isArray(presetList) ? presetList : [];
  state.selectedStructureReviewPresetId = prefs.selectedPresetId || "";
}

function syncStructureWorkflowDraft(bundle) {
  const review = bundle?.review || {};
  state.structureWorkflowDraft = {
    bundleOwner: review.bundle_owner || "",
    assignedReviewer: review.assigned_reviewer || "",
    triageState: review.triage_state || "new",
    triageNote: review.triage_note || "",
  };
}

function slugifyStructurePresetName(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function getSelectedStructureReviewPreset() {
  return getStructureReviewPresetOptions().find((preset) => preset.id === state.selectedStructureReviewPresetId) || null;
}

function applyStructureReviewPreset(preset) {
  if (!preset) {
    return;
  }
  state.structureBundleInboxFilter = preset.bundleInboxFilter || "needs_attention";
  state.structureAssignmentFilter = preset.assignmentFilter || "all";
  state.structureReviewUnitFilter = preset.reviewUnitFilter || "review_required";
  state.structureShowLowConfidence = Boolean(preset.showLowConfidence);
  state.structureShowMinorImpacts = Boolean(preset.showMinorImpacts);
  state.structureShowNonMaterial = Boolean(preset.showNonMaterial);
  state.selectedStructureReviewPresetId = preset.id || "";
  persistStructureReviewPreferences();
  render();
}

function saveStructureReviewPreset() {
  const draftName = String(state.structureReviewPresetDraftName || "").trim();
  if (!draftName) {
    setStatus("Preset name required", "Name the preset before saving the current review queue filters.");
    return;
  }
  const presetId = `custom:${slugifyStructurePresetName(draftName) || "review-preset"}`;
  const nextPreset = {
    id: presetId,
    name: draftName,
    bundleInboxFilter: state.structureBundleInboxFilter || "needs_attention",
    assignmentFilter: state.structureAssignmentFilter || "all",
    reviewUnitFilter: state.structureReviewUnitFilter || "review_required",
    showLowConfidence: Boolean(state.structureShowLowConfidence),
    showMinorImpacts: Boolean(state.structureShowMinorImpacts),
    showNonMaterial: Boolean(state.structureShowNonMaterial),
  };
  const existingPresets = Array.isArray(state.structureReviewPresets) ? state.structureReviewPresets.slice() : [];
  const existingIndex = existingPresets.findIndex((preset) => preset.id === presetId);
  if (existingIndex >= 0) {
    existingPresets.splice(existingIndex, 1, nextPreset);
  } else {
    existingPresets.push(nextPreset);
  }
  state.structureReviewPresets = existingPresets.sort((left, right) => (
    String(left.name || "").localeCompare(String(right.name || ""))
    || String(left.id || "").localeCompare(String(right.id || ""))
  ));
  state.selectedStructureReviewPresetId = presetId;
  writeLocalStorageJson(STRUCTURE_REVIEW_PRESETS_STORAGE_KEY, state.structureReviewPresets);
  persistStructureReviewPreferences();
  render();
  setStatus("Review preset saved", `${draftName} is now available in the review inbox preset list.`);
}

function deleteStructureReviewPreset() {
  const presetId = state.selectedStructureReviewPresetId || "";
  if (!presetId || String(presetId).startsWith("builtin:")) {
    setStatus("Preset delete skipped", "Choose a saved custom preset to remove it.");
    return;
  }
  const preset = getSelectedStructureReviewPreset();
  state.structureReviewPresets = (state.structureReviewPresets || []).filter((item) => item.id !== presetId);
  state.selectedStructureReviewPresetId = "";
  writeLocalStorageJson(STRUCTURE_REVIEW_PRESETS_STORAGE_KEY, state.structureReviewPresets);
  persistStructureReviewPreferences();
  render();
  setStatus("Review preset deleted", `${preset?.name || "Saved preset"} was removed from this browser profile.`);
}

function getCurrentStructureVisibleReviewContext() {
  const bundle = state.selectedStructureBundle;
  if (!state.reviewDrawerOpen || !bundle) {
    return null;
  }
  const context = buildStructureBundleContext(bundle);
  const patchGroups = groupStructureBundlePatches(bundle, context);
  const filteredPatchGroups = filterStructurePatchGroups(patchGroups);
  const visibleUnits = collectVisibleStructureReviewUnits(filteredPatchGroups);
  if (!visibleUnits.length) {
    return null;
  }
  syncActiveStructureReviewUnit(filteredPatchGroups);
  return {
    bundle,
    filteredPatchGroups,
    visibleUnits,
  };
}

function moveStructureActiveReviewUnit(delta) {
  const reviewContext = getCurrentStructureVisibleReviewContext();
  if (!reviewContext) {
    return;
  }
  const { visibleUnits } = reviewContext;
  const currentIndex = Math.max(0, visibleUnits.findIndex((unit) => unit.reviewUnitKey === state.structureActiveReviewUnitKey));
  const nextIndex = Math.max(0, Math.min(visibleUnits.length - 1, currentIndex + delta));
  const nextUnit = visibleUnits[nextIndex];
  if (!nextUnit) {
    return;
  }
  setActiveStructureReviewUnit(nextUnit.reviewUnitKey);
}

function triggerStructureKeyboardReview(decision) {
  const reviewContext = getCurrentStructureVisibleReviewContext();
  if (!reviewContext) {
    return;
  }
  const activeUnit = reviewContext.visibleUnits.find((unit) => unit.reviewUnitKey === state.structureActiveReviewUnitKey) || reviewContext.visibleUnits[0];
  if (!activeUnit?.pendingIds?.length) {
    setStatus("Keyboard review skipped", "The active review unit has no pending patches to review.");
    return;
  }
  openStructurePatchReviewConfirm(reviewContext.bundle.bundle_id, activeUnit.pendingIds, decision);
}
