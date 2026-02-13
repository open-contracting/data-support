/**
 * Field ↔ Indicator Explorer (main.js)
 * Purpose: Visualize dependencies between OCDS fields and indicators; selecting fields reveals computable indicators.
 * Global state (window.*) tracks selection, ordering, sorting, label mode and indices built from CSV.
 * Small dataset → full re-render per interaction for simplicity.
 */
import { csvParse } from "https://cdn.jsdelivr.net/npm/d3-dsv@3/+esm";

// -------------------------------------------------------------
// Global state containers (intentionally on window for quick, simple sharing
// across functions without a build system or state library). In a modular
// architecture you'd encapsulate these inside a closure or a state manager.
// -------------------------------------------------------------
window._selectedFields = new Set();
window._possibleIndicatorOrder = [];
window.fieldToIndicators = {};
window.indicatorMap = {};
window.usecaseMap = {};
window._fieldSort = { key: 'count', dir: 'desc' };
window._fieldLabelMode = 'friendly';
// Explorer mode: 'fields-to-indicators' | 'indicators-to-fields'
window._explorerMode = 'fields-to-indicators';
// Selected indicators (reverse mode)
window._selectedIndicators = new Set();
// Preserve original forward-mode header markup for restoration
let _originalFieldsHeaderHTML = null;
let _originalIndicatorsHeaderHTML = null;

const FIELD_FRIENDLY_OVERRIDES = { 'ocid': 'OCID' };

/**
 * Convert a raw field path into a user-friendly label by removing slashes,
 * expanding camelCase / underscores and capitalizing words.
 * @param {string} path Raw OCDS field path.
 * @returns {string} Friendly label.
 */
function friendlyFieldName(path) {
  if (!path) return '';
  if (FIELD_FRIENDLY_OVERRIDES[path]) return FIELD_FRIENDLY_OVERRIDES[path];
  const words = path
    .split('/')
    .map(seg => seg.replace(/_/g, ' '))
    .map(seg => seg.replace(/([a-z])([A-Z])/g, '$1 $2'))
    .join(' ')
    .split(/\s+/)
    .filter(Boolean)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
  return words.join(' ');
}

/**
 * Choose label presentation (friendly vs raw path) according to global mode.
 * @param {string} path Field path.
 * @returns {string} Display label.
 */
function formatFieldLabel(path) {
  return window._fieldLabelMode === 'friendly' ? friendlyFieldName(path) : path;
}

// -------------------------------------------------------------
// Fetch and initialize
// 1. Load CSV text.
// 2. Guard against accidentally serving HTML (common misconfig).
// 3. Parse with d3-dsv.
// 4. Build internal data structures, then render.
// -------------------------------------------------------------
fetch("indicators.csv")
  .then(r => { if (!r.ok) throw new Error("Could not load indicators.csv"); return r.text(); })
  .then(text => { const trimmed = text.trim(); if (trimmed.startsWith("<")) throw new Error("Loaded HTML instead of CSV – check path"); const rows = csvParse(text); buildDataStructures(rows); initialRender(); })
  .catch(err => { const f = document.getElementById("fields-list"); const i = document.getElementById("indicators-list"); if (f) f.innerHTML = `<div style='color:red'>${err.message}</div>`; if (i) i.innerHTML = `<div style='color:red'>${err.message}</div>`; console.error(err); });

/**
 * Build in-memory indices from parsed CSV rows.
 * - fieldToIndicators: field -> [indicators]
 * - indicatorMap: indicator -> { usecase, fields[] }
 * - usecaseMap: usecase -> [indicators]
 * @param {Object[]} data Parsed CSV row objects.
 * @returns {void}
 */
function buildDataStructures(data) {
  const f2i = {};
  const indMap = {};
  const ucMap = {};
  data.forEach(row => {
    const field = row.fields?.trim();
    const indicator = row.indicator?.trim();
    const usecase = row.usecase?.trim();
    if (!field || !indicator) return;
    if (!f2i[field]) f2i[field] = new Set();
    f2i[field].add(indicator);
    if (!indMap[indicator]) indMap[indicator] = { usecase: usecase || "", fields: new Set() };
    indMap[indicator].fields.add(field);
    if (usecase) {
      if (!ucMap[usecase]) ucMap[usecase] = new Set();
      ucMap[usecase].add(indicator);
    }
  });
  Object.keys(f2i).forEach(k => f2i[k] = [...f2i[k]]);
  Object.keys(indMap).forEach(k => indMap[k].fields = [...indMap[k].fields]);
  Object.keys(ucMap).forEach(k => ucMap[k] = [...ucMap[k]]);
  window.fieldToIndicators = f2i;
  window.indicatorMap = indMap;
  window.usecaseMap = ucMap;
}

/**
 * Initial UI render + event listener wiring (selection, sorting, reset, label toggle).
 * @returns {void}
 */
function initialRender() {
  renderPanels();
  updateFieldSortIndicators();
  // Event delegation for checkboxes (avoids re-binding every render)
  const fieldsList = document.getElementById("fields-list");
  fieldsList.addEventListener("change", (e) => {
    if (e.target.classList.contains("field-checkbox")) {
      const val = e.target.value;
      if (e.target.checked) {
        window._selectedFields.add(val);
      } else {
        window._selectedFields.delete(val);
      }
      renderPanels();
    }
  });
  // Reset button
  const resetBtn = document.getElementById("reset-fields");
  if (resetBtn) {
    const runReset = () => {
      if (window._explorerMode === 'fields-to-indicators') {
        if (window._selectedFields.size === 0) return;
        window._selectedFields.clear();
      } else {
        if (window._selectedIndicators.size === 0) return;
        window._selectedIndicators.clear();
      }
      renderPanels();
      updateFieldSortIndicators();
      syncResetButton();
    };
    resetBtn.addEventListener("click", runReset);
  }
  // Sort header clicks
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('.field-columns-header .sort-header');
    if (!btn) return;
    const key = btn.getAttribute('data-sort-key');
    const state = window._fieldSort;
    if (state.key === key) {
      state.dir = state.dir === 'asc' ? 'desc' : 'asc';
    } else {
      state.key = key;
      state.dir = key === 'count' ? 'desc' : 'asc';
    }
    renderPanels();
    updateFieldSortIndicators();
  });

  // Field label mode toggle
  const toggle = document.getElementById('toggle-field-label-mode');
  if (toggle) {
    // Initialize attributes to reflect default mode
    const friendlyInit = window._fieldLabelMode === 'friendly';
    toggle.setAttribute('aria-pressed', friendlyInit ? 'true' : 'false');
    toggle.title = friendlyInit ? 'Switch to JSON path field names' : 'Switch to friendly field names';
    toggle.setAttribute('aria-label', toggle.title);
    toggle.addEventListener('click', () => {
      window._fieldLabelMode = window._fieldLabelMode === 'path' ? 'friendly' : 'path';
      const friendly = window._fieldLabelMode === 'friendly';
      toggle.setAttribute('aria-pressed', friendly ? 'true' : 'false');
      toggle.title = friendly ? 'Switch to JSON path field names' : 'Switch to friendly field names';
      toggle.setAttribute('aria-label', toggle.title);
      renderPanels();
    });
  }

  // Central single mode toggle
  const centralToggle = document.getElementById('mode-toggle-central');
  if (centralToggle) {
    const syncCentral = () => {
      const mode = window._explorerMode;
      if (mode === 'fields-to-indicators') {
        centralToggle.dataset.mode = 'fields-to-indicators';
        centralToggle.title = 'Switch to indicators → fields mode';
        centralToggle.setAttribute('aria-label', centralToggle.title);
        centralToggle.innerHTML = '<span class="mode-toggle-icon">⇄</span><span class="mode-toggle-text">Fields → Indicators</span>';
      } else {
        centralToggle.dataset.mode = 'indicators-to-fields';
        centralToggle.title = 'Switch to fields → indicators mode';
        centralToggle.setAttribute('aria-label', centralToggle.title);
        centralToggle.innerHTML = '<span class="mode-toggle-icon">⇄</span><span class="mode-toggle-text">Indicators → Fields</span>';
      }
    };
    const applyMode = (mode) => {
      if (window._explorerMode === mode) return;
      window._explorerMode = mode;
      // Clear opposing selection set
      if (mode === 'fields-to-indicators') {
        window._selectedIndicators.clear();
      } else {
        window._selectedFields.clear();
      }
      renderPanels();
      updateFieldSortIndicators();
      syncCentral();
    };
    centralToggle.addEventListener('click', () => {
      const next = window._explorerMode === 'fields-to-indicators' ? 'indicators-to-fields' : 'fields-to-indicators';
      applyMode(next);
      syncResetButton();
    });
    syncCentral();
  }
  syncResetButton();
}

/**
 * Central dispatcher to render both panels based on current explorer mode.
 */
function renderPanels() {
  if (window._explorerMode === 'fields-to-indicators') {
    // Show field sorting header
    document.querySelectorAll('#fields-panel .field-columns-header').forEach(h => h.style.display = 'flex');
    document.querySelectorAll('#indicators-panel .field-columns-header').forEach(h => h.style.display = 'flex');
    // On first load capture originals
    const fieldsHeaderEl = document.querySelector('#fields-panel .field-columns-header');
    const indicatorsHeaderEl = document.querySelector('#indicators-panel .field-columns-header');
    if (fieldsHeaderEl && _originalFieldsHeaderHTML == null) _originalFieldsHeaderHTML = fieldsHeaderEl.innerHTML;
    if (indicatorsHeaderEl && _originalIndicatorsHeaderHTML == null) _originalIndicatorsHeaderHTML = indicatorsHeaderEl.innerHTML;
    // Restore original markup if it was overwritten by reverse mode
    if (fieldsHeaderEl && _originalFieldsHeaderHTML && fieldsHeaderEl.innerHTML !== _originalFieldsHeaderHTML) {
      fieldsHeaderEl.innerHTML = _originalFieldsHeaderHTML;
    }
    if (indicatorsHeaderEl && _originalIndicatorsHeaderHTML && indicatorsHeaderEl.innerHTML !== _originalIndicatorsHeaderHTML) {
      indicatorsHeaderEl.innerHTML = _originalIndicatorsHeaderHTML;
    }
    const leftTitle = document.querySelector('.gh-left .gh-title');
    const rightTitle = document.querySelector('.gh-right .gh-title');
    if (leftTitle) leftTitle.firstChild && (leftTitle.childNodes[0].textContent = 'Fields');
    if (rightTitle) rightTitle.firstChild && (rightTitle.childNodes[0].textContent = 'Indicators');
    syncHeaderAccessories();
    renderFieldsPanel();
    renderIndicatorsPanel();
  } else {
    // Reverse mode: repurpose headers for indicator list and required fields list
    const leftHeader = document.querySelector('#fields-panel .field-columns-header');
    const rightHeader = document.querySelector('#indicators-panel .field-columns-header');
    if (leftHeader) {
      leftHeader.style.display = 'flex';
      leftHeader.innerHTML = '<span class="col-indicator-rev" title="Indicator name">Indicator</span>';
    }
    if (rightHeader) {
      rightHeader.style.display = 'flex';
      rightHeader.innerHTML = '<span class="col-field reverse-required-field">Required Field</span><span class="col-count" title="Number of selected indicators relying on this field">Number of selected indicators relying on this field</span>';
    }
    const leftTitle = document.querySelector('.gh-left .gh-title');
    const rightTitle = document.querySelector('.gh-right .gh-title');
    if (leftTitle) leftTitle.firstChild && (leftTitle.childNodes[0].textContent = 'Indicators');
    if (rightTitle) rightTitle.firstChild && (rightTitle.childNodes[0].textContent = 'Required Fields');
    syncHeaderAccessories();
    renderIndicatorSelectionPanel();
    renderRequiredFieldsPanel();
    // Restore forward headers markup lazily when switching back (handled in forward branch above by original HTML still in DOM for that mode)
  }
  // After rendering, ensure reset button wording matches current mode
  syncResetButton();
}

/**
 * Ensure the field label toggle button and the usecase legend icon sit under the
 * correct semantic header for the current mode.
 * Forward mode: Left = Fields (label toggle), Right = Indicators (legend).
 * Reverse mode: Left = Indicators (legend), Right = Required Fields (label toggle removed/hidden).
 */
function syncHeaderAccessories() {
  const mode = window._explorerMode;
  const leftTitle = document.querySelector('.gh-left .gh-title');
  const rightTitle = document.querySelector('.gh-right .gh-title');
  const labelToggle = document.getElementById('toggle-field-label-mode');
  const legend = document.querySelector('.usecase-help');
  if (!leftTitle || !rightTitle) return;
  // Clear accidental duplicates (DOM move, not clone)
  if (mode === 'fields-to-indicators') {
    // label toggle → left, legend → right
    if (labelToggle && !leftTitle.contains(labelToggle)) leftTitle.appendChild(labelToggle);
    if (legend && !rightTitle.contains(legend)) rightTitle.appendChild(legend);
    if (labelToggle) labelToggle.style.display = 'inline-flex';
  } else {
    // legend → left, label toggle logically belongs with fields but right header now shows required fields (no toggle)
    if (legend && !leftTitle.contains(legend)) leftTitle.appendChild(legend);
    if (labelToggle) {
      // For reverse mode the label toggle still affects field labels in required fields list, keep it in right header for utility
      if (!rightTitle.contains(labelToggle)) rightTitle.appendChild(labelToggle);
      labelToggle.style.display = 'inline-flex';
    }
  }
}

/** Update reset button aria-label/title according to active mode */
function syncResetButton() {
  const btn = document.getElementById('reset-fields');
  if (!btn) return;
  const mode = window._explorerMode;
  if (mode === 'fields-to-indicators') {
    btn.title = 'Reset selected fields';
    btn.setAttribute('aria-label', 'Reset selected fields');
  } else {
    btn.title = 'Reset selected indicators';
    btn.setAttribute('aria-label', 'Reset selected indicators');
  }
}

/** Reverse mode LEFT panel: indicator selection list */
function renderIndicatorSelectionPanel() {
  const list = document.getElementById('fields-list'); // reuse left pane container
  const statsDiv = document.getElementById('fields-stats');
  if (!list) return;
  list.innerHTML = '';
  const indicatorMap = window.indicatorMap;
  const selectedIndicators = window._selectedIndicators;
  const entries = Object.entries(indicatorMap).sort((a, b) => {
    const ua = (a[1].usecase || '').toLowerCase();
    const ub = (b[1].usecase || '').toLowerCase();
    if (ua && ub) {
      if (ua !== ub) return ua < ub ? -1 : 1;
    } else if (ua && !ub) {
      return -1; // defined usecase before empty
    } else if (!ua && ub) {
      return 1;
    }
    return a[0].localeCompare(b[0]);
  });
  const usecaseIcons = {
    "Market Opportunity": "💼",
    "Public Integrity": "🕵️",
    "Service Delivery": "🚚",
    "Internal Efficiency": "⚙️",
    "Value for Money": "💰"
  };
  entries.forEach(([name, obj]) => {
    const div = document.createElement('div');
    div.className = 'indicator-select-item';
    const checked = selectedIndicators.has(name);
    div.innerHTML = `
      <label class="indicator-select-row ${checked ? 'is-selected' : ''}">
        <input type="checkbox" class="indicator-checkbox" value="${name}" ${checked ? 'checked' : ''} aria-label="Select indicator ${name}">
        <span class="indicator-usecase" title="${obj.usecase}">${usecaseIcons[obj.usecase] || '❓'}</span>
        <span class="indicator-name">${name}</span>
      </label>`;
    list.appendChild(div);
  });
  if (statsDiv) statsDiv.textContent = `${selectedIndicators.size} of ${entries.length} indicators selected`;
  // Delegate checkbox events (similar pattern) – ensure only one listener attached
  if (!list._indicatorSelectionBound) {
    list.addEventListener('change', (e) => {
      if (e.target.classList.contains('indicator-checkbox')) {
        const val = e.target.value;
        if (e.target.checked) selectedIndicators.add(val); else selectedIndicators.delete(val);
        // Immediate visual update for selection highlight
        const row = e.target.closest('.indicator-select-row');
        if (row) row.classList.toggle('is-selected', e.target.checked);
        renderRequiredFieldsPanel();
        const statsDiv2 = document.getElementById('fields-stats');
        if (statsDiv2) statsDiv2.textContent = `${selectedIndicators.size} of ${entries.length} indicators selected`;
      }
    });
    list._indicatorSelectionBound = true;
  }
}

/** Reverse mode RIGHT panel: aggregated required fields */
function renderRequiredFieldsPanel() {
  const container = document.getElementById('indicators-list');
  const statsDiv = document.getElementById('indicators-stats');
  if (!container) return;
  container.innerHTML = '';
  const indicatorMap = window.indicatorMap;
  const selectedIndicators = window._selectedIndicators;
  if (selectedIndicators.size === 0) {
    if (statsDiv) statsDiv.textContent = '0 required fields';
    container.innerHTML = '<div style="font-size:13px;color:#666;">Select indicators to see required fields.</div>';
    return;
  }
  const freq = new Map();
  selectedIndicators.forEach(ind => {
    const obj = indicatorMap[ind];
    if (!obj) return;
    obj.fields.forEach(f => {
      freq.set(f, (freq.get(f) || 0) + 1);
    });
  });
  const rows = [...freq.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  const max = rows[0] ? rows[0][1] : 1;
  const listDiv = document.createElement('div');
  listDiv.className = 'required-fields-list';
  rows.forEach(([field, count]) => {
    const pct = (count / max) * 100;
    const wrap = document.createElement('div');
    wrap.className = 'field-item';
    wrap.innerHTML = `
      <div class="field-bar" style="width:${pct.toFixed(2)}%" title="Required by ${count} selected indicator(s)">
        <span class="field-label">${formatFieldLabel(field)}</span>
      </div>
      <span class="field-count" title="Selected indicators requiring this field">${count}</span>
    `;
    listDiv.appendChild(wrap);
  });
  container.appendChild(listDiv);
  if (statsDiv) statsDiv.textContent = `${rows.length} required field${rows.length !== 1 ? 's' : ''}`;
}

/**
 * Render the fields panel: stats + sorted list + proportional width bars.
 * @returns {void}
 */
function renderFieldsPanel() {
  const fieldToIndicators = window.fieldToIndicators;
  const selectedFields = window._selectedFields;
  const sortState = window._fieldSort || { key: 'count', dir: 'desc' };
  const fieldsList = document.getElementById("fields-list");
  if (!fieldsList) return;
  fieldsList.innerHTML = "";
  const totalFields = Object.keys(fieldToIndicators).length;
  const statsDiv = document.getElementById("fields-stats");
  if (statsDiv) statsDiv.textContent = `${selectedFields.size} of ${totalFields} fields available`;
  const entries = Object.entries(fieldToIndicators);
  const globalMax = entries.reduce((m, [, arr]) => Math.max(m, arr.length), 0) || 1;
  let sorted = [...entries];
  if (sortState.key === 'count') {
    sorted.sort((a, b) => a[1].length - b[1].length);
  } else if (sortState.key === 'field') {
    sorted.sort((a, b) => a[0].localeCompare(b[0]));
  }
  if (sortState.dir === 'desc') sorted.reverse();
  const max = globalMax;
  sorted.forEach(([field, indicators]) => {
    const count = indicators.length;
    let barWidth = (count / max) * 100;
    const isTiny = barWidth > 0 && barWidth < 0.8 && count < max;
    const wrap = document.createElement("div");
    wrap.className = "field-item";
    wrap.innerHTML = `
      <label style="width:100%;display:flex;align-items:center;position:relative;">
        <input type="checkbox" class="field-checkbox" value="${field}" style="margin-right:8px;" ${selectedFields.has(field) ? "checked" : ""}>
        <div class="field-bar ${selectedFields.has(field) ? "checked" : ""} ${isTiny ? "field-bar-micro" : ""}" data-count="${count}" style="width:${barWidth.toFixed(2)}%" title="${field}">
          <span class="field-label">${formatFieldLabel(field)}</span>
        </div>
        <span class="field-count" title="Indicators depending on this field">${count}</span>
      </label>
    `;
    fieldsList.appendChild(wrap);
  });
}

/**
 * Render indicators: computable (stable emergence order) then non-computable (by completion ratio + name).
 * @returns {void}
 */
function renderIndicatorsPanel() {
  const indicatorMap = window.indicatorMap;
  const selectedFields = window._selectedFields;
  const indicatorsList = document.getElementById("indicators-list");
  if (!indicatorsList) return;
  indicatorsList.innerHTML = "";
  const usecaseIcons = {
    "Market Opportunity": "💼",
    "Public Integrity": "🕵️",
    "Service Delivery": "🚚",
    "Internal Efficiency": "⚙️",
    "Value for Money": "💰"
  };

  const all = Object.entries(indicatorMap).map(([name, obj]) => ({
    name,
    usecase: obj.usecase,
    fields: obj.fields,
    can: obj.fields.every(f => selectedFields.has(f)),
    satisfied: obj.fields.filter(f => selectedFields.has(f)),
    missing: obj.fields.filter(f => !selectedFields.has(f))
  }));
  if (!window._possibleIndicatorOrder) window._possibleIndicatorOrder = [];
  const order = window._possibleIndicatorOrder;
  const currentPossible = all.filter(i => i.can).map(i => i.name);
  for (let i = order.length - 1; i >= 0; i--) if (!currentPossible.includes(order[i])) order.splice(i, 1);
  currentPossible.forEach(name => { if (!order.includes(name)) order.push(name); });
  const possible = order.map(n => all.find(i => i.name === n)).filter(Boolean);
  const notPossible = all.filter(i => !i.can)
    .sort((a, b) => {
      const ar = a.satisfied.length / a.fields.length;
      const br = b.satisfied.length / b.fields.length;
      if (br !== ar) return br - ar;
      return a.name.localeCompare(b.name);
    });
  const statsDiv = document.getElementById("indicators-stats");
  if (statsDiv) statsDiv.textContent = `${possible.length} of ${all.length} indicators can be calculated`;
  const listDiv = document.createElement("div");
  listDiv.className = "indicator-list";
  const renderIndicator = (ind, possibleFlag) => {
    const div = document.createElement("div");
    div.className = `indicator-item ${possibleFlag ? "indicator-possible" : "indicator-disabled"}`;
    const progressLabel = possibleFlag ? "✔" : `${ind.satisfied.length}/${ind.fields.length}`;
    let missingFieldsHtml = "";
    if (!possibleFlag && ind.missing.length > 0) {
      missingFieldsHtml = `<p class='indicator-missing-fields-block'>Missing: ${ind.missing.map(f => formatFieldLabel(f)).join(", ")}</p>`;
    }
    if (possibleFlag) {
      div.innerHTML = `
        <p class="indicator-head-row">
          <span class="indicator-usecase" title="${ind.usecase}">${usecaseIcons[ind.usecase] || "❓"}</span>
          <span class="indicator-name">${ind.name}</span>
          <span class="indicator-progress" title="All required fields selected">${progressLabel}</span>
        </p>
      `;
    } else {
      div.innerHTML = `
        <p class="indicator-head-row">
          <span class="indicator-usecase" title="${ind.usecase}">${usecaseIcons[ind.usecase] || "❓"}</span>
          <span class="indicator-name">${ind.name}</span>
          <span class="indicator-progress" title="Fields selected: ${ind.satisfied.length} of ${ind.fields.length}">${progressLabel}</span>
        </p>
        ${missingFieldsHtml}
      `;
    }
    listDiv.appendChild(div);
  };
  possible.forEach(ind => renderIndicator(ind, true));
  notPossible.forEach(ind => renderIndicator(ind, false));
  indicatorsList.appendChild(listDiv);
}

/**
 * Update visual sort directional indicators in header.
 * @returns {void}
 */
function updateFieldSortIndicators() {
  const sortState = window._fieldSort;
  document.querySelectorAll('.field-columns-header .sort-indicator').forEach(el => {
    const key = el.getAttribute('data-key');
    el.classList.remove('sort-asc', 'sort-desc');
    if (key === sortState.key) {
      el.classList.add(sortState.dir === 'asc' ? 'sort-asc' : 'sort-desc');
    }
  });
}

/**
 * Resolve usecase label to emoji icon.
 * @param {string} usecase Usecase label.
 * @returns {string} Emoji icon.
 */
function getUsecaseIcon(usecase) {
  const map = {
    "Market Opportunity": "💼",
    "Public Integrity": "🕵️",
    "Service Delivery": "🚚",
    "Internal Efficiency": "⚙️",
    "Value for Money": "💰"
  };
  return map[usecase] || "❓";
}
