const state = {
  candidates: [],
  selected: null,
  sortKey: "conflict_score",
  swapAnimationTimers: [],
};

const fmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });

const TERM_DESCRIPTIONS = {
  Page: "Wikipedia article title being monitored.",
  Score: "Open-ended conflict score combining velocity, reverts, participants, recency, and cleanup penalties.",
  "Conflict Score": "Highest conflict score reached during the selected scoreboard period.",
  Severity: "Label derived from the conflict score: quiet, watch, skirmish, war, or major.",
  Edits: "Non-bot article edits counted in the current 24 hour scoring window.",
  Reverts: "Detected undo, rollback, or revert-like edits in the current window.",
  Editors: "Unique non-bot editors involved in the current scoring window.",
  Participants: "Unique editors involved in the selected live scoreboard period.",
  Status: "Current live severity label for the ranked page.",
  Battles: "Repeated article text disputes with at least two combatants.",
  Length: "Estimated time in the historical snapshot where the page stayed above the war threshold.",
  "Cleanup Penalty": "Points subtracted when revert activity looks one-sided, which can indicate vandalism cleanup instead of an edit war.",
  "Mutual Revert Points": "Points from editors reverting each other, weighted higher than one-sided reverting.",
  "Participant Points": "Points from the number of distinct human editors active on the page.",
  "Recency Points": "Points from recent edits and reverts, so currently active disputes rank higher.",
  "Revert Density Points": "Points from the share of human edits that are detected as reverts.",
  "Revert Volume Points": "Points from the number of detected reverts, making active undo/rollback behavior a primary signal.",
  "Velocity Points": "Points from edit volume relative to the window size.",
};

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((item) => item.classList.remove("is-active"));
    document.querySelectorAll(".view").forEach((view) => view.classList.remove("is-active"));
    tab.classList.add("is-active");
    document.getElementById(tab.dataset.view).classList.add("is-active");
    if (tab.dataset.view === "scoreboard") loadScoreboard();
  });
});

document.getElementById("sortLive").addEventListener("change", (event) => {
  state.sortKey = event.target.value;
  renderLive();
});

document.getElementById("scoreboardPeriod").addEventListener("change", loadScoreboard);
document.getElementById("scoreboardRows").addEventListener("click", (event) => {
  const toggle = event.target.closest(".related-segments-toggle");
  if (!toggle) return;
  const target = document.getElementById(toggle.dataset.target);
  if (!target) return;
  const isExpanded = toggle.getAttribute("aria-expanded") === "true";
  toggle.setAttribute("aria-expanded", String(!isExpanded));
  toggle.textContent = isExpanded ? toggle.dataset.collapsedLabel : "Hide sub-battles";
  target.hidden = isExpanded;
  if (!isExpanded) requestAnimationFrame(() => initializeSwapChains(target));
});

function connectEvents() {
  const streamState = document.getElementById("streamState");
  const updatedAt = document.getElementById("updatedAt");
  const source = new EventSource("/api/events");

  source.onopen = () => {
    streamState.className = "dot is-live";
    updatedAt.textContent = "Live";
  };

  source.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    state.candidates = payload.candidates || [];
    updatedAt.textContent = `Updated ${new Date().toLocaleTimeString()}`;
    renderLive();
  };

  source.onerror = () => {
    streamState.className = "dot is-error";
    updatedAt.textContent = "Reconnecting";
  };
}

async function initialLoad() {
  const response = await fetch("/api/live");
  const payload = await response.json();
  state.candidates = payload.candidates || [];
  renderLive();
  await loadHistoricalPeriods();
}

function renderLive() {
  const rows = document.getElementById("liveRows");
  const sorted = [...state.candidates].sort((a, b) => Number(b[state.sortKey] || 0) - Number(a[state.sortKey] || 0));
  rows.innerHTML = "";

  if (!sorted.length) {
    rows.innerHTML = `<tr><td colspan="6">Waiting for live article edits from EventStreams.</td></tr>`;
    return;
  }

  for (const item of sorted) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(item.page_title)}</td>
      <td><span class="severity ${item.severity}">${item.severity}</span></td>
      <td>${fmt.format(item.conflict_score)}</td>
      <td>${item.human_edit_count}</td>
      <td>${item.revert_count}</td>
      <td>${item.unique_human_editors}</td>
    `;
    tr.addEventListener("click", () => loadPage(item.wiki, item.page_id));
    rows.appendChild(tr);
  }

  if (!state.selected && sorted[0]) loadPage(sorted[0].wiki, sorted[0].page_id);
}

async function loadPage(wiki, pageId) {
  state.selected = { wiki, pageId };
  const response = await fetch(`/api/pages/${encodeURIComponent(wiki)}/${pageId}`);
  const payload = await response.json();
  renderDetail(payload);
}

function renderDetail(payload) {
  const page = payload.page || {};
  document.getElementById("detailTitle").textContent = page.page_title || "Page Detail";
  const articleLink = document.getElementById("articleLink");
  articleLink.href = payload.links?.article || "#";

  document.getElementById("summaryStats").innerHTML = [
    stat("Score", fmt.format(page.conflict_score || 0)),
    stat("Severity", page.severity || "quiet"),
    stat("Edits", page.human_edit_count || 0),
    stat("Reverts", page.revert_count || 0),
    stat("Editors", page.unique_human_editors || 0),
  ].join("");

  renderTimeline(payload.windows || []);
  renderLinks(payload.links || {});
  renderFeatures(page.features || {});
  renderEdits(payload.edits || []);
}

function renderTimeline(windows) {
  const svg = document.getElementById("timeline");
  svg.innerHTML = "";
  if (windows.length < 2) return;

  const width = 640;
  const height = 160;
  const padding = 18;
  const maxScore = Math.max(100, ...windows.map((item) => Number(item.conflict_score || 0)));
  const chartMax = maxScore > 100 ? maxScore * 1.1 : 100;
  const warThresholdY = height - padding - (75 / chartMax) * (height - padding * 2);
  const points = windows.map((item, index) => {
    const x = padding + (index / Math.max(1, windows.length - 1)) * (width - padding * 2);
    const y = height - padding - (Number(item.conflict_score || 0) / chartMax) * (height - padding * 2);
    return [x, y];
  });

  const path = points.map((point, index) => `${index ? "L" : "M"}${point[0].toFixed(1)} ${point[1].toFixed(1)}`).join(" ");
  svg.insertAdjacentHTML("beforeend", `
    <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" stroke="#d9dee7" />
    <line x1="${padding}" y1="${warThresholdY}" x2="${width - padding}" y2="${warThresholdY}" stroke="#b3261e" stroke-dasharray="4 4" />
    <path d="${path}" fill="none" stroke="#1967d2" stroke-width="3" />
  `);
}

function renderLinks(links) {
  document.getElementById("links").innerHTML = [
    link("History", links.history),
    link("Talk", links.talk),
    links.diff ? link("Diff", links.diff) : "",
  ].join("");
}

function renderFeatures(features) {
  const entries = Object.entries(features).filter(([key]) => key !== "thresholds");
  document.getElementById("features").innerHTML = entries
    .map(([key, value]) => {
      const labelText = label(key);
      return `<dt>${termLabel(labelText)}</dt><dd>${escapeHtml(String(value))}</dd>`;
    })
    .join("");
}

function renderEdits(edits) {
  const list = document.getElementById("edits");
  if (!edits.length) {
    list.innerHTML = "<li>No edits stored yet.</li>";
    return;
  }
  list.innerHTML = edits.slice(0, 12).map((edit) => `
    <li>
      <a href="https://en.wikipedia.org/w/index.php?diff=${edit.rev_id}" target="_blank" rel="noreferrer">r${edit.rev_id}</a>
      by ${escapeHtml(edit.user_text)}
      <div class="comment">${escapeHtml(edit.comment || "No edit summary")}</div>
    </li>
  `).join("");
}

async function loadScoreboard() {
  clearSwapAnimations();
  const value = document.getElementById("scoreboardPeriod").value;
  const isHistorical = value.startsWith("historical:");
  const selectedPeriod = isHistorical ? value.replace("historical:", "") : "";
  const rows = document.getElementById("scoreboardRows");
  rows.innerHTML = `<tr><td colspan="${scoreboardColumnCount(isHistorical)}">Loading scoreboard...</td></tr>`;
  const response = isHistorical
    ? await fetch(`/api/historical/scoreboard?period=${encodeURIComponent(selectedPeriod)}&limit=20`)
    : await fetch(`/api/scoreboard?hours=${value.replace("live:", "")}`);
  const payload = await response.json();
  document.getElementById("scoreboardTable").classList.toggle("is-historical", isHistorical);
  document.getElementById("scoreboardScoreHeader").textContent = isHistorical ? "Controversy" : "Conflict Score";
  document.getElementById("scoreboardMetricA").textContent = isHistorical ? "Evidence" : "Reverts";
  document.getElementById("scoreboardMetricB").textContent = "Participants";
  document.getElementById("scoreboardMetricC").textContent = "Status";
  document.getElementById("scoreboardMetricB").hidden = isHistorical;
  document.getElementById("scoreboardMetricC").hidden = isHistorical;
  rows.innerHTML = "";
  if (!payload.rows.length) {
    rows.innerHTML = `<tr><td colspan="${scoreboardColumnCount(isHistorical)}">No scoreboard rows yet.</td></tr>`;
    return;
  }
  payload.rows.forEach((row) => {
    const score = isHistorical ? (row.controversy_score ?? row.peak_score) : row.peak_score;
    const metricA = isHistorical ? formatEvidence(row) : row.total_reverts;
    const tr = document.createElement("tr");
    tr.className = "scoreboard-row";
    tr.innerHTML = `
        <td class="scoreboard-score-cell">${fmt.format(score)}</td>
        <td class="scoreboard-page-cell" title="${escapeHtml(row.page_title)}">${escapeHtml(row.page_title)}</td>
        <td class="scoreboard-metric-cell">${metricA}</td>
        ${isHistorical ? "" : `<td class="scoreboard-participants-cell">${row.participants}</td>`}
        ${isHistorical ? "" : `<td class="scoreboard-status-cell">${escapeHtml(String(row.status || ""))}</td>`}
    `;
    tr.addEventListener("click", () => toggleScoreboardSegments(tr, row, isHistorical, selectedPeriod));
    rows.appendChild(tr);
  });
}

function scoreboardColumnCount(isHistorical) {
  return isHistorical ? 3 : 5;
}

function formatLength(minutes) {
  return `${fmt.format(Number(minutes || 0))} min`;
}

function formatEvidence(row) {
  if (row.battle_count == null && row.talk_evidence_count == null) {
    const pairs = Number(row.mutual_revert_pairs || 0);
    const reverts = Number(row.revert_count || 0);
    return `${fmt.format(pairs)} pair${pairs === 1 ? "" : "s"}, ${fmt.format(reverts)} reverts`;
  }
  const battles = Number(row.battle_count || 0);
  const talk = Number(row.talk_evidence_count || 0);
  return `${fmt.format(battles)} battle${battles === 1 ? "" : "s"}, ${fmt.format(talk)} talk`;
}

async function toggleScoreboardSegments(rowElement, row, isHistorical, selectedPeriod) {
  const existing = rowElement.nextElementSibling;
  if (existing?.classList.contains("scoreboard-detail-row")) {
    clearSwapAnimations();
    existing.remove();
    rowElement.classList.remove("is-selected");
    return;
  }

  clearSwapAnimations();
  document.querySelectorAll(".scoreboard-detail-row").forEach((item) => item.remove());
  document.querySelectorAll(".scoreboard-row.is-selected").forEach((item) => item.classList.remove("is-selected"));

  rowElement.classList.add("is-selected");
  const detailRow = document.createElement("tr");
  detailRow.className = "scoreboard-detail-row";
  detailRow.innerHTML = `
    <td colspan="${scoreboardColumnCount(isHistorical)}">
      <div class="scoreboard-detail">
        <h3>Battles</h3>
        <p class="muted">Loading battles...</p>
      </div>
    </td>
  `;
  rowElement.after(detailRow);

  const params = new URLSearchParams({
    wiki: row.wiki || "enwiki",
    page_id: String(row.page_id),
    page_title: row.page_title || "",
    historical: String(isHistorical),
  });
  if (selectedPeriod) params.set("period", selectedPeriod);

  try {
    const response = await fetch(`/api/scoreboard/segments?${params.toString()}`);
    const payload = await response.json();
    detailRow.querySelector(".scoreboard-detail").innerHTML = renderScoreboardSegments(payload);
    requestAnimationFrame(() => initializeSwapChains(detailRow));
  } catch (error) {
    detailRow.querySelector(".scoreboard-detail").innerHTML = `
      <h3>Battles</h3>
      <p class="muted">Could not load battles.</p>
    `;
  }
}

function renderScoreboardSegments(payload) {
  const segments = payload.segments || [];
  const evidenceHtml = controversyEvidenceHtml(payload.controversy);
  if (payload.source === "local_evidence_missing") {
    return `
      <h3>Battles</h3>
      <p class="muted">${escapeHtml(payload.message || "Local historical battle evidence has not been backfilled for this page and period yet.")}</p>
    `;
  }
  if (!segments.length) {
    return `
      <h3>Battles</h3>
      ${evidenceHtml}
      <p class="muted">No repeated article-text battles were found in the checked revisions. This row should not receive a high controversy rank.</p>
    `;
  }
  return `
    <h3>Battles</h3>
    ${evidenceHtml}
    <div class="segment-list">
      ${segments.map((segment, index) => `
        <section class="segment-card">
          ${segmentLineHtml(segment)}
          ${relatedSegmentsHtml(segment, `related-${index}-${stableSegmentId(segment)}`)}
        </section>
      `).join("")}
    </div>
  `;
}

function segmentLineHtml(segment, options = {}) {
  const showMetrics = options.showMetrics !== false;
  const className = options.related ? "segment-text is-related" : "segment-text";
  const focusClassName = segmentHasSwapValues(segment) ? "segment-focus has-swaps" : "segment-focus";
  return `
    <div class="${className}">
      <p class="segment-sentence">
        <span class="segment-context">${escapeHtml(segment.context_before || "")}</span>
        <span class="${focusClassName}">
          ${segmentChangeTypeHtml(segment)}
          <span class="segment-focus-main">
            <span class="segment-highlight-line">[${segmentHighlightHtml(segment)}]</span>
          </span>
        </span>
        <span class="segment-context">${escapeHtml(segment.context_after || "")}</span>
      </p>
      <div class="segment-under">
        ${showMetrics ? `
          ${firstShotHtml(segment)}
          <span class="segment-metrics" aria-label="Segment metrics">
            <span class="is-battle-counts">${segment.reverts} reverts, ${segment.changes ?? segment.edits} changes</span>
            <span>${segment.combatants ?? segment.editors} combatants</span>
          </span>
        ` : ""}
      </div>
    </div>
  `;
}

function controversyEvidenceHtml(controversy) {
  if (!controversy) return "";
  const talk = Array.isArray(controversy.talk_evidence) ? controversy.talk_evidence : [];
  return `
    <div class="controversy-evidence">
      <div class="controversy-metrics" aria-label="Controversy evidence">
        <span>Controversy ${fmt.format(controversy.score || 0)}</span>
        <span>Battle ${fmt.format(controversy.battle_score || 0)}</span>
        <span>Talk ${fmt.format(controversy.talk_score || 0)}</span>
        ${controversy.cleanup_penalty ? `<span>Cleanup penalty ${fmt.format(controversy.cleanup_penalty)}</span>` : ""}
      </div>
      ${talk.length ? `
        <div class="talk-evidence">
          ${talk.slice(0, 3).map((item) => `
            <p><span>Talk</span> ${escapeHtml(item.sentence || item.comment || "Talk page dispute signal")}</p>
          `).join("")}
        </div>
      ` : `<p class="muted">No talk-page debate matched these battle terms in this period.</p>`}
    </div>
  `;
}

function firstShotHtml(segment) {
  const comment = String(segment.first_shot_comment || "").trim();
  if (!comment) return "";
  return `<p class="segment-first-shot"><span>First shot</span> ${escapeHtml(comment)}</p>`;
}

function segmentHasSwapValues(segment) {
  return (segment.change_type || "change") === "swap"
    && Array.isArray(segment.swap_values)
    && segment.swap_values.filter(Boolean).length > 0;
}

function relatedSegmentsHtml(segment, targetId) {
  const related = Array.isArray(segment.related_segments) ? segment.related_segments.slice(1) : [];
  if (!related.length) return "";
  const label = `${related.length + 1} sub-battles`;
  return `
    <button class="related-segments-toggle" type="button" data-target="${targetId}" data-collapsed-label="${label}" aria-expanded="false">${label}</button>
    <div class="related-segments" id="${targetId}" aria-label="Related contested phrases from the same reverting revisions" hidden>
      ${related.map((item) => segmentLineHtml(item, { related: true, showMetrics: false })).join("")}
    </div>
  `;
}

function stableSegmentId(segment) {
  const raw = `${segment.segment || ""}-${segment.context_before || ""}-${segment.context_after || ""}`;
  let hash = 0;
  for (let index = 0; index < raw.length; index += 1) {
    hash = ((hash << 5) - hash) + raw.charCodeAt(index);
    hash |= 0;
  }
  return Math.abs(hash).toString(36);
}

function segmentHighlightHtml(segment) {
  if (segmentHasSwapValues(segment)) return segmentSwapChainHtml(segment);
  const before = segment.highlight_before ? `${escapeHtml(segment.highlight_before)} ` : "";
  const changed = escapeHtml(segment.changed_text || segment.segment || "");
  const after = segment.highlight_after ? ` ${escapeHtml(segment.highlight_after)}` : "";
  return `${before}<mark>${changed}</mark>${after}`;
}

function segmentSwapChainHtml(segment) {
  const values = segmentSwapValues(segment);
  const current = values[0] || segment.changed_text || segment.segment || "";
  const before = segment.highlight_before ? `${escapeHtml(segment.highlight_before)} ` : "";
  const after = segment.highlight_after ? ` ${escapeHtml(segment.highlight_after)}` : "";
  return `${before}<span class="swap-chain" data-values="${escapeHtml(JSON.stringify(values))}" aria-label="Swapped terms: ${escapeHtml(values.join(", "))}"><mark class="swap-chain-mark"><span class="swap-chain-text">${escapeHtml(current)}</span></mark></span>${after}`;
}

function segmentChangeTypeHtml(segment) {
  const type = segment.change_type || "change";
  const label = type === "swap" ? "Swap" : type === "deletion" ? "Deletion" : type === "addition" ? "Addition" : "Change";
  return `<span class="segment-kind">${label}</span>`;
}

function segmentSwapValues(segment) {
  const rawValues = Array.isArray(segment.swap_values) ? segment.swap_values : [];
  const values = [...rawValues, segment.changed_text || segment.segment || ""]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  const seen = new Set();
  return values.filter((value) => {
    const key = value.toLocaleLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function initializeSwapChains(root = document) {
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  root.querySelectorAll(".swap-chain").forEach((chain, chainIndex) => {
    if (chain.dataset.swapReady === "true") return;
    if (chain.offsetParent === null) return;
    let values = [];
    try {
      values = JSON.parse(chain.dataset.values || "[]").filter(Boolean);
    } catch {
      values = [];
    }
    const text = chain.querySelector(".swap-chain-text");
    if (!text || !values.length) return;
    chain.dataset.swapReady = "true";
    setSwapChainValue(chain, values[0]);
    if (reduceMotion || values.length < 2) return;
    let index = 0;
    const timer = window.setInterval(() => {
      if (!document.body.contains(chain)) return;
      index = (index + 1) % values.length;
      setSwapChainValue(chain, values[index]);
    }, 1800 + (chainIndex % 4) * 180);
    state.swapAnimationTimers.push(timer);
  });
}

function setSwapChainValue(chain, value) {
  const text = chain.querySelector(".swap-chain-text");
  if (!text) return;
  text.classList.add("is-changing");
  window.setTimeout(() => {
    text.textContent = value;
    text.classList.remove("is-changing");
  }, 110);
}

function clearSwapAnimations() {
  state.swapAnimationTimers.forEach((timer) => window.clearInterval(timer));
  state.swapAnimationTimers = [];
}

async function loadHistoricalPeriods() {
  const response = await fetch("/api/historical/periods");
  const payload = await response.json();
  const group = document.getElementById("historicalPeriods");
  const monthCounts = historicalYearMonthCounts(payload.monthly_periods || []);
  group.innerHTML = "";
  for (const period of payload.periods || []) {
    const option = document.createElement("option");
    option.value = `historical:${period}`;
    option.textContent = formatHistoricalPeriod(period, monthCounts.get(period));
    group.appendChild(option);
  }
}

function historicalYearMonthCounts(monthlyPeriods) {
  const counts = new Map();
  for (const period of monthlyPeriods) {
    const match = String(period || "").match(/^history:(\d{4}-\d{2}):(\d{4})-\d{2}$/);
    if (!match) continue;
    const yearPeriod = `history-year:${match[1]}:${match[2]}`;
    counts.set(yearPeriod, (counts.get(yearPeriod) || 0) + 1);
  }
  return counts;
}

function formatHistoricalPeriod(period, monthCount) {
  const yearMatch = String(period || "").match(/^history-year:(\d{4}-\d{2}):(\d{4})$/);
  if (yearMatch) {
    const countLabel = Number(monthCount || 0) > 0 ? `, ${monthCount}/12 months` : "";
    return `${yearMatch[2]} (${yearMatch[1]} snapshot${countLabel})`;
  }
  const monthMatch = String(period || "").match(/^history:(\d{4}-\d{2}):(\d{4}-\d{2})$/);
  if (monthMatch) return `${monthMatch[2]} (${monthMatch[1]} snapshot)`;
  return String(period || "");
}

function stat(labelText, value) {
  return `<div class="stat"><strong>${escapeHtml(String(value))}</strong><span>${termLabel(labelText)}</span></div>`;
}

function link(labelText, href) {
  if (!href) return "";
  return `<a href="${href}" target="_blank" rel="noreferrer">${labelText}</a>`;
}

function label(key) {
  return key.replaceAll("_", " ").replace(/\b\w/g, (match) => match.toUpperCase());
}

function termLabel(labelText) {
  const description = TERM_DESCRIPTIONS[labelText];
  if (!description) return escapeHtml(labelText);
  return `<span class="term">${escapeHtml(labelText)} ${infoIcon(labelText, description)}</span>`;
}

function infoIcon(labelText, description) {
  const cleanLabel = escapeHtml(labelText);
  const cleanDescription = escapeHtml(description);
  return `<span class="info-icon" tabindex="0" role="img" aria-label="${cleanLabel}: ${cleanDescription}" data-tooltip="${cleanDescription}">i</span>`;
}

function escapeHtml(value) {
  return value.replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#039;",
  }[char]));
}

initialLoad().catch(console.error);
connectEvents();
