import {
    cancelJob,
    compareRuns,
    deleteRun,
    fetchJobs,
    fetchMetadata,
    fetchRunDetail,
    fetchRuns,
    fetchRunTrades,
    fetchStatus,
    renameRun
} from "./api.js";
import { BacktestChartsController, SERIES_PALETTE } from "./charts.js";
import { RunnerModalController } from "./runner-modal.js";

const state = {
    runs: [],
    selectedRunIds: [],
    jobs: [],
    hiddenFinishedJobIds: new Set(),
    granularity: "DAY",
    unit: "USD",
    subchart: "DRAWDOWN",
    sliceFrom: "",
    sliceTo: "",
    pendingZoomRange: null,
    comparison: null,
    showAllParams: false,
    tradesRunId: "",
    tradesPageSize: 50,
    tradesOffset: 0,
    pollTimerId: null,
    prevActiveJobIds: new Set()
};

const charts = new BacktestChartsController(
    document.getElementById("equity-chart"),
    document.getElementById("sub-chart"),
    (zoomRange) => {
        state.pendingZoomRange = zoomRange;
        document.getElementById("btn-zoom-to-slice").disabled = !zoomRange;
    }
);

const runnerModal = new RunnerModalController(async () => {
    await refreshJobs();
    startPollingIfNeeded();
});

const renameDialog = document.getElementById("rename-dialog");
const renameInput = document.getElementById("rename-input");
const deleteDialog = document.getElementById("delete-dialog");
const deleteMessage = document.getElementById("delete-message");

let pendingRenameRunId = null;
let pendingDeleteRunId = null;

async function init() {
    bindToolbarEvents();
    bindDialogEvents();

    try {
        const [statusRes, metaRes, runsRes, jobsRes] = await Promise.all([
            fetchStatus(),
            fetchMetadata(),
            fetchRuns(),
            fetchJobs()
        ]);

        const statusBadge = document.getElementById("engine-status-badge");
        statusBadge.textContent = statusRes?.status === "READY" ? "Engine READY" : (statusRes?.status || "UNKNOWN");
        statusBadge.className = statusRes?.status === "READY" ? "badge badge-ok" : "badge badge-warn";

        runnerModal.setMetadata(metaRes);
        state.runs = runsRes || [];
        state.jobs = jobsRes || [];

        for (const job of state.jobs) {
            if (job.status === "QUEUED" || job.status === "IN_PROGRESS") {
                state.prevActiveJobIds.add(job.jobId);
            }
        }

        if (state.runs.length > 0) {
            state.selectedRunIds = state.runs.slice(0, Math.min(2, state.runs.length)).map((r) => r.runId);
        }

        populateRunFilterSelects();
        renderJobs();
        renderRunsList();
        await refreshComparisonAndTrades();
        startPollingIfNeeded();
    } catch (err) {
        const statusBadge = document.getElementById("engine-status-badge");
        statusBadge.textContent = `Chyba: ${err.message}`;
        statusBadge.className = "badge badge-err";
    }
}

function bindToolbarEvents() {
    document.getElementById("btn-new-backtest").addEventListener("click", () => {
        runnerModal.openForNew();
    });

    document.getElementById("btn-clear-finished-jobs").addEventListener("click", () => {
        for (const job of state.jobs) {
            if (job.status !== "QUEUED" && job.status !== "IN_PROGRESS") {
                state.hiddenFinishedJobIds.add(job.jobId);
            }
        }
        renderJobs();
    });

    document.getElementById("filter-run-search").addEventListener("input", renderRunsList);
    document.getElementById("filter-run-strategy").addEventListener("change", renderRunsList);
    document.getElementById("filter-run-symbol").addEventListener("change", renderRunsList);

    document.getElementById("btn-compare-same-strategy").addEventListener("click", async () => {
        const anchorId = state.selectedRunIds[0] || state.runs[0]?.runId;
        const anchor = state.runs.find((r) => r.runId === anchorId);
        if (!anchor) return;
        state.selectedRunIds = state.runs
            .filter((r) => r.strategyName === anchor.strategyName)
            .map((r) => r.runId);
        renderRunsList();
        await refreshComparisonAndTrades();
    });

    document.getElementById("btn-clear-selection").addEventListener("click", async () => {
        state.selectedRunIds = [];
        renderRunsList();
        await refreshComparisonAndTrades();
    });

    document.querySelectorAll("[data-granularity]").forEach((btn) => {
        btn.addEventListener("click", async () => {
            state.granularity = btn.dataset.granularity;
            updateSegmentedActive("[data-granularity]", "granularity", state.granularity);
            await refreshComparisonAndTrades();
        });
    });

    document.querySelectorAll("[data-unit]").forEach((btn) => {
        btn.addEventListener("click", () => {
            state.unit = btn.dataset.unit;
            updateSegmentedActive("[data-unit]", "unit", state.unit);
            renderChartsAndMetrics();
        });
    });

    document.querySelectorAll("[data-subchart]").forEach((btn) => {
        btn.addEventListener("click", () => {
            state.subchart = btn.dataset.subchart;
            updateSegmentedActive("[data-subchart]", "subchart", state.subchart);
            document.getElementById("subchart-title").textContent =
                state.subchart === "DRAWDOWN" ? "Drawdown (%)" : "PnL za období ($)";
            renderChartsAndMetrics();
        });
    });

    document.getElementById("btn-apply-slice").addEventListener("click", async () => {
        state.sliceFrom = document.getElementById("slice-from").value;
        state.sliceTo = document.getElementById("slice-to").value;
        state.tradesOffset = 0;
        await refreshComparisonAndTrades();
    });

    document.querySelectorAll("[data-days]").forEach((btn) => {
        btn.addEventListener("click", async () => {
            const days = Number(btn.dataset.days);
            const maxDate = state.comparison?.availableMaxDate || new Date().toISOString().slice(0, 10);
            const end = new Date(`${maxDate}T00:00:00Z`);
            const start = new Date(end.getTime() - (days - 1) * 86400000);
            state.sliceFrom = start.toISOString().slice(0, 10);
            state.sliceTo = maxDate;
            document.getElementById("slice-from").value = state.sliceFrom;
            document.getElementById("slice-to").value = state.sliceTo;
            state.tradesOffset = 0;
            await refreshComparisonAndTrades();
        });
    });

    document.getElementById("btn-zoom-to-slice").addEventListener("click", async () => {
        if (!state.pendingZoomRange) return;
        state.sliceFrom = state.pendingZoomRange.fromDate;
        state.sliceTo = state.pendingZoomRange.toDate;
        document.getElementById("slice-from").value = state.sliceFrom;
        document.getElementById("slice-to").value = state.sliceTo;
        state.tradesOffset = 0;
        await refreshComparisonAndTrades();
    });

    document.getElementById("btn-reset-slice").addEventListener("click", async () => {
        state.sliceFrom = "";
        state.sliceTo = "";
        document.getElementById("slice-from").value = "";
        document.getElementById("slice-to").value = "";
        state.tradesOffset = 0;
        await refreshComparisonAndTrades();
    });

    document.getElementById("toggle-show-all-params").addEventListener("change", (e) => {
        state.showAllParams = e.target.checked;
        renderDiffTable();
    });

    document.getElementById("trades-run-select").addEventListener("change", async (e) => {
        state.tradesRunId = e.target.value;
        state.tradesOffset = 0;
        await refreshTrades();
    });

    document.getElementById("trades-side-filter").addEventListener("change", async () => {
        state.tradesOffset = 0;
        await refreshTrades();
    });

    document.getElementById("trades-outcome-filter").addEventListener("change", async () => {
        state.tradesOffset = 0;
        await refreshTrades();
    });

    document.getElementById("trades-search").addEventListener("input", async () => {
        state.tradesOffset = 0;
        await refreshTrades();
    });

    document.getElementById("btn-trades-prev").addEventListener("click", async () => {
        state.tradesOffset = Math.max(0, state.tradesOffset - state.tradesPageSize);
        await refreshTrades();
    });

    document.getElementById("btn-trades-next").addEventListener("click", async () => {
        state.tradesOffset += state.tradesPageSize;
        await refreshTrades();
    });
}

function bindDialogEvents() {
    renameDialog.addEventListener("close", async () => {
        const val = renameDialog.returnValue;
        renameDialog.returnValue = "";
        const targetId = pendingRenameRunId;
        pendingRenameRunId = null;
        if (val === "confirm" && targetId) {
            const newLabel = renameInput.value.trim();
            if (!newLabel) return;
            await renameRun(targetId, newLabel);
            await reloadRuns();
            await refreshComparisonAndTrades();
        }
    });

    deleteDialog.addEventListener("close", async () => {
        const val = deleteDialog.returnValue;
        deleteDialog.returnValue = "";
        const targetId = pendingDeleteRunId;
        pendingDeleteRunId = null;
        if (val === "delete" && targetId) {
            await deleteRun(targetId);
            state.selectedRunIds = state.selectedRunIds.filter((id) => id !== targetId);
            await reloadRuns();
            await refreshComparisonAndTrades();
        }
    });
}

async function reloadRuns() {
    state.runs = await fetchRuns();
    populateRunFilterSelects();
    renderRunsList();
}

async function refreshJobs() {
    state.jobs = await fetchJobs();
    let newlyCompletedRunIds = [];
    const currentActiveIds = new Set();

    for (const job of state.jobs) {
        if (job.status === "QUEUED" || job.status === "IN_PROGRESS") {
            currentActiveIds.add(job.jobId);
            state.hiddenFinishedJobIds.delete(job.jobId);
        } else if (job.status === "COMPLETED" && state.prevActiveJobIds.has(job.jobId)) {
            if (job.runId) {
                newlyCompletedRunIds.push(job.runId);
            }
        }
    }
    state.prevActiveJobIds = currentActiveIds;
    renderJobs();

    if (newlyCompletedRunIds.length > 0) {
        await reloadRuns();
        for (const runId of newlyCompletedRunIds) {
            if (!state.selectedRunIds.includes(runId)) {
                state.selectedRunIds.unshift(runId);
            }
        }
        renderRunsList();
        await refreshComparisonAndTrades();
    }
}

function startPollingIfNeeded() {
    const hasActive = state.jobs.some((j) => j.status === "QUEUED" || j.status === "IN_PROGRESS");
    if (hasActive && !state.pollTimerId) {
        state.pollTimerId = window.setInterval(async () => {
            try {
                await refreshJobs();
                const stillActive = state.jobs.some((j) => j.status === "QUEUED" || j.status === "IN_PROGRESS");
                if (!stillActive && state.pollTimerId) {
                    window.clearInterval(state.pollTimerId);
                    state.pollTimerId = null;
                }
            } catch {
                // Keep polling transiently if server restarts
            }
        }, 1000);
    }
}

function renderJobs() {
    const panel = document.getElementById("jobs-panel");
    const list = document.getElementById("jobs-list");
    const visible = state.jobs.filter((j) => !state.hiddenFinishedJobIds.has(j.jobId));

    if (visible.length === 0) {
        panel.classList.add("hidden");
        list.innerHTML = "";
        return;
    }

    panel.classList.remove("hidden");
    list.innerHTML = "";

    for (const job of visible) {
        const item = document.createElement("div");
        item.className = "job-item";

        const top = document.createElement("div");
        top.className = "job-top";

        const title = document.createElement("span");
        title.className = "job-name";
        title.textContent = job.label || job.strategyName;
        title.title = job.label || job.strategyName;

        const badge = document.createElement("span");
        badge.className = `badge ${statusBadgeClass(job.status)}`;
        badge.textContent = job.status;
        top.append(title, badge);

        const meta = document.createElement("div");
        meta.className = "job-meta";
        const pct = (job.progressPct || 0).toFixed(0);
        if (job.status === "IN_PROGRESS") {
            meta.textContent = `${job.completedDays}/${job.totalDays || "?"} dnů · ${job.completedMarkets}/${job.totalMarkets || "?"} trhů (${pct}%)${job.currentDate ? " · " + job.currentDate : ""}`;
        } else if (job.status === "QUEUED") {
            meta.textContent = `${job.strategyName} · ${job.symbol} · Čeká ve frontě…`;
        } else if (job.status === "COMPLETED") {
            const pnl = job.metrics ? formatSignedUsd(job.metrics.totalNetPnl) : "";
            meta.textContent = `${job.totalMarkets ?? 0} trhů · ${job.tradeCount ?? 0} obchodů ${pnl}`;
        } else if (job.status === "FAILED") {
            meta.textContent = job.error || "Běh selhal";
        } else {
            meta.textContent = "Zrušeno uživatelem";
        }

        item.append(top, meta);

        if (job.status === "IN_PROGRESS" || job.status === "QUEUED") {
            const track = document.createElement("div");
            track.className = "progress-track";
            const fill = document.createElement("div");
            fill.className = "progress-fill";
            fill.style.width = `${job.status === "IN_PROGRESS" ? Math.max(4, job.progressPct || 0) : 0}%`;
            track.appendChild(fill);

            const cancelRow = document.createElement("div");
            cancelRow.style.marginTop = "6px";
            cancelRow.style.display = "flex";
            cancelRow.style.justifyContent = "flex-end";
            const cancelBtn = document.createElement("button");
            cancelBtn.type = "button";
            cancelBtn.className = "btn btn-xs btn-secondary";
            cancelBtn.textContent = "Zrušit";
            cancelBtn.addEventListener("click", async () => {
                await cancelJob(job.jobId);
                await refreshJobs();
            });
            cancelRow.appendChild(cancelBtn);
            item.append(track, cancelRow);
        }

        list.appendChild(item);
    }
}

function populateRunFilterSelects() {
    const stratSelect = document.getElementById("filter-run-strategy");
    const symSelect = document.getElementById("filter-run-symbol");
    const prevStrat = stratSelect.value;
    const prevSym = symSelect.value;

    const strategies = Array.from(new Set(state.runs.map((r) => r.strategyName))).sort();
    const symbols = Array.from(new Set(state.runs.map((r) => r.symbol))).sort();

    stratSelect.innerHTML = '<option value="">Všechny strategie</option>';
    for (const s of strategies) {
        const o = document.createElement("option");
        o.value = s;
        o.textContent = s;
        stratSelect.appendChild(o);
    }
    stratSelect.value = strategies.includes(prevStrat) ? prevStrat : "";

    symSelect.innerHTML = '<option value="">Všechny symboly</option>';
    for (const sym of symbols) {
        const o = document.createElement("option");
        o.value = sym;
        o.textContent = sym;
        symSelect.appendChild(o);
    }
    symSelect.value = symbols.includes(prevSym) ? prevSym : "";

    document.getElementById("runs-count-badge").textContent = `${state.runs.length} běhů`;
}

function getColorByRunId() {
    const map = new Map();
    state.selectedRunIds.forEach((id, idx) => {
        map.set(id, SERIES_PALETTE[idx % SERIES_PALETTE.length]);
    });
    return map;
}

function renderRunsList() {
    const list = document.getElementById("runs-list");
    const query = document.getElementById("filter-run-search").value.trim().toLowerCase();
    const stratFilter = document.getElementById("filter-run-strategy").value;
    const symFilter = document.getElementById("filter-run-symbol").value;
    const colorMap = getColorByRunId();

    document.getElementById("selected-count-pill").textContent = `${state.selectedRunIds.length} vybráno`;

    const filtered = state.runs.filter((run) => {
        if (stratFilter && run.strategyName !== stratFilter) return false;
        if (symFilter && run.symbol !== symFilter) return false;
        if (query) {
            const hay = `${run.label} ${run.strategyName} ${run.symbol} ${run.startDate} ${run.endDate}`.toLowerCase();
            if (!hay.includes(query)) return false;
        }
        return true;
    });

    list.innerHTML = "";
    if (filtered.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty-state";
        empty.style.padding = "20px";
        empty.textContent = state.runs.length === 0
            ? "V DuckDB databázi zatím nejsou žádné backtesty."
            : "Žádný běh neodpovídá filtru.";
        list.appendChild(empty);
        return;
    }

    for (const run of filtered) {
        const isSelected = state.selectedRunIds.includes(run.runId);
        const card = document.createElement("div");
        card.className = `run-item${isSelected ? " selected" : ""}`;
        card.setAttribute("role", "listitem");

        const top = document.createElement("div");
        top.className = "run-item-top";

        const labelEl = document.createElement("label");
        labelEl.className = "run-check-label";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = isSelected;
        checkbox.addEventListener("change", async () => {
            if (checkbox.checked) {
                if (!state.selectedRunIds.includes(run.runId)) {
                    state.selectedRunIds.push(run.runId);
                }
            } else {
                state.selectedRunIds = state.selectedRunIds.filter((id) => id !== run.runId);
            }
            renderRunsList();
            await refreshComparisonAndTrades();
        });

        const dot = document.createElement("span");
        dot.className = "color-dot";
        dot.style.backgroundColor = isSelected ? (colorMap.get(run.runId) || "#2563eb") : "#cbd5e1";

        const titleSpan = document.createElement("span");
        titleSpan.className = "run-title";
        titleSpan.textContent = run.label;

        labelEl.append(checkbox, dot, titleSpan);

        const actions = document.createElement("div");
        actions.className = "run-actions";

        const cloneBtn = document.createElement("button");
        cloneBtn.type = "button";
        cloneBtn.className = "btn-icon";
        cloneBtn.title = "Klonovat & upravit";
        cloneBtn.textContent = "⧉";
        cloneBtn.addEventListener("click", async () => {
            const detail = await fetchRunDetail(run.runId);
            runnerModal.openForClone(detail);
        });

        const renameBtn = document.createElement("button");
        renameBtn.type = "button";
        renameBtn.className = "btn-icon";
        renameBtn.title = "Přejmenovat";
        renameBtn.textContent = "✎";
        renameBtn.addEventListener("click", () => {
            pendingRenameRunId = run.runId;
            renameInput.value = run.label;
            renameDialog.showModal();
        });

        const delBtn = document.createElement("button");
        delBtn.type = "button";
        delBtn.className = "btn-icon btn-icon-danger";
        delBtn.title = "Smazat";
        delBtn.textContent = "🗑";
        delBtn.addEventListener("click", () => {
            pendingDeleteRunId = run.runId;
            deleteMessage.textContent = `Opravdu chcete trvale smazat běh „${run.label}“?`;
            deleteDialog.showModal();
        });

        actions.append(cloneBtn, renameBtn, delBtn);
        top.append(labelEl, actions);

        const meta = document.createElement("div");
        meta.className = "run-meta";
        const overrideCount = run.strategyOverrides ? Object.keys(run.strategyOverrides).length : 0;
        meta.textContent = `${run.strategyName} · ${run.symbol} · ${run.startDate}..${run.endDate}`
            + (overrideCount > 0 ? ` · ${overrideCount} úprav` : "");

        const stats = document.createElement("div");
        stats.className = "run-stats";
        const pnl = run.metrics?.totalNetPnl ?? 0;
        const pnlSpan = document.createElement("span");
        pnlSpan.className = pnl >= 0 ? "pos" : "neg";
        pnlSpan.textContent = formatSignedUsd(pnl);

        const detailSpan = document.createElement("span");
        detailSpan.className = "muted";
        detailSpan.textContent = `${run.metrics?.totalTrades ?? 0} obch. · WR ${(run.metrics?.winRatePct ?? 0).toFixed(1)}% · DD ${(run.metrics?.maxDrawdownPct ?? 0).toFixed(1)}%`;

        stats.append(pnlSpan, detailSpan);
        card.append(top, meta, stats);
        list.appendChild(card);
    }
}

async function refreshComparisonAndTrades() {
    if (state.selectedRunIds.length === 0) {
        state.comparison = null;
        renderChartsAndMetrics();
        renderDiffTable();
        updateTradesSectionVisibility();
        return;
    }

    try {
        state.comparison = await compareRuns(
            state.selectedRunIds,
            state.granularity,
            state.sliceFrom || null,
            state.sliceTo || null
        );
    } catch (err) {
        document.getElementById("chart-subtitle").textContent = `Chyba načítání porovnání: ${err.message}`;
        return;
    }

    renderChartsAndMetrics();
    renderDiffTable();
    syncTradesRunSelector();
    await refreshTrades();
}

function renderChartsAndMetrics() {
    const emptyEl = document.getElementById("chart-empty-state");
    const containerEl = document.getElementById("charts-container");
    const subtitleEl = document.getElementById("chart-subtitle");
    const sliceBadge = document.getElementById("slice-badge");

    if (!state.comparison || !state.comparison.series || state.comparison.series.length === 0) {
        emptyEl.classList.remove("hidden");
        containerEl.classList.add("hidden");
        sliceBadge.classList.add("hidden");
        subtitleEl.textContent = "Vyberte alespoň jeden běh z historie vlevo.";
        charts.clear();
        renderMetricsTable([]);
        return;
    }

    emptyEl.classList.add("hidden");
    containerEl.classList.remove("hidden");

    const granText = state.granularity === "DAY" ? "Agregace po UTC dnech" : "Všechny vyhodnocené 15m trhy";
    subtitleEl.textContent = `${granText} · ${state.comparison.series.length} vybraných běhů`;

    if (state.sliceFrom || state.sliceTo) {
        sliceBadge.textContent = `Výsek: ${state.sliceFrom || state.comparison.availableMinDate} – ${state.sliceTo || state.comparison.availableMaxDate} (od 0)`;
        sliceBadge.classList.remove("hidden");
    } else {
        sliceBadge.classList.add("hidden");
    }

    charts.render(state.comparison, {
        unit: state.unit,
        subchartType: state.subchart,
        colorByRunId: getColorByRunId()
    });
    renderMetricsTable(state.comparison.series);
}

function renderMetricsTable(seriesList) {
    const tbody = document.getElementById("metrics-tbody");
    tbody.innerHTML = "";
    if (!seriesList || seriesList.length === 0) {
        tbody.innerHTML = '<tr><td colspan="15" class="empty-cell">Žádná data k zobrazení.</td></tr>';
        return;
    }

    const bestNetPnl = Math.max(...seriesList.map((s) => s.sliceMetrics.totalNetPnl));
    const bestWinRate = Math.max(...seriesList.map((s) => s.sliceMetrics.winRatePct));
    const bestSharpe = Math.max(...seriesList.map((s) => s.sliceMetrics.sharpeRatio));
    const colorMap = getColorByRunId();

    for (const s of seriesList) {
        const m = s.sliceMetrics;
        const retPct = m.initialCapital > 0 ? (m.totalNetPnl / m.initialCapital) * 100 : 0;
        const tr = document.createElement("tr");

        const th = document.createElement("th");
        const dot = document.createElement("span");
        dot.className = "color-dot";
        dot.style.display = "inline-block";
        dot.style.marginRight = "7px";
        dot.style.backgroundColor = colorMap.get(s.run.runId) || "#2563eb";
        th.append(dot, document.createTextNode(s.run.label));
        tr.appendChild(th);

        appendCell(tr, s.run.strategyName);
        appendCell(tr, s.run.symbol);
        appendNumCell(tr, String(s.sliceMarketsCount));
        appendNumCell(tr, `${m.totalTrades} (${m.winningTrades}W/${m.losingTrades}L)`);
        appendNumCell(tr, `${m.winRatePct.toFixed(1)}%`, seriesList.length > 1 && m.winRatePct === bestWinRate ? "best-val" : "");
        appendNumCell(
            tr,
            formatSignedUsd(m.totalNetPnl),
            `${m.totalNetPnl >= 0 ? "pos" : "neg"}${seriesList.length > 1 && m.totalNetPnl === bestNetPnl ? " best-val" : ""}`
        );
        appendNumCell(tr, `${retPct >= 0 ? "+" : ""}${retPct.toFixed(2)}%`, retPct >= 0 ? "pos" : "neg");
        appendNumCell(tr, m.profitFactor.toFixed(2));
        appendNumCell(tr, `-$${m.maxDrawdownUsd.toFixed(2)}`);
        appendNumCell(tr, `-${m.maxDrawdownPct.toFixed(2)}%`);
        appendNumCell(tr, m.sharpeRatio.toFixed(2), seriesList.length > 1 && m.sharpeRatio === bestSharpe ? "best-val" : "");
        appendNumCell(tr, m.sortinoRatio.toFixed(2));
        appendNumCell(tr, `$${m.totalFees.toFixed(2)}`);
        appendNumCell(tr, m.brierScore.toFixed(4));

        tbody.appendChild(tr);
    }
}

function renderDiffTable() {
    const section = document.getElementById("diff-section");
    const thead = document.getElementById("diff-thead");
    const tbody = document.getElementById("diff-tbody");

    if (!state.comparison || !state.comparison.series || state.comparison.series.length < 2) {
        section.classList.add("hidden");
        return;
    }
    section.classList.remove("hidden");

    const seriesList = state.comparison.series;
    const rows = (state.comparison.parameterDiffs || []).filter((r) => state.showAllParams || r.differs);
    const colorMap = getColorByRunId();

    thead.innerHTML = "";
    const headTr = document.createElement("tr");
    const thScope = document.createElement("th");
    thScope.textContent = "Skupina";
    const thParam = document.createElement("th");
    thParam.textContent = "Parametr";
    headTr.append(thScope, thParam);
    for (const s of seriesList) {
        const th = document.createElement("th");
        const dot = document.createElement("span");
        dot.className = "color-dot";
        dot.style.display = "inline-block";
        dot.style.marginRight = "6px";
        dot.style.backgroundColor = colorMap.get(s.run.runId) || "#2563eb";
        th.append(dot, document.createTextNode(s.run.label));
        headTr.appendChild(th);
    }
    thead.appendChild(headTr);

    tbody.innerHTML = "";
    if (rows.length === 0) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = 2 + seriesList.length;
        td.className = "empty-cell";
        td.textContent = "Vybrané běhy mají identické všechny parametry enginu i strategie.";
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
    }

    for (const r of rows) {
        const tr = document.createElement("tr");
        if (r.differs) {
            tr.className = "diff-row";
        }
        appendCell(tr, r.scope);
        const paramTh = document.createElement("th");
        paramTh.textContent = r.parameter;
        tr.appendChild(paramTh);
        for (const s of seriesList) {
            const val = r.valuesByRunId ? r.valuesByRunId[s.run.runId] : null;
            appendCell(tr, val === null || val === undefined ? "—" : String(val));
        }
        tbody.appendChild(tr);
    }
}

function updateTradesSectionVisibility() {
    const section = document.getElementById("trades-section");
    if (state.selectedRunIds.length === 0) {
        section.classList.add("hidden");
    } else {
        section.classList.remove("hidden");
    }
}

function syncTradesRunSelector() {
    updateTradesSectionVisibility();
    const select = document.getElementById("trades-run-select");
    const runs = (state.comparison?.series || []).map((s) => s.run);
    select.innerHTML = "";
    for (const r of runs) {
        const o = document.createElement("option");
        o.value = r.runId;
        o.textContent = r.label;
        select.appendChild(o);
    }
    if (!runs.some((r) => r.runId === state.tradesRunId)) {
        state.tradesRunId = runs[0]?.runId || "";
        state.tradesOffset = 0;
    }
    select.value = state.tradesRunId;
}

async function refreshTrades() {
    if (!state.tradesRunId) {
        updateTradesSectionVisibility();
        return;
    }
    const side = document.getElementById("trades-side-filter").value;
    const outcome = document.getElementById("trades-outcome-filter").value;
    const search = document.getElementById("trades-search").value.trim();

    try {
        const page = await fetchRunTrades(state.tradesRunId, {
            fromDate: state.sliceFrom || null,
            toDate: state.sliceTo || null,
            side,
            outcome,
            search,
            limit: state.tradesPageSize,
            offset: state.tradesOffset
        });
        renderTradesTable(page);
    } catch (err) {
        document.getElementById("trades-summary").textContent = `Chyba načítání obchodů: ${err.message}`;
    }
}

function renderTradesTable(page) {
    const tbody = document.getElementById("trades-tbody");
    const summaryEl = document.getElementById("trades-summary");
    const pageInfoEl = document.getElementById("trades-page-info");
    const prevBtn = document.getElementById("btn-trades-prev");
    const nextBtn = document.getElementById("btn-trades-next");

    const trades = page.trades || [];
    const total = page.totalCount || 0;
    summaryEl.textContent = `Celkem ${total} obchodů v aktivním výseku`;

    const currentPage = Math.floor(state.tradesOffset / state.tradesPageSize) + 1;
    const totalPages = Math.max(1, Math.ceil(total / state.tradesPageSize));
    pageInfoEl.textContent = `Strana ${currentPage} / ${totalPages}`;
    prevBtn.disabled = state.tradesOffset <= 0;
    nextBtn.disabled = state.tradesOffset + state.tradesPageSize >= total;

    tbody.innerHTML = "";
    if (trades.length === 0) {
        tbody.innerHTML = '<tr><td colspan="12" class="empty-cell">Žádné obchody pro zvolený filtr/výsek.</td></tr>';
        return;
    }

    for (const t of trades) {
        const tr = document.createElement("tr");
        appendCell(tr, t.entryTimeUtc);
        appendNumCell(tr, String(t.intervalStartSec));

        const sideTd = document.createElement("td");
        const sideBadge = document.createElement("span");
        sideBadge.className = `badge ${t.side === "UP" ? "badge-ok" : "badge-err"}`;
        sideBadge.textContent = t.side;
        sideTd.appendChild(sideBadge);
        tr.appendChild(sideTd);

        appendNumCell(tr, t.entryPrice.toFixed(4));
        appendNumCell(tr, t.exitPrice.toFixed(4));
        appendNumCell(tr, `$${t.sizeUsd.toFixed(2)}`);
        appendNumCell(tr, t.shares.toFixed(2));
        appendNumCell(tr, `$${t.feeUsd.toFixed(4)}`);
        appendNumCell(tr, formatSignedUsd(t.netPnl), t.netPnl >= 0 ? "pos" : "neg");
        appendNumCell(tr, t.edge.toFixed(4));
        appendCell(tr, t.exitReason || "—");
        appendNumCell(tr, `$${t.balanceAfterTrade.toFixed(2)}`);
        tbody.appendChild(tr);
    }
}

function updateSegmentedActive(selector, dataKey, value) {
    document.querySelectorAll(selector).forEach((btn) => {
        btn.classList.toggle("active", btn.dataset[dataKey] === value);
    });
}

function appendCell(tr, text) {
    const td = document.createElement("td");
    td.textContent = text;
    tr.appendChild(td);
}

function appendNumCell(tr, text, extraClass = "") {
    const td = document.createElement("td");
    td.className = `num${extraClass ? " " + extraClass : ""}`;
    td.textContent = text;
    tr.appendChild(td);
}

function statusBadgeClass(status) {
    switch (status) {
        case "COMPLETED": return "badge-ok";
        case "IN_PROGRESS": return "badge-info";
        case "QUEUED": return "badge-warn";
        case "FAILED": return "badge-err";
        default: return "badge-neutral";
    }
}

function formatSignedUsd(val) {
    const num = Number(val) || 0;
    const sign = num >= 0 ? "+" : "-";
    return `${sign}$${Math.abs(num).toFixed(2)}`;
}

init();
