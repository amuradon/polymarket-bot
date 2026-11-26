export const SERIES_PALETTE = [
    "#2563eb",
    "#16a34a",
    "#ea580c",
    "#9333ea",
    "#0891b2",
    "#dc2626",
    "#ca8a04",
    "#db2777",
    "#4f46e5",
    "#059669"
];

export class BacktestChartsController {
    constructor(mainEl, subEl, onZoomRangeChange) {
        this.mainEl = mainEl;
        this.subEl = subEl;
        this.onZoomRangeChange = onZoomRangeChange;
        this.equityChart = null;
        this.subChart = null;
        this.resizeObserver = null;
        this.lastTimestamps = [];
        this.lastSeriesData = [];
    }

    ensureInitialized() {
        if (this.equityChart && this.subChart) {
            return;
        }
        const echarts = window.echarts;
        if (!echarts) {
            throw new Error("Apache ECharts library failed to load");
        }
        this.equityChart = echarts.init(this.mainEl, null, { renderer: "canvas" });
        this.subChart = echarts.init(this.subEl, null, { renderer: "canvas" });
        echarts.connect([this.equityChart, this.subChart]);

        this.equityChart.on("dataZoom", () => {
            this.handleZoomChanged();
        });

        this.resizeObserver = new ResizeObserver(() => {
            if (this.equityChart) this.equityChart.resize();
            if (this.subChart) this.subChart.resize();
        });
        this.resizeObserver.observe(this.mainEl);
        this.resizeObserver.observe(this.subEl);
    }

    handleZoomChanged() {
        if (!this.equityChart || !this.onZoomRangeChange || this.lastTimestamps.length === 0) {
            return;
        }
        const opt = this.equityChart.getOption();
        const dz = opt && Array.isArray(opt.dataZoom) && opt.dataZoom.length > 0 ? opt.dataZoom[0] : null;
        if (!dz) {
            this.onZoomRangeChange(null);
            return;
        }
        const startPct = dz.start ?? 0;
        const endPct = dz.end ?? 100;
        if (startPct <= 0.1 && endPct >= 99.9) {
            this.onZoomRangeChange(null);
            return;
        }

        let minTs = null;
        let maxTs = null;
        if (typeof dz.startValue === "number" && typeof dz.endValue === "number") {
            minTs = dz.startValue;
            maxTs = dz.endValue;
        } else {
            const first = this.lastTimestamps[0];
            const last = this.lastTimestamps[this.lastTimestamps.length - 1];
            const span = last - first;
            minTs = first + (span * startPct) / 100;
            maxTs = first + (span * endPct) / 100;
        }

        const fromDate = new Date(minTs).toISOString().slice(0, 10);
        const toDate = new Date(maxTs).toISOString().slice(0, 10);
        if (fromDate <= toDate) {
            this.onZoomRangeChange({ fromDate, toDate });
        } else {
            this.onZoomRangeChange(null);
        }
    }

    render(comparison, { unit, subchartType, colorByRunId }) {
        this.ensureInitialized();

        const seriesList = comparison && Array.isArray(comparison.series) ? comparison.series : [];
        this.lastSeriesData = seriesList;
        const tsSet = new Set();
        for (const s of seriesList) {
            for (const pt of (s.points || [])) {
                tsSet.add(pt.timestampMs);
            }
        }
        this.lastTimestamps = Array.from(tsSet).sort((a, b) => a - b);

        const isPct = unit === "PCT";
        const isDay = comparison && comparison.granularity === "DAY";

        const equitySeries = seriesList.map((s, idx) => {
            const color = colorByRunId.get(s.run.runId) || SERIES_PALETTE[idx % SERIES_PALETTE.length];
            const points = s.points || [];
            const data = points.map((pt) => [
                pt.timestampMs,
                isPct ? pt.returnPct : pt.cumulativePnlUsd,
                pt
            ]);
            return {
                name: s.run.label,
                type: "line",
                smooth: false,
                showSymbol: points.length <= 3,
                symbolSize: 6,
                sampling: points.length > 2000 ? "lttb" : undefined,
                lineStyle: { width: 2.2, color },
                itemStyle: { color },
                areaStyle: seriesList.length === 1 ? {
                    color: "rgba(37, 99, 235, 0.08)"
                } : undefined,
                markLine: idx === 0 ? {
                    silent: true,
                    symbol: "none",
                    lineStyle: { type: "dashed", color: "#94a3b8", width: 1 },
                    label: { show: false },
                    data: [{ yAxis: 0 }]
                } : undefined,
                data
            };
        });

        const equityOption = {
            animation: false,
            backgroundColor: "#ffffff",
            color: seriesList.map((s, i) => colorByRunId.get(s.run.runId) || SERIES_PALETTE[i % SERIES_PALETTE.length]),
            tooltip: {
                trigger: "axis",
                backgroundColor: "#ffffff",
                borderColor: "#cbd5e1",
                borderWidth: 1,
                textStyle: { color: "#0f172a", fontSize: 12 },
                axisPointer: { type: "cross", lineStyle: { color: "#94a3b8" } },
                formatter: (params) => formatMainTooltip(params, isPct, isDay)
            },
            legend: {
                type: "scroll",
                top: 4,
                textStyle: { color: "#334155", fontSize: 12 }
            },
            grid: {
                left: 64,
                right: 24,
                top: 42,
                bottom: 58
            },
            xAxis: {
                type: "time",
                axisLine: { lineStyle: { color: "#cbd5e1" } },
                axisLabel: { color: "#475569", fontSize: 11 },
                splitLine: { show: false }
            },
            yAxis: {
                type: "value",
                name: isPct ? "Výnos (%)" : "Kumulativní PnL ($)",
                nameTextStyle: { color: "#64748b", fontSize: 11 },
                axisLabel: {
                    color: "#475569",
                    fontSize: 11,
                    formatter: (val) => isPct ? `${val.toFixed(1)}%` : `$${formatCompactNum(val)}`
                },
                splitLine: { lineStyle: { color: "#f1f5f9" } }
            },
            dataZoom: [
                { type: "inside", xAxisIndex: 0, filterMode: "none" },
                {
                    type: "slider",
                    xAxisIndex: 0,
                    height: 22,
                    bottom: 8,
                    borderColor: "#e2e8f0",
                    fillerColor: "rgba(37, 99, 235, 0.12)",
                    handleStyle: { color: "#2563eb" },
                    textStyle: { color: "#475569" },
                    filterMode: "none"
                }
            ],
            series: equitySeries
        };

        const isDrawdown = subchartType === "DRAWDOWN";
        const subSeries = seriesList.map((s, idx) => {
            const color = colorByRunId.get(s.run.runId) || SERIES_PALETTE[idx % SERIES_PALETTE.length];
            const points = s.points || [];
            if (isDrawdown) {
                return {
                    name: s.run.label,
                    type: "line",
                    smooth: false,
                    showSymbol: points.length <= 3,
                    symbolSize: 5,
                    lineStyle: { width: 1.8, color },
                    itemStyle: { color },
                    areaStyle: { color, opacity: seriesList.length === 1 ? 0.16 : 0.06 },
                    data: points.map((pt) => [pt.timestampMs, -Math.abs(pt.drawdownPct), pt])
                };
            }
            return {
                name: s.run.label,
                type: "bar",
                barMaxWidth: 18,
                itemStyle: seriesList.length === 1
                    ? {
                        color: (p) => (p.value[1] >= 0 ? "#16a34a" : "#dc2626")
                    }
                    : { color },
                data: points.map((pt) => [pt.timestampMs, pt.periodPnlUsd, pt])
            };
        });

        const subOption = {
            animation: false,
            backgroundColor: "#ffffff",
            tooltip: {
                trigger: "axis",
                backgroundColor: "#ffffff",
                borderColor: "#cbd5e1",
                borderWidth: 1,
                textStyle: { color: "#0f172a", fontSize: 12 },
                formatter: (params) => formatSubTooltip(params, isDrawdown)
            },
            grid: {
                left: 64,
                right: 24,
                top: 16,
                bottom: 26
            },
            xAxis: {
                type: "time",
                axisLine: { lineStyle: { color: "#cbd5e1" } },
                axisLabel: { color: "#475569", fontSize: 11 },
                splitLine: { show: false }
            },
            yAxis: {
                type: "value",
                axisLabel: {
                    color: "#475569",
                    fontSize: 11,
                    formatter: (val) => isDrawdown ? `${val.toFixed(1)}%` : `$${formatCompactNum(val)}`
                },
                splitLine: { lineStyle: { color: "#f1f5f9" } }
            },
            dataZoom: [
                { type: "inside", xAxisIndex: 0, filterMode: "none" }
            ],
            series: subSeries
        };

        this.equityChart.setOption(equityOption, true);
        this.subChart.setOption(subOption, true);
        if (this.onZoomRangeChange) {
            this.onZoomRangeChange(null);
        }
    }

    clear() {
        if (this.equityChart) this.equityChart.clear();
        if (this.subChart) this.subChart.clear();
        this.lastTimestamps = [];
        this.lastSeriesData = [];
        if (this.onZoomRangeChange) {
            this.onZoomRangeChange(null);
        }
    }
}

function formatMainTooltip(params, isPct, isDay) {
    if (!Array.isArray(params) || params.length === 0) return "";
    const firstPt = params[0].data && params[0].data[2] ? params[0].data[2] : null;
    const header = firstPt
        ? (isDay ? firstPt.dateUtc : firstPt.label.replace("T", " ").replace("Z", " UTC"))
        : "";
    let html = `<div style="font-weight:700;margin-bottom:6px;border-bottom:1px solid #e2e8f0;padding-bottom:4px;">${escapeHtml(header)}</div>`;
    for (const item of params) {
        const pt = item.data && item.data[2];
        if (!pt) continue;
        const primary = isPct
            ? `${pt.returnPct >= 0 ? "+" : ""}${pt.returnPct.toFixed(2)}% ($${pt.cumulativePnlUsd.toFixed(2)})`
            : `${pt.cumulativePnlUsd >= 0 ? "+" : ""}$${pt.cumulativePnlUsd.toFixed(2)} (${pt.returnPct >= 0 ? "+" : ""}${pt.returnPct.toFixed(2)}%)`;
        const periodSign = pt.periodPnlUsd >= 0 ? "+" : "";
        html += `<div style="display:flex;justify-content:space-between;gap:14px;margin:2px 0;">
            <span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${item.color};margin-right:6px;"></span>${escapeHtml(item.seriesName)}</span>
            <strong>${primary}</strong>
        </div>
        <div style="font-size:11px;color:#64748b;margin-bottom:4px;padding-left:14px;">
            Období: ${periodSign}$${pt.periodPnlUsd.toFixed(2)} · Obchody: ${pt.tradesCount} (výhry: ${pt.winningTrades}) · DD: -${pt.drawdownPct.toFixed(2)}%
        </div>`;
    }
    return html;
}

function formatSubTooltip(params, isDrawdown) {
    if (!Array.isArray(params) || params.length === 0) return "";
    const firstPt = params[0].data && params[0].data[2] ? params[0].data[2] : null;
    const header = firstPt ? firstPt.label.replace("T", " ").replace("Z", " UTC") : "";
    let html = `<div style="font-weight:700;margin-bottom:4px;">${escapeHtml(header)}</div>`;
    for (const item of params) {
        const pt = item.data && item.data[2];
        if (!pt) continue;
        const text = isDrawdown
            ? `-${pt.drawdownPct.toFixed(2)}% (-$${pt.drawdownUsd.toFixed(2)})`
            : `${pt.periodPnlUsd >= 0 ? "+" : ""}$${pt.periodPnlUsd.toFixed(2)} (${pt.tradesCount} obch.)`;
        html += `<div style="display:flex;justify-content:space-between;gap:12px;">
            <span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${item.color};margin-right:6px;"></span>${escapeHtml(item.seriesName)}</span>
            <strong>${text}</strong>
        </div>`;
    }
    return html;
}

function formatCompactNum(val) {
    if (Math.abs(val) >= 1000) {
        return val.toLocaleString("en-US", { maximumFractionDigits: 0 });
    }
    if (Math.abs(val) < 10 && Math.abs(val - Math.round(val)) > 1e-6) {
        return val.toFixed(2);
    }
    return val.toFixed(0);
}

function escapeHtml(str) {
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}
