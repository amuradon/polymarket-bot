const JSON_HEADERS = { "Content-Type": "application/json" };

async function requestJson(url, options = {}) {
    const response = await fetch(url, options);
    if (response.status === 204) {
        return null;
    }
    const text = await response.text();
    let payload = null;
    if (text) {
        try {
            payload = JSON.parse(text);
        } catch {
            payload = { error: text };
        }
    }
    if (!response.ok) {
        const message = payload && payload.error ? payload.error : `HTTP ${response.status}`;
        throw new Error(message);
    }
    return payload;
}

export function fetchStatus() {
    return requestJson("/api/status");
}

export function fetchMetadata() {
    return requestJson("/api/backtest/metadata");
}

export function fetchJobs() {
    return requestJson("/api/backtest/jobs");
}

export function submitBacktest(payload) {
    return requestJson("/api/backtest/run", {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify(payload)
    });
}

export function cancelJob(jobId) {
    return requestJson(`/api/backtest/run/${encodeURIComponent(jobId)}/cancel`, {
        method: "POST"
    });
}

export function fetchRuns(strategy = "", symbol = "") {
    const params = new URLSearchParams();
    if (strategy) params.set("strategy", strategy);
    if (symbol) params.set("symbol", symbol);
    const qs = params.toString();
    return requestJson(`/api/backtest/runs${qs ? "?" + qs : ""}`);
}

export function fetchRunDetail(runId) {
    return requestJson(`/api/backtest/runs/${encodeURIComponent(runId)}`);
}

export function renameRun(runId, label) {
    return requestJson(`/api/backtest/runs/${encodeURIComponent(runId)}`, {
        method: "PATCH",
        headers: JSON_HEADERS,
        body: JSON.stringify({ label })
    });
}

export function deleteRun(runId) {
    return requestJson(`/api/backtest/runs/${encodeURIComponent(runId)}`, {
        method: "DELETE"
    });
}

export function compareRuns(runIds, granularity, fromDate, toDate) {
    return requestJson("/api/backtest/compare", {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify({
            runIds,
            granularity,
            fromDate: fromDate || null,
            toDate: toDate || null
        })
    });
}

export function fetchRunTrades(runId, { fromDate, toDate, side, outcome, search, limit, offset }) {
    const params = new URLSearchParams();
    if (fromDate) params.set("fromDate", fromDate);
    if (toDate) params.set("toDate", toDate);
    if (side) params.set("side", side);
    if (outcome) params.set("outcome", outcome);
    if (search) params.set("search", search);
    if (limit != null) params.set("limit", String(limit));
    if (offset != null) params.set("offset", String(offset));
    return requestJson(`/api/backtest/runs/${encodeURIComponent(runId)}/trades?${params.toString()}`);
}
