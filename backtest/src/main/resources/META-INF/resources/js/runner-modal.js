import { submitBacktest } from "./api.js";

export class RunnerModalController {
    constructor(onSubmitted) {
        this.onSubmitted = onSubmitted;
        this.metadata = null;
        this.strategyByName = new Map();
        this.datasetBySymbol = new Map();
        this.currentStrategyParams = {};

        this.dialog = document.getElementById("runner-dialog");
        this.form = document.getElementById("runner-form");
        this.titleEl = document.getElementById("runner-title");
        this.errorEl = document.getElementById("runner-error");
        this.strategySelect = document.getElementById("run-strategy");
        this.labelInput = document.getElementById("run-label");
        this.symbolSelect = document.getElementById("run-symbol");
        this.datasetHint = document.getElementById("run-dataset-hint");
        this.startDateInput = document.getElementById("run-start-date");
        this.endDateInput = document.getElementById("run-end-date");
        this.fullRangeCheckbox = document.getElementById("run-full-range");
        this.forceNewCheckbox = document.getElementById("run-force-new");
        this.overwriteDaysCheckbox = document.getElementById("run-overwrite-days");
        this.capitalInput = document.getElementById("run-capital");
        this.gatewayDelayInput = document.getElementById("run-gateway-delay");
        this.binanceDelayInput = document.getElementById("run-binance-delay");
        this.krakenDelayInput = document.getElementById("run-kraken-delay");
        this.coinbaseDelayInput = document.getElementById("run-coinbase-delay");
        this.keyParamsGrid = document.getElementById("key-params-grid");
        this.rawJsonTextarea = document.getElementById("run-params-json");
        this.jsonParseErrorEl = document.getElementById("json-parse-error");
        this.submitBtn = document.getElementById("btn-runner-submit");

        this.bindEvents();
    }

    bindEvents() {
        document.getElementById("btn-runner-close").addEventListener("click", () => this.dialog.close());
        document.getElementById("btn-runner-cancel").addEventListener("click", () => this.dialog.close());
        this.dialog.addEventListener("close", () => {
            this.dialog.returnValue = "";
        });

        this.strategySelect.addEventListener("change", () => {
            this.resetParamsToStrategyDefaults(this.strategySelect.value);
        });

        this.symbolSelect.addEventListener("change", () => {
            this.updateDatasetHintAndDates(false);
        });

        this.fullRangeCheckbox.addEventListener("change", () => {
            this.syncDateInputsEnabled();
        });

        this.forceNewCheckbox.addEventListener("change", () => {
            this.syncMergeOptionsEnabled();
        });

        document.getElementById("btn-reset-strategy-params").addEventListener("click", () => {
            this.resetParamsToStrategyDefaults(this.strategySelect.value);
        });

        this.rawJsonTextarea.addEventListener("input", () => {
            this.syncFromRawJson();
        });

        this.form.addEventListener("submit", async (e) => {
            e.preventDefault();
            await this.handleSubmit();
        });
    }

    setMetadata(metadata) {
        this.metadata = metadata;
        this.strategyByName.clear();
        this.datasetBySymbol.clear();

        this.strategySelect.innerHTML = "";
        for (const s of (metadata.strategies || [])) {
            this.strategyByName.set(s.name, s);
            const opt = document.createElement("option");
            opt.value = s.name;
            opt.textContent = `${s.name} (${s.timeframe})`;
            this.strategySelect.appendChild(opt);
        }

        const symbols = [];
        for (const ds of (metadata.datasets || [])) {
            this.datasetBySymbol.set(ds.symbol, ds);
            symbols.push(ds.symbol);
        }
        if (!symbols.includes(metadata.defaultSymbol || "BTCUSDT")) {
            symbols.unshift(metadata.defaultSymbol || "BTCUSDT");
        }
        this.symbolSelect.innerHTML = "";
        for (const sym of symbols) {
            const opt = document.createElement("option");
            opt.value = sym;
            opt.textContent = sym;
            this.symbolSelect.appendChild(opt);
        }
    }

    openForNew() {
        if (!this.metadata) return;
        this.hideError();
        this.titleEl.textContent = "Spustit nový backtest";
        this.labelInput.value = "";

        this.strategySelect.value = this.metadata.defaultStrategy || this.strategySelect.options[0]?.value || "";
        this.symbolSelect.value = this.metadata.defaultSymbol || this.symbolSelect.options[0]?.value || "BTCUSDT";
        this.capitalInput.value = String(this.metadata.defaultInitialCapital ?? 10000);
        this.gatewayDelayInput.value = String(this.metadata.defaultOrderGatewayDelayMs ?? 220);
        const delays = this.metadata.defaultExchangeDelays || {};
        this.binanceDelayInput.value = String(delays.binanceDelayMs ?? 100);
        this.krakenDelayInput.value = String(delays.krakenDelayMs ?? 50);
        this.coinbaseDelayInput.value = String(delays.coinbaseDelayMs ?? 50);

        this.fullRangeCheckbox.checked = this.datasetBySymbol.has(this.symbolSelect.value);
        this.forceNewCheckbox.checked = false;
        this.overwriteDaysCheckbox.checked = false;
        this.syncMergeOptionsEnabled();
        this.updateDatasetHintAndDates(true);
        this.syncDateInputsEnabled();
        this.resetParamsToStrategyDefaults(this.strategySelect.value);

        this.dialog.showModal();
    }

    openForClone(runDetail) {
        if (!this.metadata || !runDetail || !runDetail.config) return;
        this.hideError();
        this.titleEl.textContent = "Klonovat & upravit backtest";
        const cfg = runDetail.config;
        const summary = runDetail.summary;

        this.labelInput.value = summary && summary.label ? `${summary.label} (kopie)` : "";
        if (this.strategyByName.has(cfg.strategyName)) {
            this.strategySelect.value = cfg.strategyName;
        }
        if ([...this.symbolSelect.options].some((o) => o.value === cfg.symbol)) {
            this.symbolSelect.value = cfg.symbol;
        }

        this.fullRangeCheckbox.checked = false;
        this.forceNewCheckbox.checked = false;
        this.overwriteDaysCheckbox.checked = false;
        this.syncMergeOptionsEnabled();
        this.startDateInput.value = cfg.startDate || "";
        this.endDateInput.value = cfg.endDate || "";
        this.syncDateInputsEnabled();
        this.updateDatasetHintAndDates(false);

        this.capitalInput.value = String(cfg.initialCapital ?? 10000);
        this.gatewayDelayInput.value = String(cfg.orderGatewayDelayMs ?? 220);
        this.binanceDelayInput.value = String(cfg.binanceDelayMs ?? 100);
        this.krakenDelayInput.value = String(cfg.krakenDelayMs ?? 50);
        this.coinbaseDelayInput.value = String(cfg.coinbaseDelayMs ?? 50);

        this.loadParamsForStrategy(cfg.strategyName, cfg.strategyParams || {});
        this.dialog.showModal();
    }

    updateDatasetHintAndDates(fillDatesFromBounds) {
        const ds = this.datasetBySymbol.get(this.symbolSelect.value);
        if (ds) {
            this.datasetHint.textContent = `Dostupná data: ${ds.minDate} – ${ds.maxDate} (${ds.dayCount} dnů, ${ds.marketCount} trhů)`;
            if (fillDatesFromBounds || !this.startDateInput.value || !this.endDateInput.value) {
                this.startDateInput.value = ds.minDate;
                this.endDateInput.value = ds.maxDate;
            }
        } else {
            this.datasetHint.textContent = "V collector-dir nejsou zjištěny lokální soubory pro tento symbol.";
            if (fillDatesFromBounds) {
                this.startDateInput.value = this.metadata?.defaultStartDate || "";
                this.endDateInput.value = new Date().toISOString().slice(0, 10);
            }
        }
    }

    syncDateInputsEnabled() {
        const full = this.fullRangeCheckbox.checked;
        this.startDateInput.disabled = full;
        this.endDateInput.disabled = full;
        if (full) {
            const ds = this.datasetBySymbol.get(this.symbolSelect.value);
            if (ds) {
                this.startDateInput.value = ds.minDate;
                this.endDateInput.value = ds.maxDate;
            }
        }
    }

    syncMergeOptionsEnabled() {
        const forceNew = this.forceNewCheckbox.checked;
        if (forceNew) {
            this.overwriteDaysCheckbox.checked = false;
        }
        this.overwriteDaysCheckbox.disabled = forceNew;
    }

    resetParamsToStrategyDefaults(strategyName) {
        const meta = this.strategyByName.get(strategyName);
        const defaults = {};
        if (meta && Array.isArray(meta.parameters)) {
            for (const p of meta.parameters) {
                defaults[p.name] = p.defaultValue;
            }
        }
        this.loadParamsForStrategy(strategyName, defaults);
    }

    loadParamsForStrategy(strategyName, paramValues) {
        const meta = this.strategyByName.get(strategyName);
        const merged = {};
        if (meta && Array.isArray(meta.parameters)) {
            for (const p of meta.parameters) {
                merged[p.name] = Object.prototype.hasOwnProperty.call(paramValues, p.name)
                    ? paramValues[p.name]
                    : p.defaultValue;
            }
        }
        this.currentStrategyParams = merged;
        this.renderKeyParams(meta, merged);
        this.rawJsonTextarea.value = JSON.stringify(merged, null, 2);
        this.jsonParseErrorEl.classList.add("hidden");
        this.jsonParseErrorEl.textContent = "";
    }

    renderKeyParams(meta, values) {
        this.keyParamsGrid.innerHTML = "";
        const params = meta && Array.isArray(meta.parameters) ? meta.parameters.filter((p) => p.keyParam) : [];
        if (params.length === 0) {
            const msg = document.createElement("p");
            msg.className = "field-hint";
            msg.textContent = "Tato strategie neexponuje konfigurovatelné parametry.";
            this.keyParamsGrid.appendChild(msg);
            return;
        }

        for (const p of params) {
            const field = document.createElement("div");
            field.className = "form-field";

            const label = document.createElement("label");
            label.setAttribute("for", `kp-${p.name}`);
            const nameSpan = document.createElement("span");
            nameSpan.textContent = p.name;
            const defSpan = document.createElement("span");
            defSpan.className = "muted";
            defSpan.textContent = `def: ${p.defaultValue}`;
            label.append(nameSpan, defSpan);

            const val = values[p.name];
            let input;
            if (p.type === "boolean") {
                input = document.createElement("select");
                input.className = "select";
                for (const boolVal of ["true", "false"]) {
                    const o = document.createElement("option");
                    o.value = boolVal;
                    o.textContent = boolVal;
                    input.appendChild(o);
                }
                input.value = String(Boolean(val));
            } else {
                input = document.createElement("input");
                input.className = "input";
                input.type = (p.type === "int" || p.type === "long" || p.type === "double") ? "number" : "text";
                if (p.type === "double") {
                    input.step = "any";
                } else if (p.type === "int" || p.type === "long") {
                    input.step = "1";
                }
                input.value = val != null ? String(val) : "";
            }
            input.id = `kp-${p.name}`;
            input.dataset.paramName = p.name;
            input.dataset.paramType = p.type;
            this.updateModifiedClass(input, p.defaultValue, val, p.type);

            input.addEventListener("input", () => {
                const parsed = parseInputValue(input.value, p.type);
                if (parsed !== undefined) {
                    this.currentStrategyParams[p.name] = parsed;
                    this.rawJsonTextarea.value = JSON.stringify(this.currentStrategyParams, null, 2);
                    this.jsonParseErrorEl.classList.add("hidden");
                    this.updateModifiedClass(input, p.defaultValue, parsed, p.type);
                }
            });

            field.append(label, input);
            this.keyParamsGrid.appendChild(field);
        }
    }

    syncFromRawJson() {
        try {
            const parsed = JSON.parse(this.rawJsonTextarea.value);
            if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
                throw new Error("Kořenová hodnota musí být JSON objekt s parametry.");
            }
            this.currentStrategyParams = parsed;
            this.jsonParseErrorEl.classList.add("hidden");
            this.jsonParseErrorEl.textContent = "";

            const meta = this.strategyByName.get(this.strategySelect.value);
            if (meta) {
                for (const p of meta.parameters.filter((d) => d.keyParam)) {
                    const input = document.getElementById(`kp-${p.name}`);
                    if (input && Object.prototype.hasOwnProperty.call(parsed, p.name)) {
                        input.value = String(parsed[p.name]);
                        this.updateModifiedClass(input, p.defaultValue, parsed[p.name], p.type);
                    }
                }
            }
        } catch (err) {
            this.jsonParseErrorEl.textContent = `Neplatný JSON: ${err.message}`;
            this.jsonParseErrorEl.classList.remove("hidden");
        }
    }

    updateModifiedClass(inputEl, defaultVal, currentVal, type) {
        const same = (type === "double" || type === "int" || type === "long")
            ? Number(defaultVal) === Number(currentVal)
            : String(defaultVal) === String(currentVal);
        inputEl.classList.toggle("param-modified", !same);
    }

    async handleSubmit() {
        this.hideError();
        let parsedJsonParams = this.currentStrategyParams;
        try {
            parsedJsonParams = JSON.parse(this.rawJsonTextarea.value || "{}");
            if (!parsedJsonParams || typeof parsedJsonParams !== "object" || Array.isArray(parsedJsonParams)) {
                throw new Error("Parametry strategie musí být JSON objekt.");
            }
        } catch (err) {
            this.showError(`Neplatný JSON parametrů strategie: ${err.message}`);
            return;
        }

        const meta = this.strategyByName.get(this.strategySelect.value);
        const overrides = {};
        if (meta && Array.isArray(meta.parameters) && meta.parameters.length > 0) {
            const defaultsMap = new Map(meta.parameters.map((p) => [p.name, p]));
            for (const [k, v] of Object.entries(parsedJsonParams)) {
                const desc = defaultsMap.get(k);
                if (!desc) {
                    overrides[k] = v;
                    continue;
                }
                const same = (desc.type === "double" || desc.type === "int" || desc.type === "long")
                    ? Number(desc.defaultValue) === Number(v)
                    : String(desc.defaultValue) === String(v);
                if (!same) {
                    overrides[k] = v;
                }
            }
        } else if (Object.keys(parsedJsonParams).length > 0) {
            Object.assign(overrides, parsedJsonParams);
        }

        const fullRange = this.fullRangeCheckbox.checked;
        const payload = {
            label: this.labelInput.value.trim() || null,
            strategyName: this.strategySelect.value,
            symbol: this.symbolSelect.value,
            startDate: fullRange ? null : (this.startDateInput.value || null),
            endDate: fullRange ? null : (this.endDateInput.value || null),
            fullRange,
            initialCapital: Number(this.capitalInput.value),
            orderGatewayDelayMs: Number(this.gatewayDelayInput.value),
            binanceDelayMs: Number(this.binanceDelayInput.value),
            krakenDelayMs: Number(this.krakenDelayInput.value),
            coinbaseDelayMs: Number(this.coinbaseDelayInput.value),
            strategyParams: Object.keys(overrides).length > 0 ? overrides : null,
            forceNewRun: this.forceNewCheckbox.checked,
            overwriteExistingDays: this.overwriteDaysCheckbox.checked
        };

        this.submitBtn.disabled = true;
        try {
            const job = await submitBacktest(payload);
            this.dialog.close();
            if (this.onSubmitted) {
                await this.onSubmitted(job);
            }
        } catch (err) {
            this.showError(err.message);
        } finally {
            this.submitBtn.disabled = false;
        }
    }

    showError(message) {
        this.errorEl.textContent = message;
        this.errorEl.classList.remove("hidden");
    }

    hideError() {
        this.errorEl.textContent = "";
        this.errorEl.classList.add("hidden");
    }
}

function parseInputValue(raw, type) {
    if (type === "boolean") {
        return raw === "true";
    }
    if (type === "int" || type === "long") {
        if (raw.trim() === "") return undefined;
        const n = Number(raw);
        return Number.isInteger(n) ? n : undefined;
    }
    if (type === "double") {
        if (raw.trim() === "") return undefined;
        const n = Number(raw);
        return Number.isFinite(n) ? n : undefined;
    }
    return raw;
}
