# Repository Agent Guidelines

All global rules for **Polymarket Bot** live here.

## 1. Progressive Context Disclosure & Exclusions (CRITICAL)

- **Never Preload Docs**: Do NOT read skill files, reference manuals, or unrelated configs upfront. Load workspace skills (`.agents/skills/<name>/SKILL.md`) and their `references/` **only just-in-time** when your task directly requires them (`project-architecture`, `aws-deployment`, `data-and-backtest-ops`, `network-and-performance-analysis`).
- **Strict `./docs/` Prohibition**: **NEVER** read, scan, grep, or load any files inside `./docs/` unless the user explicitly asks to edit those specific files.

---

## 2. Module Boundaries (4 Maven Submodules)

Only the internal dependencies listed under **Allowed Internal Deps** are permitted; **all other internal submodule dependencies are strictly forbidden**:

| Submodule | Type | Allowed Internal Deps (Strict Whitelist) | Scope & Isolation Notes |
| :--- | :--- | :--- | :--- |
| **`common`** | Library | *None* | Canonical L2 `PolymarketOrderBook`, `CoreTradingEngine`, `SimulatedClobExecutionEngine`, `OrderExecutionGateway`, spot indicators (`SpotTwapIndicator`, `FastSpotMicroIndicator`), domain records, and `TradingStrategy` implementations. No heavy frameworks. |
| **`connector`** | Library | *None* | Resilient WS/REST clients (Binance, Kraken, Coinbase, Polymarket Gamma/PolyBolt/CLOB). **Never depends on `common`, `bot`, or `backtest`.** |
| **`bot`** | App (`:8080`) | `common`, `connector` | Unified production application combining `StrategyTradingEngine` (`StrategyMode`: `LIVE`, `PAPER`, `OFF`), continuous `.jsonl.zst` `DataCollectorService`, `RuntimeStateStore`, and unified Web/REST dashboard. Zero `backtest` leakage. |
| **`backtest`** | App (`:8083`) | `common` | Continuous chronological historical simulation over Parquet/DuckDB driving `CoreTradingEngine` + `SimulatedClobExecutionEngine`. Isolated from `connector` and `bot`. |

- **Maximum `LIVE` / `PAPER` / `BACKTEST` Overlap (Single Engine Invariant)**:
  - Maximize shared bytecode in `common`: `CoreTradingEngine`, `PolymarketOrderBook`, spot indicators, order normalization/validation (`>= 5` whole shares, `>= $1.00` notional), collateral reservation, position accounting, and candle rollover MUST execute identically across `LIVE`, `PAPER`, and `BACKTEST`. Only thin I/O boundaries may differ (`LIVE` REST/`/ws/user` gateway vs. shared `SimulatedClobExecutionEngine` in `PAPER` and `BACKTEST`).
  - **Accuracy Over Shortcuts**: NEVER introduce separate orderbook classes, bucketed/coalesced indicator caches, simplified fill models, fixed-size VWAP fields on `OrderBookQuote`, or per-candle `strategy.reset()` / parallel candle isolation in `backtest` or `PAPER`. Replay continuous chronological streams with pre-run warm-up and evaluate exact order-size L2 sweeps on demand via `StrategyContext`.
- **Jandex Indexing**: Shared Quarkus libraries (`common`, `connector`) must include `jandex-maven-plugin` and `META-INF/beans.xml`.

---

## 3. Engineering Standards & Workflow

- **Plan First**: Before modifying source code, use `/plan` and present an Implementation Plan artifact for review. Follow the approved plan strictly; pause and update it if unexpected architectural issues arise.
- **Clean Code & Full Refactoring**: Enforce DRY (shared math/domain in `common`), SOLID/SRP, and domain-accurate naming. Leave zero dead code, unused imports/fields, or **unused constructor/method overloads**.
- **No Backward Compatibility & Production Code Priority (Fail Fast)**: Never add `@Deprecated` shims, compatibility wrappers, legacy overloads, heuristics, or silent fallbacks — **never retain backward-compatible code in `src/main/java` just to keep existing tests compiling or passing**. Production code cleanliness always takes strict priority: refactor all call sites across all 4 submodules and scripts, and **rewrite `src/test/java` tests to adapt to the clean production contract**. Fail fast with descriptive errors.
- **Hot-Path Purity & Low-Latency Java 25**:
  - **Zero Non-Essential Code on the Hot Path**: Every instruction on the WS-to-order critical path must strictly serve real-time pricing, signal calculation, or order execution. **NEVER** add unnecessary instructions, state updates, or bookkeeping on the hot path (e.g., counters, flags, or probes added to assert behavior in tests, diagnostics, or convenience).
  - **GC-Free & Lock-Free**: Eliminate heap allocations and `Double`/`Long`/`BigDecimal` boxing in real-time loops; use non-blocking Vert.x event loops without locks.
  - **Extensibility**: Design cleanly for multiple tokens (`BTC`, `ETH`, `SOL`) and `Timeframe` intervals without hardcoding single-market assumptions.
- **DI, Logging & JavaDoc**:
  - **Constructor injection** into `private final` fields only (field `@Inject` is forbidden).
  - Log exclusively via `io.quarkus.logging.Log`.
  - Add concise, human-readable JavaDoc to classes, records, interfaces, constructors, public methods, and non-trivial methods.
  - Never add test-only methods or hooks in `src/main/java`.
- **Quarkus Extension-First**: Search Quarkus extensions (`quarkus_searchDocs`, `quarkus_searchTools`) and load `quarkus_skills` before coding capabilities; present options when multiple match (`.agents/rules/quarkus.md` auto-loads on Java/Maven files).
- **TDD, Synthetic Data & Git**:
  - Follow Red-Green-Refactor TDD using **100% deterministic synthetic test fixtures** (never live/production data).
  - Pass all affected Unit, `@QuarkusTest` Component, REST/WebSocket, and UI tests (`0` failures).
  - Stage relevant changes and run local `git commit` once verified — **NEVER execute `git push`**.

---

## 4. AWS Guardrails

- **Region & IaC**: Deploy regional resources strictly in `eu-west-1` (Dublin) via CloudFormation (`infra/`) and prefer the AWS MCP Server.
- **Secrets**: Never fetch or expose raw secrets in context (`get-secret-value` is forbidden); use dynamic runtime resolution (`{{resolve:secretsmanager:...}}` / `asm-exec`). Load `aws-deployment` before any AWS work.
