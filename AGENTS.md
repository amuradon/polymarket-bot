# Repository Agent Guidelines

All global rules for **Polymarket Bot** live here.

## 1. Progressive Context Disclosure & Exclusions (CRITICAL)

- **Never Preload Docs**: Do NOT read skill files, reference manuals, or unrelated configs upfront. Load workspace skills (`.agents/skills/<name>/SKILL.md`) and their `references/` **only just-in-time** when your task directly requires them (`project-architecture`, `aws-deployment`, `data-and-backtest-ops`).
- **Strict `./docs/` Prohibition**: **NEVER** read, scan, grep, or load any files inside `./docs/` unless the user explicitly asks to edit those specific files.

---

## 2. Module Boundaries (7 Maven Submodules)

Only the internal dependencies listed under **Allowed Internal Deps** are permitted; **all other internal submodule dependencies are strictly forbidden**:

| Submodule | Type | Allowed Internal Deps (Strict Whitelist) | Scope & Isolation Notes |
| :--- | :--- | :--- | :--- |
| **`common`** | Library | *None* | Pure domain records, math/TWAP calculators (`CalculatedSpotTwapEngine`), `TradingStrategy`, `StrategyContext`, `ExecutionRouter`. No heavy frameworks. |
| **`connector`** | Library | *None* | Resilient WS/REST clients (Binance, Kraken, Coinbase, Polymarket Gamma/PolyBolt/CLOB). **Never depends on `common` or `trading`.** |
| **`data`** | App (`:8084`) | `connector` | Raw `.jsonl.zst` stream recorder with Blue-Green cutover. **Never depends on `common` or `trading`.** |
| **`trading`** | Library | `common`, `connector` | Shared runtime, UI/REST/WS wiring, and `CalculatedSpotStreamProcessor` for `paper` and `live`. Zero `data`/`backtest` leakage. |
| **`live`** | App (`:8080`) | `trading`, `connector` | Real-money Polymarket execution (EIP-712 signing, CLOB gateway, risk guards). Zero `backtest` leakage. |
| **`paper`** | App (`:8082`) | `trading`, `connector` | Virtual order simulator against live market feeds. Zero `backtest` leakage. |
| **`backtest`** | App (`:8083`) | `common` | Historical simulation engine (DuckDB, Parquet replay, simulated `ExecutionRouter`). Isolated from `connector`/`trading`/`live`/`paper`. |

- **Zero Discrepancy**: Strategy (`TradingStrategy`) and calculator bytecode in `common` executes identically across `backtest`, `paper`, and `live` via `StrategyContext` and `ExecutionRouter`.
- **Jandex Indexing**: Shared Quarkus libraries (`common`, `connector`, `trading`) must include `jandex-maven-plugin` and `META-INF/beans.xml`.

---

## 3. Engineering Standards & Workflow

- **Plan First**: Before modifying source code, use `/plan` and present an Implementation Plan artifact for review. Follow the approved plan strictly; pause and update it if unexpected architectural issues arise.
- **Clean Code & Full Refactoring**: Enforce DRY (shared math/domain in `common`), SOLID/SRP, and domain-accurate naming. Leave zero dead code, unused imports/fields, or **unused constructor/method overloads**.
- **No Backward Compatibility & Fail Fast**: Never add `@Deprecated` shims, compatibility wrappers, heuristics, or silent fallbacks. Refactor all call sites across all 7 submodules, tests, and scripts immediately; fail fast with descriptive errors.
- **Low-Latency Java 25 & Extensibility**: Keep the WS-to-order hot path GC-free (no heap allocations or `Double`/`Long`/`BigDecimal` boxing in real-time loops), use non-blocking Vert.x event loops without locks, and design extensibly for multiple tokens (`BTC`, `ETH`, `SOL`) and `Timeframe` intervals.
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
