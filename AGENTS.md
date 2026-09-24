# Repository Agent Guidelines

This repository contains the Polymarket Bot application.

---

## 1. Development Lifecycle & Workflow Protocol (NON-NEGOTIABLE)

All agents working on this repository **MUST ALWAYS** follow the engineering protocol defined in:

👉 [`.agents/rules/workflow.md`](.agents/rules/workflow.md)

### Key Lifecycle Mandates:

- **Always Use Conductor Plugin**
- **Adhere to the Plan**
- **Clean Code**
- **Test-Driven Development (TDD)**
- **Multi-Level Test Pyramid**

---

## 2. Architecture & Vision Mandates (CRITICAL)

Before proposing plans, designing features, or writing any code in this repository, agents **MUST ALWAYS** study and strictly abide by the following foundational documents:

1. 👉 [`ARCHITECTURE.md`](ARCHITECTURE.md):
   - Agents must familiarize themselves with the 5-submodule architecture (`common`, `trading`, `live`, `paper`, `backtest`) and strictly obey module boundaries and prohibited dependencies.
   - **Zero Backtest Leakage**: Never introduce backtesting tools or heavy simulation dependencies into `live` or `paper`.
   - **Zero Discrepancy**: Ensure strategies and engine logic in `common` execute identically across backtesting, paper, and live trading.

2. 👉 [`VISION.md`](VISION.md):
   - Agents must study the platform vision (multi-token support for BTC, ETH, SOL; 15m/5m/1h timeframes; GCP Dublin deployment near London data centers).
   - All code must be designed to be easily extensible, modular, and optimized for ultra-low latency (Java 25, GC-free hot path, non-blocking Vert.x event loops).

---

## 3. Java & Quarkus Development Rules

When working on any Java and Quarkus code in this repository, agents **MUST ALWAYS** follow the specific rules and instructions defined in:

👉 [`.agents/rules/quarkus.md`](.agents/rules/quarkus.md)

### Key Mandates from Quarkus Rules:
- **Extension-First Rule**: Before writing any code, always search for existing Quarkus extensions (`quarkus_searchDocs`, `quarkus_searchTools`). Never implement custom logic if a Quarkus extension exists.
- **User Consultation**: If multiple matching extensions exist, present all options to the user with a recommendation and wait for their choice.
- **Skills Usage**: Load extension skills with `quarkus_skills` prior to writing code or tests.
- **Testing & Dev MCP Tools**: Use Quarkus Dev MCP tools (`quarkus_callTool`, `devui-testing_runTests`, etc.) rather than running ad-hoc Maven commands.
- **No `mvn clean` in Dev Mode**: Never run `mvn clean` or `gradle clean` while dev mode is running.
