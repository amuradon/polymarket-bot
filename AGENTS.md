# Repository Agent Guidelines

This repository contains the Polymarket Bot application.

---

## 1. Development Lifecycle & Workflow Protocol (NON-NEGOTIABLE)

All agents working on this repository **MUST ALWAYS** follow the engineering protocol defined in:

👉 [`.agents/rules/workflow.md`](.agents/rules/workflow.md)

### Key Lifecycle Mandates:

- **Mandatory GitHub Ticket**: Always require a GitHub ticket number for every task and commit (`Resolves #<ticket-id> ...`). If not explicitly provided in the prompt, request it before proceeding.
- **Adhere to the Plan**
- **Clean Code**
- **Test-Driven Development (TDD)**
- **Synthetic Test Data Only**: Never use live or production data in automated tests; always use deterministic synthetic test fixtures.
- **Human-Readable JavaDoc**: Document classes, records, interfaces, constructors, public methods, and non-trivial methods with clear, concise JavaDoc for human clarity.
- **Multi-Level Test Pyramid**
- **Local Git Commit Only (NO Git Push)**: Always commit locally after completing the task and verifying all tests pass. Never execute `git push`.

---

## 2. Architecture & Vision Mandates (CRITICAL)

Before proposing plans, designing features, or writing any code in this repository, agents **MUST ALWAYS** study and strictly abide by the following foundational documents:

1. 👉 [`ARCHITECTURE.md`](ARCHITECTURE.md):
   - Agents must familiarize themselves with the 5-submodule architecture (`common`, `trading`, `live`, `paper`, `backtest`) and strictly obey module boundaries and prohibited dependencies.
   - **Zero Backtest Leakage**: Never introduce backtesting tools or heavy simulation dependencies into `live` or `paper`.
   - **Zero Discrepancy**: Ensure strategies and engine logic in `common` execute identically across backtesting, paper, and live trading.

2. 👉 [`VISION.md`](VISION.md):
   - Agents must study the platform vision (multi-token support for BTC, ETH, SOL; 15m/5m/1h timeframes; AWS `eu-west-1` Dublin deployment near London data centers).
   - All code must be designed to be easily extensible, modular, and optimized for ultra-low latency (Java 25, GC-free hot path, non-blocking Vert.x event loops).

3. 👉 [`DEPLOYMENT.md`](DEPLOYMENT.md):
   - When deploying data, paper, or live applications to AWS EC2 with EBS in `eu-west-1`, agents and developers **MUST ALWAYS** ensure continuous 24/7 execution (`--restart unless-stopped`), non-throttled CPU allocation, and persistent EBS volume mounting.

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

---

## 4. AWS Cloud & Infrastructure Rules

When working with AWS services, infrastructure, or deployment in this repository, agents **MUST ALWAYS** follow the rules defined in:

1. 👉 [`.agents/rules/aws-agent-rules.md`](.agents/rules/aws-agent-rules.md)
2. 👉 [`.agents/rules/aws-starter-rules.md`](.agents/rules/aws-starter-rules.md)

### Key Mandates from AWS Rules:
- **Target Region**: Deploy regional resources in AWS `eu-west-1` (Ireland / Dublin).
- **AWS MCP & Skills First**: Prefer the AWS MCP Server over direct CLI commands and check/load relevant AWS skills before starting any AWS task.
- **Infrastructure as Code**: Prefer AWS CDK or CloudFormation over imperative CLI resource creation.
- **Secret Safety**: Never expose raw secrets in context; follow the `aws-secrets-manager` resolution guidelines.
