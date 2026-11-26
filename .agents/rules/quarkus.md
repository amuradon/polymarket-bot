---
trigger: glob
globs: **/*.java, **/pom.xml, **/application*.properties
---

# Quarkus & Multi-Module Java Rules

## 1. Extension-First Rule
1. **Search Dynamically**: Before coding a new capability, search `quarkus_searchDocs` and `quarkus_searchTools` (`query='extension'`). Never hand-code features covered by an existing Quarkus extension.
2. **Consult User on Alternatives**: If multiple extensions match, present all options with a recommended default and wait for the user's choice.
3. **Load Extension Skills**: Call `quarkus_skills` for the target extension before writing code or tests, and re-run `quarkus_searchTools` after adding/removing extensions.

## 2. Dev MCP vs. Multi-Module Maven Execution
- **When Quarkus Dev Mode is running** on an application module (`bot`, `backtest`):
  - Run tests via `quarkus_callTool` using `devui-testing_runTests` or `devui-testing_runTest` (`{"className":"..."}`), delegating test runs to a subagent when helpful to keep main context lean.
  - Trigger hot reload via `devui-logstream_forceRestart`.
  - After `pom.xml` changes (or if the runner is stuck on `"Tests already in progress"`), perform a full `quarkus_stop` + `quarkus_start` cycle (`forceRestart` does not re-resolve dependencies).
  - Inspect failures via `devui-exceptions_getLastException` and clear with `devui-exceptions_clearLastException` (fall back to `quarkus_logs` only on initial boot failure or when broader log context is required).
  - **NEVER run `mvnw clean` while Dev Mode is running** — it deletes `target/test-classes` and breaks the Dev Mode test runner.
- **When Dev Mode is NOT running** (or when verifying across library modules `common`, `connector` and the full 4-module reactor):
  - Run Bash Maven commands without `clean` unless needed: `./mvnw test-compile` or `./mvnw test`.

## 3. Configuration & Testing Conventions
- Use `@QuarkusTest` for CDI, Vert.x, REST, and WebSocket component tests with purely synthetic test fixtures.
- Scope dev/test overrides in `application.properties` with `%dev.` and `%test.` profile prefixes; never hardcode environment URLs without profile prefixes.
- Use `io.quarkus.logging.Log` exclusively for logging (guard expensive debug/trace formatting with level checks).
