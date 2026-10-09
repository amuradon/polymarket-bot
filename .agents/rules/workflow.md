# Development Lifecycle & Engineering Rules

This document defines the strict, non-negotiable workflow and development protocol for the **Polymarket Bot** repository. All AI agents and developers **MUST** adhere to these rules at all times.

---

## 1. General Mandates (Obecné)

### Mandatory GitHub Ticket Number
- **Ticket Required**: For every task, planning session, and commit, a GitHub ticket number is mandatory.
- **Solicitation Protocol**: If the user has not explicitly provided a GitHub ticket number in their prompt, the agent **MUST** request it before proceeding with any implementation or file modifications.
- **Commit Association**: The ticket number must be used across all commits (including incremental fixes) belonging to that task.

---

## 2. Planning Protocol (Plánování)

- **Presentation Before Modification**: Before executing any source code changes always use /plan command and present native Implementation Plan artifact to the user for the review

---

## 3. Implementation Standards (Implementace)

### 3.1 Strict Adherence to Implementation Plan
- Strictly follow the approved implementation plan.
- Do not introduce out-of-scope refactoring, speculative features, or unplanned dependencies.
- If unexpected architectural obstacles or requirements appear during implementation, pause, update the plan, and present it to the user for approval.

### 3.2 Clean Code & Minimize Duplication (DRY)
- **Minimize Duplication**: Actively eliminate duplicate logic across the entire codebase. Shared domain models, calculators, mathematical formulas, and interfaces belong strictly in `common`.
- **SOLID Principles**: Keep classes, records, and methods small, focused, and adhering to the Single Responsibility Principle (SRP).
- **Domain-Accurate Naming**: Use explicit, unambiguous names reflecting financial, market microstructure, and prediction market concepts (e.g. `CandleTwapState`, `TwapPoint`, `Timeframe`, `PriceSnapshot`).

### 3.3 Zero Dead or Unused Code & Overloads
- **Complete Cleanup**: After completing modifications, audit the codebase to ensure no dead, orphaned, or unreferenced code remains.
- **No Unused Imports or Fields**: Remove all unused imports, unused private fields, and unreachable code branches.
- **No Unused Method or Constructor Overloads**: There must be **NO** unused method or constructor overloads left in the code. If a method or constructor signature is changed, refactor all call sites across the entire project and delete obsolete overloads completely.

### 3.4 No Backward Compatibility – Refactor the Entire Codebase
- **Zero Legacy Compatibility Layers**: Never make changes backward-compatible. Do NOT create backward-compatibility wrappers, deprecation shims (`@Deprecated`), dual-path APIs, or fallback adapter layers.
- **Full Codebase Refactoring**: Always refactor the entire codebase immediately to adopt the new solution uniformly across all submodules (`common`, `trading`, `live`, `paper`, `backtest`), tests, and scripts.

### 3.5 No Fallback Logic or Heuristics (Fail Fast)
- **Exact Execution**: The solution must execute strictly according to specification.
- **No Fallbacks or Heuristics**: Do NOT create any fallback to previous functionality or other heuristics. The solution must work exactly as defined or throw an explicit, descriptive error immediately (fail-fast principle).

### 3.6 No Test-Only Methods in Production Code
- **Production Code Integrity**: Never create methods, getters, setters, constructors, or internal hooks in production code (`src/main/java`) that are only used in tests.
- **Contract-Based Testing**: Test production classes strictly through their public contracts or clean package-private boundaries. Test fixtures, test doubles, and reflection helpers belong strictly in `src/test/java`.

### 3.7 Test-Driven Development (TDD)
- Strictly follow TDD (Red-Green-Refactor) for every class, method, and component:
  1. **Red**: Before implementing any class, method, or component, define tests specifying the intended functionality.
  2. **Green**: Implement the minimal production code necessary to make the tests pass.
  3. **Refactor**: Clean up the implementation, eliminate duplication, and verify all tests remain green.
- After implementation, all written tests must pass.

### 3.8 Constructor Injection for Dependency Injection
- **Mandatory Constructor Injection**: Always prefer and use constructor injection (`@Inject public MyService(...)`) for all CDI beans and Quarkus components.
- **Immutable Fields**: All injected dependencies must be assigned to `private final` fields.
- **No Field Injection**: Field injection (`@Inject private SomeService svc;`) is **strictly forbidden**. Constructor injection guarantees immutability, thread-safety, and seamless unit testing without container reflection.

### 3.9 Synthetic Test Data Only (Zero Production / Live Data in Tests)
- **Strict Prohibition of Live Data**: Never use real, live, or production data in automated tests under any circumstances.
- **Deterministic Synthetic Fixtures**: All test datasets, market data fixtures, order books, price feeds, and mock payloads across all test levels (unit, component, integration, API, Cucumber) must be purely synthetic, deterministic, and isolated.

### 3.10 Human-Readable JavaDoc Documentation
- **Clear & Concise Element Purpose**: All Java elements—specifically classes, records, interfaces, constructors, public methods, and non-trivial methods—must include concise JavaDoc documenting their purpose and intent.
- **Human-Friendly Explanations**: JavaDoc descriptions must be clearly understandable for human developers, explaining what the component or method does and why it exists without unnecessary verbosity.

---

## 4. Verification & Quality Assurance (Ověření)

### 4.1 Clean Multi-Module Compilation
- Verify that the entire multi-module project compiles cleanly from scratch without errors or warnings:
  ```bash
  ./mvnw clean compile
  ```

### 4.2 Multi-Level Test Pyramid Execution
- After implementation, verify complete functionality by running and passing tests across all levels using purely synthetic test data:
  1. **Unit Tests**: Isolated unit tests validating domain math, calculators, and parsers (`JUnit 5`, `AssertJ`, `Mockito`).
  2. **Component & Integration Tests**: Quarkus component tests verifying caching, Vert.x event loops, and CDI wiring (`@QuarkusTest`, `Awaitility`).
  3. **API & WebSocket Tests**: REST endpoint tests (`RestAssured`) and WebSocket streaming tests (`quarkus-websockets-next`).
  4. **UI Tests**: Web interface rendering, Qute templates, and chart component bindings.
- **100% Pass Requirement**: Every single test across all levels must pass (0 failures, 0 errors, 0 broken tests).

---

## 5. Delivery & Git Protocol (Odevzdání)

### 5.1 Local Git Commit Only (STRICTLY NO GIT PUSH)
- **Local Git Commit**: Stage all relevant modified, created, or deleted files cleanly and execute `git commit` to the local Git repository after completing the task and verifying that all tests pass.
- **NEVER Perform Git Push**: The agent **MUST NEVER** execute `git push` under any circumstances.
- **Manual Push by User**: Pushing commits to the remote repository is strictly reserved for the user after their review. Once the commit is created, inform the user that changes are committed locally and ready for their review and push.

### 5.2 Mandatory GitHub Ticket Number & Commit Message Format
- **Mandatory GitHub Ticket**: Every commit message **MUST** explicitly start with the GitHub ticket number.
- **Format**:
  ```
  Resolves #<ticket-id> <Clear, imperative description of the changes>
  ```
  *Example*: `Resolves #38 Implement Kraken fee schedule parser and unit tests`
- **Scope & Continuity**: Use the identical ticket number for every subsequent commit, including bug fixes and review refinements, in the same conversation until a new ticket number is given.
