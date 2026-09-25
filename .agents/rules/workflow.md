# Development Lifecycle & Engineering Rules

This document defines the strict, non-negotiable workflow and development protocol for the **Polymarket Bot** repository. All AI agents and developers **MUST** adhere to these rules at all times.

---

## 1. General Mandates (Obecné)

### Always Use Conductor Plugin
- **Exclusive Workflow Driver**: Every feature, bug fix, architectural refactoring, or chore MUST be tracked and executed via the **Conductor** plugin skills (`conductor-implement`, `conductor-review`, `conductor-status`, `conductor-revert`).
- **Manual Track Creation Only (NO Auto-Creation)**: New Conductor tracks are **ALWAYS created manually by the user**. The agent **MUST NEVER** automatically or autonomously create, scaffold, or initiate a new track (never run `conductor-new-track` on its own). The agent must only work on an existing track created and specified by the user. If the user requests work without specifying an active track, the agent must ask the user which track to use rather than creating one automatically.
- **No Ad-Hoc Code Changes**: Never modify production code or create features outside of an active Conductor track.
- **Environment Bootstrapping**: If Conductor is not initialized in the repository (i.e. `conductor/index.md` or `conductor/tracks.md` does not exist), the agent must first invoke `conductor-setup` to scaffold the foundational Conductor structure (`product.md`, `tech-stack.md`, `workflow.md`, `tracks.md`).
- **Spec-Driven Development (SDD)**: For the user-created track, strictly follow the execution lifecycle:
  1. Track created manually by the user (`spec.md` & `plan.md`)
  2. Execution with TDD (`conductor-implement`)
  3. Principal Review & Verification (`conductor-review`)
- **Native Modal UI**: When asking for user decisions, choices, or clarifications within Conductor workflows, the agent MUST use the native interactive modal tool `ask_question` whenever available.

---

## 2. Planning Protocol (Plánování)

### Define Cucumber Acceptance Tests During Planning
- **Acceptance Criteria as Gherkin Features**: During the planning phase (when authoring `spec.md` and `plan.md`), the agent **MUST** define concrete Cucumber (Gherkin) acceptance scenarios *before* any production code is implemented.
- **Location**: Feature files must be created or updated under the appropriate module's test resources (e.g. `trading/src/test/resources/features/<feature-name>.feature`).
- **Standard Syntax**: Scenarios must use clear `Given - When - Then - And` syntax to specify:
  - Normal / happy-path user and system behaviors
  - Boundary conditions and financial edge cases
  - Error and exceptional scenarios (fail-fast behavior)
- **Step Definitions & Fixtures**: The track plan must explicitly list the creation or extension of step definitions (e.g. `*Steps.java`) and test harnesses (`RunCucumberTest`).
- **Plan Presentation & Approval**: Present the implementation plan—including the proposed architectural changes, open questions, and defined Cucumber scenarios—to the user and obtain explicit approval before proceeding to the implementation phase.

### File-First Document Review Protocol (Auxiliary Pane)
- **Mandatory File-First Saving**: Whenever drafting or updating markdown documents for user review and alignment (e.g. Conductor `spec.md`, `plan.md`, `implementation_plan.md`, ADRs, review reports):
  - The agent **MUST NEVER** output or print the full markdown document directly into the chat stream.
  - The agent **MUST ALWAYS** first save the markdown file to disk (e.g. `conductor/tracks/<track_id>/spec.md`, `conductor/tracks/<track_id>/plan.md`, or artifact).
  - In chat, output **ONLY** a concise summary (2-5 bullet points) and a clickable markdown link (`file:///...`) inviting the user to review the document in the **Auxiliary Pane** in Antigravity 2.0.
- **Mandatory Processing of Line Comments**:
  - The user will highlight lines and add inline comments in the Auxiliary Pane.
  - The agent **MUST** process every single line comment (`Selection:` / `Comment:`), update the file on disk accordingly, and request re-review until approved.


---

## 3. Implementation Standards (Implementace)

### 3.1 Strict Adherence to Implementation Plan
- Strictly follow the steps and tasks laid out in the approved `plan.md`.
- Do not introduce out-of-scope refactoring, speculative features, or unplanned dependencies.
- If unexpected architectural obstacles or requirements appear during implementation, pause and present an updated plan to the user for approval.

### 3.2 Clean Code & DRY (Don't Repeat Yourself)
- **Zero Code Duplication**: Avoid code duplication across modules. Shared domain models, calculators, mathematical formulas, and interfaces belong strictly in `common`.
- **SOLID Principles**: Keep classes, records, and methods small, focused, and adhering to the Single Responsibility Principle (SRP).
- **Domain-Accurate Naming**: Use explicit, unambiguous names reflecting financial, market microstructure, and prediction market concepts (e.g. `CandleTwapState`, `TwapPoint`, `Timeframe`, `PriceSnapshot`).

### 3.3 Zero Dead or Unused Code
- **Complete Cleanup**: After completing modifications, audit the codebase to ensure no dead, orphaned, or unreferenced code remains.
- **No Unused Imports or Fields**: Remove all unused imports, unused private fields, and unreachable code branches.
- **No Unused Method or Constructor Overloads**: There must be **NO** unused method or constructor overloads. If a method or constructor signature is changed, refactor all call sites across the entire project and delete obsolete overloads completely.

### 3.4 No Backward Compatibility – Refactor the Entire Codebase
- **Zero Legacy Compatibility Layers**: Never implement backward-compatibility wrappers, deprecation shims (`@Deprecated`), dual-path APIs, or fallback adapter layers.
- **Full Codebase Refactoring**: When changing an interface, signature, domain contract, or configuration property, immediately refactor all call sites across all submodules (`common`, `trading`, `live`, `paper`, `backtest`), tests, and scripts to the new solution.

### 3.5 No Fallback Logic or Heuristics (Fail Fast)
- **Exact Execution**: The solution must execute strictly according to specification.
- **No Silent Fallbacks**: Never implement silent fallbacks to previous behavior, heuristic approximations, or swallowed exceptions to hide unexpected states.
- **Fail Fast**: If inputs are invalid, exchange streams are corrupted, or invariant preconditions fail, throw an explicit, descriptive runtime exception immediately.

### 3.6 No Test-Only Methods in Production Code
- **Production Code Integrity**: Production classes (`src/main/java`) must **NEVER** contain methods, getters, setters, constructors, or internal hooks whose only caller is a test class.
- **Contract-Based Testing**: Test production classes strictly through their public contracts or clean package-private boundaries. Test fixtures, test doubles, and reflection helpers belong strictly in `src/test/java`.

### 3.7 Test-Driven Development (TDD)
- Strictly follow TDD (Red-Green-Refactor) for every class, method, and component:
  1. **Red**: Write a failing unit or component test asserting the intended functionality.
  2. **Green**: Implement the minimal production code necessary to make the test pass.
  3. **Refactor**: Clean up the implementation, eliminate duplication, and verify all tests remain green.
- Never write production code before its corresponding tests exist.

### 3.8 Constructor Injection for Dependency Injection
- **Mandatory Constructor Injection**: Always use constructor injection (`@Inject public MyService(...)`) for all CDI beans and Quarkus components.
- **Immutable Fields**: All injected dependencies must be assigned to `private final` fields.
- **No Field Injection**: Field injection (`@Inject private SomeService svc;`) is **strictly forbidden**. Constructor injection guarantees immutability, thread-safety, and seamless unit testing without container reflection.

---

## 4. Verification & Quality Assurance (Ověření)

### 4.1 Clean Multi-Module Compilation
- Verify that the entire multi-module project compiles cleanly from scratch without errors or warnings:
  ```bash
  ./mvnw clean compile
  ```

### 4.2 Multi-Level Test Pyramid Execution
- Execute and verify all levels of the testing pyramid before completing a task:
  1. **Unit Tests**: Isolated unit tests validating domain math, calculators, and parsers (`JUnit 5`, `AssertJ`, `Mockito`).
  2. **Component & Integration Tests**: Quarkus component tests verifying caching, Vert.x event loops, and CDI wiring (`@QuarkusTest`, `Awaitility`).
  3. **API & WebSocket Tests**: REST endpoint tests (`RestAssured`) and WebSocket streaming tests (`quarkus-websockets-next`).
  4. **UI Tests**: Web interface rendering, Qute templates, and chart component bindings.
  5. **Cucumber BDD Acceptance Tests**: End-to-end acceptance scenarios via `RunCucumberTest`.
  6. **Python Script Tests**: Verification of offline data tools via `python -m unittest discover tests`.
- **100% Pass Requirement**: Every single test across all levels must pass (0 failures, 0 errors, 0 broken tests).

---

## 5. Delivery & Git Protocol (Odevzdání)

### 5.1 Remote Repository Synchronization
- Stage all relevant modified, created, or deleted files cleanly.
- Perform `git commit` and `git push` to push changes to the remote Git repository.

### 5.2 Commit Message Format with Conductor Track Identifier
- Every commit message **MUST** explicitly start with the Conductor track identifier.
- Format:
  ```
  Resolves #<track-id> <Clear, imperative description of the changes>
  ```
  *Example*: `Resolves #38 Implement Kraken fee schedule parser and Cucumber acceptance tests`
- Use the identical track identifier for all subsequent commits, including bug fixes and review refinements, belonging to that track until a new track is started.
