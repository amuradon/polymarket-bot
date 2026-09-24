# Implementation Plan: VS Code Launchers for Backtester

## Phase 1: Backtest Application Entry Point
- [x] Task: 1.1 Create BacktestApplication and startup test
  - [x] Write unit test `BacktestApplicationTest`
  - [x] Implement `BacktestApplication` with `@QuarkusMain` and `StartupEvent` listener
- [x] Task: 1.2 Phase Verification & Checkpoint (Refer to workflow.md)

## Phase 2: VS Code Task and Launch Configuration
- [x] Task: 2.1 Configure .vscode/tasks.json
  - [x] Add `quarkus:dev-backtest` background task
  - [x] Add `backtest:compile` and `backtest:test` tasks
- [x] Task: 2.2 Configure .vscode/launch.json
  - [x] Add `Backtest: Quarkus Dev (Attach 5005)` configuration
  - [x] Add `Backtest: Direct Main (Run/Debug)` configuration
- [x] Task: 2.3 Phase Verification & Checkpoint (Refer to workflow.md)
