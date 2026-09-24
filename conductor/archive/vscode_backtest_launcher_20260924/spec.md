# Specification: VS Code Launchers for Backtester

## 1. Overview
Configure VS Code launchers and build/dev tasks to seamlessly run and debug the `backtest` Quarkus application in development mode with live reload or directly via a standard Java main method.

## 2. Requirements
1. **Backtest Application Entry Point**:
   - Create `cz.polymarket.bot.backtest.BacktestApplication` matching `LiveTradingApplication` and `PaperTradingApplication`.
   - Provide `@QuarkusMain` with `public static void main(String... args)` invoking `Quarkus.run(BacktestApplication.class, args)`.
   - Log startup message on `StartupEvent`.
2. **VS Code Tasks (`.vscode/tasks.json`)**:
   - `quarkus:dev-backtest`:
     - Command: `./mvnw quarkus:dev -pl backtest` (Windows: `.\\mvnw.cmd quarkus:dev -pl backtest`)
     - Configured as background task with problem matcher detecting Quarkus startup and debug port `5005`.
   - `backtest:compile`: clean compile of `backtest` module.
   - `backtest:test`: run tests for `backtest` module.
3. **VS Code Launch Configurations (`.vscode/launch.json`)**:
   - `Backtest: Quarkus Dev (Attach 5005)`:
     - `request: attach`, `port: 5005`, `preLaunchTask: quarkus:dev-backtest`.
   - `Backtest: Direct Main (Run/Debug)`:
     - `request: launch`, `mainClass: cz.polymarket.bot.backtest.BacktestApplication`, `projectName: backtest`.
   - Maintain existing attach configurations for other modules.

## 3. Acceptance Criteria
- Starting `Backtest: Quarkus Dev (Attach 5005)` runs the `backtest` module on port `8083`.
- `BacktestApplication` compiles and runs cleanly.
