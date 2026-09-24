# Technology Stack: Polymarket Crypto Bot

## Backend & Frameworks
- **Language**: Java 25 (`maven.compiler.release: 25`)
- **Core Framework**: Quarkus 3.39.2
  - `quarkus-rest` & `quarkus-rest-jackson` (High-performance non-blocking REST endpoints)
  - `quarkus-rest-client` / Vert.x HTTP client (External REST communication)
  - `quarkus-websockets-next` & `quarkus-vertx` (Reactive WebSocket exchange clients and live streams)
  - `quarkus-rest-qute` (Type-safe server-side rendering)
- **Build System**: Maven Multi-Module Architecture
  - `common` (Domain models, calculators, cache, contracts)
  - `trading` (Exchange clients, TWAP engine, UI resources)
  - `live` (Standalone live trading Quarkus application)
  - `paper` (Standalone paper trading simulation Quarkus application)
  - `backtest` (Standalone backtesting & historical ingestion Quarkus application)

## Testing & Quality Assurance
- **Unit & Integration Testing**: JUnit 6 (Platform / Jupiter), `@QuarkusTest`, RestAssured
- **BDD Acceptance Testing**: Cucumber (`cucumber-java`, `cucumber-junit-platform-engine`)
- **Assertions & Concurrency**: AssertJ, Awaitility, Mockito

## Frontend & Visualization
- **Templating & Dynamic Interactions**: Quarkus Qute templates & HTMX
- **Client Scripting & UI Components**: jQuery and jQuery UI (preferred for UI state and controls)
- **Financial Charting**: TradingView Lightweight Charts (v5, configured with Light Theme)
- **Styling**: HTML5 & CSS3

## Data Engineering & Offline Tooling
- **Python**: Python 3.12
- **Data Manipulation**: PyArrow, Pandas, Zstandard, Requests, cryptohftdata
