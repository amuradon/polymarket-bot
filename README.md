# Polymarket Crypto Bot

Real-time cryptocurrency price aggregation, TWAP (Time-Weighted Average Price) calculation, and automated trading platform designed for Polymarket prediction markets.

---

## Architecture & Vision

- 👉 **[Architecture Guide (`ARCHITECTURE.md`)](ARCHITECTURE.md)**: Full module hierarchy, dependency rules, isolation guarantees, and design contracts.
- 👉 **[Product & Engineering Vision (`VISION.md`)](VISION.md)**: Platform roadmap, multi-token markets (BTC, ETH, SOL), 3-system lifecycle (Backtest $\rightarrow$ Paper $\rightarrow$ Live), and low-latency GCP Dublin deployment principles.

---

## Project Structure

The project is structured as a Maven Multi-Module project with 5 submodules:

| Submodule | Type | Description | Default Port |
| :--- | :--- | :--- | :--- |
| **`common`** | Library (`jar`) | Shared domain records, math calculators, in-memory price cache, and strategy execution contracts. | N/A |
| **`trading`** | Library (`jar`) | Exchange clients (Binance, Coinbase, Kraken), `TwapEngine`, Qute templates, web console (`/btc-usd`), and WebSocket endpoints. | N/A |
| **`live`** | Application (`jar`) | Standalone runnable Quarkus application for live trading execution on Polymarket. | `8080` |
| **`paper`** | Application (`jar`) | Standalone runnable Quarkus application for paper trading simulation with real-time market data. | `8082` |
| **`backtest`** | Application (`jar`) | Standalone runnable Quarkus application for backtesting strategies on historical datasets. | `8083` |

---

## Technology Stack

### Backend & Core
- **Java 25**: Modern Java leveraging newest language features.
- **Quarkus 3.39.2**: Supersonic Subatomic Java framework.
  - `quarkus-rest` & `quarkus-rest-jackson`: Non-blocking RESTful endpoints with JSON serialization.
  - `quarkus-rest-qute`: High-performance, type-safe server-side HTML templating.
  - `quarkus-websockets-next`: Next-generation reactive WebSocket implementation.
  - `quarkus-vertx`: Eclipse Vert.x reactive toolkit for asynchronous networking and WebSocket exchange clients.
  - `quarkus-scheduler`: Background task scheduling.

### Frontend
- **Quarkus Qute**: Server-side template rendering.
- **TradingView Lightweight Charts (v5)**: Financial charting library for high-performance canvas rendering.
- **HTML5 / CSS3 / Vanilla JavaScript**: Responsive layout and WebSocket connection management with zero heavy JS framework dependencies.

### Testing & QA
- **JUnit 5 / QuarkusTest**: Unit, integration, and reactive resource testing.
- **Cucumber (BDD)**: Gherkin feature specifications (`twap.feature`) validating TWAP calculations and business requirements.
- **AssertJ & Awaitility**: Fluent assertions and asynchronous polling assertions for WebSocket/concurrent flows.
- **Mockito**: Mocking external services and HTTP endpoints.
- **pytest**: Automated unit testing for Python tooling.

### Data Engineering (Python)
- **Python 3.12**
- **PyArrow & Pandas**: High-performance columnar Parquet processing.
- **cryptohftdata**: Bulk high-frequency data ingestion.

---

## Getting Started

### Prerequisites
- **JDK 25** installed and configured (`JAVA_HOME`).
- **Maven 3.9+** (or use the included Maven wrapper `mvnw` / `mvnw.cmd`).
- **Python 3.12+** (optional, for offline data scripts).

### Running Applications in Development Mode

#### 1. Live Trading Application (`live` - Port 8080)
```bash
./mvnw quarkus:dev -pl live
```
Navigate to `http://localhost:8080/btc-usd`.

#### 2. Paper Trading Application (`paper` - Port 8082)
```bash
./mvnw quarkus:dev -pl paper
```
Navigate to `http://localhost:8082/btc-usd`.

#### 3. Backtest Engine Application (`backtest` - Port 8083)
```bash
./mvnw quarkus:dev -pl backtest
```
Navigate to `http://localhost:8083/backtest`.

---

## Running Tests

Execute the full multi-module test suite across all submodules:
```bash
./mvnw clean test
```

---

## Running Data Scripts (Python)
```bash
# In first run create virtual environment
py -m venv .venv

# Activate virtual environment
.venv\Scripts\activate

# Install dependencies (only first run)
pip install marketlens
pip install cryptohftdata

# Download Binance Futures order book data
python scripts/download_binance_orderbook.py --symbol BTCUSDT --start-date 2026-08-01 --end-date 2026-08-02 --data-dir ./data/orderbook

# Export Polymarket historical data from marketlens.trade
python scripts/export_series.py --after 2026-08-07T00:00:00Z --before 2026-09-07T00:00:00Z

# Run Python script tests
python -m unittest discover tests
```