# Polymarket Crypto Bot

Real-time cryptocurrency price aggregation, TWAP (Time-Weighted Average Price) calculation, market data collection, and automated trading platform designed for Polymarket prediction markets.

---

## Architecture & Vision

- 👉 **[Architecture Guide (`ARCHITECTURE.md`)](ARCHITECTURE.md)**: Full module hierarchy, dependency rules, isolation guarantees, and design contracts.
- 👉 **[Product & Engineering Vision (`VISION.md`)](VISION.md)**: Platform roadmap, multi-token markets (BTC, ETH, SOL), 3-system lifecycle (Backtest $\rightarrow$ Paper $\rightarrow$ Live), and low-latency deployment principles.
- 👉 **[Deployment Guide (`DEPLOYMENT.md`)](DEPLOYMENT.md)**: AWS EC2 with EBS (`eu-west-1`) deployment instructions and continuous 24/7 execution requirements.

---

## Project Structure

The project is structured as a Maven Multi-Module project with 7 submodules:

| Submodule | Type | Description | Default Port |
| :--- | :--- | :--- | :--- |
| **`common`** | Library (`jar`) | Shared domain records, math calculators, in-memory price cache, and strategy execution contracts. | N/A |
| **`connector`** | Library (`jar`) | Shared Binance, Deribit, and Polymarket WebSocket/REST connectors and market discovery (zero dependency on `common`). | N/A |
| **`trading`** | Library (`jar`) | Exchange price tracker, `TwapEngine`, Qute templates, web console (`/btc-usd`), and WebSocket endpoints. | N/A |
| **`data`** | Application (`jar`) | Standalone runnable Quarkus application for recording raw Binance, Deribit, and Polymarket streams in JSONL + Zstandard streaming format. | `8084` |
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
- **Zstandard (`zstd-jni`)**: High-speed streaming compression (`.jsonl.zst`) for real-time market data collection.

### Frontend
- **Quarkus Qute**: Server-side template rendering.
- **TradingView Lightweight Charts (v5)**: Financial charting library for high-performance canvas rendering.
- **HTML5 / CSS3 / Vanilla JavaScript**: Responsive layout and WebSocket connection management with zero heavy JS framework dependencies.

### Testing & QA
- **JUnit 5 / QuarkusTest**: Unit, integration, and reactive resource testing.
- **AssertJ & Awaitility**: Fluent assertions and asynchronous polling assertions for WebSocket/concurrent flows.
- **Mockito**: Mocking external services and HTTP endpoints.
- **pytest**: Automated unit testing for Python tooling.

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
- Status: `http://localhost:8083/api/status`
- Trigger Binance historical data download: `POST http://localhost:8083/api/data/download/binance`
- Trigger asynchronous backtest simulation: `POST http://localhost:8083/api/backtest/run`

#### 4. Data Collector Application (`data` - Port 8084)
```bash
./mvnw quarkus:dev -pl data
```
- Collector status & active stream diagnostics: `GET http://localhost:8084/api/collector/status`

---

## Running Tests

Execute the full multi-module test suite across all submodules:
```bash
./mvnw clean test
```