# Specification: Binance Historical Data Downloader

## 1. Overview
Provide a REST API in the `backtest` application to download historical daily cryptocurrency market data from Binance (Spot aggTrades, Futures aggTrades, and Futures order book) into local directories on disk, following idempotent file-check semantics.

## 2. Functional Requirements

### 2.1 REST API Endpoints
- **Trigger Download**:
  - `POST /api/1/data/download`
  - **Request Body (JSON)**:
    ```json
    {
      "symbol": "BTCUSDT",
      "start": "2026-08-01",
      "end": "2026-08-05",
      "dataTypes": ["spot_trades", "futures_trades", "orderbook"]
    }
    ```
    *(Note: `dataTypes` is optional and defaults to all 3).*
  - **Response**: `202 Accepted`
    ```json
    {
      "jobId": "b9f5e1a2-...",
      "status": "QUEUED",
      "symbol": "BTCUSDT",
      "startDate": "2026-08-01",
      "endDate": "2026-08-05",
      "dataTypes": ["spot_trades", "futures_trades", "orderbook"]
    }
    ```
  - **Validation**:
    - `symbol` must not be blank and must be uppercase alphanumeric (e.g. `BTCUSDT`).
    - `start` and `end` must be valid dates in `yyyy-MM-dd` format.
    - `start` must not be after `end`.
    - Returns `400 Bad Request` with descriptive error payload if validation fails.

- **Check Download Status**:
  - `GET /api/1/data/download/{jobId}`
  - **Response**: `200 OK`
    ```json
    {
      "jobId": "b9f5e1a2-...",
      "status": "IN_PROGRESS | COMPLETED | FAILED",
      "symbol": "BTCUSDT",
      "downloadedFiles": 14,
      "skippedFiles": 2,
      "failedFiles": 0,
      "errors": []
    }
    ```

### 2.2 Storage Layout & Idempotency Check
Base path default: `D:\Crypto\data\Polymarket\Binance` (configurable via `application.properties`).

1. **Binance Spot aggTrades**:
   - Destination: `D:\Crypto\data\Polymarket\Binance\spot\<symbol>\aggTrades`
   - File format: `<symbol>-aggTrades-<yyyy-MM-dd>.csv`
   - Source: `https://data.binance.vision/data/spot/daily/aggTrades/<symbol>/<symbol>-aggTrades-<yyyy-MM-dd>.zip`
   - Idempotency: Before downloading, verify if `<symbol>-aggTrades-<yyyy-MM-dd>.csv` exists in the target directory. If yes, log `INFO: File <file> already exists, skipping download.` and increment skipped counter.
   - Processing: Download zip stream, decompress CSV entry directly to target path, clean up temp archive.

2. **Binance Futures aggTrades**:
   - Destination: `D:\Crypto\data\Polymarket\Binance\futures\<symbol>\aggTrades`
   - File format: `<symbol>-aggTrades-<yyyy-MM-dd>.csv`
   - Source: `https://data.binance.vision/data/futures/um/daily/aggTrades/<symbol>/<symbol>-aggTrades-<yyyy-MM-dd>.zip`
   - Idempotency: Check if `<symbol>-aggTrades-<yyyy-MM-dd>.csv` exists. If yes, log `INFO` and skip.
   - Processing: Download zip and extract CSV to target path.

3. **Binance Futures Order Book (CryptoHFTData Java Port)**:
   - Destination: `D:\Crypto\data\Polymarket\Binance\futures\<symbol>\orderBook`
   - File format: `<symbol>-orderbook-<yyyy-MM-dd-HH>.parquet` (flat layout for all 24 hours `00`..`23`)
   - Source: `https://api.cryptohftdata.com/v1/download?file=binance_futures/<yyyy-MM-dd>/<HH>/<symbol>_orderbook.parquet`
   - Auth: Optional `CRYPTOHFTDATA_API_KEY` via env / config. If omitted, uses free-tier pacing.
   - Compression: Decompress Zstandard frame (`0x28B52FFD`) to raw Parquet if compressed.
   - Idempotency: If `<symbol>-orderbook-<yyyy-MM-dd-HH>.parquet` exists, log `INFO` and skip.

## 3. Non-Functional Requirements & Architecture
- **Zero Backtest Leakage**: Ingestion logic, HTTP client, and controllers reside exclusively within the `backtest` module.
- **Asynchronous Execution**: Long-running downloads run in background managed threads without blocking Quarkus HTTP event loops.
- **Fail-Fast & Atomic Writes**: Incomplete downloads write to `.part` files and rename atomically upon successful completion.

## 4. Acceptance Criteria & Cucumber BDD
- BDD scenarios in `backtest/src/test/resources/features/binance_data_download.feature`:
  - Successfully trigger download job and receive 202 Accepted.
  - Skip existing CSV and Parquet files without downloading.
  - Validate bad requests (invalid date range, blank symbol).
  - Track job progress via status endpoint.
