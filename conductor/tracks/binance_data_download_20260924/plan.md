# Implementation Plan: Binance Historical Data Downloader

## Phase 1: Dependencies & Acceptance Testing Harness (BDD & Configuration)
- [x] Task: 1.1 Configure backtest dependencies and application properties
  - [x] Add Cucumber BDD dependencies (`cucumber-java`, `cucumber-junit-platform-engine`) to `backtest/pom.xml`
  - [x] Add Zstandard decompression dependency (`com.github.luben:zstd-jni`) to `backtest/pom.xml`
  - [x] Add configuration properties to `backtest/src/main/resources/application.properties` (base data directory default `D:\Crypto\data\Polymarket\Binance`, CryptoHFTData API key, timeouts)
- [x] Task: 1.2 Define Cucumber BDD feature and test runner
  - [x] Create Gherkin acceptance scenarios in `backtest/src/test/resources/features/binance_data_download.feature`
  - [x] Create `RunCucumberTest.java` and scaffold `DataDownloadSteps.java`
- [x] Task: 1.3 Phase Verification & Checkpoint (Refer to workflow.md)

## Phase 2: Core Downloader Services & TDD Implementation (Domain & Ingestion Logic)
- [x] Task: 2.1 Request validation and job state domain models
  - [x] Write unit tests for request validation (`DownloadRequestTest`)
  - [x] Implement `DownloadRequest`, `DownloadJob`, `DownloadResult`, and `DataType` records in `cz.polymarket.bot.backtest.data`
- [x] Task: 2.2 Binance Spot & Futures aggTrades downloader
  - [x] Write unit tests for `BinanceAggTradesDownloaderTest` (URL resolution, existence check, zip decompression, CSV extraction, skip logging)
  - [x] Implement `BinanceAggTradesDownloader`
- [x] Task: 2.3 Binance Futures order book downloader (CryptoHFTData Java port)
  - [x] Write unit tests for `BinanceOrderBookDownloaderTest` (hourly iteration 00-23, flat naming `<symbol>-orderbook-<yyyy-MM-dd-HH>.parquet`, zstd decompression, skip logging)
  - [x] Implement `BinanceOrderBookDownloader`
- [x] Task: 2.4 Asynchronous download coordinator & job manager
  - [x] Write unit tests for `DataDownloadJobManagerTest`
  - [x] Implement `DataDownloadJobManager` coordinating async task execution and state tracking
- [x] Task: 2.5 Phase Verification & Checkpoint (Refer to workflow.md)

## Phase 3: REST API Endpoints & Multi-Level Verification
- [ ] Task: 3.1 REST API endpoints & Quarkus integration tests
  - [ ] Write `@QuarkusTest` REST Assured tests in `DataDownloadResourceTest`
  - [ ] Implement `DataDownloadResource` with `POST /api/1/data/download` (202 Accepted) and `GET /api/1/data/download/{jobId}`
- [ ] Task: 3.2 Cucumber BDD acceptance tests execution
  - [ ] Implement complete step definitions in `DataDownloadSteps.java`
  - [ ] Execute and verify all BDD scenarios pass
- [ ] Task: 3.3 Multi-Level Test Pyramid & Clean Multi-Module Compilation
  - [ ] Verify `./mvnw clean compile` succeeds across all 5 submodules
  - [ ] Run full test suite across the repository
- [ ] Task: 3.4 Phase Verification & Checkpoint (Refer to workflow.md)
