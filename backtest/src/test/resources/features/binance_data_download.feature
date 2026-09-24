Feature: Binance Historical Data Download via REST API

  Background:
    Given the data storage directory is initialized

  Scenario: Successfully initiate download for daily spot trades, futures trades, and orderbook
    When a user requests download for symbol "BTCUSDT" from "2026-08-01" to "2026-08-02"
    Then the system accepts the request with status 202
    And a valid job ID is returned
    And the job downloads spot aggTrades into "spot/BTCUSDT/aggTrades"
    And the job downloads futures aggTrades into "futures/BTCUSDT/aggTrades"
    And the job downloads futures orderbook into "futures/BTCUSDT/orderBook"

  Scenario: Skip download when target files already exist
    Given spot aggTrades file "BTCUSDT-aggTrades-2026-08-01.csv" already exists in "spot/BTCUSDT/aggTrades"
    And futures aggTrades file "BTCUSDT-aggTrades-2026-08-01.csv" already exists in "futures/BTCUSDT/aggTrades"
    And futures orderbook file "BTCUSDT-orderbook-2026-08-01-00.parquet" already exists in "futures/BTCUSDT/orderBook"
    When a user requests download for symbol "BTCUSDT" from "2026-08-01" to "2026-08-01"
    Then the system accepts the request with status 202
    And the job skips the existing files and logs INFO messages

  Scenario: Reject download request when start date is after end date
    When a user requests download for symbol "BTCUSDT" from "2026-08-05" to "2026-08-01"
    Then the system rejects the request with status 400
    And the error message indicates "start date must not be after end date"

  Scenario: Query job status after triggering download
    When a user requests download for symbol "BTCUSDT" from "2026-08-01" to "2026-08-01"
    Then the system accepts the request with status 202
    When the user queries the status for the returned job ID
    Then the job status is returned with progress statistics
