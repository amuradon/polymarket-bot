Feature: Aggregate BTC 60s Rolling TWAP Calculation

  Scenario: Calculate 1-second median price from Binance, Coinbase, and Kraken
    Given Binance price is 78000.50
    And Coinbase price is 78010.00
    And Kraken price is 77995.25
    When the 1-second median price is calculated
    Then the median price should be 78000.50

  Scenario: Calculate 60-second rolling TWAP window
    Given 60 one-second median prices with thirty at 100.00 and thirty at 110.00
    When the 60-second rolling TWAP is calculated
    Then the rolling TWAP should be 105.00

  Scenario: Reconstruct history using in-memory 60s TWAP cache when full range is available
    Given 60s TWAP prices for the past 120 seconds are cached
    When candle history is reconstructed for timeframe "5m"
    Then all 121 points should be reconstructed from cache without calling Binance REST
