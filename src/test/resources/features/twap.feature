Feature: Aggregate BTC TWAP Calculation

  Scenario: Calculate 1-second median price from Binance, Coinbase, and Kraken
    Given Binance price is 78000.50
    And Coinbase price is 78010.00
    And Kraken price is 77995.25
    When the 1-second median price is calculated
    Then the median price should be 78000.50

  Scenario: Calculate cumulative TWAP starting with candle open price
    Given a candle starts with open price 100.00
    When 1-second median prices are 110.00, 120.00, and 90.00
    Then the TWAP at second 1 should be 105.00
    And the TWAP at second 2 should be 110.00
    And the TWAP at second 3 should be 105.00

  Scenario: Reconstruct history using in-memory cache when full range is available
    Given 1-second prices for the past 120 seconds are cached
    When candle history is reconstructed for timeframe "5m"
    Then all 121 points should be reconstructed from cache without calling Binance REST
