package cz.polymarket.bot.cucumber;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.MedianCalculator;
import cz.polymarket.bot.calculator.TwapCalculator;
import cz.polymarket.bot.domain.CandleTwapState;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.exchange.BinanceHistoricalClient;
import cz.polymarket.bot.exchange.HistoricalDataReconstructor;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class TwapSteps {

    private final MedianCalculator medianCalculator = new MedianCalculator();
    private final TwapCalculator twapCalculator = new TwapCalculator();

    private BigDecimal binancePrice;
    private BigDecimal coinbasePrice;
    private BigDecimal krakenPrice;
    private BigDecimal calculatedMedian;

    private final List<BigDecimal> priceWindow = new ArrayList<>();
    private BigDecimal calculatedRollingTwap;

    private HourlyPriceCache cache;
    private BinanceHistoricalClient binanceClient;
    private HistoricalDataReconstructor reconstructor;
    private CandleTwapState reconstructedState;
    private Instant now;

    @Given("Binance price is {bigdecimal}")
    public void binancePriceIs(BigDecimal price) {
        this.binancePrice = price;
    }

    @And("Coinbase price is {bigdecimal}")
    public void coinbasePriceIs(BigDecimal price) {
        this.coinbasePrice = price;
    }

    @And("Kraken price is {bigdecimal}")
    public void krakenPriceIs(BigDecimal price) {
        this.krakenPrice = price;
    }

    @When("the 1-second median price is calculated")
    public void the1SecondMedianPriceIsCalculated() {
        this.calculatedMedian = medianCalculator.calculate(binancePrice, coinbasePrice, krakenPrice);
    }

    @Then("the median price should be {bigdecimal}")
    public void theMedianPriceShouldBe(BigDecimal expected) {
        assertThat(calculatedMedian).isEqualByComparingTo(expected);
    }

    @Given("60 one-second median prices with thirty at {bigdecimal} and thirty at {bigdecimal}")
    public void sixtyOneSecondMedianPricesWithThirtyAtAndThirtyAt(BigDecimal p1, BigDecimal p2) {
        priceWindow.clear();
        for (int i = 0; i < 30; i++) {
            priceWindow.add(p1);
        }
        for (int i = 0; i < 30; i++) {
            priceWindow.add(p2);
        }
    }

    @When("the 60-second rolling TWAP is calculated")
    public void the60SecondRollingTwapIsCalculated() {
        calculatedRollingTwap = twapCalculator.calculateRollingTwap(priceWindow);
    }

    @Then("the rolling TWAP should be {bigdecimal}")
    public void theRollingTwapShouldBe(BigDecimal expected) {
        assertThat(calculatedRollingTwap).isEqualByComparingTo(expected);
    }

    @Given("60s TWAP prices for the past 120 seconds are cached")
    public void sixtySecTwapPricesForThePast120SecondsAreCached() {
        cache = new HourlyPriceCache(3600);
        binanceClient = mock(BinanceHistoricalClient.class);
        reconstructor = new HistoricalDataReconstructor(cache, binanceClient, twapCalculator);

        now = Instant.parse("2026-09-03T14:02:00Z");
        long start = Instant.parse("2026-09-03T14:00:00Z").getEpochSecond();
        long end = now.getEpochSecond();

        for (long t = start; t <= end; t++) {
            cache.put(t, new BigDecimal("78823.52"));
        }
    }

    @When("candle history is reconstructed for timeframe {string}")
    public void candleHistoryIsReconstructedForTimeframe(String tfCode) {
        Timeframe tf = Timeframe.fromCode(tfCode);
        reconstructedState = reconstructor.reconstructCandle(tf, now);
    }

    @Then("all {int} points should be reconstructed from cache without calling Binance REST")
    public void allPointsShouldBeReconstructedFromCacheWithoutCallingBinanceRest(int expectedCount) {
        assertThat(reconstructedState.points()).hasSize(expectedCount);
        verify(binanceClient, never()).fetch1sKlines(anyLong(), anyLong());
    }
}
