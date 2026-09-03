package cz.polymarket.bot.cucumber;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.MedianCalculator;
import cz.polymarket.bot.calculator.TwapCalculator;
import cz.polymarket.bot.domain.CandleTwapState;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
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

    private BigDecimal openPrice;
    private final List<TwapPoint> twapPoints = new ArrayList<>();

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

    @Given("a candle starts with open price {bigdecimal}")
    public void aCandleStartsWithOpenPrice(BigDecimal openPrice) {
        this.openPrice = openPrice;
        this.twapPoints.clear();
        this.twapPoints.add(twapCalculator.createInitialPoint(1000L, openPrice));
    }

    @When("1-second median prices are {bigdecimal}, {bigdecimal}, and {bigdecimal}")
    public void oneSecondMedianPricesAre(BigDecimal p1, BigDecimal p2, BigDecimal p3) {
        TwapPoint pt1 = twapCalculator.calculateNext(twapPoints.get(0), 1001L, p1, 1);
        twapPoints.add(pt1);

        TwapPoint pt2 = twapCalculator.calculateNext(pt1, 1002L, p2, 2);
        twapPoints.add(pt2);

        TwapPoint pt3 = twapCalculator.calculateNext(pt2, 1003L, p3, 3);
        twapPoints.add(pt3);
    }

    @Then("the TWAP at second {int} should be {bigdecimal}")
    public void theTwapAtSecondShouldBe(int second, BigDecimal expected) {
        assertThat(twapPoints.get(second).twap()).isEqualByComparingTo(expected);
    }

    @Given("1-second prices for the past 120 seconds are cached")
    public void oneSecondPricesForThePast120SecondsAreCached() {
        cache = new HourlyPriceCache(3600);
        binanceClient = mock(BinanceHistoricalClient.class);
        reconstructor = new HistoricalDataReconstructor(cache, binanceClient, twapCalculator);

        now = Instant.parse("2026-09-03T14:02:00Z");
        long start = Instant.parse("2026-09-03T14:00:00Z").getEpochSecond();
        long end = now.getEpochSecond();

        for (long t = start; t <= end; t++) {
            cache.put(t, new BigDecimal("78000.00"));
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
