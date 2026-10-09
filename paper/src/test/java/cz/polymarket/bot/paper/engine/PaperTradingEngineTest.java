package cz.polymarket.bot.paper.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.calculator.RealizedVolatilityCalculator;
import cz.polymarket.bot.calculator.VwapCalculator;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.exchange.BinanceHistoricalClient;
import cz.polymarket.bot.paper.storage.PaperTradeRepository;
import cz.polymarket.bot.strategy.NextCandleProbabilityModel;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategy;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategyConfig;
import cz.polymarket.bot.strategy.TradeDirection;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class PaperTradingEngineTest {

    @TempDir
    Path tempDir;

    private Vertx vertx;
    private PaperTradeRepository repository;
    private TWAPArbitrageStrategy strategy;
    private StrategyRegistry registry;
    private HourlyPriceCache priceCache;
    private BinanceHistoricalClient binanceHistoricalClient;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        ObjectMapper objectMapper = new ObjectMapper();
        PerformanceMetricsCalculator metricsCalculator = new PerformanceMetricsCalculator();
        repository = new PaperTradeRepository(tempDir.toString(), 10000.0, objectMapper, metricsCalculator);

        TWAPArbitrageStrategyConfig config = TWAPArbitrageStrategyConfig.defaultIteration20();
        strategy = new TWAPArbitrageStrategy(
                new NextCandleProbabilityModel(new cz.polymarket.bot.calculator.KellyPositionSizer(10000.0, 0.25, 50.0, 300.0)),
                config
        );
        registry = new StrategyRegistry(Map.of("TWAPArbitrageStrategy", strategy));
        priceCache = new HourlyPriceCache(3600L);
        binanceHistoricalClient = mock(BinanceHistoricalClient.class);
    }

    @AfterEach
    void tearDown() {
        if (vertx != null) {
            vertx.close();
        }
    }

    @Test
    void shouldResolveConfiguredStrategyAndDefaultLatency() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                50L,
                vertx,
                binanceHistoricalClient
        );

        assertThat(engine.getActiveStrategyName()).isEqualTo("TWAPArbitrageStrategy");
        assertThat(engine.getOrderLatencyMs()).isEqualTo(50L);
        assertThat(engine.getActivePosition()).isNull();
        assertThat(engine.getStatus()).isEqualTo("RUNNING");
    }

    @Test
    void shouldExecuteBuyOrderWithConfiguredLatency() throws InterruptedException {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                20L,
                vertx,
                binanceHistoricalClient
        );

        OrderCommand buyCmd = new OrderCommand(
                "order-1",
                "0xMarket15m",
                "0xTokenUp",
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                BigDecimal.valueOf(0.55),
                BigDecimal.valueOf(1000.0)
        );

        engine.submitOrder(buyCmd);

        // Immediate check: latency should delay fill
        assertThat(engine.getActivePosition()).isNull();

        // Wait for latency expiration
        Thread.sleep(60);

        PaperPosition pos = engine.getActivePosition();
        assertThat(pos).isNotNull();
        assertThat(pos.clientOrderId()).isEqualTo("order-1");
        assertThat(pos.side()).isEqualTo(TradeDirection.UP);
        assertThat(pos.entryPrice()).isEqualTo(0.55);
        assertThat(pos.shares()).isEqualTo(1000.0);
        assertThat(pos.sizeUsd()).isEqualTo(550.0);
        assertThat(pos.entryFee()).isGreaterThan(0.0);
    }

    @Test
    void shouldExecuteSellOrderAndRecordCompletedTrade() throws InterruptedException {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                0L, // synchronous for this test
                vertx,
                binanceHistoricalClient
        );

        OrderCommand buyCmd = new OrderCommand(
                "order-buy",
                "0xMarket15m",
                "0xTokenUp",
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                BigDecimal.valueOf(0.50),
                BigDecimal.valueOf(1000.0)
        );
        engine.submitOrder(buyCmd);
        assertThat(engine.getActivePosition()).isNotNull();

        // Submit Take-Profit SELL order
        OrderCommand sellCmd = new OrderCommand(
                "order-sell",
                "0xMarket15m",
                "0xTokenUp",
                Timeframe.FIFTEEN_MINUTES,
                "SELL",
                BigDecimal.valueOf(0.70),
                BigDecimal.valueOf(1000.0)
        );
        engine.submitOrder(sellCmd);

        // Position should now be closed
        assertThat(engine.getActivePosition()).isNull();
        assertThat(repository.getTrades()).hasSize(1);

        TradeRecord trade = repository.getTrades().get(0);
        assertThat(trade.side()).isEqualTo(TradeDirection.UP);
        assertThat(trade.entryPrice()).isEqualTo(0.50);
        assertThat(trade.exitPrice()).isEqualTo(0.70);
        assertThat(trade.exitReason()).isEqualTo("Take Profit (0.70)");
        assertThat(trade.isWin()).isTrue();
        assertThat(trade.netPnl()).isGreaterThan(0.0);
        assertThat(repository.getCurrentBalance()).isGreaterThan(10000.0);
    }

    @Test
    void shouldResolveHeldPositionAtCandleExpiration() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                0L,
                vertx,
                binanceHistoricalClient
        );

        long tStart = 1791038400L;
        long tEnd = tStart + 900L;

        // Initialize candle via TWAP update
        engine.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                tStart,
                tEnd,
                BigDecimal.valueOf(85000.0),
                new TwapPoint(tStart + 60, BigDecimal.valueOf(85100.0), BigDecimal.valueOf(85100.0)),
                true
        ));

        // Submit buy order for UP
        OrderCommand buyCmd = new OrderCommand(
                "order-up",
                "0xMarket15m",
                "0xTokenUp",
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                BigDecimal.valueOf(0.60),
                BigDecimal.valueOf(1000.0)
        );
        engine.submitOrder(buyCmd);
        assertThat(engine.getActivePosition()).isNotNull();

        // Resolve candle with winning outcome (UP)
        engine.resolveCandle(TradeDirection.UP, 85200.0);

        assertThat(engine.getActivePosition()).isNull();
        assertThat(repository.getTrades()).hasSize(1);
        TradeRecord trade = repository.getTrades().get(0);
        assertThat(trade.side()).isEqualTo(TradeDirection.UP);
        assertThat(trade.exitPrice()).isEqualTo(1.00);
        assertThat(trade.exitReason()).isEqualTo("Resolution (TWAP 60s)");
        assertThat(trade.isWin()).isTrue();
        assertThat(trade.netPnl()).isGreaterThan(0.0);
    }

    @Test
    void shouldSeedInitialCandlesOnStartup() {
        List<MarketCandle> mockCandles = List.of(
                new MarketCandle(1000, 1900, 80000, 80100, 79900, 80050, 10, 800000, 2, 80000, 80100, 79900, 80050, 10, 800000, 2, 0),
                new MarketCandle(1900, 2800, 80050, 80200, 80000, 80150, 12, 960000, 3, 80050, 80200, 80000, 80150, 12, 960000, 3, 0),
                new MarketCandle(2800, 3700, 80150, 80300, 80100, 80250, 15, 1200000, 4, 80150, 80300, 80100, 80250, 15, 1200000, 4, 0),
                new MarketCandle(3700, 4600, 80250, 80400, 80200, 80350, 11, 880000, 1, 80250, 80400, 80200, 80350, 11, 880000, 1, 0)
        );
        when(binanceHistoricalClient.fetch15mKlines(24)).thenReturn(mockCandles);

        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                0L,
                vertx,
                binanceHistoricalClient
        );

        engine.seedInitialCandles();
        verify(binanceHistoricalClient).fetch15mKlines(24);
    }

    @Test
    void shouldTriggerPhase2LateArbWhenDivergenceExceedsThreshold() throws InterruptedException {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                0L,
                vertx,
                binanceHistoricalClient
        );

        long tStart = 1791038400L;
        long tEnd = tStart + 900L;
        double openPrice = 85000.0;

        // 1. Initial candle setup at t=0
        engine.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                tStart,
                tEnd,
                BigDecimal.valueOf(openPrice),
                new TwapPoint(tStart, BigDecimal.valueOf(openPrice), BigDecimal.valueOf(openPrice)),
                true
        ));

        // 2. Advance to t=605s with spot diverging +$90 above open (> 80.0 threshold for Late Arb)
        double spotLate = openPrice + 90.0;
        engine.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                tStart,
                tEnd,
                BigDecimal.valueOf(openPrice),
                new TwapPoint(tStart + 605, BigDecimal.valueOf(openPrice + 50.0), BigDecimal.valueOf(spotLate)),
                false
        ));

        // Position should have been automatically opened by Phase 2 Late Arb
        PaperPosition pos = engine.getActivePosition();
        assertThat(pos).isNotNull();
        assertThat(pos.side()).isEqualTo(TradeDirection.UP);
        assertThat(pos.modelProbability()).isEqualTo(0.94);
        assertThat(pos.edge()).isGreaterThanOrEqualTo(0.06);

        // 3. Resolve candle at t=900s with UP outcome
        engine.resolveCandle(TradeDirection.UP, spotLate);

        assertThat(engine.getActivePosition()).isNull();
        assertThat(repository.getTrades()).hasSize(1);
        TradeRecord trade = repository.getTrades().get(0);
        assertThat(trade.side()).isEqualTo(TradeDirection.UP);
        assertThat(trade.exitPrice()).isEqualTo(1.00);
        assertThat(trade.exitReason()).isEqualTo("Resolution (TWAP 60s)");
        assertThat(trade.isWin()).isTrue();
        assertThat(trade.netPnl()).isGreaterThan(0.0);
    }

    @Test
    void shouldTriggerTakeProfitExitOnEarlyPhase1Entry() {
        // Pre-seed strategy with 4 historical candles so Phase 1 evaluates
        List<MarketCandle> mockCandles = List.of(
                new MarketCandle(1000, 1900, 80000, 80100, 79900, 80050, 10, 800000, 5, 80000, 80100, 79900, 80050, 10, 800000, 5, 2.0),
                new MarketCandle(1900, 2800, 80050, 80200, 80000, 80150, 12, 960000, 6, 80050, 80200, 80000, 80150, 12, 960000, 6, 2.0),
                new MarketCandle(2800, 3700, 80150, 80300, 80100, 80250, 15, 1200000, 7, 80150, 80300, 80100, 80250, 15, 1200000, 7, 2.0),
                new MarketCandle(3700, 4600, 80250, 80400, 80200, 80350, 11, 880000, 6, 80250, 80400, 80200, 80350, 11, 880000, 6, 2.0)
        );
        for (MarketCandle c : mockCandles) {
            strategy.onMarketCandleCompleted(c);
        }

        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                0L,
                vertx,
                binanceHistoricalClient
        );

        long tStart = 1791038400L;
        long tEnd = tStart + 900L;
        double openPrice = 80400.0;

        // 1. Initial candle setup at t=0
        engine.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                tStart,
                tEnd,
                BigDecimal.valueOf(openPrice),
                new TwapPoint(tStart, BigDecimal.valueOf(openPrice), BigDecimal.valueOf(openPrice)),
                true
        ));

        // 2. Simulate t=60s Phase 1 entry (bullish CVD and VWAP momentum)
        engine.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                tStart,
                tEnd,
                BigDecimal.valueOf(openPrice),
                new TwapPoint(tStart + 60, BigDecimal.valueOf(openPrice + 5.0), BigDecimal.valueOf(openPrice + 10.0)),
                false
        ));

        // Position was entered at ~0.50 - 0.53
        PaperPosition pos = engine.getActivePosition();
        if (pos != null) {
            assertThat(pos.entryPrice()).isLessThan(0.70);

            // 3. Contract bid rises to 0.70 at t=300s
            OrderBookQuote tpQuote = new OrderBookQuote(
                    0.72, 0.70, 0.30, 0.28, 5000, 5000, 0.72, 0.30, (tStart + 300) * 1000L
            );
            engine.onOrderBookQuote(tpQuote);

            // Take Profit should have exited
            assertThat(engine.getActivePosition()).isNull();
            assertThat(repository.getTrades()).hasSize(1);
            TradeRecord trade = repository.getTrades().get(0);
            assertThat(trade.exitPrice()).isEqualTo(0.70);
            assertThat(trade.exitReason()).contains("Take Profit");
            assertThat(trade.isWin()).isTrue();
        }
    }

    @Test
    void shouldIgnoreNonStrategyTimeframeUpdates() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                0,
                vertx,
                binanceHistoricalClient
        );

        long start15m = 1791038400L;
        // Process valid 15m update
        engine.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                start15m,
                start15m + 900L,
                BigDecimal.valueOf(85000.0),
                new TwapPoint(start15m, BigDecimal.valueOf(85000.0), BigDecimal.valueOf(85000.0)),
                false
        ));
        assertThat(engine.getTwapOpenPrice()).isEqualTo(85000.0);

        // Send 5m update with different start and open
        engine.onTwapUpdate(new TwapUpdate(
                Timeframe.FIVE_MINUTES,
                start15m + 300L,
                start15m + 600L,
                BigDecimal.valueOf(89000.0),
                new TwapPoint(start15m + 300L, BigDecimal.valueOf(89000.0), BigDecimal.valueOf(89000.0)),
                false
        ));

        // Must still retain 15m candle open price
        assertThat(engine.getTwapOpenPrice()).isEqualTo(85000.0);
    }

    @Test
    void shouldResetEngineAndRepository() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                null,
                priceCache,
                "TWAPArbitrageStrategy",
                0,
                vertx,
                binanceHistoricalClient
        );

        long tStart = 1791038400L;
        engine.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                tStart,
                tStart + 900L,
                BigDecimal.valueOf(85000.0),
                new TwapPoint(tStart, BigDecimal.valueOf(85000.0), BigDecimal.valueOf(85000.0)),
                false
        ));
        assertThat(engine.getTwapOpenPrice()).isEqualTo(85000.0);

        engine.reset();
        assertThat(engine.getTwapOpenPrice()).isEqualTo(0.0);
        assertThat(engine.getActivePosition()).isNull();

        repository.reset();
        assertThat(repository.getTrades()).isEmpty();
        assertThat(repository.getCurrentBalance()).isEqualTo(10000.0);
    }
}
