package cz.polymarket.bot.paper.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.calculator.RealizedVolatilityCalculator;
import cz.polymarket.bot.calculator.VwapCalculator;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class PaperTradingEngineTest {

    @TempDir
    Path tempDir;

    private Vertx vertx;
    private PaperTradeRepository repository;
    private TWAPArbitrageStrategy strategy;
    private StrategyRegistry registry;
    private HourlyPriceCache priceCache;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        ObjectMapper objectMapper = new ObjectMapper();
        PerformanceMetricsCalculator metricsCalculator = new PerformanceMetricsCalculator();
        repository = new PaperTradeRepository(tempDir.toString(), 10000.0, objectMapper, metricsCalculator);

        TWAPArbitrageStrategyConfig config = TWAPArbitrageStrategyConfig.defaultIteration20();
        strategy = new TWAPArbitrageStrategy(
                new VwapCalculator(),
                new RealizedVolatilityCalculator(),
                new NextCandleProbabilityModel(new cz.polymarket.bot.calculator.KellyPositionSizer(10000.0, 0.25, 50.0, 300.0)),
                config
        );
        registry = new StrategyRegistry(Map.of("TWAPArbitrageStrategy", strategy));
        priceCache = new HourlyPriceCache(3600L);
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
                vertx
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
                vertx
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
                vertx
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
                vertx
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
}
