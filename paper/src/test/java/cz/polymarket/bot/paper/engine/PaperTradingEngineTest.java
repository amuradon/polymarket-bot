package cz.polymarket.bot.paper.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.paper.storage.PaperTradeRepository;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.UtterParameciumStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategyConfig;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PaperTradingEngineTest {

    @TempDir
    Path tempDir;

    private Vertx vertx;
    private PaperTradeRepository repository;
    private StrategyRegistry registry;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        ObjectMapper objectMapper = new ObjectMapper();
        PerformanceMetricsCalculator metricsCalculator = new PerformanceMetricsCalculator();
        repository = new PaperTradeRepository(tempDir.toString(), 10000.0, objectMapper, metricsCalculator);

        UtterParameciumStrategy utterStrategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        registry = new StrategyRegistry(Map.of(
                utterStrategy.getName(), utterStrategy
        ));
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
                "UtterParameciumStrategy",
                50L,
                vertx
        );

        assertThat(engine.getActiveStrategyName()).isEqualTo("UtterParameciumStrategy");
        assertThat(engine.getOrderLatencyMs()).isEqualTo(50L);
        assertThat(engine.getActivePosition()).isNull();
        assertThat(engine.getStatus()).isEqualTo("RUNNING");
    }

    @Test
    void shouldExecuteBuyOrderWithConfiguredLatency() throws InterruptedException {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                "UtterParameciumStrategy",
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
        for (int i = 0; i < 50 && engine.getActivePosition() == null; i++) {
            Thread.sleep(20);
        }

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
    void shouldExecuteSellOrderAndRecordCompletedTrade() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                "UtterParameciumStrategy",
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
                "UtterParameciumStrategy",
                0L,
                vertx
        );

        long tStart = 1785542400L;
        long tStartMs = tStart * 1000L;

        // Initialize candle via 60s TWAP update
        engine.onPolymarketCryptoTwap60s(tStartMs, "btcusd", "chainlink", 85000.0, 60, tStartMs);

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
    void shouldAccumulateMultiBatchAndBidirectionalPositionsAndResolveAtExpiration() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                "utter_paramecium",
                0L,
                vertx
        );

        assertThat(engine.getActiveStrategyName()).isEqualTo("UtterParameciumStrategy");

        long tStart = 1785542400L;
        long tStartMs = tStart * 1000L;

        engine.onPolymarketCryptoTwap60s(tStartMs, "btcusd", "chainlink", 85000.0, 60, tStartMs);
        assertThat(engine.getTwapOpenPrice()).isEqualTo(85000.0);

        // Submit two UP batch orders and one DOWN batch order in the same candle
        engine.submitOrder(new OrderCommand(
                "up-batch-1", "0xMarket15m", "0xTokenUp",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.55), BigDecimal.valueOf(15.0)
        ));
        engine.submitOrder(new OrderCommand(
                "up-batch-2", "0xMarket15m", "0xTokenUp",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.58), BigDecimal.valueOf(25.0)
        ));
        engine.submitOrder(new OrderCommand(
                "down-batch-1", "0xMarket15m", "0xTokenDown",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.60), BigDecimal.valueOf(10.0)
        ));

        assertThat(engine.getOpenPositions()).hasSize(3);

        // Resolve candle via next candle's 60s TWAP update -> 2 UP batches win (exit 1.00), 1 DOWN batch loses (exit 0.00)
        long tEndMs = (tStart + 900L) * 1000L;
        engine.onPolymarketCryptoTwap60s(tEndMs, "btcusd", "chainlink", 85050.0, 60, tEndMs);

        assertThat(engine.getOpenPositions()).isEmpty();
        assertThat(engine.getActivePosition()).isNull();
        assertThat(repository.getTrades()).hasSize(3);
        assertThat(repository.getTrades().stream().filter(TradeRecord::isWin).count()).isEqualTo(2);
        assertThat(repository.getTrades().stream().filter(t -> !t.isWin()).count()).isEqualTo(1);
    }

    @Test
    void shouldResetEngineAndRepository() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                "UtterParameciumStrategy",
                0,
                vertx
        );

        long tStartMs = 1785542400L * 1000L;
        engine.onPolymarketCryptoTwap60s(tStartMs, "btcusd", "chainlink", 85000.0, 60, tStartMs);
        assertThat(engine.getTwapOpenPrice()).isEqualTo(85000.0);

        engine.reset();
        assertThat(engine.getTwapOpenPrice()).isEqualTo(0.0);
        assertThat(engine.getActivePosition()).isNull();

        repository.reset();
        assertThat(repository.getTrades()).isEmpty();
        assertThat(repository.getCurrentBalance()).isEqualTo(10000.0);
    }

    @Test
    void shouldFillBuyOrderAtCurrentBestAskWhenBelowLimitPriceAndRejectWhenAskExceedsLimitPrice() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                "utter_paramecium",
                0L,
                vertx
        );

        long tsMs = 1_785_542_400_000L;
        engine.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tsMs
        ));

        // 1. Order with looser limit price 0.70 must fill at current bestAskUp = 0.65 (price improvement)
        engine.submitOrder(new OrderCommand(
                "buy-price-improved", "0xMarket15m", "BTC-UP",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.70), BigDecimal.valueOf(10.0)
        ));

        assertThat(engine.getOpenPositions()).hasSize(1);
        assertThat(engine.getOpenPositions().getFirst().entryPrice()).isEqualTo(0.65);

        // 2. Order with limit price 0.62 (< current bestAskUp 0.65) must be rejected (no match for FAK)
        engine.submitOrder(new OrderCommand(
                "buy-rejected-slippage", "0xMarket15m", "BTC-UP",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.62), BigDecimal.valueOf(10.0)
        ));

        assertThat(engine.getOpenPositions()).hasSize(1);
    }

    @Test
    void shouldIgnoreOutOfOrderOlderCandleTwapTicksWithoutRollingActiveCandleBackwards() {
        PaperTradingEngine engine = new PaperTradingEngine(
                registry,
                repository,
                "utter_paramecium",
                0L,
                vertx
        );

        long activeStartSec = 1_794_240_000L;
        long activeStartMs = activeStartSec * 1000L;

        engine.onPolymarketCryptoTwap60s(activeStartMs, "btcusd", "chainlink", 85_000.0, 60, activeStartMs);
        assertThat(engine.getTwapOpenPrice()).isEqualTo(85_000.0);

        engine.submitOrder(new OrderCommand(
                "up-hold-1", "0xMarket15m", "BTC-UP",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.55), BigDecimal.valueOf(20.0)
        ));
        assertThat(engine.getOpenPositions()).hasSize(1);

        long priorCandleTickMs = (activeStartSec - 48L) * 1000L;
        engine.onPolymarketCryptoTwap60s(priorCandleTickMs, "btcusd", "chainlink", 84_950.0, 60, priorCandleTickMs);
        engine.onPolymarketCryptoTwap60s(activeStartMs + 11_000L, "btcusd", "chainlink", 85_010.0, 60, activeStartMs + 11_000L);

        assertThat(engine.getOpenPositions()).hasSize(1);
        assertThat(repository.getTrades()).isEmpty();
        assertThat(engine.getTwapOpenPrice()).isEqualTo(85_000.0);
    }
}
