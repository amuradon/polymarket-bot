package cz.polymarket.bot.paper.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
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
import io.vertx.core.http.HttpServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PaperTradingEngineTest {

    @TempDir
    Path tempDir;

    private Vertx vertx;
    private HttpServer mockServer;
    private int port;
    private ObjectMapper objectMapper;
    private PaperTradeRepository repository;
    private StrategyRegistry registry;
    private BinanceStreamConnector binanceConnector;
    private KrakenStreamConnector krakenConnector;
    private CoinbaseStreamConnector coinbaseConnector;

    @BeforeEach
    void setUp() throws Exception {
        vertx = Vertx.vertx();
        objectMapper = new ObjectMapper();
        PerformanceMetricsCalculator metricsCalculator = new PerformanceMetricsCalculator();
        repository = new PaperTradeRepository(tempDir.toString(), 10000.0, objectMapper, metricsCalculator);

        UtterParameciumStrategy utterStrategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        registry = new StrategyRegistry(Map.of(
                utterStrategy.getName(), utterStrategy
        ));

        mockServer = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    String path = ws.path();
                    if (path.startsWith("/binance/spot/") && path.endsWith("@aggTrade")) {
                        ws.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1785542401000,\"s\":\"BTCUSDT\",\"a\":1001,\"p\":\"85010.00\",\"q\":\"0.75\",\"m\":false}");
                    } else if ("/kraken/ws/v2".equals(path)) {
                        ws.textMessageHandler(msg -> {
                            if (msg.contains("\"USDT/USD\"")) {
                                ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"USDT/USD\",\"side\":\"buy\",\"price\":1.0000,\"qty\":200.0,\"trade_id\":101}]}");
                            } else if (msg.contains("\"BTC/USD\"")) {
                                ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"side\":\"buy\",\"price\":85010.0,\"qty\":0.50,\"trade_id\":102}]}");
                            }
                        });
                    } else if ("/coinbase/ws".equals(path)) {
                        ws.textMessageHandler(msg -> {
                            if (msg.contains("\"matches\"") && msg.contains("\"BTC-USD\"")) {
                                ws.writeTextMessage("{\"type\":\"match\",\"trade_id\":201,\"side\":\"buy\",\"size\":\"0.60\",\"price\":\"85010.00\",\"product_id\":\"BTC-USD\"}");
                            }
                        });
                    }
                });
        mockServer.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        port = mockServer.actualPort();

        binanceConnector = new BinanceStreamConnector(
                vertx,
                "ws://localhost:" + port + "/binance/spot",
                "ws://localhost:" + port + "/binance/futures",
                1000L,
                10000L
        );
        krakenConnector = new KrakenStreamConnector(
                vertx,
                "ws://localhost:" + port + "/kraken/ws/v2",
                1000L,
                10000L
        );
        coinbaseConnector = new CoinbaseStreamConnector(
                vertx,
                "ws://localhost:" + port + "/coinbase/ws",
                1000L,
                10000L
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (binanceConnector != null) {
            binanceConnector.destroy();
        }
        if (krakenConnector != null) {
            krakenConnector.destroy();
        }
        if (coinbaseConnector != null) {
            coinbaseConnector.destroy();
        }
        if (mockServer != null) {
            mockServer.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private PaperTradingEngine createPaperEngine(String strategyName, long orderLatencyMs, boolean streamsEnabled) {
        return new PaperTradingEngine(
                registry,
                repository,
                binanceConnector,
                krakenConnector,
                coinbaseConnector,
                objectMapper,
                strategyName,
                "BTCUSDT",
                orderLatencyMs,
                streamsEnabled,
                vertx
        );
    }

    @Test
    void shouldResolveConfiguredStrategyAndDefaultLatency() {
        PaperTradingEngine engine = createPaperEngine("UtterParameciumStrategy", 50L, false);

        assertThat(engine.getActiveStrategyName()).isEqualTo("UtterParameciumStrategy");
        assertThat(engine.getOrderLatencyMs()).isEqualTo(50L);
        assertThat(engine.getActivePosition()).isNull();
        assertThat(engine.getStatus()).isEqualTo("RUNNING");
    }

    @Test
    void shouldExecuteBuyOrderWithConfiguredLatency() throws InterruptedException {
        PaperTradingEngine engine = createPaperEngine("UtterParameciumStrategy", 20L, false);

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
        PaperTradingEngine engine = createPaperEngine("UtterParameciumStrategy", 0L, false);

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
        PaperTradingEngine engine = createPaperEngine("UtterParameciumStrategy", 0L, false);

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
        PaperTradingEngine engine = createPaperEngine("utter_paramecium", 0L, false);

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
        PaperTradingEngine engine = createPaperEngine("UtterParameciumStrategy", 0L, false);

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
        PaperTradingEngine engine = createPaperEngine("utter_paramecium", 0L, false);

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
        PaperTradingEngine engine = createPaperEngine("utter_paramecium", 0L, false);

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

    @Test
    void shouldProcessBinanceKrakenAndCoinbaseSpotStreamsIntoCalculatedSpotTwapEngineAndUtterParameciumStrategy() {
        PaperTradingEngine engine = createPaperEngine("UtterParameciumStrategy", 0L, false);

        long candleStartSec = 1_785_542_400L;
        long candleStartMs = candleStartSec * 1_000L;
        long candleEndMs = (candleStartSec + 900L) * 1_000L;

        engine.onPolymarketCryptoTwap60s(candleStartMs, "btcusd", "chainlink", 90_000.00, 60, candleStartMs);

        // 1. Kraken USDT/USD peg update
        engine.onRawKrakenFrame(
                "USDTUSD",
                "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"USDT/USD\",\"side\":\"buy\",\"price\":1.0000,\"qty\":500.0}]}",
                candleEndMs - 61_000L
        );
        assertThat(engine.getUsdtUsdEma()).isCloseTo(1.0, within(1e-8));

        // 2. Warm up 15s of Binance, Coinbase, and Kraken spot trades + Chainlink ticks in settlement window
        long windowStartMs = candleEndMs - 60_000L;
        for (int s = 0; s <= 14; s++) {
            long ts = windowStartMs + s * 1_000L;
            double osc = (s % 2 == 0) ? 0.75 : -0.75;
            double spot = 89_995.00 + osc;
            engine.onRawBinanceFrame(
                    BinanceStreamType.SPOT_AGG_TRADES,
                    "{\"e\":\"aggTrade\",\"p\":\"" + spot + "\",\"q\":\"1.0\",\"m\":false}",
                    ts - 240L
            );
            engine.onRawCoinbaseFrame(
                    "BTCUSD",
                    "{\"type\":\"match\",\"price\":\"" + spot + "\",\"size\":\"1.0\"}",
                    ts - 220L
            );
            engine.onRawKrakenFrame(
                    "BTCUSD",
                    "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"price\":" + spot + ",\"qty\":1.0}]}",
                    ts - 200L
            );
            engine.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_997.00 + osc, ts);
        }

        long tau46Ms = candleEndMs - 46_000L;
        engine.onOrderBookQuote(new OrderBookQuote(
                0.46, 0.44, 0.72, 0.70, 100.0, 100.0, 0.46, 0.72, tau46Ms + 100L
        ));
        assertThat(engine.getOpenPositions()).isEmpty();

        // 3. Breakout across Binance, Coinbase, and Kraken before Chainlink updates -> triggers virtual paper fill!
        long breakoutTs = tau46Ms + 400L;
        engine.onRawBinanceFrame(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"e\":\"aggTrade\",\"p\":\"90018.00\",\"q\":\"2.0\",\"m\":false}",
                breakoutTs
        );
        engine.onRawCoinbaseFrame(
                "BTCUSD",
                "{\"type\":\"match\",\"price\":\"90018.00\",\"size\":\"2.0\"}",
                breakoutTs + 5L
        );
        engine.onRawKrakenFrame(
                "BTCUSD",
                "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"price\":90018.00,\"qty\":2.0}]}",
                breakoutTs + 10L
        );

        assertThat(engine.getLastCalculatedSpotPriceUsd()).isGreaterThan(90_010.0);
        assertThat(engine.getLastCalculatedTwap60sPriceUsd()).isGreaterThan(89_990.0);
        assertThat(engine.getOpenPositions()).hasSize(1);
        assertThat(engine.getOpenPositions().getFirst().side()).isEqualTo(TradeDirection.UP);
        assertThat(engine.getOpenPositions().getFirst().entryPrice()).isEqualTo(0.46);
    }

    @Test
    void shouldConnectAndProcessSpotWebSocketStreamsWhenStreamsEnabled() {
        PaperTradingEngine engine = createPaperEngine("UtterParameciumStrategy", 0L, true);
        try {
            engine.onStart(null);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        assertThat(engine.getLastCalculatedSpotPriceUsd()).isCloseTo(85_010.0, within(1e-2));
                        assertThat(engine.getLastCalculatedTwap60sPriceUsd()).isCloseTo(85_010.0, within(1e-2));
                        assertThat(engine.getUsdtUsdEma()).isCloseTo(1.0, within(1e-6));
                    });
        } finally {
            engine.onStop(null);
        }
    }
}
