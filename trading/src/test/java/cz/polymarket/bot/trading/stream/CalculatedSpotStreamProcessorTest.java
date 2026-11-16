package cz.polymarket.bot.trading.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.calculator.CalculatedSpotTwapEngine;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.TradingStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategyConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link CalculatedSpotStreamProcessor} verifying zero-allocation parsing of
 * Binance Spot {@code aggTrades}, Kraken Spot {@code trade} updates ({@code USDTUSD} + {@code <ASSET>USD}),
 * and Coinbase Spot {@code matches} frames into {@link CalculatedSpotTwapEngine} and {@link UtterParameciumStrategy}.
 */
class CalculatedSpotStreamProcessorTest {

    private ObjectMapper objectMapper;
    private CalculatedSpotTwapEngine twapEngine;
    private CalculatedSpotStreamProcessor processor;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        twapEngine = new CalculatedSpotTwapEngine();
        processor = new CalculatedSpotStreamProcessor(objectMapper, twapEngine, "BTCUSDT");
    }

    @Test
    @DisplayName("Should validate constructor arguments")
    void shouldValidateConstructorArguments() {
        assertThatThrownBy(() -> new CalculatedSpotStreamProcessor(null, twapEngine, "BTCUSDT"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CalculatedSpotStreamProcessor(objectMapper, null, "BTCUSDT"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should process Kraken USDT/USD peg updates and aggregate Binance, Coinbase, and Kraken spot trades")
    void shouldProcessKrakenUsdtPegAndAggregateThreeVenueSpotTrades() {
        List<double[]> recordedSpotTwap = new ArrayList<>();
        List<String> recordedSymbols = new ArrayList<>();

        TradingStrategy recordingStrategy = new TradingStrategy() {
            @Override
            public void init(StrategyContext context) {
            }

            @Override
            public void onExecutionReport(ExecutionReport report) {
            }

            @Override
            public void onCalculatedSpotTwap(long timestampMs, String symbol, double spotPrice, double twap60sPrice) {
                recordedSymbols.add(symbol);
                recordedSpotTwap.add(new double[]{timestampMs, spotPrice, twap60sPrice});
            }
        };

        // 1. Kraken USDT/USD update initializes peg to 0.9990 without firing onCalculatedSpotTwap
        byte[] krakenUsdtBytes = """
                {"channel":"trade","type":"update","data":[{"symbol":"USDT/USD","side":"sell","price":0.9990,"qty":500.0,"trade_id":101}]}
                """.getBytes(StandardCharsets.UTF_8);
        processor.processKrakenFrameBytes("USDTUSD", krakenUsdtBytes, 1_000L, recordingStrategy);
        assertThat(twapEngine.getUsdtUsdEma()).isCloseTo(0.9990, within(1e-8));
        assertThat(recordedSpotTwap).isEmpty();

        // 2. Non-SPOT_AGG_TRADES Binance stream types must be ignored
        byte[] bookTickerBytes = """
                {"u":400900217,"s":"BTCUSDT","b":"70000.00","B":"1.25","a":"70000.10","A":"0.80"}
                """.getBytes(StandardCharsets.UTF_8);
        processor.processBinanceFrameBytes(BinanceStreamType.SPOT_BOOK_TICKER, bookTickerBytes, 1_000L, recordingStrategy);
        assertThat(recordedSpotTwap).isEmpty();

        // 3. Binance SPOT_AGG_TRADES at 70,070.0701 USDT -> converted via 0.9990 EMA = $70,000.00 USD
        byte[] binanceAggTradeBytes = """
                {"e":"aggTrade","E":1000,"s":"BTCUSDT","a":1001,"p":"70070.07007007","q":"1.00000000","m":true}
                """.getBytes(StandardCharsets.UTF_8);
        processor.processBinanceFrameBytes(
                BinanceStreamType.SPOT_AGG_TRADES, binanceAggTradeBytes, 1_000L, recordingStrategy
        );
        assertThat(recordedSpotTwap).hasSize(1);
        assertThat(recordedSymbols).containsExactly("btcusd");
        assertThat(recordedSpotTwap.getFirst()[1]).isCloseTo(70_000.0, within(1e-2));

        // 4. Coinbase match at $70,020.00 USD (qty = 1.0)
        byte[] coinbaseMatchBytes = """
                {"type":"match","trade_id":1120371409,"side":"buy","size":"1.00000000","price":"70020.00","product_id":"BTC-USD"}
                """.getBytes(StandardCharsets.UTF_8);
        processor.processCoinbaseFrameBytes("BTCUSD", coinbaseMatchBytes, 1_000L, recordingStrategy);
        assertThat(recordedSpotTwap).hasSize(2);

        // 5. Kraken BTC/USD update at $70,040.00 USD (qty = 1.0) -> 3-venue blend: 0.415*70000 + 0.345*70020 + 0.240*70040
        byte[] krakenBtcBytes = """
                {"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":70040.0,"qty":1.0,"trade_id":202}]}
                """.getBytes(StandardCharsets.UTF_8);
        processor.processKrakenFrameBytes("BTCUSD", krakenBtcBytes, 1_000L, recordingStrategy);
        assertThat(recordedSpotTwap).hasSize(3);

        double expectedBlendedSpot = 0.415 * 70_000.0 + 0.345 * 70_020.0 + 0.240 * 70_040.0;
        assertThat(recordedSpotTwap.getLast()[1]).isCloseTo(expectedBlendedSpot, within(1e-2));
        assertThat(twapEngine.getLastSpotPriceUsd()).isCloseTo(expectedBlendedSpot, within(1e-2));
    }

    @Test
    @DisplayName("Should ignore Kraken snapshots/heartbeats and Coinbase last_match/subscriptions frames")
    void shouldIgnoreControlAndSnapshotFrames() {
        List<Double> spots = new ArrayList<>();
        TradingStrategy strategy = new TradingStrategy() {
            @Override
            public void init(StrategyContext context) {
            }

            @Override
            public void onExecutionReport(ExecutionReport report) {
            }

            @Override
            public void onCalculatedSpotTwap(long timestampMs, String symbol, double spotPrice, double twap60sPrice) {
                spots.add(spotPrice);
            }
        };

        processor.processKrakenFrame(
                "BTCUSD",
                "{\"channel\":\"trade\",\"type\":\"snapshot\",\"data\":[{\"symbol\":\"BTC/USD\",\"price\":70000.0,\"qty\":1.0}]}",
                1_000L,
                strategy
        );

        processor.processKrakenFrame(
                "BTCUSD",
                "{\"channel\":\"heartbeat\"}",
                1_000L,
                strategy
        );

        processor.processCoinbaseFrame(
                "BTCUSD",
                "{\"type\":\"last_match\",\"price\":\"70000.0\",\"size\":\"1.0\"}",
                1_000L,
                strategy
        );

        assertThat(spots).isEmpty();
    }

    @Test
    @DisplayName("Should drive UtterParameciumStrategy de-biased spot breakout order from Binance, Kraken, and Coinbase streams")
    void shouldDriveUtterParameciumStrategyBreakoutFromThreeSpotStreams() {
        UtterParameciumStrategy strategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        List<OrderCommand> submittedOrders = new ArrayList<>();
        strategy.init(new StrategyContext() {
            @Override
            public ExecutionRouter getExecutionRouter() {
                return new ExecutionRouter() {
                    @Override
                    public void submitOrder(OrderCommand command) {
                        submittedOrders.add(command);
                    }

                    @Override
                    public void cancelOrder(String clientOrderId) {
                    }
                };
            }
        });

        long candleStartMs = 1_785_542_400_000L;
        long candleEndMs = candleStartMs + 900_000L;
        strategy.onPolymarketCryptoTwap60s(candleStartMs, "btcusd", "chainlink", 90_000.00, 60, candleStartMs);

        // Initialize Kraken USDT/USD peg at 1.0000
        processor.processKrakenFrame(
                "USDTUSD",
                "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"USDT/USD\",\"price\":1.0000,\"qty\":500.0}]}",
                candleEndMs - 61_000L,
                strategy
        );

        // Warm up 15 seconds of spot and Chainlink oscillations around $89,996-$89,997
        long windowStartMs = candleEndMs - 60_000L;
        for (int s = 0; s <= 14; s++) {
            long ts = windowStartMs + s * 1_000L;
            double osc = (s % 2 == 0) ? 0.75 : -0.75;
            double spotVal = 89_995.00 + osc;
            processor.processBinanceFrame(
                    BinanceStreamType.SPOT_AGG_TRADES,
                    "{\"e\":\"aggTrade\",\"p\":\"" + spotVal + "\",\"q\":\"1.0\",\"m\":false}",
                    ts - 250L,
                    strategy
            );
            processor.processCoinbaseFrame(
                    "BTCUSD",
                    "{\"type\":\"match\",\"price\":\"" + spotVal + "\",\"size\":\"1.0\"}",
                    ts - 220L,
                    strategy
            );
            processor.processKrakenFrame(
                    "BTCUSD",
                    "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"price\":" + spotVal + ",\"qty\":1.0}]}",
                    ts - 200L,
                    strategy
            );
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_997.00 + osc, ts);
        }

        long tau46Ms = candleEndMs - 46_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.46, 0.44, 0.72, 0.70, 100.0, 100.0, 0.46, 0.72, tau46Ms + 100L
        ));
        assertThat(submittedOrders).isEmpty();

        // Multi-exchange spot surges to $90,018.00 across Binance, Coinbase, and Kraken before Chainlink updates!
        long breakoutTs = tau46Ms + 400L;
        processor.processBinanceFrame(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"e\":\"aggTrade\",\"p\":\"90018.00\",\"q\":\"2.5\",\"m\":false}",
                breakoutTs,
                strategy
        );
        processor.processCoinbaseFrame(
                "BTCUSD",
                "{\"type\":\"match\",\"price\":\"90018.50\",\"size\":\"1.8\"}",
                breakoutTs + 10L,
                strategy
        );
        processor.processKrakenFrame(
                "BTCUSD",
                "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"price\":90018.25,\"qty\":1.2}]}",
                breakoutTs + 20L,
                strategy
        );

        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().token()).isEqualTo("BTC-UP");
        assertThat(submittedOrders.getFirst().side()).isEqualTo("BUY");
    }
}
