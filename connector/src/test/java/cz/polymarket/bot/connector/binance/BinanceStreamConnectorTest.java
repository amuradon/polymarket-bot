package cz.polymarket.bot.connector.binance;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.StreamRedundancyStatus;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BinanceStreamType} and {@link BinanceStreamConnector} verifying strict isolation
 * between single-leg collector {@code bookTicker} streams and dual Active-Active strategy
 * {@code SPOT_AGG_TRADES} streams, zero duplication, fast-forward filtering, and clean {@code OFF} mode stop.
 */
class BinanceStreamConnectorTest {

    private Vertx vertx;
    private MarketWebSocketRuntime marketWsRuntime;
    private HttpServer server;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        marketWsRuntime = new MarketWebSocketRuntime(30L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (marketWsRuntime != null) {
            marketWsRuntime.destroy();
        }
        if (server != null) {
            server.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldBuildExpectedStreamUrlsForSymbol() {
        String spotBase = "wss://stream.binance.com:9443/ws";
        String futuresBase = "wss://fstream.binance.com";

        assertThat(BinanceStreamType.SPOT_AGG_TRADES.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://stream.binance.com:9443/ws/btcusdt@aggTrade");
        assertThat(BinanceStreamType.SPOT_BOOK_TICKER.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://stream.binance.com:9443/ws/btcusdt@bookTicker");
        assertThat(BinanceStreamType.FUTURES_AGG_TRADES.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://fstream.binance.com/market/ws/btcusdt@aggTrade");
        assertThat(BinanceStreamType.FUTURES_BOOK_TICKER.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://fstream.binance.com/public/ws/btcusdt@bookTicker");
    }

    @Test
    void shouldIsolateBookTickerCollectorStreamsFromDualSpotAggTradesStrategyStreamsWithoutDuplication() throws Exception {
        List<ServerWebSocket> aggTradeSockets = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    String path = ws.path();
                    if (path.endsWith("@aggTrade")) {
                        aggTradeSockets.add(ws);
                        ws.writeTextMessage("{\"e\":\"aggTrade\",\"a\":5001,\"p\":\"85000.00\",\"q\":\"0.10\"}");
                    } else if (path.endsWith("@bookTicker")) {
                        ws.writeTextMessage("{\"u\":9001,\"s\":\"BTCUSDT\",\"b\":\"84999.00\",\"B\":\"1.0\",\"a\":\"85001.00\",\"A\":\"1.0\",\"path\":\"" + path + "\"}");
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        BinanceStreamConnector connector = new BinanceStreamConnector(
                marketWsRuntime,
                "ws://127.0.0.1:" + port + "/spot",
                "ws://127.0.0.1:" + port + "/futures",
                200L,
                0L
        );

        Map<BinanceStreamType, List<String>> collectorFrames = new ConcurrentHashMap<>();
        List<Long> strategyAggTradeIds = new CopyOnWriteArrayList<>();

        // Reject non-bookTicker stream types in startBookTickerStreams
        assertThatThrownBy(() -> connector.startBookTickerStreams(
                "BTCUSDT",
                EnumSet.of(BinanceStreamType.SPOT_AGG_TRADES),
                (type, rawJson, ts) -> {}
        )).isInstanceOf(IllegalArgumentException.class);

        // Start both bookTicker collector streams AND dual SPOT_AGG_TRADES strategy streams simultaneously
        connector.startBookTickerStreams(
                "BTCUSDT",
                EnumSet.of(BinanceStreamType.SPOT_BOOK_TICKER, BinanceStreamType.FUTURES_BOOK_TICKER),
                new BinanceStreamListener() {
                    @Override
                    public void onStreamMessage(BinanceStreamType streamType, String rawJson, long recvTimestampMs) {
                        throw new AssertionError("Expected zero-decode byte[] callback");
                    }

                    @Override
                    public void onStreamFrame(BinanceStreamType streamType, byte[] rawUtf8Bytes, long recvTimestampMs) {
                        collectorFrames.computeIfAbsent(streamType, k -> new CopyOnWriteArrayList<>())
                                .add(new String(rawUtf8Bytes, StandardCharsets.UTF_8));
                    }
                }
        );

        Thread dummyStrategyWorker = Thread.ofPlatform().daemon(true).name("dummy-binance-strat").unstarted(() -> {});
        connector.startSpotAggTradesBatch("BTCUSDT", batch -> {
            for (TimestampedFrame frame : batch) {
                strategyAggTradeIds.add(BinanceStreamConnector.extractAggTradeId(frame.rawUtf8Bytes()));
            }
        }, dummyStrategyWorker);

        List<TimestampedFrame> scratch = new ArrayList<>(16);
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(aggTradeSockets).hasSize(2);
                    connector.drainStreams(scratch);
                    assertThat(strategyAggTradeIds).containsExactly(5001L);
                    assertThat(collectorFrames.get(BinanceStreamType.SPOT_BOOK_TICKER)).hasSize(1);
                    assertThat(collectorFrames.get(BinanceStreamType.FUTURES_BOOK_TICKER)).hasSize(1);
                    assertThat(collectorFrames).doesNotContainKey(BinanceStreamType.SPOT_AGG_TRADES);
                    assertThat(connector.getBookTickerConnectionStates())
                            .containsEntry(BinanceStreamType.SPOT_BOOK_TICKER, ConnectionState.CONNECTED)
                            .containsEntry(BinanceStreamType.FUTURES_BOOK_TICKER, ConnectionState.CONNECTED);
                    assertThat(connector.getSpotAggTradesState()).isEqualTo(ConnectionState.CONNECTED);
                });

        // Stop strategy streams (mode OFF) and verify bookTicker collector streams stay CONNECTED
        connector.stopSpotAggTrades();
        assertThat(connector.getSpotAggTradesState()).isEqualTo(ConnectionState.DISCONNECTED);
        assertThat(connector.hasQueuedFrames()).isFalse();
        assertThat(connector.getBookTickerConnectionStates())
                .containsEntry(BinanceStreamType.SPOT_BOOK_TICKER, ConnectionState.CONNECTED)
                .containsEntry(BinanceStreamType.FUTURES_BOOK_TICKER, ConnectionState.CONNECTED);

        connector.destroy();
    }

    @Test
    void shouldFastForwardDualSpotAggTradesLegsAndDropDelayedOutOfOrderRetransmissions() throws Exception {
        List<ServerWebSocket> aggTradeSockets = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    if ("/spot/btcusdt@aggTrade".equals(ws.path())) {
                        aggTradeSockets.add(ws);
                        if (aggTradeSockets.size() == 2) {
                            ServerWebSocket fastLeg = aggTradeSockets.get(0);
                            ServerWebSocket slowRetransmittingLeg = aggTradeSockets.get(1);
                            fastLeg.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1000,\"s\":\"BTCUSDT\",\"a\":1002,\"p\":\"85020.00\",\"q\":\"0.50\",\"m\":false}");
                            vertx.setTimer(30L, id -> {
                                slowRetransmittingLeg.writeTextMessage("{\"e\":\"aggTrade\",\"E\":999,\"s\":\"BTCUSDT\",\"a\":1001,\"p\":\"85000.00\",\"q\":\"0.25\",\"m\":true}");
                                slowRetransmittingLeg.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1000,\"s\":\"BTCUSDT\",\"a\":1002,\"p\":\"85020.00\",\"q\":\"0.50\",\"m\":false}");
                                slowRetransmittingLeg.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1005,\"s\":\"BTCUSDT\",\"a\":1003,\"p\":\"85035.00\",\"q\":\"0.75\",\"m\":false}");
                            });
                        }
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String spotWsBase = "ws://127.0.0.1:" + port + "/spot";
        String futuresWsBase = "ws://127.0.0.1:" + port + "/futures";

        BinanceStreamConnector connector = new BinanceStreamConnector(
                marketWsRuntime,
                spotWsBase,
                futuresWsBase,
                200L,
                5000L
        );

        List<Long> deliveredAggTradeIds = new CopyOnWriteArrayList<>();
        connector.startSpotAggTradesBatch("BTCUSDT", batch -> {
            for (TimestampedFrame frame : batch) {
                deliveredAggTradeIds.add(BinanceStreamConnector.extractAggTradeId(frame.rawUtf8Bytes()));
            }
        }, null);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(aggTradeSockets).hasSize(2);
                    assertThat(deliveredAggTradeIds).containsExactly(1002L, 1003L);
                    assertThat(connector.getSpotAggTradesState()).isEqualTo(ConnectionState.CONNECTED);
                    assertThat(connector.getStreamTelemetries()).hasSize(2);
                });

        long coalescedTotal = connector.getStreamTelemetries().stream()
                .mapToLong(WebSocketStreamTelemetry::coalescedFramesCount)
                .sum();
        assertThat(coalescedTotal).isGreaterThanOrEqualTo(2L);

        StreamRedundancyStatus redundancy =
                connector.describeStreamRedundancy("BINANCE-SPOT_AGG_TRADES-BTCUSDT");
        assertThat(redundancy.dualStream()).isTrue();
        assertThat(redundancy.peerStreamName()).isEqualTo("BINANCE-SPOT_AGG_TRADES-BTCUSDT-BACKUP");
        assertThat(redundancy.failoverActive()).isTrue();

        connector.destroy();
    }
}
