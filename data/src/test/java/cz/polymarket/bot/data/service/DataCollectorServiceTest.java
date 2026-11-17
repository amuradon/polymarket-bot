package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.data.storage.CollectorWriteCoordinator;
import cz.polymarket.bot.data.storage.CollectorWriteMode;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.data.web.CollectorStatusDto;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DataCollectorService} verifying default Polymarket-only startup,
 * optional Binance Spot/Futures {@code bookTicker} toggles, shutdown cleanup, and {@code readyForCutover} evaluation
 * (including Blue-Green cutover from an older full-stream slot to a Polymarket-only slot).
 */
class DataCollectorServiceTest {

    @Test
    void shouldStartOnlyPolymarketStreamsAndReportReadyForCutoverWhenBookTickersDisabled() {
        BinanceStreamConnector binanceConnector = mock(BinanceStreamConnector.class);
        DailyBinanceStorageManager binanceStorage = mock(DailyBinanceStorageManager.class);
        DailyPolyBoltStorageManager polyBoltDailyStorage = mock(DailyPolyBoltStorageManager.class);
        PolymarketLifecycleCoordinator polymarketCoordinator = mock(PolymarketLifecycleCoordinator.class);
        PolymarketMarketStorageManager polymarketStorage = mock(PolymarketMarketStorageManager.class);
        PolymarketPolyBoltClient polyBoltClient = mock(PolymarketPolyBoltClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);
        CollectorWriteCoordinator writeCoordinator = mock(CollectorWriteCoordinator.class);
        Vertx vertx = mock(Vertx.class);

        when(polyBoltClient.getTwapState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getCryptoPriceState()).thenReturn(ConnectionState.CONNECTED);
        when(polymarketCoordinator.getTrackedMarketSlugs()).thenReturn(List.of("btc-updown-15m-1794828600"));
        when(clobClient.getConnectionStates()).thenReturn(Map.of("btc-updown-15m-1794828600", ConnectionState.CONNECTED));
        when(writeCoordinator.getWriteMode()).thenReturn(CollectorWriteMode.STANDBY);
        when(writeCoordinator.getStandbyBufferedRecords()).thenReturn(0L);

        DataCollectorService service = new DataCollectorService(
                binanceConnector,
                binanceStorage,
                polyBoltDailyStorage,
                polymarketCoordinator,
                polymarketStorage,
                polyBoltClient,
                clobClient,
                writeCoordinator,
                vertx,
                "BTCUSDT",
                "15m",
                false,
                false,
                0
        );

        service.onStart(new StartupEvent());

        verify(binanceConnector, never()).start(any(), any());
        verify(binanceConnector, never()).start(any(), any(), any(), any());
        verify(polyBoltClient).startCryptoPriceStream(eq("BTCUSDT"), any());
        verify(polyBoltClient).startTwapStream(eq("BTCUSDT"), any());
        verify(polymarketCoordinator).start(any(Long.class));

        // 0 buffered/written records -> not ready yet
        assertThat(service.getStatus().readyForCutover()).isFalse();

        // Once standby buffer receives Polymarket records -> readyForCutover is true even with Binance DISCONNECTED
        when(writeCoordinator.getStandbyBufferedRecords()).thenReturn(5L);
        when(binanceConnector.getConnectionStates()).thenReturn(Map.of());

        CollectorStatusDto status = service.getStatus();
        assertThat(status.symbol()).isEqualTo("BTCUSDT");
        assertThat(status.binanceSpotBookTickerEnabled()).isFalse();
        assertThat(status.binanceFuturesBookTickerEnabled()).isFalse();
        assertThat(status.binanceStreams()).isEmpty();
        assertThat(status.writeMode()).isEqualTo("STANDBY");
        assertThat(status.readyForCutover()).isTrue();

        // If PolyBolt disconnects -> readyForCutover becomes false
        when(polyBoltClient.getTwapState()).thenReturn(ConnectionState.RECONNECTING);
        assertThat(service.getStatus().readyForCutover()).isFalse();

        service.scheduledFlush();
        verify(writeCoordinator).flushAllIfActive();

        service.onStop(new ShutdownEvent());
        verify(binanceConnector).stop();
        verify(polymarketCoordinator).stop();
        verify(polyBoltClient).stop();
        verify(writeCoordinator).closeAll();
    }

    @Test
    void shouldStartOnlyEnabledBinanceBookTickerStreamsAndCheckThemForReadyForCutover() {
        BinanceStreamConnector binanceConnector = mock(BinanceStreamConnector.class);
        DailyBinanceStorageManager binanceStorage = mock(DailyBinanceStorageManager.class);
        DailyPolyBoltStorageManager polyBoltDailyStorage = mock(DailyPolyBoltStorageManager.class);
        PolymarketLifecycleCoordinator polymarketCoordinator = mock(PolymarketLifecycleCoordinator.class);
        PolymarketMarketStorageManager polymarketStorage = mock(PolymarketMarketStorageManager.class);
        PolymarketPolyBoltClient polyBoltClient = mock(PolymarketPolyBoltClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);
        CollectorWriteCoordinator writeCoordinator = mock(CollectorWriteCoordinator.class);
        Vertx vertx = mock(Vertx.class);

        Map<BinanceStreamType, ConnectionState> bookTickerStates = new EnumMap<>(BinanceStreamType.class);
        bookTickerStates.put(BinanceStreamType.SPOT_BOOK_TICKER, ConnectionState.CONNECTED);
        bookTickerStates.put(BinanceStreamType.FUTURES_BOOK_TICKER, ConnectionState.CONNECTED);

        when(binanceConnector.getConnectionStates()).thenReturn(bookTickerStates);
        when(polyBoltClient.getTwapState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getCryptoPriceState()).thenReturn(ConnectionState.CONNECTED);
        when(polymarketCoordinator.getTrackedMarketSlugs()).thenReturn(List.of("btc-updown-15m-1794828600"));
        when(clobClient.getConnectionStates()).thenReturn(Map.of("btc-updown-15m-1794828600", ConnectionState.CONNECTED));
        when(writeCoordinator.getWriteMode()).thenReturn(CollectorWriteMode.STANDBY);
        when(writeCoordinator.getStandbyBufferedRecords()).thenReturn(10L);

        DataCollectorService service = new DataCollectorService(
                binanceConnector,
                binanceStorage,
                polyBoltDailyStorage,
                polymarketCoordinator,
                polymarketStorage,
                polyBoltClient,
                clobClient,
                writeCoordinator,
                vertx,
                "ETHUSDT",
                "15m",
                true,
                true,
                0
        );

        service.onStart(new StartupEvent());

        verify(binanceConnector).start(
                eq("ETHUSDT"),
                eq(EnumSet.of(BinanceStreamType.SPOT_BOOK_TICKER, BinanceStreamType.FUTURES_BOOK_TICKER)),
                any(),
                isNull()
        );

        CollectorStatusDto readyStatus = service.getStatus();
        assertThat(readyStatus.binanceSpotBookTickerEnabled()).isTrue();
        assertThat(readyStatus.binanceFuturesBookTickerEnabled()).isTrue();
        assertThat(readyStatus.binanceStreams()).containsOnlyKeys("SPOT_BOOK_TICKER", "FUTURES_BOOK_TICKER");
        assertThat(readyStatus.readyForCutover()).isTrue();

        // If an enabled bookTicker stream drops -> readyForCutover becomes false
        bookTickerStates.put(BinanceStreamType.FUTURES_BOOK_TICKER, ConnectionState.RECONNECTING);
        assertThat(service.getStatus().readyForCutover()).isFalse();
    }

    @Test
    void shouldLogWarnForAllUnexpectedStreamDisconnectsInDataCollector() {
        List<java.util.logging.LogRecord> capturedLogs = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.logging.Logger serviceLogger = java.util.logging.Logger.getLogger(DataCollectorService.class.getName());
        java.util.logging.Handler testHandler = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord record) {
                if (record != null) {
                    capturedLogs.add(record);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        serviceLogger.addHandler(testHandler);

        try {
            BinanceStreamConnector binanceConnector = mock(BinanceStreamConnector.class);
            DailyBinanceStorageManager binanceStorage = mock(DailyBinanceStorageManager.class);
            DailyPolyBoltStorageManager polyBoltDailyStorage = mock(DailyPolyBoltStorageManager.class);
            PolymarketLifecycleCoordinator polymarketCoordinator = mock(PolymarketLifecycleCoordinator.class);
            PolymarketMarketStorageManager polymarketStorage = mock(PolymarketMarketStorageManager.class);
            PolymarketPolyBoltClient polyBoltClient = mock(PolymarketPolyBoltClient.class);
            PolymarketClobClient clobClient = mock(PolymarketClobClient.class);
            CollectorWriteCoordinator writeCoordinator = mock(CollectorWriteCoordinator.class);
            Vertx vertx = mock(Vertx.class);

            org.mockito.ArgumentCaptor<cz.polymarket.bot.connector.ws.WebSocketDisconnectListener> listenerCaptor =
                    org.mockito.ArgumentCaptor.forClass(cz.polymarket.bot.connector.ws.WebSocketDisconnectListener.class);

            new DataCollectorService(
                    binanceConnector,
                    binanceStorage,
                    polyBoltDailyStorage,
                    polymarketCoordinator,
                    polymarketStorage,
                    polyBoltClient,
                    clobClient,
                    writeCoordinator,
                    vertx,
                    "BTCUSDT",
                    "15m",
                    false,
                    false,
                    0
            );

            verify(polyBoltClient).setDisconnectListener(listenerCaptor.capture());
            cz.polymarket.bot.connector.ws.WebSocketDisconnectListener disconnectListener = listenerCaptor.getValue();

            disconnectListener.onUnexpectedDisconnect(new cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry(
                    "POLYMARKET-POLYBOLT-CRYPTO",
                    ConnectionState.RECONNECTING,
                    "ws-live-v2.polymarket.com:443",
                    0, 5, 0L, 2L, 10L, 50L, 100L, 0L, 1L, 1789041600000L, 1006, "network drop"
            ));
            disconnectListener.onUnexpectedDisconnect(new cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry(
                    "POLYMARKET-POLYBOLT-TWAP",
                    ConnectionState.RECONNECTING,
                    "ws-live-v2.polymarket.com:443",
                    0, 5, 0L, 2L, 10L, 50L, 100L, 0L, 1L, 1789041600000L, 1013, "slow consumer"
            ));

            assertThat(capturedLogs)
                    .noneMatch(r -> r.getLevel().intValue() >= java.util.logging.Level.SEVERE.intValue());
            assertThat(capturedLogs)
                    .filteredOn(r -> r.getLevel().equals(java.util.logging.Level.WARNING))
                    .hasSize(2);
        } finally {
            serviceLogger.removeHandler(testHandler);
        }
    }
}
