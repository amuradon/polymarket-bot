package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.data.storage.CollectorWriteCoordinator;
import cz.polymarket.bot.data.storage.CollectorWriteMode;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyCoinbaseStorageManager;
import cz.polymarket.bot.data.storage.DailyKrakenStorageManager;
import cz.polymarket.bot.data.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.data.web.CollectorStatusDto;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DataCollectorService} verifying startup wiring, shutdown cleanup,
 * {@code readyForCutover} evaluation, and the {@code polymarket.collector.kraken.usdtusd-enabled} toggle.
 */
class DataCollectorServiceTest {

    @Test
    void shouldPassKrakenUsdtUsdDisabledFlagToKrakenConnectorOnStartup() {
        BinanceStreamConnector binanceConnector = mock(BinanceStreamConnector.class);
        DailyBinanceStorageManager binanceStorage = mock(DailyBinanceStorageManager.class);
        KrakenStreamConnector krakenConnector = mock(KrakenStreamConnector.class);
        DailyKrakenStorageManager krakenStorage = mock(DailyKrakenStorageManager.class);
        CoinbaseStreamConnector coinbaseConnector = mock(CoinbaseStreamConnector.class);
        DailyCoinbaseStorageManager coinbaseStorage = mock(DailyCoinbaseStorageManager.class);
        DailyPolyBoltStorageManager polyBoltDailyStorage = mock(DailyPolyBoltStorageManager.class);
        PolymarketLifecycleCoordinator polymarketCoordinator = mock(PolymarketLifecycleCoordinator.class);
        PolymarketMarketStorageManager polymarketStorage = mock(PolymarketMarketStorageManager.class);
        PolymarketPolyBoltClient polyBoltClient = mock(PolymarketPolyBoltClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);
        CollectorWriteCoordinator writeCoordinator = mock(CollectorWriteCoordinator.class);
        Vertx vertx = mock(Vertx.class);

        when(binanceConnector.getConnectionStates()).thenReturn(Map.of());
        when(krakenConnector.getConnectionStates()).thenReturn(Map.of("ETHUSD", ConnectionState.CONNECTED));
        when(coinbaseConnector.getState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getTwapState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getCryptoPriceState()).thenReturn(ConnectionState.CONNECTED);
        when(clobClient.getConnectionStates()).thenReturn(Map.of());
        when(polymarketCoordinator.getTrackedMarketSlugs()).thenReturn(List.of());
        when(writeCoordinator.getWriteMode()).thenReturn(CollectorWriteMode.ACTIVE);

        DataCollectorService service = new DataCollectorService(
                binanceConnector,
                binanceStorage,
                krakenConnector,
                krakenStorage,
                coinbaseConnector,
                coinbaseStorage,
                polyBoltDailyStorage,
                polymarketCoordinator,
                polymarketStorage,
                polyBoltClient,
                clobClient,
                writeCoordinator,
                vertx,
                "ETHUSDT",
                "15m",
                false,
                0
        );

        service.onStart(new StartupEvent());

        verify(krakenConnector).start(eq("ETHUSDT"), eq(false), any());
        verify(coinbaseConnector).start(eq("ETHUSDT"), any());

        CollectorStatusDto status = service.getStatus();
        assertThat(status.symbol()).isEqualTo("ETHUSDT");
        assertThat(status.krakenUsdtUsdEnabled()).isFalse();
        assertThat(status.krakenStreams()).containsExactlyEntriesOf(Map.of("ETHUSD", "CONNECTED"));
        assertThat(status.coinbaseStreamState()).isEqualTo("CONNECTED");
        assertThat(status.writeMode()).isEqualTo("ACTIVE");
        assertThat(status.readyForCutover()).isFalse();

        service.scheduledFlush();
        verify(writeCoordinator).flushAllIfActive();

        service.onStop(new ShutdownEvent());
        verify(krakenConnector).stop();
        verify(coinbaseConnector).stop();
        verify(writeCoordinator).closeAll();
    }

    @Test
    void shouldEvaluateReadyForCutoverCorrectlyAcrossAllStreamsAndBufferedRecords() {
        BinanceStreamConnector binanceConnector = mock(BinanceStreamConnector.class);
        DailyBinanceStorageManager binanceStorage = mock(DailyBinanceStorageManager.class);
        KrakenStreamConnector krakenConnector = mock(KrakenStreamConnector.class);
        DailyKrakenStorageManager krakenStorage = mock(DailyKrakenStorageManager.class);
        CoinbaseStreamConnector coinbaseConnector = mock(CoinbaseStreamConnector.class);
        DailyCoinbaseStorageManager coinbaseStorage = mock(DailyCoinbaseStorageManager.class);
        DailyPolyBoltStorageManager polyBoltDailyStorage = mock(DailyPolyBoltStorageManager.class);
        PolymarketLifecycleCoordinator polymarketCoordinator = mock(PolymarketLifecycleCoordinator.class);
        PolymarketMarketStorageManager polymarketStorage = mock(PolymarketMarketStorageManager.class);
        PolymarketPolyBoltClient polyBoltClient = mock(PolymarketPolyBoltClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);
        CollectorWriteCoordinator writeCoordinator = mock(CollectorWriteCoordinator.class);
        Vertx vertx = mock(Vertx.class);

        Map<BinanceStreamType, ConnectionState> allBinanceConnected = new EnumMap<>(BinanceStreamType.class);
        for (BinanceStreamType type : BinanceStreamType.values()) {
            allBinanceConnected.put(type, ConnectionState.CONNECTED);
        }

        when(binanceConnector.getConnectionStates()).thenReturn(allBinanceConnected);
        when(krakenConnector.getConnectionStates()).thenReturn(Map.of(
                "BTCUSD", ConnectionState.CONNECTED,
                "USDTUSD", ConnectionState.CONNECTED
        ));
        when(coinbaseConnector.getState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getTwapState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getCryptoPriceState()).thenReturn(ConnectionState.CONNECTED);
        when(polymarketCoordinator.getTrackedMarketSlugs()).thenReturn(List.of("btc-updown-15m-1794828600"));
        when(clobClient.getConnectionStates()).thenReturn(Map.of("btc-updown-15m-1794828600", ConnectionState.CONNECTED));
        when(writeCoordinator.getWriteMode()).thenReturn(CollectorWriteMode.STANDBY);
        when(writeCoordinator.getStandbyBufferedRecords()).thenReturn(0L);

        DataCollectorService service = new DataCollectorService(
                binanceConnector,
                binanceStorage,
                krakenConnector,
                krakenStorage,
                coinbaseConnector,
                coinbaseStorage,
                polyBoltDailyStorage,
                polymarketCoordinator,
                polymarketStorage,
                polyBoltClient,
                clobClient,
                writeCoordinator,
                vertx,
                "BTCUSDT",
                "15m",
                true,
                0
        );

        // 0 buffered/written records -> not ready yet
        assertThat(service.getStatus().readyForCutover()).isFalse();

        // Once standby buffer has received records and all streams are CONNECTED -> readyForCutover is true
        when(writeCoordinator.getStandbyBufferedRecords()).thenReturn(15L);
        CollectorStatusDto readyStatus = service.getStatus();
        assertThat(readyStatus.writeMode()).isEqualTo("STANDBY");
        assertThat(readyStatus.standbyBufferedRecords()).isEqualTo(15L);
        assertThat(readyStatus.readyForCutover()).isTrue();

        // If any stream is RECONNECTING -> readyForCutover is false
        when(coinbaseConnector.getState()).thenReturn(ConnectionState.RECONNECTING);
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
            KrakenStreamConnector krakenConnector = mock(KrakenStreamConnector.class);
            DailyKrakenStorageManager krakenStorage = mock(DailyKrakenStorageManager.class);
            CoinbaseStreamConnector coinbaseConnector = mock(CoinbaseStreamConnector.class);
            DailyCoinbaseStorageManager coinbaseStorage = mock(DailyCoinbaseStorageManager.class);
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
                    krakenConnector,
                    krakenStorage,
                    coinbaseConnector,
                    coinbaseStorage,
                    polyBoltDailyStorage,
                    polymarketCoordinator,
                    polymarketStorage,
                    polyBoltClient,
                    clobClient,
                    writeCoordinator,
                    vertx,
                    "BTCUSDT",
                    "15m",
                    true,
                    0
            );

            verify(binanceConnector).setDisconnectListener(listenerCaptor.capture());
            cz.polymarket.bot.connector.ws.WebSocketDisconnectListener disconnectListener = listenerCaptor.getValue();

            disconnectListener.onUnexpectedDisconnect(new cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry(
                    "BINANCE-SPOT_AGG_TRADES-BTCUSDT",
                    ConnectionState.RECONNECTING,
                    "stream.binance.com:9443",
                    0, 5, 0L, 2L, 10L, 50L, 100L, 0L, 1L, 1789041600000L, 1000, "24h rotation"
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
