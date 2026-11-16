package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyCoinbaseStorageManager;
import cz.polymarket.bot.data.storage.DailyKrakenStorageManager;
import cz.polymarket.bot.data.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.data.web.CollectorStatusDto;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.junit.jupiter.api.Test;

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
 * and the {@code polymarket.collector.kraken.usdtusd-enabled} toggle.
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

        when(binanceConnector.getConnectionStates()).thenReturn(Map.of());
        when(krakenConnector.getConnectionStates()).thenReturn(Map.of("ETHUSD", ConnectionState.CONNECTED));
        when(coinbaseConnector.getState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getTwapState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getCryptoPriceState()).thenReturn(ConnectionState.CONNECTED);
        when(clobClient.getConnectionStates()).thenReturn(Map.of());
        when(polymarketCoordinator.getTrackedMarketSlugs()).thenReturn(List.of());

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
                "ETHUSDT",
                "15m",
                false
        );

        service.onStart(new StartupEvent());

        verify(krakenConnector).start(eq("ETHUSDT"), eq(false), any());
        verify(coinbaseConnector).start(eq("ETHUSDT"), any());

        CollectorStatusDto status = service.getStatus();
        assertThat(status.symbol()).isEqualTo("ETHUSDT");
        assertThat(status.krakenUsdtUsdEnabled()).isFalse();
        assertThat(status.krakenStreams()).containsExactlyEntriesOf(Map.of("ETHUSD", "CONNECTED"));
        assertThat(status.coinbaseStreamState()).isEqualTo("CONNECTED");

        service.scheduledFlush();
        verify(krakenStorage).flushAll();
        verify(coinbaseStorage).flushAll();

        service.onStop(new ShutdownEvent());
        verify(krakenConnector).stop();
        verify(coinbaseConnector).stop();
        verify(krakenStorage).closeAll();
        verify(coinbaseStorage).closeAll();
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
                    "BTCUSDT",
                    "15m",
                    true
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
