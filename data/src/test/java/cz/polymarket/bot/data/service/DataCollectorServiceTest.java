package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
import cz.polymarket.bot.connector.deribit.DeribitStreamConnector;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyCoinbaseStorageManager;
import cz.polymarket.bot.data.storage.DailyDeribitStorageManager;
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
        DeribitStreamConnector deribitConnector = mock(DeribitStreamConnector.class);
        DailyDeribitStorageManager deribitStorage = mock(DailyDeribitStorageManager.class);
        DailyPolyBoltStorageManager polyBoltDailyStorage = mock(DailyPolyBoltStorageManager.class);
        PolymarketLifecycleCoordinator polymarketCoordinator = mock(PolymarketLifecycleCoordinator.class);
        PolymarketMarketStorageManager polymarketStorage = mock(PolymarketMarketStorageManager.class);
        PolymarketPolyBoltClient polyBoltClient = mock(PolymarketPolyBoltClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);

        when(binanceConnector.getConnectionStates()).thenReturn(Map.of());
        when(krakenConnector.getConnectionStates()).thenReturn(Map.of("ETHUSD", ConnectionState.CONNECTED));
        when(coinbaseConnector.getState()).thenReturn(ConnectionState.CONNECTED);
        when(deribitConnector.getState()).thenReturn(ConnectionState.CONNECTED);
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
                deribitConnector,
                deribitStorage,
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
}
