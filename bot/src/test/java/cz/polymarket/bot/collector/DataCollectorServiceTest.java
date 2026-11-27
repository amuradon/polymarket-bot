package cz.polymarket.bot.collector;

import cz.polymarket.bot.collector.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.collector.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.collector.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.web.CollectorStatusDto;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DataCollectorService} verifying default Polymarket-only recording,
 * preserved inactive Binance {@code bookTicker} toggles, and health evaluation.
 */
class DataCollectorServiceTest {

    @Test
    void shouldStartOnlyPolymarketCollectorStreamsByDefaultAndEvaluateHealth() {
        BinanceStreamConnector binanceConnector = mock(BinanceStreamConnector.class);
        DailyBinanceStorageManager binanceStorage = mock(DailyBinanceStorageManager.class);
        DailyPolyBoltStorageManager polyBoltDailyStorage = mock(DailyPolyBoltStorageManager.class);
        PolymarketLifecycleCoordinator polymarketCoordinator = mock(PolymarketLifecycleCoordinator.class);
        PolymarketMarketStorageManager polymarketStorage = mock(PolymarketMarketStorageManager.class);
        PolymarketPolyBoltClient polyBoltClient = mock(PolymarketPolyBoltClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);

        long nowSec = Instant.now().getEpochSecond();
        String active15mSlug = PolymarketMarketInfo.buildMarketSlug(
                "BTCUSDT", "15m", PolymarketMarketInfo.alignCycleStart(nowSec, 900L));
        String active5mSlug = PolymarketMarketInfo.buildMarketSlug(
                "BTCUSDT", "5m", PolymarketMarketInfo.alignCycleStart(nowSec, 300L));

        when(polymarketCoordinator.getLookaheadCount()).thenReturn(1);
        when(polyBoltClient.getTwapState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getCryptoPriceState()).thenReturn(ConnectionState.CONNECTED);
        when(polymarketCoordinator.getTrackedMarketSlugs()).thenReturn(List.of(active15mSlug, active5mSlug));
        when(clobClient.getCollectorConnectionStates()).thenReturn(Map.of(
                active15mSlug, ConnectionState.CONNECTED,
                active5mSlug, ConnectionState.CONNECTED
        ));
        when(polyBoltDailyStorage.getTotalRecordsWritten()).thenReturn(0L);

        DataCollectorService service = new DataCollectorService(
                binanceConnector,
                binanceStorage,
                polyBoltDailyStorage,
                polymarketCoordinator,
                polymarketStorage,
                polyBoltClient,
                clobClient,
                "BTCUSDT",
                List.of("15m", "5m"),
                true,
                false,
                false
        );

        service.startStreams(nowSec);

        verify(binanceConnector, never()).startBookTickerStreams(any(), any(), any());
        verify(polyBoltClient).startCollectorStreams(eq("BTCUSDT"), any(), any());
        verify(polymarketCoordinator).start(eq(nowSec));

        // 0 records written -> unhealthy initially
        assertThat(service.getStatus().healthy()).isFalse();

        // Once Polymarket records are written -> healthy
        when(polyBoltDailyStorage.getTotalRecordsWritten()).thenReturn(5L);
        CollectorStatusDto status = service.getStatus();
        assertThat(status.healthy()).isTrue();
        assertThat(status.timeframes()).containsExactly("15m", "5m");
        assertThat(status.lookaheadCount()).isEqualTo(1);
        assertThat(status.binanceSpotBookTickerEnabled()).isFalse();
        assertThat(status.binanceFuturesBookTickerEnabled()).isFalse();
        assertThat(status.binanceStreams()).isEmpty();

        service.stopAndCloseAll();
        verify(binanceConnector).stopBookTickerStreams();
        verify(polymarketCoordinator).stop();
        verify(polyBoltClient).stopCollectorStreams();
        verify(binanceStorage).closeAll();
        verify(polyBoltDailyStorage).closeAll();
        verify(polymarketStorage).closeAll();
    }

    @Test
    void shouldStartEnabledBinanceBookTickerStreamsAndCheckTheirHealth() {
        BinanceStreamConnector binanceConnector = mock(BinanceStreamConnector.class);
        DailyBinanceStorageManager binanceStorage = mock(DailyBinanceStorageManager.class);
        DailyPolyBoltStorageManager polyBoltDailyStorage = mock(DailyPolyBoltStorageManager.class);
        PolymarketLifecycleCoordinator polymarketCoordinator = mock(PolymarketLifecycleCoordinator.class);
        PolymarketMarketStorageManager polymarketStorage = mock(PolymarketMarketStorageManager.class);
        PolymarketPolyBoltClient polyBoltClient = mock(PolymarketPolyBoltClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);

        long nowSec = Instant.now().getEpochSecond();
        String activeSlug = PolymarketMarketInfo.buildMarketSlug(
                "ETHUSDT", "15m", PolymarketMarketInfo.alignCycleStart(nowSec, 900L));

        Map<BinanceStreamType, ConnectionState> bookTickerStates = new EnumMap<>(BinanceStreamType.class);
        bookTickerStates.put(BinanceStreamType.SPOT_BOOK_TICKER, ConnectionState.CONNECTED);
        bookTickerStates.put(BinanceStreamType.FUTURES_BOOK_TICKER, ConnectionState.CONNECTED);

        when(binanceConnector.getBookTickerConnectionStates()).thenReturn(bookTickerStates);
        when(polyBoltClient.getTwapState()).thenReturn(ConnectionState.CONNECTED);
        when(polyBoltClient.getCryptoPriceState()).thenReturn(ConnectionState.CONNECTED);
        when(polymarketCoordinator.getTrackedMarketSlugs()).thenReturn(List.of(activeSlug));
        when(clobClient.getCollectorConnectionStates()).thenReturn(Map.of(activeSlug, ConnectionState.CONNECTED));
        when(binanceStorage.getTotalRecordsWritten()).thenReturn(4L);

        DataCollectorService service = new DataCollectorService(
                binanceConnector,
                binanceStorage,
                polyBoltDailyStorage,
                polymarketCoordinator,
                polymarketStorage,
                polyBoltClient,
                clobClient,
                "ETHUSDT",
                List.of("15m"),
                true,
                true,
                true
        );

        service.startStreams(nowSec);
        verify(binanceConnector).startBookTickerStreams(
                eq("ETHUSDT"),
                eq(EnumSet.of(BinanceStreamType.SPOT_BOOK_TICKER, BinanceStreamType.FUTURES_BOOK_TICKER)),
                any()
        );

        assertThat(service.getStatus().healthy()).isTrue();

        bookTickerStates.put(BinanceStreamType.FUTURES_BOOK_TICKER, ConnectionState.RECONNECTING);
        assertThat(service.getStatus().healthy()).isFalse();
    }
}
