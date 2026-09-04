package cz.polymarket.bot.service;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.TwapCalculator;
import cz.polymarket.bot.domain.CandleTwapState;
import cz.polymarket.bot.domain.PriceSnapshot;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.exchange.BinanceWebSocketClient;
import cz.polymarket.bot.exchange.CoinbaseWebSocketClient;
import cz.polymarket.bot.exchange.ExchangePriceTracker;
import cz.polymarket.bot.exchange.HistoricalDataReconstructor;
import cz.polymarket.bot.exchange.KrakenWebSocketClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TwapEngineTest {

    private ExchangePriceTracker priceTracker;
    private HistoricalDataReconstructor reconstructor;
    private HourlyPriceCache cache;
    private TwapCalculator twapCalculator;
    private TwapEngine engine;

    @BeforeEach
    void setUp() {
        priceTracker = mock(ExchangePriceTracker.class);
        reconstructor = mock(HistoricalDataReconstructor.class);
        cache = new HourlyPriceCache(3600);
        twapCalculator = new TwapCalculator();
        BinanceWebSocketClient binanceClient = mock(BinanceWebSocketClient.class);
        CoinbaseWebSocketClient coinbaseClient = mock(CoinbaseWebSocketClient.class);
        KrakenWebSocketClient krakenClient = mock(KrakenWebSocketClient.class);

        engine = new TwapEngine(priceTracker, reconstructor, cache, twapCalculator, binanceClient, coinbaseClient, krakenClient, "5m");
    }

    @Test
    void shouldInitializeFromReconstructor() {
        Instant now = Instant.parse("2026-09-03T14:02:00Z");
        List<TwapPoint> points = new ArrayList<>();
        points.add(new TwapPoint(1788444000L, new BigDecimal("78823.52"), new BigDecimal("78820.00")));

        CandleTwapState mockState = new CandleTwapState(
                Timeframe.FIVE_MINUTES,
                1788444000L,
                1788444300L,
                new BigDecimal("78823.52"),
                points
        );
        when(reconstructor.reconstructCandle(any(Timeframe.class), eq(now))).thenReturn(mockState);

        engine.initialize(now);

        CandleTwapState state = engine.getCurrentCandleState(Timeframe.FIVE_MINUTES);
        assertThat(state).isNotNull();
        assertThat(state.candleStart()).isEqualTo(1788444000L);
        assertThat(state.openPrice()).isEqualByComparingTo("78823.52");
        assertThat(state.points()).hasSize(1);
    }

    @Test
    void shouldProcessTickOnBinanceTimestamp() {
        Instant t0 = Instant.parse("2026-09-03T14:00:00Z");
        List<TwapPoint> initialPoints = new ArrayList<>();
        initialPoints.add(new TwapPoint(t0.getEpochSecond(), new BigDecimal("100.00"), new BigDecimal("100.00")));

        CandleTwapState mockState = new CandleTwapState(
                Timeframe.FIVE_MINUTES,
                t0.getEpochSecond(),
                t0.plusSeconds(300).getEpochSecond(),
                new BigDecimal("100.00"),
                initialPoints
        );
        when(reconstructor.reconstructCandle(any(Timeframe.class), eq(t0))).thenReturn(mockState);
        engine.initialize(t0);

        List<TwapUpdate> updates = new ArrayList<>();
        engine.registerListener(updates::add);

        // Binance tick at t0 + 1 second (14:00:01)
        Instant t1 = t0.plusSeconds(1);
        long t1Ms = t1.toEpochMilli();
        PriceSnapshot snap1 = new PriceSnapshot(t1, new BigDecimal("110.00"), new BigDecimal("110.00"), new BigDecimal("110.00"), new BigDecimal("110.00"));
        when(priceTracker.getSnapshot(t1)).thenReturn(Optional.of(snap1));
        when(priceTracker.getLast60SecondsMedians(t1.getEpochSecond())).thenReturn(List.of(new BigDecimal("100.00"), new BigDecimal("110.00")));

        engine.onBinanceTick(t1Ms);

        assertThat(updates).isNotEmpty();
        TwapUpdate u1 = updates.stream().filter(u -> u.timeframe() == Timeframe.FIVE_MINUTES).findFirst().orElseThrow();
        assertThat(u1.isNewCandle()).isFalse();
        assertThat(u1.point().time()).isEqualTo(t1.getEpochSecond());
        assertThat(u1.point().twap()).isEqualByComparingTo("105.00");
        assertThat(cache.get(t1.getEpochSecond())).isEqualByComparingTo("105.00");
    }

    @Test
    void shouldForwardFillMissingSecondsWhenNoTradesOccur() {
        Instant t0 = Instant.parse("2026-09-03T14:00:00Z");
        List<TwapPoint> initialPoints = new ArrayList<>();
        initialPoints.add(new TwapPoint(t0.getEpochSecond(), new BigDecimal("9.00"), new BigDecimal("9.00")));

        CandleTwapState mockState = new CandleTwapState(
                Timeframe.FIVE_MINUTES,
                t0.getEpochSecond(),
                t0.plusSeconds(300).getEpochSecond(),
                new BigDecimal("9.00"),
                initialPoints
        );
        when(reconstructor.reconstructCandle(any(Timeframe.class), eq(t0))).thenReturn(mockState);
        engine.initialize(t0);

        List<TwapUpdate> updates = new ArrayList<>();
        engine.registerListener(updates::add);

        // For gap seconds t0 + 1 and t0 + 2:
        when(priceTracker.getLast60SecondsMedians(t0.getEpochSecond() + 1)).thenReturn(List.of(new BigDecimal("9.00")));
        when(priceTracker.getLast60SecondsMedians(t0.getEpochSecond() + 2)).thenReturn(List.of(new BigDecimal("9.00")));

        // At t0 + 3, new tick arrives with price 10.00
        Instant t3 = t0.plusSeconds(3);
        PriceSnapshot snap3 = new PriceSnapshot(t3, new BigDecimal("10.00"), new BigDecimal("10.00"), new BigDecimal("10.00"), new BigDecimal("10.00"));
        when(priceTracker.getSnapshot(t3)).thenReturn(Optional.of(snap3));
        when(priceTracker.getLast60SecondsMedians(t3.getEpochSecond())).thenReturn(List.of(new BigDecimal("9.00"), new BigDecimal("9.00"), new BigDecimal("9.00"), new BigDecimal("10.00")));

        // Jump straight to t0 + 3 (e.g. gap of 2 seconds)
        engine.onBinanceTick(t3.toEpochMilli());

        // Should verify forward fill was recorded for seconds 1 and 2
        verify(priceTracker).recordForwardFilledMedian(t0.getEpochSecond() + 1);
        verify(priceTracker).recordForwardFilledMedian(t0.getEpochSecond() + 2);

        // 3 updates for 5m: second 1, second 2, second 3
        long count5m = updates.stream().filter(u -> u.timeframe() == Timeframe.FIVE_MINUTES).count();
        assertThat(count5m).isEqualTo(3);
    }

    @Test
    void shouldRollOverCandleOnBoundaryFromBinanceTimestamp() {
        Instant t0 = Instant.parse("2026-09-03T14:04:59Z");
        List<TwapPoint> initialPoints = new ArrayList<>();
        long candleStart = Instant.parse("2026-09-03T14:00:00Z").getEpochSecond();
        long candleEnd = Instant.parse("2026-09-03T14:05:00Z").getEpochSecond();

        initialPoints.add(new TwapPoint(candleStart, new BigDecimal("100.00"), new BigDecimal("100.00")));
        initialPoints.add(new TwapPoint(t0.getEpochSecond(), new BigDecimal("102.00"), new BigDecimal("104.00")));

        CandleTwapState mockState = new CandleTwapState(
                Timeframe.FIVE_MINUTES,
                candleStart,
                candleEnd,
                new BigDecimal("100.00"),
                initialPoints
        );
        when(reconstructor.reconstructCandle(any(Timeframe.class), eq(t0))).thenReturn(mockState);
        engine.initialize(t0);

        List<TwapUpdate> updates = new ArrayList<>();
        engine.registerListener(updates::add);

        // Binance tick at 14:05:00 -> Candle boundary reached!
        Instant tBoundary = Instant.parse("2026-09-03T14:05:00Z");
        PriceSnapshot snapBoundary = new PriceSnapshot(tBoundary, new BigDecimal("120.00"), new BigDecimal("120.00"), new BigDecimal("120.00"), new BigDecimal("120.00"));
        when(priceTracker.getSnapshot(tBoundary)).thenReturn(Optional.of(snapBoundary));
        when(priceTracker.getLast60SecondsMedians(tBoundary.getEpochSecond())).thenReturn(List.of(new BigDecimal("120.00")));

        engine.onBinanceTick(tBoundary.toEpochMilli());

        assertThat(updates).isNotEmpty();
        TwapUpdate rollUpdate = updates.stream().filter(u -> u.timeframe() == Timeframe.FIVE_MINUTES).findFirst().orElseThrow();
        assertThat(rollUpdate.isNewCandle()).isTrue();
        assertThat(rollUpdate.candleStart()).isEqualTo(tBoundary.getEpochSecond());
        assertThat(rollUpdate.openPrice()).isEqualByComparingTo("120.00");
        assertThat(rollUpdate.point().twap()).isEqualByComparingTo("120.00");

        CandleTwapState currentState = engine.getCurrentCandleState(Timeframe.FIVE_MINUTES);
        assertThat(currentState.candleStart()).isEqualTo(tBoundary.getEpochSecond());
        assertThat(currentState.points()).hasSize(1);
    }

    @Test
    void shouldSwitchTimeframe() {
        Instant now = Instant.parse("2026-09-03T14:07:00Z");
        CandleTwapState state5m = new CandleTwapState(
                Timeframe.FIVE_MINUTES,
                Instant.parse("2026-09-03T14:05:00Z").getEpochSecond(),
                Instant.parse("2026-09-03T14:10:00Z").getEpochSecond(),
                new BigDecimal("100.00"),
                new ArrayList<>()
        );
        CandleTwapState state15m = new CandleTwapState(
                Timeframe.FIFTEEN_MINUTES,
                Instant.parse("2026-09-03T14:00:00Z").getEpochSecond(),
                Instant.parse("2026-09-03T14:15:00Z").getEpochSecond(),
                new BigDecimal("95.00"),
                new ArrayList<>()
        );

        when(reconstructor.reconstructCandle(Timeframe.FIVE_MINUTES, now)).thenReturn(state5m);
        when(reconstructor.reconstructCandle(Timeframe.FIFTEEN_MINUTES, now)).thenReturn(state15m);

        engine.initialize(now);
        assertThat(engine.getActiveTimeframe()).isEqualTo(Timeframe.FIVE_MINUTES);

        CandleTwapState switched = engine.switchTimeframe(Timeframe.FIFTEEN_MINUTES, now);
        assertThat(switched.timeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(switched.openPrice()).isEqualByComparingTo("95.00");
        assertThat(engine.getActiveTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
    }
}
