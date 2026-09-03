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
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@ApplicationScoped
public class TwapEngine {

    private static final Logger LOG = Logger.getLogger(TwapEngine.class);

    private final ExchangePriceTracker priceTracker;
    private final HistoricalDataReconstructor reconstructor;
    private final HourlyPriceCache cache;
    private final TwapCalculator twapCalculator;
    private final BinanceWebSocketClient binanceClient;
    private final CoinbaseWebSocketClient coinbaseClient;
    private final KrakenWebSocketClient krakenClient;
    private final String defaultTimeframeCode;

    private final AtomicReference<Timeframe> activeTimeframe = new AtomicReference<>();
    private final Map<Timeframe, CandleTwapState> candleStates = new ConcurrentHashMap<>();
    private final List<Consumer<TwapUpdate>> listeners = new CopyOnWriteArrayList<>();
    private volatile boolean initialized = false;

    @Inject
    public TwapEngine(
            ExchangePriceTracker priceTracker,
            HistoricalDataReconstructor reconstructor,
            HourlyPriceCache cache,
            TwapCalculator twapCalculator,
            BinanceWebSocketClient binanceClient,
            CoinbaseWebSocketClient coinbaseClient,
            KrakenWebSocketClient krakenClient,
            @ConfigProperty(name = "polymarket.twap.default-timeframe", defaultValue = "5m") String defaultTimeframeCode) {
        this.priceTracker = priceTracker;
        this.reconstructor = reconstructor;
        this.cache = cache;
        this.twapCalculator = twapCalculator;
        this.binanceClient = binanceClient;
        this.coinbaseClient = coinbaseClient;
        this.krakenClient = krakenClient;
        this.defaultTimeframeCode = defaultTimeframeCode;
        this.activeTimeframe.set(Timeframe.fromCode(defaultTimeframeCode));
    }

    void onStart(@Observes StartupEvent ev) {
        LOG.info("Starting Exchange WebSocket streams...");
        binanceClient.start();
        coinbaseClient.start();
        krakenClient.start();

        initialize(Instant.now());
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOG.info("Stopping Exchange WebSocket streams...");
        binanceClient.stop();
        coinbaseClient.stop();
        krakenClient.stop();
    }

    public synchronized void initialize(Instant now) {
        for (Timeframe tf : Timeframe.values()) {
            try {
                CandleTwapState state = reconstructor.reconstructCandle(tf, now);
                candleStates.put(tf, state);
                LOG.infof("Initialized TWAP engine for timeframe %s, open price: %s, points: %d",
                        tf.getCode(), state.openPrice(), state.points().size());
            } catch (Exception e) {
                LOG.warnf("Failed to reconstruct candle history for %s: %s", tf.getCode(), e.getMessage());
            }
        }
        initialized = !candleStates.isEmpty();
    }

    @Scheduled(every = "1s")
    void scheduledTick() {
        tick(Instant.now());
    }

    public synchronized void tick(Instant now) {
        Optional<PriceSnapshot> snapshotOpt = priceTracker.getSnapshot(now);
        BigDecimal currentMedian = null;
        if (snapshotOpt.isPresent()) {
            currentMedian = snapshotOpt.get().medianPrice();
            cache.put(now.getEpochSecond(), currentMedian);
        } else {
            currentMedian = cache.get(now.getEpochSecond());
        }

        if (currentMedian == null) {
            return;
        }

        if (!initialized) {
            initialize(now);
            if (!initialized) {
                return;
            }
        }

        long nowSec = now.getEpochSecond();

        for (Timeframe tf : Timeframe.values()) {
            CandleTwapState currentState = candleStates.get(tf);
            if (currentState == null) {
                try {
                    currentState = reconstructor.reconstructCandle(tf, now);
                    candleStates.put(tf, currentState);
                } catch (Exception e) {
                    continue;
                }
            }

            // Check if candle boundary has been reached
            if (nowSec >= currentState.candleEnd()) {
                Instant newStart = tf.getCandleStart(now);
                Instant newEnd = tf.getCandleEnd(newStart);

                BigDecimal openPrice = currentMedian;
                TwapPoint p0 = twapCalculator.createInitialPoint(newStart.getEpochSecond(), openPrice);

                List<TwapPoint> newPoints = new ArrayList<>();
                newPoints.add(p0);

                CandleTwapState newState = new CandleTwapState(
                        tf,
                        newStart.getEpochSecond(),
                        newEnd.getEpochSecond(),
                        openPrice,
                        newPoints
                );
                candleStates.put(tf, newState);

                TwapUpdate update = new TwapUpdate(tf, newState.candleStart(), newState.candleEnd(), openPrice, p0, true);
                notifyListeners(update);
            } else {
                List<TwapPoint> points = currentState.points();
                if (points.isEmpty()) {
                    TwapPoint p0 = twapCalculator.createInitialPoint(currentState.candleStart(), currentState.openPrice());
                    points.add(p0);
                }

                TwapPoint lastPoint = points.get(points.size() - 1);
                if (lastPoint.time() == nowSec) {
                    continue;
                }

                TwapPoint nextPoint = twapCalculator.calculateNext(lastPoint, nowSec, currentMedian, points.size());
                points.add(nextPoint);

                TwapUpdate update = new TwapUpdate(tf, currentState.candleStart(), currentState.candleEnd(), currentState.openPrice(), nextPoint, false);
                notifyListeners(update);
            }
        }
    }

    public synchronized CandleTwapState switchTimeframe(Timeframe newTimeframe, Instant now) {
        if (newTimeframe == null) {
            throw new IllegalArgumentException("Timeframe cannot be null");
        }
        activeTimeframe.set(newTimeframe);
        CandleTwapState state = reconstructor.reconstructCandle(newTimeframe, now);
        candleStates.put(newTimeframe, state);
        return state;
    }

    public CandleTwapState getCurrentCandleState(Timeframe timeframe) {
        CandleTwapState state = candleStates.get(timeframe);
        if (state == null) {
            state = switchTimeframe(timeframe, Instant.now());
        }
        // Return defensive copy of points
        return new CandleTwapState(
                state.timeframe(),
                state.candleStart(),
                state.candleEnd(),
                state.openPrice(),
                new ArrayList<>(state.points())
        );
    }

    public Timeframe getActiveTimeframe() {
        return activeTimeframe.get();
    }

    public void registerListener(Consumer<TwapUpdate> listener) {
        listeners.add(listener);
    }

    public void removeListener(Consumer<TwapUpdate> listener) {
        listeners.remove(listener);
    }

    private void notifyListeners(TwapUpdate update) {
        for (Consumer<TwapUpdate> listener : listeners) {
            try {
                listener.accept(update);
            } catch (Exception e) {
                LOG.warnf("Error notifying listener: %s", e.getMessage());
            }
        }
    }
}
