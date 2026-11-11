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
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@ApplicationScoped
public class TwapEngine {

    private final ExchangePriceTracker priceTracker;
    private final HistoricalDataReconstructor reconstructor;
    private final HourlyPriceCache cache;
    private final TwapCalculator twapCalculator;
    private final BinanceWebSocketClient binanceClient;
    private final CoinbaseWebSocketClient coinbaseClient;
    private final KrakenWebSocketClient krakenClient;
    private final String defaultTimeframeCode;
    private final Vertx vertx;
    private final StrategyRegistry strategyRegistry;
    private final String strategyName;
    private final boolean enabledForStrategy;

    private final AtomicReference<Timeframe> activeTimeframe = new AtomicReference<>();
    private final Map<Timeframe, CandleTwapState> candleStates = new ConcurrentHashMap<>();
    private final List<Consumer<TwapUpdate>> listeners = new CopyOnWriteArrayList<>();
    private final AtomicLong lastProcessedSecond = new AtomicLong(0);
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
            @ConfigProperty(name = "polymarket.twap.default-timeframe", defaultValue = "5m") String defaultTimeframeCode,
            Vertx vertx,
            StrategyRegistry strategyRegistry,
            @ConfigProperty(name = "polymarket.strategy.name", defaultValue = "TWAPArbitrageStrategy") String strategyName) {
        this.priceTracker = priceTracker;
        this.reconstructor = reconstructor;
        this.cache = cache;
        this.twapCalculator = twapCalculator;
        this.binanceClient = binanceClient;
        this.coinbaseClient = coinbaseClient;
        this.krakenClient = krakenClient;
        this.defaultTimeframeCode = defaultTimeframeCode;
        this.vertx = vertx;
        this.strategyRegistry = strategyRegistry;
        this.strategyName = strategyName;
        this.enabledForStrategy = resolveEnabledForStrategy(strategyRegistry, strategyName);
        this.activeTimeframe.set(Timeframe.fromCode(defaultTimeframeCode));
        if (this.enabledForStrategy && this.binanceClient != null) {
            this.binanceClient.setTickListener(this::onBinanceTick);
        }
    }

    private static boolean resolveEnabledForStrategy(StrategyRegistry registry, String name) {
        if (registry == null || name == null || name.isBlank()) {
            return true;
        }
        try {
            TradingStrategy strategy = registry.getStrategy(name);
            return strategy.getRequiredExternalStreams() != null
                    && strategy.getRequiredExternalStreams().contains(ExternalStreamType.TWAP_ENGINE);
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    /**
     * Returns whether this {@link TwapEngine} is required by and enabled for the active strategy.
     */
    public boolean isEnabledForStrategy() {
        return enabledForStrategy;
    }

    void onStart(@Observes StartupEvent ev) {
        if (!enabledForStrategy) {
            Log.infof("Skipping TwapEngine startup because active strategy '%s' does not require TWAP_ENGINE external stream",
                    strategyName);
            return;
        }
        Log.info("Initializing TWAP Engine and starting Exchange WebSocket streams...");
        if (vertx != null) {
            vertx.executeBlocking(() -> {
                initialize(Instant.now());
                return null;
            }).onComplete(ar -> {
                if (ar.succeeded()) {
                    Log.info("TWAP Engine historical reconstruction completed successfully");
                } else {
                    Log.errorf("TWAP Engine historical reconstruction encountered error: %s", ar.cause().getMessage());
                }
            });
        } else {
            try {
                initialize(Instant.now());
            } catch (Exception e) {
                Log.errorf("TWAP Engine historical reconstruction error: %s", e.getMessage());
            }
        }

        binanceClient.start();
        coinbaseClient.start();
        krakenClient.start();
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (!enabledForStrategy) {
            return;
        }
        Log.info("Stopping Exchange WebSocket streams...");
        binanceClient.stop();
        coinbaseClient.stop();
        krakenClient.stop();
    }

    public synchronized void initialize(Instant now) {
        for (Timeframe tf : Timeframe.values()) {
            try {
                CandleTwapState state = reconstructor.reconstructCandle(tf, now);
                candleStates.put(tf, state);
                Log.infof("Initialized TWAP engine for timeframe %s, open price: %s, points: %d",
                        tf.getCode(), state.openPrice(), state.points().size());
            } catch (Exception e) {
                Log.errorf("Failed to reconstruct candle history for %s: %s", tf.getCode(), e.getMessage());
            }
        }
        initialized = !candleStates.isEmpty();
        if (initialized) {
            lastProcessedSecond.set(now.getEpochSecond());
        }
    }

    public synchronized void onBinanceTick(long eventTimeMs) {
        if (eventTimeMs <= 0) {
            return;
        }

        long currentSec = eventTimeMs / 1000L;

        if (!initialized) {
            if (vertx != null) {
                vertx.executeBlocking(() -> {
                    initialize(Instant.ofEpochSecond(currentSec));
                    return null;
                });
            } else {
                initialize(Instant.ofEpochSecond(currentSec));
            }
            lastProcessedSecond.set(currentSec);
            priceTracker.getSnapshot(Instant.ofEpochSecond(currentSec));
            processSecond(currentSec);
            return;
        }

        long lastSec = lastProcessedSecond.get();
        if (lastSec == 0) {
            lastProcessedSecond.set(currentSec);
            priceTracker.getSnapshot(Instant.ofEpochSecond(currentSec));
            processSecond(currentSec);
            return;
        }

        if (currentSec <= lastSec) {
            return;
        }

        // In case of a large gap > 60s (network stall, container unfreeze), clamp forward fill to at most 60 seconds
        long startGap = lastSec + 1;
        if (currentSec - lastSec > 60) {
            Log.warnf("Detected time gap of %d seconds in Binance ticks. Clamping forward fill to last 60 seconds.", (currentSec - lastSec));
            startGap = currentSec - 60;
        }

        // In case of low activity / gap: forward fill missing seconds with last known price
        for (long s = startGap; s < currentSec; s++) {
            priceTracker.recordForwardFilledMedian(s);
            processSecond(s);
        }

        // Process current second with latest snapshot
        priceTracker.getSnapshot(Instant.ofEpochSecond(currentSec));
        processSecond(currentSec);
        lastProcessedSecond.set(currentSec);
    }

    private void processSecond(long sec) {
        List<BigDecimal> windowPrices = priceTracker.getLast60SecondsMedians(sec);
        if (windowPrices.isEmpty()) {
            return;
        }

        BigDecimal rollingTwap = twapCalculator.calculateRollingTwap(windowPrices);
        BigDecimal currentMedian = windowPrices.get(windowPrices.size() - 1);

        // Store 60s TWAP directly in HourlyPriceCache
        cache.put(sec, rollingTwap);

        Instant time = Instant.ofEpochSecond(sec);

        for (Timeframe tf : Timeframe.values()) {
            CandleTwapState currentState = candleStates.get(tf);
            if (currentState == null) {
                try {
                    currentState = reconstructor.reconstructCandle(tf, time);
                    candleStates.put(tf, currentState);
                } catch (Exception e) {
                    continue;
                }
            }

            // Check if candle boundary has been reached
            if (sec >= currentState.candleEnd()) {
                Instant newStart = tf.getCandleStart(time);
                Instant newEnd = tf.getCandleEnd(newStart);

                // Open price for new candle is 60s TWAP at candle start
                BigDecimal openPrice = rollingTwap;
                TwapPoint p0 = twapCalculator.createPoint(newStart.getEpochSecond(), rollingTwap, currentMedian);

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
                    TwapPoint p0 = twapCalculator.createPoint(currentState.candleStart(), currentState.openPrice(), currentMedian);
                    points.add(p0);
                }

                TwapPoint lastPoint = points.get(points.size() - 1);
                if (lastPoint.time() == sec) {
                    continue;
                }

                TwapPoint nextPoint = twapCalculator.createPoint(sec, rollingTwap, currentMedian);
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
                Log.errorf("Error notifying listener: %s", e.getMessage());
            }
        }
    }
}
