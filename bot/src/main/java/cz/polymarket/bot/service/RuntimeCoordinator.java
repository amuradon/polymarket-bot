package cz.polymarket.bot.service;

import cz.polymarket.bot.collector.DataCollectorService;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.engine.StrategyMode;
import cz.polymarket.bot.engine.StrategyTradingEngine;
import cz.polymarket.bot.state.RuntimeStateSnapshot;
import cz.polymarket.bot.state.RuntimeStateStore;
import cz.polymarket.bot.storage.PaperTradeRepository;
import cz.polymarket.bot.web.CollectorStatusDto;
import cz.polymarket.bot.web.HealthDto;
import cz.polymarket.bot.web.StatusDto;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Coordinates deterministic startup, shutdown, runtime {@link StrategyMode} transitions,
 * {@link RuntimeStateSnapshot} persistence, and unified health/status evaluation across
 * {@link DataCollectorService} and {@link StrategyTradingEngine}.
 */
@ApplicationScoped
public class RuntimeCoordinator {

    private final DataCollectorService collectorService;
    private final StrategyTradingEngine tradingEngine;
    private final PaperTradeRepository paperRepository;
    private final RuntimeStateStore stateStore;

    /**
     * Constructs the runtime coordinator.
     *
     * @param collectorService Data Collector orchestrator service
     * @param tradingEngine    unified Strategy Trading Engine
     * @param paperRepository  persistent paper trade repository
     * @param stateStore       atomic runtime state store
     */
    @Inject
    public RuntimeCoordinator(
            DataCollectorService collectorService,
            StrategyTradingEngine tradingEngine,
            PaperTradeRepository paperRepository,
            RuntimeStateStore stateStore) {
        if (collectorService == null) {
            throw new IllegalArgumentException("collectorService cannot be null");
        }
        if (tradingEngine == null) {
            throw new IllegalArgumentException("tradingEngine cannot be null");
        }
        if (paperRepository == null) {
            throw new IllegalArgumentException("paperRepository cannot be null");
        }
        if (stateStore == null) {
            throw new IllegalArgumentException("stateStore cannot be null");
        }
        this.collectorService = collectorService;
        this.tradingEngine = tradingEngine;
        this.paperRepository = paperRepository;
        this.stateStore = stateStore;
    }

    /**
     * Restores persisted {@link RuntimeStateSnapshot} (if present and current), starts Data Collector streams,
     * and launches the Strategy Trading Engine in the effective {@link StrategyMode}.
     *
     * @param event Quarkus startup event
     */
    public void onStart(@Observes StartupEvent event) {
        startRuntime(Instant.now().getEpochSecond());
    }

    /**
     * Deterministically initializes runtime state, Data Collector streams, and Strategy Engine for {@code nowEpochSec}.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void startRuntime(long nowEpochSec) {
        Optional<RuntimeStateSnapshot> saved = stateStore.load();
        saved.ifPresent(snapshot -> tradingEngine.restoreStateSnapshot(snapshot, nowEpochSec));

        if (tradingEngine.getStrategyMode() == StrategyMode.LIVE) {
            tradingEngine.initializeCredentialsAndSigner();
        }

        collectorService.startStreams(nowEpochSec);
        tradingEngine.startConfiguredMode(nowEpochSec);
        stateStore.save(tradingEngine.captureStateSnapshot());
    }

    /**
     * Persists the latest {@link RuntimeStateSnapshot} (including active market {@code twapOpenPrice},
     * {@link StrategyMode}, and open positions) before stopping Strategy threads and flushing Data Collector Zstd files.
     *
     * @param event Quarkus shutdown event
     */
    public void onStop(@Observes ShutdownEvent event) {
        stopRuntime();
    }

    /**
     * Synchronously saves {@link RuntimeStateSnapshot}, stops Strategy workers/streams, and flushes/closes
     * all Data Collector Zstandard writers.
     */
    public synchronized void stopRuntime() {
        Log.info("Shutting down Polymarket Bot: saving RuntimeStateSnapshot, stopping Strategy pipeline, and flushing Data Collector...");
        stateStore.save(tradingEngine.captureStateSnapshot());
        tradingEngine.stopStrategyStreams();
        RuntimeStateSnapshot finalSnapshot = tradingEngine.captureStateSnapshot();
        stateStore.save(finalSnapshot);
        collectorService.stopAndCloseAll();
        Log.infof("Polymarket Bot shutdown complete (saved mode=%s, activeCandleStart=%d, twapOpenPrice=$%.2f)",
                finalSnapshot.strategyMode(), finalSnapshot.activeCandleStartSec(), finalSnapshot.twapOpenPrice());
    }

    /**
     * Evaluates the composite operational health of Data Collector and Strategy Engine.
     *
     * @return current {@link HealthDto}
     */
    public HealthDto evaluateHealth() {
        CollectorStatusDto collector = collectorService.getStatus();
        List<String> issues = new ArrayList<>();
        boolean collectorHealthy = collector.healthy();
        if (!collectorHealthy) {
            issues.add("Data Collector is unhealthy (streams disconnected or zero records written)");
        }
        boolean strategyHealthy = tradingEngine.evaluateStrategyHealth(issues);
        boolean overallHealthy = collectorHealthy && strategyHealthy;
        return new HealthDto(
                overallHealthy,
                tradingEngine.getStrategyMode(),
                collectorHealthy,
                strategyHealthy,
                tradingEngine.getTwapOpenPrice(),
                List.copyOf(issues)
        );
    }

    /**
     * Builds the unified {@link StatusDto} combining Strategy runtime state, Data Collector status,
     * Paper Trading metrics, and 4-core / OS / WebSocket telemetry.
     *
     * @return current {@link StatusDto}
     */
    public StatusDto getStatus() {
        CollectorStatusDto collector = collectorService.getStatus();
        List<String> issues = new ArrayList<>();
        boolean strategyHealthy = tradingEngine.evaluateStrategyHealth(issues);
        boolean overallHealthy = collector.healthy() && strategyHealthy;
        PolymarketMarketInfo market = tradingEngine.getActiveMarketInfo();

        return new StatusDto(
                overallHealthy,
                tradingEngine.getStrategyMode(),
                tradingEngine.getActiveStrategyName(),
                tradingEngine.getOrderType(),
                tradingEngine.getOrderLatencyMs(),
                market != null ? market.slug() : "",
                market != null ? market.yesTokenId() : "",
                market != null ? market.noTokenId() : "",
                tradingEngine.getTwapOpenPrice(),
                tradingEngine.getCurrentSpotPrice(),
                tradingEngine.getInitialCapital(),
                tradingEngine.getCurrentBalance(),
                tradingEngine.getActivePosition(),
                tradingEngine.getOpenPositions(),
                paperRepository.getMetrics(),
                collector,
                tradingEngine.getTelemetry()
        );
    }

    /**
     * Switches the active {@link StrategyMode} at runtime and immediately persists the updated {@link RuntimeStateSnapshot}.
     *
     * @param targetMode target {@link StrategyMode} ({@code LIVE}, {@code PAPER}, or {@code OFF})
     * @return updated {@link StatusDto}
     */
    public synchronized StatusDto switchStrategyMode(StrategyMode targetMode) {
        tradingEngine.setStrategyMode(targetMode, Instant.now().getEpochSecond());
        stateStore.save(tradingEngine.captureStateSnapshot());
        return getStatus();
    }

    /**
     * Resets engine state (and paper trade history when {@code resetPaperHistory} is {@code true}) and persists state.
     *
     * @param resetPaperHistory whether to also clear {@link PaperTradeRepository}
     */
    public synchronized void reset(boolean resetPaperHistory) {
        tradingEngine.reset();
        if (resetPaperHistory) {
            paperRepository.reset();
        }
        stateStore.save(tradingEngine.captureStateSnapshot());
    }
}
