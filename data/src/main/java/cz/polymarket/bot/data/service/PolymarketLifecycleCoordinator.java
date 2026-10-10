package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketGammaClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinates the lifecycle of Polymarket prediction markets, maintaining CLOB WebSocket connections
 * to the current market and {@code lookahead-count} (default 5) future markets.
 * Records all raw CLOB WebSocket messages without JSON parsing overhead, and disconnects each market's
 * stream and closes its {@code .jsonl.zst} file once its resolution is recorded.
 */
@ApplicationScoped
public class PolymarketLifecycleCoordinator {

    private static final Logger LOG = Logger.getLogger(PolymarketLifecycleCoordinator.class);

    private final PolymarketGammaClient gammaClient;
    private final PolymarketClobClient clobClient;
    private final PolymarketMarketStorageManager storageManager;
    private final String symbol;
    private final String timeframe;
    private final int lookaheadCount;
    private final long postResolutionGraceSeconds;
    private final long durationSeconds;

    private final Map<String, PolymarketMarketInfo> trackedMarketsBySlug = new ConcurrentHashMap<>();

    /**
     * Constructs the Polymarket lifecycle coordinator.
     *
     * @param gammaClient                Gamma REST client for market discovery and resolution polling
     * @param clobClient                 CLOB WebSocket client managing per-market streams
     * @param storageManager             per-market Zstd storage manager
     * @param symbol                     configured asset symbol (e.g. BTCUSDT)
     * @param timeframe                  configured Polymarket timeframe (e.g. 15m)
     * @param lookaheadCount             number of future markets to record ahead of the current market (default 5)
     * @param postResolutionGraceSeconds grace period after endSec before closing an unresolved market file
     */
    @Inject
    public PolymarketLifecycleCoordinator(
            PolymarketGammaClient gammaClient,
            PolymarketClobClient clobClient,
            PolymarketMarketStorageManager storageManager,
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.collector.timeframe", defaultValue = "15m")
            String timeframe,
            @ConfigProperty(name = "polymarket.collector.lookahead-count", defaultValue = "5")
            int lookaheadCount,
            @ConfigProperty(name = "polymarket.collector.post-resolution-grace-seconds", defaultValue = "300")
            long postResolutionGraceSeconds) {
        this.gammaClient = gammaClient;
        this.clobClient = clobClient;
        this.storageManager = storageManager;
        this.symbol = symbol;
        this.timeframe = timeframe;
        this.lookaheadCount = Math.max(0, lookaheadCount);
        this.postResolutionGraceSeconds = Math.max(10L, postResolutionGraceSeconds);
        this.durationSeconds = PolymarketMarketInfo.parseTimeframeSeconds(timeframe);
    }

    /**
     * Performs initial discovery and CLOB stream subscription for the current market and
     * {@code lookaheadCount} future markets.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public void start(long nowEpochSec) {
        discoverAndRefreshMarkets(nowEpochSec);
    }

    /**
     * Stops all active CLOB WebSocket streams and closes all open market files.
     */
    public void stop() {
        clobClient.stopAll();
        storageManager.closeAll();
        trackedMarketsBySlug.clear();
    }

    /**
     * Checks ended markets for resolution (disconnecting their streams and closing their files once resolved)
     * and opens streams for new future markets so that the current market plus {@code lookaheadCount}
     * future markets are continuously recorded.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void discoverAndRefreshMarkets(long nowEpochSec) {
        // 1. Check tracked markets that have passed their endSec for resolution and cleanup
        for (String slug : new ArrayList<>(trackedMarketsBySlug.keySet())) {
            PolymarketMarketInfo info = trackedMarketsBySlug.get(slug);
            if (info == null) {
                continue;
            }
            long endSec = info.endSec();
            if (endSec > 0 && nowEpochSec >= endSec) {
                Optional<PolymarketMarketInfo> refreshed = gammaClient.fetchMarketBySlug(slug, durationSeconds);
                if (refreshed.isPresent() && refreshed.get().closed()) {
                    storageManager.recordMarketEvent(
                            slug,
                            System.currentTimeMillis(),
                            "market_resolved",
                            refreshed.get().rawJson()
                    );
                    finalizeMarket(slug);
                } else if (nowEpochSec >= endSec + postResolutionGraceSeconds) {
                    refreshed.ifPresent(latest -> storageManager.recordMarketEvent(
                            slug,
                            System.currentTimeMillis(),
                            "market_resolved",
                            latest.rawJson()
                    ));
                    finalizeMarket(slug);
                }
            }
        }

        // 2. If a previous market is still open awaiting resolution, wait until it resolves before opening the next future slot
        boolean awaitingPreviousResolution = trackedMarketsBySlug.values().stream()
                .anyMatch(m -> m.endSec() > 0 && m.endSec() <= nowEpochSec);
        int maxFutureIndex = awaitingPreviousResolution ? Math.max(0, lookaheadCount - 1) : lookaheadCount;

        long currentStartSec = PolymarketMarketInfo.alignCycleStart(nowEpochSec, durationSeconds);
        for (int i = 0; i <= maxFutureIndex; i++) {
            long slotStartSec = currentStartSec + (i * durationSeconds);
            String slug = PolymarketMarketInfo.buildMarketSlug(symbol, timeframe, slotStartSec);
            if (!trackedMarketsBySlug.containsKey(slug)) {
                Optional<PolymarketMarketInfo> discovered = gammaClient.fetchMarketBySlug(slug, durationSeconds);
                discovered.ifPresent(info -> registerNewMarket(info, System.currentTimeMillis()));
            }
        }
    }

    /**
     * Records a raw CLOB WebSocket message for a specific market without parsing.
     *
     * @param slug            market slug owning the CLOB WebSocket stream
     * @param rawJson         unmodified JSON payload from CLOB WebSocket
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawClobMessage(String slug, String rawJson, long recvTimestampMs) {
        if (!isJsonPayload(rawJson) || !trackedMarketsBySlug.containsKey(slug)) {
            return;
        }
        storageManager.recordMarketEvent(slug, recvTimestampMs, "clob", rawJson);
    }

    /**
     * Returns the list of currently tracked market slugs.
     *
     * @return list of active market slugs
     */
    public List<String> getTrackedMarketSlugs() {
        return new ArrayList<>(trackedMarketsBySlug.keySet());
    }

    private void registerNewMarket(PolymarketMarketInfo info, long recvTs) {
        trackedMarketsBySlug.put(info.slug(), info);
        storageManager.recordMarketEvent(info.slug(), recvTs, "market_discovered", info.rawJson());
        clobClient.openMarketStream(
                info.slug(),
                info.clobTokenIds(),
                (rawJson, msgRecvTs) -> onRawClobMessage(info.slug(), rawJson, msgRecvTs)
        );
        LOG.infof("Tracking Polymarket market %s (start=%d, end=%d, tokens=%s)",
                info.slug(), info.startSec(), info.endSec(), info.clobTokenIds());
    }

    private void finalizeMarket(String slug) {
        trackedMarketsBySlug.remove(slug);
        clobClient.closeMarketStream(slug);
        storageManager.closeMarketWriter(slug);
    }

    private static boolean isJsonPayload(String raw) {
        if (raw == null || raw.isBlank()) {
            return false;
        }
        char first = raw.stripLeading().charAt(0);
        return first == '{' || first == '[';
    }
}
