package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketGammaClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates the lifecycle of Polymarket prediction markets, maintaining CLOB WebSocket connections
 * to the current market and {@code lookahead-count} (default 5) future markets.
 * Records all raw CLOB WebSocket messages as UTF-8 {@code byte[]} directly to Zstandard storage without JSON parsing
 * or {@link String} allocation overhead, and permanently closes each market's stream and {@code .jsonl.zst} file
 * when the Polymarket CLOB WebSocket server closes the resolved market stream with {@code closeCode=1000},
 * when the stream disconnects at or after the market's {@code endSec}, or at the latest 5 minutes ({@code 300s})
 * after {@code endSec}.
 */
@ApplicationScoped
public class PolymarketLifecycleCoordinator {

    /**
     * Maximum retention window in seconds after a market's {@code endSec} (5 minutes = 300 seconds)
     * before its CLOB WebSocket stream and {@code .jsonl.zst} archive are forcefully closed.
     */
    public static final long MAX_POST_END_RETENTION_SECONDS = 300L;

    private final PolymarketGammaClient gammaClient;
    private final PolymarketClobClient clobClient;
    private final PolymarketMarketStorageManager storageManager;
    private final String symbol;
    private final String timeframe;
    private final int lookaheadCount;
    private final long durationSeconds;

    private final Map<String, PolymarketMarketInfo> trackedMarketsBySlug = new ConcurrentHashMap<>();
    private final Map<String, Long> closedMarketsEndSecBySlug = new ConcurrentHashMap<>();
    private volatile long lastRefreshEpochSec = 0L;
    private volatile long lastRefreshNanoTime = 0L;

    /**
     * Constructs the Polymarket lifecycle coordinator.
     *
     * @param gammaClient    Gamma REST client for initial market discovery
     * @param clobClient     CLOB WebSocket client managing per-market streams
     * @param storageManager per-market Zstd storage manager
     * @param symbol         configured asset symbol (e.g. BTCUSDT)
     * @param timeframe      configured Polymarket timeframe (e.g. 15m)
     * @param lookaheadCount number of future markets to record ahead of the current market (default 5)
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
            int lookaheadCount) {
        this.gammaClient = gammaClient;
        this.clobClient = clobClient;
        this.storageManager = storageManager;
        this.symbol = symbol;
        this.timeframe = timeframe;
        this.lookaheadCount = Math.max(0, lookaheadCount);
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
        closedMarketsEndSecBySlug.clear();
    }

    /**
     * Discovers and opens CLOB WebSocket streams for new future markets so that the current market
     * plus {@code lookaheadCount} future markets are continuously recorded.
     * Ended markets remain open until the CLOB WebSocket server closes their stream with {@code closeCode=1000},
     * the stream disconnects after {@code endSec}, or {@link #MAX_POST_END_RETENTION_SECONDS} (5 minutes)
     * elapses after {@code endSec}.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void discoverAndRefreshMarkets(long nowEpochSec) {
        this.lastRefreshEpochSec = nowEpochSec;
        this.lastRefreshNanoTime = System.nanoTime();

        long currentStartSec = PolymarketMarketInfo.alignCycleStart(nowEpochSec, durationSeconds);
        closedMarketsEndSecBySlug.entrySet().removeIf(entry -> entry.getValue() < currentStartSec);

        closeExpiredOrDisconnectedEndedMarkets(nowEpochSec);

        // If a previous market is still open awaiting server-side closeCode=1000 resolution,
        // wait until it closes before opening the next future slot
        boolean awaitingPreviousResolution = trackedMarketsBySlug.values().stream()
                .anyMatch(m -> m.endSec() > 0 && m.endSec() <= nowEpochSec);
        int maxFutureIndex = awaitingPreviousResolution ? Math.max(0, lookaheadCount - 1) : lookaheadCount;

        for (int i = 0; i <= maxFutureIndex; i++) {
            long slotStartSec = currentStartSec + (i * durationSeconds);
            String slug = PolymarketMarketInfo.buildMarketSlug(symbol, timeframe, slotStartSec);
            if (!trackedMarketsBySlug.containsKey(slug) && !closedMarketsEndSecBySlug.containsKey(slug)) {
                Optional<PolymarketMarketInfo> discovered = gammaClient.fetchMarketBySlug(slug, durationSeconds);
                discovered.ifPresent(info -> registerNewMarket(info, System.currentTimeMillis()));
            }
        }
    }

    private void closeExpiredOrDisconnectedEndedMarkets(long nowEpochSec) {
        Map<String, ConnectionState> connectionStates = clobClient.getConnectionStates();
        for (PolymarketMarketInfo market : List.copyOf(trackedMarketsBySlug.values())) {
            if (market.endSec() <= 0) {
                continue;
            }
            String slug = market.slug();
            if (nowEpochSec >= market.endSec() + MAX_POST_END_RETENTION_SECONDS) {
                trackedMarketsBySlug.remove(slug);
                closedMarketsEndSecBySlug.put(slug, market.endSec());
                clobClient.closeMarketStream(slug);
                storageManager.closeMarketWriter(slug);
                Log.infof("Polymarket market %s reached %ds post-end safeguard timeout (endSec=%d, now=%d); closing stream and finalizing market archive.",
                        slug, MAX_POST_END_RETENTION_SECONDS, market.endSec(), nowEpochSec);
            } else if (nowEpochSec >= market.endSec() && connectionStates != null) {
                ConnectionState state = connectionStates.get(slug);
                if (state == ConnectionState.DISCONNECTED || state == ConnectionState.RECONNECTING) {
                    trackedMarketsBySlug.remove(slug);
                    closedMarketsEndSecBySlug.put(slug, market.endSec());
                    clobClient.closeMarketStream(slug);
                    storageManager.closeMarketWriter(slug);
                    Log.infof("Polymarket market %s is %s after market end (endSec=%d, now=%d); closing stream without reconnect and finalizing market archive.",
                            slug, state, market.endSec(), nowEpochSec);
                }
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
     * Records a raw UTF-8 {@code byte[]} CLOB WebSocket message for a specific market without {@link String} allocation.
     *
     * @param slug            market slug owning the CLOB WebSocket stream
     * @param rawPayloadBytes unmodified UTF-8 JSON bytes from CLOB WebSocket
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawClobMessageBytes(String slug, byte[] rawPayloadBytes, long recvTimestampMs) {
        if (!isJsonPayloadBytes(rawPayloadBytes) || !trackedMarketsBySlug.containsKey(slug)) {
            return;
        }
        storageManager.recordMarketEventBytes(slug, recvTimestampMs, "clob", rawPayloadBytes);
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
        String slug = info.slug();
        trackedMarketsBySlug.put(slug, info);
        storageManager.recordMarketEvent(slug, recvTs, "market_discovered", info.rawJson());
        clobClient.openMarketStream(
                slug,
                info.clobTokenIds(),
                new RawWebSocketListener() {
                    @Override
                    public void onMessage(String rawJson, long msgRecvTs) {
                        onRawClobMessage(slug, rawJson, msgRecvTs);
                    }

                    @Override
                    public void onMessageBytes(TimestampedFrame frame) {
                        if (frame != null) {
                            onRawClobMessageBytes(slug, frame.rawUtf8Bytes(), frame.recvTs());
                        }
                    }
                },
                telemetry -> onMarketClosedByServer(slug, telemetry),
                () -> shouldReconnectMarketStream(info)
        );
        Log.infof("Tracking Polymarket market %s (start=%d, end=%d, tokens=%s)",
                slug, info.startSec(), info.endSec(), info.clobTokenIds());
    }

    boolean shouldReconnectMarketStream(PolymarketMarketInfo info) {
        if (info == null || info.endSec() <= 0) {
            return true;
        }
        return currentEpochSec() < info.endSec();
    }

    private long currentEpochSec() {
        long baseEpochSec = this.lastRefreshEpochSec;
        if (baseEpochSec <= 0L) {
            return Instant.now().getEpochSecond();
        }
        long elapsedSec = Math.max(0L, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - this.lastRefreshNanoTime));
        return baseEpochSec + elapsedSec;
    }

    private synchronized void onMarketClosedByServer(String slug, WebSocketStreamTelemetry telemetry) {
        PolymarketMarketInfo removed = trackedMarketsBySlug.remove(slug);
        if (removed != null) {
            closedMarketsEndSecBySlug.put(slug, removed.endSec());
        }
        clobClient.closeMarketStream(slug);
        storageManager.closeMarketWriter(slug);
        if (telemetry != null) {
            if (telemetry.lastCloseCode() == 1000) {
                Log.infof("Polymarket market %s closed normally by server (closeCode=%d, reason=%s, totalFrames=%d); market archive finalized.",
                        slug, telemetry.lastCloseCode(), telemetry.lastCloseReason(), telemetry.totalFramesReceived());
            } else {
                Log.infof("Polymarket market %s disconnected after market end (closeCode=%d, reason=%s, totalFrames=%d); skipping reconnect and finalizing market archive.",
                        slug, telemetry.lastCloseCode(), telemetry.lastCloseReason(), telemetry.totalFramesReceived());
            }
        } else {
            Log.infof("Polymarket market %s closed by server; market archive finalized.", slug);
        }
    }

    private static boolean isJsonPayload(String raw) {
        if (raw == null || raw.isBlank()) {
            return false;
        }
        char first = raw.stripLeading().charAt(0);
        return first == '{' || first == '[';
    }

    private static boolean isJsonPayloadBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return false;
        }
        for (byte b : bytes) {
            int u = b & 0xFF;
            if (u > ' ') {
                return u == '{' || u == '[';
            }
        }
        return false;
    }
}
