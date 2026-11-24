package cz.polymarket.bot.engine;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamListener;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
import cz.polymarket.bot.connector.kraken.KrakenStreamListener;
import cz.polymarket.bot.connector.os.LinuxCpuAffinity;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketGammaClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketSecretsResolver;
import cz.polymarket.bot.connector.polymarket.clob.ClobCancelResponse;
import cz.polymarket.bot.connector.polymarket.clob.ClobOrderEnvelope;
import cz.polymarket.bot.connector.polymarket.clob.ClobOrderResponse;
import cz.polymarket.bot.connector.polymarket.clob.PolymarketClobRestClient;
import cz.polymarket.bot.connector.polymarket.clob.PolymarketEip712Signer;
import cz.polymarket.bot.connector.polymarket.stream.PolyBoltTick;
import cz.polymarket.bot.connector.polymarket.stream.PolymarketBboQuote;
import cz.polymarket.bot.connector.polymarket.stream.PolymarketLocalBook;
import cz.polymarket.bot.connector.polymarket.stream.PolymarketStreamParser;
import cz.polymarket.bot.connector.polymarket.stream.PolymarketUserTradeEvent;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import cz.polymarket.bot.connector.ws.StreamRedundancyStatus;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.indicators.FastSpotMicroListener;
import cz.polymarket.bot.indicators.SpotTwapListener;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor.MonitoredThreadSpec;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor.SystemTelemetrySnapshot;
import cz.polymarket.bot.monitoring.TelemetryHistoryPoint;
import cz.polymarket.bot.monitoring.ThreadUtilizationTelemetry;
import cz.polymarket.bot.monitoring.WebSocketDisconnectEvent;
import cz.polymarket.bot.state.RuntimeStateSnapshot;
import cz.polymarket.bot.state.RuntimeStateStore;
import cz.polymarket.bot.storage.PaperTradeRepository;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import cz.polymarket.bot.stream.SpotTwapProcessor;
import cz.polymarket.bot.web.TelemetryDto;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Unified Strategy Trading Engine supporting {@link StrategyMode#LIVE}, {@link StrategyMode#PAPER},
 * and {@link StrategyMode#OFF} across a strict 4-core CPU-pinned lock-free pipeline designed for
 * {@code c8gn.xlarge} (4 physical Graviton4 cores):
 * <ul>
 *   <li><b>vCPU #0 ({@code clob-user-worker} + housekeeping)</b>: OS/ENA interrupts, ZGC, Quarkus HTTP/scheduler,
 *       Data Collector Zstandard writers, 1s telemetry/state maintenance, and authenticated
 *       {@code POLYMARKET-CLOB-USER} balance event draining in {@link StrategyMode#LIVE}.</li>
 *   <li><b>vCPU #1 ({@code market-ws-io-reader})</b>: Consolidated single-EventLoop native {@code epoll} + BoringSSL
 *       reader ({@link MarketWebSocketRuntime}) servicing all WebSocket sockets into lock-free MPSC queues.</li>
 *   <li><b>vCPU #2 ({@code market-strategy-worker})</b>: Fused lock-free L1-cache loop batch-draining CLOB orderbook
 *       deltas ({@link PolymarketLocalBook}), PolyBolt oracle ticks, and Binance/Kraken/Coinbase spot trades
 *       ({@code SpotTwapIndicator}) and immediately evaluating {@link TradingStrategy} with zero cross-thread
 *       quote handoff and adaptive user-space micro-spinning.</li>
 *   <li><b>vCPU #3 ({@code order-execution-io-worker})</b>: Dedicated isolated core exclusively executing EIP-712
 *       signing and HTTP {@code POST /order} / {@code DELETE /order} execution in {@link StrategyMode#LIVE}.</li>
 * </ul>
 * When {@link StrategyMode#OFF} is active, all strategy worker threads and strategy streams are stopped and
 * detached so zero strategy queues accumulate frames while Data Collector continues recording uninterrupted.
 */
@ApplicationScoped
public class StrategyTradingEngine implements StrategyContext, ExecutionRouter {

    private static final double TAKER_FEE_RATE = 0.07;
    private static final long DEBUG_MESSAGE_LOG_INTERVAL = 100L;
    private static final int MAX_TRACKED_USER_TRADES = 1024;
    private static final long REALTIME_CLOCK_WINDOW_MS = 3_600_000L;
    private static final long STALE_QUOTE_WARN_INTERVAL_MS = 1_000L;
    private static final long MARKET_PREFETCH_LOOKAHEAD_SEC = 60L;

    private final PolymarketSecretsResolver secretsResolver;
    private final PolymarketClobRestClient clobRestClient;
    private final PolymarketGammaClient gammaClient;
    private final PolymarketClobClient clobWsClient;
    private final PolymarketPolyBoltClient polyBoltClient;
    private final BinanceStreamConnector binanceConnector;
    private final KrakenStreamConnector krakenConnector;
    private final CoinbaseStreamConnector coinbaseConnector;
    private final PolymarketStreamParser streamParser;
    private final SpotTwapProcessor spotStreamProcessor;
    private final PaperTradeRepository paperRepository;
    private final RuntimeStateStore stateStore;
    private final SystemNetworkTelemetryMonitor telemetryMonitor;
    private final Vertx vertx;
    private final TradingStrategy activeStrategy;
    private final String symbol;
    private final String timeframe;
    private final long durationSeconds;
    private final boolean streamsEnabled;
    private final String orderType;
    private final long maxQuoteAgeMs;
    private final long orderLatencyMs;

    private final Object strategyLock = new Object();
    private final AtomicReference<Position> activePosition = new AtomicReference<>();
    private final List<Position> openPositions = new CopyOnWriteArrayList<>();
    private final AtomicLong activeCandleStart = new AtomicLong(0L);
    private final AtomicLong activeCandleEnd = new AtomicLong(0L);
    private final AtomicReference<PolymarketMarketInfo> activeMarketInfo = new AtomicReference<>();
    private final AtomicReference<PolymarketMarketInfo> prefetchedNextMarketInfo = new AtomicReference<>();
    private final AtomicReference<PolymarketLocalBook> activeLocalBook = new AtomicReference<>();
    private final AtomicLong polyBoltMarketMsgCount = new AtomicLong(0L);
    private final AtomicLong clobMarketMsgCount = new AtomicLong(0L);
    private final AtomicLong lastEvaluatedMarketCycleStart = new AtomicLong(0L);
    private final AtomicLong lastTwapOracleTimestampMs = new AtomicLong(0L);
    private final AtomicLong lastCryptoPriceOracleTimestampMs = new AtomicLong(0L);
    private final AtomicLong debugPolyBoltBatchCounter = new AtomicLong(0L);
    private final AtomicLong debugClobBatchCounter = new AtomicLong(0L);
    private final AtomicBoolean balanceSyncRequested = new AtomicBoolean(false);
    private final AtomicBoolean balanceSyncInFlight = new AtomicBoolean(false);

    // 4-Core Pipeline Coordination Queues & L1 Staged Quote Slot
    private final AtomicBoolean workersRunning = new AtomicBoolean(false);
    private final AtomicReference<OrderBookQuote> stagedQuoteSlot = new AtomicReference<>();
    private final ConcurrentLinkedQueue<OrderCommand> orderExecutionQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ExecutionReport> executionReportQueue = new ConcurrentLinkedQueue<>();

    // Dedicated Worker Threads (vCPU #0, #2, #3; vCPU #1 is owned by MarketWebSocketRuntime) & Busy Time Accumulators
    private volatile Thread clobUserWorkerThread;
    private volatile Thread marketStrategyThread;
    private volatile Thread orderExecutionWorkerThread;
    private volatile Thread telemetryMaintenanceThread;
    private final AtomicLong userWorkerBusyNs = new AtomicLong(0L);
    private final AtomicLong marketStrategyWorkerBusyNs = new AtomicLong(0L);
    private final AtomicLong orderExecutionWorkerBusyNs = new AtomicLong(0L);

    // Real-time Execution & Freshness Telemetry Counters
    private final AtomicLong ordersSubmitted = new AtomicLong(0L);
    private final AtomicLong ordersFilled = new AtomicLong(0L);
    private final AtomicLong ordersRejected = new AtomicLong(0L);
    private final AtomicLong staleQuotesSkipped = new AtomicLong(0L);
    private final AtomicLong conflatedQuotesCount = new AtomicLong(0L);
    private final AtomicLong lastQuoteAgeMs = new AtomicLong(0L);
    private final AtomicLong maxQuoteAgeMsObserved = new AtomicLong(0L);
    private final AtomicLong lastOrderRttMs = new AtomicLong(0L);
    private final AtomicLong maxOrderRttMs = new AtomicLong(0L);
    private final AtomicLong lastStaleWarnLogMs = new AtomicLong(0L);

    private final Map<String, Double> appliedUserTradeDeltas = new LinkedHashMap<>(128, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Double> eldest) {
            return size() > MAX_TRACKED_USER_TRADES;
        }
    };

    private volatile StrategyMode strategyMode;
    private volatile boolean polyBoltStartedByEngine = false;
    private volatile String subscribedStrategyMarketSlug = null;
    private volatile double liveInitialCapital = 0.0;
    private volatile double liveCurrentBalance = 0.0;
    private volatile PolymarketCredentials credentials;
    private volatile PolymarketEip712Signer eip712Signer;
    private volatile double twapOpenPrice = 0.0;
    private volatile double currentSpotPrice = 0.0;
    private volatile double candleHigh = 0.0;
    private volatile double candleLow = 0.0;
    private volatile long currentTimestampSec = 0L;
    private volatile long latestOracleRecvTimestampMs = 0L;
    private volatile OrderBookQuote latestQuote;

    /**
     * Constructs the unified Strategy Trading Engine.
     *
     * @param strategyRegistry       registry of available trading strategies
     * @param secretsResolver        AWS Secrets Manager / environment credentials resolver
     * @param clobRestClient         Polymarket CLOB REST client (L2 HMAC + EIP-712 order gateway)
     * @param gammaClient            Polymarket Gamma REST client ({@code GET /markets/slug/{slug}})
     * @param clobWsClient           Polymarket CLOB Market &amp; User WebSocket client
     * @param polyBoltClient         Polymarket PolyBolt v2 authenticated WebSocket client
     * @param binanceConnector       Binance Spot {@code aggTrades} WebSocket connector
     * @param krakenConnector        Kraken Spot {@code trade} WebSocket connector ({@code USDTUSD} + {@code <SYMBOL>})
     * @param coinbaseConnector      Coinbase Spot {@code matches} WebSocket connector
     * @param streamParser           JSON frame parser for PolyBolt and CLOB streams
     * @param spotStreamProcessor    CDI processor parsing Binance, Kraken, and Coinbase spot trades into {@code SpotTwapIndicator}
     * @param paperRepository        persistent paper trade repository ({@code trades.jsonl} + {@code metrics.json})
     * @param stateStore             atomic disk persistence store for {@link RuntimeStateSnapshot}
     * @param telemetryMonitor       OS kernel TCP buffer, JVM ZGC, and per-thread CPU utilization monitor
     * @param vertx                  Vert.x instance used for simulated paper execution latency timers
     * @param configuredStrategyMode initial configured {@link StrategyMode} ({@code LIVE}, {@code PAPER}, or {@code OFF})
     * @param strategyName           configured strategy name (defaults to {@code OraclePulseLockStrategy})
     * @param symbol                 trading asset symbol (defaults to {@code BTCUSDT})
     * @param timeframe              market timeframe (defaults to {@code 15m})
     * @param streamsEnabled         whether to connect strategy WebSocket streams when in {@code LIVE} or {@code PAPER} mode
     * @param orderType              CLOB order time-in-force in {@code LIVE} mode (defaults to {@code FAK})
     * @param maxQuoteAgeMs          maximum allowed orderbook quote age in milliseconds before skipping signal evaluation
     * @param orderLatencyMs         simulated order execution latency in milliseconds in {@code PAPER} mode
     */
    @Inject
    public StrategyTradingEngine(
            StrategyRegistry strategyRegistry,
            PolymarketSecretsResolver secretsResolver,
            PolymarketClobRestClient clobRestClient,
            PolymarketGammaClient gammaClient,
            PolymarketClobClient clobWsClient,
            PolymarketPolyBoltClient polyBoltClient,
            BinanceStreamConnector binanceConnector,
            KrakenStreamConnector krakenConnector,
            CoinbaseStreamConnector coinbaseConnector,
            PolymarketStreamParser streamParser,
            SpotTwapProcessor spotStreamProcessor,
            PaperTradeRepository paperRepository,
            RuntimeStateStore stateStore,
            SystemNetworkTelemetryMonitor telemetryMonitor,
            Vertx vertx,
            @ConfigProperty(name = "polymarket.strategy.mode", defaultValue = "LIVE")
            String configuredStrategyMode,
            @ConfigProperty(name = "polymarket.strategy.name", defaultValue = "OraclePulseLockStrategy")
            String strategyName,
            @ConfigProperty(name = "polymarket.trading.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.trading.timeframe", defaultValue = "15m")
            String timeframe,
            @ConfigProperty(name = "polymarket.trading.streams-enabled", defaultValue = "true")
            boolean streamsEnabled,
            @ConfigProperty(name = "polymarket.trading.order-type", defaultValue = "FAK")
            String orderType,
            @ConfigProperty(name = "polymarket.trading.max-quote-age-ms", defaultValue = "250")
            long maxQuoteAgeMs,
            @ConfigProperty(name = "polymarket.paper.order-latency-ms", defaultValue = "50")
            long orderLatencyMs) {
        if (strategyRegistry == null) {
            throw new IllegalArgumentException("strategyRegistry cannot be null");
        }
        if (secretsResolver == null) {
            throw new IllegalArgumentException("secretsResolver cannot be null");
        }
        if (clobRestClient == null) {
            throw new IllegalArgumentException("clobRestClient cannot be null");
        }
        if (gammaClient == null) {
            throw new IllegalArgumentException("gammaClient cannot be null");
        }
        if (clobWsClient == null) {
            throw new IllegalArgumentException("clobWsClient cannot be null");
        }
        if (polyBoltClient == null) {
            throw new IllegalArgumentException("polyBoltClient cannot be null");
        }
        if (binanceConnector == null) {
            throw new IllegalArgumentException("binanceConnector cannot be null");
        }
        if (krakenConnector == null) {
            throw new IllegalArgumentException("krakenConnector cannot be null");
        }
        if (coinbaseConnector == null) {
            throw new IllegalArgumentException("coinbaseConnector cannot be null");
        }
        if (streamParser == null) {
            throw new IllegalArgumentException("streamParser cannot be null");
        }
        if (spotStreamProcessor == null) {
            throw new IllegalArgumentException("spotStreamProcessor cannot be null");
        }
        if (paperRepository == null) {
            throw new IllegalArgumentException("paperRepository cannot be null");
        }
        if (stateStore == null) {
            throw new IllegalArgumentException("stateStore cannot be null");
        }
        if (telemetryMonitor == null) {
            throw new IllegalArgumentException("telemetryMonitor cannot be null");
        }

        this.secretsResolver = secretsResolver;
        this.clobRestClient = clobRestClient;
        this.gammaClient = gammaClient;
        this.clobWsClient = clobWsClient;
        this.polyBoltClient = polyBoltClient;
        this.binanceConnector = binanceConnector;
        this.krakenConnector = krakenConnector;
        this.coinbaseConnector = coinbaseConnector;
        this.streamParser = streamParser;
        this.spotStreamProcessor = spotStreamProcessor;
        this.paperRepository = paperRepository;
        this.stateStore = stateStore;
        this.telemetryMonitor = telemetryMonitor;
        this.vertx = vertx;
        this.strategyMode = StrategyMode.parse(configuredStrategyMode);
        this.symbol = symbol != null && !symbol.isBlank() ? symbol.trim() : "BTCUSDT";
        this.timeframe = timeframe != null && !timeframe.isBlank() ? timeframe.trim() : "15m";
        this.durationSeconds = PolymarketMarketInfo.parseTimeframeSeconds(this.timeframe);
        this.streamsEnabled = streamsEnabled;
        this.orderType = orderType != null && !orderType.isBlank() ? orderType.trim().toUpperCase(Locale.ROOT) : "FAK";
        this.maxQuoteAgeMs = Math.max(10L, maxQuoteAgeMs);
        this.orderLatencyMs = Math.max(0L, orderLatencyMs);

        String resolvedName = (strategyName == null || strategyName.isBlank())
                ? "OraclePulseLockStrategy"
                : strategyName.trim();
        this.activeStrategy = strategyRegistry.getStrategy(resolvedName);
        this.activeStrategy.init(this);
        if (this.activeStrategy instanceof SpotTwapListener listener) {
            this.spotStreamProcessor.getIndicator().addListener(listener);
        }
        if (this.activeStrategy instanceof FastSpotMicroListener fastListener) {
            this.spotStreamProcessor.getFastSpotMicroIndicator().addListener(fastListener);
        }
        this.clobWsClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.polyBoltClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.binanceConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.krakenConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.coinbaseConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
    }

    /**
     * Resolves Polymarket credentials via {@link PolymarketSecretsResolver} and initializes the
     * {@link PolymarketEip712Signer}. Fails fast with {@link IllegalStateException} if credentials are invalid.
     */
    public synchronized void initializeCredentialsAndSigner() {
        try {
            PolymarketCredentials resolved = secretsResolver.resolveCredentials();
            this.credentials = resolved;
            if (resolved.signerKeyHex() != null && !resolved.signerKeyHex().isBlank()) {
                this.eip712Signer = new PolymarketEip712Signer(resolved.signerKeyHex());
            }
            if (resolved.hasL2Credentials()) {
                polyBoltClient.configureCredentials(resolved.apiKey(), resolved.secret(), resolved.passphrase());
            }
            Log.infof("Initialized Polymarket credentials and EIP-712 signer (makerAddress=%s, signerAddress=%s)",
                    resolved.makerAddress(), resolved.signerAddress());
        } catch (Exception e) {
            Log.fatalf("Fatal configuration error: failed to initialize Polymarket credentials for live trading: %s",
                    e.getMessage());
            throw new IllegalStateException("Failed to resolve Polymarket credentials for live trading", e);
        }
    }

    /**
     * Starts the engine according to the currently configured (or restored) {@link StrategyMode}.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void startConfiguredMode(long nowEpochSec) {
        StrategyMode mode = this.strategyMode;
        if (mode == StrategyMode.LIVE) {
            initializeCredentialsAndSigner();
            if (streamsEnabled) {
                syncAccountBalanceFromClob();
            }
        }
        Log.infof("Initializing StrategyTradingEngine in mode=%s with strategy=%s (symbol=%s, timeframe=%s, durationSec=%d, orderType=%s, maxQuoteAgeMs=%d, orderLatencyMs=%d, initialCapital=$%.2f, currentBalance=$%.2f)",
                mode, activeStrategy.getName(), symbol, timeframe, durationSeconds, orderType, maxQuoteAgeMs, orderLatencyMs,
                getInitialCapital(), getCurrentBalance());

        if (mode == StrategyMode.OFF) {
            stopStrategyStreams();
            return;
        }
        if (streamsEnabled) {
            startStrategyStreams(nowEpochSec);
        }
    }

    /**
     * Switches the runtime {@link StrategyMode} ({@code LIVE}, {@code PAPER}, or {@code OFF}), starting or stopping
     * strategy worker threads and WebSocket subscriptions accordingly while leaving Data Collector untouched.
     *
     * @param targetMode  target {@link StrategyMode}
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void setStrategyMode(StrategyMode targetMode, long nowEpochSec) {
        if (targetMode == null) {
            throw new IllegalArgumentException("targetMode cannot be null");
        }
        StrategyMode previousMode = this.strategyMode;
        if (previousMode == targetMode) {
            if (targetMode == StrategyMode.OFF && workersRunning.get()) {
                stopStrategyStreams();
            } else if (targetMode != StrategyMode.OFF && streamsEnabled && !workersRunning.get()) {
                if (targetMode == StrategyMode.LIVE) {
                    initializeCredentialsAndSigner();
                    syncAccountBalanceFromClob();
                }
                startStrategyStreams(nowEpochSec);
            }
            return;
        }

        if (targetMode == StrategyMode.OFF) {
            Log.infof("Switching StrategyTradingEngine mode %s -> OFF (stopping strategy threads and streams)...", previousMode);
            this.strategyMode = StrategyMode.OFF;
            stopStrategyStreams();
            stateStore.save(captureStateSnapshot());
            return;
        }

        if (targetMode == StrategyMode.LIVE) {
            initializeCredentialsAndSigner();
            if (streamsEnabled) {
                syncAccountBalanceFromClob();
            }
        }

        Log.infof("Switching StrategyTradingEngine mode %s -> %s...", previousMode, targetMode);
        long currentCycleStartSec = PolymarketMarketInfo.alignCycleStart(nowEpochSec, durationSeconds);
        synchronized (strategyLock) {
            openPositions.clear();
            activePosition.set(null);
            orderExecutionQueue.clear();
            executionReportQueue.clear();
            activeStrategy.reset();
            if (activeCandleStart.get() > 0L && activeCandleStart.get() != currentCycleStartSec) {
                activeCandleStart.set(0L);
                activeCandleEnd.set(0L);
                twapOpenPrice = 0.0;
                candleHigh = 0.0;
                candleLow = 0.0;
            } else {
                replayActiveCandleTargetPriceIfPresent();
            }
        }

        this.strategyMode = targetMode;
        if (streamsEnabled) {
            if (!workersRunning.get()) {
                startStrategyStreams(nowEpochSec);
            } else if (targetMode == StrategyMode.LIVE) {
                ensureLiveUserWorkerAndStreamRunning();
            } else if (targetMode == StrategyMode.PAPER) {
                stopLiveUserWorkerAndStream();
            }
        }
        stateStore.save(captureStateSnapshot());
    }

    /**
     * Starts the CPU-pinned strategy worker threads and binds market streams to {@code market-strategy-worker}
     * ({@code CPU #2}) plus the authenticated CLOB User stream to {@code clob-user-worker} ({@code CPU #0}) in {@code LIVE} mode.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void startStrategyStreams(long nowEpochSec) {
        StrategyMode mode = this.strategyMode;
        if (mode == StrategyMode.OFF) {
            return;
        }
        Log.infof("Starting 4-core strategy pipeline (%s) and WebSocket streams for symbol=%s, timeframe=%s...",
                mode, symbol, timeframe);
        if (workersRunning.compareAndSet(false, true)) {
            if (mode == StrategyMode.LIVE) {
                Thread userThread = Thread.ofPlatform()
                        .daemon(true)
                        .priority(Thread.NORM_PRIORITY)
                        .name("clob-user-worker")
                        .unstarted(this::clobUserWorkerLoop);
                this.clobUserWorkerThread = userThread;
                userThread.start();
            }
            Thread stratThread = Thread.ofPlatform()
                    .daemon(true)
                    .priority(Thread.MAX_PRIORITY)
                    .name("market-strategy-worker")
                    .unstarted(this::marketStrategyWorkerLoop);
            Thread execThread = Thread.ofPlatform()
                    .daemon(true)
                    .priority(Thread.MAX_PRIORITY)
                    .name("order-execution-io-worker")
                    .unstarted(this::orderExecutionWorkerLoop);
            Thread maintThread = Thread.ofPlatform()
                    .daemon(true)
                    .priority(Thread.MIN_PRIORITY)
                    .name("telemetry-maintenance-worker")
                    .unstarted(this::telemetryMaintenanceLoop);
            this.marketStrategyThread = stratThread;
            this.orderExecutionWorkerThread = execThread;
            this.telemetryMaintenanceThread = maintThread;
            stratThread.start();
            execThread.start();
            maintThread.start();
        }

        polyBoltClient.attachStrategyConsumer(new RawWebSocketListener() {
            @Override
            public void onMessage(String rawJson, long recvTs) {
                onRawPolyBoltFrame(rawJson, recvTs);
            }

            @Override
            public void onMessageBytes(TimestampedFrame frame) {
                if (frame != null) {
                    onRawPolyBoltFrame(frame.rawText(), frame.recvTs());
                }
            }
        }, this.marketStrategyThread);

        if (!polyBoltClient.isStreamsActive() && polyBoltClient.hasCredentials()) {
            polyBoltClient.startCollectorStreams(symbol, null, null);
            this.polyBoltStartedByEngine = true;
        }

        binanceConnector.startSpotAggTradesBatch(symbol, this::onRawBinanceFrameBatch, this.marketStrategyThread);
        krakenConnector.start(symbol, true, new KrakenStreamListener() {
            @Override
            public void onStreamMessage(String normSymbol, String rawJson, long recvTimestampMs) {
                onRawKrakenFrame(normSymbol, rawJson, recvTimestampMs);
            }

            @Override
            public void onStreamFrame(String normSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                onRawKrakenFrameBytes(normSymbol, rawUtf8Bytes, recvTimestampMs);
            }
        }, this.marketStrategyThread);
        coinbaseConnector.start(symbol, new CoinbaseStreamListener() {
            @Override
            public void onStreamMessage(String normSymbol, String rawJson, long recvTimestampMs) {
                onRawCoinbaseFrame(normSymbol, rawJson, recvTimestampMs);
            }

            @Override
            public void onStreamFrame(String normSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                onRawCoinbaseFrameBytes(normSymbol, rawUtf8Bytes, recvTimestampMs);
            }
        }, this.marketStrategyThread);

        if (mode == StrategyMode.LIVE && credentials != null && credentials.hasL2Credentials()) {
            clobWsClient.openUserStream(credentials, this::onRawUserFrame, this.clobUserWorkerThread);
        }
        discoverAndRotateMarket(nowEpochSec);
    }

    /**
     * Stops all strategy worker threads and disconnects strategy-owned WebSocket streams while leaving
     * Data Collector PolyBolt, CLOB, and Binance {@code bookTicker} streams untouched.
     */
    public synchronized void stopStrategyStreams() {
        workersRunning.set(false);
        polyBoltClient.detachStrategyConsumer();
        if (polyBoltStartedByEngine) {
            polyBoltStartedByEngine = false;
            polyBoltClient.stopCollectorStreams();
        }
        clobWsClient.stopAllStrategyStreams();
        subscribedStrategyMarketSlug = null;
        binanceConnector.stopSpotAggTrades();
        krakenConnector.stop();
        coinbaseConnector.stop();
        orderExecutionQueue.clear();
        executionReportQueue.clear();
        stagedQuoteSlot.set(null);

        Thread t0 = this.clobUserWorkerThread;
        Thread t2 = this.marketStrategyThread;
        Thread t3 = this.orderExecutionWorkerThread;
        Thread tm = this.telemetryMaintenanceThread;
        unparkAllWorkers();
        joinQuietly(t0);
        joinQuietly(t2);
        joinQuietly(t3);
        joinQuietly(tm);
        clobUserWorkerThread = null;
        marketStrategyThread = null;
        orderExecutionWorkerThread = null;
        telemetryMaintenanceThread = null;
    }

    private synchronized void ensureLiveUserWorkerAndStreamRunning() {
        if (!workersRunning.get()) {
            return;
        }
        if (this.clobUserWorkerThread == null || !this.clobUserWorkerThread.isAlive()) {
            Thread userThread = Thread.ofPlatform()
                    .daemon(true)
                    .priority(Thread.NORM_PRIORITY)
                    .name("clob-user-worker")
                    .unstarted(this::clobUserWorkerLoop);
            this.clobUserWorkerThread = userThread;
            userThread.start();
        }
        if (credentials != null && credentials.hasL2Credentials()) {
            clobWsClient.openUserStream(credentials, this::onRawUserFrame, this.clobUserWorkerThread);
        }
    }

    private synchronized void stopLiveUserWorkerAndStream() {
        clobWsClient.closeUserStream();
        Thread t0 = this.clobUserWorkerThread;
        this.clobUserWorkerThread = null;
        if (t0 != null) {
            LockSupport.unpark(t0);
            joinQuietly(t0);
        }
    }

    private static void joinQuietly(Thread thread) {
        if (thread == null || thread == Thread.currentThread()) {
            return;
        }
        try {
            thread.join(1_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Captures an immutable {@link RuntimeStateSnapshot} of the current active market target price,
     * {@link StrategyMode}, balances, and open positions for seamless recovery across deployments.
     *
     * @return current {@link RuntimeStateSnapshot}
     */
    public RuntimeStateSnapshot captureStateSnapshot() {
        PolymarketMarketInfo market = activeMarketInfo.get();
        return new RuntimeStateSnapshot(
                System.currentTimeMillis(),
                symbol,
                timeframe,
                strategyMode,
                activeCandleStart.get(),
                activeCandleEnd.get(),
                twapOpenPrice,
                market != null ? market.slug() : "",
                market != null ? market.conditionId() : "",
                market != null ? market.yesTokenId() : "",
                market != null ? market.noTokenId() : "",
                lastTwapOracleTimestampMs.get(),
                lastCryptoPriceOracleTimestampMs.get(),
                currentSpotPrice,
                candleHigh,
                candleLow,
                getInitialCapital(),
                getCurrentBalance(),
                List.copyOf(openPositions)
        );
    }

    /**
     * Restores persisted runtime state from {@link RuntimeStateSnapshot} during application startup.
     * Always restores {@link StrategyMode}; restores market target price ({@code twapOpenPrice}),
     * market metadata, and open positions only when {@code snapshot.activeCandleStartSec()} matches
     * the currently active market interval at {@code nowEpochSec}.
     *
     * @param snapshot    loaded runtime state snapshot
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void restoreStateSnapshot(RuntimeStateSnapshot snapshot, long nowEpochSec) {
        if (snapshot == null) {
            return;
        }
        if (snapshot.strategyMode() != null) {
            this.strategyMode = snapshot.strategyMode();
        }
        long currentCycleStartSec = PolymarketMarketInfo.alignCycleStart(nowEpochSec, durationSeconds);
        long savedEndSec = snapshot.activeCandleEndSec() > snapshot.activeCandleStartSec()
                ? snapshot.activeCandleEndSec()
                : (snapshot.activeCandleStartSec() + durationSeconds);

        if (snapshot.activeCandleStartSec() <= 0L
                || snapshot.activeCandleStartSec() != currentCycleStartSec
                || nowEpochSec >= savedEndSec) {
            Log.infof("Loaded RuntimeStateSnapshot mode=%s; skipping expired candle state (savedCandleStart=%d, currentCycleStart=%d)",
                    this.strategyMode, snapshot.activeCandleStartSec(), currentCycleStartSec);
            return;
        }

        synchronized (strategyLock) {
            activeCandleStart.set(snapshot.activeCandleStartSec());
            activeCandleEnd.set(savedEndSec);
            twapOpenPrice = snapshot.twapOpenPrice();
            currentSpotPrice = snapshot.currentSpotPrice();
            candleHigh = snapshot.candleHigh();
            candleLow = snapshot.candleLow();
            lastTwapOracleTimestampMs.set(snapshot.lastTwapOracleTimestampMs());
            lastCryptoPriceOracleTimestampMs.set(snapshot.lastCryptoPriceOracleTimestampMs());

            if (!snapshot.activeMarketSlug().isBlank()
                    && !snapshot.yesTokenId().isBlank()
                    && !snapshot.noTokenId().isBlank()) {
                PolymarketMarketInfo restoredMarket = new PolymarketMarketInfo(
                        snapshot.activeMarketSlug(),
                        snapshot.conditionId(),
                        snapshot.activeCandleStartSec(),
                        savedEndSec,
                        List.of(snapshot.yesTokenId(), snapshot.noTokenId()),
                        false,
                        null
                );
                setActiveMarketInfo(restoredMarket);
            }

            if (this.strategyMode == StrategyMode.LIVE) {
                if (snapshot.initialCapital() > 0.0 && this.liveInitialCapital <= 0.0) {
                    this.liveInitialCapital = snapshot.initialCapital();
                }
                if (snapshot.currentBalance() > 0.0 && this.liveCurrentBalance <= 0.0) {
                    this.liveCurrentBalance = snapshot.currentBalance();
                }
            }

            openPositions.clear();
            if (!snapshot.openPositions().isEmpty()) {
                openPositions.addAll(snapshot.openPositions());
                activePosition.set(openPositions.getLast());
            } else {
                activePosition.set(null);
            }

            replayActiveCandleTargetPriceIfPresent();
            if (snapshot.currentSpotPrice() > 0.0) {
                long spotTsMs = Math.max(snapshot.activeCandleStartSec() * 1000L + 1000L, snapshot.savedAtTimestampMs());
                if (spotTsMs < savedEndSec * 1000L) {
                    String polyBoltSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(this.symbol);
                    activeStrategy.onPolymarketCryptoPrice(
                            spotTsMs,
                            polyBoltSymbol,
                            "restored-state",
                            snapshot.currentSpotPrice(),
                            spotTsMs
                    );
                }
            }
        }
        Log.infof("Restored active candle [%d, %d] from RuntimeStateSnapshot: mode=%s, slug=%s, twapOpenPrice=$%.2f, openPositions=%d",
                snapshot.activeCandleStartSec(), savedEndSec, this.strategyMode,
                snapshot.activeMarketSlug(), snapshot.twapOpenPrice(), openPositions.size());
    }

    private void replayActiveCandleTargetPriceIfPresent() {
        long startSec = activeCandleStart.get();
        double target = this.twapOpenPrice;
        if (startSec > 0L && target > 0.0) {
            long boundaryTsMs = startSec * 1000L;
            String polyBoltSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(this.symbol);
            activeStrategy.onPolymarketCryptoTwap60s(
                    boundaryTsMs,
                    polyBoltSymbol,
                    "restored-state",
                    target,
                    60,
                    boundaryTsMs
            );
        }
    }

    /**
     * Dedicated loop for vCPU #0 ({@code clob-user-worker}): drains authenticated {@code POLYMARKET-CLOB-USER}
     * trade balance updates without head-of-line blocking from synchronous {@code POST /order} HTTP execution.
     */
    private void clobUserWorkerLoop() {
        LinuxCpuAffinity.pinCurrentThreadIfSupported(0, "vCPU #0: User WS & Control");
        List<TimestampedFrame> scratchBatch = new ArrayList<>(32);
        while (workersRunning.get() && this.clobUserWorkerThread == Thread.currentThread()) {
            long startNs = System.nanoTime();
            int userDrained = clobWsClient.drainUserStream(scratchBatch);
            if (userDrained > 0) {
                userWorkerBusyNs.addAndGet(Math.max(0L, System.nanoTime() - startNs));
                continue;
            }
            ReconnectingWebSocketStream.spinWaitBeforePark(
                    () -> !workersRunning.get()
                            || this.clobUserWorkerThread != Thread.currentThread()
                            || clobWsClient.hasQueuedUserFrames());
            if (!workersRunning.get()
                    || this.clobUserWorkerThread != Thread.currentThread()
                    || clobWsClient.hasQueuedUserFrames()) {
                continue;
            }
            if (clobWsClient.prepareUserStreamToPark()) {
                continue;
            }
            if (!workersRunning.get() || this.clobUserWorkerThread != Thread.currentThread()) {
                break;
            }
            LockSupport.parkNanos(this, ReconnectingWebSocketStream.IDLE_PARK_TIMEOUT_NS);
        }
    }

    /**
     * Fused single-core L1-cache loop for vCPU #2 ({@code market-strategy-worker}): processes execution reports,
     * CLOB L2 orderbook batches, PolyBolt oracle ticks, and Binance/Kraken/Coinbase spot trades, then immediately
     * evaluates the latest conflated {@link OrderBookQuote} on {@link TradingStrategy} without cross-thread handoff.
     */
    private void marketStrategyWorkerLoop() {
        LinuxCpuAffinity.pinCurrentThreadIfSupported(2, "vCPU #2: Book & Strategy");
        List<TimestampedFrame> scratchBatch = new ArrayList<>(64);
        while (workersRunning.get()) {
            boolean didWork = false;
            long startNs = System.nanoTime();

            ExecutionReport report;
            while ((report = executionReportQueue.poll()) != null) {
                didWork = true;
                synchronized (strategyLock) {
                    activeStrategy.onExecutionReport(report);
                }
            }

            if (clobWsClient.drainMarketStreams(scratchBatch) > 0) {
                didWork = true;
            }

            if (polyBoltClient.drainStreams(scratchBatch) > 0) {
                didWork = true;
            }

            if (binanceConnector.drainStreams(scratchBatch) > 0) {
                didWork = true;
            }

            if (krakenConnector.drainStreams(scratchBatch) > 0) {
                didWork = true;
            }

            if (coinbaseConnector.drainStream(scratchBatch) > 0) {
                didWork = true;
            }

            OrderBookQuote quote = stagedQuoteSlot.getAndSet(null);
            if (quote != null) {
                didWork = true;
                evaluateQuoteWithFreshnessGuard(quote, true);
            }

            if (didWork) {
                marketStrategyWorkerBusyNs.addAndGet(Math.max(0L, System.nanoTime() - startNs));
                continue;
            }

            ReconnectingWebSocketStream.spinWaitBeforePark(() -> !workersRunning.get()
                    || !executionReportQueue.isEmpty()
                    || clobWsClient.hasQueuedMarketFrames()
                    || polyBoltClient.hasQueuedFrames()
                    || binanceConnector.hasQueuedFrames()
                    || krakenConnector.hasQueuedFrames()
                    || coinbaseConnector.hasQueuedFrames()
                    || stagedQuoteSlot.get() != null);
            if (!workersRunning.get()
                    || !executionReportQueue.isEmpty()
                    || clobWsClient.hasQueuedMarketFrames()
                    || polyBoltClient.hasQueuedFrames()
                    || binanceConnector.hasQueuedFrames()
                    || krakenConnector.hasQueuedFrames()
                    || coinbaseConnector.hasQueuedFrames()
                    || stagedQuoteSlot.get() != null) {
                continue;
            }
            if (clobWsClient.prepareMarketStreamsToPark()
                    || polyBoltClient.prepareStreamsToPark()
                    || binanceConnector.prepareStreamsToPark()
                    || krakenConnector.prepareStreamsToPark()
                    || coinbaseConnector.prepareStreamToPark()) {
                continue;
            }
            if (!workersRunning.get()
                    || !executionReportQueue.isEmpty()
                    || stagedQuoteSlot.get() != null) {
                continue;
            }
            LockSupport.parkNanos(this, ReconnectingWebSocketStream.IDLE_PARK_TIMEOUT_NS);
        }
    }

    /**
     * Dedicated isolated loop for vCPU #3 ({@code order-execution-io-worker}): exclusively executes EIP-712 signing
     * and HTTP {@code POST /order} / {@code DELETE /order} calls without interference from WebSocket streams, GC, or IRQs.
     */
    private void orderExecutionWorkerLoop() {
        LinuxCpuAffinity.pinCurrentThreadIfSupported(3, "vCPU #3: Order Execution");
        while (workersRunning.get()) {
            boolean didWork = false;
            long startNs = System.nanoTime();

            OrderCommand cmd;
            while ((cmd = orderExecutionQueue.poll()) != null) {
                didWork = true;
                executeOrderCommandInternal(cmd, true);
            }

            if (didWork) {
                orderExecutionWorkerBusyNs.addAndGet(Math.max(0L, System.nanoTime() - startNs));
                continue;
            }

            ReconnectingWebSocketStream.spinWaitBeforePark(
                    () -> !workersRunning.get() || !orderExecutionQueue.isEmpty());
            if (!workersRunning.get() || !orderExecutionQueue.isEmpty()) {
                continue;
            }
            LockSupport.parkNanos(this, ReconnectingWebSocketStream.IDLE_PARK_TIMEOUT_NS);
        }
    }

    /**
     * Low-priority background maintenance loop on vCPU #0 for 1s `/proc/net/tcp` telemetry sampling,
     * pre-activation `GET /balance-allowance` REST synchronization, and off-hot-path {@link RuntimeStateSnapshot} backup.
     */
    private void telemetryMaintenanceLoop() {
        LinuxCpuAffinity.pinCurrentThreadIfSupported(0, "vCPU #0: Telemetry Maintenance");
        long lastTelemetrySampleNs = 0L;
        long lastHistoryRecordNs = 0L;
        StrategyMode lastSavedMode = null;
        long lastSavedCandleStart = -1L;
        long lastSavedTwapBits = Long.MIN_VALUE;
        int lastSavedOpenPositions = -1;
        String lastSavedSlug = null;

        while (workersRunning.get()) {
            if (balanceSyncRequested.compareAndSet(true, false)) {
                try {
                    if (this.strategyMode == StrategyMode.LIVE) {
                        syncAccountBalanceFromClob();
                    }
                } finally {
                    balanceSyncInFlight.set(false);
                }
            }

            long nowNs = System.nanoTime();
            if (nowNs - lastTelemetrySampleNs >= 1_000_000_000L) {
                lastTelemetrySampleNs = nowNs;
                SystemTelemetrySnapshot sysSnap = telemetryMonitor.sample(buildMonitoredThreadSpecs());
                if (lastHistoryRecordNs == 0L || nowNs - lastHistoryRecordNs >= 10_000_000_000L) {
                    lastHistoryRecordNs = nowNs;
                    recordTelemetryHistorySnapshot(sysSnap, clobWsClient.getStreamTelemetries());
                }

                StrategyMode curMode = this.strategyMode;
                long curCandleStart = activeCandleStart.get();
                long curTwapBits = Double.doubleToLongBits(this.twapOpenPrice);
                int curPosCount = openPositions.size();
                PolymarketMarketInfo curMarket = activeMarketInfo.get();
                String curSlug = curMarket != null ? curMarket.slug() : "";
                if (curMode != lastSavedMode
                        || curCandleStart != lastSavedCandleStart
                        || curTwapBits != lastSavedTwapBits
                        || curPosCount != lastSavedOpenPositions
                        || !Objects.equals(curSlug, lastSavedSlug)) {
                    lastSavedMode = curMode;
                    lastSavedCandleStart = curCandleStart;
                    lastSavedTwapBits = curTwapBits;
                    lastSavedOpenPositions = curPosCount;
                    lastSavedSlug = curSlug;
                    stateStore.save(captureStateSnapshot());
                }
            }

            if (!workersRunning.get() || balanceSyncRequested.get()) {
                continue;
            }
            LockSupport.parkNanos(this, 100_000_000L);
        }
    }

    private void unparkAllWorkers() {
        Thread t0 = this.clobUserWorkerThread;
        if (t0 != null) {
            LockSupport.unpark(t0);
        }
        Thread t2 = this.marketStrategyThread;
        if (t2 != null) {
            LockSupport.unpark(t2);
        }
        Thread t3 = this.orderExecutionWorkerThread;
        if (t3 != null) {
            LockSupport.unpark(t3);
        }
        Thread tm = this.telemetryMaintenanceThread;
        if (tm != null) {
            LockSupport.unpark(tm);
        }
    }

    /**
     * Processes a raw JSON frame from the authenticated Polymarket CLOB User WebSocket channel ({@code /ws/user})
     * and updates the in-memory live account balance upon each incoming {@code BUY} or {@code SELL} trade event.
     *
     * @param rawJson         raw User channel JSON frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawUserFrame(String rawJson, long recvTimestampMs) {
        if (rawJson == null || rawJson.isBlank() || "PONG".equalsIgnoreCase(rawJson)) {
            return;
        }
        if (Log.isDebugEnabled()) {
            Log.debugf("Received Polymarket CLOB user channel frame at %d ms: %s", recvTimestampMs, rawJson);
        }
        List<PolymarketUserTradeEvent> tradeEvents = streamParser.parseUserTradeEvents(rawJson);
        for (PolymarketUserTradeEvent event : tradeEvents) {
            applyUserTradeEvent(event);
        }
    }

    private void applyUserTradeEvent(PolymarketUserTradeEvent event) {
        String status = event.status() != null ? event.status().trim() : "";
        String tradeId = event.tradeId() != null ? event.tradeId().trim() : "";

        if ("FAILED".equalsIgnoreCase(status)) {
            if (!tradeId.isEmpty()) {
                Double previousDelta = removeAppliedTradeDelta(tradeId);
                if (previousDelta != null && previousDelta != 0.0) {
                    applyAccountBalanceDelta(-previousDelta);
                    Log.warnf("Reverted live balance delta $%.2f for FAILED WS user trade %s: balance=$%.2f",
                            previousDelta, tradeId, liveCurrentBalance);
                }
            }
            return;
        }
        if ("RETRYING".equalsIgnoreCase(status)) {
            return;
        }

        double size = event.size();
        double price = event.price();
        String side = event.side() != null ? event.side().trim() : "";
        if (size <= 0.0 || price <= 0.0 || (!"BUY".equalsIgnoreCase(side) && !"SELL".equalsIgnoreCase(side))) {
            return;
        }

        double notionalUsd = size * price;
        double feeRate = event.feeRateBps() >= 0 ? (event.feeRateBps() / 10_000.0) : TAKER_FEE_RATE;
        double feeUsd = (price > 0.0 && price < 1.0)
                ? (size * feeRate * price * (1.0 - price))
                : 0.0;
        double signedDeltaUsd = "BUY".equalsIgnoreCase(side)
                ? -(notionalUsd + feeUsd)
                : (notionalUsd - feeUsd);

        if (!tradeId.isEmpty() && !recordAppliedTradeDeltaIfAbsent(tradeId, signedDeltaUsd)) {
            return;
        }

        applyAccountBalanceDelta(signedDeltaUsd);
        Log.infof("Updated live balance from WS user trade %s (%s %.2f shares @ %.4f, fee=$%.4f, delta=$%.2f): balance=$%.2f",
                tradeId, side.toUpperCase(Locale.ROOT), size, price, feeUsd, signedDeltaUsd, liveCurrentBalance);
    }

    private synchronized boolean recordAppliedTradeDeltaIfAbsent(String tradeId, double signedDeltaUsd) {
        if (appliedUserTradeDeltas.containsKey(tradeId)) {
            return false;
        }
        appliedUserTradeDeltas.put(tradeId, signedDeltaUsd);
        return true;
    }

    private synchronized Double removeAppliedTradeDelta(String tradeId) {
        return appliedUserTradeDeltas.remove(tradeId);
    }

    /**
     * Synchronizes the in-memory live balance with the authoritative Polymarket CLOB collateral USDC balance.
     *
     * @param accountBalanceUsd available Polymarket account USDC collateral balance in USD
     */
    public synchronized void syncAccountBalance(double accountBalanceUsd) {
        if (!Double.isFinite(accountBalanceUsd) || accountBalanceUsd < 0.0) {
            throw new IllegalArgumentException("accountBalanceUsd must be finite and non-negative");
        }
        this.liveCurrentBalance = accountBalanceUsd;
        if (this.liveInitialCapital <= 0.0) {
            this.liveInitialCapital = accountBalanceUsd;
        }
    }

    /**
     * Applies a signed balance delta to the in-memory live account balance.
     *
     * @param deltaUsd signed USD balance change
     */
    public synchronized void applyAccountBalanceDelta(double deltaUsd) {
        if (Double.isFinite(deltaUsd) && deltaUsd != 0.0) {
            this.liveCurrentBalance = Math.max(0.0, this.liveCurrentBalance + deltaUsd);
        }
    }

    /**
     * Fetches the authoritative Polymarket CLOB collateral USDC balance via REST and updates in-memory state.
     */
    public void syncAccountBalanceFromClob() {
        try {
            double collateralUsd = clobRestClient.getCollateralBalanceUsd();
            syncAccountBalance(collateralUsd);
            Log.infof("Synchronized Polymarket live account collateral balance: $%.2f (initialCapital=$%.2f)",
                    liveCurrentBalance, liveInitialCapital);
        } catch (Exception e) {
            Log.warnf("Failed to synchronize Polymarket collateral balance from CLOB REST API: %s", e.getMessage());
        }
    }

    @Override
    public void refreshAccountBalance() {
        if (this.strategyMode != StrategyMode.LIVE) {
            return;
        }
        if (!balanceSyncInFlight.compareAndSet(false, true)) {
            return;
        }
        Thread maintWorker = this.telemetryMaintenanceThread;
        if (workersRunning.get() && maintWorker != null) {
            balanceSyncRequested.set(true);
            LockSupport.unpark(maintWorker);
        } else {
            Thread.ofPlatform()
                    .daemon(true)
                    .name("polymarket-balance-sync")
                    .start(() -> {
                        try {
                            syncAccountBalanceFromClob();
                        } finally {
                            balanceSyncInFlight.set(false);
                        }
                    });
        }
    }

    /**
     * Periodically checks for 15m market cycle boundary transitions and rotates the active strategy CLOB market subscription.
     */
    @Scheduled(every = "${polymarket.trading.discovery-interval:5s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void scheduledMarketRollover() {
        if (!streamsEnabled || this.strategyMode == StrategyMode.OFF || !workersRunning.get() || this.marketStrategyThread == null) {
            return;
        }
        discoverAndRotateMarket(Instant.now().getEpochSecond());
    }

    /**
     * Periodically sends a {@code GET /time} keepalive heartbeat to the Polymarket CLOB REST API in {@code LIVE} mode.
     */
    @Scheduled(every = "${polymarket.trading.keepalive-interval:15s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void scheduledKeepalive() {
        if (!streamsEnabled || this.strategyMode != StrategyMode.LIVE || !workersRunning.get() || this.marketStrategyThread == null) {
            return;
        }
        clobRestClient.pingKeepalive();
    }

    /**
     * Discovers the active market for the cycle containing {@code nowEpochSec} via {@code GET /markets/slug/{slug}}
     * and rotates the dual Strategy CLOB WebSocket subscription if the cycle has changed or is not yet subscribed.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     * @return discovered {@link PolymarketMarketInfo} if found
     */
    public Optional<PolymarketMarketInfo> discoverAndRotateMarket(long nowEpochSec) {
        long cycleStartSec = PolymarketMarketInfo.alignCycleStart(nowEpochSec, durationSeconds);
        String expectedSlug = PolymarketMarketInfo.buildMarketSlug(symbol, timeframe, cycleStartSec);

        PolymarketMarketInfo current = activeMarketInfo.get();
        if (current != null && expectedSlug.equals(current.slug())) {
            if (!expectedSlug.equals(this.subscribedStrategyMarketSlug)) {
                openStrategyClobStreamForMarket(current);
            }
            long nextCycleStartSec = cycleStartSec + durationSeconds;
            if (nowEpochSec >= nextCycleStartSec - MARKET_PREFETCH_LOOKAHEAD_SEC) {
                prefetchUpcomingMarket(nextCycleStartSec);
            }
            return Optional.of(current);
        }

        Optional<PolymarketMarketInfo> discovered;
        PolymarketMarketInfo prefetched = prefetchedNextMarketInfo.get();
        if (prefetched != null && expectedSlug.equals(prefetched.slug())) {
            prefetchedNextMarketInfo.compareAndSet(prefetched, null);
            discovered = Optional.of(prefetched);
        } else {
            discovered = gammaClient.fetchMarketBySlug(expectedSlug, durationSeconds);
        }

        if (discovered.isEmpty()) {
            if (Log.isDebugEnabled()) {
                Log.debugf("Market %s not yet available on Gamma API", expectedSlug);
            }
            return Optional.empty();
        }

        PolymarketMarketInfo nextMarket = discovered.get();
        String previousSlugToClose = null;
        synchronized (this) {
            PolymarketMarketInfo latestCurrent = activeMarketInfo.get();
            if (latestCurrent != null && nextMarket.slug().equals(latestCurrent.slug())) {
                if (!nextMarket.slug().equals(this.subscribedStrategyMarketSlug)) {
                    openStrategyClobStreamForMarket(latestCurrent);
                }
                return Optional.of(latestCurrent);
            }
            if (latestCurrent != null && !latestCurrent.slug().equals(nextMarket.slug())) {
                evaluateCompletedMarketStreamActivity(latestCurrent.slug(), latestCurrent.startSec());
                previousSlugToClose = latestCurrent.slug();
            }

            activeMarketInfo.set(nextMarket);
            if (activeCandleStart.compareAndSet(0L, nextMarket.startSec())) {
                activeCandleEnd.set(nextMarket.endSec());
            }
            activeLocalBook.set(new PolymarketLocalBook(nextMarket.yesTokenId(), nextMarket.noTokenId()));
            this.latestQuote = null;
            this.stagedQuoteSlot.set(null);
        }
        if (previousSlugToClose != null) {
            clobWsClient.closeMarketStream(previousSlugToClose);
        }
        openStrategyClobStreamForMarket(nextMarket);

        long nextCycleStartSec = cycleStartSec + durationSeconds;
        if (nowEpochSec >= nextCycleStartSec - MARKET_PREFETCH_LOOKAHEAD_SEC) {
            prefetchUpcomingMarket(nextCycleStartSec);
        }

        Log.infof("Subscribed Strategy (%s) to Polymarket market %s (conditionId=%s, yesToken=%s, noToken=%s)",
                this.strategyMode, nextMarket.slug(), nextMarket.conditionId(), nextMarket.yesTokenId(), nextMarket.noTokenId());
        return Optional.of(nextMarket);
    }

    private synchronized void openStrategyClobStreamForMarket(PolymarketMarketInfo market) {
        if (market == null || this.strategyMode == StrategyMode.OFF) {
            return;
        }
        clobWsClient.openMarketStreamBatch(
                market.slug(),
                market.clobTokenIds(),
                this::onRawClobFrameBatch,
                this.marketStrategyThread
        );
        this.subscribedStrategyMarketSlug = market.slug();
        Thread stratWorker = this.marketStrategyThread;
        if (stratWorker != null) {
            LockSupport.unpark(stratWorker);
        }
    }

    private void prefetchUpcomingMarket(long nextCycleStartSec) {
        String nextSlug = PolymarketMarketInfo.buildMarketSlug(symbol, timeframe, nextCycleStartSec);
        PolymarketMarketInfo existing = prefetchedNextMarketInfo.get();
        if (existing != null && nextSlug.equals(existing.slug())) {
            return;
        }
        Optional<PolymarketMarketInfo> fetched = gammaClient.fetchMarketBySlug(nextSlug, durationSeconds);
        if (fetched.isPresent()) {
            PolymarketMarketInfo nextInfo = fetched.get();
            prefetchedNextMarketInfo.set(nextInfo);
            Log.infof("Pre-fetched upcoming Polymarket market %s (yesToken=%s, noToken=%s) ahead of cycle rollover",
                    nextInfo.slug(), nextInfo.yesTokenId(), nextInfo.noTokenId());
        }
    }

    private void evaluateCompletedMarketStreamActivity(String marketLabel, long marketStartSec) {
        if (marketStartSec > 0L && lastEvaluatedMarketCycleStart.getAndSet(marketStartSec) == marketStartSec) {
            return;
        }
        long polyBoltCount = polyBoltMarketMsgCount.getAndSet(0L);
        long clobCount = clobMarketMsgCount.getAndSet(0L);

        if (polyBoltCount == 0L || clobCount == 0L) {
            Log.errorf("Market %s completed with missing WebSocket stream messages (polyBoltMessages=%d, clobWsMessages=%d)",
                    marketLabel, polyBoltCount, clobCount);
        }
        Log.infof("Market %s summary statistics: mode=%s, polyBoltMessages=%d, clobWsMessages=%d, twapOpen=%.2f, lastSpot=%.2f, high=%.2f, low=%.2f, balance=$%.2f",
                marketLabel, strategyMode, polyBoltCount, clobCount, twapOpenPrice, currentSpotPrice, candleHigh, candleLow, getCurrentBalance());
    }

    /**
     * Processes a raw JSON frame from the authenticated PolyBolt v2 WebSocket stream.
     *
     * @param rawJson         raw PolyBolt JSON text frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawPolyBoltFrame(String rawJson, long recvTimestampMs) {
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        if (Log.isTraceEnabled()) {
            Log.tracef("Received raw PolyBolt frame at %d ms: %s", recvTimestampMs, rawJson);
        }
        List<PolyBoltTick> ticks = streamParser.parsePolyBoltFrame(rawJson, recvTimestampMs);
        if (!ticks.isEmpty()) {
            long marketTotal = polyBoltMarketMsgCount.incrementAndGet();
            if (Log.isDebugEnabled()) {
                long debugCount = debugPolyBoltBatchCounter.incrementAndGet();
                if (debugCount % DEBUG_MESSAGE_LOG_INTERVAL == 0L) {
                    PolyBoltTick last = ticks.getLast();
                    Log.debugf("Processed %d PolyBolt messages (marketMessages=%d, latestChannel=%s, latestPrice=%.2f)",
                            debugCount, marketTotal, last.isTwap() ? "price.crypto.twap" : "price.crypto", last.price());
                }
            }
        }
        for (PolyBoltTick tick : ticks) {
            if (tick.isTwap()) {
                if (!tryAdvanceOracleTimestamp(lastTwapOracleTimestampMs, tick.oracleTimestampMs())) {
                    continue;
                }
                onPolymarketCryptoTwap60s(
                        tick.timestampMs(),
                        tick.symbol(),
                        tick.source(),
                        tick.price(),
                        tick.windowSeconds(),
                        tick.oracleTimestampMs()
                );
            } else {
                if (!tryAdvanceOracleTimestamp(lastCryptoPriceOracleTimestampMs, tick.oracleTimestampMs())) {
                    continue;
                }
                onPolymarketCryptoPrice(
                        tick.timestampMs(),
                        tick.symbol(),
                        tick.source(),
                        tick.price(),
                        tick.oracleTimestampMs()
                );
            }
        }
    }

    private static boolean tryAdvanceOracleTimestamp(AtomicLong tracker, long oracleTimestampMs) {
        if (oracleTimestampMs <= 0L) {
            return true;
        }
        while (true) {
            long prev = tracker.get();
            if (oracleTimestampMs <= prev) {
                return false;
            }
            if (tracker.compareAndSet(prev, oracleTimestampMs)) {
                return true;
            }
        }
    }

    /**
     * Processes a batch of raw CLOB JSON frames on the fused {@code market-strategy-worker} thread ({@code CPU #2}),
     * fast-forwarding past superseded snapshots, mutating {@link PolymarketLocalBook} lock-free, and staging
     * only the single final {@link OrderBookQuote} in {@code stagedQuoteSlot} for immediate same-core strategy evaluation.
     *
     * @param batch chronological batch of CLOB frames drained from the inbound queue
     */
    public void onRawClobFrameBatch(List<TimestampedFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        PolymarketLocalBook localBook = activeLocalBook.get();
        if (localBook == null) {
            localBook = new PolymarketLocalBook("", "");
            activeLocalBook.compareAndSet(null, localBook);
        }
        PolymarketStreamParser.ClobBatchResult result = streamParser.applyClobFrameBatch(batch, localBook);
        if (result.fastForwardedFramesCount() > 0) {
            PolymarketMarketInfo info = activeMarketInfo.get();
            if (info != null) {
                clobWsClient.addMarketCoalescedFrames(info.slug(), result.fastForwardedFramesCount());
            }
        }
        if (result.appliedFramesCount() > 0) {
            long marketTotal = clobMarketMsgCount.addAndGet(result.appliedFramesCount());
            if (result.quote().isPresent()) {
                PolymarketBboQuote bbo = result.quote().get();
                if (Log.isDebugEnabled()) {
                    long debugCount = debugClobBatchCounter.incrementAndGet();
                    if (debugCount % DEBUG_MESSAGE_LOG_INTERVAL == 0L) {
                        Log.debugf("Processed %d CLOB WS batches (marketMessages=%d, bestBidUp=%.3f, bestAskUp=%.3f, bestBidDown=%.3f, bestAskDown=%.3f)",
                                debugCount, marketTotal, bbo.bestBidUp(), bbo.bestAskUp(), bbo.bestBidDown(), bbo.bestAskDown());
                    }
                }
                publishUpdatedBboIfChanged(bbo);
            }
        }
    }

    /**
     * Processes a single raw JSON frame from the Polymarket CLOB Market WebSocket stream.
     *
     * @param rawJson         raw CLOB JSON text frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawClobFrame(String rawJson, long recvTimestampMs) {
        if (Log.isTraceEnabled()) {
            Log.tracef("Received raw CLOB WS frame at %d ms: %s", recvTimestampMs, rawJson);
        }
        PolymarketLocalBook localBook = activeLocalBook.get();
        if (localBook == null) {
            localBook = new PolymarketLocalBook("", "");
            activeLocalBook.compareAndSet(null, localBook);
        }
        Optional<PolymarketBboQuote> bboOpt = streamParser.applyClobFrame(rawJson, recvTimestampMs, localBook);
        if (bboOpt.isPresent()) {
            clobMarketMsgCount.incrementAndGet();
            publishUpdatedBboIfChanged(bboOpt.get());
        }
    }

    private void publishUpdatedBboIfChanged(PolymarketBboQuote bbo) {
        OrderBookQuote prevQuote = this.latestQuote;
        if (prevQuote != null && isSameBbo(prevQuote, bbo)) {
            return;
        }
        OrderBookQuote nextQuote = new OrderBookQuote(
                bbo.bestAskUp(),
                bbo.bestBidUp(),
                bbo.bestAskDown(),
                bbo.bestBidDown(),
                bbo.askSizeUp(),
                bbo.askSizeDown(),
                bbo.midUp(),
                bbo.midDown(),
                bbo.timestampMs()
        );
        this.latestQuote = nextQuote;
        updatePositionObservation(nextQuote);

        Thread stratWorker = this.marketStrategyThread;
        if (workersRunning.get() && stratWorker != null && Thread.currentThread() == stratWorker) {
            OrderBookQuote overwritten = stagedQuoteSlot.getAndSet(nextQuote);
            if (overwritten != null) {
                conflatedQuotesCount.incrementAndGet();
            }
        } else {
            evaluateQuoteWithFreshnessGuard(nextQuote, false);
        }
    }

    private void updatePositionObservation(OrderBookQuote quote) {
        Position pos = activePosition.get();
        if (pos != null) {
            double currentBid = (pos.side() == TradeDirection.UP) ? quote.bestBidUp() : quote.bestBidDown();
            boolean armed = currentBid >= (pos.entryPrice() + 0.05);
            activePosition.set(pos.withPriceObservation(currentBid, armed));
        }
    }

    private static boolean isSameBbo(OrderBookQuote prev, PolymarketBboQuote next) {
        return Double.compare(prev.bestAskUp(), next.bestAskUp()) == 0
                && Double.compare(prev.bestBidUp(), next.bestBidUp()) == 0
                && Double.compare(prev.bestAskDown(), next.bestAskDown()) == 0
                && Double.compare(prev.bestBidDown(), next.bestBidDown()) == 0;
    }

    @Override
    public ExecutionRouter getExecutionRouter() {
        return this;
    }

    @Override
    public void submitOrder(OrderCommand command) {
        if (command == null) {
            return;
        }
        StrategyMode mode = this.strategyMode;
        if (mode == StrategyMode.OFF) {
            Log.warnf("Ignoring submitOrder(%s) because StrategyMode is OFF", command.clientOrderId());
            return;
        }
        if (Log.isTraceEnabled()) {
            Log.tracef("Submitting order command (%s): clientOrderId=%s, side=%s, token=%s, price=%s, size=%s",
                    mode, command.clientOrderId(), command.side(), command.token(), command.price(), command.size());
        }

        if (mode == StrategyMode.PAPER) {
            if (orderLatencyMs > 0L && vertx != null) {
                vertx.setTimer(orderLatencyMs, id -> executePaperOrderCommand(command, workersRunning.get()));
            } else {
                executePaperOrderCommand(command, false);
            }
            return;
        }

        Thread ioWorker = this.orderExecutionWorkerThread;
        if (workersRunning.get() && ioWorker != null && Thread.currentThread() == this.marketStrategyThread) {
            orderExecutionQueue.offer(command);
            LockSupport.unpark(ioWorker);
        } else {
            executeOrderCommandInternal(command, false);
        }
    }

    private void executeOrderCommandInternal(OrderCommand command, boolean asyncReportDelivery) {
        if (this.strategyMode == StrategyMode.PAPER) {
            executePaperOrderCommand(command, asyncReportDelivery);
            return;
        }
        if (this.strategyMode == StrategyMode.OFF) {
            return;
        }
        if ("BUY".equalsIgnoreCase(command.side())) {
            executeLiveBuyOrder(command, asyncReportDelivery);
        } else if ("SELL".equalsIgnoreCase(command.side())) {
            executeLiveSellOrder(command, asyncReportDelivery);
        }
    }

    private synchronized void executePaperOrderCommand(OrderCommand command, boolean asyncReportDelivery) {
        if (this.strategyMode != StrategyMode.PAPER) {
            return;
        }
        if ("BUY".equalsIgnoreCase(command.side())) {
            executePaperBuyOrder(command, asyncReportDelivery);
        } else if ("SELL".equalsIgnoreCase(command.side())) {
            executePaperSellOrder(command, asyncReportDelivery);
        }
    }

    private void deliverExecutionReport(ExecutionReport report, boolean asyncReportDelivery) {
        Thread stratWorker = this.marketStrategyThread;
        if (asyncReportDelivery && workersRunning.get() && stratWorker != null && Thread.currentThread() != stratWorker) {
            executionReportQueue.offer(report);
            LockSupport.unpark(stratWorker);
        } else {
            synchronized (strategyLock) {
                activeStrategy.onExecutionReport(report);
            }
        }
    }

    @Override
    public void cancelOrder(String clientOrderId) {
        if (clientOrderId == null || clientOrderId.isBlank()) {
            return;
        }
        StrategyMode mode = this.strategyMode;
        if (mode == StrategyMode.OFF) {
            return;
        }
        if (mode == StrategyMode.PAPER) {
            ExecutionReport report = new ExecutionReport(
                    clientOrderId,
                    "sim-cancel-" + clientOrderId,
                    "CANCELLED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    "Order cancelled in paper mode"
            );
            deliverExecutionReport(report, false);
            return;
        }

        String clobOrderId = clientOrderId;
        Position currentPos = activePosition.get();
        if (currentPos != null && clientOrderId.equals(currentPos.clientOrderId())
                && !currentPos.clobOrderId().isBlank()) {
            clobOrderId = currentPos.clobOrderId();
        }

        if (Log.isTraceEnabled()) {
            Log.tracef("Cancelling order: clientOrderId=%s, clobOrderId=%s", clientOrderId, clobOrderId);
        }

        try {
            ClobCancelResponse resp = clobRestClient.cancelOrder(clobOrderId);
            if (!resp.isCanceled(clobOrderId)) {
                Log.warnf("CLOB order %s was not canceled: %s", clobOrderId, resp.notCanceled());
            }
        } catch (Exception e) {
            Log.errorf("Failed to cancel CLOB order %s: %s", clobOrderId, e.getMessage());
        }

        ExecutionReport report = new ExecutionReport(
                clientOrderId,
                clobOrderId,
                "CANCELLED",
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                System.currentTimeMillis(),
                "Order cancelled"
        );
        deliverExecutionReport(report, false);
    }

    private void executePaperBuyOrder(OrderCommand command, boolean asyncReportDelivery) {
        ordersSubmitted.incrementAndGet();
        double requestedPrice = command.price().doubleValue();
        double shares = command.size().doubleValue();
        double fillPrice = requestedPrice;
        boolean isDown = command.token() != null && command.token().toLowerCase(Locale.ROOT).contains("down");

        OrderBookQuote quote = this.latestQuote;
        if (quote != null) {
            double bestAsk = isDown ? quote.bestAskDown() : quote.bestAskUp();
            if (bestAsk > 0.0) {
                if (bestAsk > requestedPrice + 1e-4) {
                    ordersRejected.incrementAndGet();
                    Log.warnf("Paper BUY order %s rejected: bestAsk %.4f exceeds limit price %.4f",
                            command.clientOrderId(), bestAsk, requestedPrice);
                    deliverExecutionReport(new ExecutionReport(
                            command.clientOrderId(),
                            "paper-reject-" + UUID.randomUUID(),
                            "REJECTED",
                            BigDecimal.ZERO,
                            BigDecimal.ZERO,
                            System.currentTimeMillis(),
                            "Best ask exceeds limit price"
                    ), asyncReportDelivery);
                    return;
                }
                fillPrice = bestAsk;
            }
        }

        double sizeUsd = fillPrice * shares;
        double availableBalance = paperRepository.getCurrentBalance();
        if (availableBalance < sizeUsd) {
            ordersRejected.incrementAndGet();
            Log.warnf("Insufficient virtual paper balance ($%.2f) for trade size $%.2f", availableBalance, sizeUsd);
            deliverExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "paper-reject-" + UUID.randomUUID(),
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    "Insufficient virtual balance"
            ), asyncReportDelivery);
            return;
        }

        ordersFilled.incrementAndGet();
        lastOrderRttMs.set(orderLatencyMs);
        updateMaxLong(maxOrderRttMs, orderLatencyMs);

        double entryFee = shares * TAKER_FEE_RATE * fillPrice * (1.0 - fillPrice);
        TradeDirection side = isDown ? TradeDirection.DOWN : TradeDirection.UP;
        double distTwap = currentSpotPrice - twapOpenPrice;
        double modelProbability = Math.abs(distTwap) > 80.0 ? 0.94 : Math.min(0.95, fillPrice + 0.10);
        double edge = Math.max(0.0, modelProbability - fillPrice);
        String paperOrderId = "paper-fill-" + UUID.randomUUID();
        String clobTokenId = resolveClobTokenId(command.token(), isDown);

        Position pos = new Position(
                command.clientOrderId(),
                paperOrderId,
                side,
                fillPrice,
                shares,
                sizeUsd,
                entryFee,
                System.currentTimeMillis(),
                activeCandleStart.get(),
                command.token(),
                clobTokenId,
                modelProbability,
                fillPrice,
                edge,
                fillPrice,
                false
        );
        openPositions.add(pos);
        activePosition.set(pos);

        ExecutionReport report = new ExecutionReport(
                command.clientOrderId(),
                paperOrderId,
                "FILLED",
                BigDecimal.valueOf(fillPrice),
                BigDecimal.valueOf(shares),
                System.currentTimeMillis(),
                "Virtual fill after " + orderLatencyMs + "ms latency"
        );
        deliverExecutionReport(report, asyncReportDelivery);
        Log.infof("Opened paper position: %s %s %.2f shares @ %.4f ($%.2f, fee: $%.4f)",
                command.clientOrderId(), side, shares, fillPrice, sizeUsd, entryFee);
    }

    private void executePaperSellOrder(OrderCommand command, boolean asyncReportDelivery) {
        Position pos = activePosition.get();
        if (pos == null) {
            Log.warnf("Ignoring paper SELL order %s because no active position exists", command.clientOrderId());
            return;
        }

        ordersSubmitted.incrementAndGet();
        double fillPrice = command.price().doubleValue();
        OrderBookQuote quote = this.latestQuote;
        if (quote != null) {
            double bestBid = (pos.side() == TradeDirection.DOWN) ? quote.bestBidDown() : quote.bestBidUp();
            if (bestBid > 0.0) {
                fillPrice = Math.min(fillPrice, bestBid);
            }
        }

        ordersFilled.incrementAndGet();
        lastOrderRttMs.set(orderLatencyMs);
        updateMaxLong(maxOrderRttMs, orderLatencyMs);

        double exitFee = (fillPrice > 0.0 && fillPrice < 1.0)
                ? (pos.shares() * TAKER_FEE_RATE * fillPrice * (1.0 - fillPrice))
                : 0.0;

        String exitReason;
        if (fillPrice >= 0.70) {
            exitReason = "Take Profit (0.70)";
        } else if (pos.trailingStopArmed()) {
            exitReason = "Trailing Stop (+0.05)";
        } else {
            exitReason = String.format(Locale.ROOT, "Early Exit (%.2f)", fillPrice);
        }

        completePaperTrade(pos, fillPrice, exitFee, exitReason);

        ExecutionReport report = new ExecutionReport(
                command.clientOrderId(),
                "paper-exit-" + UUID.randomUUID(),
                "FILLED",
                BigDecimal.valueOf(fillPrice),
                BigDecimal.valueOf(pos.shares()),
                System.currentTimeMillis(),
                "Virtual exit fill after " + orderLatencyMs + "ms latency"
        );
        deliverExecutionReport(report, asyncReportDelivery);
    }

    private void completePaperTrade(Position pos, double exitPrice, double exitFee, String exitReason) {
        double payout = pos.shares() * exitPrice;
        double totalFee = pos.entryFee() + exitFee;
        double netPnl = payout - pos.sizeUsd() - totalFee;
        double newBalance = paperRepository.getCurrentBalance() + netPnl;
        boolean isWin = netPnl > 0.0;

        TradeRecord trade = new TradeRecord(
                Instant.now().toString(),
                pos.intervalStartSec(),
                pos.side(),
                pos.entryPrice(),
                exitPrice,
                pos.sizeUsd(),
                pos.shares(),
                totalFee,
                netPnl,
                exitReason,
                pos.modelProbability(),
                pos.marketPrice(),
                pos.edge(),
                isWin,
                newBalance
        );

        paperRepository.recordTrade(trade);
        openPositions.remove(pos);
        activePosition.set(openPositions.isEmpty() ? null : openPositions.getLast());
        Log.infof("Closed paper trade: %s @ %.4f -> Net PnL: $%.2f (%s), Paper Balance: $%.2f",
                pos.side(), exitPrice, netPnl, exitReason, newBalance);
    }

    private void executeLiveBuyOrder(OrderCommand command, boolean asyncReportDelivery) {
        double requestedPrice = command.price().doubleValue();
        double requestedShares = command.size().doubleValue();
        boolean isDown = command.token() != null && command.token().toLowerCase(Locale.ROOT).contains("down");
        TradeDirection side = isDown ? TradeDirection.DOWN : TradeDirection.UP;
        String clobTokenId = resolveClobTokenId(command.token(), isDown);

        double estimatedCostUsd = requestedPrice * requestedShares;
        if (liveCurrentBalance < estimatedCostUsd) {
            Log.warnf("Insufficient live balance ($%.2f) for order notional $%.2f",
                    liveCurrentBalance, estimatedCostUsd);
            ordersRejected.incrementAndGet();
            deliverExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "live-reject-" + UUID.randomUUID(),
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    "Insufficient balance"
            ), asyncReportDelivery);
            return;
        }

        if (clobTokenId.isBlank()) {
            Log.warnf("Cannot submit live BUY order %s: active market token ID is not resolved", command.clientOrderId());
            ordersRejected.incrementAndGet();
            deliverExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "",
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    "Active market token ID not resolved"
            ), asyncReportDelivery);
            return;
        }

        ordersSubmitted.incrementAndGet();
        long orderStartMs = System.currentTimeMillis();
        double fillPrice;
        double filledShares;
        String clobOrderId;
        try {
            ensureSignerReady();
            int priceMc = (int) Math.round(requestedPrice * 1000.0);
            long wholeShares = Math.max(1L, Math.round(requestedShares));
            ClobOrderEnvelope envelope = eip712Signer.buildSignedBuyOrder(
                    credentials.makerAddress(),
                    credentials.apiKey(),
                    clobTokenId,
                    priceMc,
                    wholeShares,
                    orderType
            );
            ClobOrderResponse response = clobRestClient.submitOrder(envelope);
            long rttMs = Math.max(0L, System.currentTimeMillis() - orderStartMs);
            lastOrderRttMs.set(rttMs);
            updateMaxLong(maxOrderRttMs, rttMs);

            double actualShares = response.filledShares("BUY");
            if (!response.success() || actualShares <= 0.0) {
                ordersRejected.incrementAndGet();
                String err = (response.errorMsg() != null && !response.errorMsg().isBlank())
                        ? response.errorMsg()
                        : ("Order status: " + response.status());
                Log.warnf("Live CLOB BUY order %s rejected/unfilled: %s", command.clientOrderId(), err);
                deliverExecutionReport(new ExecutionReport(
                        command.clientOrderId(),
                        response.orderID() != null ? response.orderID() : "",
                        "REJECTED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        System.currentTimeMillis(),
                        err
                ), asyncReportDelivery);
                return;
            }
            ordersFilled.incrementAndGet();
            filledShares = actualShares;
            int avgMc = response.avgPriceMc("BUY");
            fillPrice = avgMc > 0 ? (avgMc / 1000.0) : requestedPrice;
            clobOrderId = response.orderID() != null ? response.orderID() : "";
        } catch (Exception e) {
            long rttMs = Math.max(0L, System.currentTimeMillis() - orderStartMs);
            lastOrderRttMs.set(rttMs);
            updateMaxLong(maxOrderRttMs, rttMs);
            ordersRejected.incrementAndGet();
            Log.errorf("Live CLOB BUY order %s failed: %s", command.clientOrderId(), e.getMessage());
            deliverExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "",
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    e.getMessage()
            ), asyncReportDelivery);
            return;
        }

        double sizeUsd = fillPrice * filledShares;
        double entryFee = filledShares * TAKER_FEE_RATE * fillPrice * (1.0 - fillPrice);
        double distTwap = currentSpotPrice - twapOpenPrice;
        double modelProbability = Math.abs(distTwap) > 80.0 ? 0.94 : Math.min(0.95, fillPrice + 0.10);
        double edge = Math.max(0.0, modelProbability - fillPrice);

        Position pos = new Position(
                command.clientOrderId(),
                clobOrderId,
                side,
                fillPrice,
                filledShares,
                sizeUsd,
                entryFee,
                System.currentTimeMillis(),
                activeCandleStart.get(),
                command.token(),
                clobTokenId,
                modelProbability,
                fillPrice,
                edge,
                fillPrice,
                false
        );
        openPositions.add(pos);
        activePosition.set(pos);

        ExecutionReport report = new ExecutionReport(
                command.clientOrderId(),
                clobOrderId,
                "FILLED",
                BigDecimal.valueOf(fillPrice),
                BigDecimal.valueOf(filledShares),
                System.currentTimeMillis(),
                "Matched on Polymarket CLOB"
        );
        deliverExecutionReport(report, asyncReportDelivery);
        Log.infof("Opened live position: %s (clobOrderId=%s) %s %.2f shares @ %.4f ($%.2f, fee: $%.4f, rtt=%d ms)",
                command.clientOrderId(), clobOrderId, side, filledShares, fillPrice, sizeUsd, entryFee, lastOrderRttMs.get());
    }

    private void executeLiveSellOrder(OrderCommand command, boolean asyncReportDelivery) {
        Position pos = activePosition.get();
        if (pos == null) {
            Log.warnf("Ignoring live SELL order %s because no active position exists", command.clientOrderId());
            return;
        }

        ordersSubmitted.incrementAndGet();
        long orderStartMs = System.currentTimeMillis();
        double requestedPrice = command.price().doubleValue();
        double fillPrice;
        String clobOrderId;
        try {
            ensureSignerReady();
            int limitPriceMc = (int) Math.round(requestedPrice * 1000.0);
            long microShares = Math.round(pos.shares() * 1_000_000.0);
            String tokenId = !pos.clobTokenId().isBlank()
                    ? pos.clobTokenId()
                    : resolveClobTokenId(pos.token(), pos.side() == TradeDirection.DOWN);
            ClobOrderEnvelope envelope = eip712Signer.buildSignedSellOrder(
                    credentials.makerAddress(),
                    credentials.apiKey(),
                    tokenId,
                    limitPriceMc,
                    microShares,
                    orderType,
                    PolymarketEip712Signer.generateOrderSalt(),
                    System.currentTimeMillis()
            );
            ClobOrderResponse response = clobRestClient.submitOrder(envelope);
            long rttMs = Math.max(0L, System.currentTimeMillis() - orderStartMs);
            lastOrderRttMs.set(rttMs);
            updateMaxLong(maxOrderRttMs, rttMs);

            if (!response.success() || response.filledShares("SELL") <= 0.0) {
                ordersRejected.incrementAndGet();
                String err = (response.errorMsg() != null && !response.errorMsg().isBlank())
                        ? response.errorMsg()
                        : ("Exit order status: " + response.status());
                Log.warnf("Live CLOB SELL order %s rejected/unfilled: %s", command.clientOrderId(), err);
                deliverExecutionReport(new ExecutionReport(
                        command.clientOrderId(),
                        response.orderID() != null ? response.orderID() : "",
                        "REJECTED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        System.currentTimeMillis(),
                        err
                ), asyncReportDelivery);
                return;
            }
            ordersFilled.incrementAndGet();
            int avgMc = response.avgPriceMc("SELL");
            fillPrice = avgMc > 0 ? (avgMc / 1000.0) : requestedPrice;
            clobOrderId = response.orderID() != null ? response.orderID() : "";
        } catch (Exception e) {
            long rttMs = Math.max(0L, System.currentTimeMillis() - orderStartMs);
            lastOrderRttMs.set(rttMs);
            updateMaxLong(maxOrderRttMs, rttMs);
            ordersRejected.incrementAndGet();
            Log.errorf("Live CLOB SELL order %s failed: %s", command.clientOrderId(), e.getMessage());
            deliverExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "",
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    e.getMessage()
            ), asyncReportDelivery);
            return;
        }

        double exitFee = (fillPrice > 0.0 && fillPrice < 1.0)
                ? (pos.shares() * TAKER_FEE_RATE * fillPrice * (1.0 - fillPrice))
                : 0.0;
        String exitReason = String.format(Locale.ROOT, "Early Exit (%.2f)", fillPrice);
        closeLivePositionInMemory(pos, fillPrice, exitFee, exitReason);

        ExecutionReport report = new ExecutionReport(
                command.clientOrderId(),
                clobOrderId,
                "FILLED",
                BigDecimal.valueOf(fillPrice),
                BigDecimal.valueOf(pos.shares()),
                System.currentTimeMillis(),
                "Exit matched on Polymarket CLOB"
        );
        deliverExecutionReport(report, asyncReportDelivery);
    }

    /**
     * Settles all open positions for the completed 15m candle according to the binary resolution outcome.
     * In {@link StrategyMode#PAPER}, appends settled trades to {@link PaperTradeRepository}.
     *
     * @param actualOutcome resolved candle outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
     * @param finalPrice    closing reference price
     */
    public void resolveCandle(TradeDirection actualOutcome, double finalPrice) {
        synchronized (strategyLock) {
            int settledPositions = openPositions.size();
            StrategyMode mode = this.strategyMode;
            if (!openPositions.isEmpty() || activePosition.get() != null) {
                activeStrategy.onCandleResolution(actualOutcome);

                List<Position> remaining = new ArrayList<>(openPositions);
                if (remaining.isEmpty() && activePosition.get() != null) {
                    remaining.add(activePosition.get());
                }
                settledPositions = remaining.size();
                for (Position pos : remaining) {
                    double exitPrice = (actualOutcome == pos.side()) ? 1.00 : 0.00;
                    if (mode == StrategyMode.PAPER) {
                        completePaperTrade(pos, exitPrice, 0.0, "Resolution (TWAP 60s)");
                    } else {
                        closeLivePositionInMemory(pos, exitPrice, 0.0, "Resolution (TWAP 60s)");
                    }
                }
                openPositions.clear();
                activePosition.set(null);
            }

            long start = activeCandleStart.get() > 0L
                    ? activeCandleStart.get()
                    : (currentTimestampSec > 0L ? currentTimestampSec - durationSeconds : 1700000000L);
            long end = activeCandleEnd.get() > start ? activeCandleEnd.get() : (start + durationSeconds);
            double open = twapOpenPrice > 0.0 ? twapOpenPrice : finalPrice;
            double high = candleHigh > 0.0 ? Math.max(candleHigh, Math.max(open, finalPrice)) : Math.max(open, finalPrice);
            double low = candleLow > 0.0 ? Math.min(candleLow, Math.min(open, finalPrice)) : Math.min(open, finalPrice);

            Log.infof("Resolved market candle [%d, %d] (%s): outcome=%s, open=%.2f, close=%.2f, high=%.2f, low=%.2f, settledPositions=%d, balance=$%.2f",
                    start, end, mode, actualOutcome, open, finalPrice, high, low, settledPositions, getCurrentBalance());

            candleHigh = 0.0;
            candleLow = 0.0;
        }
    }

    private void closeLivePositionInMemory(Position pos, double exitPrice, double exitFee, String exitReason) {
        double payout = pos.shares() * exitPrice;
        double totalFee = pos.entryFee() + exitFee;
        double netPnl = payout - pos.sizeUsd() - totalFee;
        openPositions.remove(pos);
        activePosition.set(openPositions.isEmpty() ? null : openPositions.getLast());
        Log.infof("Closed live position: %s @ %.4f -> Net PnL: $%.2f (%s), Balance: $%.2f",
                pos.side(), exitPrice, netPnl, exitReason, liveCurrentBalance);
    }

    /**
     * Processes a PolyBolt {@code price.crypto.twap} (60s) update, managing 15m candle boundary resolution
     * and forwarding the tick to the active strategy.
     *
     * @param timestampMs     local receipt timestamp in epoch milliseconds
     * @param symbol          PolyBolt symbol (e.g. {@code "btcusd"})
     * @param source          oracle source (e.g. {@code "chainlink"})
     * @param twapPrice       60-second Chainlink TWAP price in USD
     * @param windowSeconds   TWAP window in seconds (60)
     * @param twapTimestampMs oracle timestamp in epoch milliseconds
     */
    public void onPolymarketCryptoTwap60s(
            long timestampMs,
            String symbol,
            String source,
            double twapPrice,
            int windowSeconds,
            long twapTimestampMs) {
        if (twapPrice <= 0.0 || timestampMs <= 0L) {
            return;
        }
        if (timestampMs > latestOracleRecvTimestampMs) {
            latestOracleRecvTimestampMs = timestampMs;
        }
        long effectiveTsMs = twapTimestampMs > 0L ? twapTimestampMs : timestampMs;
        long candleStartSec = (effectiveTsMs / 1000L / durationSeconds) * durationSeconds;

        synchronized (strategyLock) {
            long currentCandleStart = activeCandleStart.get();
            if (currentCandleStart > 0L && candleStartSec < currentCandleStart) {
                activeStrategy.onPolymarketCryptoTwap60s(timestampMs, symbol, source, twapPrice, windowSeconds, twapTimestampMs);
                return;
            }
            if (currentCandleStart > 0L && candleStartSec > currentCandleStart) {
                long prevCandleStart = currentCandleStart;
                PolymarketMarketInfo currentMarket = activeMarketInfo.get();
                String marketLabel = (currentMarket != null && currentMarket.startSec() == prevCandleStart)
                        ? currentMarket.slug()
                        : PolymarketMarketInfo.buildMarketSlug(this.symbol, this.timeframe, prevCandleStart);
                evaluateCompletedMarketStreamActivity(marketLabel, prevCandleStart);

                if (twapOpenPrice > 0.0) {
                    TradeDirection outcome = (twapPrice >= twapOpenPrice) ? TradeDirection.UP : TradeDirection.DOWN;
                    resolveCandle(outcome, twapPrice);
                } else {
                    Log.warnf("Skipping candle resolution for market %s (intervalStart=%d): target price was missing from PolyBolt 60s TWAP stream",
                            marketLabel, prevCandleStart);
                    openPositions.clear();
                    activePosition.set(null);
                }
                activeCandleStart.set(candleStartSec);
                activeCandleEnd.set(candleStartSec + durationSeconds);
                twapOpenPrice = (Math.abs(effectiveTsMs - candleStartSec * 1000L) <= 10_000L) ? twapPrice : 0.0;
            } else if (currentCandleStart == 0L) {
                activeCandleStart.set(candleStartSec);
                activeCandleEnd.set(candleStartSec + durationSeconds);
                twapOpenPrice = (Math.abs(effectiveTsMs - candleStartSec * 1000L) <= 10_000L) ? twapPrice : 0.0;
            } else if (twapOpenPrice <= 0.0 && Math.abs(effectiveTsMs - candleStartSec * 1000L) <= 10_000L) {
                twapOpenPrice = twapPrice;
            }

            activeStrategy.onPolymarketCryptoTwap60s(timestampMs, symbol, source, twapPrice, windowSeconds, twapTimestampMs);
        }
    }

    /**
     * Processes a PolyBolt {@code price.crypto} 1-second spot price update and forwards it to the active strategy.
     *
     * @param timestampMs      local receipt timestamp in epoch milliseconds
     * @param symbol           PolyBolt symbol (e.g. {@code "btcusd"})
     * @param source           oracle source (e.g. {@code "chainlink"})
     * @param price            1s Chainlink price in USD
     * @param priceTimestampMs oracle timestamp in epoch milliseconds
     */
    public void onPolymarketCryptoPrice(
            long timestampMs,
            String symbol,
            String source,
            double price,
            long priceTimestampMs) {
        if (price <= 0.0 || timestampMs <= 0L) {
            return;
        }
        if (timestampMs > latestOracleRecvTimestampMs) {
            latestOracleRecvTimestampMs = timestampMs;
        }
        currentTimestampSec = timestampMs / 1000L;
        currentSpotPrice = price;
        if (candleHigh == 0.0 || price > candleHigh) {
            candleHigh = price;
        }
        if (candleLow == 0.0 || price < candleLow) {
            candleLow = price;
        }
        if (activeCandleStart.get() == 0L) {
            long candleStartSec = (currentTimestampSec / durationSeconds) * durationSeconds;
            activeCandleStart.set(candleStartSec);
            activeCandleEnd.set(candleStartSec + durationSeconds);
        }

        synchronized (strategyLock) {
            activeStrategy.onPolymarketCryptoPrice(timestampMs, symbol, source, price, priceTimestampMs);
        }
    }

    /**
     * Processes a batch of forward-progressing Binance Spot {@code aggTrades} frames into
     * {@code SpotTwapIndicator} and notifies registered {@link SpotTwapListener} instances once for the batch.
     *
     * @param batch chronological batch of fast-forwarded Binance {@code aggTrade} frames
     */
    public void onRawBinanceFrameBatch(List<TimestampedFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        long lastRecvTs = batch.getLast().recvTs();
        if (lastRecvTs > latestOracleRecvTimestampMs) {
            latestOracleRecvTimestampMs = lastRecvTs;
        }
        synchronized (strategyLock) {
            spotStreamProcessor.processBinanceFrameBatch(batch);
        }
    }

    /**
     * Processes a raw JSON text frame from the Kraken Spot v2 {@code trade} WebSocket stream
     * ({@code USDTUSD} or {@code <SYMBOL>}), updating {@code SpotTwapIndicator} and forwarding
     * the calculated USD spot and 60s TWAP to registered {@link SpotTwapListener} strategies.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawJson         raw Kraken JSON frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawKrakenFrame(String streamSymbol, String rawJson, long recvTimestampMs) {
        if (recvTimestampMs > latestOracleRecvTimestampMs) {
            latestOracleRecvTimestampMs = recvTimestampMs;
        }
        synchronized (strategyLock) {
            spotStreamProcessor.processKrakenFrame(streamSymbol, rawJson, recvTimestampMs);
        }
    }

    /**
     * Processes a raw UTF-8 {@code byte[]} frame from the Kraken Spot v2 {@code trade} WebSocket stream
     * ({@code USDTUSD} or {@code <SYMBOL>}) without intermediate {@link String} allocation.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawKrakenFrameBytes(String streamSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
        if (recvTimestampMs > latestOracleRecvTimestampMs) {
            latestOracleRecvTimestampMs = recvTimestampMs;
        }
        synchronized (strategyLock) {
            spotStreamProcessor.processKrakenFrameBytes(streamSymbol, rawUtf8Bytes, recvTimestampMs);
        }
    }

    /**
     * Processes a raw JSON text frame from the Coinbase Spot {@code matches} WebSocket stream,
     * updating {@code SpotTwapIndicator} and forwarding the calculated USD spot and 60s TWAP
     * to registered {@link SpotTwapListener} strategies.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawJson         raw Coinbase JSON frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawCoinbaseFrame(String streamSymbol, String rawJson, long recvTimestampMs) {
        if (recvTimestampMs > latestOracleRecvTimestampMs) {
            latestOracleRecvTimestampMs = recvTimestampMs;
        }
        synchronized (strategyLock) {
            spotStreamProcessor.processCoinbaseFrame(streamSymbol, rawJson, recvTimestampMs);
        }
    }

    /**
     * Processes a raw UTF-8 {@code byte[]} frame from the Coinbase Spot {@code matches} WebSocket stream
     * without intermediate {@link String} allocation.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawCoinbaseFrameBytes(String streamSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
        if (recvTimestampMs > latestOracleRecvTimestampMs) {
            latestOracleRecvTimestampMs = recvTimestampMs;
        }
        synchronized (strategyLock) {
            spotStreamProcessor.processCoinbaseFrameBytes(streamSymbol, rawUtf8Bytes, recvTimestampMs);
        }
    }

    /**
     * Processes a normalized Polymarket L2 orderbook BBO quote, enforcing the quote freshness guard
     * ({@code maxQuoteAgeMs}) before forwarding to the active strategy.
     *
     * @param quote current BBO quote across Up and Down tokens
     */
    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        this.latestQuote = quote;
        updatePositionObservation(quote);
        evaluateQuoteWithFreshnessGuard(quote, false);
    }

    private void evaluateQuoteWithFreshnessGuard(OrderBookQuote quote, boolean fromRealtimeWorker) {
        if (quote.timestampMs() > 0L) {
            long wallNowMs = System.currentTimeMillis();
            long referenceNowMs;
            if (fromRealtimeWorker || Math.abs(wallNowMs - quote.timestampMs()) < REALTIME_CLOCK_WINDOW_MS) {
                referenceNowMs = wallNowMs;
            } else {
                referenceNowMs = Math.max(quote.timestampMs(), latestOracleRecvTimestampMs);
            }
            long quoteAgeMs = Math.max(0L, referenceNowMs - quote.timestampMs());
            lastQuoteAgeMs.set(quoteAgeMs);
            updateMaxLong(maxQuoteAgeMsObserved, quoteAgeMs);

            if (quoteAgeMs > maxQuoteAgeMs) {
                long totalStale = staleQuotesSkipped.incrementAndGet();
                long prevLog = lastStaleWarnLogMs.get();
                if (wallNowMs - prevLog >= STALE_QUOTE_WARN_INTERVAL_MS
                        && lastStaleWarnLogMs.compareAndSet(prevLog, wallNowMs)) {
                    Log.warnf("Skipping stale CLOB OrderBookQuote (quoteAgeMs=%d > maxQuoteAgeMs=%d, totalSkipped=%d)",
                            quoteAgeMs, maxQuoteAgeMs, totalStale);
                }
                return;
            }
        }
        if (Log.isTraceEnabled()) {
            Log.tracef("OrderBookQuote evaluated at %d ms: bidUp=%.3f, askUp=%.3f, bidDown=%.3f, askDown=%.3f",
                    quote.timestampMs(), quote.bestBidUp(), quote.bestAskUp(), quote.bestBidDown(), quote.bestAskDown());
        }
        synchronized (strategyLock) {
            activeStrategy.onOrderBookQuote(quote);
        }
    }

    /**
     * Explicitly sets the active market metadata.
     *
     * @param marketInfo active {@link PolymarketMarketInfo}
     */
    public synchronized void setActiveMarketInfo(PolymarketMarketInfo marketInfo) {
        this.activeMarketInfo.set(marketInfo);
        if (marketInfo != null) {
            this.activeLocalBook.set(new PolymarketLocalBook(marketInfo.yesTokenId(), marketInfo.noTokenId()));
        }
    }

    /**
     * Resets engine state and clears open positions.
     */
    public void reset() {
        synchronized (strategyLock) {
            openPositions.clear();
            activePosition.set(null);
            activeStrategy.reset();
            spotStreamProcessor.reset();
            activeCandleStart.set(0L);
            activeCandleEnd.set(0L);
            polyBoltMarketMsgCount.set(0L);
            clobMarketMsgCount.set(0L);
            lastEvaluatedMarketCycleStart.set(0L);
            lastTwapOracleTimestampMs.set(0L);
            lastCryptoPriceOracleTimestampMs.set(0L);
            debugPolyBoltBatchCounter.set(0L);
            debugClobBatchCounter.set(0L);
            twapOpenPrice = 0.0;
            currentSpotPrice = 0.0;
            candleHigh = 0.0;
            candleLow = 0.0;
            currentTimestampSec = 0L;
            latestOracleRecvTimestampMs = 0L;
            latestQuote = null;
            stagedQuoteSlot.set(null);
            prefetchedNextMarketInfo.set(null);
            orderExecutionQueue.clear();
            executionReportQueue.clear();
            if (liveCurrentBalance > 0.0) {
                liveInitialCapital = liveCurrentBalance;
            }
        }
        synchronized (this) {
            appliedUserTradeDeltas.clear();
        }
        Log.info("Reset StrategyTradingEngine internal state");
    }

    /**
     * Evaluates whether the Strategy Trading Engine satisfies all operational health checks for the active
     * {@link StrategyMode}, appending any detected failures to {@code issues}.
     *
     * @param issues mutable list receiving human-readable descriptions of failing checks
     * @return {@code true} if healthy for the current {@link StrategyMode}
     */
    public boolean evaluateStrategyHealth(List<String> issues) {
        StrategyMode mode = this.strategyMode;
        if (mode == StrategyMode.OFF) {
            boolean ok = true;
            if (workersRunning.get()) {
                issues.add("Strategy workers still marked running in OFF mode");
                ok = false;
            }
            if (clobWsClient.hasQueuedMarketFrames() || clobWsClient.hasQueuedUserFrames()) {
                issues.add("Strategy CLOB queue has unconsumed frames in OFF mode");
                ok = false;
            }
            int polyBoltDepth = polyBoltClient.getStrategyQueueDepth();
            if (polyBoltDepth > 0) {
                issues.add("Strategy PolyBolt queue depth is " + polyBoltDepth + " (> 0) in OFF mode");
                ok = false;
            }
            return ok;
        }

        if (!streamsEnabled) {
            return true;
        }

        boolean ok = true;
        if (!workersRunning.get()
                || marketStrategyThread == null || !marketStrategyThread.isAlive()
                || orderExecutionWorkerThread == null || !orderExecutionWorkerThread.isAlive()) {
            issues.add("Strategy worker threads are not running for mode " + mode);
            ok = false;
        }
        if (mode == StrategyMode.LIVE && (clobUserWorkerThread == null || !clobUserWorkerThread.isAlive())) {
            issues.add("CLOB user worker thread is not running in LIVE mode");
            ok = false;
        }
        if (polyBoltClient.getTwapState() != ConnectionState.CONNECTED
                || polyBoltClient.getCryptoPriceState() != ConnectionState.CONNECTED) {
            issues.add("PolyBolt WebSocket streams are not CONNECTED");
            ok = false;
        }
        if (binanceConnector.getSpotAggTradesState() != ConnectionState.CONNECTED) {
            issues.add("Binance Spot aggTrades stream is not CONNECTED");
            ok = false;
        }
        Map<String, ConnectionState> krakenStates = krakenConnector.getConnectionStates();
        if (krakenStates.isEmpty() || krakenStates.values().stream().anyMatch(s -> s != ConnectionState.CONNECTED)) {
            issues.add("Kraken Spot trade streams are not CONNECTED");
            ok = false;
        }
        if (coinbaseConnector.getState() != ConnectionState.CONNECTED) {
            issues.add("Coinbase Spot matches stream is not CONNECTED");
            ok = false;
        }
        PolymarketMarketInfo market = activeMarketInfo.get();
        Map<String, ConnectionState> clobStates = clobWsClient.getConnectionStates();
        if (market == null || clobStates.get(market.slug()) != ConnectionState.CONNECTED) {
            issues.add("Strategy Polymarket CLOB market stream is not CONNECTED");
            ok = false;
        }
        if (mode == StrategyMode.LIVE && clobStates.get("USER") != ConnectionState.CONNECTED) {
            issues.add("Polymarket CLOB USER stream is not CONNECTED in LIVE mode");
            ok = false;
        }
        return ok;
    }

    /**
     * Builds a comprehensive point-in-time {@link TelemetryDto} snapshot combining OS TCP buffers,
     * JVM ZGC metrics, native transport/TLS status, 4-core thread utilization, order execution latency,
     * WebSocket stream queues, disconnect forensic snapshots, and historical time-series samples.
     *
     * @return current {@link TelemetryDto}
     */
    public TelemetryDto getTelemetry() {
        SystemTelemetrySnapshot sys = telemetryMonitor.getLatestOrSample(buildMonitoredThreadSpecs());
        List<WebSocketStreamTelemetry> clobStreams = clobWsClient.getStreamTelemetries();
        List<WebSocketStreamTelemetry> streams = new ArrayList<>(12);
        streams.addAll(clobStreams);
        streams.addAll(polyBoltClient.getStreamTelemetries());
        streams.addAll(binanceConnector.getStreamTelemetries());
        streams.addAll(krakenConnector.getStreamTelemetries());
        streams.addAll(coinbaseConnector.getStreamTelemetries());

        List<TelemetryHistoryPoint> history = telemetryMonitor.getHistory();
        if (history.isEmpty()) {
            recordTelemetryHistorySnapshot(sys, clobStreams);
            history = telemetryMonitor.getHistory();
        }

        return new TelemetryDto(
                sys.kernelRmemMaxBytes(),
                sys.currentTcpRxQueueBytes(),
                sys.maxTcpRxQueueBytes(),
                sys.currentTcpTxQueueBytes(),
                sys.maxTcpTxQueueBytes(),
                sys.activeTcpSockets(),
                sys.tcpRcvCollapsed(),
                sys.tcpOfoQueue(),
                sys.tcpBacklogDrop(),
                sys.gcName(),
                clobWsClient.isNativeTransportEnabled(),
                clobWsClient.isOpenSslAvailable(),
                sys.processCpuLoadPct(),
                sys.maxProcessCpuLoadPct(),
                sys.systemCpuLoadPct(),
                sys.gcCount(),
                sys.totalGcPauseMs(),
                sys.usedHeapMb(),
                sys.maxHeapMb(),
                ordersSubmitted.get(),
                ordersFilled.get(),
                ordersRejected.get(),
                staleQuotesSkipped.get(),
                conflatedQuotesCount.get(),
                lastQuoteAgeMs.get(),
                maxQuoteAgeMsObserved.get(),
                lastOrderRttMs.get(),
                maxOrderRttMs.get(),
                sys.threads(),
                streams,
                telemetryMonitor.getDisconnectEvents(),
                history
        );
    }

    /**
     * Captures a diagnostic snapshot at the exact moment a WebSocket stream disconnects unexpectedly,
     * recording it in the bounded {@link SystemNetworkTelemetryMonitor} ring buffer and logging
     * full system/thread/queue forensics.
     *
     * @param streamSnap telemetry snapshot of the disconnected stream
     */
    public void onStreamUnexpectedDisconnect(WebSocketStreamTelemetry streamSnap) {
        if (streamSnap == null) {
            return;
        }
        SystemTelemetrySnapshot sys = telemetryMonitor.getLatestOrSample(buildMonitoredThreadSpecs());
        double ioReaderCpu = extractThreadCpuPct(sys.threads(), 1);
        double marketStrategyWorkerCpu = extractThreadCpuPct(sys.threads(), 2);
        StreamRedundancyStatus redundancy =
                clobWsClient.describeMarketStreamRedundancy(streamSnap.streamName());
        if (!redundancy.dualStream()) {
            redundancy = binanceConnector.describeStreamRedundancy(streamSnap.streamName());
        }
        String remoteLabel = streamSnap.remoteAddress() != null && !streamSnap.remoteAddress().isEmpty()
                ? streamSnap.remoteAddress()
                : "unknown";
        String peerStateLabel = redundancy.dualStream() ? redundancy.peerState().name() : "SINGLE_STREAM";

        WebSocketDisconnectEvent event = new WebSocketDisconnectEvent(
                System.currentTimeMillis(),
                streamSnap.streamName(),
                streamSnap.remoteAddress() != null ? streamSnap.remoteAddress() : "",
                streamSnap.lastCloseCode(),
                streamSnap.lastCloseReason(),
                streamSnap.reconnectCount(),
                redundancy.peerStreamName(),
                peerStateLabel,
                redundancy.failoverActive(),
                streamSnap.queueDepth(),
                streamSnap.maxQueueDepth(),
                streamSnap.lastQueueLagMs(),
                streamSnap.maxQueueLagMs(),
                streamSnap.lastProcessTimeUs(),
                streamSnap.maxProcessTimeUs(),
                sys.currentTcpRxQueueBytes(),
                sys.tcpRcvCollapsed(),
                sys.processCpuLoadPct(),
                ioReaderCpu,
                marketStrategyWorkerCpu
        );
        telemetryMonitor.recordDisconnectEvent(event);
        if (redundancy.dualStream() && redundancy.failoverActive()) {
            String peerRemote = redundancy.peerRemoteAddress().isEmpty() ? "unknown" : redundancy.peerRemoteAddress();
            Log.warnf("[%s] Recorded WebSocket disconnect forensics (remote=%s, code=%d, reason=%s, FAILOVER ACTIVE: peerStream=%s is %s @ %s — 0 ms stream outage, queueDepth=%d/%d, queueLagMs=%d/%d, processUs=%d/%d, tcpRxBytes=%d, tcpCollapsed=%d, procCpu=%.1f%%, ioReaderCpu=%.1f%%, marketStrategyCpu=%.1f%%)",
                    event.streamName(),
                    remoteLabel,
                    event.closeCode(),
                    event.closeReason(),
                    event.peerStreamName(),
                    event.peerState(),
                    peerRemote,
                    event.queueDepthAtDisconnect(),
                    event.maxQueueDepth(),
                    event.lastQueueLagMs(),
                    event.maxQueueLagMs(),
                    event.lastProcessTimeUs(),
                    event.maxProcessTimeUs(),
                    event.tcpRxQueueBytes(),
                    event.tcpRcvCollapsed(),
                    event.processCpuLoadPct(),
                    event.ioReaderCpuPct(),
                    event.marketStrategyWorkerCpuPct());
        } else if (redundancy.dualStream()) {
            Log.warnf("[%s] Recorded WebSocket disconnect forensics (remote=%s, code=%d, reason=%s, BOTH STREAMS DOWN: peerStream=%s is %s, queueDepth=%d/%d, queueLagMs=%d/%d, processUs=%d/%d, tcpRxBytes=%d, tcpCollapsed=%d, procCpu=%.1f%%, ioReaderCpu=%.1f%%, marketStrategyCpu=%.1f%%)",
                    event.streamName(),
                    remoteLabel,
                    event.closeCode(),
                    event.closeReason(),
                    event.peerStreamName(),
                    event.peerState(),
                    event.queueDepthAtDisconnect(),
                    event.maxQueueDepth(),
                    event.lastQueueLagMs(),
                    event.maxQueueLagMs(),
                    event.lastProcessTimeUs(),
                    event.maxProcessTimeUs(),
                    event.tcpRxQueueBytes(),
                    event.tcpRcvCollapsed(),
                    event.processCpuLoadPct(),
                    event.ioReaderCpuPct(),
                    event.marketStrategyWorkerCpuPct());
        } else {
            Log.warnf("[%s] Recorded WebSocket disconnect forensics (remote=%s, code=%d, reason=%s, mode=SINGLE_STREAM, queueDepth=%d/%d, queueLagMs=%d/%d, processUs=%d/%d, tcpRxBytes=%d, tcpCollapsed=%d, procCpu=%.1f%%, ioReaderCpu=%.1f%%, marketStrategyCpu=%.1f%%)",
                    event.streamName(),
                    remoteLabel,
                    event.closeCode(),
                    event.closeReason(),
                    event.queueDepthAtDisconnect(),
                    event.maxQueueDepth(),
                    event.lastQueueLagMs(),
                    event.maxQueueLagMs(),
                    event.lastProcessTimeUs(),
                    event.maxProcessTimeUs(),
                    event.tcpRxQueueBytes(),
                    event.tcpRcvCollapsed(),
                    event.processCpuLoadPct(),
                    event.ioReaderCpuPct(),
                    event.marketStrategyWorkerCpuPct());
        }
    }

    private void recordTelemetryHistorySnapshot(
            SystemTelemetrySnapshot sys,
            List<WebSocketStreamTelemetry> clobStreams) {
        int clobQueueDepth = 0;
        long clobMaxLagMs = 0L;
        if (clobStreams != null) {
            for (WebSocketStreamTelemetry s : clobStreams) {
                if (s.streamName() != null
                        && s.streamName().startsWith("POLYMARKET-CLOB-")
                        && !s.streamName().endsWith("-USER")) {
                    clobQueueDepth = Math.max(clobQueueDepth, s.queueDepth());
                    clobMaxLagMs = Math.max(clobMaxLagMs, s.maxQueueLagMs());
                }
            }
        }
        List<ThreadUtilizationTelemetry> threads = sys.threads();
        TelemetryHistoryPoint point = new TelemetryHistoryPoint(
                System.currentTimeMillis(),
                sys.processCpuLoadPct(),
                sys.systemCpuLoadPct(),
                extractThreadCpuPct(threads, 0),
                extractThreadCpuPct(threads, 1),
                extractThreadCpuPct(threads, 2),
                extractThreadCpuPct(threads, 3),
                sys.currentTcpRxQueueBytes(),
                clobQueueDepth,
                clobMaxLagMs,
                lastQuoteAgeMs.get(),
                sys.usedHeapMb()
        );
        telemetryMonitor.recordHistoryPoint(point);
    }

    private static double extractThreadCpuPct(List<ThreadUtilizationTelemetry> threads, int index) {
        if (threads == null || index < 0 || index >= threads.size()) {
            return 0.0;
        }
        return threads.get(index).cpuUtilizationPct();
    }

    private List<MonitoredThreadSpec> buildMonitoredThreadSpecs() {
        Thread ioReader = clobWsClient.getIoReaderThread();
        if (ioReader == null) {
            ioReader = polyBoltClient.getIoReaderThread();
        }
        return List.of(
                new MonitoredThreadSpec("vCPU #0: User WS & Control", "clob-user-worker", clobUserWorkerThread, userWorkerBusyNs.get()),
                new MonitoredThreadSpec("vCPU #1: Market WS I/O", MarketWebSocketRuntime.DEDICATED_MARKET_IO_THREAD_NAME, ioReader, -1L),
                new MonitoredThreadSpec("vCPU #2: Book & Strategy", "market-strategy-worker", marketStrategyThread, marketStrategyWorkerBusyNs.get()),
                new MonitoredThreadSpec("vCPU #3: Order Execution", "order-execution-io-worker", orderExecutionWorkerThread, orderExecutionWorkerBusyNs.get())
        );
    }

    /**
     * Returns the current {@link StrategyMode} ({@code LIVE}, {@code PAPER}, or {@code OFF}).
     *
     * @return current {@link StrategyMode}
     */
    public StrategyMode getStrategyMode() {
        return strategyMode;
    }

    /**
     * Returns the initial bankroll in USD for the active {@link StrategyMode}.
     *
     * @return initial capital in USD
     */
    public double getInitialCapital() {
        return strategyMode == StrategyMode.PAPER ? paperRepository.getInitialCapital() : liveInitialCapital;
    }

    /**
     * Returns the current balance in USD for the active {@link StrategyMode}.
     *
     * @return current balance in USD
     */
    public double getCurrentBalance() {
        return strategyMode == StrategyMode.PAPER ? paperRepository.getCurrentBalance() : liveCurrentBalance;
    }

    /**
     * Returns the captured 60-second Chainlink TWAP target price for the active 15m candle.
     *
     * @return target price in USD (or {@code 0.0} if not yet locked)
     */
    public double getTwapOpenPrice() {
        return twapOpenPrice;
    }

    /**
     * Returns the configured simulated order execution latency in milliseconds (used in {@code PAPER} mode).
     *
     * @return paper order latency in milliseconds
     */
    public long getOrderLatencyMs() {
        return orderLatencyMs;
    }

    /**
     * Returns the number of PolyBolt WebSocket messages received by Strategy in the current market cycle.
     *
     * @return current market PolyBolt message count
     */
    public long getPolyBoltMarketMessageCount() {
        return polyBoltMarketMsgCount.get();
    }

    /**
     * Returns the number of Polymarket CLOB WebSocket messages received by Strategy in the current market cycle.
     *
     * @return current market CLOB WebSocket message count
     */
    public long getClobMarketMessageCount() {
        return clobMarketMsgCount.get();
    }

    /**
     * Returns the name of the active trading strategy.
     *
     * @return strategy name
     */
    public String getActiveStrategyName() {
        return activeStrategy.getName();
    }

    /**
     * Returns the configured CLOB order time-in-force (e.g. {@code "FAK"}).
     *
     * @return order type string
     */
    public String getOrderType() {
        return orderType;
    }

    /**
     * Returns the currently tracked Polymarket market metadata, if any.
     *
     * @return active {@link PolymarketMarketInfo} or {@code null}
     */
    public PolymarketMarketInfo getActiveMarketInfo() {
        return activeMarketInfo.get();
    }

    /**
     * Returns the pre-fetched upcoming Polymarket market metadata when inside the lookahead window, or {@code null}.
     *
     * @return pre-fetched upcoming {@link PolymarketMarketInfo} or {@code null}
     */
    public PolymarketMarketInfo getPrefetchedNextMarketInfo() {
        return prefetchedNextMarketInfo.get();
    }

    /**
     * Returns the latest active position, or {@code null} if idle.
     *
     * @return active {@link Position}
     */
    public Position getActivePosition() {
        return activePosition.get();
    }

    /**
     * Returns an unmodifiable snapshot of all open positions in the active candle.
     *
     * @return open positions
     */
    public List<Position> getOpenPositions() {
        return List.copyOf(openPositions);
    }

    /**
     * Returns the latest normalized orderbook BBO quote, or {@code null} if none received yet.
     *
     * @return latest {@link OrderBookQuote}
     */
    public OrderBookQuote getLatestQuote() {
        return latestQuote;
    }

    /**
     * Returns the latest 1-second spot price received from PolyBolt.
     *
     * @return spot price in USD
     */
    public double getCurrentSpotPrice() {
        return currentSpotPrice;
    }

    private String resolveClobTokenId(String strategyToken, boolean isDown) {
        PolymarketMarketInfo info = activeMarketInfo.get();
        if (info != null) {
            String candidate = isDown ? info.noTokenId() : info.yesTokenId();
            if (!candidate.isBlank()) {
                return candidate;
            }
        }
        if (strategyToken != null && strategyToken.chars().allMatch(Character::isDigit) && strategyToken.length() > 10) {
            return strategyToken;
        }
        return "";
    }

    private void ensureSignerReady() {
        if (credentials == null || eip712Signer == null) {
            initializeCredentialsAndSigner();
        }
        if (credentials == null || !credentials.isCompleteForTrading() || eip712Signer == null) {
            String msg = "Live Polymarket credentials (signerKey, apiKey, passphrase, secret, makerAddress) are not configured";
            Log.fatal(msg);
            throw new IllegalStateException(msg);
        }
    }

    private static void updateMaxLong(AtomicLong holder, long candidate) {
        long prev = holder.get();
        while (candidate > prev) {
            if (holder.compareAndSet(prev, candidate)) {
                return;
            }
            prev = holder.get();
        }
    }
}
