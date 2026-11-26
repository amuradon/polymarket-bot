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
import cz.polymarket.bot.connector.polymarket.stream.ClobBookSink;
import cz.polymarket.bot.connector.polymarket.stream.PolyBoltTick;
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
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor.MonitoredThreadSpec;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor.SystemTelemetrySnapshot;
import cz.polymarket.bot.monitoring.TelemetryHistoryPoint;
import cz.polymarket.bot.monitoring.ThreadUtilizationTelemetry;
import cz.polymarket.bot.monitoring.WebSocketDisconnectEvent;
import cz.polymarket.bot.orderbook.PolymarketOrderBook;
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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Unified Strategy Trading Engine supporting {@link StrategyMode#LIVE}, {@link StrategyMode#PAPER},
 * and {@link StrategyMode#OFF} over the canonical shared {@link CoreTradingEngine}, {@link PolymarketOrderBook},
 * and {@link SimulatedClobExecutionEngine} in {@code common}.
 * <p>
 * Preserves the 4-core CPU-pinned pipeline designed for {@code c8gn.xlarge} (4 physical Graviton4 cores):
 * <ul>
 *   <li><b>vCPU #0 ({@code clob-user-worker} + housekeeping)</b>: OS/ENA interrupts, ZGC, Quarkus HTTP/scheduler,
 *       Data Collector Zstandard writers, 1s telemetry/state maintenance, and authenticated
 *       {@code POLYMARKET-CLOB-USER} trade balance reconciliation in {@link StrategyMode#LIVE}.</li>
 *   <li><b>vCPU #1 ({@code market-ws-io-reader})</b>: Consolidated single-EventLoop native {@code epoll} + BoringSSL
 *       reader ({@link MarketWebSocketRuntime}) servicing all WebSocket sockets into lock-free MPSC queues.</li>
 *   <li><b>vCPU #2 ({@code market-strategy-worker})</b>: Fused single-writer loop draining execution reports,
 *       CLOB L2 orderbook deltas into {@link PolymarketOrderBook}, PolyBolt oracle ticks, and Binance/Kraken/Coinbase
 *       spot trades into {@link CoreTradingEngine} with zero cross-thread quote handoff.</li>
 *   <li><b>vCPU #3 ({@code order-execution-io-worker})</b>: Dedicated isolated core exclusively executing EIP-712
 *       signing and HTTP {@code POST /order} / {@code DELETE /order} execution in {@link StrategyMode#LIVE}.</li>
 * </ul>
 */
@ApplicationScoped
public class StrategyTradingEngine implements StrategyContext, ExecutionRouter {

    private static final double TAKER_FEE_RATE = PolymarketOrderBook.DEFAULT_TAKER_FEE_RATE;
    private static final long DEBUG_MESSAGE_LOG_INTERVAL = 100L;
    private static final int MAX_TRACKED_USER_TRADES = 1024;
    private static final int MAX_LOCAL_REST_FILLS = 64;
    private static final long REALTIME_CLOCK_WINDOW_MS = 3_600_000L;
    private static final long MAX_BOUNDARY_TWAP_OFFSET_MS = 10_000L;
    private static final long MARKET_PREFETCH_LOOKAHEAD_SEC = 60L;

    private record LiveOrderTask(
            OrderCommand command,
            TradeDirection side,
            boolean isBuy,
            int limitPriceMc,
            double normalizedShares,
            long submitTimestampMs,
            String clobTokenId
    ) {}

    private record CompletedExecution(
            ExecutionReport report,
            long reportTimestampMs,
            long rttMs
    ) {}

    private record LocalRestFill(
            String clobOrderId,
            boolean isBuy,
            double shares,
            double price,
            double signedDeltaUsd
    ) {}

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
    private final CoreTradingEngine coreEngine;
    private final SimulatedClobExecutionEngine paperExecutionEngine;
    private final OrderExecutionGateway paperExecutionGateway;
    private final OrderExecutionGateway liveExecutionGateway;
    private final EngineClobBookSink clobBookSink;
    private final String symbol;
    private final String timeframe;
    private final long durationSeconds;
    private final boolean streamsEnabled;
    private final String orderType;
    private final long maxQuoteAgeMs;
    private final long orderLatencyMs;

    private final Object strategyLock = new Object();
    private final AtomicReference<PolymarketMarketInfo> activeMarketInfo = new AtomicReference<>();
    private final AtomicReference<PolymarketMarketInfo> prefetchedNextMarketInfo = new AtomicReference<>();
    private final AtomicLong polyBoltMarketMsgCount = new AtomicLong(0L);
    private final AtomicLong clobMarketMsgCount = new AtomicLong(0L);
    private final AtomicLong lastEvaluatedMarketCycleStart = new AtomicLong(0L);
    private final AtomicLong debugPolyBoltBatchCounter = new AtomicLong(0L);
    private final AtomicLong debugClobBatchCounter = new AtomicLong(0L);
    private final AtomicBoolean balanceSyncRequested = new AtomicBoolean(false);
    private final AtomicBoolean balanceSyncInFlight = new AtomicBoolean(false);

    // 4-Core Pipeline Coordination Queues & Staged Book Timestamp
    private final AtomicBoolean workersRunning = new AtomicBoolean(false);
    private final AtomicLong stagedBookEvalTsMs = new AtomicLong(0L);
    private final AtomicLong conflatedQuotesCount = new AtomicLong(0L);
    private final ConcurrentLinkedQueue<LiveOrderTask> orderExecutionQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<CompletedExecution> executionReportQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Runnable> paperTimerTaskQueue = new ConcurrentLinkedQueue<>();

    // Dedicated Worker Threads (vCPU #0, #2, #3; vCPU #1 is owned by MarketWebSocketRuntime) & Busy Time Accumulators
    private volatile Thread clobUserWorkerThread;
    private volatile Thread marketStrategyThread;
    private volatile Thread orderExecutionWorkerThread;
    private volatile Thread telemetryMaintenanceThread;
    private final AtomicLong userWorkerBusyNs = new AtomicLong(0L);
    private final AtomicLong marketStrategyWorkerBusyNs = new AtomicLong(0L);
    private final AtomicLong orderExecutionWorkerBusyNs = new AtomicLong(0L);

    private final Map<String, Double> appliedUserTradeDeltas = new LinkedHashMap<>(128, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Double> eldest) {
            return size() > MAX_TRACKED_USER_TRADES;
        }
    };
    private final Deque<LocalRestFill> pendingWsLocalFills = new ArrayDeque<>(16);

    private volatile StrategyMode strategyMode;
    private volatile boolean polyBoltStartedByEngine = false;
    private volatile String subscribedStrategyMarketSlug = null;
    private volatile double liveInitialCapital = 0.0;
    private volatile PolymarketCredentials credentials;
    private volatile PolymarketEip712Signer eip712Signer;
    private volatile double candleHigh = 0.0;
    private volatile double candleLow = 0.0;

    /**
     * Constructs the unified Strategy Trading Engine backed by {@link CoreTradingEngine}.
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
     * @param spotStreamProcessor    CDI processor parsing Binance, Kraken, and Coinbase spot trades into shared indicators
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
     * @param orderLatencyMs         simulated CLOB gateway delay in milliseconds in {@code PAPER} mode (defaults to {@code 220})
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
            @ConfigProperty(name = "polymarket.paper.order-latency-ms", defaultValue = "220")
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
        TradingStrategy strategy = strategyRegistry.getStrategy(resolvedName);
        double startingBalance = this.strategyMode == StrategyMode.PAPER ? paperRepository.getCurrentBalance() : 0.0;

        PolymarketOrderBook sharedOrderBook = new PolymarketOrderBook();
        this.coreEngine = new CoreTradingEngine(
                sharedOrderBook,
                spotStreamProcessor.getIndicator(),
                spotStreamProcessor.getFastSpotMicroIndicator(),
                strategy,
                this.symbol,
                startingBalance
        );
        this.coreEngine.setMaxQuoteAgeMs(this.maxQuoteAgeMs);
        this.coreEngine.setTakerFeeRate(TAKER_FEE_RATE);
        this.coreEngine.setEnforceClobMinimums(true);
        this.coreEngine.setTradingEnabled(this.strategyMode != StrategyMode.OFF);
        this.coreEngine.setTradeLifecycleListener(new EngineTradeLifecycleBridge());

        this.clobBookSink = new EngineClobBookSink();
        this.paperExecutionEngine = new SimulatedClobExecutionEngine(sharedOrderBook, this.orderLatencyMs, 1.0);
        this.paperExecutionEngine.bind(this::schedulePaperSimulatedAction, this::onPaperSimulatedReport);
        this.paperExecutionGateway = new OrderExecutionGateway() {
            @Override
            public void executeOrder(
                    OrderCommand command,
                    TradeDirection side,
                    boolean isBuy,
                    int limitPriceMc,
                    double normalizedShares,
                    long submitTimestampMs) {
                logOrderSubmissionDiagnostics("PAPER", command, side, isBuy, limitPriceMc, normalizedShares);
                paperExecutionEngine.executeOrder(command, side, isBuy, limitPriceMc, normalizedShares, submitTimestampMs);
            }

            @Override
            public void cancelOrder(String clientOrderId, long cancelTimestampMs) {
                ExecutionReport report = new ExecutionReport(
                        clientOrderId,
                        "sim-cancel-" + clientOrderId,
                        "CANCELLED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        cancelTimestampMs,
                        "Order cancelled in paper mode"
                );
                coreEngine.onExecutionReport(report, cancelTimestampMs, 0L);
            }
        };
        this.liveExecutionGateway = new LiveClobExecutionGateway();
        updateActiveExecutionGateway(this.strategyMode);

        this.clobWsClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.polyBoltClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.binanceConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.krakenConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.coinbaseConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
    }

    private void updateActiveExecutionGateway(StrategyMode mode) {
        synchronized (strategyLock) {
            if (mode == StrategyMode.PAPER) {
                coreEngine.setTradingEnabled(true);
                coreEngine.setExecutionGateway(paperExecutionGateway);
            } else if (mode == StrategyMode.LIVE) {
                coreEngine.setTradingEnabled(true);
                coreEngine.setExecutionGateway(liveExecutionGateway);
            } else {
                coreEngine.setTradingEnabled(false);
                coreEngine.setExecutionGateway(null);
            }
        }
    }

    private void schedulePaperSimulatedAction(long targetTimestampMs, Runnable action) {
        if (vertx == null || orderLatencyMs <= 0L) {
            action.run();
            return;
        }
        long nowRefMs;
        synchronized (strategyLock) {
            long clockMs = coreEngine.getCurrentClockMs();
            long wallNow = System.currentTimeMillis();
            nowRefMs = (clockMs > 0L && Math.abs(wallNow - clockMs) >= REALTIME_CLOCK_WINDOW_MS)
                    ? clockMs
                    : wallNow;
        }
        long delayMs = Math.max(1L, targetTimestampMs - nowRefMs);
        vertx.setTimer(delayMs, id -> {
            if (this.strategyMode != StrategyMode.PAPER) {
                return;
            }
            Thread stratWorker = this.marketStrategyThread;
            if (workersRunning.get() && stratWorker != null) {
                paperTimerTaskQueue.offer(action);
                LockSupport.unpark(stratWorker);
            } else {
                synchronized (strategyLock) {
                    action.run();
                }
            }
        });
    }

    private void onPaperSimulatedReport(ExecutionReport report, long reportTimestampMs, long totalRttMs) {
        synchronized (strategyLock) {
            if (this.strategyMode != StrategyMode.PAPER) {
                return;
            }
            coreEngine.onExecutionReport(report, reportTimestampMs, totalRttMs);
            logTerminalExecutionDecision("PAPER", report, totalRttMs);
        }
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
        updateActiveExecutionGateway(mode);
        if (mode == StrategyMode.LIVE) {
            initializeCredentialsAndSigner();
            if (streamsEnabled) {
                syncAccountBalanceFromClob();
            }
        } else if (mode == StrategyMode.PAPER) {
            synchronized (strategyLock) {
                if (coreEngine.getOpenPositions().isEmpty()) {
                    coreEngine.resetAccountState(paperRepository.getCurrentBalance(), false, false);
                }
            }
        }

        Log.infof("Initializing StrategyTradingEngine in mode=%s with strategy=%s (symbol=%s, timeframe=%s, durationSec=%d, orderType=%s, maxQuoteAgeMs=%d, orderLatencyMs=%d, initialCapital=$%.2f, currentBalance=$%.2f)",
                mode, getActiveStrategyName(), symbol, timeframe, durationSeconds, orderType, maxQuoteAgeMs, orderLatencyMs,
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
            updateActiveExecutionGateway(StrategyMode.OFF);
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
            orderExecutionQueue.clear();
            executionReportQueue.clear();
            paperTimerTaskQueue.clear();
            double modeBalance = targetMode == StrategyMode.PAPER
                    ? paperRepository.getCurrentBalance()
                    : coreEngine.getCashBalanceUsd();
            coreEngine.resetAccountState(modeBalance, true, false);
            long existingCandleStart = coreEngine.getActiveCandleStartSec();
            if (existingCandleStart > 0L && existingCandleStart != currentCycleStartSec) {
                coreEngine.clearCandleBounds();
                candleHigh = 0.0;
                candleLow = 0.0;
            } else {
                coreEngine.replayActiveCandleTargetPriceIfPresent();
            }
        }

        this.strategyMode = targetMode;
        updateActiveExecutionGateway(targetMode);
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
        paperTimerTaskQueue.clear();
        stagedBookEvalTsMs.set(0L);

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
        synchronized (strategyLock) {
            return new RuntimeStateSnapshot(
                    System.currentTimeMillis(),
                    symbol,
                    timeframe,
                    strategyMode,
                    coreEngine.getActiveCandleStartSec(),
                    coreEngine.getActiveCandleEndSec(),
                    coreEngine.getTwapOpenPrice(),
                    market != null ? market.slug() : "",
                    market != null ? market.conditionId() : "",
                    market != null ? market.yesTokenId() : "",
                    market != null ? market.noTokenId() : "",
                    coreEngine.getLastPolyBoltTwapOracleTsMs(),
                    coreEngine.getLastPolyBoltPriceOracleTsMs(),
                    coreEngine.getCurrentSpotPrice(),
                    candleHigh,
                    candleLow,
                    getInitialCapital(),
                    getCurrentBalance(),
                    List.copyOf(coreEngine.getOpenPositions())
            );
        }
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
            updateActiveExecutionGateway(this.strategyMode);
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
            candleHigh = snapshot.candleHigh();
            candleLow = snapshot.candleLow();

            String yesToken = snapshot.yesTokenId();
            String noToken = snapshot.noTokenId();
            if (!snapshot.activeMarketSlug().isBlank() && !yesToken.isBlank() && !noToken.isBlank()) {
                PolymarketMarketInfo restoredMarket = new PolymarketMarketInfo(
                        snapshot.activeMarketSlug(),
                        snapshot.conditionId(),
                        snapshot.activeCandleStartSec(),
                        savedEndSec,
                        List.of(yesToken, noToken),
                        false,
                        null
                );
                this.activeMarketInfo.set(restoredMarket);
                this.clobBookSink.updateTokens(yesToken, noToken);
            }

            boolean isLive = this.strategyMode == StrategyMode.LIVE;
            if (isLive && snapshot.initialCapital() > 0.0 && this.liveInitialCapital <= 0.0) {
                this.liveInitialCapital = snapshot.initialCapital();
            }
            double restoredBalance = isLive
                    ? (snapshot.currentBalance() > 0.0 ? snapshot.currentBalance() : coreEngine.getCashBalanceUsd())
                    : paperRepository.getCurrentBalance();

            long spotTsMs = Math.max(snapshot.activeCandleStartSec() * 1_000L + 1_000L, snapshot.savedAtTimestampMs());
            coreEngine.restoreActiveCandleSnapshot(
                    snapshot.activeCandleStartSec(),
                    savedEndSec,
                    snapshot.twapOpenPrice(),
                    snapshot.currentSpotPrice(),
                    snapshot.lastTwapOracleTimestampMs(),
                    Math.max(snapshot.lastCryptoPriceOracleTimestampMs(), spotTsMs),
                    yesToken,
                    noToken,
                    restoredBalance,
                    isLive,
                    snapshot.openPositions()
            );
        }
        Log.infof("Restored active candle [%d, %d] from RuntimeStateSnapshot: mode=%s, slug=%s, twapOpenPrice=$%.2f, openPositions=%d",
                snapshot.activeCandleStartSec(), savedEndSec, this.strategyMode,
                snapshot.activeMarketSlug(), snapshot.twapOpenPrice(), getOpenPositions().size());
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
     * Fused single-core L1-cache loop for vCPU #2 ({@code market-strategy-worker}): drains execution reports,
     * paper latency callbacks, CLOB L2 orderbook batches, PolyBolt oracle ticks, and Binance/Kraken/Coinbase
     * spot trades, then evaluates {@link CoreTradingEngine#onOrderBookUpdated(long)} on the same core.
     */
    private void marketStrategyWorkerLoop() {
        LinuxCpuAffinity.pinCurrentThreadIfSupported(2, "vCPU #2: Book & Strategy");
        List<TimestampedFrame> scratchBatch = new ArrayList<>(64);
        while (workersRunning.get()) {
            boolean didWork = false;
            long startNs = System.nanoTime();

            Runnable timerTask;
            while ((timerTask = paperTimerTaskQueue.poll()) != null) {
                didWork = true;
                synchronized (strategyLock) {
                    timerTask.run();
                }
            }

            CompletedExecution completed;
            while ((completed = executionReportQueue.poll()) != null) {
                didWork = true;
                synchronized (strategyLock) {
                    coreEngine.onExecutionReport(completed.report(), completed.reportTimestampMs(), completed.rttMs());
                    logTerminalExecutionDecision("LIVE", completed.report(), completed.rttMs());
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

            long stagedTs = stagedBookEvalTsMs.getAndSet(0L);
            if (stagedTs > 0L) {
                didWork = true;
                long evalNowMs = System.currentTimeMillis();
                synchronized (strategyLock) {
                    coreEngine.onOrderBookUpdated(Math.max(stagedTs, evalNowMs));
                }
            }

            if (didWork) {
                marketStrategyWorkerBusyNs.addAndGet(Math.max(0L, System.nanoTime() - startNs));
                continue;
            }

            ReconnectingWebSocketStream.spinWaitBeforePark(() -> !workersRunning.get()
                    || !paperTimerTaskQueue.isEmpty()
                    || !executionReportQueue.isEmpty()
                    || clobWsClient.hasQueuedMarketFrames()
                    || polyBoltClient.hasQueuedFrames()
                    || binanceConnector.hasQueuedFrames()
                    || krakenConnector.hasQueuedFrames()
                    || coinbaseConnector.hasQueuedFrames()
                    || stagedBookEvalTsMs.get() > 0L);
            if (!workersRunning.get()
                    || !paperTimerTaskQueue.isEmpty()
                    || !executionReportQueue.isEmpty()
                    || clobWsClient.hasQueuedMarketFrames()
                    || polyBoltClient.hasQueuedFrames()
                    || binanceConnector.hasQueuedFrames()
                    || krakenConnector.hasQueuedFrames()
                    || coinbaseConnector.hasQueuedFrames()
                    || stagedBookEvalTsMs.get() > 0L) {
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
                    || !paperTimerTaskQueue.isEmpty()
                    || !executionReportQueue.isEmpty()
                    || stagedBookEvalTsMs.get() > 0L) {
                continue;
            }
            LockSupport.parkNanos(this, ReconnectingWebSocketStream.IDLE_PARK_TIMEOUT_NS);
        }
    }

    /**
     * Dedicated isolated loop for vCPU #3 ({@code order-execution-io-worker}): exclusively executes EIP-712 signing
     * and HTTP {@code POST /order} calls without interference from WebSocket streams, GC, or IRQs.
     */
    private void orderExecutionWorkerLoop() {
        LinuxCpuAffinity.pinCurrentThreadIfSupported(3, "vCPU #3: Order Execution");
        while (workersRunning.get()) {
            boolean didWork = false;
            long startNs = System.nanoTime();

            LiveOrderTask task;
            while ((task = orderExecutionQueue.poll()) != null) {
                didWork = true;
                executeLiveOrderTask(task, true);
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
     * `GET /balance-allowance` REST synchronization, and off-hot-path {@link RuntimeStateSnapshot} backup.
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
                long curCandleStart;
                long curTwapBits;
                int curPosCount;
                synchronized (strategyLock) {
                    curCandleStart = coreEngine.getActiveCandleStartSec();
                    curTwapBits = Double.doubleToLongBits(coreEngine.getTwapOpenPrice());
                    curPosCount = coreEngine.getOpenPositions().size();
                }
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
     * and reconciles the live USDC account balance without double-counting local REST fills.
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
                            previousDelta, tradeId, getCurrentBalance());
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
        boolean isBuy = "BUY".equalsIgnoreCase(side);
        boolean isSell = "SELL".equalsIgnoreCase(side);
        if (size <= 0.0 || price <= 0.0 || (!isBuy && !isSell)) {
            return;
        }

        double notionalUsd = size * price;
        double feeRate = event.feeRateBps() >= 0 ? (event.feeRateBps() / 10_000.0) : TAKER_FEE_RATE;
        double feeUsd = (price > 0.0 && price < 1.0)
                ? (size * feeRate * price * (1.0 - price))
                : 0.0;
        double signedDeltaUsd = isBuy ? -(notionalUsd + feeUsd) : (notionalUsd - feeUsd);

        if (!tradeId.isEmpty() && !recordAppliedTradeDeltaIfAbsent(tradeId, signedDeltaUsd)) {
            return;
        }

        LocalRestFill matchedLocalFill = consumeMatchingLocalRestFill(event.orderId(), isBuy, size, price);
        if (matchedLocalFill != null) {
            double feeAdjustment = signedDeltaUsd - matchedLocalFill.signedDeltaUsd();
            if (Math.abs(feeAdjustment) > 1e-6) {
                applyAccountBalanceDelta(feeAdjustment);
            }
            Log.infof("Confirmed WS user trade %s for local REST order (%s %.2f shares @ %.4f, fee=$%.4f): balance=$%.2f",
                    tradeId, side.toUpperCase(Locale.ROOT), size, price, feeUsd, getCurrentBalance());
            return;
        }

        applyAccountBalanceDelta(signedDeltaUsd);
        Log.infof("Updated live balance from WS user trade %s (%s %.2f shares @ %.4f, fee=$%.4f, delta=$%.2f): balance=$%.2f",
                tradeId, side.toUpperCase(Locale.ROOT), size, price, feeUsd, signedDeltaUsd, getCurrentBalance());
    }

    private synchronized void recordLocalRestFill(String clobOrderId, boolean isBuy, double shares, double price, double signedDeltaUsd) {
        while (pendingWsLocalFills.size() >= MAX_LOCAL_REST_FILLS) {
            pendingWsLocalFills.pollFirst();
        }
        pendingWsLocalFills.addLast(new LocalRestFill(
                clobOrderId != null ? clobOrderId : "",
                isBuy,
                shares,
                price,
                signedDeltaUsd
        ));
    }

    private synchronized LocalRestFill consumeMatchingLocalRestFill(String orderId, boolean isBuy, double shares, double price) {
        String cleanOrderId = orderId != null ? orderId.trim() : "";
        Iterator<LocalRestFill> it = pendingWsLocalFills.iterator();
        while (it.hasNext()) {
            LocalRestFill candidate = it.next();
            if (!cleanOrderId.isEmpty() && !candidate.clobOrderId().isEmpty()) {
                if (cleanOrderId.equals(candidate.clobOrderId())) {
                    it.remove();
                    return candidate;
                }
                continue;
            }
            if (candidate.isBuy() == isBuy
                    && Math.abs(candidate.shares() - shares) <= 1e-3
                    && Math.abs(candidate.price() - price) <= 2e-3) {
                it.remove();
                return candidate;
            }
        }
        return null;
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
        if (this.liveInitialCapital <= 0.0) {
            this.liveInitialCapital = accountBalanceUsd;
        }
        synchronized (strategyLock) {
            if (this.strategyMode != StrategyMode.PAPER) {
                coreEngine.syncLiveCollateralBalanceUsd(accountBalanceUsd);
            }
        }
    }

    /**
     * Applies a signed balance delta to the in-memory live account balance.
     *
     * @param deltaUsd signed USD balance change
     */
    public synchronized void applyAccountBalanceDelta(double deltaUsd) {
        if (Double.isFinite(deltaUsd) && deltaUsd != 0.0) {
            synchronized (strategyLock) {
                if (this.strategyMode != StrategyMode.PAPER) {
                    coreEngine.applyLiveBalanceDeltaUsd(deltaUsd);
                }
            }
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
                    getCurrentBalance(), liveInitialCapital);
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
            synchronized (strategyLock) {
                if (coreEngine.getActiveCandleStartSec() == 0L) {
                    coreEngine.beginCandleCycle(
                            nextMarket.startSec(),
                            nextMarket.endSec(),
                            0.0,
                            nextMarket.yesTokenId(),
                            nextMarket.noTokenId(),
                            true
                    );
                } else {
                    coreEngine.setActiveMarketTokens(nextMarket.yesTokenId(), nextMarket.noTokenId(), true);
                }
                clobBookSink.updateTokens(nextMarket.yesTokenId(), nextMarket.noTokenId());
            }
            this.stagedBookEvalTsMs.set(0L);
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
        if (marketStartSec > 0L && !tryAdvanceMonotonic(lastEvaluatedMarketCycleStart, marketStartSec)) {
            return;
        }
        long polyBoltCount = polyBoltMarketMsgCount.getAndSet(0L);
        long clobCount = clobMarketMsgCount.getAndSet(0L);

        if (polyBoltCount == 0L || clobCount == 0L) {
            Log.errorf("Market %s completed with missing WebSocket stream messages (polyBoltMessages=%d, clobWsMessages=%d)",
                    marketLabel, polyBoltCount, clobCount);
        }
        Log.infof("Market %s summary statistics: mode=%s, polyBoltMessages=%d, clobWsMessages=%d, twapOpen=%.2f, lastSpot=%.2f, high=%.2f, low=%.2f, balance=$%.2f",
                marketLabel, strategyMode, polyBoltCount, clobCount, getTwapOpenPrice(), getCurrentSpotPrice(), candleHigh, candleLow, getCurrentBalance());
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
                onPolymarketCryptoTwap60s(
                        tick.timestampMs(),
                        tick.symbol(),
                        tick.source(),
                        tick.price(),
                        tick.windowSeconds(),
                        tick.oracleTimestampMs()
                );
            } else {
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

    private static boolean tryAdvanceMonotonic(AtomicLong tracker, long candidate) {
        if (candidate <= 0L) {
            return true;
        }
        while (true) {
            long prev = tracker.get();
            if (candidate <= prev) {
                return false;
            }
            if (tracker.compareAndSet(prev, candidate)) {
                return true;
            }
        }
    }

    /**
     * Processes a batch of raw CLOB JSON frames on {@code market-strategy-worker} ({@code CPU #2}),
     * fast-forwarding past superseded dual snapshots, mutating {@link PolymarketOrderBook} directly via
     * {@link ClobBookSink}, and staging book evaluation for same-core execution after CEX/PolyBolt drains.
     *
     * @param batch chronological batch of CLOB frames drained from the inbound queue
     */
    public void onRawClobFrameBatch(List<TimestampedFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        PolymarketStreamParser.ClobBatchResult result;
        synchronized (strategyLock) {
            result = streamParser.applyClobFrameBatch(batch, clobBookSink);
        }
        if (result.fastForwardedFramesCount() > 0) {
            PolymarketMarketInfo info = activeMarketInfo.get();
            if (info != null) {
                clobWsClient.addMarketCoalescedFrames(info.slug(), result.fastForwardedFramesCount());
            }
        }
        if (result.appliedFramesCount() > 0) {
            long marketTotal = clobMarketMsgCount.addAndGet(result.appliedFramesCount());
            if (Log.isDebugEnabled()) {
                long debugCount = debugClobBatchCounter.incrementAndGet();
                if (debugCount % DEBUG_MESSAGE_LOG_INTERVAL == 0L) {
                    PolymarketOrderBook book = coreEngine.getOrderBook();
                    Log.debugf("Processed %d CLOB WS batches (marketMessages=%d, bestBidUp=%.4f, bestAskUp=%.4f, depth1cUp=%.1f, depth1cDown=%.1f)",
                            debugCount, marketTotal, book.getBestBidUp(), book.getBestAskUp(), book.getDepth1cUp(), book.getDepth1cDown());
                }
            }
            if (result.updated()) {
                stageOrEvaluateBookUpdate(result.lastUpdatedRecvTs());
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
        boolean updated;
        synchronized (strategyLock) {
            updated = streamParser.applyClobFrame(rawJson, recvTimestampMs, clobBookSink);
        }
        if (updated) {
            clobMarketMsgCount.incrementAndGet();
            stageOrEvaluateBookUpdate(recvTimestampMs);
        }
    }

    private void stageOrEvaluateBookUpdate(long recvTimestampMs) {
        long safeTs = recvTimestampMs > 0L ? recvTimestampMs : System.currentTimeMillis();
        Thread stratWorker = this.marketStrategyThread;
        if (workersRunning.get() && stratWorker != null && Thread.currentThread() == stratWorker) {
            long prev = stagedBookEvalTsMs.getAndSet(safeTs);
            if (prev > 0L) {
                conflatedQuotesCount.incrementAndGet();
            }
        } else {
            long wallNowMs = System.currentTimeMillis();
            long evalTs = Math.abs(wallNowMs - safeTs) < REALTIME_CLOCK_WINDOW_MS
                    ? Math.max(safeTs, wallNowMs)
                    : safeTs;
            synchronized (strategyLock) {
                coreEngine.onOrderBookUpdated(evalTs);
            }
        }
    }

    @Override
    public ExecutionRouter getExecutionRouter() {
        return this;
    }

    @Override
    public double getAccountBalanceUsd() {
        synchronized (strategyLock) {
            return coreEngine.getAccountBalanceUsd();
        }
    }

    @Override
    public double estimateBuyFillableShares(TradeDirection side, double limitPrice) {
        synchronized (strategyLock) {
            return coreEngine.estimateBuyFillableShares(side, limitPrice);
        }
    }

    @Override
    public double estimateBuyFillVwap(TradeDirection side, double requestedShares, double limitPrice) {
        synchronized (strategyLock) {
            return coreEngine.estimateBuyFillVwap(side, requestedShares, limitPrice);
        }
    }

    @Override
    public void submitOrder(OrderCommand command) {
        if (command == null) {
            return;
        }
        if (this.strategyMode == StrategyMode.OFF) {
            Log.warnf("Ignoring submitOrder(%s) because StrategyMode is OFF", command.clientOrderId());
            return;
        }
        synchronized (strategyLock) {
            coreEngine.submitOrder(command);
        }
    }

    @Override
    public void cancelOrder(String clientOrderId) {
        if (clientOrderId == null || clientOrderId.isBlank() || this.strategyMode == StrategyMode.OFF) {
            return;
        }
        synchronized (strategyLock) {
            coreEngine.cancelOrder(clientOrderId);
        }
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
            long start = coreEngine.getActiveCandleStartSec();
            long end = coreEngine.getActiveCandleEndSec();
            double open = coreEngine.getTwapOpenPrice() > 0.0 ? coreEngine.getTwapOpenPrice() : finalPrice;
            double high = candleHigh > 0.0 ? Math.max(candleHigh, Math.max(open, finalPrice)) : Math.max(open, finalPrice);
            double low = candleLow > 0.0 ? Math.min(candleLow, Math.min(open, finalPrice)) : Math.min(open, finalPrice);
            int openBefore = coreEngine.getOpenPositions().size();
            long settleTs = coreEngine.getCurrentClockMs() > 0L ? coreEngine.getCurrentClockMs() : System.currentTimeMillis();

            coreEngine.resolveCandle(actualOutcome, finalPrice, settleTs);

            Log.infof("Resolved market candle [%d, %d] (%s): outcome=%s, open=%.2f, close=%.2f, high=%.2f, low=%.2f, settledPositions=%d, balance=$%.2f",
                    start, end, strategyMode, actualOutcome, open, finalPrice, high, low, openBefore, getCurrentBalance());
            candleHigh = 0.0;
            candleLow = 0.0;
        }
    }

    /**
     * Processes a PolyBolt {@code price.crypto.twap} (60s) update, delegating monotonic filtering and
     * 15m candle boundary resolution to {@link CoreTradingEngine}.
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
        long effectiveTsMs = twapTimestampMs > 0L ? twapTimestampMs : timestampMs;
        long candleStartSec = (effectiveTsMs / 1_000L / durationSeconds) * durationSeconds;
        long offsetFromBoundaryMs = Math.abs(effectiveTsMs - candleStartSec * 1_000L);

        synchronized (strategyLock) {
            long prevCandleStart = coreEngine.getActiveCandleStartSec();
            if (prevCandleStart > 0L
                    && candleStartSec == prevCandleStart + durationSeconds
                    && offsetFromBoundaryMs <= MAX_BOUNDARY_TWAP_OFFSET_MS) {
                PolymarketMarketInfo currentMarket = activeMarketInfo.get();
                String marketLabel = (currentMarket != null && currentMarket.startSec() == prevCandleStart)
                        ? currentMarket.slug()
                        : PolymarketMarketInfo.buildMarketSlug(this.symbol, this.timeframe, prevCandleStart);
                evaluateCompletedMarketStreamActivity(marketLabel, prevCandleStart);
                candleHigh = 0.0;
                candleLow = 0.0;
            } else if (prevCandleStart > 0L && candleStartSec > prevCandleStart) {
                candleHigh = 0.0;
                candleLow = 0.0;
            }
            coreEngine.onPolyBoltCryptoTwap60s(timestampMs, symbol, source, twapPrice, windowSeconds, twapTimestampMs);
        }
    }

    /**
     * Processes a PolyBolt {@code price.crypto} 1-second spot price update and forwards it to {@link CoreTradingEngine}.
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
        synchronized (strategyLock) {
            if (coreEngine.onPolyBoltCryptoPrice(timestampMs, symbol, source, price, priceTimestampMs)) {
                if (candleHigh == 0.0 || price > candleHigh) {
                    candleHigh = price;
                }
                if (candleLow == 0.0 || price < candleLow) {
                    candleLow = price;
                }
            }
        }
    }

    /**
     * Processes a batch of forward-progressing Binance Spot {@code aggTrades} frames into shared indicators.
     *
     * @param batch chronological batch of fast-forwarded Binance {@code aggTrade} frames
     */
    public void onRawBinanceFrameBatch(List<TimestampedFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        synchronized (strategyLock) {
            coreEngine.setCurrentClockMs(batch.getLast().recvTs());
            spotStreamProcessor.processBinanceFrameBatch(batch);
        }
    }

    /**
     * Processes a raw JSON text frame from the Kraken Spot v2 {@code trade} WebSocket stream.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawJson         raw Kraken JSON frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawKrakenFrame(String streamSymbol, String rawJson, long recvTimestampMs) {
        synchronized (strategyLock) {
            coreEngine.setCurrentClockMs(recvTimestampMs);
            spotStreamProcessor.processKrakenFrame(streamSymbol, rawJson, recvTimestampMs);
        }
    }

    /**
     * Processes a raw UTF-8 {@code byte[]} frame from the Kraken Spot v2 {@code trade} WebSocket stream.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawKrakenFrameBytes(String streamSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
        synchronized (strategyLock) {
            coreEngine.setCurrentClockMs(recvTimestampMs);
            spotStreamProcessor.processKrakenFrameBytes(streamSymbol, rawUtf8Bytes, recvTimestampMs);
        }
    }

    /**
     * Processes a raw JSON text frame from the Coinbase Spot {@code matches} WebSocket stream.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawJson         raw Coinbase JSON frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawCoinbaseFrame(String streamSymbol, String rawJson, long recvTimestampMs) {
        synchronized (strategyLock) {
            coreEngine.setCurrentClockMs(recvTimestampMs);
            spotStreamProcessor.processCoinbaseFrame(streamSymbol, rawJson, recvTimestampMs);
        }
    }

    /**
     * Processes a raw UTF-8 {@code byte[]} frame from the Coinbase Spot {@code matches} WebSocket stream.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawCoinbaseFrameBytes(String streamSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
        synchronized (strategyLock) {
            coreEngine.setCurrentClockMs(recvTimestampMs);
            spotStreamProcessor.processCoinbaseFrameBytes(streamSymbol, rawUtf8Bytes, recvTimestampMs);
        }
    }

    /**
     * Processes an explicit {@link OrderBookQuote}, enforcing {@code maxQuoteAgeMs} freshness, seeding
     * {@link PolymarketOrderBook}, and dispatching to {@link CoreTradingEngine}.
     *
     * @param quote current BBO quote across Up and Down tokens
     */
    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        long wallNowMs = System.currentTimeMillis();
        synchronized (strategyLock) {
            if (quote.timestampMs() > 0L && Math.abs(wallNowMs - quote.timestampMs()) < REALTIME_CLOCK_WINDOW_MS) {
                coreEngine.setCurrentClockMs(wallNowMs);
            }
            coreEngine.onDirectOrderBookQuote(quote);
        }
    }

    /**
     * Explicitly sets the active market metadata and updates {@link PolymarketOrderBook} token mappings.
     *
     * @param marketInfo active {@link PolymarketMarketInfo}
     */
    public synchronized void setActiveMarketInfo(PolymarketMarketInfo marketInfo) {
        this.activeMarketInfo.set(marketInfo);
        if (marketInfo != null) {
            synchronized (strategyLock) {
                coreEngine.setActiveMarketTokens(marketInfo.yesTokenId(), marketInfo.noTokenId(), true);
                clobBookSink.updateTokens(marketInfo.yesTokenId(), marketInfo.noTokenId());
            }
        }
    }

    /**
     * Resets engine state and clears open positions.
     */
    public void reset() {
        synchronized (strategyLock) {
            double resetBalance = strategyMode == StrategyMode.PAPER
                    ? paperRepository.getInitialCapital()
                    : coreEngine.getCashBalanceUsd();
            coreEngine.resetAccountState(resetBalance, true, true);
            spotStreamProcessor.reset();
            polyBoltMarketMsgCount.set(0L);
            clobMarketMsgCount.set(0L);
            lastEvaluatedMarketCycleStart.set(0L);
            debugPolyBoltBatchCounter.set(0L);
            debugClobBatchCounter.set(0L);
            candleHigh = 0.0;
            candleLow = 0.0;
            stagedBookEvalTsMs.set(0L);
            prefetchedNextMarketInfo.set(null);
            orderExecutionQueue.clear();
            executionReportQueue.clear();
            paperTimerTaskQueue.clear();
            if (strategyMode == StrategyMode.LIVE && coreEngine.getCashBalanceUsd() > 0.0) {
                liveInitialCapital = coreEngine.getCashBalanceUsd();
            }
        }
        synchronized (this) {
            appliedUserTradeDeltas.clear();
            pendingWsLocalFills.clear();
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

        long submitted;
        long filled;
        long rejected;
        long staleSkipped;
        long lastAgeMs;
        long maxAgeMs;
        long lastRttMs;
        long maxRttMs;
        synchronized (strategyLock) {
            submitted = coreEngine.getOrdersSubmitted();
            filled = coreEngine.getOrdersFilled();
            rejected = coreEngine.getOrdersRejected();
            staleSkipped = coreEngine.getStaleQuotesSkipped();
            lastAgeMs = coreEngine.getLastQuoteAgeMs();
            maxAgeMs = coreEngine.getMaxQuoteAgeMsObserved();
            lastRttMs = coreEngine.getLastOrderRttMs();
            maxRttMs = coreEngine.getMaxOrderRttMs();
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
                submitted,
                filled,
                rejected,
                staleSkipped,
                conflatedQuotesCount.get(),
                lastAgeMs,
                maxAgeMs,
                lastRttMs,
                maxRttMs,
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
        long lastQuoteAge;
        synchronized (strategyLock) {
            lastQuoteAge = coreEngine.getLastQuoteAgeMs();
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
                lastQuoteAge,
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
     * Returns the underlying shared {@link CoreTradingEngine}.
     *
     * @return shared {@link CoreTradingEngine}
     */
    public CoreTradingEngine getCoreEngine() {
        return coreEngine;
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
        if (strategyMode == StrategyMode.PAPER) {
            return paperRepository.getCurrentBalance();
        }
        synchronized (strategyLock) {
            return coreEngine.getCashBalanceUsd();
        }
    }

    /**
     * Returns the captured 60-second Chainlink TWAP target price for the active 15m candle.
     *
     * @return target price in USD (or {@code 0.0} if not yet locked)
     */
    public double getTwapOpenPrice() {
        synchronized (strategyLock) {
            return coreEngine.getTwapOpenPrice();
        }
    }

    /**
     * Returns the configured simulated CLOB order gateway latency in milliseconds (used in {@code PAPER} mode).
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
        return coreEngine.getActiveStrategy().getName();
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
        synchronized (strategyLock) {
            return coreEngine.getLatestOpenPosition();
        }
    }

    /**
     * Returns an unmodifiable snapshot of all open positions in the active candle.
     *
     * @return open positions
     */
    public List<Position> getOpenPositions() {
        synchronized (strategyLock) {
            return List.copyOf(coreEngine.getOpenPositions());
        }
    }

    /**
     * Returns the latest normalized orderbook quote dispatched to the strategy, or {@code null} if none received yet.
     *
     * @return latest {@link OrderBookQuote}
     */
    public OrderBookQuote getLatestQuote() {
        synchronized (strategyLock) {
            return coreEngine.getLatestDispatchedQuote();
        }
    }

    /**
     * Lazily returns the current effective 1-cent L2 ask depth for the UP token in shares (zero hot-path overhead).
     *
     * @return 1-cent UP ask depth in shares
     */
    public double getDepth1cUp() {
        OrderBookQuote q = getLatestQuote();
        return q != null ? q.depth1cUp() : 0.0;
    }

    /**
     * Lazily returns the current effective 1-cent L2 ask depth for the DOWN token in shares (zero hot-path overhead).
     *
     * @return 1-cent DOWN ask depth in shares
     */
    public double getDepth1cDown() {
        OrderBookQuote q = getLatestQuote();
        return q != null ? q.depth1cDown() : 0.0;
    }

    /**
     * Returns the latest observed spot price in USD.
     *
     * @return spot price in USD
     */
    public double getCurrentSpotPrice() {
        synchronized (strategyLock) {
            return coreEngine.getCurrentSpotPrice();
        }
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

    private void logOrderSubmissionDiagnostics(
            String modeLabel,
            OrderCommand command,
            TradeDirection side,
            boolean isBuy,
            int limitPriceMc,
            double normalizedShares) {
        PolymarketOrderBook book = coreEngine.getOrderBook();
        boolean isUp = side == TradeDirection.UP;
        double limitPrice = limitPriceMc * 1e-3;
        double bestAsk = isUp ? book.getBestAskUp() : book.getBestAskDown();
        double bestBid = isUp ? book.getBestBidUp() : book.getBestBidDown();
        double depth1c = isUp ? book.getDepth1cUp() : book.getDepth1cDown();
        double fillableSh = isBuy ? book.estimateBuyFillableShares(isUp, limitPrice) : normalizedShares;
        double estFillVwap = isBuy ? book.estimateBuyFillVwap(isUp, normalizedShares, limitPrice) : bestBid;
        double pUp = coreEngine.getActiveStrategy().getLastModelProbabilityUp();
        double modelProb = (Double.isFinite(pUp) && pUp > 0.0 && pUp < 1.0)
                ? (isUp ? pUp : (1.0 - pUp))
                : Double.NaN;
        double edge = (Double.isFinite(modelProb) && Double.isFinite(estFillVwap))
                ? (modelProb - estFillVwap)
                : Double.NaN;
        Log.infof("[%s] Submitting %s order %s (%s): limit=%.3f, shares=%.2f, bestBid=%.4f, bestAsk=%.4f, depth1c=%.1f, fillableSh=%.1f, estFillVwap=%.4f, modelProb=%.4f, edge=%.4f, quoteAgeMs=%d, wsOneWayMs=%d",
                modeLabel,
                isBuy ? "BUY" : "SELL",
                command.clientOrderId(),
                side,
                limitPrice,
                normalizedShares,
                bestBid,
                bestAsk,
                depth1c,
                fillableSh,
                estFillVwap,
                modelProb,
                edge,
                coreEngine.getLastQuoteAgeMs(),
                book.getLastObservedOneWayLatencyMs());
    }

    private static void logTerminalExecutionDecision(String modeLabel, ExecutionReport report, long rttMs) {
        if ("FILLED".equalsIgnoreCase(report.status()) || "PARTIALLY_FILLED".equalsIgnoreCase(report.status())) {
            Log.infof("[%s] Order %s %s (clobOrderId=%s): executedShares=%s @ %s (rtt=%d ms)",
                    modeLabel,
                    report.clientOrderId(),
                    report.status(),
                    report.exchangeOrderId(),
                    report.executedSize(),
                    report.executedPrice(),
                    rttMs);
        } else {
            Log.warnf("[%s] Order %s %s (clobOrderId=%s, rtt=%d ms): %s",
                    modeLabel,
                    report.clientOrderId(),
                    report.status(),
                    report.exchangeOrderId(),
                    rttMs,
                    report.message());
        }
    }

    private void executeLiveOrderTask(LiveOrderTask task, boolean asyncReportDelivery) {
        OrderCommand command = task.command();
        if (task.clobTokenId().isBlank()) {
            Log.warnf("Cannot submit live %s order %s: active market token ID is not resolved",
                    task.isBuy() ? "BUY" : "SELL", command.clientOrderId());
            deliverLiveExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "",
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    "Active market token ID not resolved"
            ), 0L, asyncReportDelivery);
            return;
        }

        long orderStartMs = System.currentTimeMillis();
        try {
            ensureSignerReady();
            ClobOrderEnvelope envelope;
            if (task.isBuy()) {
                long wholeShares = Math.max(1L, Math.round(task.normalizedShares()));
                envelope = eip712Signer.buildSignedBuyOrder(
                        credentials.makerAddress(),
                        credentials.apiKey(),
                        task.clobTokenId(),
                        task.limitPriceMc(),
                        wholeShares,
                        orderType
                );
            } else {
                long microShares = Math.max(1L, Math.round(task.normalizedShares() * 1_000_000.0));
                envelope = eip712Signer.buildSignedSellOrder(
                        credentials.makerAddress(),
                        credentials.apiKey(),
                        task.clobTokenId(),
                        task.limitPriceMc(),
                        microShares,
                        orderType,
                        PolymarketEip712Signer.generateOrderSalt(),
                        orderStartMs
                );
            }

            ClobOrderResponse response = clobRestClient.submitOrder(envelope);
            long finishMs = System.currentTimeMillis();
            long rttMs = Math.max(0L, finishMs - orderStartMs);
            String sideLabel = task.isBuy() ? "BUY" : "SELL";
            double actualShares = response.filledShares(sideLabel);
            String clobOrderId = response.orderID() != null ? response.orderID() : "";

            if (!response.success() || actualShares <= 0.0) {
                String err = (response.errorMsg() != null && !response.errorMsg().isBlank())
                        ? response.errorMsg()
                        : ("Order status: " + response.status());
                deliverLiveExecutionReport(new ExecutionReport(
                        command.clientOrderId(),
                        clobOrderId,
                        "REJECTED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        finishMs,
                        err
                ), rttMs, asyncReportDelivery);
                return;
            }

            int avgMc = response.avgPriceMc(sideLabel);
            double fillPrice = avgMc > 0 ? (avgMc / 1_000.0) : (task.limitPriceMc() / 1_000.0);
            double filledShares = task.isBuy() ? actualShares : Math.min(task.normalizedShares(), actualShares);
            double feeUsd = (fillPrice > 0.0 && fillPrice < 1.0)
                    ? (filledShares * TAKER_FEE_RATE * fillPrice * (1.0 - fillPrice))
                    : 0.0;
            double signedDeltaUsd = task.isBuy()
                    ? -(filledShares * fillPrice + feeUsd)
                    : (filledShares * fillPrice - feeUsd);
            recordLocalRestFill(clobOrderId, task.isBuy(), filledShares, fillPrice, signedDeltaUsd);

            deliverLiveExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    clobOrderId,
                    "FILLED",
                    BigDecimal.valueOf(fillPrice),
                    BigDecimal.valueOf(filledShares),
                    finishMs,
                    "Matched on Polymarket CLOB"
            ), rttMs, asyncReportDelivery);
        } catch (Exception e) {
            long finishMs = System.currentTimeMillis();
            long rttMs = Math.max(0L, finishMs - orderStartMs);
            deliverLiveExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "",
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    finishMs,
                    e.getMessage() != null ? e.getMessage() : "CLOB REST failure"
            ), rttMs, asyncReportDelivery);
        }
    }

    private void deliverLiveExecutionReport(ExecutionReport report, long rttMs, boolean asyncReportDelivery) {
        long reportTs = report.timestampMs() > 0L ? report.timestampMs() : System.currentTimeMillis();
        Thread stratWorker = this.marketStrategyThread;
        if (asyncReportDelivery && workersRunning.get() && stratWorker != null && Thread.currentThread() != stratWorker) {
            executionReportQueue.offer(new CompletedExecution(report, reportTs, rttMs));
            LockSupport.unpark(stratWorker);
        } else {
            synchronized (strategyLock) {
                coreEngine.onExecutionReport(report, reportTs, rttMs);
                logTerminalExecutionDecision("LIVE", report, rttMs);
            }
        }
    }

    private final class LiveClobExecutionGateway implements OrderExecutionGateway {
        @Override
        public void executeOrder(
                OrderCommand command,
                TradeDirection side,
                boolean isBuy,
                int limitPriceMc,
                double normalizedShares,
                long submitTimestampMs) {
            logOrderSubmissionDiagnostics("LIVE", command, side, isBuy, limitPriceMc, normalizedShares);
            boolean isDown = side == TradeDirection.DOWN;
            String clobTokenId = "";
            if (!isBuy) {
                for (Position pos : coreEngine.getOpenPositions()) {
                    if (pos.side() == side && !pos.clobTokenId().isBlank()) {
                        clobTokenId = pos.clobTokenId();
                        break;
                    }
                }
            }
            if (clobTokenId.isBlank()) {
                clobTokenId = resolveClobTokenId(command.token(), isDown);
            }

            LiveOrderTask task = new LiveOrderTask(
                    command, side, isBuy, limitPriceMc, normalizedShares, submitTimestampMs, clobTokenId);
            Thread ioWorker = orderExecutionWorkerThread;
            if (workersRunning.get() && ioWorker != null && Thread.currentThread() == marketStrategyThread) {
                orderExecutionQueue.offer(task);
                LockSupport.unpark(ioWorker);
            } else {
                executeLiveOrderTask(task, false);
            }
        }

        @Override
        public void cancelOrder(String clientOrderId, long cancelTimestampMs) {
            String clobOrderId = clientOrderId;
            for (Position pos : coreEngine.getOpenPositions()) {
                if (clientOrderId.equals(pos.clientOrderId()) && !pos.clobOrderId().isBlank()) {
                    clobOrderId = pos.clobOrderId();
                    break;
                }
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
            coreEngine.onExecutionReport(report, report.timestampMs(), 0L);
        }
    }

    private final class EngineTradeLifecycleBridge implements CoreTradingEngine.TradeLifecycleListener {
        @Override
        public void onPositionOpened(Position position, long rttMs) {
            Log.infof("Opened %s position: %s (clobOrderId=%s) %s %.2f shares @ %.4f ($%.2f, fee: $%.4f, modelProb=%.4f, edge=%.4f, rtt=%d ms)",
                    strategyMode,
                    position.clientOrderId(),
                    position.clobOrderId(),
                    position.side(),
                    position.shares(),
                    position.entryPrice(),
                    position.sizeUsd(),
                    position.entryFee(),
                    position.modelProbability(),
                    position.edge(),
                    rttMs);
        }

        @Override
        public void onTradeClosed(TradeRecord tradeRecord) {
            if (strategyMode == StrategyMode.PAPER) {
                paperRepository.recordTrade(tradeRecord);
            }
            Log.infof("Closed %s trade: %s @ %.4f -> Net PnL: $%.2f (%s), Balance: $%.2f",
                    strategyMode,
                    tradeRecord.side(),
                    tradeRecord.exitPrice(),
                    tradeRecord.netPnl(),
                    tradeRecord.exitReason(),
                    getCurrentBalance());
        }

        @Override
        public void onBalanceRefreshRequested() {
            refreshAccountBalance();
        }
    }

    private final class EngineClobBookSink implements ClobBookSink {
        private volatile byte[] upTokenBytes = new byte[0];
        private volatile byte[] downTokenBytes = new byte[0];

        void updateTokens(String upTokenId, String downTokenId) {
            this.upTokenBytes = (upTokenId != null && !upTokenId.isBlank())
                    ? upTokenId.getBytes(StandardCharsets.UTF_8)
                    : new byte[0];
            this.downTokenBytes = (downTokenId != null && !downTokenId.isBlank())
                    ? downTokenId.getBytes(StandardCharsets.UTF_8)
                    : new byte[0];
        }

        @Override
        public byte[] getUpTokenBytes() {
            return upTokenBytes;
        }

        @Override
        public byte[] getDownTokenBytes() {
            return downTokenBytes;
        }

        @Override
        public int resolveTokenSide(String assetId) {
            if (assetId == null || assetId.isEmpty()) {
                return -1;
            }
            PolymarketOrderBook book = coreEngine.getOrderBook();
            if (assetId.equals(book.getUpTokenId())) {
                return 1;
            }
            if (assetId.equals(book.getDownTokenId())) {
                return 0;
            }
            return -1;
        }

        @Override
        public boolean tryAcceptServerTimestamp(boolean isUp, long serverTimestampMs) {
            return coreEngine.getOrderBook().tryAcceptServerTimestamp(isUp, serverTimestampMs);
        }

        @Override
        public void updateObservedLatency(long receiptTimestampMs, long serverTimestampMs) {
            coreEngine.getOrderBook().updateObservedLatency(receiptTimestampMs, serverTimestampMs);
        }

        @Override
        public void onBookSnapshotStart(boolean isUp) {
            coreEngine.getOrderBook().clearSideForSnapshot(isUp);
        }

        @Override
        public void onBookSnapshotLevel(boolean isUp, boolean isBid, int tick, double size) {
            coreEngine.getOrderBook().setSnapshotLevel(isUp, isBid, tick, size);
        }

        @Override
        public void onBookSnapshotEnd(boolean isUp, long recvTimestampMs) {
            coreEngine.getOrderBook().finishBookSnapshot(isUp, recvTimestampMs);
        }

        @Override
        public void onPriceChangeLevel(boolean isUp, boolean isBid, int tick, double size, long recvTimestampMs) {
            coreEngine.getOrderBook().applyTickDelta(isUp, isBid, tick, size, recvTimestampMs);
        }

        @Override
        public void onBboHint(boolean isUp, int bestBidTick, int bestAskTick, long recvTimestampMs) {
            coreEngine.getOrderBook().applyBboHint(isUp, bestBidTick, bestAskTick, recvTimestampMs);
        }
    }
}
