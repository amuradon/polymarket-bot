package cz.polymarket.bot.live.engine;

import cz.polymarket.bot.cache.HourlyPriceCache;
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
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.live.monitoring.SystemNetworkTelemetryMonitor;
import cz.polymarket.bot.live.monitoring.SystemNetworkTelemetryMonitor.MonitoredThreadSpec;
import cz.polymarket.bot.live.monitoring.SystemNetworkTelemetryMonitor.SystemTelemetrySnapshot;
import cz.polymarket.bot.live.web.LiveTelemetryDto;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Core Live Trading Engine orchestrating real-money execution on Polymarket CLOB across a
 * 4-core dedicated lock-free pipeline designed for {@code c8gn.xlarge} (4 vCPU Graviton4):
 * <ul>
 *   <li><b>vCPU #1 ({@code ws-io-reader})</b>: Single Netty I/O event-loop thread reading raw WebSocket frames into lock-free queues.</li>
 *   <li><b>vCPU #2 ({@code clob-book-worker})</b>: Dedicated platform thread batch-draining CLOB orderbook frames, fast-forwarding superseded snapshots, and updating {@link PolymarketLocalBook} lock-free.</li>
 *   <li><b>vCPU #3 ({@code strategy-calculator-worker})</b>: Dedicated platform thread draining PolyBolt ticks and the latest conflated {@link OrderBookQuote} to evaluate {@link TradingStrategy} lock-free.</li>
 *   <li><b>vCPU #4 ({@code order-execution-io-worker})</b>: Dedicated platform thread executing EIP-712 signing, blocking HTTP {@code POST /order} calls, in-memory User WS balance updates, and 1s OS/thread telemetry sampling.</li>
 * </ul>
 */
@ApplicationScoped
public class LiveTradingEngine implements StrategyContext, ExecutionRouter {

    private static final double TAKER_FEE_RATE = 0.07;
    private static final long DEBUG_MESSAGE_LOG_INTERVAL = 100L;
    private static final int MAX_TRACKED_USER_TRADES = 1024;
    private static final int SPIN_ITERATIONS_BEFORE_PARK = 64;
    private static final long REALTIME_CLOCK_WINDOW_MS = 3_600_000L;
    private static final long STALE_QUOTE_WARN_INTERVAL_MS = 1_000L;

    private final HourlyPriceCache priceCache;
    private final PolymarketSecretsResolver secretsResolver;
    private final PolymarketClobRestClient clobRestClient;
    private final PolymarketGammaClient gammaClient;
    private final PolymarketClobClient clobWsClient;
    private final PolymarketPolyBoltClient polyBoltClient;
    private final PolymarketStreamParser streamParser;
    private final SystemNetworkTelemetryMonitor telemetryMonitor;
    private final TradingStrategy activeStrategy;
    private final String symbol;
    private final String timeframe;
    private final long durationSeconds;
    private final boolean streamsEnabled;
    private final String orderType;
    private final long maxQuoteAgeMs;

    private final Object strategyLock = new Object();
    private final AtomicReference<LivePosition> activePosition = new AtomicReference<>();
    private final List<LivePosition> openPositions = new CopyOnWriteArrayList<>();
    private final AtomicLong activeCandleStart = new AtomicLong(0L);
    private final AtomicLong activeCandleEnd = new AtomicLong(0L);
    private final AtomicReference<PolymarketMarketInfo> activeMarketInfo = new AtomicReference<>();
    private final AtomicReference<PolymarketLocalBook> activeLocalBook = new AtomicReference<>();
    private final AtomicLong polyBoltMarketMsgCount = new AtomicLong(0L);
    private final AtomicLong clobMarketMsgCount = new AtomicLong(0L);
    private final AtomicLong lastEvaluatedMarketCycleStart = new AtomicLong(0L);
    private final AtomicLong debugPolyBoltBatchCounter = new AtomicLong(0L);
    private final AtomicLong debugClobBatchCounter = new AtomicLong(0L);
    private final AtomicBoolean balanceSyncRequested = new AtomicBoolean(false);
    private final AtomicBoolean balanceSyncInFlight = new AtomicBoolean(false);

    // 4-Core Pipeline Coordination Queues & Slots
    private final AtomicBoolean workersRunning = new AtomicBoolean(false);
    private final AtomicReference<OrderBookQuote> latestQuoteSlot = new AtomicReference<>();
    private final ConcurrentLinkedQueue<OrderCommand> orderExecutionQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ExecutionReport> executionReportQueue = new ConcurrentLinkedQueue<>();

    // Dedicated Worker Threads (vCPU #2, #3, #4) & Busy Time Accumulators
    private volatile Thread clobBookWorkerThread;
    private volatile Thread strategyCalculatorThread;
    private volatile Thread orderExecutionWorkerThread;
    private final AtomicLong clobBookWorkerBusyNs = new AtomicLong(0L);
    private final AtomicLong strategyWorkerBusyNs = new AtomicLong(0L);
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

    private volatile double initialCapital = 0.0;
    private volatile double currentBalance = 0.0;
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
     * Constructs the Live Trading Engine.
     *
     * @param strategyRegistry  registry of available trading strategies
     * @param priceCache        sliding window price cache
     * @param secretsResolver   AWS Secrets Manager / environment credentials resolver
     * @param clobRestClient    Polymarket CLOB REST client (L2 HMAC + EIP-712 order gateway)
     * @param gammaClient       Polymarket Gamma REST client ({@code GET /markets/slug/{slug}})
     * @param clobWsClient      Polymarket CLOB Market WebSocket client
     * @param polyBoltClient    Polymarket PolyBolt v2 authenticated WebSocket client
     * @param streamParser      JSON frame parser for PolyBolt and CLOB streams
     * @param telemetryMonitor  OS kernel TCP buffer, JVM ZGC, and per-thread CPU utilization monitor
     * @param strategyName      configured strategy name (defaults to {@code UtterParameciumStrategy})
     * @param symbol            trading asset symbol (defaults to {@code BTCUSDT})
     * @param timeframe         market timeframe (defaults to {@code 15m})
     * @param streamsEnabled    whether to auto-connect live WebSocket streams on startup
     * @param orderType         CLOB order time-in-force (defaults to {@code FAK})
     * @param maxQuoteAgeMs     maximum allowed orderbook quote age in milliseconds before skipping signal evaluation
     */
    @Inject
    public LiveTradingEngine(
            StrategyRegistry strategyRegistry,
            HourlyPriceCache priceCache,
            PolymarketSecretsResolver secretsResolver,
            PolymarketClobRestClient clobRestClient,
            PolymarketGammaClient gammaClient,
            PolymarketClobClient clobWsClient,
            PolymarketPolyBoltClient polyBoltClient,
            PolymarketStreamParser streamParser,
            SystemNetworkTelemetryMonitor telemetryMonitor,
            @ConfigProperty(name = "polymarket.strategy.name", defaultValue = "UtterParameciumStrategy")
            String strategyName,
            @ConfigProperty(name = "polymarket.live.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.live.timeframe", defaultValue = "15m")
            String timeframe,
            @ConfigProperty(name = "polymarket.live.streams-enabled", defaultValue = "true")
            boolean streamsEnabled,
            @ConfigProperty(name = "polymarket.live.order-type", defaultValue = "FAK")
            String orderType,
            @ConfigProperty(name = "polymarket.live.max-quote-age-ms", defaultValue = "250")
            long maxQuoteAgeMs) {
        if (strategyRegistry == null) {
            throw new IllegalArgumentException("strategyRegistry cannot be null");
        }
        if (priceCache == null) {
            throw new IllegalArgumentException("priceCache cannot be null");
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
        if (streamParser == null) {
            throw new IllegalArgumentException("streamParser cannot be null");
        }
        if (telemetryMonitor == null) {
            throw new IllegalArgumentException("telemetryMonitor cannot be null");
        }

        this.priceCache = priceCache;
        this.secretsResolver = secretsResolver;
        this.clobRestClient = clobRestClient;
        this.gammaClient = gammaClient;
        this.clobWsClient = clobWsClient;
        this.polyBoltClient = polyBoltClient;
        this.streamParser = streamParser;
        this.telemetryMonitor = telemetryMonitor;
        this.symbol = symbol != null && !symbol.isBlank() ? symbol.trim() : "BTCUSDT";
        this.timeframe = timeframe != null && !timeframe.isBlank() ? timeframe.trim() : "15m";
        this.durationSeconds = PolymarketMarketInfo.parseTimeframeSeconds(this.timeframe);
        this.streamsEnabled = streamsEnabled;
        this.orderType = orderType != null && !orderType.isBlank() ? orderType.trim().toUpperCase(Locale.ROOT) : "FAK";
        this.maxQuoteAgeMs = Math.max(10L, maxQuoteAgeMs);

        String resolvedName = (strategyName == null || strategyName.isBlank())
                ? "UtterParameciumStrategy"
                : strategyName.trim();
        this.activeStrategy = strategyRegistry.getStrategy(resolvedName);
        this.activeStrategy.init(this);
    }

    void onStart(@Observes StartupEvent event) {
        try {
            initializeCredentialsAndSigner();
        } catch (Exception e) {
            Log.fatalf("Fatal configuration error: failed to initialize Polymarket credentials for live trading: %s", e.getMessage());
            throw new IllegalStateException("Failed to resolve Polymarket credentials for live trading", e);
        }

        if (streamsEnabled) {
            syncAccountBalanceFromClob();
        }

        Log.infof("Initializing LiveTradingEngine with strategy: %s (symbol=%s, timeframe=%s, durationSec=%d, orderType=%s, maxQuoteAgeMs=%d, initialCapital=$%.2f, currentBalance=$%.2f)",
                activeStrategy.getName(), symbol, timeframe, durationSeconds, orderType, maxQuoteAgeMs,
                initialCapital, currentBalance);

        if (streamsEnabled) {
            startLiveStreams(Instant.now().getEpochSecond());
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        Log.info("Stopping LiveTradingEngine 4-core pipeline and WebSocket streams...");
        workersRunning.set(false);
        clobWsClient.stopAll();
        polyBoltClient.stop();
        unparkAllWorkers();
        clobBookWorkerThread = null;
        strategyCalculatorThread = null;
        orderExecutionWorkerThread = null;
    }

    /**
     * Resolves Polymarket credentials via {@link PolymarketSecretsResolver} and initializes the
     * {@link PolymarketEip712Signer}.
     */
    public synchronized void initializeCredentialsAndSigner() {
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
    }

    /**
     * Starts the 3 dedicated application worker threads ({@code clob-book-worker}, {@code strategy-calculator-worker},
     * {@code order-execution-io-worker}), binds the authenticated PolyBolt v2 and CLOB User streams to their respective
     * dedicated threads, and discovers/subscribes the current Polymarket 15m market.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void startLiveStreams(long nowEpochSec) {
        Log.infof("Starting 4-core live pipeline and WebSocket streams for symbol=%s, timeframe=%s...", symbol, timeframe);
        if (workersRunning.compareAndSet(false, true)) {
            this.clobBookWorkerThread = Thread.ofPlatform()
                    .daemon(true)
                    .priority(Thread.MAX_PRIORITY)
                    .name("clob-book-worker")
                    .start(this::clobBookWorkerLoop);
            this.strategyCalculatorThread = Thread.ofPlatform()
                    .daemon(true)
                    .priority(Thread.MAX_PRIORITY)
                    .name("strategy-calculator-worker")
                    .start(this::strategyCalculatorLoop);
            this.orderExecutionWorkerThread = Thread.ofPlatform()
                    .daemon(true)
                    .name("order-execution-io-worker")
                    .start(this::orderExecutionWorkerLoop);
        }

        polyBoltClient.startTwapStream(symbol, this::onRawPolyBoltFrame, this.strategyCalculatorThread);
        polyBoltClient.startCryptoPriceStream(symbol, this::onRawPolyBoltFrame, this.strategyCalculatorThread);
        if (credentials != null && credentials.hasL2Credentials()) {
            clobWsClient.openUserStream(credentials, this::onRawUserFrame, this.orderExecutionWorkerThread);
        }
        discoverAndRotateMarket(nowEpochSec);
    }

    /**
     * Dedicated loop for vCPU #2 ({@code clob-book-worker}): drains CLOB market frames in batches
     * and updates {@link PolymarketLocalBook} without lock contention.
     */
    private void clobBookWorkerLoop() {
        List<TimestampedFrame> scratchBatch = new ArrayList<>(64);
        while (workersRunning.get()) {
            int drained = clobWsClient.drainMarketStreams(scratchBatch);
            if (drained > 0) {
                continue;
            }
            for (int spin = 0; spin < SPIN_ITERATIONS_BEFORE_PARK && workersRunning.get() && !clobWsClient.hasQueuedMarketFrames(); spin++) {
                Thread.onSpinWait();
            }
            if (!workersRunning.get() || clobWsClient.hasQueuedMarketFrames()) {
                continue;
            }
            LockSupport.parkNanos(this, 50_000_000L);
        }
    }

    /**
     * Dedicated loop for vCPU #3 ({@code strategy-calculator-worker}): processes execution reports,
     * PolyBolt oracle ticks, and the latest conflated {@link OrderBookQuote} from {@code latestQuoteSlot}.
     */
    private void strategyCalculatorLoop() {
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

            int polyDrained = polyBoltClient.drainStreams(scratchBatch);
            if (polyDrained > 0) {
                didWork = true;
            }

            OrderBookQuote quote = latestQuoteSlot.getAndSet(null);
            if (quote != null) {
                didWork = true;
                evaluateQuoteWithFreshnessGuard(quote, true);
            }

            if (didWork) {
                strategyWorkerBusyNs.addAndGet(Math.max(0L, System.nanoTime() - startNs));
                continue;
            }

            for (int spin = 0; spin < SPIN_ITERATIONS_BEFORE_PARK && workersRunning.get()
                    && executionReportQueue.isEmpty()
                    && !polyBoltClient.hasQueuedFrames()
                    && latestQuoteSlot.get() == null; spin++) {
                Thread.onSpinWait();
            }
            if (!workersRunning.get()
                    || !executionReportQueue.isEmpty()
                    || polyBoltClient.hasQueuedFrames()
                    || latestQuoteSlot.get() != null) {
                continue;
            }
            LockSupport.parkNanos(this, 50_000_000L);
        }
    }

    /**
     * Dedicated loop for vCPU #4 ({@code order-execution-io-worker}): executes EIP-712 signing,
     * HTTP {@code POST /order} calls, CLOB User channel balance updates, and 1s OS/thread telemetry sampling.
     */
    private void orderExecutionWorkerLoop() {
        List<TimestampedFrame> scratchBatch = new ArrayList<>(32);
        long lastTelemetrySampleNs = 0L;
        while (workersRunning.get()) {
            boolean didWork = false;
            long startNs = System.nanoTime();

            OrderCommand cmd;
            while ((cmd = orderExecutionQueue.poll()) != null) {
                didWork = true;
                executeOrderCommandInternal(cmd, true);
            }

            int userDrained = clobWsClient.drainUserStream(scratchBatch);
            if (userDrained > 0) {
                didWork = true;
            }

            if (balanceSyncRequested.compareAndSet(true, false)) {
                didWork = true;
                try {
                    syncAccountBalanceFromClob();
                } finally {
                    balanceSyncInFlight.set(false);
                }
            }

            long nowNs = System.nanoTime();
            if (nowNs - lastTelemetrySampleNs >= 1_000_000_000L) {
                didWork = true;
                lastTelemetrySampleNs = nowNs;
                telemetryMonitor.sample(buildMonitoredThreadSpecs());
            }

            if (didWork) {
                orderExecutionWorkerBusyNs.addAndGet(Math.max(0L, System.nanoTime() - startNs));
                continue;
            }

            LockSupport.parkNanos(this, 100_000_000L);
        }
    }

    private void unparkAllWorkers() {
        Thread t2 = this.clobBookWorkerThread;
        if (t2 != null) {
            LockSupport.unpark(t2);
        }
        Thread t3 = this.strategyCalculatorThread;
        if (t3 != null) {
            LockSupport.unpark(t3);
        }
        Thread t4 = this.orderExecutionWorkerThread;
        if (t4 != null) {
            LockSupport.unpark(t4);
        }
    }

    /**
     * Processes a raw JSON frame from the authenticated Polymarket CLOB User WebSocket channel ({@code /ws/user})
     * and updates the in-memory account balance upon each incoming {@code BUY} or {@code SELL} trade event.
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
                            previousDelta, tradeId, currentBalance);
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
                tradeId, side.toUpperCase(Locale.ROOT), size, price, feeUsd, signedDeltaUsd, currentBalance);
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
     * Synchronizes the in-memory balance with the authoritative Polymarket CLOB collateral USDC balance.
     *
     * @param accountBalanceUsd available Polymarket account USDC collateral balance in USD
     */
    public synchronized void syncAccountBalance(double accountBalanceUsd) {
        if (!Double.isFinite(accountBalanceUsd) || accountBalanceUsd < 0.0) {
            throw new IllegalArgumentException("accountBalanceUsd must be finite and non-negative");
        }
        this.currentBalance = accountBalanceUsd;
        if (this.initialCapital <= 0.0) {
            this.initialCapital = accountBalanceUsd;
        }
    }

    /**
     * Applies a signed balance delta to the in-memory account balance.
     *
     * @param deltaUsd signed USD balance change
     */
    public synchronized void applyAccountBalanceDelta(double deltaUsd) {
        if (Double.isFinite(deltaUsd) && deltaUsd != 0.0) {
            this.currentBalance = Math.max(0.0, this.currentBalance + deltaUsd);
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
                    currentBalance, initialCapital);
        } catch (Exception e) {
            Log.warnf("Failed to synchronize Polymarket collateral balance from CLOB REST API: %s", e.getMessage());
        }
    }

    @Override
    public void refreshAccountBalance() {
        if (!balanceSyncInFlight.compareAndSet(false, true)) {
            return;
        }
        Thread ioWorker = this.orderExecutionWorkerThread;
        if (workersRunning.get() && ioWorker != null) {
            balanceSyncRequested.set(true);
            LockSupport.unpark(ioWorker);
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
     * Periodically checks for 15m market cycle boundary transitions and rotates the active CLOB market subscription.
     */
    @Scheduled(every = "${polymarket.live.discovery-interval:5s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void scheduledMarketRollover() {
        if (!streamsEnabled) {
            return;
        }
        discoverAndRotateMarket(Instant.now().getEpochSecond());
    }

    /**
     * Periodically sends a {@code GET /time} keepalive heartbeat to the Polymarket CLOB REST API.
     */
    @Scheduled(every = "${polymarket.live.keepalive-interval:15s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void scheduledKeepalive() {
        if (!streamsEnabled) {
            return;
        }
        clobRestClient.pingKeepalive();
    }

    /**
     * Discovers the active market for the cycle containing {@code nowEpochSec} via {@code GET /markets/slug/{slug}}
     * and rotates the CLOB WebSocket subscription if the cycle has changed.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     * @return discovered {@link PolymarketMarketInfo} if found
     */
    public Optional<PolymarketMarketInfo> discoverAndRotateMarket(long nowEpochSec) {
        long cycleStartSec = PolymarketMarketInfo.alignCycleStart(nowEpochSec, durationSeconds);
        String expectedSlug = PolymarketMarketInfo.buildMarketSlug(symbol, timeframe, cycleStartSec);

        PolymarketMarketInfo current = activeMarketInfo.get();
        if (current != null && expectedSlug.equals(current.slug())) {
            return Optional.of(current);
        }

        Optional<PolymarketMarketInfo> discovered = gammaClient.fetchMarketBySlug(expectedSlug, durationSeconds);
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
                return Optional.of(latestCurrent);
            }
            if (latestCurrent != null && !latestCurrent.slug().equals(nextMarket.slug())) {
                evaluateCompletedMarketStreamActivity(latestCurrent.slug(), latestCurrent.startSec());
                previousSlugToClose = latestCurrent.slug();
            }

            activeMarketInfo.set(nextMarket);
            activeLocalBook.set(new PolymarketLocalBook(nextMarket.yesTokenId(), nextMarket.noTokenId()));
            this.latestQuote = null;
            this.latestQuoteSlot.set(null);
        }
        if (previousSlugToClose != null) {
            clobWsClient.closeMarketStream(previousSlugToClose);
        }
        clobWsClient.openMarketStreamBatch(
                nextMarket.slug(),
                nextMarket.clobTokenIds(),
                this::onRawClobFrameBatch,
                this.clobBookWorkerThread
        );
        Thread bookWorker = this.clobBookWorkerThread;
        if (bookWorker != null) {
            LockSupport.unpark(bookWorker);
        }

        Log.infof("Subscribed to live Polymarket market %s (conditionId=%s, yesToken=%s, noToken=%s)",
                nextMarket.slug(), nextMarket.conditionId(), nextMarket.yesTokenId(), nextMarket.noTokenId());
        return Optional.of(nextMarket);
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
        Log.infof("Market %s summary statistics: polyBoltMessages=%d, clobWsMessages=%d, twapOpen=%.2f, lastSpot=%.2f, high=%.2f, low=%.2f, balance=$%.2f",
                marketLabel, polyBoltCount, clobCount, twapOpenPrice, currentSpotPrice, candleHigh, candleLow, currentBalance);
    }

    /**
     * Processes a raw JSON frame from the authenticated PolyBolt v2 WebSocket stream.
     *
     * @param rawJson         raw PolyBolt JSON text frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void onRawPolyBoltFrame(String rawJson, long recvTimestampMs) {
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

    /**
     * Processes a batch of raw CLOB JSON frames on the dedicated {@code clob-book-worker} thread (vCPU #2),
     * fast-forwarding past superseded snapshots, mutating {@link PolymarketLocalBook} lock-free, and publishing
     * only the single final {@link OrderBookQuote} to {@code latestQuoteSlot} for {@code strategy-calculator-worker} (vCPU #3).
     *
     * @param batch chronological batch of CLOB frames drained from the inbound queue
     */
    public void onRawClobFrameBatch(List<TimestampedFrame> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        long startNs = System.nanoTime();
        try {
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
        } finally {
            clobBookWorkerBusyNs.addAndGet(Math.max(0L, System.nanoTime() - startNs));
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

        Thread stratWorker = this.strategyCalculatorThread;
        if (workersRunning.get() && stratWorker != null) {
            OrderBookQuote overwritten = latestQuoteSlot.getAndSet(nextQuote);
            if (overwritten != null) {
                conflatedQuotesCount.incrementAndGet();
            }
            LockSupport.unpark(stratWorker);
        } else {
            evaluateQuoteWithFreshnessGuard(nextQuote, false);
        }
    }

    private void updatePositionObservation(OrderBookQuote quote) {
        LivePosition pos = activePosition.get();
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
    public HourlyPriceCache getPriceCache() {
        return priceCache;
    }

    @Override
    public void submitOrder(OrderCommand command) {
        if (command == null) {
            return;
        }
        if (Log.isTraceEnabled()) {
            Log.tracef("Submitting order command: clientOrderId=%s, side=%s, token=%s, price=%s, size=%s",
                    command.clientOrderId(), command.side(), command.token(), command.price(), command.size());
        }
        Thread ioWorker = this.orderExecutionWorkerThread;
        if (workersRunning.get() && ioWorker != null && Thread.currentThread() == this.strategyCalculatorThread) {
            orderExecutionQueue.offer(command);
            LockSupport.unpark(ioWorker);
        } else {
            executeOrderCommandInternal(command, false);
        }
    }

    private void executeOrderCommandInternal(OrderCommand command, boolean asyncReportDelivery) {
        if ("BUY".equalsIgnoreCase(command.side())) {
            executeBuyOrder(command, asyncReportDelivery);
        } else if ("SELL".equalsIgnoreCase(command.side())) {
            executeSellOrder(command, asyncReportDelivery);
        }
    }

    private void deliverExecutionReport(ExecutionReport report, boolean asyncReportDelivery) {
        Thread stratWorker = this.strategyCalculatorThread;
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
        String clobOrderId = clientOrderId;
        LivePosition currentPos = activePosition.get();
        if (currentPos != null && clientOrderId.equals(currentPos.clientOrderId())
                && currentPos.clobOrderId() != null && !currentPos.clobOrderId().isBlank()) {
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

    private void executeBuyOrder(OrderCommand command, boolean asyncReportDelivery) {
        double requestedPrice = command.price().doubleValue();
        double requestedShares = command.size().doubleValue();
        boolean isDown = command.token() != null && command.token().toLowerCase(Locale.ROOT).contains("down");
        TradeDirection side = isDown ? TradeDirection.DOWN : TradeDirection.UP;
        String clobTokenId = resolveClobTokenId(command.token(), isDown);

        double estimatedCostUsd = requestedPrice * requestedShares;
        if (currentBalance < estimatedCostUsd) {
            Log.warnf("Insufficient live balance ($%.2f) for order notional $%.2f",
                    currentBalance, estimatedCostUsd);
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

        LivePosition pos = new LivePosition(
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

    private void executeSellOrder(OrderCommand command, boolean asyncReportDelivery) {
        LivePosition pos = activePosition.get();
        if (pos == null) {
            Log.warnf("Ignoring SELL order %s because no active position exists", command.clientOrderId());
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
        closePositionInMemory(pos, fillPrice, exitFee, exitReason);

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
     *
     * @param actualOutcome resolved candle outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
     * @param finalPrice    closing reference price
     */
    public void resolveCandle(TradeDirection actualOutcome, double finalPrice) {
        synchronized (strategyLock) {
            int settledPositions = openPositions.size();
            if (!openPositions.isEmpty() || activePosition.get() != null) {
                activeStrategy.onCandleResolution(actualOutcome);

                List<LivePosition> remaining = new ArrayList<>(openPositions);
                if (remaining.isEmpty() && activePosition.get() != null) {
                    remaining.add(activePosition.get());
                }
                settledPositions = remaining.size();
                for (LivePosition pos : remaining) {
                    double exitPrice = (actualOutcome == pos.side()) ? 1.00 : 0.00;
                    closePositionInMemory(pos, exitPrice, 0.0, "Resolution (TWAP 60s)");
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

            double priceDelta = finalPrice - open;
            double approxVolumeBtc = Math.max(50.0, Math.abs(priceDelta) * 2.0);
            double approxVolumeUsd = approxVolumeBtc * finalPrice;
            double approxDeltaBtc = Math.clamp(priceDelta / 10.0, -10.0, 10.0);

            MarketCandle candle = new MarketCandle(
                    start,
                    end,
                    open,
                    high,
                    low,
                    finalPrice,
                    approxVolumeBtc,
                    approxVolumeUsd,
                    approxDeltaBtc,
                    open,
                    high,
                    low,
                    finalPrice,
                    approxVolumeBtc,
                    approxVolumeUsd,
                    approxDeltaBtc,
                    0.0
            );
            activeStrategy.onMarketCandleCompleted(candle);
            Log.infof("Resolved market candle [%d, %d]: outcome=%s, open=%.2f, close=%.2f, high=%.2f, low=%.2f, settledPositions=%d, balance=$%.2f",
                    start, end, actualOutcome, open, finalPrice, high, low, settledPositions, currentBalance);

            candleHigh = 0.0;
            candleLow = 0.0;
        }
    }

    private void closePositionInMemory(LivePosition pos, double exitPrice, double exitFee, String exitReason) {
        double payout = pos.shares() * exitPrice;
        double totalFee = pos.entryFee() + exitFee;
        double netPnl = payout - pos.sizeUsd() - totalFee;
        openPositions.remove(pos);
        activePosition.set(openPositions.isEmpty() ? null : openPositions.getLast());
        Log.infof("Closed live position: %s @ %.4f -> Net PnL: $%.2f (%s), Balance: $%.2f",
                pos.side(), exitPrice, netPnl, exitReason, currentBalance);
    }

    /**
     * Forwards a {@link TwapUpdate} to the active strategy when multi-exchange TWAP updates are used.
     *
     * @param update TWAP update
     */
    public void onTwapUpdate(TwapUpdate update) {
        if (update == null) {
            return;
        }
        Timeframe strategyTf = activeStrategy.getTimeframe();
        if (update.timeframe() != null && strategyTf != null && update.timeframe() != strategyTf) {
            return;
        }
        synchronized (strategyLock) {
            activeStrategy.onTwapUpdate(update);
        }
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
            if (activeCandleStart.get() > 0L && candleStartSec != activeCandleStart.get()) {
                long prevCandleStart = activeCandleStart.get();
                PolymarketMarketInfo currentMarket = activeMarketInfo.get();
                String marketLabel = currentMarket != null
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
            } else if (activeCandleStart.get() == 0L) {
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
     * @param price            1s Chainlink BTC/USD price
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
     * Explicitly sets the active market metadata (useful in unit/component tests or manual overrides).
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
            activeCandleStart.set(0L);
            activeCandleEnd.set(0L);
            polyBoltMarketMsgCount.set(0L);
            clobMarketMsgCount.set(0L);
            lastEvaluatedMarketCycleStart.set(0L);
            debugPolyBoltBatchCounter.set(0L);
            debugClobBatchCounter.set(0L);
            twapOpenPrice = 0.0;
            currentSpotPrice = 0.0;
            candleHigh = 0.0;
            candleLow = 0.0;
            currentTimestampSec = 0L;
            latestOracleRecvTimestampMs = 0L;
            latestQuote = null;
            latestQuoteSlot.set(null);
            orderExecutionQueue.clear();
            executionReportQueue.clear();
            if (currentBalance > 0.0) {
                initialCapital = currentBalance;
            }
        }
        synchronized (this) {
            appliedUserTradeDeltas.clear();
        }
        Log.info("Reset LiveTradingEngine internal state");
    }

    /**
     * Builds a comprehensive point-in-time {@link LiveTelemetryDto} snapshot combining OS TCP buffers,
     * JVM ZGC metrics, 4-core thread utilization, order execution latency, and WebSocket stream queues.
     *
     * @return current {@link LiveTelemetryDto}
     */
    public LiveTelemetryDto getTelemetry() {
        SystemTelemetrySnapshot sys = telemetryMonitor.getLatestOrSample(buildMonitoredThreadSpecs());
        List<WebSocketStreamTelemetry> streams = new ArrayList<>(4);
        streams.addAll(clobWsClient.getStreamTelemetries());
        streams.addAll(polyBoltClient.getStreamTelemetries());

        return new LiveTelemetryDto(
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
                streams
        );
    }

    private List<MonitoredThreadSpec> buildMonitoredThreadSpecs() {
        Thread ioReader = clobWsClient.getIoReaderThread();
        if (ioReader == null) {
            ioReader = polyBoltClient.getIoReaderThread();
        }
        return List.of(
                new MonitoredThreadSpec("vCPU #1: I/O Reader", PolymarketClobClient.DEDICATED_MARKET_IO_THREAD_NAME, ioReader, -1L),
                new MonitoredThreadSpec("vCPU #2: CLOB Book Worker", "clob-book-worker", clobBookWorkerThread, clobBookWorkerBusyNs.get()),
                new MonitoredThreadSpec("vCPU #3: Strategy Calculator", "strategy-calculator-worker", strategyCalculatorThread, strategyWorkerBusyNs.get()),
                new MonitoredThreadSpec("vCPU #4: Order & I/O Worker", "order-execution-io-worker", orderExecutionWorkerThread, orderExecutionWorkerBusyNs.get())
        );
    }

    /**
     * Returns the initial live bankroll in USD.
     *
     * @return initial capital in USD
     */
    public double getInitialCapital() {
        return initialCapital;
    }

    /**
     * Returns the current in-memory live balance in USD.
     *
     * @return current balance in USD
     */
    public double getCurrentBalance() {
        return currentBalance;
    }

    /**
     * Returns the number of PolyBolt WebSocket messages received in the current market cycle.
     *
     * @return current market PolyBolt message count
     */
    public long getPolyBoltMarketMessageCount() {
        return polyBoltMarketMsgCount.get();
    }

    /**
     * Returns the number of Polymarket CLOB WebSocket messages received in the current market cycle.
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
     * Returns the current engine status string.
     *
     * @return {@code "LIVE"}
     */
    public String getStatus() {
        return "LIVE";
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
     * Returns the latest active position, or {@code null} if idle.
     *
     * @return active {@link LivePosition}
     */
    public LivePosition getActivePosition() {
        return activePosition.get();
    }

    /**
     * Returns an unmodifiable snapshot of all open positions in the active candle.
     *
     * @return open positions
     */
    public List<LivePosition> getOpenPositions() {
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
