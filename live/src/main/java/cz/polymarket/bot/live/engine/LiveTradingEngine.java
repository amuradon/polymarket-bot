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
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.live.storage.LiveTradeRepository;
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
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Core Live Trading Engine orchestrating real-money (and optional dry-run) execution on Polymarket CLOB.
 * <p>
 * Implements {@link StrategyContext} and {@link ExecutionRouter} from {@code common} so that
 * {@link cz.polymarket.bot.strategy.UtterParameciumStrategy} (and any registered {@link TradingStrategy})
 * executes with 100% zero-discrepancy parity across backtest, paper, and live environments.
 */
@ApplicationScoped
public class LiveTradingEngine implements StrategyContext, ExecutionRouter {

    private static final double TAKER_FEE_RATE = 0.07;
    private static final long DEBUG_MESSAGE_LOG_INTERVAL = 100L;

    private final HourlyPriceCache priceCache;
    private final LiveTradeRepository repository;
    private final PolymarketSecretsResolver secretsResolver;
    private final PolymarketClobRestClient clobRestClient;
    private final PolymarketGammaClient gammaClient;
    private final PolymarketClobClient clobWsClient;
    private final PolymarketPolyBoltClient polyBoltClient;
    private final PolymarketStreamParser streamParser;
    private final TradingStrategy activeStrategy;
    private final String symbol;
    private final String timeframe;
    private final long durationSeconds;
    private final boolean streamsEnabled;
    private final boolean dryRun;
    private final String orderType;

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
    private final java.util.concurrent.atomic.AtomicBoolean balanceSyncInFlight =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    private volatile PolymarketCredentials credentials;
    private volatile PolymarketEip712Signer eip712Signer;
    private volatile double twapOpenPrice = 0.0;
    private volatile double currentSpotPrice = 0.0;
    private volatile double candleHigh = 0.0;
    private volatile double candleLow = 0.0;
    private volatile long currentTimestampSec = 0L;
    private volatile OrderBookQuote latestQuote;

    /**
     * Constructs the Live Trading Engine.
     *
     * @param strategyRegistry  registry of available trading strategies
     * @param priceCache        sliding window price cache
     * @param repository        persistent live trade repository
     * @param secretsResolver   AWS Secrets Manager / environment credentials resolver
     * @param clobRestClient    Polymarket CLOB REST client (L2 HMAC + EIP-712 order gateway)
     * @param gammaClient       Polymarket Gamma REST client ({@code GET /markets/slug/{slug}})
     * @param clobWsClient      Polymarket CLOB Market WebSocket client
     * @param polyBoltClient    Polymarket PolyBolt v2 authenticated WebSocket client
     * @param streamParser      JSON frame parser for PolyBolt and CLOB streams
     * @param strategyName      configured strategy name (defaults to {@code UtterParameciumStrategy})
     * @param symbol            trading asset symbol (defaults to {@code BTCUSDT})
     * @param timeframe         market timeframe (defaults to {@code 15m})
     * @param streamsEnabled    whether to auto-connect live WebSocket streams on startup
     * @param dryRun            whether to simulate order fills without sending live CLOB orders
     * @param orderType         CLOB order time-in-force (defaults to {@code FAK})
     */
    @Inject
    public LiveTradingEngine(
            StrategyRegistry strategyRegistry,
            HourlyPriceCache priceCache,
            LiveTradeRepository repository,
            PolymarketSecretsResolver secretsResolver,
            PolymarketClobRestClient clobRestClient,
            PolymarketGammaClient gammaClient,
            PolymarketClobClient clobWsClient,
            PolymarketPolyBoltClient polyBoltClient,
            PolymarketStreamParser streamParser,
            @ConfigProperty(name = "polymarket.strategy.name", defaultValue = "UtterParameciumStrategy")
            String strategyName,
            @ConfigProperty(name = "polymarket.live.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.live.timeframe", defaultValue = "15m")
            String timeframe,
            @ConfigProperty(name = "polymarket.live.streams-enabled", defaultValue = "true")
            boolean streamsEnabled,
            @ConfigProperty(name = "polymarket.live.dry-run", defaultValue = "false")
            boolean dryRun,
            @ConfigProperty(name = "polymarket.live.order-type", defaultValue = "FAK")
            String orderType) {
        if (strategyRegistry == null) {
            throw new IllegalArgumentException("strategyRegistry cannot be null");
        }
        if (priceCache == null) {
            throw new IllegalArgumentException("priceCache cannot be null");
        }
        if (repository == null) {
            throw new IllegalArgumentException("repository cannot be null");
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

        this.priceCache = priceCache;
        this.repository = repository;
        this.secretsResolver = secretsResolver;
        this.clobRestClient = clobRestClient;
        this.gammaClient = gammaClient;
        this.clobWsClient = clobWsClient;
        this.polyBoltClient = polyBoltClient;
        this.streamParser = streamParser;
        this.symbol = symbol != null && !symbol.isBlank() ? symbol.trim() : "BTCUSDT";
        this.timeframe = timeframe != null && !timeframe.isBlank() ? timeframe.trim() : "15m";
        this.durationSeconds = PolymarketMarketInfo.parseTimeframeSeconds(this.timeframe);
        this.streamsEnabled = streamsEnabled;
        this.dryRun = dryRun;
        this.orderType = orderType != null && !orderType.isBlank() ? orderType.trim().toUpperCase(Locale.ROOT) : "FAK";

        String resolvedName = (strategyName == null || strategyName.isBlank())
                ? "UtterParameciumStrategy"
                : strategyName.trim();
        this.activeStrategy = strategyRegistry.getStrategy(resolvedName);
        this.activeStrategy.init(this);
    }

    void onStart(@Observes StartupEvent event) {
        if (!dryRun || streamsEnabled) {
            try {
                initializeCredentialsAndSigner();
                if (!dryRun) {
                    syncAccountBalanceFromClob();
                }
            } catch (Exception e) {
                if (!dryRun) {
                    Log.fatalf("Fatal configuration error: failed to initialize Polymarket credentials or account balance for live trading: %s", e.getMessage());
                    throw new IllegalStateException("Failed to resolve Polymarket credentials for live trading", e);
                }
                Log.warnf("Running in dry-run mode without full Polymarket credentials: %s", e.getMessage());
            }
        }

        Log.infof("Initializing LiveTradingEngine with strategy: %s (symbol=%s, timeframe=%s, durationSec=%d, dryRun=%s, orderType=%s, initialCapital=$%.2f, currentBalance=$%.2f)",
                activeStrategy.getName(), symbol, timeframe, durationSeconds, dryRun, orderType,
                repository.getInitialCapital(), repository.getCurrentBalance());

        if (streamsEnabled) {
            startLiveStreams(Instant.now().getEpochSecond());
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        Log.info("Stopping LiveTradingEngine streams...");
        clobWsClient.stopAll();
        polyBoltClient.stop();
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
     * Starts the authenticated PolyBolt v2 streams ({@code price.crypto.twap} and {@code price.crypto})
     * and discovers/subscribes the current Polymarket 15m market via Gamma and CLOB WebSocket.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void startLiveStreams(long nowEpochSec) {
        Log.infof("Starting live WebSocket streams for symbol=%s, timeframe=%s...", symbol, timeframe);
        polyBoltClient.startTwapStream(symbol, this::onRawPolyBoltFrame);
        polyBoltClient.startCryptoPriceStream(symbol, this::onRawPolyBoltFrame);
        if (credentials != null && credentials.hasL2Credentials()) {
            clobWsClient.openUserStream(credentials, this::onRawUserFrame);
        }
        discoverAndRotateMarket(nowEpochSec);
    }

    /**
     * Processes a raw JSON frame from the authenticated Polymarket CLOB User WebSocket channel ({@code /ws/user})
     * and triggers a non-blocking background collateral balance refresh when trade or order events occur.
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
        if (rawJson.contains("\"trade\"") || rawJson.contains("\"order\"")) {
            triggerAsyncBalanceSync();
        }
    }

    /**
     * Synchronizes the repository balance with the authoritative Polymarket CLOB collateral USDC balance.
     */
    public void syncAccountBalanceFromClob() {
        try {
            double collateralUsd = clobRestClient.getCollateralBalanceUsd();
            repository.syncAccountBalance(collateralUsd);
            Log.infof("Synchronized Polymarket live account collateral balance: $%.2f (initialCapital=$%.2f)",
                    repository.getCurrentBalance(), repository.getInitialCapital());
        } catch (Exception e) {
            Log.warnf("Failed to synchronize Polymarket collateral balance from CLOB REST API: %s", e.getMessage());
        }
    }

    private void triggerAsyncBalanceSync() {
        if (dryRun || !balanceSyncInFlight.compareAndSet(false, true)) {
            return;
        }
        Thread.ofVirtual()
                .name("polymarket-balance-sync")
                .start(() -> {
                    try {
                        if (openPositions.isEmpty()) {
                            syncAccountBalanceFromClob();
                        }
                    } finally {
                        balanceSyncInFlight.set(false);
                    }
                });
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
        if (!streamsEnabled && dryRun) {
            return;
        }
        clobRestClient.pingKeepalive();
        if (!dryRun && openPositions.isEmpty()) {
            triggerAsyncBalanceSync();
        }
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
        }
        if (previousSlugToClose != null) {
            clobWsClient.closeMarketStream(previousSlugToClose);
        }
        clobWsClient.openMarketStream(nextMarket.slug(), nextMarket.clobTokenIds(), this::onRawClobFrame);

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
                marketLabel, polyBoltCount, clobCount, twapOpenPrice, currentSpotPrice, candleHigh, candleLow, repository.getCurrentBalance());
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
     * Processes a raw JSON frame from the Polymarket CLOB Market WebSocket stream.
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
            long marketTotal = clobMarketMsgCount.incrementAndGet();
            PolymarketBboQuote bbo = bboOpt.get();
            if (Log.isDebugEnabled()) {
                long debugCount = debugClobBatchCounter.incrementAndGet();
                if (debugCount % DEBUG_MESSAGE_LOG_INTERVAL == 0L) {
                    Log.debugf("Processed %d CLOB WS messages (marketMessages=%d, bestBidUp=%.3f, bestAskUp=%.3f, bestBidDown=%.3f, bestAskDown=%.3f)",
                            debugCount, marketTotal, bbo.bestBidUp(), bbo.bestAskUp(), bbo.bestBidDown(), bbo.bestAskDown());
                }
            }
            OrderBookQuote prevQuote = this.latestQuote;
            if (prevQuote == null || !isSameBbo(prevQuote, bbo)) {
                onOrderBookQuote(new OrderBookQuote(
                        bbo.bestAskUp(),
                        bbo.bestBidUp(),
                        bbo.bestAskDown(),
                        bbo.bestBidDown(),
                        bbo.askSizeUp(),
                        bbo.askSizeDown(),
                        bbo.midUp(),
                        bbo.midDown(),
                        bbo.timestampMs()
                ));
            }
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
    public synchronized void submitOrder(OrderCommand command) {
        if (command == null) {
            return;
        }
        if (Log.isTraceEnabled()) {
            Log.tracef("Submitting order command: clientOrderId=%s, side=%s, token=%s, price=%s, size=%s",
                    command.clientOrderId(), command.side(), command.token(), command.price(), command.size());
        }
        if ("BUY".equalsIgnoreCase(command.side())) {
            executeBuyOrder(command);
        } else if ("SELL".equalsIgnoreCase(command.side())) {
            executeSellOrder(command);
        }
    }

    @Override
    public synchronized void cancelOrder(String clientOrderId) {
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
            Log.tracef("Cancelling order: clientOrderId=%s, clobOrderId=%s (dryRun=%s)", clientOrderId, clobOrderId, dryRun);
        }

        if (!dryRun) {
            try {
                ClobCancelResponse resp = clobRestClient.cancelOrder(clobOrderId);
                if (!resp.isCanceled(clobOrderId)) {
                    Log.warnf("CLOB order %s was not canceled: %s", clobOrderId, resp.notCanceled());
                }
            } catch (Exception e) {
                Log.errorf("Failed to cancel CLOB order %s: %s", clobOrderId, e.getMessage());
            }
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
        activeStrategy.onExecutionReport(report);
    }

    private void executeBuyOrder(OrderCommand command) {
        double requestedPrice = command.price().doubleValue();
        double requestedShares = command.size().doubleValue();
        boolean isDown = command.token() != null && command.token().toLowerCase(Locale.ROOT).contains("down");
        TradeDirection side = isDown ? TradeDirection.DOWN : TradeDirection.UP;
        String clobTokenId = resolveClobTokenId(command.token(), isDown);

        double estimatedCostUsd = requestedPrice * requestedShares;
        if (repository.getCurrentBalance() < estimatedCostUsd) {
            Log.warnf("Insufficient live balance ($%.2f) for order notional $%.2f",
                    repository.getCurrentBalance(), estimatedCostUsd);
            activeStrategy.onExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "live-reject-" + UUID.randomUUID(),
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    "Insufficient balance"
            ));
            return;
        }

        double fillPrice = requestedPrice;
        double filledShares = requestedShares;
        String clobOrderId = "dryrun-" + UUID.randomUUID();

        if (!dryRun) {
            if (clobTokenId.isBlank()) {
                Log.warnf("Cannot submit live BUY order %s: active market token ID is not resolved", command.clientOrderId());
                activeStrategy.onExecutionReport(new ExecutionReport(
                        command.clientOrderId(),
                        "",
                        "REJECTED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        System.currentTimeMillis(),
                        "Active market token ID not resolved"
                ));
                return;
            }
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
                double actualShares = response.filledShares("BUY");
                if (!response.success() || actualShares <= 0.0) {
                    String err = (response.errorMsg() != null && !response.errorMsg().isBlank())
                            ? response.errorMsg()
                            : ("Order status: " + response.status());
                    Log.warnf("Live CLOB BUY order %s rejected/unfilled: %s", command.clientOrderId(), err);
                    activeStrategy.onExecutionReport(new ExecutionReport(
                            command.clientOrderId(),
                            response.orderID() != null ? response.orderID() : "",
                            "REJECTED",
                            BigDecimal.ZERO,
                            BigDecimal.ZERO,
                            System.currentTimeMillis(),
                            err
                    ));
                    return;
                }
                filledShares = actualShares;
                int avgMc = response.avgPriceMc("BUY");
                fillPrice = avgMc > 0 ? (avgMc / 1000.0) : requestedPrice;
                clobOrderId = response.orderID() != null ? response.orderID() : "";
            } catch (Exception e) {
                Log.errorf("Live CLOB BUY order %s failed: %s", command.clientOrderId(), e.getMessage());
                activeStrategy.onExecutionReport(new ExecutionReport(
                        command.clientOrderId(),
                        "",
                        "REJECTED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        System.currentTimeMillis(),
                        e.getMessage()
                ));
                return;
            }
        } else if (latestQuote != null) {
            double bestAsk = isDown ? latestQuote.bestAskDown() : latestQuote.bestAskUp();
            if (bestAsk > 0.0) {
                fillPrice = Math.max(requestedPrice, bestAsk);
            }
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
        repository.debitOpenPositionCost(sizeUsd + entryFee);

        ExecutionReport report = new ExecutionReport(
                command.clientOrderId(),
                clobOrderId,
                "FILLED",
                BigDecimal.valueOf(fillPrice),
                BigDecimal.valueOf(filledShares),
                System.currentTimeMillis(),
                dryRun ? "Dry-run simulated fill" : "Matched on Polymarket CLOB"
        );
        activeStrategy.onExecutionReport(report);
        Log.infof("Opened live position: %s (clobOrderId=%s) %s %.2f shares @ %.4f ($%.2f, fee: $%.4f)",
                command.clientOrderId(), clobOrderId, side, filledShares, fillPrice, sizeUsd, entryFee);
    }

    private void executeSellOrder(OrderCommand command) {
        LivePosition pos = activePosition.get();
        if (pos == null) {
            Log.warnf("Ignoring SELL order %s because no active position exists", command.clientOrderId());
            return;
        }

        double requestedPrice = command.price().doubleValue();
        double fillPrice = requestedPrice;
        String clobOrderId = "dryrun-exit-" + UUID.randomUUID();

        if (!dryRun) {
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
                if (!response.success() || response.filledShares("SELL") <= 0.0) {
                    String err = (response.errorMsg() != null && !response.errorMsg().isBlank())
                            ? response.errorMsg()
                            : ("Exit order status: " + response.status());
                    Log.warnf("Live CLOB SELL order %s rejected/unfilled: %s", command.clientOrderId(), err);
                    activeStrategy.onExecutionReport(new ExecutionReport(
                            command.clientOrderId(),
                            response.orderID() != null ? response.orderID() : "",
                            "REJECTED",
                            BigDecimal.ZERO,
                            BigDecimal.ZERO,
                            System.currentTimeMillis(),
                            err
                    ));
                    return;
                }
                int avgMc = response.avgPriceMc("SELL");
                fillPrice = avgMc > 0 ? (avgMc / 1000.0) : requestedPrice;
                clobOrderId = response.orderID() != null ? response.orderID() : "";
            } catch (Exception e) {
                Log.errorf("Live CLOB SELL order %s failed: %s", command.clientOrderId(), e.getMessage());
                activeStrategy.onExecutionReport(new ExecutionReport(
                        command.clientOrderId(),
                        "",
                        "REJECTED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        System.currentTimeMillis(),
                        e.getMessage()
                ));
                return;
            }
        } else if (latestQuote != null) {
            double bestBid = (pos.side() == TradeDirection.DOWN) ? latestQuote.bestBidDown() : latestQuote.bestBidUp();
            if (bestBid > 0.0) {
                fillPrice = Math.min(requestedPrice, bestBid);
            }
        }

        double exitFee = (fillPrice > 0.0 && fillPrice < 1.0)
                ? (pos.shares() * TAKER_FEE_RATE * fillPrice * (1.0 - fillPrice))
                : 0.0;
        String exitReason = String.format(Locale.ROOT, "Early Exit (%.2f)", fillPrice);
        completeTrade(pos, fillPrice, exitFee, exitReason);

        ExecutionReport report = new ExecutionReport(
                command.clientOrderId(),
                clobOrderId,
                "FILLED",
                BigDecimal.valueOf(fillPrice),
                BigDecimal.valueOf(pos.shares()),
                System.currentTimeMillis(),
                dryRun ? "Dry-run exit fill" : "Exit matched on Polymarket CLOB"
        );
        activeStrategy.onExecutionReport(report);
    }

    /**
     * Settles all open positions for the completed 15m candle according to the binary resolution outcome.
     *
     * @param actualOutcome resolved candle outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
     * @param finalPrice    closing reference price
     */
    public synchronized void resolveCandle(TradeDirection actualOutcome, double finalPrice) {
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
                completeTrade(pos, exitPrice, 0.0, "Resolution (TWAP 60s)");
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
                start, end, actualOutcome, open, finalPrice, high, low, settledPositions, repository.getCurrentBalance());

        candleHigh = 0.0;
        candleLow = 0.0;
    }

    private void completeTrade(LivePosition pos, double exitPrice, double exitFee, String exitReason) {
        double payout = pos.shares() * exitPrice;
        double totalFee = pos.entryFee() + exitFee;
        double netPnl = payout - pos.sizeUsd() - totalFee;
        double newBalance = repository.getCurrentBalance() + payout - exitFee;
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

        repository.recordTrade(trade);
        openPositions.remove(pos);
        activePosition.set(openPositions.isEmpty() ? null : openPositions.getLast());
        if (openPositions.isEmpty()) {
            triggerAsyncBalanceSync();
        }
        Log.infof("Closed live trade: %s @ %.4f -> Net PnL: $%.2f (%s), Balance: $%.2f",
                pos.side(), exitPrice, netPnl, exitReason, newBalance);
    }

    /**
     * Forwards a {@link TwapUpdate} to the active strategy when multi-exchange TWAP updates are used.
     *
     * @param update TWAP update
     */
    public synchronized void onTwapUpdate(TwapUpdate update) {
        if (update == null) {
            return;
        }
        Timeframe strategyTf = activeStrategy.getTimeframe();
        if (update.timeframe() != null && strategyTf != null && update.timeframe() != strategyTf) {
            return;
        }
        activeStrategy.onTwapUpdate(update);
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
    public synchronized void onPolymarketCryptoTwap60s(
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
        long candleStartSec = (effectiveTsMs / 1000L / durationSeconds) * durationSeconds;

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

    /**
     * Processes a PolyBolt {@code price.crypto} 1-second spot price update and forwards it to the active strategy.
     *
     * @param timestampMs      local receipt timestamp in epoch milliseconds
     * @param symbol           PolyBolt symbol (e.g. {@code "btcusd"})
     * @param source           oracle source (e.g. {@code "chainlink"})
     * @param price            1s Chainlink BTC/USD price
     * @param priceTimestampMs oracle timestamp in epoch milliseconds
     */
    public synchronized void onPolymarketCryptoPrice(
            long timestampMs,
            String symbol,
            String source,
            double price,
            long priceTimestampMs) {
        if (price <= 0.0 || timestampMs <= 0L) {
            return;
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

        activeStrategy.onPolymarketCryptoPrice(timestampMs, symbol, source, price, priceTimestampMs);
    }

    /**
     * Processes a normalized Polymarket L2 orderbook BBO quote and forwards it to the active strategy.
     *
     * @param quote current BBO quote across Up and Down tokens
     */
    public synchronized void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        if (Log.isTraceEnabled()) {
            Log.tracef("OrderBookQuote received at %d ms: bidUp=%.3f, askUp=%.3f, bidDown=%.3f, askDown=%.3f",
                    quote.timestampMs(), quote.bestBidUp(), quote.bestAskUp(), quote.bestBidDown(), quote.bestAskDown());
        }
        this.latestQuote = quote;

        LivePosition pos = activePosition.get();
        if (pos != null) {
            double currentBid = (pos.side() == TradeDirection.UP) ? quote.bestBidUp() : quote.bestBidDown();
            boolean armed = currentBid >= (pos.entryPrice() + 0.05);
            activePosition.set(pos.withPriceObservation(currentBid, armed));
        }

        activeStrategy.onOrderBookQuote(quote);
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
    public synchronized void reset() {
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
        latestQuote = null;
        Log.info("Reset LiveTradingEngine internal state");
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
     * @return {@code "DRY_RUN"} or {@code "LIVE"}
     */
    public String getStatus() {
        return dryRun ? "DRY_RUN" : "LIVE";
    }

    /**
     * Returns whether the engine is running in dry-run simulation mode.
     *
     * @return true if dry-run mode is enabled
     */
    public boolean isDryRun() {
        return dryRun;
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
}
