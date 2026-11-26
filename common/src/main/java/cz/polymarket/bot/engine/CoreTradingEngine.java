package cz.polymarket.bot.engine;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.indicators.FastSpotMicroIndicator;
import cz.polymarket.bot.indicators.FastSpotMicroListener;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.indicators.SpotTwapListener;
import cz.polymarket.bot.orderbook.PolymarketOrderBook;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Canonical single-writer event-driven trading engine shared identically across {@code LIVE},
 * {@code PAPER}, and {@code BACKTEST} execution modes.
 * <p>
 * Centralizes all domain mechanics so backtesting and paper trading execute the exact same bytecode
 * as live production trading:
 * <ul>
 *   <li>Level 2 {@link PolymarketOrderBook} quote freshness gating ({@code maxQuoteAgeMs}),
 *       executable-state deduplication ({@link OrderBookQuote#hasSameExecutableState(OrderBookQuote)}),
 *       and zero-allocation on-demand L2 VWAP sweep queries ({@link StrategyContext}),</li>
 *   <li>Direct multi-exchange spot ingestion into {@link SpotTwapIndicator} and {@link FastSpotMicroIndicator}
 *       without bucket coalescing or timestamp distortion,</li>
 *   <li>Monotonic PolyBolt {@code price.crypto} and {@code price.crypto.twap} filtering and continuous
 *       15-minute candle rollover/settlement <b>without</b> resetting cross-candle strategy state,</li>
 *   <li>Polymarket CLOB order normalization ({@code wholeShares}, {@code priceMc}, minimum shares/notional),
 *       in-flight collateral reservation, multi-clip {@link Position} tracking, FIFO {@code SELL} closing,
 *       true strategy {@code modelProbability} capture, and {@link TradeRecord} generation.</li>
 * </ul>
 */
public class CoreTradingEngine implements StrategyContext, ExecutionRouter, SpotTwapListener, FastSpotMicroListener {

    /** Polymarket CLOB minimum marketable {@code BUY} order share count. */
    public static final long MIN_CLOB_BUY_SHARES = 5L;

    /** Polymarket CLOB minimum marketable {@code BUY} order notional in millicent-shares ({@code $1.00 = 1,000}). */
    public static final long MIN_CLOB_BUY_MC_SHARES = 1_000L;

    /** Default maximum allowed quote age in milliseconds before skipping stale order book quotes. */
    public static final long DEFAULT_MAX_QUOTE_AGE_MS = 250L;

    private static final long MAX_BOUNDARY_TWAP_OFFSET_MS = 10_000L;
    private static final double EPS = 1e-9;

    /**
     * Listener notified outside the hot tick loop when positions open, close, or settle at candle resolution.
     */
    public interface TradeLifecycleListener {
        /**
         * Invoked when a {@code BUY} fill opens a new position clip.
         *
         * @param position opened position
         * @param rttMs    order round-trip latency in milliseconds
         */
        default void onPositionOpened(Position position, long rttMs) {
        }

        /**
         * Invoked when a position (or partial clip) is closed via {@code SELL} or binary candle settlement.
         *
         * @param tradeRecord finalized trade record
         */
        default void onTradeClosed(TradeRecord tradeRecord) {
        }

        /**
         * Invoked when a completed market candle settles.
         *
         * @param candleStartSec settled candle start in epoch seconds UTC
         * @param actualOutcome  winning binary outcome direction
         * @param settledTrades  trades finalized during settlement (excluding earlier {@code SELL} exits)
         * @param modelProbUp    final estimated UP probability before resolution in {@code (0.0, 1.0)}
         */
        default void onCandleSettled(
                long candleStartSec,
                TradeDirection actualOutcome,
                List<TradeRecord> settledTrades,
                double modelProbUp) {
        }

        /**
         * Invoked when the strategy requests an asynchronous live collateral balance refresh.
         */
        default void onBalanceRefreshRequested() {
        }
    }

    private record PendingOrder(
            OrderCommand command,
            TradeDirection side,
            boolean isBuy,
            int limitPriceMc,
            double requestedShares,
            double reservedCollateralUsd,
            double modelProbSide,
            double marketAskAtSubmit,
            long candleStartSec,
            long submitTimestampMs
    ) {}

    private final PolymarketOrderBook orderBook;
    private final SpotTwapIndicator spotTwapIndicator;
    private final FastSpotMicroIndicator fastSpotMicroIndicator;
    private TradingStrategy activeStrategy;
    private OrderExecutionGateway executionGateway;
    private TradeLifecycleListener tradeLifecycleListener;

    private String symbol;
    private String polyBoltSymbol;
    private long maxQuoteAgeMs = DEFAULT_MAX_QUOTE_AGE_MS;
    private double takerFeeRate = PolymarketOrderBook.DEFAULT_TAKER_FEE_RATE;
    private boolean enforceClobMinimums = true;
    private boolean tradingEnabled = true;
    private boolean warmupMode = false;

    private long currentClockMs = 0L;
    private long activeCandleStartSec = 0L;
    private long activeCandleEndSec = 0L;
    private double twapOpenPrice = 0.0;
    private double currentSpotPrice = 0.0;
    private double currentTwap60sPrice = 0.0;
    private double lastChainlinkSpotPrice = 0.0;
    private double lastChainlinkTwapPrice = 0.0;
    private long lastPolyBoltPriceOracleTsMs = -1L;
    private long lastPolyBoltTwapOracleTsMs = -1L;

    private OrderBookQuote latestDispatchedQuote = null;
    private double cashBalanceUsd;
    private double realizedEquityUsd;
    private double reservedBuyUsd = 0.0;

    private final List<Position> openPositions = new ArrayList<>(8);
    private final Map<String, PendingOrder> pendingOrders = new LinkedHashMap<>(4);
    private final List<TradeRecord> completedTradesHistory = new ArrayList<>();

    // Diagnostics counters updated lazily or on rare order/stale events
    private long staleQuotesSkipped = 0L;
    private long lastQuoteAgeMs = 0L;
    private long maxQuoteAgeMsObserved = 0L;
    private long ordersSubmitted = 0L;
    private long ordersFilled = 0L;
    private long ordersRejected = 0L;
    private long lastOrderRttMs = 0L;
    private long maxOrderRttMs = 0L;

    /**
     * Constructs a {@link CoreTradingEngine} around the supplied orderbook, indicators, and active strategy.
     *
     * @param orderBook              canonical L2 orderbook
     * @param spotTwapIndicator      multi-exchange 60s USD TWAP indicator
     * @param fastSpotMicroIndicator 50ms direct-USD microstructure indicator
     * @param activeStrategy         active trading strategy
     * @param symbol                 asset symbol (e.g. {@code "BTC"})
     * @param initialBalanceUsd      starting account collateral balance in USD
     */
    public CoreTradingEngine(
            PolymarketOrderBook orderBook,
            SpotTwapIndicator spotTwapIndicator,
            FastSpotMicroIndicator fastSpotMicroIndicator,
            TradingStrategy activeStrategy,
            String symbol,
            double initialBalanceUsd) {
        if (orderBook == null) {
            throw new IllegalArgumentException("orderBook cannot be null");
        }
        if (spotTwapIndicator == null) {
            throw new IllegalArgumentException("spotTwapIndicator cannot be null");
        }
        if (fastSpotMicroIndicator == null) {
            throw new IllegalArgumentException("fastSpotMicroIndicator cannot be null");
        }
        if (activeStrategy == null) {
            throw new IllegalArgumentException("activeStrategy cannot be null");
        }
        if (!Double.isFinite(initialBalanceUsd) || initialBalanceUsd < 0.0) {
            throw new IllegalArgumentException("initialBalanceUsd must be finite and non-negative");
        }
        this.orderBook = orderBook;
        this.spotTwapIndicator = spotTwapIndicator;
        this.fastSpotMicroIndicator = fastSpotMicroIndicator;
        this.activeStrategy = activeStrategy;
        setSymbol(symbol);
        this.cashBalanceUsd = initialBalanceUsd;
        this.realizedEquityUsd = initialBalanceUsd;

        bindStrategyIndicatorListeners();
        this.activeStrategy.init(this);
    }

    private void bindStrategyIndicatorListeners() {
        this.spotTwapIndicator.clearListeners();
        this.fastSpotMicroIndicator.clearListeners();
        if (this.activeStrategy instanceof SpotTwapListener) {
            this.spotTwapIndicator.addListener(this);
        }
        if (this.activeStrategy instanceof FastSpotMicroListener) {
            this.fastSpotMicroIndicator.addListener(this);
        }
    }

    /**
     * Updates the traded asset symbol (e.g. {@code "BTC"} -> PolyBolt {@code "btcusd"}).
     *
     * @param symbol base crypto symbol
     */
    public void setSymbol(String symbol) {
        String clean = (symbol != null && !symbol.isBlank()) ? symbol.trim().toUpperCase(Locale.ROOT) : "BTC";
        if (clean.endsWith("USDT")) {
            clean = clean.substring(0, clean.length() - 4);
        } else if (clean.endsWith("USD") && clean.length() > 3) {
            clean = clean.substring(0, clean.length() - 3);
        }
        this.symbol = clean;
        this.polyBoltSymbol = (clean + "usd").toLowerCase(Locale.ROOT);
    }

    /** Returns the normalized base asset symbol (e.g. {@code "BTC"}). */
    public String getSymbol() {
        return symbol;
    }

    /** Returns the canonical L2 order book owned by this engine. */
    public PolymarketOrderBook getOrderBook() {
        return orderBook;
    }

    /** Returns the 60s USD spot TWAP indicator owned by this engine. */
    public SpotTwapIndicator getSpotTwapIndicator() {
        return spotTwapIndicator;
    }

    /** Returns the 50ms direct-USD microstructure indicator owned by this engine. */
    public FastSpotMicroIndicator getFastSpotMicroIndicator() {
        return fastSpotMicroIndicator;
    }

    /** Returns the currently active {@link TradingStrategy}. */
    public TradingStrategy getActiveStrategy() {
        return activeStrategy;
    }

    /**
     * Replaces the active {@link TradingStrategy} and initializes it with this engine context.
     *
     * @param strategy new active strategy
     */
    public void setActiveStrategy(TradingStrategy strategy) {
        if (strategy == null) {
            throw new IllegalArgumentException("strategy cannot be null");
        }
        this.activeStrategy = strategy;
        bindStrategyIndicatorListeners();
        this.activeStrategy.init(this);
    }

    /** Configures the order execution gateway ({@code LIVE} REST adapter or {@link SimulatedClobExecutionEngine}). */
    public void setExecutionGateway(OrderExecutionGateway executionGateway) {
        this.executionGateway = executionGateway;
    }

    /** Registers an optional listener for position, trade, and settlement lifecycle notifications. */
    public void setTradeLifecycleListener(TradeLifecycleListener listener) {
        this.tradeLifecycleListener = listener;
    }

    /** Configures the maximum acceptable quote age in milliseconds ({@code <= 0} disables quote-age rejection). */
    public void setMaxQuoteAgeMs(long maxQuoteAgeMs) {
        this.maxQuoteAgeMs = Math.max(0L, maxQuoteAgeMs);
    }

    /** Returns the configured maximum acceptable quote age in milliseconds. */
    public long getMaxQuoteAgeMs() {
        return maxQuoteAgeMs;
    }

    /** Configures the parabolic taker fee rate (default {@code 0.07}). */
    public void setTakerFeeRate(double takerFeeRate) {
        if (!Double.isFinite(takerFeeRate) || takerFeeRate < 0.0) {
            throw new IllegalArgumentException("takerFeeRate must be finite and non-negative");
        }
        this.takerFeeRate = takerFeeRate;
    }

    /** Configures whether Polymarket CLOB minimum order constraints ({@code 5} shares and {@code $1.00}) are enforced. */
    public void setEnforceClobMinimums(boolean enforceClobMinimums) {
        this.enforceClobMinimums = enforceClobMinimums;
    }

    /** Enables or disables order submission (set {@code false} in {@code OFF} mode while keeping indicators warm). */
    public void setTradingEnabled(boolean tradingEnabled) {
        this.tradingEnabled = tradingEnabled;
    }

    /** Returns {@code true} when order submission is enabled. */
    public boolean isTradingEnabled() {
        return tradingEnabled;
    }

    /**
     * Enables or disables pre-run warm-up mode. During warm-up, indicators, orderbook, and strategy ring buffers/EWMAs
     * process events normally, while order submissions and trade/settlement recordings are suppressed.
     *
     * @param warmupMode {@code true} to enter warm-up mode, {@code false} to enable live/backtest evaluation
     */
    public void setWarmupMode(boolean warmupMode) {
        this.warmupMode = warmupMode;
        if (!warmupMode) {
            // Reset active candle boundary tracking if exiting warm-up into the first evaluated candle
            this.latestDispatchedQuote = null;
        }
    }

    /** Returns {@code true} if pre-run warm-up mode is active. */
    public boolean isWarmupMode() {
        return warmupMode;
    }

    /**
     * Advances the engine clock (used by deterministic backtesting and synthetic tests).
     *
     * @param clockMs epoch timestamp in milliseconds UTC
     */
    public void setCurrentClockMs(long clockMs) {
        if (clockMs > this.currentClockMs) {
            this.currentClockMs = clockMs;
        }
    }

    /** Returns the current engine clock timestamp in epoch milliseconds UTC. */
    public long getCurrentClockMs() {
        return currentClockMs;
    }

    /**
     * Resets balance and open/pending positions (used when switching between {@code LIVE} and {@code PAPER} modes
     * or restoring persisted paper balance), without wiping warm spot indicators unless requested.
     *
     * @param balanceUsd        new collateral balance in USD
     * @param resetStrategy     {@code true} to reset internal strategy buffers as well
     * @param resetIndicators   {@code true} to reset spot indicators and orderbook
     */
    public void resetAccountState(double balanceUsd, boolean resetStrategy, boolean resetIndicators) {
        this.cashBalanceUsd = balanceUsd;
        this.realizedEquityUsd = balanceUsd;
        this.reservedBuyUsd = 0.0;
        this.openPositions.clear();
        this.pendingOrders.clear();
        this.completedTradesHistory.clear();
        this.latestDispatchedQuote = null;
        if (resetIndicators) {
            this.orderBook.clear();
            this.spotTwapIndicator.reset();
            this.fastSpotMicroIndicator.reset();
            this.lastPolyBoltPriceOracleTsMs = -1L;
            this.lastPolyBoltTwapOracleTsMs = -1L;
            this.activeCandleStartSec = 0L;
            this.activeCandleEndSec = 0L;
            this.twapOpenPrice = 0.0;
        }
        if (resetStrategy) {
            this.activeStrategy.reset();
        }
    }

    /**
     * Synchronizes free USDC collateral balance from Polymarket CLOB {@code GET /balance-allowance} in {@code LIVE} mode,
     * preserving accounting consistency with any currently open positions.
     *
     * @param liveFreeCollateralUsd free USDC collateral reported by Polymarket CLOB
     */
    public void syncLiveCollateralBalanceUsd(double liveFreeCollateralUsd) {
        if (!Double.isFinite(liveFreeCollateralUsd) || liveFreeCollateralUsd < 0.0) {
            return;
        }
        this.cashBalanceUsd = liveFreeCollateralUsd;
        double openCostWithFees = 0.0;
        for (int i = 0, n = openPositions.size(); i < n; i++) {
            Position p = openPositions.get(i);
            openCostWithFees += p.sizeUsd() + p.entryFee();
        }
        this.realizedEquityUsd = liveFreeCollateralUsd + openCostWithFees;
    }

    /**
     * Applies a signed USDC balance delta in {@code LIVE} mode (e.g., external or reverted {@code /ws/user} trade).
     *
     * @param signedDeltaUsd signed USD balance adjustment
     */
    public void applyLiveBalanceDeltaUsd(double signedDeltaUsd) {
        if (!Double.isFinite(signedDeltaUsd) || signedDeltaUsd == 0.0) {
            return;
        }
        this.cashBalanceUsd = Math.max(0.0, this.cashBalanceUsd + signedDeltaUsd);
        this.realizedEquityUsd = Math.max(0.0, this.realizedEquityUsd + signedDeltaUsd);
    }

    /**
     * Restores active candle state, PolyBolt oracle watermarks, balance, and open positions from a persisted
     * runtime snapshot without clearing warm spot indicators.
     *
     * @param candleStartSec          active candle start in epoch seconds UTC
     * @param candleEndSec            active candle end in epoch seconds UTC
     * @param restoredTwapOpen        locked 60s TWAP target price ({@code > 0} if locked)
     * @param restoredSpotPrice       latest observed spot price
     * @param lastTwapOracleTsMs      PolyBolt TWAP oracle timestamp watermark in ms
     * @param lastPriceOracleTsMs     PolyBolt spot oracle timestamp watermark in ms
     * @param upTokenId               UP conditional token ID
     * @param downTokenId             DOWN conditional token ID
     * @param balanceUsd              restored balance in USD (free collateral in {@code LIVE}, realized equity in {@code PAPER})
     * @param balanceIsFreeCollateral {@code true} when {@code balanceUsd} is already net of open positions ({@code LIVE}),
     *                                {@code false} when {@code balanceUsd} is realized equity before open cost ({@code PAPER})
     * @param restoredOpen            open positions belonging to the active candle
     */
    public void restoreActiveCandleSnapshot(
            long candleStartSec,
            long candleEndSec,
            double restoredTwapOpen,
            double restoredSpotPrice,
            long lastTwapOracleTsMs,
            long lastPriceOracleTsMs,
            String upTokenId,
            String downTokenId,
            double balanceUsd,
            boolean balanceIsFreeCollateral,
            List<Position> restoredOpen) {
        this.activeCandleStartSec = candleStartSec;
        this.activeCandleEndSec = candleEndSec;
        this.twapOpenPrice = (Double.isFinite(restoredTwapOpen) && restoredTwapOpen > 0.0) ? restoredTwapOpen : 0.0;
        if (Double.isFinite(restoredSpotPrice) && restoredSpotPrice > 0.0) {
            this.currentSpotPrice = restoredSpotPrice;
            this.lastChainlinkSpotPrice = restoredSpotPrice;
        }
        setActiveMarketTokens(upTokenId, downTokenId, true);

        this.openPositions.clear();
        this.pendingOrders.clear();
        this.reservedBuyUsd = 0.0;
        double openCost = 0.0;
        if (restoredOpen != null) {
            for (Position pos : restoredOpen) {
                if (pos != null) {
                    this.openPositions.add(pos);
                    openCost += pos.sizeUsd() + pos.entryFee();
                }
            }
        }
        double cleanBalance = (Double.isFinite(balanceUsd) && balanceUsd >= 0.0) ? balanceUsd : 0.0;
        if (balanceIsFreeCollateral) {
            this.cashBalanceUsd = cleanBalance;
            this.realizedEquityUsd = cleanBalance + openCost;
        } else {
            this.realizedEquityUsd = cleanBalance;
            this.cashBalanceUsd = Math.max(0.0, cleanBalance - openCost);
        }

        if (candleStartSec > 0L && this.twapOpenPrice > 0.0) {
            long boundaryTsMs = candleStartSec * 1_000L;
            advanceClock(boundaryTsMs);
            activeStrategy.onPolymarketCryptoTwap60s(
                    boundaryTsMs, polyBoltSymbol, "restored-state", this.twapOpenPrice, 60, boundaryTsMs);
        }
        if (Double.isFinite(restoredSpotPrice) && restoredSpotPrice > 0.0 && candleStartSec > 0L) {
            long spotTsMs = Math.max(candleStartSec * 1_000L + 1_000L, lastPriceOracleTsMs);
            if (candleEndSec <= 0L || spotTsMs < candleEndSec * 1_000L) {
                advanceClock(spotTsMs);
                activeStrategy.onPolymarketCryptoPrice(
                        spotTsMs, polyBoltSymbol, "restored-state", restoredSpotPrice, spotTsMs);
            }
        }

        if (lastTwapOracleTsMs > this.lastPolyBoltTwapOracleTsMs) {
            this.lastPolyBoltTwapOracleTsMs = lastTwapOracleTsMs;
        }
        if (lastPriceOracleTsMs > this.lastPolyBoltPriceOracleTsMs) {
            this.lastPolyBoltPriceOracleTsMs = lastPriceOracleTsMs;
        }
    }

    /**
     * Replays the currently locked {@code twapOpenPrice} into the active strategy (e.g. after switching modes mid-candle).
     */
    public void replayActiveCandleTargetPriceIfPresent() {
        if (activeCandleStartSec > 0L && twapOpenPrice > 0.0) {
            long boundaryTsMs = activeCandleStartSec * 1_000L;
            activeStrategy.onPolymarketCryptoTwap60s(
                    boundaryTsMs, polyBoltSymbol, "restored-state", twapOpenPrice, 60, boundaryTsMs);
        }
    }

    /**
     * Clears expired candle bounds and open positions without resetting warm spot indicators.
     */
    public void clearCandleBounds() {
        this.activeCandleStartSec = 0L;
        this.activeCandleEndSec = 0L;
        this.twapOpenPrice = 0.0;
    }

    /**
     * Updates the active Polymarket conditional token IDs and optionally clears stale previous-market book depth.
     *
     * @param upTokenId   UP ("Yes") conditional token ID
     * @param downTokenId DOWN ("No") conditional token ID
     * @param clearBook   {@code true} to clear existing L2 levels when rotating to a new 15m market
     */
    public void setActiveMarketTokens(String upTokenId, String downTokenId, boolean clearBook) {
        boolean changed = (upTokenId != null && !upTokenId.equals(orderBook.getUpTokenId()))
                || (downTokenId != null && !downTokenId.equals(orderBook.getDownTokenId()));
        if (clearBook && changed) {
            orderBook.clear();
            latestDispatchedQuote = null;
        }
        orderBook.setMarketTokenIds(upTokenId, downTokenId);
    }

    @Override
    public ExecutionRouter getExecutionRouter() {
        return this;
    }

    @Override
    public double getAccountBalanceUsd() {
        return Math.max(0.0, cashBalanceUsd - reservedBuyUsd);
    }

    /** Returns cumulative realized account equity ({@code initialBalance + sum(realizedNetPnl)}). */
    public double getRealizedEquityUsd() {
        return realizedEquityUsd;
    }

    /** Returns free cash balance in USD (after subtracting open position costs, before in-flight reservations). */
    public double getCashBalanceUsd() {
        return cashBalanceUsd;
    }

    @Override
    public double estimateBuyFillableShares(TradeDirection side, double limitPrice) {
        if (side == null || side == TradeDirection.NO_TRADE) {
            return 0.0;
        }
        return orderBook.estimateBuyFillableShares(side == TradeDirection.UP, limitPrice);
    }

    @Override
    public double estimateBuyFillVwap(TradeDirection side, double requestedShares, double limitPrice) {
        if (side == null || side == TradeDirection.NO_TRADE) {
            return Double.NaN;
        }
        return orderBook.estimateBuyFillVwap(side == TradeDirection.UP, requestedShares, limitPrice);
    }

    @Override
    public void refreshAccountBalance() {
        if (!warmupMode && tradeLifecycleListener != null) {
            tradeLifecycleListener.onBalanceRefreshRequested();
        }
    }

    /**
     * Ingests a Kraken Spot {@code USDT/USD} peg trade into both spot indicators.
     *
     * @param arrivalTimestampMs effective arrival timestamp in milliseconds UTC
     * @param usdtUsdPrice       trade price
     */
    public void onKrakenUsdtUsdTrade(long arrivalTimestampMs, double usdtUsdPrice) {
        advanceClock(arrivalTimestampMs);
        if (spotTwapIndicator.hasListeners()) {
            spotTwapIndicator.onUsdtUsdTrade(arrivalTimestampMs, usdtUsdPrice);
        }
        if (fastSpotMicroIndicator.hasListeners()) {
            fastSpotMicroIndicator.onUsdtUsdTrade(arrivalTimestampMs, usdtUsdPrice);
        }
    }

    /**
     * Ingests a Binance Spot {@code <ASSET>USDT} trade directly into active spot indicators and dispatches updates.
     *
     * @param arrivalTimestampMs effective arrival timestamp in milliseconds UTC
     * @param usdtPrice          trade price in USDT
     * @param quantity           base-asset trade quantity
     * @param isTakerBuy         {@code true} when buyer is aggressor ({@code !isBuyerMaker})
     */
    public void onBinanceUsdtTrade(long arrivalTimestampMs, double usdtPrice, double quantity, boolean isTakerBuy) {
        advanceClock(arrivalTimestampMs);
        if (spotTwapIndicator.hasListeners()) {
            spotTwapIndicator.onBinanceUsdtTrade(arrivalTimestampMs, usdtPrice, quantity);
            spotTwapIndicator.notifyListeners(arrivalTimestampMs, polyBoltSymbol);
        }
        if (fastSpotMicroIndicator.hasListeners()) {
            fastSpotMicroIndicator.onBinanceUsdtTrade(arrivalTimestampMs, usdtPrice, quantity, isTakerBuy);
            fastSpotMicroIndicator.notifyListeners(arrivalTimestampMs, polyBoltSymbol);
        }
    }

    /**
     * Ingests a Coinbase Spot {@code <ASSET>USD} trade directly into active spot indicators and dispatches updates.
     *
     * @param arrivalTimestampMs effective arrival timestamp in milliseconds UTC
     * @param usdPrice           trade price in USD
     * @param quantity           base-asset trade quantity
     * @param isTakerBuy         {@code true} when buyer is aggressor
     */
    public void onCoinbaseUsdTrade(long arrivalTimestampMs, double usdPrice, double quantity, boolean isTakerBuy) {
        advanceClock(arrivalTimestampMs);
        if (spotTwapIndicator.hasListeners()) {
            spotTwapIndicator.onCoinbaseUsdTrade(arrivalTimestampMs, usdPrice, quantity);
            spotTwapIndicator.notifyListeners(arrivalTimestampMs, polyBoltSymbol);
        }
        if (fastSpotMicroIndicator.hasListeners()) {
            fastSpotMicroIndicator.onCoinbaseUsdTrade(arrivalTimestampMs, usdPrice, quantity, isTakerBuy);
            fastSpotMicroIndicator.notifyListeners(arrivalTimestampMs, polyBoltSymbol);
        }
    }

    /**
     * Ingests a Kraken Spot {@code <ASSET>USD} trade directly into active spot indicators and dispatches updates.
     *
     * @param arrivalTimestampMs effective arrival timestamp in milliseconds UTC
     * @param usdPrice           trade price in USD
     * @param quantity           base-asset trade quantity
     * @param isTakerBuy         {@code true} when buyer is aggressor
     */
    public void onKrakenUsdTrade(long arrivalTimestampMs, double usdPrice, double quantity, boolean isTakerBuy) {
        advanceClock(arrivalTimestampMs);
        if (spotTwapIndicator.hasListeners()) {
            spotTwapIndicator.onKrakenUsdTrade(arrivalTimestampMs, usdPrice, quantity);
            spotTwapIndicator.notifyListeners(arrivalTimestampMs, polyBoltSymbol);
        }
        if (fastSpotMicroIndicator.hasListeners()) {
            fastSpotMicroIndicator.onKrakenUsdTrade(arrivalTimestampMs, usdPrice, quantity, isTakerBuy);
            fastSpotMicroIndicator.notifyListeners(arrivalTimestampMs, polyBoltSymbol);
        }
    }

    @Override
    public void onSpotTwap(long timestampMs, String symbolArg, double spotPrice, double twap60sPrice) {
        advanceClock(timestampMs);
        this.currentSpotPrice = spotPrice;
        this.currentTwap60sPrice = twap60sPrice;
        if (activeStrategy instanceof SpotTwapListener listener) {
            listener.onSpotTwap(timestampMs, symbolArg != null ? symbolArg : polyBoltSymbol, spotPrice, twap60sPrice);
        }
    }

    @Override
    public void onFastSpotMicro(
            long timestampMs,
            String symbolArg,
            double fastDirectUsd,
            double signedOfi1s,
            double syncMove500Usd) {
        advanceClock(timestampMs);
        if (currentSpotPrice <= 0.0) {
            this.currentSpotPrice = fastDirectUsd;
        }
        if (activeStrategy instanceof FastSpotMicroListener listener) {
            listener.onFastSpotMicro(
                    timestampMs, symbolArg != null ? symbolArg : polyBoltSymbol, fastDirectUsd, signedOfi1s, syncMove500Usd);
        }
    }

    /**
     * Processes a PolyBolt {@code price.crypto} 1s Chainlink oracle price update with strict monotonic
     * {@code oracleTimestampMs} deduplication identical across {@code LIVE}, {@code PAPER}, and {@code BACKTEST}.
     *
     * @param receiptTimestampMs local receipt timestamp in epoch milliseconds UTC
     * @param tickSymbol         PolyBolt symbol (e.g. {@code "btcusd"})
     * @param source             oracle source (e.g. {@code "chainlink"})
     * @param price              full-accuracy oracle spot price in USD
     * @param oracleTimestampMs  Chainlink oracle source timestamp in epoch milliseconds UTC
     * @return {@code true} if accepted, {@code false} if filtered as duplicate/stale
     */
    public boolean onPolyBoltCryptoPrice(
            long receiptTimestampMs,
            String tickSymbol,
            String source,
            double price,
            long oracleTimestampMs) {
        if (!Double.isFinite(price) || price <= 0.0) {
            return false;
        }
        if (oracleTimestampMs > 0L) {
            if (oracleTimestampMs <= lastPolyBoltPriceOracleTsMs) {
                return false;
            }
            lastPolyBoltPriceOracleTsMs = oracleTimestampMs;
        }
        advanceClock(receiptTimestampMs);
        this.lastChainlinkSpotPrice = price;
        this.currentSpotPrice = price;
        if (!warmupMode && activeCandleStartSec == 0L && receiptTimestampMs > 0L) {
            long durationSeconds = activeStrategy.getTimeframe().getDurationSeconds();
            long candleSec = ((receiptTimestampMs / 1_000L) / durationSeconds) * durationSeconds;
            this.activeCandleStartSec = candleSec;
            this.activeCandleEndSec = candleSec + durationSeconds;
        }
        activeStrategy.onPolymarketCryptoPrice(
                receiptTimestampMs,
                tickSymbol != null ? tickSymbol : polyBoltSymbol,
                source != null ? source : "chainlink",
                price,
                oracleTimestampMs
        );
        return true;
    }

    /**
     * Processes a PolyBolt {@code price.crypto.twap} 60s Chainlink TWAP update with strict monotonic
     * {@code oracleTimestampMs} deduplication and automatic 15m candle cycle rollover/settlement.
     *
     * @param receiptTimestampMs local receipt timestamp in epoch milliseconds UTC
     * @param tickSymbol         PolyBolt symbol (e.g. {@code "btcusd"})
     * @param source             oracle source (e.g. {@code "chainlink"})
     * @param twapPrice          full-accuracy 60s TWAP price in USD
     * @param windowSeconds      TWAP window length in seconds (typically {@code 60})
     * @param oracleTimestampMs  Chainlink oracle source timestamp in epoch milliseconds UTC
     * @return {@code true} if accepted, {@code false} if filtered as duplicate/stale
     */
    public boolean onPolyBoltCryptoTwap60s(
            long receiptTimestampMs,
            String tickSymbol,
            String source,
            double twapPrice,
            int windowSeconds,
            long oracleTimestampMs) {
        if (!Double.isFinite(twapPrice) || twapPrice <= 0.0) {
            return false;
        }
        if (oracleTimestampMs > 0L) {
            if (oracleTimestampMs <= lastPolyBoltTwapOracleTsMs) {
                return false;
            }
            lastPolyBoltTwapOracleTsMs = oracleTimestampMs;
        }
        advanceClock(receiptTimestampMs);
        this.lastChainlinkTwapPrice = twapPrice;
        this.currentTwap60sPrice = twapPrice;

        long durationSeconds = activeStrategy.getTimeframe().getDurationSeconds();
        long effectiveTimeMs = oracleTimestampMs > 0L ? oracleTimestampMs : receiptTimestampMs;
        long effectiveSec = effectiveTimeMs / 1_000L;
        long candleStartSec = (effectiveSec / durationSeconds) * durationSeconds;
        long offsetFromBoundaryMs = Math.max(0L, effectiveTimeMs - (candleStartSec * 1_000L));

        if (!warmupMode) {
            if (activeCandleStartSec > 0L && candleStartSec < activeCandleStartSec) {
                activeStrategy.onPolymarketCryptoTwap60s(
                        receiptTimestampMs,
                        tickSymbol != null ? tickSymbol : polyBoltSymbol,
                        source != null ? source : "chainlink",
                        twapPrice,
                        windowSeconds,
                        oracleTimestampMs
                );
                return true;
            }
            if (activeCandleStartSec > 0L && candleStartSec > activeCandleStartSec) {
                long expectedPrevCandleStart = candleStartSec - durationSeconds;
                if (activeCandleStartSec == expectedPrevCandleStart
                        && offsetFromBoundaryMs <= MAX_BOUNDARY_TWAP_OFFSET_MS
                        && twapOpenPrice > 0.0) {
                    TradeDirection outcome = twapPrice >= twapOpenPrice ? TradeDirection.UP : TradeDirection.DOWN;
                    resolveCandle(outcome, twapPrice, receiptTimestampMs);
                } else {
                    Log.warnf("Skipping candle resolution for intervalStart=%d (newCandleStart=%d, offsetMs=%d, twapOpen=%.2f): missing target or skipped rollover",
                            activeCandleStartSec, candleStartSec, offsetFromBoundaryMs, twapOpenPrice);
                    boolean hadOpen = !openPositions.isEmpty();
                    abandonOpenPositionsWithoutSettlement();
                    if (hadOpen && tradeLifecycleListener != null) {
                        tradeLifecycleListener.onBalanceRefreshRequested();
                    }
                }
                activeCandleStartSec = candleStartSec;
                activeCandleEndSec = candleStartSec + durationSeconds;
                twapOpenPrice = offsetFromBoundaryMs <= MAX_BOUNDARY_TWAP_OFFSET_MS ? twapPrice : 0.0;
            } else if (activeCandleStartSec == 0L) {
                activeCandleStartSec = candleStartSec;
                activeCandleEndSec = candleStartSec + durationSeconds;
                twapOpenPrice = offsetFromBoundaryMs <= MAX_BOUNDARY_TWAP_OFFSET_MS ? twapPrice : 0.0;
            } else if (twapOpenPrice <= 0.0 && offsetFromBoundaryMs <= MAX_BOUNDARY_TWAP_OFFSET_MS) {
                twapOpenPrice = twapPrice;
            }
        }

        activeStrategy.onPolymarketCryptoTwap60s(
                receiptTimestampMs,
                tickSymbol != null ? tickSymbol : polyBoltSymbol,
                source != null ? source : "chainlink",
                twapPrice,
                windowSeconds,
                oracleTimestampMs
        );
        return true;
    }

    /**
     * Explicitly starts or transitions into a known 15m candle cycle (used by {@code BACKTEST} metadata or
     * {@code PolymarketLifecycleCoordinator} in {@code bot}).
     *
     * @param candleStartSec candle start in epoch seconds UTC
     * @param candleEndSec   candle end in epoch seconds UTC
     * @param targetTwapOpen known opening 60s TWAP target price ({@code > 0} if known, {@code 0.0} otherwise)
     * @param upTokenId      active UP conditional token ID
     * @param downTokenId    active DOWN conditional token ID
     * @param clearBook      {@code true} to clear orderbook levels when rotating markets
     */
    public void beginCandleCycle(
            long candleStartSec,
            long candleEndSec,
            double targetTwapOpen,
            String upTokenId,
            String downTokenId,
            boolean clearBook) {
        this.activeCandleStartSec = candleStartSec;
        this.activeCandleEndSec = candleEndSec;
        if (targetTwapOpen > 0.0 && Double.isFinite(targetTwapOpen)) {
            this.twapOpenPrice = targetTwapOpen;
        } else if (clearBook) {
            this.twapOpenPrice = 0.0;
        }
        setActiveMarketTokens(upTokenId, downTokenId, clearBook);
    }

    /**
     * Evaluates the current {@link PolymarketOrderBook} state after a batch of mutations, enforcing
     * {@code maxQuoteAgeMs} freshness and {@link OrderBookQuote#hasSameExecutableState(OrderBookQuote)}
     * deduplication before notifying {@link TradingStrategy#onOrderBookQuote(OrderBookQuote)}.
     *
     * @param evalTimestampMs evaluation timestamp in epoch milliseconds UTC
     */
    public void onOrderBookUpdated(long evalTimestampMs) {
        advanceClock(evalTimestampMs);
        long bookTs = orderBook.getLastUpdateTimeMs();
        long nowMs = currentClockMs > 0L ? currentClockMs : evalTimestampMs;
        if (bookTs > 0L && nowMs >= bookTs) {
            long ageMs = nowMs - bookTs;
            this.lastQuoteAgeMs = ageMs;
            if (ageMs > maxQuoteAgeMsObserved) {
                maxQuoteAgeMsObserved = ageMs;
            }
            if (maxQuoteAgeMs > 0L && ageMs > maxQuoteAgeMs) {
                staleQuotesSkipped++;
                return;
            }
        }

        OrderBookQuote nextQuote = orderBook.toOrderBookQuoteOrNull(evalTimestampMs > 0L ? evalTimestampMs : bookTs);
        if (nextQuote == null) {
            return;
        }
        if (nextQuote.hasSameExecutableState(latestDispatchedQuote)) {
            return;
        }
        this.latestDispatchedQuote = nextQuote;
        updatePositionPriceObservations(nextQuote);
        activeStrategy.onOrderBookQuote(nextQuote);
    }

    /**
     * Publishes a pre-seeded L2 order book snapshot at the start of a bounded backtest CLOB lookback window
     * without rejecting it against an already-advanced oracle/spot clock.
     *
     * @param seededTimestampMs capture timestamp of the last pre-window CLOB frame in epoch milliseconds UTC
     */
    public void publishSeededOrderBook(long seededTimestampMs) {
        OrderBookQuote nextQuote = orderBook.toOrderBookQuoteOrNull(
                seededTimestampMs > 0L ? seededTimestampMs : orderBook.getLastUpdateTimeMs());
        if (nextQuote == null || nextQuote.hasSameExecutableState(latestDispatchedQuote)) {
            return;
        }
        this.latestDispatchedQuote = nextQuote;
        updatePositionPriceObservations(nextQuote);
        activeStrategy.onOrderBookQuote(nextQuote);
    }

    /**
     * Directly forwards an explicit {@link OrderBookQuote} (for synthetic tests or external callers),
     * enforcing {@code maxQuoteAgeMs} freshness, seeding top-of-book L2 levels in {@link PolymarketOrderBook},
     * and applying executable-state deduplication.
     *
     * @param quote order book quote
     */
    public void onDirectOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        if (quote.timestampMs() > 0L) {
            advanceClock(quote.timestampMs());
            long ageMs = Math.max(0L, currentClockMs - quote.timestampMs());
            this.lastQuoteAgeMs = ageMs;
            if (ageMs > maxQuoteAgeMsObserved) {
                maxQuoteAgeMsObserved = ageMs;
            }
            if (maxQuoteAgeMs > 0L && ageMs > maxQuoteAgeMs) {
                staleQuotesSkipped++;
                return;
            }
        }
        seedOrderBookFromDirectQuote(quote);
        if (quote.hasSameExecutableState(latestDispatchedQuote)) {
            return;
        }
        this.latestDispatchedQuote = quote;
        updatePositionPriceObservations(quote);
        activeStrategy.onOrderBookQuote(quote);
    }

    private void seedOrderBookFromDirectQuote(OrderBookQuote quote) {
        orderBook.clearConsumedLiquidity();
        orderBook.clearSideForSnapshot(true);
        int bidUpTick = PolymarketOrderBook.priceToTick(quote.bestBidUp());
        int askUpTick = PolymarketOrderBook.priceToTick(quote.bestAskUp());
        double upAskDepth = quote.depth1cUp() > 0.0 ? quote.depth1cUp() : 100.0;
        double upBidDepth = Math.max(upAskDepth, 100.0);
        if (bidUpTick > 0) {
            orderBook.setSnapshotLevel(true, true, bidUpTick, upBidDepth);
        }
        if (askUpTick > 0) {
            orderBook.setSnapshotLevel(true, false, askUpTick, upAskDepth);
        }
        orderBook.finishBookSnapshot(true, quote.timestampMs());

        orderBook.clearSideForSnapshot(false);
        int bidDownTick = PolymarketOrderBook.priceToTick(quote.bestBidDown());
        int askDownTick = PolymarketOrderBook.priceToTick(quote.bestAskDown());
        double downAskDepth = quote.depth1cDown() > 0.0 ? quote.depth1cDown() : 100.0;
        double downBidDepth = Math.max(downAskDepth, 100.0);
        if (bidDownTick > 0) {
            orderBook.setSnapshotLevel(false, true, bidDownTick, downBidDepth);
        }
        if (askDownTick > 0) {
            orderBook.setSnapshotLevel(false, false, askDownTick, downAskDepth);
        }
        orderBook.finishBookSnapshot(false, quote.timestampMs());
    }

    @Override
    public void submitOrder(OrderCommand command) {
        if (command == null || !tradingEnabled || warmupMode) {
            return;
        }
        long submitTs = currentClockMs > 0L ? currentClockMs : System.currentTimeMillis();
        boolean isBuy = "BUY".equalsIgnoreCase(command.side());
        boolean isUp = orderBook.isUpToken(command.token());
        TradeDirection side = isUp ? TradeDirection.UP : TradeDirection.DOWN;
        int limitPriceMc = Math.clamp((int) Math.round(command.price().doubleValue() * 1_000.0), 1, 999);

        if (isBuy) {
            long wholeShares = Math.max(0L, Math.round(command.size().doubleValue()));
            if (wholeShares <= 0L
                    || (enforceClobMinimums && (wholeShares < MIN_CLOB_BUY_SHARES
                    || (wholeShares * limitPriceMc) < MIN_CLOB_BUY_MC_SHARES))) {
                ordersRejected++;
                ExecutionReport reject = new ExecutionReport(
                        command.clientOrderId(),
                        "",
                        "REJECTED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        submitTs,
                        "Order below Polymarket minimum (5 shares and $1.00 notional)"
                );
                activeStrategy.onExecutionReport(reject);
                return;
            }

            double limitPrice = limitPriceMc * 1e-3;
            double worstFee = wholeShares * takerFeeRate * limitPrice * Math.max(0.0, 1.0 - limitPrice);
            double requiredCollateralUsd = wholeShares * limitPrice + worstFee;
            double availableUsd = cashBalanceUsd - reservedBuyUsd;
            if (availableUsd + 1e-6 < requiredCollateralUsd) {
                ordersRejected++;
                if (Log.isDebugEnabled()) {
                    Log.debugf("Rejected BUY %s: available=$%.2f < required=$%.2f",
                            command.clientOrderId(), availableUsd, requiredCollateralUsd);
                }
                ExecutionReport reject = new ExecutionReport(
                        command.clientOrderId(),
                        "",
                        "REJECTED",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        submitTs,
                        "Insufficient balance"
                );
                activeStrategy.onExecutionReport(reject);
                return;
            }

            double pUp = activeStrategy.getLastModelProbabilityUp();
            double marketAsk = resolveCurrentMarketAsk(side, limitPrice);
            double modelProbSide;
            if (Double.isFinite(pUp) && pUp > 0.0 && pUp < 1.0) {
                modelProbSide = side == TradeDirection.UP ? pUp : (1.0 - pUp);
            } else {
                modelProbSide = Math.min(0.95, marketAsk + 0.10);
            }

            long candleSec = activeCandleStartSec > 0L
                    ? activeCandleStartSec
                    : ((submitTs / 1_000L) / activeStrategy.getTimeframe().getDurationSeconds())
                    * activeStrategy.getTimeframe().getDurationSeconds();

            PendingOrder pending = new PendingOrder(
                    command,
                    side,
                    true,
                    limitPriceMc,
                    wholeShares,
                    requiredCollateralUsd,
                    modelProbSide,
                    marketAsk,
                    candleSec,
                    submitTs
            );
            reservedBuyUsd += requiredCollateralUsd;
            pendingOrders.put(command.clientOrderId(), pending);
            ordersSubmitted++;

            if (executionGateway != null) {
                executionGateway.executeOrder(command, side, true, limitPriceMc, wholeShares, submitTs);
            }
        } else {
            double openSideShares = getOpenSharesForSide(side);
            if (openSideShares <= EPS) {
                return;
            }
            double sellShares = Math.min(openSideShares, command.size().doubleValue());
            long candleSec = activeCandleStartSec;
            PendingOrder pending = new PendingOrder(
                    command,
                    side,
                    false,
                    limitPriceMc,
                    sellShares,
                    0.0,
                    0.50,
                    limitPriceMc * 1e-3,
                    candleSec,
                    submitTs
            );
            pendingOrders.put(command.clientOrderId(), pending);
            ordersSubmitted++;

            if (executionGateway != null) {
                executionGateway.executeOrder(command, side, false, limitPriceMc, sellShares, submitTs);
            }
        }
    }

    @Override
    public void cancelOrder(String clientOrderId) {
        if (clientOrderId == null || clientOrderId.isBlank()) {
            return;
        }
        long ts = currentClockMs > 0L ? currentClockMs : System.currentTimeMillis();
        if (executionGateway != null) {
            executionGateway.cancelOrder(clientOrderId, ts);
        }
    }

    /**
     * Processes an execution report arriving from {@link OrderExecutionGateway} (either simulated or live CLOB),
     * updating collateral reservations, positions, realized equity, and notifying the active strategy.
     *
     * @param report            execution report
     * @param reportTimestampMs arrival timestamp of the report in epoch milliseconds UTC
     * @param rttMs             measured or simulated round-trip order latency in milliseconds
     */
    public void onExecutionReport(ExecutionReport report, long reportTimestampMs, long rttMs) {
        if (report == null) {
            return;
        }
        advanceClock(reportTimestampMs);
        if (rttMs >= 0L) {
            this.lastOrderRttMs = rttMs;
            if (rttMs > this.maxOrderRttMs) {
                this.maxOrderRttMs = rttMs;
            }
        }

        PendingOrder pending = pendingOrders.remove(report.clientOrderId());
        if (pending != null && pending.reservedCollateralUsd > 0.0) {
            reservedBuyUsd = Math.max(0.0, reservedBuyUsd - pending.reservedCollateralUsd);
        }

        String status = report.status();
        boolean isFill = "FILLED".equalsIgnoreCase(status) || "PARTIALLY_FILLED".equalsIgnoreCase(status);
        double executedShares = report.executedSize() != null ? report.executedSize().doubleValue() : 0.0;
        double executedPrice = report.executedPrice() != null ? report.executedPrice().doubleValue() : 0.0;

        if (isFill && executedShares > EPS && executedPrice > 0.0 && pending != null) {
            ordersFilled++;
            if (pending.isBuy) {
                double sizeUsd = executedShares * executedPrice;
                double entryFee = (executedPrice > 0.0 && executedPrice < 1.0)
                        ? (executedShares * takerFeeRate * executedPrice * (1.0 - executedPrice))
                        : 0.0;
                cashBalanceUsd = Math.max(0.0, cashBalanceUsd - (sizeUsd + entryFee));
                double edge = Math.max(0.0, pending.modelProbSide - executedPrice);
                String clobTokenId = pending.side == TradeDirection.UP
                        ? orderBook.getUpTokenId()
                        : orderBook.getDownTokenId();

                Position pos = new Position(
                        pending.command.clientOrderId(),
                        report.exchangeOrderId() != null ? report.exchangeOrderId() : "",
                        pending.side,
                        executedPrice,
                        executedShares,
                        sizeUsd,
                        entryFee,
                        reportTimestampMs > 0L ? reportTimestampMs : currentClockMs,
                        pending.candleStartSec,
                        pending.command.token(),
                        clobTokenId,
                        pending.modelProbSide,
                        pending.marketAskAtSubmit > 0.0 ? pending.marketAskAtSubmit : executedPrice,
                        edge,
                        executedPrice,
                        false
                );
                openPositions.add(pos);
                if (tradeLifecycleListener != null && !warmupMode) {
                    tradeLifecycleListener.onPositionOpened(pos, rttMs);
                }
            } else {
                closeOpenPositionsFifo(pending.side, executedShares, executedPrice);
            }
        } else if ("REJECTED".equalsIgnoreCase(status)
                || "EXPIRED".equalsIgnoreCase(status)) {
            ordersRejected++;
        }

        activeStrategy.onExecutionReport(report);

        // When simulated execution consumed L2 book depth, refresh strategy's view of the book if valid
        if (isFill && orderBook.hasValidTwoSidedQuote()) {
            onOrderBookUpdated(reportTimestampMs > 0L ? reportTimestampMs : currentClockMs);
        }
    }

    /**
     * Settles all open positions against the binary candle resolution outcome, updates cash and realized equity,
     * notifies {@link TradingStrategy#onCandleResolution(TradeDirection)}, and emits finalized {@link TradeRecord}s.
     *
     * @param actualOutcome       winning binary outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
     * @param finalReferencePrice closing reference price (for logging/diagnostics)
     * @param settlementTimeMs    settlement timestamp in epoch milliseconds UTC
     * @return unmodifiable list of {@link TradeRecord}s settled at resolution
     */
    public List<TradeRecord> resolveCandle(
            TradeDirection actualOutcome,
            double finalReferencePrice,
            long settlementTimeMs) {
        advanceClock(settlementTimeMs);
        TradeDirection resolvedOutcome = (actualOutcome == null || actualOutcome == TradeDirection.NO_TRADE)
                ? TradeDirection.UP
                : actualOutcome;

        double modelProbUpBeforeReset = estimateCurrentModelProbUp();
        // Cancel/clear any stranded pending reservations at expiry
        if (!pendingOrders.isEmpty()) {
            pendingOrders.clear();
            reservedBuyUsd = 0.0;
        }

        activeStrategy.onCandleResolution(resolvedOutcome);

        if (openPositions.isEmpty()) {
            if (tradeLifecycleListener != null && !warmupMode && activeCandleStartSec > 0L) {
                tradeLifecycleListener.onCandleSettled(
                        activeCandleStartSec, resolvedOutcome, List.of(), modelProbUpBeforeReset);
            }
            return List.of();
        }

        List<TradeRecord> settledRecords = new ArrayList<>(openPositions.size());
        for (Position pos : openPositions) {
            double exitPrice = (pos.side() == resolvedOutcome) ? 1.00 : 0.00;
            double payout = pos.shares() * exitPrice;
            double netPnl = payout - pos.sizeUsd() - pos.entryFee();
            cashBalanceUsd += payout;
            realizedEquityUsd += netPnl;
            boolean isWin = netPnl > 0.0;

            TradeRecord record = new TradeRecord(
                    Instant.ofEpochMilli(pos.entryTimestampMs()).toString(),
                    pos.intervalStartSec(),
                    pos.side(),
                    pos.entryPrice(),
                    exitPrice,
                    pos.sizeUsd(),
                    pos.shares(),
                    pos.entryFee(),
                    netPnl,
                    "Resolution (TWAP Oracle)",
                    pos.modelProbability(),
                    pos.marketPrice(),
                    pos.edge(),
                    isWin,
                    realizedEquityUsd
            );
            settledRecords.add(record);
            completedTradesHistory.add(record);
            if (tradeLifecycleListener != null && !warmupMode) {
                tradeLifecycleListener.onTradeClosed(record);
            }
        }
        openPositions.clear();

        List<TradeRecord> immutableSettled = Collections.unmodifiableList(settledRecords);
        if (tradeLifecycleListener != null && !warmupMode) {
            long settledCandleSec = activeCandleStartSec > 0L
                    ? activeCandleStartSec
                    : settledRecords.getFirst().intervalStartSec();
            tradeLifecycleListener.onCandleSettled(
                    settledCandleSec, resolvedOutcome, immutableSettled, modelProbUpBeforeReset);
        }
        return immutableSettled;
    }

    /**
     * Estimates the current {@link TradeDirection#UP} probability for Brier calibration, preferring
     * {@link TradingStrategy#getLastModelProbabilityUp()} and falling back to opening-TWAP distance.
     *
     * @return estimated {@code UP} probability in {@code (0.0, 1.0)}
     */
    public double estimateCurrentModelProbUp() {
        double pUp = activeStrategy.getLastModelProbabilityUp();
        if (Double.isFinite(pUp) && pUp > 0.0 && pUp < 1.0) {
            return pUp;
        }
        if (twapOpenPrice > 0.0 && currentSpotPrice > 0.0) {
            double dist = currentSpotPrice - twapOpenPrice;
            if (dist > 80.0) {
                return 0.94;
            } else if (dist < -80.0) {
                return 0.06;
            }
        }
        return 0.50;
    }

    /** Returns an unmodifiable view of currently open positions. */
    public List<Position> getOpenPositions() {
        return Collections.unmodifiableList(openPositions);
    }

    /** Returns the most recently opened position if any exists, or {@code null}. */
    public Position getLatestOpenPosition() {
        return openPositions.isEmpty() ? null : openPositions.getLast();
    }

    /** Returns an unmodifiable view of completed trades recorded in this engine session. */
    public List<TradeRecord> getCompletedTradesHistory() {
        return Collections.unmodifiableList(completedTradesHistory);
    }

    /** Returns the latest dispatched {@link OrderBookQuote}, or {@code null}. */
    public OrderBookQuote getLatestDispatchedQuote() {
        return latestDispatchedQuote;
    }

    /** Returns active candle start in epoch seconds UTC. */
    public long getActiveCandleStartSec() {
        return activeCandleStartSec;
    }

    /** Returns active candle end in epoch seconds UTC. */
    public long getActiveCandleEndSec() {
        return activeCandleEndSec;
    }

    /** Returns the locked 60s TWAP opening target price for the active candle. */
    public double getTwapOpenPrice() {
        return twapOpenPrice;
    }

    /** Returns the latest observed underlying spot price in USD. */
    public double getCurrentSpotPrice() {
        return currentSpotPrice;
    }

    /** Returns the latest observed 60s TWAP price in USD. */
    public double getCurrentTwap60sPrice() {
        return currentTwap60sPrice;
    }

    /** Returns latest Chainlink 1s spot price in USD. */
    public double getLastChainlinkSpotPrice() {
        return lastChainlinkSpotPrice;
    }

    /** Returns latest Chainlink 60s TWAP price in USD. */
    public double getLastChainlinkTwapPrice() {
        return lastChainlinkTwapPrice;
    }

    /** Returns count of stale orderbook updates skipped due to {@code maxQuoteAgeMs}. */
    public long getStaleQuotesSkipped() {
        return staleQuotesSkipped;
    }

    /** Returns the age in milliseconds of the most recently evaluated orderbook quote. */
    public long getLastQuoteAgeMs() {
        return lastQuoteAgeMs;
    }

    /** Returns maximum observed quote age in milliseconds. */
    public long getMaxQuoteAgeMsObserved() {
        return maxQuoteAgeMsObserved;
    }

    /** Returns the high-water mark PolyBolt 60s TWAP oracle timestamp in milliseconds ({@code 0} if none). */
    public long getLastPolyBoltTwapOracleTsMs() {
        return Math.max(0L, lastPolyBoltTwapOracleTsMs);
    }

    /** Returns the high-water mark PolyBolt 1s spot oracle timestamp in milliseconds ({@code 0} if none). */
    public long getLastPolyBoltPriceOracleTsMs() {
        return Math.max(0L, lastPolyBoltPriceOracleTsMs);
    }

    /** Returns total count of submitted orders. */
    public long getOrdersSubmitted() {
        return ordersSubmitted;
    }

    /** Returns total count of filled orders. */
    public long getOrdersFilled() {
        return ordersFilled;
    }

    /** Returns total count of rejected orders. */
    public long getOrdersRejected() {
        return ordersRejected;
    }

    /** Returns most recent order round-trip latency in milliseconds. */
    public long getLastOrderRttMs() {
        return lastOrderRttMs;
    }

    /** Returns maximum observed order round-trip latency in milliseconds. */
    public long getMaxOrderRttMs() {
        return maxOrderRttMs;
    }

    /** Returns {@code true} if there is currently any pending in-flight order. */
    public boolean hasPendingOrders() {
        return !pendingOrders.isEmpty();
    }

    private void advanceClock(long timestampMs) {
        if (timestampMs > this.currentClockMs) {
            this.currentClockMs = timestampMs;
        }
    }

    private void abandonOpenPositionsWithoutSettlement() {
        if (!pendingOrders.isEmpty()) {
            pendingOrders.clear();
            reservedBuyUsd = 0.0;
        }
        if (!openPositions.isEmpty()) {
            // Refund reserved cost basis to cashBalanceUsd since trade was neither settled nor recorded
            for (Position pos : openPositions) {
                cashBalanceUsd += pos.sizeUsd() + pos.entryFee();
            }
            openPositions.clear();
        }
        activeStrategy.onCandleResolution(TradeDirection.NO_TRADE);
    }

    private double resolveCurrentMarketAsk(TradeDirection side, double fallbackLimitPrice) {
        double ask = side == TradeDirection.UP ? orderBook.getBestAskUp() : orderBook.getBestAskDown();
        if (Double.isFinite(ask) && ask > 0.0 && ask <= 1.0) {
            return ask;
        }
        if (latestDispatchedQuote != null) {
            return side == TradeDirection.UP ? latestDispatchedQuote.bestAskUp() : latestDispatchedQuote.bestAskDown();
        }
        return fallbackLimitPrice;
    }

    private double getOpenSharesForSide(TradeDirection side) {
        double sum = 0.0;
        for (int i = 0, n = openPositions.size(); i < n; i++) {
            Position p = openPositions.get(i);
            if (p.side() == side) {
                sum += p.shares();
            }
        }
        return sum;
    }

    private void updatePositionPriceObservations(OrderBookQuote quote) {
        if (openPositions.isEmpty()) {
            return;
        }
        for (int i = 0, n = openPositions.size(); i < n; i++) {
            Position pos = openPositions.get(i);
            double bid = pos.side() == TradeDirection.UP ? quote.bestBidUp() : quote.bestBidDown();
            if (bid > pos.maxObservedPrice()) {
                openPositions.set(i, pos.withPriceObservation(bid, pos.trailingStopArmed()));
            }
        }
    }

    private void closeOpenPositionsFifo(TradeDirection side, double totalSoldShares, double exitPrice) {
        double sharesToClose = totalSoldShares;
        double totalExitFee = (exitPrice > 0.0 && exitPrice < 1.0)
                ? (totalSoldShares * takerFeeRate * exitPrice * (1.0 - exitPrice))
                : 0.0;
        String exitReason = String.format(Locale.ROOT, "Early Exit (%.2f)", exitPrice);

        for (int i = 0; i < openPositions.size() && sharesToClose > EPS; ) {
            Position pos = openPositions.get(i);
            if (pos.side() != side) {
                i++;
                continue;
            }
            double closedShares = Math.min(sharesToClose, pos.shares());
            double posFraction = closedShares / pos.shares();
            double allocatedEntryFee = pos.entryFee() * posFraction;
            double allocatedExitFee = totalExitFee * (closedShares / totalSoldShares);
            double closedSizeUsd = closedShares * pos.entryPrice();
            double payout = closedShares * exitPrice;
            double netPnl = payout - closedSizeUsd - allocatedEntryFee - allocatedExitFee;

            cashBalanceUsd += (payout - allocatedExitFee);
            realizedEquityUsd += netPnl;
            boolean isWin = netPnl > 0.0;

            TradeRecord record = new TradeRecord(
                    Instant.ofEpochMilli(pos.entryTimestampMs()).toString(),
                    pos.intervalStartSec(),
                    pos.side(),
                    pos.entryPrice(),
                    exitPrice,
                    closedSizeUsd,
                    closedShares,
                    allocatedEntryFee + allocatedExitFee,
                    netPnl,
                    exitReason,
                    pos.modelProbability(),
                    pos.marketPrice(),
                    pos.edge(),
                    isWin,
                    realizedEquityUsd
            );
            completedTradesHistory.add(record);
            if (tradeLifecycleListener != null && !warmupMode) {
                tradeLifecycleListener.onTradeClosed(record);
            }

            double remainingShares = pos.shares() - closedShares;
            sharesToClose -= closedShares;
            if (remainingShares <= EPS) {
                openPositions.remove(i);
            } else {
                openPositions.set(i, pos.withRemainingShares(remainingShares, pos.entryFee() - allocatedEntryFee));
                i++;
            }
        }
    }
}
