package cz.polymarket.bot.strategy;

import cz.polymarket.bot.calculator.RealizedVolatilityCalculator;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapUpdate;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TWAPArbitrageStrategy implements Iteration 20 (Dublin/London 10ms Stat-Arb Engine).
 * Fully adheres to the TradingStrategy interface and executes identically across
 * Backtesting, Paper Trading, and Live Trading without discrepancy.
 * Technical indicators are calculated and provided dynamically by the engine.
 */
@ApplicationScoped
public class TWAPArbitrageStrategy implements TradingStrategy {

    private final NextCandleProbabilityModel probabilityModel;
    private final TWAPArbitrageStrategyConfig config;

    private StrategyContext context;

    // Active candle tracking
    private final AtomicLong activeCandleStart = new AtomicLong(0);
    private final AtomicLong activeCandleEnd = new AtomicLong(0);
    private volatile double twapOpenPrice = 0.0;
    private volatile double currentSpotPrice = 0.0;
    private volatile long currentTimestampSec = 0;
    private volatile boolean phase1Evaluated = false;
    private volatile boolean phase2Evaluated = false;

    // Dynamic indicators delivered by Engine
    private volatile double currentVwapZScore = 0.0;
    private volatile double currentVol4h = 0.001;
    private volatile double currentSpotCvd = 0.0;
    private volatile double currentFutCvd = 0.0;
    private volatile double currentFutH1Cvd = 0.0;
    private volatile double currentBasisBps = 0.0;
    private volatile double currentTwap = 0.0;

    // Position state
    private volatile boolean hasPosition = false;
    private volatile TradeDirection positionSide = TradeDirection.NO_TRADE;
    private volatile double entryPrice = 0.0;
    private volatile double sizeUsd = 0.0;
    private volatile double shares = 0.0;
    private volatile double maxObservedPrice = 0.0;
    private volatile boolean trailingStopArmed = false;
    private volatile boolean positionClosed = false;
    private volatile String activeClientOrderId = null;
    private volatile boolean isLateArbPosition = false;

    /**
     * Declares the set of technical indicators required by this strategy.
     * The engine ensures these are calculated and updated dynamically.
     *
     * @return the set of required indicator types
     */
    @Override
    public Set<IndicatorType> getRequiredIndicators() {
        return Set.of(
                IndicatorType.TWAP,
                IndicatorType.BASIS,
                IndicatorType.VWAP_ZSCORE,
                IndicatorType.CVD,
                IndicatorType.VOLATILITY_4H
        );
    }

    /**
     * Explicitly sets the current basis spread in basis points (used in testing or calibration).
     *
     * @param basisBps basis spread in basis points
     */
    public void setCurrentBasisBps(double basisBps) {
        this.currentBasisBps = basisBps;
    }

    /**
     * Resets the internal state of the strategy between candles or test runs.
     */
    @Override
    public void reset() {
        activeCandleStart.set(0);
        activeCandleEnd.set(0);
        twapOpenPrice = 0.0;
        currentSpotPrice = 0.0;
        currentTimestampSec = 0;
        currentBasisBps = 0.0;
        currentVwapZScore = 0.0;
        currentVol4h = 0.001;
        currentSpotCvd = 0.0;
        currentFutCvd = 0.0;
        currentFutH1Cvd = 0.0;
        currentTwap = 0.0;
        isLateArbPosition = false;
        phase1Evaluated = false;
        phase2Evaluated = false;
        hasPosition = false;
        positionSide = TradeDirection.NO_TRADE;
        entryPrice = 0.0;
        sizeUsd = 0.0;
        shares = 0.0;
        maxObservedPrice = 0.0;
        trailingStopArmed = false;
        positionClosed = false;
        activeClientOrderId = null;
    }

    /**
     * Constructs the TWAPArbitrageStrategy with injected statistical probability model and configuration.
     *
     * @param probabilityModel predictive model computing directional probability and edge
     * @param config empirical strategy threshold and parameter configuration bean
     */
    @Inject
    public TWAPArbitrageStrategy(
            NextCandleProbabilityModel probabilityModel,
            TWAPArbitrageStrategyConfig config) {
        if (probabilityModel == null) {
            throw new IllegalArgumentException("probabilityModel cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.probabilityModel = probabilityModel;
        this.config = config;
    }

    /**
     * Initializes the strategy with the execution context (routing and price caches).
     *
     * @param context the strategy execution context
     */
    @Override
    public void init(StrategyContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        this.context = context;
    }

    /**
     * Returns the primary market timeframe evaluated by this strategy (15 minutes).
     *
     * @return 15-minute timeframe
     */
    @Override
    public Timeframe getTimeframe() {
        return Timeframe.FIFTEEN_MINUTES;
    }

    /**
     * Handles incoming aggregate TWAP reference price updates, advancing active candle state.
     *
     * @param update latest aggregate TWAP sample update
     */
    @Override
    public void onTwapUpdate(TwapUpdate update) {
        if (update == null) {
            return;
        }

        // Only process updates corresponding to the strategy's target 15m timeframe
        if (update.timeframe() != null && update.timeframe() != Timeframe.FIFTEEN_MINUTES) {
            return;
        }

        long start = update.candleStart();
        if (start != activeCandleStart.get()) {
            activeCandleStart.set(start);
            activeCandleEnd.set(update.candleEnd());
            twapOpenPrice = update.openPrice().doubleValue();
            phase1Evaluated = false;
            phase2Evaluated = false;
            hasPosition = false;
            positionSide = TradeDirection.NO_TRADE;
            entryPrice = 0.0;
            sizeUsd = 0.0;
            shares = 0.0;
            maxObservedPrice = 0.0;
            trailingStopArmed = false;
            positionClosed = false;
            activeClientOrderId = null;
            isLateArbPosition = false;
        }

        if (update.openPrice() != null) {
            twapOpenPrice = update.openPrice().doubleValue();
        }

        if (update.point() != null) {
            currentTimestampSec = update.point().time();
            if (update.point().medianPrice() != null) {
                currentSpotPrice = update.point().medianPrice().doubleValue();
            }
        }
    }

    /**
     * Receives dynamically calculated indicator updates from the engine on-the-fly.
     *
     * @param type technical indicator type
     * @param value newly computed indicator value
     * @param timestampMs event timestamp in milliseconds UTC
     */
    @Override
    public void onIndicatorUpdate(IndicatorType type, double value, long timestampMs) {
        if (type == null) {
            return;
        }
        switch (type) {
            case VWAP_ZSCORE, VWAP -> this.currentVwapZScore = value;
            case VOLATILITY_4H -> this.currentVol4h = value;
            case CVD -> {
                this.currentSpotCvd = value;
                this.currentFutCvd = value;
                this.currentFutH1Cvd = value;
            }
            case BASIS -> this.currentBasisBps = value;
            case TWAP -> this.currentTwap = value;
            default -> {}
        }
    }

    /**
     * Handles order execution feedback from matching engine or exchange (fills, rejections).
     *
     * @param report execution report containing order status and executed price/size
     */
    @Override
    public void onExecutionReport(ExecutionReport report) {
        if (report == null || report.clientOrderId() == null) {
            return;
        }

        if (report.clientOrderId().equals(activeClientOrderId)) {
            if ("FILLED".equalsIgnoreCase(report.status())) {
                this.hasPosition = true;
                if (report.executedPrice() != null) {
                    this.entryPrice = report.executedPrice().doubleValue();
                }
                if (report.executedSize() != null) {
                    this.shares = report.executedSize().doubleValue();
                }
                this.sizeUsd = this.entryPrice * this.shares;
                this.maxObservedPrice = this.entryPrice;
            } else if ("REJECTED".equalsIgnoreCase(report.status()) || "CANCELLED".equalsIgnoreCase(report.status())) {
                this.hasPosition = false;
                this.positionSide = TradeDirection.NO_TRADE;
            }
        }
    }

    /**
     * Updates current spot asset price and clock upon receiving a Binance Spot aggTrade.
     *
     * @param timestampMs transaction timestamp in milliseconds UTC
     * @param price execution price
     * @param quantity trade volume
     * @param isBuyerMaker true if maker was buyer (aggressive sell), false if aggressive buy
     */
    @Override
    public void onBinanceSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        if (price > 0.0) {
            this.currentSpotPrice = price;
        }
        if (timestampMs > 0) {
            this.currentTimestampSec = timestampMs / 1000L;
        }
    }

    /**
     * Updates basis spread upon receiving a Binance Futures aggTrade.
     *
     * @param timestampMs transaction timestamp in milliseconds UTC
     * @param price futures execution price
     * @param quantity trade volume
     * @param isBuyerMaker true if maker was buyer, false if aggressive buy
     */
    @Override
    public void onBinanceFuturesTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        if (this.currentSpotPrice > 0.0) {
            this.currentBasisBps = ((price - this.currentSpotPrice) / this.currentSpotPrice) * 10000.0;
        }
    }

    /**
     * Handles candle completion notification for historical metrics tracking.
     *
     * @param candle completed market candle interval
     */
    @Override
    public void onMarketCandleCompleted(MarketCandle candle) {
        // Strategy relies on engine to compute historical indicator metrics
    }

    /**
     * Evaluates entry or exit decisions upon receiving an updated Polymarket order book quote.
     *
     * @param quote current order book quote with best bids, asks, and depth
     */
    @Override
    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null || context == null) {
            return;
        }

        if (quote.timestampMs() > 0) {
            long quoteSec = quote.timestampMs() / 1000L;
            if (activeCandleStart.get() <= 0 || quoteSec >= activeCandleStart.get()) {
                this.currentTimestampSec = quoteSec;
            }
        }

        long secondInCandle = activeCandleStart.get() > 0 ? (currentTimestampSec - activeCandleStart.get()) : 0;

        // 1. If we hold an active position, evaluate exit criteria
        if (hasPosition && !positionClosed) {
            evaluatePositionExit(quote);
            return;
        }

        // 2. If no position, evaluate entry phases
        if (!hasPosition && !positionClosed) {
            if (!phase1Evaluated && secondInCandle >= config.phase1EntrySecond() && secondInCandle < config.phase2LateArbSecond()) {
                evaluatePhase1Entry(quote);
            } else if (!phase2Evaluated && secondInCandle >= config.phase2LateArbSecond()) {
                evaluatePhase2LateArb(quote);
            }
        }
    }

    /**
     * Handles binary contract outcome resolution at candle close (payout 1.0 or 0.0).
     *
     * @param actualOutcome settled binary outcome direction (UP or DOWN)
     */
    @Override
    public void onCandleResolution(TradeDirection actualOutcome) {
        if (!hasPosition || positionClosed || context == null) {
            return;
        }

        if (trailingStopArmed && actualOutcome != positionSide) {
            double exitPrice = entryPrice + config.trailingStopProfitLock();
            submitExitOrder(exitPrice, "Trailing Stop (+0.05)");
        } else {
            double exitPrice = (actualOutcome == positionSide) ? 1.00 : 0.00;
            positionClosed = true;
        }
    }

    /**
     * Evaluates statistical entry criteria during Phase 1 (typically at t = 60s).
     * Combines mean-reversion VWAP z-score, CVD delta flows, and basis spread.
     *
     * @param quote current order book quote
     */
    private void evaluatePhase1Entry(OrderBookQuote quote) {
        phase1Evaluated = true;

        boolean isLowVol = (currentVol4h > 0.0 && currentVol4h < RealizedVolatilityCalculator.MIN_VOLATILITY_THRESHOLD);
        double distTwap = (twapOpenPrice > 0.0) ? (currentSpotPrice - twapOpenPrice) : 0.0;

        StrategySignal signal = probabilityModel.evaluate(
                currentVwapZScore,
                currentSpotCvd,
                currentFutCvd,
                currentFutH1Cvd,
                currentBasisBps,
                distTwap,
                quote.bestAskUp(),
                quote.bestAskDown(),
                quote.estimatedFillPriceUp(),
                quote.estimatedFillPriceDown(),
                isLowVol
        );

        if (signal.isTrade()) {
            this.isLateArbPosition = false;
            submitEntryOrder(signal);
        }
    }

    /**
     * Evaluates late oracle lock-in arbitrage during Phase 2 (typically at t = 600s / 10m).
     * Locks in high-probability outcome when spot distance from TWAP open exceeds threshold.
     *
     * @param quote current order book quote
     */
    private void evaluatePhase2LateArb(OrderBookQuote quote) {
        phase2Evaluated = true;

        double distTwap = currentSpotPrice - twapOpenPrice;

        StrategySignal signal = probabilityModel.evaluate(
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                distTwap,
                quote.bestAskUp(),
                quote.bestAskDown(),
                quote.estimatedFillPriceUp(),
                quote.estimatedFillPriceDown(),
                false
        );

        if (signal.isTrade()) {
            this.isLateArbPosition = true;
            submitEntryOrder(signal);
        }
    }

    /**
     * Evaluates exit conditions for an open position: Take-Profit (0.70) or Trailing Stop (+0.05).
     *
     * @param quote current order book quote
     */
    private void evaluatePositionExit(OrderBookQuote quote) {
        double currentContractPrice = (positionSide == TradeDirection.UP) ? quote.bestBidUp() : quote.bestBidDown();

        if (currentContractPrice > maxObservedPrice) {
            maxObservedPrice = currentContractPrice;
        }

        // Arm trailing stop if price exceeded profit lock threshold
        if (maxObservedPrice >= entryPrice + config.trailingStopActivationDelta()) {
            trailingStopArmed = true;
        }

        // 1. Take Profit Exit
        if (currentContractPrice >= config.targetTakeProfitPrice()) {
            submitExitOrder(config.targetTakeProfitPrice(), "Take Profit (" + config.targetTakeProfitPrice() + ")");
            return;
        }

        // 2. Trailing Stop Exit
        if (trailingStopArmed && currentContractPrice <= entryPrice + config.trailingStopProfitLock()) {
            submitExitOrder(currentContractPrice, "Trailing Stop (+" + config.trailingStopProfitLock() + ")");
        }
    }

    /**
     * Submits a market/taker buy order to enter a position based on strategy signal.
     *
     * @param signal directional trade signal with suggested size and market price
     */
    private void submitEntryOrder(StrategySignal signal) {
        String clientOrderId = "twap-entry-" + UUID.randomUUID();
        this.activeClientOrderId = clientOrderId;
        this.positionSide = signal.direction();
        this.entryPrice = signal.marketPrice();
        this.sizeUsd = signal.suggestedSizeUsd();
        this.shares = (this.entryPrice > 0.0) ? (this.sizeUsd / this.entryPrice) : 0.0;

        String token = (signal.direction() == TradeDirection.UP) ? config.upToken() : config.downToken();
        BigDecimal price = BigDecimal.valueOf(signal.marketPrice()).setScale(4, RoundingMode.HALF_UP);
        BigDecimal size = BigDecimal.valueOf(this.shares).setScale(4, RoundingMode.HALF_UP);

        OrderCommand command = new OrderCommand(
                clientOrderId,
                config.defaultMarketId(),
                token,
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                price,
                size
        );

        context.getExecutionRouter().submitOrder(command);
    }

    /**
     * Submits a sell order to close an existing open position before candle resolution.
     *
     * @param exitPrice target limit price for the exit order
     * @param reason human-readable justification (e.g. Take Profit or Trailing Stop)
     */
    private void submitExitOrder(double exitPrice, String reason) {
        positionClosed = true;
        String clientOrderId = "twap-exit-" + UUID.randomUUID();
        this.activeClientOrderId = clientOrderId;

        String token = (positionSide == TradeDirection.UP) ? config.upToken() : config.downToken();
        BigDecimal price = BigDecimal.valueOf(exitPrice).setScale(4, RoundingMode.HALF_UP);
        BigDecimal size = BigDecimal.valueOf(shares).setScale(4, RoundingMode.HALF_UP);

        OrderCommand command = new OrderCommand(
                clientOrderId,
                config.defaultMarketId(),
                token,
                Timeframe.FIFTEEN_MINUTES,
                "SELL",
                price,
                size
        );

        context.getExecutionRouter().submitOrder(command);
    }

    /**
     * Calculates Polymarket crypto binary option taker fee based on fee curve formula:
     * Fee = shares * feeRate * price * (1.0 - price).
     *
     * @param shares trade volume in shares
     * @param price execution contract price between 0.0 and 1.0
     * @return calculated fee in USD
     */
    public double calculateTakerFee(double shares, double price) {
        return shares * config.takerFeeRate() * price * (1.0 - price);
    }

    /**
     * Checks whether the strategy currently holds an active open contract position.
     *
     * @return true if holding an open position
     */
    public boolean hasPosition() {
        return hasPosition;
    }

    /**
     * Returns the side of the currently held position (UP, DOWN, or NO_TRADE).
     *
     * @return active position direction
     */
    public TradeDirection getPositionSide() {
        return positionSide;
    }

    /**
     * Returns the execution entry price of the active position.
     *
     * @return entry price
     */
    public double getEntryPrice() {
        return entryPrice;
    }

    /**
     * Returns the total position capital invested in USD.
     *
     * @return position size in USD
     */
    public double getSizeUsd() {
        return sizeUsd;
    }

    /**
     * Returns the quantity of contract shares purchased.
     *
     * @return share volume
     */
    public double getShares() {
        return shares;
    }

    /**
     * Checks whether trailing stop profit-protection has been armed.
     *
     * @return true if trailing stop is armed
     */
    public boolean isTrailingStopArmed() {
        return trailingStopArmed;
    }

    /**
     * Checks whether the active position has been closed prior to resolution.
     *
     * @return true if position is closed
     */
    public boolean isPositionClosed() {
        return positionClosed;
    }

    /**
     * Returns the starting epoch second of the current 15-minute candle.
     *
     * @return start timestamp in seconds UTC
     */
    public long getActiveCandleStart() {
        return activeCandleStart.get();
    }

    /**
     * Returns the ending epoch second of the current 15-minute candle.
     *
     * @return end timestamp in seconds UTC
     */
    public long getActiveCandleEnd() {
        return activeCandleEnd.get();
    }

    /**
     * Returns the opening reference spot price used for TWAP comparison.
     *
     * @return TWAP open price
     */
    public double getTwapOpenPrice() {
        return twapOpenPrice;
    }

    /**
     * Returns the current observed Binance spot price.
     *
     * @return current spot price
     */
    public double getCurrentSpotPrice() {
        return currentSpotPrice;
    }

    /**
     * Returns the current basis spread in basis points.
     *
     * @return basis in bps
     */
    public double getCurrentBasisBps() {
        return currentBasisBps;
    }

    /**
     * Returns the strategy configuration bean.
     *
     * @return strategy config
     */
    public TWAPArbitrageStrategyConfig getConfig() {
        return config;
    }
}
