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

    public void setCurrentBasisBps(double basisBps) {
        this.currentBasisBps = basisBps;
    }

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

    @Override
    public void init(StrategyContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        this.context = context;
    }

    @Override
    public Timeframe getTimeframe() {
        return Timeframe.FIFTEEN_MINUTES;
    }

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

    @Override
    public void onBinanceSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        this.currentSpotPrice = price;
        this.currentTimestampSec = timestampMs / 1000L;
    }

    @Override
    public void onBinanceFuturesTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        if (this.currentSpotPrice > 0.0) {
            this.currentBasisBps = ((price - this.currentSpotPrice) / this.currentSpotPrice) * 10000.0;
        }
    }

    @Override
    public void onMarketCandleCompleted(MarketCandle candle) {
        // Strategy relies on engine to compute historical indicator metrics
    }

    @Override
    public void onBinanceSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        if (price > 0.0) {
            this.currentSpotPrice = price;
        }
        if (timestampMs > 0) {
            this.currentTimestampSec = timestampMs / 1000L;
        }
    }

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

    public double calculateTakerFee(double shares, double price) {
        return shares * config.takerFeeRate() * price * (1.0 - price);
    }

    // Getters for strategy introspection & unit testing
    public boolean hasPosition() {
        return hasPosition;
    }

    public TradeDirection getPositionSide() {
        return positionSide;
    }

    public double getEntryPrice() {
        return entryPrice;
    }

    public double getSizeUsd() {
        return sizeUsd;
    }

    public double getShares() {
        return shares;
    }

    public boolean isTrailingStopArmed() {
        return trailingStopArmed;
    }

    public boolean isPositionClosed() {
        return positionClosed;
    }

    public long getActiveCandleStart() {
        return activeCandleStart.get();
    }

    public long getActiveCandleEnd() {
        return activeCandleEnd.get();
    }

    public double getTwapOpenPrice() {
        return twapOpenPrice;
    }

    public double getCurrentSpotPrice() {
        return currentSpotPrice;
    }

    public double getCurrentBasisBps() {
        return currentBasisBps;
    }

    public TWAPArbitrageStrategyConfig getConfig() {
        return config;
    }
}
