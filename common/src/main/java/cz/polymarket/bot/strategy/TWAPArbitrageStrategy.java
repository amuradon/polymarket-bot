package cz.polymarket.bot.strategy;

import cz.polymarket.bot.calculator.RealizedVolatilityCalculator;
import cz.polymarket.bot.calculator.VwapCalculator;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapUpdate;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TWAPArbitrageStrategy implements Iteration 20 (Dublin/London 10ms Stat-Arb Engine).
 * Fully adheres to the TradingStrategy interface and executes identically across
 * Backtesting, Paper Trading, and Live Trading without discrepancy.
 */
@ApplicationScoped
public class TWAPArbitrageStrategy implements TradingStrategy {

    private final VwapCalculator vwapCalculator;
    private final RealizedVolatilityCalculator volCalculator;
    private final NextCandleProbabilityModel probabilityModel;
    private final TWAPArbitrageStrategyConfig config;

    private StrategyContext context;
    private final List<MarketCandle> candleHistory = new CopyOnWriteArrayList<>();

    // Active candle tracking
    private final AtomicLong activeCandleStart = new AtomicLong(0);
    private final AtomicLong activeCandleEnd = new AtomicLong(0);
    private volatile double twapOpenPrice = 0.0;
    private volatile double currentSpotPrice = 0.0;
    private volatile long currentTimestampSec = 0;
    private volatile boolean phase1Evaluated = false;
    private volatile boolean phase2Evaluated = false;

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

    @Inject
    public TWAPArbitrageStrategy(
            VwapCalculator vwapCalculator,
            RealizedVolatilityCalculator volCalculator,
            NextCandleProbabilityModel probabilityModel,
            TWAPArbitrageStrategyConfig config) {
        if (vwapCalculator == null) {
            throw new IllegalArgumentException("vwapCalculator cannot be null");
        }
        if (volCalculator == null) {
            throw new IllegalArgumentException("volCalculator cannot be null");
        }
        if (probabilityModel == null) {
            throw new IllegalArgumentException("probabilityModel cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.vwapCalculator = vwapCalculator;
        this.volCalculator = volCalculator;
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
    public void onTwapUpdate(TwapUpdate update) {
        if (update == null) {
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

    public void onMarketCandleCompleted(MarketCandle candle) {
        if (candle != null) {
            candleHistory.add(candle);
        }
    }

    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null || context == null) {
            return;
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

        List<MarketCandle> history = Collections.unmodifiableList(new ArrayList<>(candleHistory));
        VwapCalculator.VwapResult vwapRes = vwapCalculator.calculate(history, currentSpotPrice);
        double vol4h = volCalculator.calculate4hRealizedVolatility(history);
        boolean isLowVol = volCalculator.isVolatilityTooLow(vol4h);

        double spotDelta = 0.0;
        double futDelta = 0.0;
        double futH1Delta = 0.0;
        double basisBps = 0.0;

        if (!history.isEmpty()) {
            MarketCandle last = history.get(history.size() - 1);
            spotDelta = last.spotDeltaBtc();
            futDelta = last.futuresDeltaBtc();
            basisBps = last.basisOpenBps();

            int h1Start = Math.max(0, history.size() - 4);
            for (int i = h1Start; i < history.size(); i++) {
                futH1Delta += history.get(i).futuresDeltaBtc();
            }
        }

        StrategySignal signal = probabilityModel.evaluate(
                vwapRes.zScore(),
                spotDelta,
                futDelta,
                futH1Delta,
                basisBps,
                0.0, // distTwap = 0 at phase 1
                quote.bestAskUp(),
                quote.bestAskDown(),
                quote.estimatedFillPriceUp(),
                quote.estimatedFillPriceDown(),
                isLowVol
        );

        if (signal.isTrade()) {
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
            submitEntryOrder(signal);
        }
    }

    private void evaluatePositionExit(OrderBookQuote quote) {
        double currentPrice = (positionSide == TradeDirection.UP) ? quote.bestBidUp() : quote.bestBidDown();
        if (currentPrice > maxObservedPrice) {
            maxObservedPrice = currentPrice;
        }

        // Dynamic Take Profit (0.70 USD)
        if (maxObservedPrice >= config.targetTakeProfitPrice()) {
            submitExitOrder(config.targetTakeProfitPrice(), "Take Profit (0.70)");
            return;
        }

        // Trailing Stop arming check
        if (maxObservedPrice >= (entryPrice + config.trailingStopActivationDelta())) {
            trailingStopArmed = true;
        }
    }

    private void submitEntryOrder(StrategySignal signal) {
        this.positionSide = signal.direction();
        this.entryPrice = signal.marketPrice();
        this.sizeUsd = signal.suggestedSizeUsd();
        this.shares = this.sizeUsd / this.entryPrice;
        this.activeClientOrderId = UUID.randomUUID().toString();

        String token = (signal.direction() == TradeDirection.UP) ? config.upToken() : config.downToken();
        BigDecimal priceBd = BigDecimal.valueOf(signal.marketPrice()).setScale(4, RoundingMode.HALF_UP);
        BigDecimal sizeBd = BigDecimal.valueOf(this.shares).setScale(4, RoundingMode.HALF_UP);

        OrderCommand command = new OrderCommand(
                activeClientOrderId,
                config.defaultMarketId(),
                token,
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                priceBd,
                sizeBd
        );

        context.getExecutionRouter().submitOrder(command);
    }

    private void submitExitOrder(double exitPrice, String reason) {
        this.positionClosed = true;
        String exitOrderId = UUID.randomUUID().toString();
        String token = (positionSide == TradeDirection.UP) ? config.upToken() : config.downToken();
        BigDecimal priceBd = BigDecimal.valueOf(exitPrice).setScale(4, RoundingMode.HALF_UP);
        BigDecimal sizeBd = BigDecimal.valueOf(this.shares).setScale(4, RoundingMode.HALF_UP);

        OrderCommand command = new OrderCommand(
                exitOrderId,
                config.defaultMarketId(),
                token,
                Timeframe.FIFTEEN_MINUTES,
                "SELL",
                priceBd,
                sizeBd
        );

        context.getExecutionRouter().submitOrder(command);
    }

    public double calculateTakerFee(double shares, double price) {
        return shares * config.takerFeeRate() * price * (1.0 - price);
    }

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

    public boolean isPositionClosed() {
        return positionClosed;
    }

    public boolean isTrailingStopArmed() {
        return trailingStopArmed;
    }

    public long getActiveCandleStart() {
        return activeCandleStart.get();
    }

    public long getActiveCandleEnd() {
        return activeCandleEnd.get();
    }

    public double getCurrentSpotPrice() {
        return currentSpotPrice;
    }

    public double getTwapOpenPrice() {
        return twapOpenPrice;
    }

    public TWAPArbitrageStrategyConfig getConfig() {
        return config;
    }
}
