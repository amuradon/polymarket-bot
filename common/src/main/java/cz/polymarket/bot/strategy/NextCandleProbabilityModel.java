package cz.polymarket.bot.strategy;

import cz.polymarket.bot.calculator.KellyPositionSizer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Probability model predicting direction for the Polymarket BTC 15m binary market.
 * Synthesizes mean-reversion VWAP Z-score, Futures CVD delta flows, Basis spread,
 * and late oracle arbitrage lock-in.
 */
@ApplicationScoped
public class NextCandleProbabilityModel {

    public static final double DEFAULT_MIN_PROB = 0.63;
    public static final double DEFAULT_MIN_EDGE = 0.06;
    public static final double DEFAULT_SLIPPAGE_CAP = 0.005;
    public static final double DEFAULT_SIGMOID_K = 0.70;
    public static final double DEFAULT_LATE_ARB_DIST = 80.0;
    public static final double DEFAULT_LATE_ARB_PROB = 0.94;

    private final KellyPositionSizer positionSizer;
    private final double minProb;
    private final double minEdge;
    private final double slippageCap;
    private final double sigmoidK;
    private final double lateArbDist;
    private final double lateArbProb;

    /**
     * Constructs probability model with customizable statistical threshold parameters.
     *
     * @param positionSizer quarter-Kelly position sizer
     * @param minProb minimum directional probability required to enter a trade
     * @param minEdge minimum positive edge (modelProb - marketPrice) required
     * @param slippageCap maximum allowed execution slippage (fillPrice - marketPrice)
     * @param sigmoidK logistic sigmoid slope constant
     * @param lateArbDist minimum spot price distance from TWAP open to trigger late arbitrage
     * @param lateArbProb directional probability assigned during late oracle arbitrage lock-in
     */
    @Inject
    public NextCandleProbabilityModel(
            KellyPositionSizer positionSizer,
            @ConfigProperty(name = "polymarket.strategy.min-prob", defaultValue = "0.63") double minProb,
            @ConfigProperty(name = "polymarket.strategy.min-edge", defaultValue = "0.06") double minEdge,
            @ConfigProperty(name = "polymarket.strategy.slippage-cap", defaultValue = "0.005") double slippageCap,
            @ConfigProperty(name = "polymarket.strategy.sigmoid-k", defaultValue = "0.70") double sigmoidK,
            @ConfigProperty(name = "polymarket.strategy.late-arb-dist", defaultValue = "80.0") double lateArbDist,
            @ConfigProperty(name = "polymarket.strategy.late-arb-prob", defaultValue = "0.94") double lateArbProb) {
        if (positionSizer == null) {
            throw new IllegalArgumentException("positionSizer cannot be null");
        }
        this.positionSizer = positionSizer;
        this.minProb = minProb;
        this.minEdge = minEdge;
        this.slippageCap = slippageCap;
        this.sigmoidK = sigmoidK;
        this.lateArbDist = lateArbDist;
        this.lateArbProb = lateArbProb;
    }

    /**
     * Constructs probability model using default empirical constants from Iteration 20.
     *
     * @param positionSizer quarter-Kelly position sizer
     */
    public NextCandleProbabilityModel(KellyPositionSizer positionSizer) {
        this(positionSizer, DEFAULT_MIN_PROB, DEFAULT_MIN_EDGE, DEFAULT_SLIPPAGE_CAP,
                DEFAULT_SIGMOID_K, DEFAULT_LATE_ARB_DIST, DEFAULT_LATE_ARB_PROB);
    }

    /**
     * Evaluates statistical signals and calculates directional probability, expected edge, and position sizing.
     *
     * @param zVwap rolling VWAP Z-score
     * @param spotDelta spot cumulative volume delta in BTC
     * @param futDelta futures cumulative volume delta in BTC
     * @param futH1Delta 1-hour futures cumulative volume delta in BTC
     * @param basisOpenBps basis spread at candle open in basis points
     * @param distTwap current spot price distance from TWAP open in USD
     * @param marketPriceUp best ask for UP contract
     * @param marketPriceDown best ask for DOWN contract
     * @param execPriceUp estimated fill price for UP contract
     * @param execPriceDown estimated fill price for DOWN contract
     * @param isLowVolatility true if market volatility is below minimum threshold
     * @return StrategySignal containing trade direction, probabilities, edge, and suggested size
     */
    public StrategySignal evaluate(
            double zVwap,
            double spotDelta,
            double futDelta,
            double futH1Delta,
            double basisOpenBps,
            double distTwap,
            double marketPriceUp,
            double marketPriceDown,
            double execPriceUp,
            double execPriceDown,
            boolean isLowVolatility) {

        if (isLowVolatility) {
            return StrategySignal.noTrade("Skipped by volatility regime filter");
        }

        // 1. Mean-reversion score
        double score = 0.0;
        if (zVwap > 1.2) {
            score -= 0.6;
        } else if (zVwap < -1.2) {
            score += 0.6;
        } else {
            score += 0.2 * Math.clamp(spotDelta / 5.0, -1.0, 1.0);
        }

        // 2. Futures flow
        score += 0.4 * Math.clamp(futDelta / 8.0, -1.0, 1.0);
        score += 0.3 * Math.clamp(futH1Delta / 20.0, -1.0, 1.0);

        // 3. Basis spread
        if (basisOpenBps > 1.5) {
            score += 0.35;
        } else if (basisOpenBps < -1.5) {
            score -= 0.35;
        }

        // 4. Logistic sigmoid conversion
        double clippedScore = Math.clamp(score, -3.5, 3.5);
        double pUp = 1.0 / (1.0 + Math.exp(-sigmoidK * clippedScore));
        double pDown = 1.0 - pUp;

        // 5. Late window oracle arbitrage
        if (Math.abs(distTwap) > lateArbDist) {
            if (distTwap > lateArbDist) {
                pUp = lateArbProb;
                pDown = 1.0 - lateArbProb;
            } else {
                pUp = 1.0 - lateArbProb;
                pDown = lateArbProb;
            }
        }

        // 6. Signal evaluation
        double edgeUp = pUp - marketPriceUp;
        double edgeDown = pDown - marketPriceDown;

        TradeDirection direction = TradeDirection.NO_TRADE;
        double chosenModelP = 0.50;
        double chosenMarketP = 0.50;
        double chosenExecP = 0.50;
        double chosenEdge = 0.0;

        if (pUp >= minProb && edgeUp >= minEdge) {
            direction = TradeDirection.UP;
            chosenModelP = pUp;
            chosenMarketP = marketPriceUp;
            chosenExecP = execPriceUp;
            chosenEdge = edgeUp;
        } else if (pDown >= minProb && edgeDown >= minEdge) {
            direction = TradeDirection.DOWN;
            chosenModelP = pDown;
            chosenMarketP = marketPriceDown;
            chosenExecP = execPriceDown;
            chosenEdge = edgeDown;
        }

        if (direction == TradeDirection.NO_TRADE) {
            return StrategySignal.noTrade("No statistical edge or probability under threshold");
        }

        // 7. Slippage cap verification
        double slippage = chosenExecP - chosenMarketP;
        if (slippage > slippageCap) {
            return StrategySignal.noTrade("Execution slippage (" + slippage + ") exceeded slippage cap (" + slippageCap + ")");
        }

        // 8. Quarter-Kelly position size
        double sizeUsd = positionSizer.calculateSizeUsd(chosenModelP, chosenMarketP);

        return new StrategySignal(direction, chosenModelP, chosenMarketP, chosenEdge, sizeUsd, "Signal confirmed");
    }

    /**
     * Minimum directional probability required for taking a trade.
     */
    public double getMinProb() {
        return minProb;
    }

    /**
     * Minimum positive edge required (model probability minus market price).
     */
    public double getMinEdge() {
        return minEdge;
    }

    /**
     * Maximum tolerated slippage between estimated execution fill price and book price.
     */
    public double getSlippageCap() {
        return slippageCap;
    }

    /**
     * Logistic sigmoid curve slope constant.
     */
    public double getSigmoidK() {
        return sigmoidK;
    }

    /**
     * Threshold distance in USD from TWAP open triggering late oracle arbitrage.
     */
    public double getLateArbDist() {
        return lateArbDist;
    }

    /**
     * Locked-in directional probability assigned when late oracle arbitrage condition is met.
     */
    public double getLateArbProb() {
        return lateArbProb;
    }
}
