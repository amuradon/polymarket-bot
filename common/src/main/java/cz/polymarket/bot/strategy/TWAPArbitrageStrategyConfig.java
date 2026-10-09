package cz.polymarket.bot.strategy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Configuration bean for the TWAPArbitrageStrategy, adhering to Iteration 20 empirical parameters.
 * CDI-managed with configurable MicroProfile properties and low-latency defaults.
 */
@ApplicationScoped
public class TWAPArbitrageStrategyConfig {

    private final double targetTakeProfitPrice;
    private final double trailingStopActivationDelta;
    private final double trailingStopProfitLock;
    private final long phase1EntrySecond;
    private final long phase2LateArbSecond;
    private final double takerFeeRate;
    private final String defaultMarketId;
    private final String upToken;
    private final String downToken;

    /**
     * Constructs strategy configuration with configurable threshold parameters.
     *
     * @param targetTakeProfitPrice target contract price for immediate take-profit exit (e.g. 0.70)
     * @param trailingStopActivationDelta profit delta above entry price required to arm trailing stop (e.g. 0.14)
     * @param trailingStopProfitLock profit delta above entry price guaranteed once armed (e.g. 0.05)
     * @param phase1EntrySecond second offset into 15m candle for Phase 1 statistical entry (e.g. 60)
     * @param phase2LateArbSecond second offset into 15m candle for Phase 2 late oracle arbitrage (e.g. 600)
     * @param takerFeeRate Polymarket binary taker fee multiplier (e.g. 0.07)
     * @param defaultMarketId default market identifier
     * @param upToken identifier for UP (YES) contract
     * @param downToken identifier for DOWN (NO) contract
     */
    @Inject
    public TWAPArbitrageStrategyConfig(
            @ConfigProperty(name = "polymarket.strategy.tp-price", defaultValue = "0.70") double targetTakeProfitPrice,
            @ConfigProperty(name = "polymarket.strategy.trailing-activation", defaultValue = "0.14") double trailingStopActivationDelta,
            @ConfigProperty(name = "polymarket.strategy.trailing-lock", defaultValue = "0.05") double trailingStopProfitLock,
            @ConfigProperty(name = "polymarket.strategy.phase1-second", defaultValue = "60") long phase1EntrySecond,
            @ConfigProperty(name = "polymarket.strategy.phase2-second", defaultValue = "600") long phase2LateArbSecond,
            @ConfigProperty(name = "polymarket.strategy.taker-fee-rate", defaultValue = "0.07") double takerFeeRate,
            @ConfigProperty(name = "polymarket.strategy.default-market-id", defaultValue = "pm-btc-15m") String defaultMarketId,
            @ConfigProperty(name = "polymarket.strategy.up-token", defaultValue = "BTC-UP") String upToken,
            @ConfigProperty(name = "polymarket.strategy.down-token", defaultValue = "BTC-DOWN") String downToken) {
        if (targetTakeProfitPrice <= 0.0 || targetTakeProfitPrice >= 1.0) {
            throw new IllegalArgumentException("targetTakeProfitPrice must be between 0.0 and 1.0");
        }
        if (trailingStopActivationDelta <= 0.0) {
            throw new IllegalArgumentException("trailingStopActivationDelta must be positive");
        }
        this.targetTakeProfitPrice = targetTakeProfitPrice;
        this.trailingStopActivationDelta = trailingStopActivationDelta;
        this.trailingStopProfitLock = trailingStopProfitLock;
        this.phase1EntrySecond = phase1EntrySecond;
        this.phase2LateArbSecond = phase2LateArbSecond;
        this.takerFeeRate = takerFeeRate;
        this.defaultMarketId = defaultMarketId;
        this.upToken = upToken;
        this.downToken = downToken;
    }

    /**
     * Creates a default configuration instance adhering to empirical parameters of Iteration 20.
     *
     * @return default Iteration 20 strategy configuration
     */
    public static TWAPArbitrageStrategyConfig defaultIteration20() {
        return new TWAPArbitrageStrategyConfig(0.70, 0.14, 0.05, 60L, 600L, 0.07, "pm-btc-15m", "BTC-UP", "BTC-DOWN");
    }

    /**
     * Target contract price for immediate take-profit order exit.
     */
    public double targetTakeProfitPrice() {
        return targetTakeProfitPrice;
    }

    /**
     * Minimum price increase required above entry price to activate the trailing stop mechanism.
     */
    public double trailingStopActivationDelta() {
        return trailingStopActivationDelta;
    }

    /**
     * Locked profit delta above entry price preserved if the trailing stop triggers.
     */
    public double trailingStopProfitLock() {
        return trailingStopProfitLock;
    }

    /**
     * Second offset into the 15-minute candle at which Phase 1 entry criteria are evaluated.
     */
    public long phase1EntrySecond() {
        return phase1EntrySecond;
    }

    /**
     * Second offset into the 15-minute candle at which Phase 2 late oracle arbitrage is evaluated.
     */
    public long phase2LateArbSecond() {
        return phase2LateArbSecond;
    }

    /**
     * Polymarket taker fee rate factor (0.07).
     */
    public double takerFeeRate() {
        return takerFeeRate;
    }

    /**
     * Default market identifier for order commands.
     */
    public String defaultMarketId() {
        return defaultMarketId;
    }

    /**
     * Symbol / token identifier for UP contract.
     */
    public String upToken() {
        return upToken;
    }

    /**
     * Symbol / token identifier for DOWN contract.
     */
    public String downToken() {
        return downToken;
    }
}
