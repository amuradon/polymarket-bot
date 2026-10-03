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

    public static TWAPArbitrageStrategyConfig defaultIteration20() {
        return new TWAPArbitrageStrategyConfig(0.70, 0.14, 0.05, 60L, 600L, 0.07, "pm-btc-15m", "BTC-UP", "BTC-DOWN");
    }

    public double targetTakeProfitPrice() {
        return targetTakeProfitPrice;
    }

    public double trailingStopActivationDelta() {
        return trailingStopActivationDelta;
    }

    public double trailingStopProfitLock() {
        return trailingStopProfitLock;
    }

    public long phase1EntrySecond() {
        return phase1EntrySecond;
    }

    public long phase2LateArbSecond() {
        return phase2LateArbSecond;
    }

    public double takerFeeRate() {
        return takerFeeRate;
    }

    public String defaultMarketId() {
        return defaultMarketId;
    }

    public String upToken() {
        return upToken;
    }

    public String downToken() {
        return downToken;
    }
}
