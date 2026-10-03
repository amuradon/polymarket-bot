package cz.polymarket.bot.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Calculates position sizing based on Quarter-Kelly criterion with bounding protection.
 * Implements strict risk management preventing excessive risk per market.
 */
@ApplicationScoped
public class KellyPositionSizer {

    private final double capitalUsd;
    private final double kellyFraction;
    private final double minSizeUsd;
    private final double maxSizeUsd;

    @Inject
    public KellyPositionSizer(
            @ConfigProperty(name = "polymarket.strategy.capital-usd", defaultValue = "10000.0") double capitalUsd,
            @ConfigProperty(name = "polymarket.strategy.kelly-fraction", defaultValue = "0.25") double kellyFraction,
            @ConfigProperty(name = "polymarket.strategy.min-size-usd", defaultValue = "50.0") double minSizeUsd,
            @ConfigProperty(name = "polymarket.strategy.max-size-usd", defaultValue = "300.0") double maxSizeUsd) {
        if (capitalUsd <= 0.0) {
            throw new IllegalArgumentException("Capital must be positive");
        }
        if (kellyFraction <= 0.0 || kellyFraction > 1.0) {
            throw new IllegalArgumentException("Kelly fraction must be in range (0, 1]");
        }
        if (minSizeUsd <= 0.0 || maxSizeUsd < minSizeUsd) {
            throw new IllegalArgumentException("Invalid size bounds: min=" + minSizeUsd + ", max=" + maxSizeUsd);
        }
        this.capitalUsd = capitalUsd;
        this.kellyFraction = kellyFraction;
        this.minSizeUsd = minSizeUsd;
        this.maxSizeUsd = maxSizeUsd;
    }

    public double calculateSizeUsd(double modelProb, double marketPrice) {
        double safeMarketP = Math.max(marketPrice, 0.05);
        double bOdds = (1.0 - safeMarketP) / safeMarketP;
        double safeBOdds = Math.max(bOdds, 0.1);

        double qLoss = 1.0 - modelProb;
        double kellyF = Math.max((modelProb * safeBOdds - qLoss) / safeBOdds, 0.0);

        double rawSize = capitalUsd * (kellyF * kellyFraction);
        return Math.clamp(rawSize, minSizeUsd, maxSizeUsd);
    }

    public double getCapitalUsd() {
        return capitalUsd;
    }

    public double getKellyFraction() {
        return kellyFraction;
    }

    public double getMinSizeUsd() {
        return minSizeUsd;
    }

    public double getMaxSizeUsd() {
        return maxSizeUsd;
    }
}
