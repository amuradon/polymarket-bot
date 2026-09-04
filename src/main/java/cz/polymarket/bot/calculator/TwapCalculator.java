package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.TwapPoint;
import jakarta.enterprise.context.ApplicationScoped;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@ApplicationScoped
public class TwapCalculator {

    private static final int SCALE = 8;
    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

    public BigDecimal calculateRollingTwap(List<BigDecimal> windowPrices) {
        if (windowPrices == null || windowPrices.isEmpty()) {
            throw new IllegalArgumentException("Window prices cannot be null or empty");
        }

        BigDecimal sum = BigDecimal.ZERO;
        for (BigDecimal price : windowPrices) {
            if (price == null) {
                throw new IllegalArgumentException("Price in window cannot be null");
            }
            sum = sum.add(price);
        }

        return sum.divide(BigDecimal.valueOf(windowPrices.size()), SCALE, ROUNDING_MODE);
    }

    public TwapPoint createPoint(long timestampSec, BigDecimal rollingTwap, BigDecimal medianPrice) {
        if (rollingTwap == null || medianPrice == null) {
            throw new IllegalArgumentException("Rolling TWAP and median price cannot be null");
        }
        return new TwapPoint(
                timestampSec,
                rollingTwap.setScale(SCALE, ROUNDING_MODE),
                medianPrice.setScale(SCALE, ROUNDING_MODE)
        );
    }
}
