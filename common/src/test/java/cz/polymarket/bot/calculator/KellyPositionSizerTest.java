package cz.polymarket.bot.calculator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class KellyPositionSizerTest {

    private final KellyPositionSizer sizer = new KellyPositionSizer(10000.0, 0.25, 50.0, 300.0);

    @Test
    @DisplayName("Should calculate Quarter-Kelly size capped at maximum 300 USD on high conviction")
    void shouldCapAtMaximumSizeOnHighConviction() {
        // Model P = 0.94, Market P = 0.50
        double size = sizer.calculateSizeUsd(0.94, 0.50);
        assertThat(size).isEqualTo(300.0);
    }

    @Test
    @DisplayName("Should clip size to minimum 50 USD when calculated size is below 50 USD")
    void shouldClipToMinimumSize() {
        // Model P = 0.505, Market P = 0.50 => b = 1.0 => kelly_f = 0.01
        // raw size = 10000 * 0.01 * 0.25 = 25.0 USD => clip to 50.0
        double size = sizer.calculateSizeUsd(0.505, 0.50);
        assertThat(size).isEqualTo(50.0);
    }

    @Test
    @DisplayName("Should calculate intermediate size between 50 and 300 USD")
    void shouldCalculateIntermediateSize() {
        // Model P = 0.53, Market P = 0.50 => b = 1.0 => kelly_f = (0.53 - 0.47)/1.0 = 0.06
        // size = 10000 * (0.06 * 0.25) = 150.0 USD
        double size = sizer.calculateSizeUsd(0.53, 0.50);
        assertThat(size).isCloseTo(150.0, within(1e-6));
    }

    @Test
    @DisplayName("Should handle extreme market prices safely")
    void shouldHandleExtremeMarketPrices() {
        double sizeLow = sizer.calculateSizeUsd(0.70, 0.01);
        assertThat(sizeLow).isBetween(50.0, 300.0);

        double sizeHigh = sizer.calculateSizeUsd(0.99, 0.95);
        assertThat(sizeHigh).isBetween(50.0, 300.0);
    }
}
