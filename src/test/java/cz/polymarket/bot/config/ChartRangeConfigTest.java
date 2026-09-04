package cz.polymarket.bot.config;

import cz.polymarket.bot.domain.Timeframe;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChartRangeConfigTest {

    @Test
    void shouldReturnConfiguredRangeForSymbolAndTimeframe() {
        ChartRangeConfig config = new ChartRangeConfig(new BigDecimal("30"), new BigDecimal("50"));

        assertThat(config.getYRange("btc-usd", Timeframe.FIVE_MINUTES)).isEqualByComparingTo("30");
        assertThat(config.getYRange("btc-usd", Timeframe.FIFTEEN_MINUTES)).isEqualByComparingTo("50");
        assertThat(config.getYRange("BTC-USD", Timeframe.FIVE_MINUTES)).isEqualByComparingTo("30");
    }

    @Test
    void shouldThrowWhenSymbolOrTimeframeIsNull() {
        ChartRangeConfig config = new ChartRangeConfig(new BigDecimal("30"), new BigDecimal("50"));

        assertThatThrownBy(() -> config.getYRange(null, Timeframe.FIVE_MINUTES))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> config.getYRange("btc-usd", null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
