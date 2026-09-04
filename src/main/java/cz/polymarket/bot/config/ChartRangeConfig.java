package cz.polymarket.bot.config;

import cz.polymarket.bot.domain.Timeframe;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;

@ApplicationScoped
public class ChartRangeConfig {

    private final BigDecimal range5m;
    private final BigDecimal range15m;
    private final int xRangeMinutes5m;
    private final int xRangeMinutes15m;

    @Inject
    public ChartRangeConfig(
            @ConfigProperty(name = "polymarket.chart.btc-usd.5m.y-range", defaultValue = "30") BigDecimal range5m,
            @ConfigProperty(name = "polymarket.chart.btc-usd.15m.y-range", defaultValue = "50") BigDecimal range15m,
            @ConfigProperty(name = "polymarket.chart.btc-usd.5m.x-range-minutes", defaultValue = "5") int xRangeMinutes5m,
            @ConfigProperty(name = "polymarket.chart.btc-usd.15m.x-range-minutes", defaultValue = "5") int xRangeMinutes15m) {
        this.range5m = range5m;
        this.range15m = range15m;
        this.xRangeMinutes5m = xRangeMinutes5m;
        this.xRangeMinutes15m = xRangeMinutes15m;
    }

    public BigDecimal getYRange(String symbol, Timeframe timeframe) {
        if (symbol == null || timeframe == null) {
            throw new IllegalArgumentException("Symbol and timeframe cannot be null");
        }
        if ("btc-usd".equalsIgnoreCase(symbol)) {
            return timeframe == Timeframe.FIFTEEN_MINUTES ? range15m : range5m;
        }
        return timeframe == Timeframe.FIFTEEN_MINUTES ? range15m : range5m;
    }

    public int getXRangeMinutes(String symbol, Timeframe timeframe) {
        if (symbol == null || timeframe == null) {
            throw new IllegalArgumentException("Symbol and timeframe cannot be null");
        }
        if ("btc-usd".equalsIgnoreCase(symbol)) {
            return timeframe == Timeframe.FIFTEEN_MINUTES ? xRangeMinutes15m : xRangeMinutes5m;
        }
        return timeframe == Timeframe.FIFTEEN_MINUTES ? xRangeMinutes15m : xRangeMinutes5m;
    }

    public BigDecimal getRange5m() {
        return range5m;
    }

    public BigDecimal getRange15m() {
        return range15m;
    }

    public int getXRangeMinutes5m() {
        return xRangeMinutes5m;
    }

    public int getXRangeMinutes15m() {
        return xRangeMinutes15m;
    }
}
