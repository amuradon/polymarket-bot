package cz.polymarket.bot.exchange;

import cz.polymarket.bot.calculator.MedianCalculator;
import cz.polymarket.bot.domain.Exchange;
import cz.polymarket.bot.domain.PriceSnapshot;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class ExchangePriceTracker {

    private final MedianCalculator medianCalculator;
    private final AtomicReference<BigDecimal> binancePrice = new AtomicReference<>();
    private final AtomicReference<BigDecimal> coinbasePrice = new AtomicReference<>();
    private final AtomicReference<BigDecimal> krakenPrice = new AtomicReference<>();

    @Inject
    public ExchangePriceTracker(MedianCalculator medianCalculator) {
        this.medianCalculator = medianCalculator;
    }

    public void updatePrice(Exchange exchange, BigDecimal price) {
        if (exchange == null || price == null) {
            throw new IllegalArgumentException("Exchange and price cannot be null");
        }
        switch (exchange) {
            case BINANCE -> binancePrice.set(price);
            case COINBASE -> coinbasePrice.set(price);
            case KRAKEN -> krakenPrice.set(price);
        }
    }

    public Optional<PriceSnapshot> getSnapshot(Instant timestamp) {
        BigDecimal b = binancePrice.get();
        BigDecimal c = coinbasePrice.get();
        BigDecimal k = krakenPrice.get();

        if (b == null || c == null || k == null) {
            return Optional.empty();
        }

        BigDecimal median = medianCalculator.calculate(b, c, k);
        return Optional.of(new PriceSnapshot(timestamp, b, c, k, median));
    }
}
