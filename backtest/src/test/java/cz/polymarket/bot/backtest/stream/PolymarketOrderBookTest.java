package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.OrderBookQuote;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PolymarketOrderBookTest {

    @Test
    @DisplayName("Should apply snapshot and track best bid and ask for UP and DOWN")
    void shouldApplySnapshotAndDeriveDown() {
        PolymarketOrderBook book = new PolymarketOrderBook();

        String bidsJson = "[{\"price\":\"0.51\",\"size\":\"200.0\"},{\"price\":\"0.50\",\"size\":\"100.0\"}]";
        String asksJson = "[{\"price\":\"0.53\",\"size\":\"150.0\"},{\"price\":\"0.54\",\"size\":\"250.0\"}]";

        book.applySnapshot(bidsJson, asksJson, 1000L);

        assertThat(book.getBestBidUp()).isEqualTo(0.51);
        assertThat(book.getBestAskUp()).isEqualTo(0.53);

        // Down derivation
        assertThat(book.getBestBidDown()).isCloseTo(0.47, within(0.001)); // 1.0 - 0.53
        assertThat(book.getBestAskDown()).isCloseTo(0.49, within(0.001)); // 1.0 - 0.51

        OrderBookQuote quote = book.toOrderBookQuote(1000L);
        assertThat(quote.bestAskUp()).isEqualTo(0.53);
        assertThat(quote.bestBidUp()).isEqualTo(0.51);
        assertThat(quote.bestAskDown()).isCloseTo(0.49, within(0.001));
        assertThat(quote.bestBidDown()).isCloseTo(0.47, within(0.001));
        assertThat(quote.timestampMs()).isEqualTo(1000L);
    }

    @Test
    @DisplayName("Should apply delta updates, adding and removing price levels")
    void shouldApplyDeltaUpdates() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.applyDelta("BUY", 0.55, 300.0, 2000L);
        book.applyDelta("SELL", 0.58, 200.0, 2000L);

        assertThat(book.getBestBidUp()).isEqualTo(0.55);
        assertThat(book.getBestAskUp()).isEqualTo(0.58);

        // Remove level with size 0
        book.applyDelta("BUY", 0.55, 0.0, 2100L);
        assertThat(book.getBestBidUp()).isEqualTo(0.49); // default fallback
    }

    @Test
    @DisplayName("Should simulate market buy fill and compute crypto taker fee")
    void shouldSimulateFillWithFee() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        String asksJson = "[{\"price\":\"0.50\",\"size\":\"50.0\"},{\"price\":\"0.52\",\"size\":\"50.0\"}]";
        book.applySnapshot("[]", asksJson, 3000L);

        // Buy 100 UP shares: 50 @ 0.50 + 50 @ 0.52 -> avg price 0.51
        PolymarketOrderBook.FillResult fill = book.simulateFill(true, true, 100.0, 1.0);

        assertThat(fill.executedShares()).isEqualTo(100.0);
        assertThat(fill.avgPrice()).isCloseTo(0.51, within(0.001));

        // Fee = shares * 0.07 * P * (1 - P) = 100 * 0.07 * 0.51 * 0.49 ~= 1.7489
        assertThat(fill.fee()).isCloseTo(1.7489, within(0.01));
    }
}
