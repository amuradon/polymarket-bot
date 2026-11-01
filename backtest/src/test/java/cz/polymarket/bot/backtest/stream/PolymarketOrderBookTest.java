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

        assertThat(book.getBestBidUp()).isCloseTo(0.51, within(0.0001));
        assertThat(book.getBestAskUp()).isCloseTo(0.53, within(0.0001));

        // Down derivation via complementarity when only UP book is populated
        assertThat(book.getBestBidDown()).isCloseTo(0.47, within(0.001));
        assertThat(book.getBestAskDown()).isCloseTo(0.49, within(0.001));

        OrderBookQuote quote = book.toOrderBookQuote(1000L);
        assertThat(quote.bestAskUp()).isCloseTo(0.53, within(0.0001));
        assertThat(quote.bestBidUp()).isCloseTo(0.51, within(0.0001));
        assertThat(quote.bestAskDown()).isCloseTo(0.49, within(0.001));
        assertThat(quote.bestBidDown()).isCloseTo(0.47, within(0.001));
        assertThat(quote.timestampMs()).isEqualTo(1000L);
    }

    @Test
    @DisplayName("Should apply collector clob, book, price_change, and track real-time observed one-way latency")
    void shouldApplyCollectorEventsAndTrackObservedLatency() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.setMarketTokenIds("95285", "18701");

        String clobPayload = "[" +
                "{\"market\":\"0xabc\",\"asset_id\":\"95285\",\"timestamp\":\"1775939386039\"," +
                "\"bids\":[{\"price\":\"0.49\",\"size\":\"200\"}],\"asks\":[{\"price\":\"0.51\",\"size\":\"250\"}]}," +
                "{\"market\":\"0xabc\",\"asset_id\":\"18701\",\"timestamp\":\"1775939386039\"," +
                "\"bids\":[{\"price\":\"0.49\",\"size\":\"250\"}],\"asks\":[{\"price\":\"0.51\",\"size\":\"200\"}]}" +
                "]";

        // Receipt ts = 1775939386058 -> observed latency = 1775939386058 - 1775939386039 = 19ms
        book.applyCollectorEvent(new BacktestEvent.PolymarketClobEvent(
                1775939386058L, 1L, "clob", null, 0.0, 0.0, null, clobPayload
        ));

        assertThat(book.getLastObservedOneWayLatencyMs()).isEqualTo(19L);
        assertThat(book.getBestBidUp()).isCloseTo(0.49, within(0.0001));
        assertThat(book.getBestAskUp()).isCloseTo(0.51, within(0.0001));
        assertThat(book.getBestBidDown()).isCloseTo(0.49, within(0.0001));
        assertThat(book.getBestAskDown()).isCloseTo(0.51, within(0.0001));

        // Apply price_change event with server timestamp 1775939406635 and receipt ts 1775939406647 (12ms latency)
        String priceChangePayload = "{\"market\":\"0xabc\",\"price_changes\":[" +
                "{\"asset_id\":\"95285\",\"price\":\"0.52\",\"size\":\"346\",\"side\":\"SELL\",\"best_bid\":\"0.49\",\"best_ask\":\"0.51\"}," +
                "{\"asset_id\":\"95285\",\"price\":\"0.51\",\"size\":\"0\",\"side\":\"SELL\",\"best_bid\":\"0.49\",\"best_ask\":\"0.52\"}," +
                "{\"asset_id\":\"18701\",\"price\":\"0.48\",\"size\":\"346\",\"side\":\"BUY\",\"best_bid\":\"0.48\",\"best_ask\":\"0.51\"}" +
                "],\"timestamp\":\"1775939406635\"}";

        book.applyCollectorEvent(new BacktestEvent.PolymarketClobEvent(
                1775939406647L, 2L, "price_change", null, 0.0, 0.0, null, priceChangePayload
        ));

        assertThat(book.getLastObservedOneWayLatencyMs()).isEqualTo(12L);
        assertThat(book.getBestAskUp()).isCloseTo(0.52, within(0.0001));
    }

    @Test
    @DisplayName("Should apply delta updates, adding and removing price levels")
    void shouldApplyDeltaUpdates() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.applyDelta("BUY", 0.55, 300.0, 2000L);
        book.applyDelta("SELL", 0.58, 200.0, 2000L);

        assertThat(book.getBestBidUp()).isCloseTo(0.55, within(0.0001));
        assertThat(book.getBestAskUp()).isCloseTo(0.58, within(0.0001));

        // Remove level with size 0
        book.applyDelta("BUY", 0.55, 0.0, 2100L);
        assertThat(book.getBestBidUp()).isNaN();
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

    @Test
    @DisplayName("Should not synthesize liquidity when order book is empty or has partial depth")
    void shouldNotSynthesizeLiquidityWhenBookInsufficient() {
        PolymarketOrderBook book = new PolymarketOrderBook();

        PolymarketOrderBook.FillResult emptyFill = book.simulateFill(true, true, 100.0, 1.0);
        assertThat(emptyFill.executedShares()).isEqualTo(0.0);
        assertThat(emptyFill.avgPrice()).isEqualTo(0.0);
        assertThat(emptyFill.fee()).isEqualTo(0.0);

        String asksJson = "[{\"price\":\"0.50\",\"size\":\"40.0\"}]";
        book.applySnapshot("[]", asksJson, 3000L);

        PolymarketOrderBook.FillResult partialFill = book.simulateFill(true, true, 100.0, 1.0);
        assertThat(partialFill.executedShares()).isEqualTo(40.0);
        assertThat(partialFill.avgPrice()).isCloseTo(0.50, within(0.0001));
    }
}
