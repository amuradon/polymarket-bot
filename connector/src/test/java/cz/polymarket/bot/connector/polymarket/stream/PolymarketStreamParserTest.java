package cz.polymarket.bot.connector.polymarket.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link PolymarketStreamParser} and {@link PolymarketLocalBook} verifying
 * PolyBolt v2 snapshot & live tick parsing and CLOB {@code book}, {@code price_change},
 * and {@code best_bid_ask} local orderbook BBO extraction.
 */
class PolymarketStreamParserTest {

    private final PolymarketStreamParser parser = new PolymarketStreamParser(new ObjectMapper());

    @Test
    void shouldParsePolyBoltInitialSnapshotAndLiveTwapAndSpotFrames() {
        String snapshotJson = """
                {
                  "channel": "price.crypto",
                  "snapshot": true,
                  "payload": {
                    "source": "chainlink",
                    "symbol": "btcusd",
                    "data": [
                      {"full_accuracy_value": "74200.12000000", "value": 74200.12, "timestamp": 1792741500000},
                      {"full_accuracy_value": "74205.50000000", "value": 74205.50, "timestamp": 1792741501000}
                    ]
                  }
                }
                """;

        List<PolyBoltTick> snapshotTicks = parser.parsePolyBoltFrame(snapshotJson, 1792741502000L);
        assertThat(snapshotTicks).hasSize(2);
        assertThat(snapshotTicks.get(0).isTwap()).isFalse();
        assertThat(snapshotTicks.get(0).price()).isCloseTo(74200.12, within(1e-6));
        assertThat(snapshotTicks.get(0).timestampMs()).isEqualTo(1792741500000L);
        assertThat(snapshotTicks.get(1).price()).isCloseTo(74205.50, within(1e-6));

        String liveTwapJson = """
                {
                  "v": 1,
                  "channel": "price.crypto.twap",
                  "seq": 10,
                  "ts": 1792741510000,
                  "payload": {
                    "symbol": "btcusd",
                    "source": "chainlink",
                    "window_seconds": 60,
                    "value": 74210.25,
                    "full_accuracy_value": "74210.25000000",
                    "timestamp": 1792741510000
                  }
                }
                """;

        List<PolyBoltTick> twapTicks = parser.parsePolyBoltFrame(liveTwapJson, 1792741510050L);
        assertThat(twapTicks).hasSize(1);
        PolyBoltTick twap = twapTicks.getFirst();
        assertThat(twap.isTwap()).isTrue();
        assertThat(twap.windowSeconds()).isEqualTo(60);
        assertThat(twap.price()).isCloseTo(74210.25, within(1e-6));
        assertThat(twap.timestampMs()).isEqualTo(1792741510050L);
        assertThat(twap.oracleTimestampMs()).isEqualTo(1792741510000L);

        // Control frame yields empty list
        assertThat(parser.parsePolyBoltFrame("{\"op\":\"authed\",\"rid\":\"a1\"}", 1000L)).isEmpty();
    }

    @Test
    void shouldMaintainLocalOrderBookAcrossBookSnapshotPriceChangeAndBestBidAsk() {
        String yesToken = "token_up_1";
        String noToken = "token_down_1";
        PolymarketLocalBook localBook = new PolymarketLocalBook(yesToken, noToken);

        String bookFrame = """
                [
                  {
                    "event_type": "book",
                    "asset_id": "token_up_1",
                    "bids": [{"price": "0.52", "size": "120"}, {"price": "0.54", "size": "80"}],
                    "asks": [{"price": "0.57", "size": "90"}, {"price": "0.56", "size": "60"}]
                  },
                  {
                    "event_type": "book",
                    "asset_id": "token_down_1",
                    "bids": [{"price": "0.43", "size": "70"}, {"price": "0.44", "size": "60"}],
                    "asks": [{"price": "0.46", "size": "80"}, {"price": "0.48", "size": "120"}]
                  }
                ]
                """;

        Optional<PolymarketBboQuote> quote1 = parser.applyClobFrame(bookFrame, 1792741505000L, localBook);
        assertThat(quote1).isPresent();
        assertThat(quote1.get().bestBidUp()).isCloseTo(0.54, within(1e-6));
        assertThat(quote1.get().bestAskUp()).isCloseTo(0.56, within(1e-6));
        assertThat(quote1.get().bestBidDown()).isCloseTo(0.44, within(1e-6));
        assertThat(quote1.get().bestAskDown()).isCloseTo(0.46, within(1e-6));
        assertThat(quote1.get().askSizeUp()).isCloseTo(60.0, within(1e-6));
        assertThat(quote1.get().askSizeDown()).isCloseTo(80.0, within(1e-6));

        // Apply price_change removing 0.56 ask on UP and adding 0.55 ask
        String deltaFrame = """
                {
                  "event_type": "price_change",
                  "price_changes": [
                    {"asset_id": "token_up_1", "price": "0.56", "size": "0", "side": "SELL"},
                    {"asset_id": "token_up_1", "price": "0.55", "size": "150", "side": "SELL"}
                  ]
                }
                """;

        Optional<PolymarketBboQuote> quote2 = parser.applyClobFrame(deltaFrame, 1792741506000L, localBook);
        assertThat(quote2).isPresent();
        assertThat(quote2.get().bestAskUp()).isCloseTo(0.55, within(1e-6));
        assertThat(quote2.get().askSizeUp()).isCloseTo(150.0, within(1e-6));
    }
}
