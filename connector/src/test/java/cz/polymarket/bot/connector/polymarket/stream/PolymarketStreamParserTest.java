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

    private final PolymarketStreamParser parser = new PolymarketStreamParser(new ObjectMapper(), "BTCUSDT");

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

        // Configured fallback symbol (e.g. ETHUSDT -> ethusd) is used when frame omits symbol
        PolymarketStreamParser ethParser = new PolymarketStreamParser(new ObjectMapper(), "ETHUSDT");
        String ethFrameWithoutSymbol = """
                {
                  "channel": "price.crypto",
                  "ts": 1792741520000,
                  "payload": {
                    "value": 3150.75,
                    "timestamp": 1792741520000
                  }
                }
                """;
        List<PolyBoltTick> ethTicks = ethParser.parsePolyBoltFrame(ethFrameWithoutSymbol, 1792741520010L);
        assertThat(ethTicks).hasSize(1);
        assertThat(ethTicks.getFirst().symbol()).isEqualTo("ethusd");
        assertThat(ethTicks.getFirst().price()).isCloseTo(3150.75, within(1e-6));
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

        // Apply price_change with embedded best_bid and best_ask fields and best_bid_ask event
        String priceChangeWithBboFrame = """
                {
                  "market": "0xcond1",
                  "price_changes": [
                    {
                      "asset_id": "token_up_1",
                      "price": "0.10",
                      "size": "500",
                      "side": "BUY",
                      "hash": "0xhash1",
                      "best_bid": "0.54",
                      "best_ask": "0.55"
                    },
                    {
                      "asset_id": "token_down_1",
                      "price": "0.45",
                      "size": "180",
                      "side": "BUY",
                      "hash": "0xhash2",
                      "best_bid": "0.45",
                      "best_ask": "0.46"
                    }
                  ],
                  "timestamp": "1792741507000",
                  "event_type": "price_change"
                }
                """;
        Optional<PolymarketBboQuote> quote3 = parser.applyClobFrame(priceChangeWithBboFrame, 1792741507000L, localBook);
        assertThat(quote3).isPresent();
        assertThat(quote3.get().bestBidUp()).isCloseTo(0.54, within(1e-6));
        assertThat(quote3.get().bestAskUp()).isCloseTo(0.55, within(1e-6));
        assertThat(quote3.get().bestBidDown()).isCloseTo(0.45, within(1e-6));
        assertThat(quote3.get().bestAskDown()).isCloseTo(0.46, within(1e-6));

        String bestBidAskFrame = """
                {
                  "event_type": "best_bid_ask",
                  "asset_id": "token_up_1",
                  "best_bid": "0.55",
                  "best_ask": "0.56",
                  "timestamp": "1792741508000"
                }
                """;
        Optional<PolymarketBboQuote> quote4 = parser.applyClobFrame(bestBidAskFrame, 1792741508000L, localBook);
        assertThat(quote4).isPresent();
        assertThat(quote4.get().bestBidUp()).isCloseTo(0.55, within(1e-6));
        assertThat(quote4.get().bestAskUp()).isCloseTo(0.56, within(1e-6));

        // Ignored event_type (last_trade_price and tick_size_change) returns Optional.empty() quickly
        String tradeFrame = """
                {
                  "market": "0xcond1",
                  "asset_id": "token_up_1",
                  "price": "0.56",
                  "size": "25",
                  "event_type": "last_trade_price"
                }
                """;
        assertThat(parser.applyClobFrame(tradeFrame, 1792741509000L, localBook)).isEmpty();

        String tickSizeFrame = """
                {
                  "market": "0xcond1",
                  "asset_id": "token_up_1",
                  "old_tick_size": "0.01",
                  "new_tick_size": "0.001",
                  "event_type": "tick_size_change"
                }
                """;
        assertThat(parser.applyClobFrame(tickSizeFrame, 1792741510000L, localBook)).isEmpty();
    }

    @Test
    void shouldParseUserChannelBuyAndSellTradeEventsAndIgnoreOrderEvents() {
        String singleBuyTrade = """
                {
                  "event_type": "trade",
                  "id": "trade-buy-1",
                  "asset_id": "token_up_1",
                  "taker_order_id": "0xorder-buy-1",
                  "side": "BUY",
                  "size": "100.0",
                  "price": "0.55",
                  "fee_rate_bps": "700",
                  "status": "MATCHED",
                  "trader_side": "TAKER",
                  "timestamp": "1792741510000"
                }
                """;
        List<PolymarketUserTradeEvent> buyEvents = parser.parseUserTradeEvents(singleBuyTrade);
        assertThat(buyEvents).hasSize(1);
        PolymarketUserTradeEvent buy = buyEvents.getFirst();
        assertThat(buy.tradeId()).isEqualTo("trade-buy-1");
        assertThat(buy.status()).isEqualTo("MATCHED");
        assertThat(buy.side()).isEqualTo("BUY");
        assertThat(buy.size()).isCloseTo(100.0, within(1e-6));
        assertThat(buy.price()).isCloseTo(0.55, within(1e-6));
        assertThat(buy.feeRateBps()).isEqualTo(700);
        assertThat(buy.assetId()).isEqualTo("token_up_1");
        assertThat(buy.orderId()).isEqualTo("0xorder-buy-1");

        String batchMixedEvents = """
                [
                  {
                    "event_type": "order",
                    "id": "0xorder-sell-1",
                    "status": "LIVE",
                    "side": "SELL",
                    "price": "0.70"
                  },
                  {
                    "type": "TRADE",
                    "id": "trade-sell-1",
                    "asset_id": "token_up_1",
                    "taker_order_id": "0xorder-sell-1",
                    "side": "SELL",
                    "size": "100",
                    "price": "0.70",
                    "fee_rate_bps": "700",
                    "status": "CONFIRMED"
                  }
                ]
                """;
        List<PolymarketUserTradeEvent> mixedEvents = parser.parseUserTradeEvents(batchMixedEvents);
        assertThat(mixedEvents).hasSize(1);
        PolymarketUserTradeEvent sell = mixedEvents.getFirst();
        assertThat(sell.tradeId()).isEqualTo("trade-sell-1");
        assertThat(sell.status()).isEqualTo("CONFIRMED");
        assertThat(sell.side()).isEqualTo("SELL");
        assertThat(sell.size()).isCloseTo(100.0, within(1e-6));
        assertThat(sell.price()).isCloseTo(0.70, within(1e-6));
        assertThat(sell.feeRateBps()).isEqualTo(700);

        assertThat(parser.parseUserTradeEvents("PONG")).isEmpty();
        assertThat(parser.parseUserTradeEvents("{\"event_type\":\"order\",\"id\":\"o1\"}")).isEmpty();
    }


    @Test
    void shouldFastForwardOlderFramesBeforeFullDualBookSnapshotInBatch() {
        String yesToken = "token_up_batch";
        String noToken = "token_down_batch";
        PolymarketLocalBook localBook = new PolymarketLocalBook(yesToken, noToken);

        cz.polymarket.bot.connector.ws.TimestampedFrame obsolete1 = new cz.polymarket.bot.connector.ws.TimestampedFrame(
                "{\"event_type\":\"best_bid_ask\",\"asset_id\":\"token_up_batch\",\"best_bid\":\"0.20\",\"best_ask\":\"0.21\"}",
                1000L
        );
        cz.polymarket.bot.connector.ws.TimestampedFrame obsolete2 = new cz.polymarket.bot.connector.ws.TimestampedFrame(
                "{\"event_type\":\"best_bid_ask\",\"asset_id\":\"token_down_batch\",\"best_bid\":\"0.79\",\"best_ask\":\"0.80\"}",
                1001L
        );
        cz.polymarket.bot.connector.ws.TimestampedFrame fullBookSnapshot = new cz.polymarket.bot.connector.ws.TimestampedFrame(
                """
                [
                  {
                    "event_type": "book",
                    "asset_id": "token_up_batch",
                    "bids": [{"price": "0.60", "size": "200"}],
                    "asks": [{"price": "0.62", "size": "150"}]
                  },
                  {
                    "event_type": "book",
                    "asset_id": "token_down_batch",
                    "bids": [{"price": "0.38", "size": "200"}],
                    "asks": [{"price": "0.40", "size": "150"}]
                  }
                ]
                """,
                1002L
        );
        cz.polymarket.bot.connector.ws.TimestampedFrame postSnapshotDelta = new cz.polymarket.bot.connector.ws.TimestampedFrame(
                """
                {
                  "event_type": "price_change",
                  "price_changes": [
                    {"asset_id": "token_up_batch", "price": "0.61", "size": "300", "side": "SELL", "best_bid": "0.60", "best_ask": "0.61"}
                  ]
                }
                """,
                1003L
        );

        PolymarketStreamParser.ClobBatchResult result = parser.applyClobFrameBatch(
                List.of(obsolete1, obsolete2, fullBookSnapshot, postSnapshotDelta),
                localBook
        );

        assertThat(result.fastForwardedFramesCount()).isEqualTo(2);
        assertThat(result.appliedFramesCount()).isEqualTo(2);
        assertThat(result.quote()).isPresent();
        assertThat(result.quote().get().bestBidUp()).isCloseTo(0.60, within(1e-6));
        assertThat(result.quote().get().bestAskUp()).isCloseTo(0.61, within(1e-6));
        assertThat(result.quote().get().bestBidDown()).isCloseTo(0.38, within(1e-6));
        assertThat(result.quote().get().bestAskDown()).isCloseTo(0.40, within(1e-6));
        assertThat(result.quote().get().timestampMs()).isEqualTo(1003L);
    }
}
