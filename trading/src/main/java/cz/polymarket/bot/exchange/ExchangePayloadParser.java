package cz.polymarket.bot.exchange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.domain.MarketCandle;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class ExchangePayloadParser {

    private final ObjectMapper objectMapper;

    @Inject
    public ExchangePayloadParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<MarketCandle> parseBinanceMarketCandles(String json) {
        List<MarketCandle> candles = new ArrayList<>();
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root.isArray()) {
                for (JsonNode kline : root) {
                    if (kline.isArray() && kline.size() >= 10) {
                        long openTimeMs = kline.get(0).asLong();
                        long openTimeSec = openTimeMs / 1000L;
                        double open = kline.get(1).asDouble();
                        double high = kline.get(2).asDouble();
                        double low = kline.get(3).asDouble();
                        double close = kline.get(4).asDouble();
                        double volumeBtc = kline.get(5).asDouble();
                        long closeTimeMs = kline.get(6).asLong();
                        long closeTimeSec = closeTimeMs / 1000L;
                        double volumeUsd = kline.get(7).asDouble();
                        double takerBuyBtc = kline.get(9).asDouble();
                        double deltaBtc = (2.0 * takerBuyBtc) - volumeBtc;

                        candles.add(new MarketCandle(
                                openTimeSec,
                                closeTimeSec,
                                open,
                                high,
                                low,
                                close,
                                volumeBtc,
                                volumeUsd,
                                deltaBtc,
                                open,
                                high,
                                low,
                                close,
                                volumeBtc,
                                volumeUsd,
                                deltaBtc,
                                0.0
                        ));
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return candles;
    }
}
