package cz.polymarket.bot.exchange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.domain.MarketCandle;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@ApplicationScoped
public class ExchangePayloadParser {

    private final ObjectMapper objectMapper;

    @Inject
    public ExchangePayloadParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Optional<cz.polymarket.bot.domain.BinanceTicker> parseBinanceTicker(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root.hasNonNull("c") && root.hasNonNull("E")) {
                long eventTimeMs = root.get("E").asLong();
                BigDecimal price = new BigDecimal(root.get("c").asText());
                return Optional.of(new cz.polymarket.bot.domain.BinanceTicker(eventTimeMs, price));
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public Optional<BigDecimal> parseCoinbaseTicker(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if ("ticker".equals(root.path("type").asText()) && root.hasNonNull("price")) {
                return Optional.of(new BigDecimal(root.get("price").asText()));
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public Optional<BigDecimal> parseKrakenTicker(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if ("ticker".equals(root.path("channel").asText()) && root.has("data")) {
                JsonNode data = root.get("data");
                if (data.isArray() && !data.isEmpty()) {
                    JsonNode item = data.get(0);
                    if (item.hasNonNull("last")) {
                        return Optional.of(new BigDecimal(item.get("last").asText()));
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public Map<Long, BigDecimal> parseBinanceKlines(String json) {
        Map<Long, BigDecimal> result = new TreeMap<>();
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root.isArray()) {
                for (JsonNode kline : root) {
                    if (kline.isArray() && kline.size() >= 5) {
                        long openTimeMs = kline.get(0).asLong();
                        long openTimeSec = openTimeMs / 1000L;
                        BigDecimal closePrice = new BigDecimal(kline.get(4).asText());
                        result.put(openTimeSec, closePrice);
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return result;
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
