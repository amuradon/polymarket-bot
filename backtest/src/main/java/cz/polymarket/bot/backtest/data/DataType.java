package cz.polymarket.bot.backtest.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.Locale;

@Schema(name = "DataType", description = "Supported Binance market data types for historical download.", enumeration = {"spot_trades", "futures_trades", "orderbook"})
public enum DataType {
    SPOT_TRADES("spot_trades"),
    FUTURES_TRADES("futures_trades"),
    ORDER_BOOK("orderbook");

    private final String key;

    DataType(String key) {
        this.key = key;
    }

    @JsonValue
    public String getKey() {
        return key;
    }

    @JsonCreator
    public static DataType fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("DataType value must not be blank");
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT).replace("-", "_");
        for (DataType type : values()) {
            if (type.key.equals(normalized) || type.name().toLowerCase(Locale.ROOT).equals(normalized)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown data type: " + value);
    }
}
