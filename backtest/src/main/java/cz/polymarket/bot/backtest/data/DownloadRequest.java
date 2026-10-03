package cz.polymarket.bot.backtest.data;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public record DownloadRequest(
        String symbol,
        LocalDate startDate,
        LocalDate endDate,
        Set<DataType> dataTypes
) {
    public DownloadRequest {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol must not be blank");
        }
        symbol = symbol.trim().toUpperCase(Locale.ROOT);

        Objects.requireNonNull(startDate, "startDate must not be null");
        Objects.requireNonNull(endDate, "endDate must not be null");

        if (startDate.isAfter(endDate)) {
            throw new IllegalArgumentException(
                    "start date must not be after end date (start: " + startDate + ", end: " + endDate + ")"
            );
        }

        if (dataTypes == null || dataTypes.isEmpty()) {
            dataTypes = Collections.unmodifiableSet(EnumSet.allOf(DataType.class));
        } else {
            dataTypes = Collections.unmodifiableSet(EnumSet.copyOf(dataTypes));
        }
    }

    public static DownloadRequest of(String symbol, String startStr, String endStr, Set<DataType> types) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol must not be blank");
        }
        try {
            LocalDate start = LocalDate.parse(startStr);
            LocalDate end = LocalDate.parse(endStr);
            return new DownloadRequest(symbol, start, end, types);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date format, expected yyyy-MM-dd: " + e.getMessage(), e);
        }
    }
}
