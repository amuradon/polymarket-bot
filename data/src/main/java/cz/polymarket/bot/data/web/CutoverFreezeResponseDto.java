package cz.polymarket.bot.data.web;

/**
 * Response payload returned when freezing an active Data Collector slot during Blue-Green cutover.
 *
 * @param writeMode           resulting write mode ({@code "FROZEN"})
 * @param cutoverTimestampMs  exact epoch millisecond cutoff timestamp at which Zstandard files were flushed and closed
 * @param totalRecordsWritten total records written across all storage managers prior to freeze
 */
public record CutoverFreezeResponseDto(
        String writeMode,
        long cutoverTimestampMs,
        long totalRecordsWritten
) {
}
