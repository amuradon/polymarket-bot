package cz.polymarket.bot.data.web;

/**
 * Response payload returned when activating a standby Data Collector slot during Blue-Green cutover.
 *
 * @param writeMode                 resulting write mode ({@code "ACTIVE"})
 * @param activatedAfterTimestampMs cutoff timestamp in epoch milliseconds; only events strictly after this timestamp were replayed
 * @param replayedRecords           number of buffered standby events written to disk during activation
 * @param droppedDuplicateRecords   number of buffered standby events with {@code ts <= activatedAfterTimestampMs} discarded as duplicates
 */
public record CutoverActivateResponseDto(
        String writeMode,
        long activatedAfterTimestampMs,
        long replayedRecords,
        long droppedDuplicateRecords
) {
}
