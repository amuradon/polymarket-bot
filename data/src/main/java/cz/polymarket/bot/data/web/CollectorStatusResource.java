package cz.polymarket.bot.data.web;

import cz.polymarket.bot.data.service.DataCollectorService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/**
 * REST endpoint exposing live connection health, storage counters, and Blue-Green cutover actions
 * for the Data Collector.
 */
@Path("/api/collector")
@Produces(MediaType.APPLICATION_JSON)
public class CollectorStatusResource {

    private final DataCollectorService collectorService;

    /**
     * Constructs the status resource injecting the collector orchestrator service.
     *
     * @param collectorService data collector service
     */
    @Inject
    public CollectorStatusResource(DataCollectorService collectorService) {
        this.collectorService = collectorService;
    }

    /**
     * Returns the current connection states, write mode, readiness flag, tracked markets, and record counters.
     *
     * @return current {@link CollectorStatusDto}
     */
    @GET
    @Path("/status")
    public CollectorStatusDto getStatus() {
        return collectorService.getStatus();
    }

    /**
     * Atomically freezes this Data Collector instance at {@code timestampMs} (defaults to {@code System.currentTimeMillis()}),
     * flushing and closing all open {@code .jsonl.zst} files and releasing the public HTTP port so the standby Green
     * instance can take over without data loss or duplication.
     *
     * @param timestampMs optional explicit cutover timestamp in epoch milliseconds
     * @return {@link CutoverFreezeResponseDto} containing the exact {@code cutoverTimestampMs}
     */
    @POST
    @Path("/cutover/freeze")
    public CutoverFreezeResponseDto freeze(@QueryParam("timestampMs") Long timestampMs) {
        long cutoffTs = (timestampMs != null && timestampMs > 0L) ? timestampMs : System.currentTimeMillis();
        return collectorService.freeze(cutoffTs);
    }

    /**
     * Atomically activates this Data Collector instance from {@code STANDBY} into {@code ACTIVE}, replaying
     * buffered events with {@code recvTimestampMs > afterTimestampMs} to disk and discarding earlier duplicates.
     *
     * @param afterTimestampMs cutover timestamp returned by the frozen Blue instance (defaults to 0)
     * @return {@link CutoverActivateResponseDto} summarizing replayed and dropped records
     */
    @POST
    @Path("/cutover/activate")
    public CutoverActivateResponseDto activate(@QueryParam("afterTimestampMs") Long afterTimestampMs) {
        long cutoffTs = (afterTimestampMs != null) ? afterTimestampMs : 0L;
        return collectorService.activate(cutoffTs);
    }
}
