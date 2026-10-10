package cz.polymarket.bot.data.web;

import cz.polymarket.bot.data.service.DataCollectorService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * REST endpoint exposing live connection health and storage counters for the Data Collector.
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
     * Returns the current connection states, tracked markets, and record counters.
     *
     * @return current {@link CollectorStatusDto}
     */
    @GET
    @Path("/status")
    public CollectorStatusDto getStatus() {
        return collectorService.getStatus();
    }
}
