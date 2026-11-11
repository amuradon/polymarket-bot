package cz.polymarket.bot.live.web;

import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.live.engine.LiveTradingEngine;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

/**
 * REST and Web resource for the Live Trading dashboard and monitoring APIs.
 * Exposes live execution status, active Polymarket market metadata, open positions,
 * and 4-core / OS / WebSocket telemetry.
 */
@Path("/")
@Blocking
@ApplicationScoped
public class LiveDashboardResource {

    private final LiveTradingEngine engine;
    private final Template dashboardTemplate;

    /**
     * Constructs the live dashboard REST resource.
     *
     * @param engine            live trading engine
     * @param dashboardTemplate Qute HTML dashboard template
     */
    @Inject
    public LiveDashboardResource(
            LiveTradingEngine engine,
            @Location("live-dashboard.html") Template dashboardTemplate) {
        if (engine == null) {
            throw new IllegalArgumentException("engine cannot be null");
        }
        if (dashboardTemplate == null) {
            throw new IllegalArgumentException("dashboardTemplate cannot be null");
        }
        this.engine = engine;
        this.dashboardTemplate = dashboardTemplate;
    }

    /**
     * Renders the HTML live trading dashboard.
     *
     * @return rendered {@link TemplateInstance}
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance getDashboard() {
        PolymarketMarketInfo market = engine.getActiveMarketInfo();

        return dashboardTemplate
                .data("strategyName", engine.getActiveStrategyName())
                .data("status", engine.getStatus())
                .data("orderType", engine.getOrderType())
                .data("activeMarketSlug", market != null ? market.slug() : "DISCOVERING")
                .data("initialCapital", engine.getInitialCapital())
                .data("currentBalance", engine.getCurrentBalance())
                .data("activePosition", engine.getActivePosition())
                .data("telemetry", engine.getTelemetry());
    }

    /**
     * Returns the current live trading status snapshot as JSON.
     *
     * @return HTTP 200 with {@link LiveStatusDto}
     */
    @GET
    @Path("/api/live/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStatus() {
        PolymarketMarketInfo market = engine.getActiveMarketInfo();
        LiveStatusDto status = new LiveStatusDto(
                engine.getActiveStrategyName(),
                engine.getStatus(),
                engine.getOrderType(),
                market != null ? market.slug() : "",
                market != null ? market.yesTokenId() : "",
                market != null ? market.noTokenId() : "",
                engine.getInitialCapital(),
                engine.getCurrentBalance(),
                engine.getActivePosition(),
                engine.getOpenPositions(),
                engine.getTelemetry()
        );
        return Response.ok(status).build();
    }

    /**
     * Returns the current OS, JVM ZGC, 4-core thread utilization, order execution, and WebSocket stream telemetry.
     *
     * @return HTTP 200 with {@link LiveTelemetryDto}
     */
    @GET
    @Path("/api/live/telemetry")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTelemetry() {
        return Response.ok(engine.getTelemetry()).build();
    }

    /**
     * Returns the list of currently open live positions in the active candle.
     *
     * @return HTTP 200 with {@code List<LivePosition>}
     */
    @GET
    @Path("/api/live/positions")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPositions() {
        return Response.ok(engine.getOpenPositions()).build();
    }

    /**
     * Resets live trading engine state and open positions.
     *
     * @return HTTP 200 confirmation payload
     */
    @POST
    @Path("/api/live/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        engine.reset();
        return Response.ok(Map.of(
                "status", "SUCCESS",
                "message", "Live trading engine state reset successfully",
                "initialCapital", engine.getInitialCapital(),
                "currentBalance", engine.getCurrentBalance()
        )).build();
    }
}
