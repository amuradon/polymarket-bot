package cz.polymarket.bot.live.web;

import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.live.engine.LiveTradingEngine;
import cz.polymarket.bot.live.storage.LiveTradeRepository;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * REST and Web resource for the Live Trading dashboard and monitoring APIs.
 * Exposes live execution status, active Polymarket market metadata, open positions, and trade history.
 */
@Path("/")
@ApplicationScoped
public class LiveDashboardResource {

    private final LiveTradingEngine engine;
    private final LiveTradeRepository repository;
    private final Template dashboardTemplate;

    /**
     * Constructs the live dashboard REST resource.
     *
     * @param engine            live trading engine
     * @param repository        persistent live trade repository
     * @param dashboardTemplate Qute HTML dashboard template
     */
    @Inject
    public LiveDashboardResource(
            LiveTradingEngine engine,
            LiveTradeRepository repository,
            @Location("live-dashboard.html") Template dashboardTemplate) {
        if (engine == null) {
            throw new IllegalArgumentException("engine cannot be null");
        }
        if (repository == null) {
            throw new IllegalArgumentException("repository cannot be null");
        }
        if (dashboardTemplate == null) {
            throw new IllegalArgumentException("dashboardTemplate cannot be null");
        }
        this.engine = engine;
        this.repository = repository;
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
        List<TradeRecord> allTrades = new ArrayList<>(repository.getTrades());
        Collections.reverse(allTrades);
        PolymarketMarketInfo market = engine.getActiveMarketInfo();

        return dashboardTemplate
                .data("strategyName", engine.getActiveStrategyName())
                .data("status", engine.getStatus())
                .data("orderType", engine.getOrderType())
                .data("activeMarketSlug", market != null ? market.slug() : "DISCOVERING")
                .data("initialCapital", repository.getInitialCapital())
                .data("currentBalance", repository.getCurrentBalance())
                .data("metrics", repository.getMetrics())
                .data("activePosition", engine.getActivePosition())
                .data("trades", allTrades);
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
                engine.isDryRun(),
                engine.getOrderType(),
                market != null ? market.slug() : "",
                market != null ? market.yesTokenId() : "",
                market != null ? market.noTokenId() : "",
                repository.getInitialCapital(),
                repository.getCurrentBalance(),
                repository.getTrades().size(),
                engine.getActivePosition(),
                engine.getOpenPositions()
        );
        return Response.ok(status).build();
    }

    /**
     * Returns the current aggregated live performance metrics.
     *
     * @return HTTP 200 with {@link cz.polymarket.bot.calculator.PerformanceMetrics}
     */
    @GET
    @Path("/api/live/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMetrics() {
        return Response.ok(repository.getMetrics()).build();
    }

    /**
     * Returns the list of all settled live trades.
     *
     * @return HTTP 200 with {@code List<TradeRecord>}
     */
    @GET
    @Path("/api/live/trades")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTrades() {
        return Response.ok(repository.getTrades()).build();
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
     * Resets live trading metrics and trade history to initial capital.
     *
     * @return HTTP 200 confirmation payload
     */
    @POST
    @Path("/api/live/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        engine.reset();
        repository.reset();
        return Response.ok(Map.of(
                "status", "SUCCESS",
                "message", "Live trading history and metrics reset successfully",
                "initialCapital", repository.getInitialCapital(),
                "currentBalance", repository.getCurrentBalance()
        )).build();
    }
}
