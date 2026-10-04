package cz.polymarket.bot.paper.web;

import cz.polymarket.bot.paper.engine.PaperTradingEngine;
import cz.polymarket.bot.paper.storage.PaperTradeRepository;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * REST and Web resource for the paper trading dashboard and APIs.
 * Exposes live performance metrics, active position details, and trade history.
 */
@Path("/")
@ApplicationScoped
public class PaperDashboardResource {

    private final PaperTradingEngine engine;
    private final PaperTradeRepository repository;
    private final Template dashboardTemplate;

    @Inject
    public PaperDashboardResource(
            PaperTradingEngine engine,
            PaperTradeRepository repository,
            @Location("paper-dashboard.html") Template dashboardTemplate) {
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

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance getDashboard() {
        List<cz.polymarket.bot.domain.TradeRecord> allTrades = new ArrayList<>(repository.getTrades());
        Collections.reverse(allTrades);

        return dashboardTemplate
                .data("strategyName", engine.getActiveStrategyName())
                .data("status", engine.getStatus())
                .data("latencyMs", engine.getOrderLatencyMs())
                .data("initialCapital", repository.getInitialCapital())
                .data("currentBalance", repository.getCurrentBalance())
                .data("metrics", repository.getMetrics())
                .data("activePosition", engine.getActivePosition())
                .data("trades", allTrades);
    }

    @GET
    @Path("/api/paper/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStatus() {
        PaperStatusDto status = new PaperStatusDto(
                engine.getActiveStrategyName(),
                engine.getStatus(),
                engine.getOrderLatencyMs(),
                repository.getInitialCapital(),
                repository.getCurrentBalance(),
                repository.getTrades().size(),
                engine.getActivePosition()
        );
        return Response.ok(status).build();
    }

    @GET
    @Path("/api/paper/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMetrics() {
        return Response.ok(repository.getMetrics()).build();
    }

    @GET
    @Path("/api/paper/trades")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTrades() {
        return Response.ok(repository.getTrades()).build();
    }

    @jakarta.ws.rs.POST
    @Path("/api/paper/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        engine.reset();
        repository.reset();
        return Response.ok(java.util.Map.of(
                "status", "SUCCESS",
                "message", "Paper trading history and metrics reset successfully",
                "initialCapital", repository.getInitialCapital(),
                "currentBalance", repository.getCurrentBalance()
        )).build();
    }
}
