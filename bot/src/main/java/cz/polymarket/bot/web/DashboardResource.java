package cz.polymarket.bot.web;

import cz.polymarket.bot.collector.DataCollectorService;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.engine.StrategyMode;
import cz.polymarket.bot.engine.StrategyTradingEngine;
import cz.polymarket.bot.service.RuntimeCoordinator;
import cz.polymarket.bot.storage.PaperTradeRepository;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unified REST and Web Dashboard resource for Polymarket Bot.
 * Exposes the Qute dashboard ({@code GET /}), composite status and health checks ({@code GET /api/status},
 * {@code GET /api/health}), runtime {@link StrategyMode} switching ({@code POST /api/mode}),
 * 4-core / OS / WebSocket telemetry ({@code GET /api/telemetry}), open positions ({@code GET /api/positions}),
 * Paper Trading metrics and trade history ({@code GET /api/paper/metrics}, {@code GET /api/paper/trades}),
 * state reset ({@code POST /api/reset}), and Data Collector status ({@code GET /api/collector/status}).
 */
@Path("/")
@Blocking
@ApplicationScoped
public class DashboardResource {

    private final RuntimeCoordinator coordinator;
    private final StrategyTradingEngine tradingEngine;
    private final DataCollectorService collectorService;
    private final PaperTradeRepository paperRepository;
    private final Template dashboardTemplate;

    /**
     * Constructs the unified dashboard REST resource.
     *
     * @param coordinator       runtime coordinator
     * @param tradingEngine     strategy trading engine
     * @param collectorService  Data Collector orchestrator service
     * @param paperRepository   persistent paper trade repository
     * @param dashboardTemplate Qute HTML dashboard template ({@code dashboard.html})
     */
    @Inject
    public DashboardResource(
            RuntimeCoordinator coordinator,
            StrategyTradingEngine tradingEngine,
            DataCollectorService collectorService,
            PaperTradeRepository paperRepository,
            @Location("dashboard.html") Template dashboardTemplate) {
        if (coordinator == null) {
            throw new IllegalArgumentException("coordinator cannot be null");
        }
        if (tradingEngine == null) {
            throw new IllegalArgumentException("tradingEngine cannot be null");
        }
        if (collectorService == null) {
            throw new IllegalArgumentException("collectorService cannot be null");
        }
        if (paperRepository == null) {
            throw new IllegalArgumentException("paperRepository cannot be null");
        }
        if (dashboardTemplate == null) {
            throw new IllegalArgumentException("dashboardTemplate cannot be null");
        }
        this.coordinator = coordinator;
        this.tradingEngine = tradingEngine;
        this.collectorService = collectorService;
        this.paperRepository = paperRepository;
        this.dashboardTemplate = dashboardTemplate;
    }

    /**
     * Renders the unified HTML dashboard combining Strategy mode controls, Data Collector status,
     * active positions, Paper Trading metrics/trades, and 4-core / WebSocket telemetry.
     *
     * @return rendered {@link TemplateInstance}
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance getDashboard() {
        StatusDto status = coordinator.getStatus();
        List<TradeRecord> reversedTrades = new ArrayList<>(paperRepository.getTrades());
        Collections.reverse(reversedTrades);

        return dashboardTemplate
                .data("status", status)
                .data("activeMarketLabel", status.activeMarketSlug().isEmpty() ? "DISCOVERING" : status.activeMarketSlug())
                .data("isLive", status.strategyMode() == StrategyMode.LIVE)
                .data("isPaper", status.strategyMode() == StrategyMode.PAPER)
                .data("isOff", status.strategyMode() == StrategyMode.OFF)
                .data("trades", reversedTrades);
    }

    /**
     * Returns the composite {@link StatusDto} snapshot as JSON.
     *
     * @return HTTP 200 with {@link StatusDto}
     */
    @GET
    @Path("/api/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStatus() {
        return Response.ok(coordinator.getStatus()).build();
    }

    /**
     * Evaluates operational health across Data Collector and Strategy Engine.
     * Returns HTTP 200 when healthy or HTTP 503 when degraded so deployment scripts can trigger rollback.
     *
     * @return HTTP 200 or 503 with {@link HealthDto}
     */
    @GET
    @Path("/api/health")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHealth() {
        HealthDto health = coordinator.evaluateHealth();
        Response.Status httpStatus = health.healthy()
                ? Response.Status.OK
                : Response.Status.SERVICE_UNAVAILABLE;
        return Response.status(httpStatus).entity(health).build();
    }

    /**
     * Switches the runtime {@link StrategyMode} ({@code LIVE}, {@code PAPER}, or {@code OFF}).
     *
     * @param request mode switch request payload
     * @return HTTP 200 with updated {@link StatusDto}, or HTTP 400 on invalid mode
     */
    @POST
    @Path("/api/mode")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response switchMode(ModeSwitchRequestDto request) {
        if (request == null || request.mode() == null || request.mode().isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Missing 'mode' field (expected LIVE, PAPER, or OFF)"))
                    .build();
        }
        final StrategyMode targetMode;
        try {
            targetMode = StrategyMode.parse(request.mode());
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
        try {
            StatusDto updated = coordinator.switchStrategyMode(targetMode);
            return Response.ok(updated).build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    /**
     * Returns the current OS, JVM ZGC, 4-core thread utilization, order execution, and WebSocket stream telemetry.
     *
     * @return HTTP 200 with {@link TelemetryDto}
     */
    @GET
    @Path("/api/telemetry")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTelemetry() {
        return Response.ok(tradingEngine.getTelemetry()).build();
    }

    /**
     * Returns the list of currently open positions in the active candle.
     *
     * @return HTTP 200 with {@code List<Position>}
     */
    @GET
    @Path("/api/positions")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPositions() {
        return Response.ok(tradingEngine.getOpenPositions()).build();
    }

    /**
     * Returns aggregated Paper Trading performance metrics.
     *
     * @return HTTP 200 with {@code PerformanceMetrics}
     */
    @GET
    @Path("/api/paper/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPaperMetrics() {
        return Response.ok(paperRepository.getMetrics()).build();
    }

    /**
     * Returns recorded Paper Trading execution history.
     *
     * @return HTTP 200 with {@code List<TradeRecord>}
     */
    @GET
    @Path("/api/paper/trades")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPaperTrades() {
        return Response.ok(paperRepository.getTrades()).build();
    }

    /**
     * Resets engine state (and Paper Trading history when in {@code PAPER} mode or {@code resetPaper=true}).
     *
     * @param resetPaper optional flag forcing paper history reset regardless of current mode
     * @return HTTP 200 confirmation payload
     */
    @POST
    @Path("/api/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset(@QueryParam("paper") Boolean resetPaper) {
        boolean shouldResetPaper = Boolean.TRUE.equals(resetPaper)
                || tradingEngine.getStrategyMode() == StrategyMode.PAPER;
        coordinator.reset(shouldResetPaper);
        return Response.ok(Map.of(
                "status", "SUCCESS",
                "strategyMode", tradingEngine.getStrategyMode().name(),
                "paperHistoryReset", shouldResetPaper,
                "initialCapital", tradingEngine.getInitialCapital(),
                "currentBalance", tradingEngine.getCurrentBalance()
        )).build();
    }

    /**
     * Returns the operational status of the Data Collector subsystem.
     *
     * @return HTTP 200 when healthy or HTTP 503 when unhealthy with {@link CollectorStatusDto}
     */
    @GET
    @Path("/api/collector/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCollectorStatus() {
        CollectorStatusDto collectorStatus = collectorService.getStatus();
        Response.Status httpStatus = collectorStatus.healthy()
                ? Response.Status.OK
                : Response.Status.SERVICE_UNAVAILABLE;
        return Response.status(httpStatus).entity(collectorStatus).build();
    }
}
