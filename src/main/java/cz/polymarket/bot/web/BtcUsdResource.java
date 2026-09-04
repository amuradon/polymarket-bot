package cz.polymarket.bot.web;

import cz.polymarket.bot.config.ChartRangeConfig;
import cz.polymarket.bot.domain.CandleTwapState;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.service.TwapEngine;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

@Path("/")
@ApplicationScoped
public class BtcUsdResource {

    private final TwapEngine twapEngine;
    private final Template btcUsdTemplate;
    private final ChartRangeConfig chartRangeConfig;

    @Inject
    public BtcUsdResource(
            TwapEngine twapEngine,
            @Location("btc-usd.html") Template btcUsdTemplate,
            ChartRangeConfig chartRangeConfig) {
        this.twapEngine = twapEngine;
        this.btcUsdTemplate = btcUsdTemplate;
        this.chartRangeConfig = chartRangeConfig;
    }

    @GET
    @Path("/btc-usd")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance getConsolePage(@QueryParam("timeframe") @DefaultValue("5m") String timeframe) {
        return btcUsdTemplate
                .data("selectedTimeframe", timeframe)
                .data("yRange5m", chartRangeConfig.getYRange("btc-usd", Timeframe.FIVE_MINUTES))
                .data("yRange15m", chartRangeConfig.getYRange("btc-usd", Timeframe.FIFTEEN_MINUTES));
    }

    @GET
    @Path("/api/twap")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTwapData(@QueryParam("timeframe") @DefaultValue("5m") String timeframeCode) {
        try {
            Timeframe timeframe = Timeframe.fromCode(timeframeCode);
            CandleTwapState state = twapEngine.getCurrentCandleState(timeframe);
            return Response.ok(state).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }
}
