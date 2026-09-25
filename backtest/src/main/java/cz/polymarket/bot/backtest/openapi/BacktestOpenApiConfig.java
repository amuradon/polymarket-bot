package cz.polymarket.bot.backtest.openapi;

import jakarta.ws.rs.core.Application;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.info.Contact;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * OpenAPI 3.0 specification definition and global tags for the Backtest application.
 */
@OpenAPIDefinition(
        info = @Info(
                title = "Polymarket Bot - Backtest & Data Ingestion API",
                version = "1.0.0",
                description = "REST API for the backtesting simulation engine and automated Binance market data ingestion (spot aggTrades, futures aggTrades, futures orderbook).",
                contact = @Contact(name = "Polymarket Bot Engineering Team")
        ),
        tags = {
                @Tag(name = "Data Ingestion", description = "Operations for triggering and monitoring Binance historical data downloads"),
                @Tag(name = "Backtest Engine", description = "Backtest simulation engine status and lifecycle controls")
        }
)
public class BacktestOpenApiConfig extends Application {
}
