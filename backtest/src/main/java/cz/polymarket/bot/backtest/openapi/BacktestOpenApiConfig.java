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
                title = "Polymarket Bot - Backtest API",
                version = "1.0.0",
                description = "REST API for the backtesting simulation engine.",
                contact = @Contact(name = "Polymarket Bot Engineering Team")
        ),
        tags = {
                @Tag(name = "Backtest Engine", description = "Backtest simulation engine status and lifecycle controls")
        }
)
public class BacktestOpenApiConfig extends Application {
}
