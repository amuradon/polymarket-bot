package cz.polymarket.bot.paper.engine;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.paper.storage.PaperTradeRepository;
import cz.polymarket.bot.service.TwapEngine;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategy;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Paper trading execution engine coordinating virtual order fills with simulated latency,
 * position management, candle settlement, and persistence.
 */
@ApplicationScoped
public class PaperTradingEngine implements StrategyContext, ExecutionRouter {

    private static final Logger LOG = Logger.getLogger(PaperTradingEngine.class);
    private static final double TAKER_FEE_RATE = 0.07;

    private final StrategyRegistry strategyRegistry;
    private final PaperTradeRepository repository;
    private final TwapEngine twapEngine;
    private final HourlyPriceCache priceCache;
    private final String strategyName;
    private final long orderLatencyMs;
    private final Vertx vertx;

    private final TradingStrategy activeStrategy;
    private final AtomicReference<PaperPosition> activePosition = new AtomicReference<>();

    private final AtomicLong activeCandleStart = new AtomicLong(0);
    private final AtomicLong activeCandleEnd = new AtomicLong(0);
    private volatile double twapOpenPrice = 0.0;
    private volatile double currentSpotPrice = 0.0;
    private volatile long currentTimestampSec = 0;
    private volatile OrderBookQuote latestQuote = null;

    @Inject
    public PaperTradingEngine(
            StrategyRegistry strategyRegistry,
            PaperTradeRepository repository,
            TwapEngine twapEngine,
            HourlyPriceCache priceCache,
            @ConfigProperty(name = "polymarket.strategy.name", defaultValue = "TWAPArbitrageStrategy") String strategyName,
            @ConfigProperty(name = "polymarket.paper.order-latency-ms", defaultValue = "50") long orderLatencyMs,
            Vertx vertx) {
        if (strategyRegistry == null) {
            throw new IllegalArgumentException("strategyRegistry cannot be null");
        }
        if (repository == null) {
            throw new IllegalArgumentException("repository cannot be null");
        }
        if (strategyName == null || strategyName.isBlank()) {
            throw new IllegalArgumentException("strategyName cannot be null or blank");
        }

        this.strategyRegistry = strategyRegistry;
        this.repository = repository;
        this.twapEngine = twapEngine;
        this.priceCache = priceCache != null ? priceCache : new HourlyPriceCache(3600L);
        this.strategyName = strategyName;
        this.orderLatencyMs = Math.max(0, orderLatencyMs);
        this.vertx = vertx;

        this.activeStrategy = this.strategyRegistry.getStrategy(this.strategyName);
        this.activeStrategy.init(this);
        LOG.infof("Initialized PaperTradingEngine with strategy '%s' (orderLatencyMs: %d ms)",
                this.activeStrategy.getName(), this.orderLatencyMs);
    }

    void onStart(@Observes StartupEvent ev) {
        if (twapEngine != null) {
            LOG.infof("Registering PaperTradingEngine listener with TwapEngine...");
            twapEngine.registerListener(this::onTwapUpdate);
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (twapEngine != null) {
            twapEngine.removeListener(this::onTwapUpdate);
        }
    }

    @Override
    public ExecutionRouter getExecutionRouter() {
        return this;
    }

    @Override
    public HourlyPriceCache getPriceCache() {
        return priceCache;
    }

    @Override
    public void submitOrder(OrderCommand command) {
        if (command == null) {
            return;
        }

        if (orderLatencyMs > 0 && vertx != null) {
            vertx.setTimer(orderLatencyMs, id -> executeOrder(command));
        } else {
            executeOrder(command);
        }
    }

    @Override
    public void cancelOrder(String clientOrderId) {
        if (clientOrderId == null) {
            return;
        }
        ExecutionReport report = new ExecutionReport(
                clientOrderId,
                "sim-cancel-" + clientOrderId,
                "CANCELLED",
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                System.currentTimeMillis(),
                "Order cancelled in paper engine"
        );
        activeStrategy.onExecutionReport(report);
    }

    private synchronized void executeOrder(OrderCommand command) {
        if ("BUY".equalsIgnoreCase(command.side())) {
            executeBuyOrder(command);
        } else if ("SELL".equalsIgnoreCase(command.side())) {
            executeSellOrder(command);
        }
    }

    private void executeBuyOrder(OrderCommand command) {
        if (activePosition.get() != null) {
            LOG.warnf("Ignoring BUY order %s because position already exists", command.clientOrderId());
            return;
        }

        double requestedPrice = command.price().doubleValue();
        double shares = command.size().doubleValue();
        double fillPrice = requestedPrice;

        // If order book quote is available, ensure fill reflects ask side after latency
        if (latestQuote != null) {
            boolean isDown = command.token().toLowerCase().contains("down");
            double bestAsk = isDown ? latestQuote.bestAskDown() : latestQuote.bestAskUp();
            if (bestAsk > 0) {
                fillPrice = Math.max(requestedPrice, bestAsk);
            }
        }

        double sizeUsd = fillPrice * shares;
        if (repository.getCurrentBalance() < sizeUsd) {
            LOG.warnf("Insufficient virtual balance ($%.2f) for trade size $%.2f",
                    repository.getCurrentBalance(), sizeUsd);
            return;
        }

        double entryFee = shares * TAKER_FEE_RATE * fillPrice * (1.0 - fillPrice);
        TradeDirection side = command.token().toLowerCase().contains("down")
                ? TradeDirection.DOWN
                : TradeDirection.UP;

        double distTwap = currentSpotPrice - twapOpenPrice;
        double modelProbability = Math.abs(distTwap) > 80.0 ? 0.94 : Math.min(0.95, fillPrice + 0.10);
        double edge = Math.max(0.0, modelProbability - fillPrice);

        PaperPosition pos = new PaperPosition(
                command.clientOrderId(),
                side,
                fillPrice,
                shares,
                sizeUsd,
                entryFee,
                System.currentTimeMillis(),
                activeCandleStart.get(),
                command.token(),
                modelProbability,
                fillPrice,
                edge,
                fillPrice,
                false
        );
        activePosition.set(pos);

        ExecutionReport report = new ExecutionReport(
                command.clientOrderId(),
                "paper-fill-" + UUID.randomUUID(),
                "FILLED",
                BigDecimal.valueOf(fillPrice),
                BigDecimal.valueOf(shares),
                System.currentTimeMillis(),
                "Virtual fill after " + orderLatencyMs + "ms latency"
        );
        activeStrategy.onExecutionReport(report);
        LOG.infof("Opened paper position: %s %s @ %.4f ($%.2f, fee: $%.4f)",
                command.clientOrderId(), side, fillPrice, sizeUsd, entryFee);
    }

    private void executeSellOrder(OrderCommand command) {
        PaperPosition pos = activePosition.get();
        if (pos == null) {
            LOG.warnf("Ignoring SELL order %s because no active position exists", command.clientOrderId());
            return;
        }

        double fillPrice = command.price().doubleValue();
        if (latestQuote != null) {
            double bestBid = (pos.side() == TradeDirection.DOWN) ? latestQuote.bestBidDown() : latestQuote.bestBidUp();
            if (bestBid > 0) {
                fillPrice = Math.min(fillPrice, bestBid);
            }
        }

        double exitFee = (fillPrice > 0.0 && fillPrice < 1.0)
                ? (pos.shares() * TAKER_FEE_RATE * fillPrice * (1.0 - fillPrice))
                : 0.0;

        String exitReason;
        if (fillPrice >= 0.70) {
            exitReason = "Take Profit (0.70)";
        } else if (pos.trailingStopArmed()) {
            exitReason = "Trailing Stop (+0.05)";
        } else {
            exitReason = String.format(Locale.ROOT, "Early Exit (%.2f)", fillPrice);
        }

        completeTrade(pos, fillPrice, exitFee, exitReason);

        ExecutionReport report = new ExecutionReport(
                command.clientOrderId(),
                "paper-exit-" + UUID.randomUUID(),
                "FILLED",
                BigDecimal.valueOf(fillPrice),
                BigDecimal.valueOf(pos.shares()),
                System.currentTimeMillis(),
                "Virtual exit fill after " + orderLatencyMs + "ms latency"
        );
        activeStrategy.onExecutionReport(report);
    }

    public synchronized void resolveCandle(TradeDirection actualOutcome, double finalPrice) {
        PaperPosition pos = activePosition.get();
        if (pos != null) {
            activeStrategy.onCandleResolution(actualOutcome);

            // If not exited by strategy during resolution call, settle at binary outcome
            if (activePosition.get() != null) {
                double exitPrice = (actualOutcome == pos.side()) ? 1.00 : 0.00;
                String exitReason = "Resolution (TWAP 60s)";
                completeTrade(pos, exitPrice, 0.0, exitReason);
            }
        }

        long start = activeCandleStart.get() > 0 ? activeCandleStart.get() : (currentTimestampSec > 0 ? currentTimestampSec - 900 : 1700000000L);
        long end = activeCandleEnd.get() > start ? activeCandleEnd.get() : (start + 900);
        double open = twapOpenPrice > 0 ? twapOpenPrice : finalPrice;

        MarketCandle candle = new MarketCandle(
                start,
                end,
                open,
                Math.max(open, finalPrice),
                Math.min(open, finalPrice),
                finalPrice,
                0.0, 0.0, 0.0,
                open,
                Math.max(open, finalPrice),
                Math.min(open, finalPrice),
                finalPrice,
                0.0, 0.0, 0.0,
                0.0
        );
        activeStrategy.onMarketCandleCompleted(candle);
    }

    private void completeTrade(PaperPosition pos, double exitPrice, double exitFee, String exitReason) {
        double payout = pos.shares() * exitPrice;
        double totalFee = pos.entryFee() + exitFee;
        double netPnl = payout - pos.sizeUsd() - totalFee;
        double newBalance = repository.getCurrentBalance() + netPnl;
        boolean isWin = netPnl > 0.0;

        TradeRecord trade = new TradeRecord(
                Instant.now().toString(),
                pos.intervalStartSec(),
                pos.side(),
                pos.entryPrice(),
                exitPrice,
                pos.sizeUsd(),
                pos.shares(),
                totalFee,
                netPnl,
                exitReason,
                pos.modelProbability(),
                pos.marketPrice(),
                pos.edge(),
                isWin,
                newBalance
        );

        repository.recordTrade(trade);
        activePosition.set(null);
        LOG.infof("Closed paper trade: %s @ %.4f -> Net PnL: $%.2f (%s), Balance: $%.2f",
                pos.side(), exitPrice, netPnl, exitReason, newBalance);
    }

    public synchronized void onTwapUpdate(TwapUpdate update) {
        if (update == null) {
            return;
        }

        long start = update.candleStart();
        if (start != activeCandleStart.get()) {
            if (activeCandleStart.get() > 0 && currentSpotPrice > 0) {
                TradeDirection outcome = (currentSpotPrice >= twapOpenPrice) ? TradeDirection.UP : TradeDirection.DOWN;
                resolveCandle(outcome, currentSpotPrice);
            }

            activeCandleStart.set(start);
            activeCandleEnd.set(update.candleEnd());
            twapOpenPrice = update.openPrice().doubleValue();
        }

        if (update.point() != null) {
            currentTimestampSec = update.point().time();
            if (update.point().medianPrice() != null) {
                currentSpotPrice = update.point().medianPrice().doubleValue();
            }
        }

        activeStrategy.onTwapUpdate(update);

        // Synthesize realistic OrderBookQuote from current spot and twap open if quote feed is missing
        if (twapOpenPrice > 0 && currentSpotPrice > 0) {
            double delta = currentSpotPrice - twapOpenPrice;
            double pUp = Math.min(0.95, Math.max(0.05, 0.50 + (delta / 200.0)));
            double pDown = 1.0 - pUp;
            OrderBookQuote synthQuote = new OrderBookQuote(
                    Math.min(0.99, pUp + 0.01),
                    Math.max(0.01, pUp - 0.01),
                    Math.min(0.99, pDown + 0.01),
                    Math.max(0.01, pDown - 0.01),
                    5000.0, 5000.0,
                    pUp, pDown,
                    System.currentTimeMillis()
            );
            onOrderBookQuote(synthQuote);
        }
    }

    public synchronized void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        this.latestQuote = quote;

        PaperPosition pos = activePosition.get();
        if (pos != null) {
            double currentBid = (pos.side() == TradeDirection.UP) ? quote.bestBidUp() : quote.bestBidDown();
            boolean armed = currentBid >= (pos.entryPrice() + 0.05);
            activePosition.set(pos.withPriceObservation(currentBid, armed));
        }

        activeStrategy.onOrderBookQuote(quote);
    }

    public String getActiveStrategyName() {
        return activeStrategy.getName();
    }

    public long getOrderLatencyMs() {
        return orderLatencyMs;
    }

    public PaperPosition getActivePosition() {
        return activePosition.get();
    }

    public String getStatus() {
        return "RUNNING";
    }

    public double getTwapOpenPrice() {
        return twapOpenPrice;
    }

    public double getCurrentSpotPrice() {
        return currentSpotPrice;
    }
}
