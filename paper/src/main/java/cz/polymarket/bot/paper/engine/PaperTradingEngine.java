package cz.polymarket.bot.paper.engine;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamListener;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
import cz.polymarket.bot.connector.kraken.KrakenStreamListener;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.paper.storage.PaperTradeRepository;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import cz.polymarket.bot.trading.stream.CalculatedSpotStreamProcessor;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Paper trading execution engine coordinating virtual order fills with simulated latency,
 * multi-exchange spot stream ingestion ({@link CalculatedSpotStreamProcessor}), position management,
 * candle settlement, and persistence.
 */
@ApplicationScoped
public class PaperTradingEngine implements StrategyContext, ExecutionRouter {

    private static final double TAKER_FEE_RATE = 0.07;

    private final StrategyRegistry strategyRegistry;
    private final PaperTradeRepository repository;
    private final BinanceStreamConnector binanceConnector;
    private final KrakenStreamConnector krakenConnector;
    private final CoinbaseStreamConnector coinbaseConnector;
    private final CalculatedSpotStreamProcessor spotStreamProcessor;
    private final String strategyName;
    private final String symbol;
    private final long orderLatencyMs;
    private final boolean streamsEnabled;
    private final Vertx vertx;

    private final TradingStrategy activeStrategy;
    private final AtomicReference<PaperPosition> activePosition = new AtomicReference<>();
    private final List<PaperPosition> openPositions = new java.util.ArrayList<>();

    private final AtomicLong activeCandleStart = new AtomicLong(0);
    private final AtomicLong activeCandleEnd = new AtomicLong(0);
    private volatile double twapOpenPrice = 0.0;
    private volatile double currentSpotPrice = 0.0;
    private volatile long currentTimestampSec = 0;
    private volatile OrderBookQuote latestQuote = null;

    /**
     * Constructs the Paper Trading Engine.
     *
     * @param strategyRegistry    registry of available trading strategies
     * @param repository          persistent paper trade repository
     * @param binanceConnector    Binance Spot {@code aggTrades} WebSocket connector
     * @param krakenConnector     Kraken Spot {@code trade} WebSocket connector ({@code USDTUSD} + {@code <SYMBOL>})
     * @param coinbaseConnector   Coinbase Spot {@code matches} WebSocket connector
     * @param spotStreamProcessor CDI processor parsing Binance, Kraken, and Coinbase spot trades into {@code CalculatedSpotTwapEngine}
     * @param strategyName        configured strategy name (defaults to {@code UtterParameciumStrategy})
     * @param symbol              trading asset symbol (defaults to {@code BTCUSDT})
     * @param orderLatencyMs      simulated order execution latency in milliseconds
     * @param streamsEnabled      whether to auto-connect live WebSocket spot streams on startup
     * @param vertx               Vert.x instance for simulated latency timers
     */
    @Inject
    public PaperTradingEngine(
            StrategyRegistry strategyRegistry,
            PaperTradeRepository repository,
            BinanceStreamConnector binanceConnector,
            KrakenStreamConnector krakenConnector,
            CoinbaseStreamConnector coinbaseConnector,
            CalculatedSpotStreamProcessor spotStreamProcessor,
            @ConfigProperty(name = "polymarket.strategy.name", defaultValue = "UtterParameciumStrategy") String strategyName,
            @ConfigProperty(name = "polymarket.paper.symbol", defaultValue = "BTCUSDT") String symbol,
            @ConfigProperty(name = "polymarket.paper.order-latency-ms", defaultValue = "50") long orderLatencyMs,
            @ConfigProperty(name = "polymarket.paper.streams-enabled", defaultValue = "true") boolean streamsEnabled,
            Vertx vertx) {
        if (strategyRegistry == null) {
            throw new IllegalArgumentException("strategyRegistry cannot be null");
        }
        if (repository == null) {
            throw new IllegalArgumentException("repository cannot be null");
        }
        if (binanceConnector == null) {
            throw new IllegalArgumentException("binanceConnector cannot be null");
        }
        if (krakenConnector == null) {
            throw new IllegalArgumentException("krakenConnector cannot be null");
        }
        if (coinbaseConnector == null) {
            throw new IllegalArgumentException("coinbaseConnector cannot be null");
        }
        if (spotStreamProcessor == null) {
            throw new IllegalArgumentException("spotStreamProcessor cannot be null");
        }
        if (strategyName == null || strategyName.isBlank()) {
            throw new IllegalArgumentException("strategyName cannot be null or blank");
        }

        this.strategyRegistry = strategyRegistry;
        this.repository = repository;
        this.binanceConnector = binanceConnector;
        this.krakenConnector = krakenConnector;
        this.coinbaseConnector = coinbaseConnector;
        this.spotStreamProcessor = spotStreamProcessor;
        this.strategyName = strategyName;
        this.symbol = (symbol != null && !symbol.isBlank()) ? symbol.trim() : "BTCUSDT";
        this.orderLatencyMs = Math.max(0, orderLatencyMs);
        this.streamsEnabled = streamsEnabled;
        this.vertx = vertx;

        this.activeStrategy = this.strategyRegistry.getStrategy(this.strategyName);
        this.activeStrategy.init(this);
        Log.infof("Initialized PaperTradingEngine with strategy '%s' (symbol=%s, orderLatencyMs=%d ms, streamsEnabled=%b)",
                this.activeStrategy.getName(), this.symbol, this.orderLatencyMs, this.streamsEnabled);
    }

    /**
     * Starts live spot WebSocket streams when the application starts if {@code streamsEnabled} is {@code true}.
     *
     * @param event Quarkus startup event
     */
    public void onStart(@Observes StartupEvent event) {
        if (streamsEnabled) {
            startSpotStreams();
        }
    }

    /**
     * Stops active spot WebSocket streams when the application shuts down.
     *
     * @param event Quarkus shutdown event
     */
    public void onStop(@Observes ShutdownEvent event) {
        binanceConnector.stop();
        krakenConnector.stop();
        coinbaseConnector.stop();
    }

    /**
     * Starts Binance Spot ({@code SPOT_AGG_TRADES}), Kraken Spot ({@code USDTUSD} + {@code <SYMBOL>}),
     * and Coinbase Spot ({@code <SYMBOL>}) WebSocket trade streams feeding {@link CalculatedSpotTwapEngine}.
     */
    public synchronized void startSpotStreams() {
        Log.infof("Starting PaperTradingEngine Binance, Kraken, and Coinbase spot WebSocket streams for symbol=%s...", symbol);
        binanceConnector.startSpotAggTradesBatch(symbol, this::onRawBinanceFrameBatch, null);
        krakenConnector.start(symbol, true, new KrakenStreamListener() {
            @Override
            public void onStreamMessage(String normSymbol, String rawJson, long recvTimestampMs) {
                onRawKrakenFrame(normSymbol, rawJson, recvTimestampMs);
            }

            @Override
            public void onStreamFrame(String normSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                onRawKrakenFrameBytes(normSymbol, rawUtf8Bytes, recvTimestampMs);
            }
        });
        coinbaseConnector.start(symbol, new CoinbaseStreamListener() {
            @Override
            public void onStreamMessage(String normSymbol, String rawJson, long recvTimestampMs) {
                onRawCoinbaseFrame(normSymbol, rawJson, recvTimestampMs);
            }

            @Override
            public void onStreamFrame(String normSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                onRawCoinbaseFrameBytes(normSymbol, rawUtf8Bytes, recvTimestampMs);
            }
        });
    }

    @Override
    public ExecutionRouter getExecutionRouter() {
        return this;
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
        double requestedPrice = command.price().doubleValue();
        double shares = command.size().doubleValue();
        double fillPrice = requestedPrice;

        // If order book quote is available, fill at current bestAsk if within limit price, or reject if ask exceeded limit
        if (latestQuote != null) {
            boolean isDown = command.token().toLowerCase().contains("down");
            double bestAsk = isDown ? latestQuote.bestAskDown() : latestQuote.bestAskUp();
            if (bestAsk > 0) {
                if (bestAsk > requestedPrice + 1e-4) {
                    Log.warnf("Paper BUY order %s rejected: bestAsk %.4f exceeds limit price %.4f",
                            command.clientOrderId(), bestAsk, requestedPrice);
                    activeStrategy.onExecutionReport(new ExecutionReport(
                            command.clientOrderId(),
                            "paper-reject-" + UUID.randomUUID(),
                            "REJECTED",
                            BigDecimal.ZERO,
                            BigDecimal.ZERO,
                            System.currentTimeMillis(),
                            "Best ask exceeds limit price"
                    ));
                    return;
                }
                fillPrice = bestAsk;
            }
        }

        double sizeUsd = fillPrice * shares;
        if (repository.getCurrentBalance() < sizeUsd) {
            Log.warnf("Insufficient virtual balance ($%.2f) for trade size $%.2f",
                    repository.getCurrentBalance(), sizeUsd);
            activeStrategy.onExecutionReport(new ExecutionReport(
                    command.clientOrderId(),
                    "paper-reject-" + UUID.randomUUID(),
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    System.currentTimeMillis(),
                    "Insufficient virtual balance"
            ));
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
        openPositions.add(pos);
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
        Log.infof("Opened paper position: %s %s @ %.4f ($%.2f, fee: $%.4f)",
                command.clientOrderId(), side, fillPrice, sizeUsd, entryFee);
    }

    private void executeSellOrder(OrderCommand command) {
        PaperPosition pos = activePosition.get();
        if (pos == null) {
            Log.warnf("Ignoring SELL order %s because no active position exists", command.clientOrderId());
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
        if (!openPositions.isEmpty() || activePosition.get() != null) {
            activeStrategy.onCandleResolution(actualOutcome);

            // Settle all remaining positions at binary outcome
            List<PaperPosition> remaining = new java.util.ArrayList<>(openPositions);
            if (remaining.isEmpty() && activePosition.get() != null) {
                remaining.add(activePosition.get());
            }
            for (PaperPosition pos : remaining) {
                double exitPrice = (actualOutcome == pos.side()) ? 1.00 : 0.00;
                String exitReason = "Resolution (TWAP 60s)";
                completeTrade(pos, exitPrice, 0.0, exitReason);
            }
            openPositions.clear();
            activePosition.set(null);
        }
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
        openPositions.remove(pos);
        activePosition.set(openPositions.isEmpty() ? null : openPositions.getLast());
        Log.infof("Closed paper trade: %s @ %.4f -> Net PnL: $%.2f (%s), Balance: $%.2f",
                pos.side(), exitPrice, netPnl, exitReason, newBalance);
    }

    /**
     * Forwards a PolyBolt {@code price.crypto.twap} (60s) update to the active strategy and updates
     * the explicit target price for the current candle interval.
     */
    public synchronized void onPolymarketCryptoTwap60s(
            long timestampMs,
            String symbol,
            String source,
            double twapPrice,
            int windowSeconds,
            long twapTimestampMs) {
        if (twapPrice <= 0.0 || timestampMs <= 0L) {
            return;
        }
        long effectiveTsMs = twapTimestampMs > 0L ? twapTimestampMs : timestampMs;
        long durationSec = activeStrategy.getTimeframe() != null
                ? activeStrategy.getTimeframe().getDurationSeconds()
                : 900L;
        long candleStartSec = (effectiveTsMs / 1000L / durationSec) * durationSec;
        long currentActiveStart = activeCandleStart.get();
        if (currentActiveStart > 0L && candleStartSec < currentActiveStart) {
            activeStrategy.onPolymarketCryptoTwap60s(timestampMs, symbol, source, twapPrice, windowSeconds, twapTimestampMs);
            return;
        }

        if (currentActiveStart > 0L && candleStartSec > currentActiveStart) {
            if (twapOpenPrice > 0.0) {
                TradeDirection outcome = (twapPrice >= twapOpenPrice) ? TradeDirection.UP : TradeDirection.DOWN;
                resolveCandle(outcome, twapPrice);
            } else {
                Log.warnf("Skipping candle resolution for interval %d: target price was missing from PolyBolt 60s TWAP stream",
                        currentActiveStart);
                openPositions.clear();
                activePosition.set(null);
            }
            activeCandleStart.set(candleStartSec);
            activeCandleEnd.set(candleStartSec + durationSec);
            twapOpenPrice = (Math.abs(effectiveTsMs - candleStartSec * 1000L) <= 10_000L) ? twapPrice : 0.0;
        } else if (currentActiveStart == 0L) {
            activeCandleStart.set(candleStartSec);
            activeCandleEnd.set(candleStartSec + durationSec);
            twapOpenPrice = (Math.abs(effectiveTsMs - candleStartSec * 1000L) <= 10_000L) ? twapPrice : 0.0;
        } else if (twapOpenPrice <= 0.0 && Math.abs(effectiveTsMs - candleStartSec * 1000L) <= 10_000L) {
            twapOpenPrice = twapPrice;
        }

        activeStrategy.onPolymarketCryptoTwap60s(timestampMs, symbol, source, twapPrice, windowSeconds, twapTimestampMs);
    }

    /**
     * Forwards a PolyBolt {@code price.crypto} update to the active strategy and tracks current spot price.
     */
    public synchronized void onPolymarketCryptoPrice(
            long timestampMs,
            String symbol,
            String source,
            double price,
            long priceTimestampMs) {
        if (price <= 0.0 || timestampMs <= 0L) {
            return;
        }
        currentTimestampSec = timestampMs / 1000L;
        currentSpotPrice = price;
        if (activeCandleStart.get() == 0) {
            long durationSec = activeStrategy.getTimeframe() != null
                    ? activeStrategy.getTimeframe().getDurationSeconds()
                    : 900L;
            long candleStartSec = (currentTimestampSec / durationSec) * durationSec;
            activeCandleStart.set(candleStartSec);
            activeCandleEnd.set(candleStartSec + durationSec);
        }

        activeStrategy.onPolymarketCryptoPrice(timestampMs, symbol, source, price, priceTimestampMs);
    }

    /**
     * Processes a batch of forward-progressing Binance Spot {@code aggTrades} frames into
     * {@link CalculatedSpotTwapEngine} and forwards the final spot/TWAP state to the active strategy.
     *
     * @param batch chronological batch of Binance {@code aggTrade} frames
     */
    public synchronized void onRawBinanceFrameBatch(List<TimestampedFrame> batch) {
        spotStreamProcessor.processBinanceFrameBatch(batch, activeStrategy);
    }

    /**
     * Processes a raw JSON text frame from the Kraken Spot v2 {@code trade} WebSocket stream
     * ({@code USDTUSD} or {@code <SYMBOL>}), updating {@link CalculatedSpotTwapEngine} and forwarding
     * the calculated USD spot and 60s TWAP to the active strategy.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawJson         raw Kraken JSON frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public synchronized void onRawKrakenFrame(String streamSymbol, String rawJson, long recvTimestampMs) {
        spotStreamProcessor.processKrakenFrame(streamSymbol, rawJson, recvTimestampMs, activeStrategy);
    }

    /**
     * Processes a raw UTF-8 {@code byte[]} frame from the Kraken Spot v2 {@code trade} WebSocket stream
     * ({@code USDTUSD} or {@code <SYMBOL>}) without intermediate {@link String} allocation.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public synchronized void onRawKrakenFrameBytes(String streamSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
        spotStreamProcessor.processKrakenFrameBytes(streamSymbol, rawUtf8Bytes, recvTimestampMs, activeStrategy);
    }

    /**
     * Processes a raw JSON text frame from the Coinbase Spot {@code matches} WebSocket stream,
     * updating {@link CalculatedSpotTwapEngine} and forwarding the calculated USD spot and 60s TWAP
     * to the active strategy.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawJson         raw Coinbase JSON frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public synchronized void onRawCoinbaseFrame(String streamSymbol, String rawJson, long recvTimestampMs) {
        spotStreamProcessor.processCoinbaseFrame(streamSymbol, rawJson, recvTimestampMs, activeStrategy);
    }

    /**
     * Processes a raw UTF-8 {@code byte[]} frame from the Coinbase Spot {@code matches} WebSocket stream
     * without intermediate {@link String} allocation.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public synchronized void onRawCoinbaseFrameBytes(String streamSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
        spotStreamProcessor.processCoinbaseFrameBytes(streamSymbol, rawUtf8Bytes, recvTimestampMs, activeStrategy);
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

    public synchronized void reset() {
        openPositions.clear();
        activePosition.set(null);
        activeStrategy.reset();
        spotStreamProcessor.reset();
        activeCandleStart.set(0);
        activeCandleEnd.set(0);
        twapOpenPrice = 0.0;
        currentSpotPrice = 0.0;
        currentTimestampSec = 0;
        latestQuote = null;
        Log.info("Reset PaperTradingEngine internal state");
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

    /**
     * Returns an unmodifiable snapshot of all currently open batch positions in the active candle.
     */
    public List<PaperPosition> getOpenPositions() {
        return List.copyOf(openPositions);
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
