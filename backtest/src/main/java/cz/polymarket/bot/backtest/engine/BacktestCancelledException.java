package cz.polymarket.bot.backtest.engine;

/**
 * Exception thrown when an active backtest simulation run is cooperatively cancelled by the user.
 */
public class BacktestCancelledException extends RuntimeException {

    /**
     * Constructs a new {@link BacktestCancelledException} with the given message.
     *
     * @param message descriptive cancellation reason
     */
    public BacktestCancelledException(String message) {
        super(message);
    }
}
