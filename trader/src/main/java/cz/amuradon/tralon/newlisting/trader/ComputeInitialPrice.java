package cz.amuradon.tralon.newlisting.trader;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import cz.amuradon.tralon.connector.mexc.OrderBook;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ComputeInitialPrice {

	public static final String BUY_ORDER_LIMIT_PRICE_PROP_NAME = "buyOrder.price";

	public static final String BEAN_NAME = "computeInitialPrice";
	
	private final String buyOrderPriceProperty;
	
	@Inject
	public ComputeInitialPrice(@ConfigProperty(name = BUY_ORDER_LIMIT_PRICE_PROP_NAME) final String buyOrderPriceProperty) {
		this.buyOrderPriceProperty = buyOrderPriceProperty;
	}
	
	public BigDecimal execute(String symbol,
			int priceScale,
			OrderBook orderBook) {
		if (buyOrderPriceProperty.startsWith("slippage")) {
			String slippage = extractValue(buyOrderPriceProperty);
	
			List<List<BigDecimal>> asks = orderBook.asks();
			BigDecimal priceSum = BigDecimal.ZERO;
			BigDecimal volumeSum = BigDecimal.ZERO;
	
			if (!asks.isEmpty()) {
				BigDecimal prevPrice = null;
				
				for (List<BigDecimal> ask: asks) {
					BigDecimal price = ask.get(0);
					BigDecimal volume = ask.get(1);
					
					if (prevPrice != null && getPercentDiff(prevPrice, price).compareTo(new BigDecimal(10)) < 0) {
						break;
					} else {
						priceSum = priceSum.add(price.multiply(volume));
						volumeSum = volumeSum.add(volume);
						prevPrice = price;
					}
				}
				
				BigDecimal slip = new BigDecimal(slippage)
						.divide(new BigDecimal(100), 4, RoundingMode.HALF_UP).add(BigDecimal.ONE);
				return priceSum.divide(volumeSum, 10, RoundingMode.HALF_UP)
						.multiply(slip).setScale(priceScale, RoundingMode.HALF_UP);
			} else {
				return BigDecimal.ZERO;
			}
		} else if (buyOrderPriceProperty.startsWith("manual")) {
			return new BigDecimal(extractValue(buyOrderPriceProperty));
		} else {
			if (!buyOrderPriceProperty.equalsIgnoreCase("auto")) {
				Log.errorf("The property '%s' has invalid value '%s'. Defaulting to 'auto'",
						BUY_ORDER_LIMIT_PRICE_PROP_NAME, buyOrderPriceProperty);
			}
			// TODO auto-computation
			return BigDecimal.ZERO;
		}
	}

	private String extractValue(String property) {
		return property.substring(property.indexOf(":") + 1);
	}

	private BigDecimal getPercentDiff(BigDecimal number, BigDecimal nextNumber) {
		return nextNumber.subtract(number).divide(number, 10, RoundingMode.HALF_UP).multiply(new BigDecimal(100));
	}
}
