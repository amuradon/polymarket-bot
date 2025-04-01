package cz.amuradon.tralon.newlisting.trader;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cz.amuradon.tralon.connector.mexc.ListenKey;
import cz.amuradon.tralon.connector.mexc.MexcClient;
import cz.amuradon.tralon.connector.mexc.MexcWsClient;
import cz.amuradon.tralon.connector.mexc.OrderBook;
import cz.amuradon.tralon.connector.mexc.OrderResponse;
import cz.amuradon.tralon.connector.mexc.Side;
import cz.amuradon.tralon.newlisting.json.ExchangeInfo;
import cz.amuradon.tralon.newlisting.json.SymbolInfo;
import cz.amuradon.tralon.newlisting.trader.RequestBuilder.NewOrderRequestBuilder;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class TraderAgent {
	
	private static final String NOT_YET_TRADING_ERR_CODE = "30001";
	
	private static final String ORDER_PRICE_ABOVE_LIMIT_ERR_CODE = "30010";

	private static final String TIME_PROP_NAME = "time";

	private final ScheduledExecutorService scheduler;
	
	private final MexcClient mexcClient;
	
	private final MexcWsClient mexcWsClient;
	
	private final ComputeInitialPrice computeInitialPrice;
	
    private final BigDecimal usdtVolume;
    private final String symbol;
    private final ObjectMapper mapper;
    
    private final int buyOrderRequestsPerSecond;
	private final int buyOrderMaxAttempts;
    
    private final Path dataDir;
    
    private final int listingHour;
    
    private final int listingMinute;
    
    private final DataHolder dataHolder;
    
	private final RequestBuilder requestBuilder; 
	
	@Inject
    public TraderAgent(
    		@RestClient final MexcClient mexcClient,
    		final MexcWsClient mexcWsClient,
    		final ComputeInitialPrice computeInitialPrice,
    		@ConfigProperty(name = "mexc.secretKey") final String secretKey,
    		@ConfigProperty(name = "usdtVolume") final String usdtVolume,
    		@Named(BeanConfig.SYMBOL) final String symbol,
    		@ConfigProperty(name = TIME_PROP_NAME) final String time,
    		@Named(BeanConfig.DATA_DIR) final Path dataDir,
    		@ConfigProperty(name = "buyOrder.requestsPerSecond") final int buyOrderRequestsPerSecond,
    		@ConfigProperty(name = "buyOrder.maxAttempts") final int buyOrderMaxAttempts,
    		final DataHolder dataHolder,
    		final RequestBuilder requestBuilder) {
    	
		scheduler = Executors.newScheduledThreadPool(2);
		this.mexcClient = mexcClient;
		this.mexcWsClient = mexcWsClient;
		this.computeInitialPrice = computeInitialPrice;
    	this.usdtVolume = new BigDecimal(usdtVolume);
    	this.symbol = symbol;
    	this.buyOrderRequestsPerSecond = buyOrderRequestsPerSecond;
    	this.buyOrderMaxAttempts = buyOrderMaxAttempts;
    	this.dataDir = dataDir;
    	this.mapper = new ObjectMapper();
    	this.dataHolder = dataHolder;
    	this.requestBuilder = requestBuilder;
    	
    	String[] timeParts = time.split(":");
    	if (timeParts.length >= 2) {
    		listingHour = Integer.parseInt(timeParts[0]);
    		listingMinute = Integer.parseInt(timeParts[1]);
    	} else {
    		throw new IllegalArgumentException(
    				String.format("The property '%s' has invalid value '%s'. The expected format is HH:mm",
    						TIME_PROP_NAME, time));
    	}
    }
    
	// XXX Temporary testing
	@Startup
    public void run() {
		int startHour = listingHour;
		int startMinute = listingMinute;
    		
		if (startMinute == 0) {
			startHour--;
			startMinute = 59;
		} else {
			startMinute--;
		}

		Log.infof("Listing at %d:%d", listingHour, listingMinute);
		Log.infof("Agent starts at %d:%d", startHour, startMinute);
		
		LocalDateTime beforeStart = LocalDateTime.now().withHour(startHour).withMinute(startMinute).withSecond(50);
		Log.infof("Listing start: %s", beforeStart);
		LocalDateTime now = LocalDateTime.now();
		
		if (now.isAfter(beforeStart)) {
			throw new IllegalStateException(String.format("The start time '%s' is in past", beforeStart));
		}
		
		ScheduledFuture<?> prepareTask = scheduler.schedule(this::prepare, Math.max(0, now.until(beforeStart, ChronoUnit.SECONDS)),
				TimeUnit.SECONDS);
		ScheduledFuture<?> placeNewBuyOrderTask = scheduler.schedule(this::placeNewBuyOrder,
				Math.max(0, now.until(beforeStart.withSecond(59).withNano(980000000), ChronoUnit.MILLIS)),
				TimeUnit.MILLISECONDS);
		
		try {
			prepareTask.get();
			placeNewBuyOrderTask.get();
		} catch (Exception e) {
			throw new RuntimeException("The execution failed", e);
		}
		
	}
    
    // XXX Temporary testing
//    @Startup
	public void prepare() {
		/*
		 * TODO
		 * - When application starts delete all existing listenKeys
		 *   - I can't there might be more agents running at the same time
		 * - Every 60 minutes sent keep alive request
		 * - After 24 hours reconnect - create a new listen key?
		 */
		
		// subscribe updates
		long timestamp = new Date().getTime();
		
		Map<String, String> queryParams = new LinkedHashMap<>();
		queryParams.put("timestamp", String.valueOf(timestamp));
		
		ListenKey listenKey = mexcClient.userDataStream(requestBuilder.signQueryParams(queryParams));
		mexcWsClient.connect(listenKey.listenKey(), symbol);
		
		// get order book
		String exchangeInfoJson = mexcClient.exchangeInfo();
		String orderBookJson= mexcClient.depth(symbol);

		try {
			Files.writeString(dataDir.resolve("exchangeInfo.json"), exchangeInfoJson, StandardOpenOption.CREATE);
			Files.writeString(dataDir.resolve("depth.json"), orderBookJson, StandardOpenOption.CREATE);
		} catch (IOException e) {
			throw new IllegalStateException("Could write JSON to files", e);
		}
		

		try {
			ExchangeInfo exchangeInfo = mapper.readValue(exchangeInfoJson, ExchangeInfo.class);
			OrderBook orderBook = mapper.readValue(orderBookJson, OrderBook.class);
			
			dataHolder.setInitialBuyPrice(computeInitialPrice.execute(symbol, extractPriceScale(symbol, exchangeInfo), orderBook));
			Log.infof("Computed buy limit order price: %s", dataHolder.getInitialBuyPrice());
		} catch (JsonProcessingException e) {
			throw new IllegalStateException("JSON could not be parsed", e);
		}
		
		// XXX temporary testing
//		placeNewBuyOrder();
	}
    
	private int extractPriceScale(String symbol, ExchangeInfo exchangeInfo) {
		int priceScale = 4;
		for (SymbolInfo symbolInfo: exchangeInfo.symbols()) {
			if (symbolInfo.symbol().equalsIgnoreCase(symbol)) {
				priceScale = symbolInfo.quotePrecision();
				break;
			}
		}
		dataHolder.setPriceScale(priceScale);
		return priceScale;
	}
	
	// XXX Opened for component test for now
	public void placeNewBuyOrder() {
		// TODO kdyz price jeste neni nasetovana metodou vyse - muze se stat, je to async
	
		String clientOrderId = symbol + "-" + HexFormat.of().toHexDigits(new Date().getTime());
		Log.infof("Client Order ID: %s", clientOrderId);
		LocalDateTime now = LocalDateTime.now();
		BigDecimal price = dataHolder.getInitialBuyPrice();
		dataHolder.setBuyClientOrderId(clientOrderId);
		long timestamp = LocalDateTime.of(now.getYear(), now.getMonthValue(), now.getDayOfMonth(),
				listingHour, listingMinute)
			.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
		// XXX Temporary testing
//		long timestamp = new Date().getTime();
		
		int recvWindow = 60000;
		NewOrderRequestBuilder newOrderBuilder = requestBuilder.newOrder()
			.clientOrderId(clientOrderId)
			.symbol(symbol)
			.side(Side.BUY)
			.type("LIMIT")
			// FIXME quantity je spocitana na tu max price!!!
			.quantity(usdtVolume.divide(price, 2, RoundingMode.HALF_UP))
			.price(price)
			.recvWindow(recvWindow)
			.timestamp(timestamp)
			.signParams();
		
		// TODO muze byt az sem vsechno udelano dopredu a tady pockat na spravny cas otevreni burzy?

		long previousSendTime = 0;
		long msPerRequest = Math.round(Math.ceil(1000.0 / buyOrderRequestsPerSecond));
		for (int i = 0; i < buyOrderMaxAttempts;) {
			long currentTime = System.currentTimeMillis();
			if (currentTime - timestamp >= recvWindow) {
				timestamp = currentTime;
				newOrderBuilder.timestamp(timestamp).signParams();
			}
			if (currentTime - previousSendTime > msPerRequest) {
				i++;
				previousSendTime = currentTime; 
				Log.infof("Place new buy limit order attempt %d", i);
				try {
					OrderResponse response = newOrderBuilder.send();
					dataHolder.setBuyOrderId(response.orderId());
					Log.infof("New order placed: %s", response);
					break;
				} catch (WebApplicationException e) {
					Response response = e.getResponse();
					ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
					int status = response.getStatus();
					Log.errorf("ERR response: %d - %s: %s, Headers: %s", status,
							response.getStatusInfo().getReasonPhrase(), errorResponse, response.getHeaders());
					if (ORDER_PRICE_ABOVE_LIMIT_ERR_CODE.equalsIgnoreCase(errorResponse.code())) {
						Matcher matcher = Pattern.compile(".*\\s(\\d+(\\.\\d+)?)USDT").matcher(errorResponse.msg());
						if (matcher.find()) {
							String maxPrice = matcher.group(1);
							Log.infof("Resetting max price: '%s'", maxPrice);
							timestamp = currentTime;
							newOrderBuilder.timestamp(timestamp).price(new BigDecimal(maxPrice)).signParams();
						}
					} else if (status == 429) {
						Log.warnf("Retry after: ", response.getHeaderString("Retry-After"));
						// Do nothing, repeat
					} else if (!NOT_YET_TRADING_ERR_CODE.equalsIgnoreCase(errorResponse.code())) {
						Log.infof("It is not \"Not yet trading\" error code '%s', not retrying...", NOT_YET_TRADING_ERR_CODE);
						break;
					}
				}
			}
		}
	}

}
