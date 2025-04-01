package cz.amuradon.tralon.newlisting.trader;

import java.math.BigDecimal;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import cz.amuradon.tralon.connector.mexc.MexcClient;
import cz.amuradon.tralon.connector.mexc.OrderResponse;
import cz.amuradon.tralon.connector.mexc.Side;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RequestBuilder {
	
	private static final String HMAC_SHA256 = "HmacSHA256";

	private final MexcClient mexcClient;
	
	private Mac mac;
	
	@Inject
    public RequestBuilder(@ConfigProperty(name = "mexc.secretKey") final String secretKey,
    		@RestClient final MexcClient mexcClient) {
		this.mexcClient = mexcClient;
		try {
			mac = Mac.getInstance(HMAC_SHA256);
			mac.init(new SecretKeySpec(secretKey.getBytes(), HMAC_SHA256));
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			throw new IllegalStateException("Could not setup encoder", e);
		}
    }
	
    public Map<String, String> signQueryParams(Map<String, String> params) {
    	StringJoiner joiner = new StringJoiner("&");
    	for (Entry<String, String> entry : params.entrySet()) {
			joiner.add(entry.getKey() + "=" + entry.getValue());
		}
    	String signature = HexFormat.of().formatHex(mac.doFinal(joiner.toString().getBytes()));
    	params.put("signature", signature);
    	return params;
    }
    
    public NewOrderRequestBuilder newOrder() {
    	return new NewOrderRequestBuilder();
    }

    public OrderResponse cancelOrder(String symbol, String orderId) {
    	Map<String, String> params = new LinkedHashMap<>();
    	params.put("symbol", symbol);
    	params.put("orderId", orderId);
    	params.put("timestamp", String.valueOf(new Date().getTime()));
    	return mexcClient.cancelOrder(signQueryParams(params));
    }

    public class NewOrderRequestBuilder {
    	
    	private static final String TIMESTAMP = "timestamp";
		private Map<String, String> params = new LinkedHashMap<>();
		private boolean signed = false;
    	
		public NewOrderRequestBuilder clientOrderId(String clientOrderId) {
			params.put("newClientOrderId", clientOrderId);
			signed = false;
			return this;
		}

		public NewOrderRequestBuilder side(Side side) {
    		params.put("side", side.name());
    		signed = false;
    		return this;
    	}

    	public NewOrderRequestBuilder symbol(String symbol) {
    		params.put("symbol", symbol);
    		signed = false;
    		return this;
    	}

    	public NewOrderRequestBuilder type(String type) {
    		params.put("type", type);
    		signed = false;
    		return this;
    	}
    	
    	public NewOrderRequestBuilder quantity(BigDecimal quantity) {
    		params.put("quantity", quantity.toPlainString());
    		signed = false;
    		return this;
    	}
   
    	public NewOrderRequestBuilder price(BigDecimal price) {
    		params.put("price", price.toPlainString());
    		signed = false;
    		return this;
    	}
    	
    	public NewOrderRequestBuilder timestamp(long timestamp) {
    		params.put(TIMESTAMP, String.valueOf(timestamp));
    		signed = false;
    		return this;
    	}

    	public NewOrderRequestBuilder recvWindow(long timestamp) {
    		params.put("recvWindow", String.valueOf(timestamp));
    		signed = false;
    		return this;
    	}

    	public NewOrderRequestBuilder signParams() {
    		params = signQueryParams(params);
    		signed = true;
    		return this;
    	}
    	
    	public OrderResponse send() {
    		if (params.get(TIMESTAMP) == null) {
    			params.put(TIMESTAMP, String.valueOf(new Date().getTime()));
    			signed = false;
    		}
    		return mexcClient.newOrder(signed ? params : signQueryParams(params));
    	}
    }
    
}
