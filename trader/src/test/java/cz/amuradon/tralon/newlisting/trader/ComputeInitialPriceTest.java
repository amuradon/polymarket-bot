package cz.amuradon.tralon.newlisting.trader;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import cz.amuradon.tralon.connector.mexc.OrderBook;

public class ComputeInitialPriceTest {

	@Test
	public void testSlippage() {
		ComputeInitialPrice compute = new ComputeInitialPrice("slippage:20.0");
		List<List<BigDecimal>> asks = new ArrayList<>();
		asks.add(Lists.newArrayList(new BigDecimal("0.3"), new BigDecimal("5")));
		asks.add(Lists.newArrayList(new BigDecimal("0.35"), new BigDecimal("20")));
		asks.add(Lists.newArrayList(new BigDecimal("0.36"), new BigDecimal("30")));
		
		OrderBook orderBook = new OrderBook(null, asks); 
		BigDecimal result = compute.execute("VPTUSDT",
				6,
				orderBook);
		Assertions.assertEquals(new BigDecimal("0.408000"), result);
	}
	
	@Test
	public void testFixed() {
		ComputeInitialPrice compute = new ComputeInitialPrice("manual:0.00125");
		BigDecimal result = compute.execute("VPTUSDT", 6, null);
		Assertions.assertEquals(new BigDecimal("0.00125"), result);
	}
	
	@Test
	public void test() {
		System.out.println(HexFormat.of().toHexDigits(new Date().getTime()));
	}
}
