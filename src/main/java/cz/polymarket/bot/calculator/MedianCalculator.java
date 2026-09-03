package cz.polymarket.bot.calculator;

import jakarta.enterprise.context.ApplicationScoped;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ApplicationScoped
public class MedianCalculator {

    public BigDecimal calculate(BigDecimal a, BigDecimal b, BigDecimal c) {
        if (a == null || b == null || c == null) {
            throw new IllegalArgumentException("Exchange prices cannot be null when calculating median");
        }

        List<BigDecimal> list = new ArrayList<>(3);
        list.add(a);
        list.add(b);
        list.add(c);
        Collections.sort(list);

        return list.get(1);
    }
}
