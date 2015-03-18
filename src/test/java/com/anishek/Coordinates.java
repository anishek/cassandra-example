package com.anishek;

import java.math.BigDecimal;
import java.util.Random;

public class Coordinates {

    private static final float LATITUDE_BOUNDARY = 180f;
    private static final float LONGITUDE_BOUNDARY = 360f;
    private static final int ACCURACY = 3;


    private Random random = new Random(System.currentTimeMillis());

    public double lat() {
        return roundDownValue(random.nextFloat() * LATITUDE_BOUNDARY - 90);
    }

    public float lon() {
        return roundDownValue(random.nextFloat() * LONGITUDE_BOUNDARY - 180);
    }

    private float roundDownValue(float value) {
        BigDecimal bigDecimal = new BigDecimal(Float.toString(value));
        bigDecimal.setScale(ACCURACY, BigDecimal.ROUND_HALF_DOWN);
        return bigDecimal.floatValue();
    }
}
