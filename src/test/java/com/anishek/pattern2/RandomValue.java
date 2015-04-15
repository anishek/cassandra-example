package com.anishek.pattern2;

import java.nio.charset.Charset;
import java.util.Random;

public class RandomValue {
    private Random random;
    private int bytes;

    public RandomValue(int numberOfBits) {
        this.random = new Random(System.nanoTime());
        this.bytes = new Double(Math.ceil((double) numberOfBits)).intValue()
        ;
    }

    public String next() {
        byte[] current = new byte[bytes];
        random.nextBytes(current);
        return new String(current, Charset.forName("UTF-8"));
    }
}
