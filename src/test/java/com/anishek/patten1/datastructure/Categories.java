package com.anishek.patten1.datastructure;

import java.util.HashSet;
import java.util.Set;

public enum Categories {
    CATEGORY_ONE, CATEGORY_TWO, CATEGORY_THREE, CATEGORY_FOUR;

    private static Set<String> values = new HashSet<String>();
    static {
        values.add(CATEGORY_ONE.name());
        values.add(CATEGORY_TWO.name());
        values.add(CATEGORY_THREE.name());
        values.add(CATEGORY_FOUR.name());
    }

    public static Set<String> categoryValues() {
        return values;
    }
}
