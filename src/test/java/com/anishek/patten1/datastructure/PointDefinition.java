package com.anishek.patten1.datastructure;

import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Random;

public class PointDefinition {

    private Coordinates coordinates;

    private Random random;

    public PointDefinition() {
        coordinates = new Coordinates();
        random = new Random(System.nanoTime());
    }

    public String serialize() {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("lat", coordinates.lat());
        map.put("long", coordinates.lat());
        map.put("someValue", random.nextLong());
        map.put("set1", Categories.values());
        map.put("set2", Categories.values());
        return JSONObject.toJSONString(map);
    }
}
