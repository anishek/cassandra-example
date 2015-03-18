package com.anishek;

import java.util.Map;
import java.util.concurrent.Callable;

public class RunnerFactory {

    private Class clazz;
    private final Map<String, Object> otherArguments;

    public RunnerFactory(Class clazz, Map<String, Object> otherArguments) {
        this.clazz = clazz;
        this.otherArguments = otherArguments;
    }

    public Callable<Long> create(long start, long end) throws Exception {
        return (Callable<Long>) clazz.getConstructor(long.class, long.class, Map.class).newInstance(start, end, otherArguments);
    }
}

