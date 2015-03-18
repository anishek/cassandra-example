package com.anishek;

import java.util.concurrent.Callable;

public class RunnerFactory {

    private Class clazz;

    public RunnerFactory(Class clazz) {
        this.clazz = clazz;
    }

    public Callable<Long> create(long start, long end) throws Exception {
        return (Callable<Long>) clazz.getConstructor(long.class, long.class).newInstance(start, end);
    }
}

