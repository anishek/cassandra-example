package com.anishek;

import com.datastax.driver.core.Session;

import java.util.concurrent.Callable;

public class RunnerFactory {

    private final Session session;
    private Class clazz;

    public RunnerFactory(Class clazz, Session session) {
        this.clazz = clazz;
        this.session = session;
    }

    public Callable<Long> create(long start, long end) throws Exception {
        return (Callable<Long>) clazz.getConstructor(long.class, long.class, Session.class).newInstance(start, end, session);
    }
}

