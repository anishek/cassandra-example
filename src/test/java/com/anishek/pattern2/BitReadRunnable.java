package com.anishek.pattern2;

import com.anishek.Constants;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Stopwatch;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class BitReadRunnable implements Callable<Long> {
    private final long start;
    private final long stop;
    private final Session session;
    private RandomValue randomValue;

    public BitReadRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.randomValue = new RandomValue(Constants.BITS);
    }

    @Override
    public Long call() throws Exception {
        Stopwatch started = Stopwatch.createStarted();
        for (long i = start; i < stop; i++) {
            Select.Where select = QueryBuilder.select().all().from("segments").where(QueryBuilder.eq("id", i));
            ResultSet execute = session.execute(select);
            execute.iterator().next();
        }
        return started.elapsed(TimeUnit.MILLISECONDS) / (stop - start);
    }
}
