package com.anishek.pattern2;

import com.anishek.Constants;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Stopwatch;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class BitReadRunnable implements Callable<Long> {
    private final Session session;
    private Random random;
    private final long totalKeySpace;

    public BitReadRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.totalKeySpace = (long) otherArguments.get(Constants.TOTAL_PARTITION_KEYS);
        this.random = new Random(System.nanoTime());
    }

    @Override
    public Long call() throws Exception {
        Stopwatch started = Stopwatch.createStarted();
        for (long i = 0; i < 10000; i++) {
            Long key = new Double(random.nextFloat() * totalKeySpace).longValue();
            Select.Where select = QueryBuilder.select().all().from("segments").where(QueryBuilder.eq("id", key));
            ResultSet execute = session.execute(select);
            execute.iterator().next();
        }
        return started.elapsed(TimeUnit.MILLISECONDS) / 10000;
    }
}
