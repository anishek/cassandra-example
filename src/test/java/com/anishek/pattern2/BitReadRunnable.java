package com.anishek.pattern2;

import com.anishek.Constants;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class BitReadRunnable implements Callable<BitReadRunnable.ReadCallable> {
    private final Session session;
    private Random random;
    private final long totalKeySpace;
    private final long timeThreshold;

    public BitReadRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.totalKeySpace = (long) otherArguments.get(Constants.TOTAL_PARTITION_KEYS);
        this.random = new Random(System.nanoTime());
        this.timeThreshold = (long) otherArguments.get(Constants.TIME_THRESHOLD_IN_MILLIS);
    }

    @Override
    public ReadCallable call() throws Exception {
        ReadCallable readCallable = new ReadCallable();
        for (long i = 0; i < readCallable.numberOfRuns; i++) {
            Long key = new Double(random.nextFloat() * totalKeySpace).longValue();
            Statement statement = QueryBuilder.select().column("ts").from("segments")
                    .where(QueryBuilder.eq("id", key)).and(QueryBuilder.lt("ts", new Date()))
                    .limit(1).setFetchSize(1).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
            Stopwatch started = Stopwatch.createStarted();
            ResultSet execute = session.execute(statement);
            execute.iterator().next();
            long elapsed = started.elapsed(TimeUnit.MILLISECONDS);
            if (elapsed > timeThreshold) {
                readCallable.countAboveThreshold++;
            }
            readCallable.timeTaken += elapsed;
        }
        return readCallable;
    }

    public static class ReadCallable {
        public long countAboveThreshold = 0;
        public long timeTaken = 0;
        public long numberOfRuns = 100000;
    }
}
