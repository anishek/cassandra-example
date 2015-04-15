package com.anishek.pattern2;

import com.anishek.Constants;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class BitInsertRunnable implements Callable<Long> {
    private final long start;
    private final long stop;
    private final Session session;
    private RandomValue randomValue;

    public BitInsertRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.randomValue = new RandomValue(Constants.BITS);
    }

    @Override
    public Long call() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (long i = start; i < stop; i++) {
            Statement statement = QueryBuilder.insertInto("test", "Segments")
                    .value("id", i)
                    .value("segment_bits", randomValue.next()).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
            session.execute(statement);
        }
        stopwatch.stop();
        return stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    public static class Callback implements FutureCallback<Long> {
        long timeTakenInMilliSeconds = 0;

        @Override
        public void onSuccess(Long time) {
            timeTakenInMilliSeconds = time;
        }

        @Override
        public void onFailure(Throwable throwable) {
            throw new RuntimeException("an Exception occurred ", throwable);
        }

    }
}
