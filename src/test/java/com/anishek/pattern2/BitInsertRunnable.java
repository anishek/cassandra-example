package com.anishek.pattern2;

import com.anishek.Constants;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;

import java.util.Date;
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
            Statement statement = QueryBuilder.insertInto("test", "segments")
                    .value("id", i)
                    .value("ts", new Date())
                    .value("segment_bits", randomValue.next()).setConsistencyLevel(ConsistencyLevel.ONE);
            session.execute(statement);
        }
        return stopwatch.elapsed(TimeUnit.MILLISECONDS) / (stop - start);
    }

}
