package com.anishek;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;

import java.sql.Time;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class InsertSamePartitionRunnableCatchingException implements Callable<Long> {
    private final Coordinates coordinates;
    private long start;
    private long stop;
    private Session session;
    private long entriesPerPartition;

    public InsertSamePartitionRunnableCatchingException(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.coordinates = new Coordinates();
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.entriesPerPartition = new Long(otherArguments.get(Constants.ENTRIES_PER_PARTITION).toString());
    }

    @Override
    public Long call() throws Exception {
        long time = 0;
        for (long i = start; i < stop; i++) {
            for (long k = 0; k < entriesPerPartition; k++) {
                Statement statement = QueryBuilder.insertInto("test", "t1")
                        .value("id", i)
                        .value("ts", new Date())
                        .value("cat1", Categories.categoryValues())
                        .value("cat2", Categories.categoryValues())
                        .value("lat", coordinates.lat())
                        .value("lon", coordinates.lon())
                        .value("a", k);
                Stopwatch watch = Stopwatch.createStarted();
                try {
                    session.execute(statement);
                } catch (WriteTimeoutException ex) {
                    throw new TimeoutException(watch.elapsed(TimeUnit.MICROSECONDS), ex);
                }
                time += watch.elapsed(TimeUnit.MICROSECONDS);
            }
        }
        return time / ((stop - start) * entriesPerPartition);
    }

    public static class TimeoutException extends Exception {

        public long timeoutInMicorSeconds;

        public TimeoutException(long timeoutInMicorSeconds, Exception exception) {
            super(exception);
            this.timeoutInMicorSeconds = timeoutInMicorSeconds;
        }

    }


}



