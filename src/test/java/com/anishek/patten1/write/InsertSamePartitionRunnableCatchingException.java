package com.anishek.patten1.write;

import com.anishek.Constants;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class InsertSamePartitionRunnableCatchingException implements Callable<Long> {
    private long start;
    private long stop;
    private Session session;
    private long entriesPerPartition;
    private ColumnStructure columnStructure;

    public InsertSamePartitionRunnableCatchingException(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.entriesPerPartition = new Long(otherArguments.get(Constants.ENTRIES_PER_PARTITION).toString());
        this.columnStructure = (ColumnStructure) otherArguments.get(Constants.COLUMN_STRUCTURE);
    }

    @Override
    public Long call() throws Exception {
        long time = 0;
        for (long i = start; i < stop; i++) {
            for (long k = 0; k < entriesPerPartition; k++) {
                Insert insert = QueryBuilder.insertInto("test", "t1")
                        .value("id", i)
                        .value("ts", new Date());
                Stopwatch watch = Stopwatch.createStarted();
                try {
                    session.execute(columnStructure.populate(insert));
                } catch (WriteTimeoutException ex) {
                    throw new TimeoutException(watch.elapsed(TimeUnit.MICROSECONDS), ex);
                }
                time += watch.elapsed(TimeUnit.MICROSECONDS);
            }
        }
        return time / ((stop - start) * entriesPerPartition);
    }

    public static class TimeoutException extends Exception {

        public long timeoutInMicroSeconds;

        public TimeoutException(long timeoutInMicroSeconds, Exception exception) {
            super(exception);
            this.timeoutInMicroSeconds = timeoutInMicroSeconds;
        }

    }


}



