package com.anishek;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class InsertSamePartitionRunnable implements Callable<Long> {
    private final Coordinates coordinates;
    private long start;
    private long stop;
    private Session session;
    private long entriesPerPartition;

    public InsertSamePartitionRunnable(long start, long stop, Map<String, Object> otherArguments) {
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
                session.execute(statement);
                time += watch.elapsed(TimeUnit.MILLISECONDS);
            }
        }
        return time / ((stop - start) * entriesPerPartition);
    }
}
