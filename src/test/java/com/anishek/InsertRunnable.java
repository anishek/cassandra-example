package com.anishek;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class InsertRunnable implements Callable<Long> {
    private final Coordinates coordinates;
    private long start;
    private long stop;

    public InsertRunnable(long start, long stop) {
        this.start = start;
        this.stop = stop;
        this.coordinates = new Coordinates();
    }

    @Override
    public Long call() throws Exception {
        Cluster localhost = Cluster.builder().addContactPoint("localhost").build();
        Session session = localhost.connect("test");
        Stopwatch stopwatch = Stopwatch.createStarted();

        for (long i = start; i < stop; i++) {
            Statement statement = QueryBuilder.insertInto("test", "t1")
                    .value("id", i)
                    .value("ts", new Date())
                    .value("cat1", Categories.categoryValues())
                    .value("cat2", Categories.categoryValues())
                    .value("lat", coordinates.lat())
                    .value("lon", coordinates.lon());

            session.execute(statement);
        }
        stopwatch.stop();
        return stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }
}
