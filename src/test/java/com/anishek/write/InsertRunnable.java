package com.anishek.write;

import com.anishek.Constants;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class InsertRunnable implements Callable<Long> {
    private final ColumnStructure columnStructure;
    private long start;
    private long stop;
    private Session session;

    public InsertRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.columnStructure = (ColumnStructure) otherArguments.get(Constants.COLUMN_STRUCTURE);
    }

    @Override
    public Long call() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (long i = start; i < stop; i++) {
            Insert insert = QueryBuilder.insertInto("test", "t1")
                    .value("id", i)
                    .value("ts", new Date());

            session.execute(columnStructure.populate(insert));
        }
        stopwatch.stop();
        return stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }
}
