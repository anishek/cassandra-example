package com.anishek.patten1.write;

import com.anishek.Constants;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class InsertSamePartitionWithTTLRunnable implements Callable<Long> {
    private final int variableRangeTTL;
    private final int definiteTTLInSec;
    private long start;
    private long stop;
    private Session session;
    private long entriesPerPartition;
    private Random random = new Random(System.nanoTime());
    private ColumnStructure columnStructure;

    public InsertSamePartitionWithTTLRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.entriesPerPartition = new Long(otherArguments.get(Constants.ENTRIES_PER_PARTITION).toString());
        this.variableRangeTTL = new Integer(otherArguments.get(Constants.VARIABLE_RANGE_TTL).toString());
        this.definiteTTLInSec = new Integer(otherArguments.get(Constants.DEFINITE_TTL_IN_SEC).toString());
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
                Statement statement = columnStructure.populate(insert)
                        .using(QueryBuilder.ttl(ttl()));

                Stopwatch watch = Stopwatch.createStarted();
                session.execute(statement);
                time += watch.elapsed(TimeUnit.MICROSECONDS);
            }
        }
        return time / ((stop - start) * entriesPerPartition);
    }

    private int ttl() {
        return definiteTTLInSec + (int) (random.nextFloat() * variableRangeTTL);
    }
}
