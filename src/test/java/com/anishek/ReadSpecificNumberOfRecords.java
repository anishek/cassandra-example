package com.anishek;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ReadSpecificNumberOfRecords implements Callable<ReadResult> {

    private long start;
    private long stop;
    private Session session;
    private final int recordsToRead;
    private final long totalPartitionKeys;
    private Random random = new Random(System.nanoTime());


    public ReadSpecificNumberOfRecords(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.recordsToRead = new Integer(otherArguments.get(Constants.RECORDS_TO_READ).toString());
        this.totalPartitionKeys = new Long(otherArguments.get(Constants.TOTAL_PARTITION_KEYS).toString());
    }


    @Override
    public ReadResult call() throws Exception {
        long timePerReadInMicro = 0;
        for (long i = start; i < stop; i++) {
            long partitionId = (i + random.nextLong()) % totalPartitionKeys;
            Statement statement = QueryBuilder.select()
                    .all().from("test", "t1")
                    .where(QueryBuilder.eq("id", partitionId))
                    .orderBy(QueryBuilder.asc("ts"))
                    .setFetchSize(recordsToRead);
            Stopwatch stopwatch = Stopwatch.createStarted();
            session.execute(statement);
            timePerReadInMicro += stopwatch.elapsed(TimeUnit.MICROSECONDS);
        }
        ReadResult readResult = new ReadResult();
        readResult.timeTaken = timePerReadInMicro / (stop - start);
        return readResult;
    }
}
