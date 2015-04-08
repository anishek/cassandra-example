package com.anishek.read;

import com.anishek.Constants;
import com.anishek.ReadResult;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ReadRunnable implements Callable<ReadResult> {
    public static final String READ_OPERATIONS_PER_KEY = "readOperationsPerKey";

    private long start;
    private long stop;
    private Session session;
    private long readOperationsPerKey;
    private final Long entriesPerPartition;

    private Random random = new Random(System.currentTimeMillis());

    public ReadRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.readOperationsPerKey = new Long(otherArguments.get(READ_OPERATIONS_PER_KEY).toString());
        this.entriesPerPartition = new Long(otherArguments.get(Constants.ENTRIES_PER_PARTITION).toString());
    }


    @Override
    public ReadResult call() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long rowsRead = 0;
        for (long i = start; i < stop; i++) {
            for (long k = 0; k < readOperationsPerKey; k++) {
                int numberOfRowsToGet = new Float(random.nextFloat() * entriesPerPartition).intValue();
                Statement statement = QueryBuilder.select().all().from("test", "t1").where(QueryBuilder.eq("id", i)).and(QueryBuilder.lt("ts", new Date())).limit(numberOfRowsToGet).setFetchSize(numberOfRowsToGet);
                for (Row row : session.execute(statement)) {
                    rowsRead++;
                }
            }
        }
        stopwatch.stop();
        ReadResult readResult = new ReadResult();
        readResult.timeTaken = stopwatch.elapsed(TimeUnit.MILLISECONDS) / (readOperationsPerKey * (stop - start));
        readResult.averageRowsRead = rowsRead / (readOperationsPerKey * (stop - start));
        return readResult;
    }

}
