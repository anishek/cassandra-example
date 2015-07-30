package com.anishek.lcs;

import com.anishek.Constants;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class SingleReadWriteRunnable implements Callable<Long> {
    public static final String SEGMENTS_TTL = "sttl";
    public static final String TTL = "ttl";

    private final long start;
    private final long stop;
    private final Session session;
    private final int segmentsTTL;
    private final double writePercentage = 0.2d;
    private final long keySpace;
    private Random random;


    public SingleReadWriteRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.segmentsTTL = (int) otherArguments.get(SEGMENTS_TTL);
        this.keySpace = (long) otherArguments.get(SingleTableReadWriteTest.NUM_PARTITIONS);
        this.random = new Random(System.nanoTime());
    }

    @Override
    public Long call() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (long i = start; i < stop; i++) {

            Statement select = QueryBuilder.select()
                    .column("segments")
                    .from("test")
                    .where(QueryBuilder.eq("id", Math.round(keySpace * random.nextDouble())))
                    .and(QueryBuilder.eq("client_id", random.nextInt(11)))
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

            session.execute(select);

            if (random.nextDouble() > writePercentage) {

                Statement statement = QueryBuilder.insertInto("activity_log", "test")
                        .value("id", i)
                        .value("client_id", i % 10)
                        .value("segments", segments())
                        .using(QueryBuilder.ttl(this.segmentsTTL))
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
                session.execute(statement);
            }
        }
        long elapsed = stopwatch.elapsed(TimeUnit.MICROSECONDS);
        System.out.println(Thread.currentThread().getName() + " : time(millisec) : " + elapsed);
        return elapsed;
    }

    private Set<String> segments() {
        HashSet<String> segments = new HashSet<>();
        int numberOfSegments = random.nextInt(10);
        for (int i = 0; i < numberOfSegments; i++) {
            segments.add("segment_" + i);
        }
        return segments;
    }
}
