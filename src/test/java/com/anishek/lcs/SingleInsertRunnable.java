package com.anishek.lcs;

import com.anishek.Constants;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Stopwatch;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class SingleInsertRunnable implements Callable<Long> {
    public static final String SEGMENTS_TTL = "sttl";
    public static final String TTL = "ttl";

    private final long start;
    private final long stop;
    private final Session session;
    private final int segmentsTTL;
    private final int attributesTTL;
    private Random random;


    public SingleInsertRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        this.session = (Session) otherArguments.get(Constants.SESSION);
        this.segmentsTTL = (int) otherArguments.get(SEGMENTS_TTL);
        this.attributesTTL = (int) otherArguments.get(TTL);
        this.random = new Random(System.nanoTime());
    }

    @Override
    public Long call() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Stopwatch intermediate = Stopwatch.createStarted();
        for (long i = start; i < stop; i++) {
            if ((i - start) % 10000 == 0 && i != start) {
                System.out.println(Thread.currentThread().getName() + " : " + i + " : time(millisec) : " + intermediate.elapsed(TimeUnit.MILLISECONDS));
                intermediate.reset().start();
            }
            HashMap<String, String> attributes = new HashMap<>();
            attributes.put("a", "AAAA");
            attributes.put("b", "BBBBBB");
            Statement statement = QueryBuilder.insertInto("activity_log", "test")
                    .value("id", i)
                    .value("client_id", 11)
                    .value("attributes", attributes)
                    .using(QueryBuilder.ttl(attributesTTL))
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            session.execute(statement);
            for (int j = 0; j < 10; j++) {
                statement = QueryBuilder.insertInto("activity_log", "test")
                        .value("id", i)
                        .value("client_id", i % 10)
                        .value("segments", segments())
                        .using(QueryBuilder.ttl(this.segmentsTTL))
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
                session.execute(statement);
            }
        }
        return stopwatch.elapsed(TimeUnit.MICROSECONDS);
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
