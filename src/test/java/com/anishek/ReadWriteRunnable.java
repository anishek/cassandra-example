package com.anishek;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Stopwatch;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ReadWriteRunnable implements Callable<Long> {

    public static final String UPDATE_EXISTING_PERCENTAGE = "updateExistingPercentage";
    private final Long totalPartitionKeys;
    private long start;
    private long stop;
    private Session session;
    private float updateExistingPercentage;
    private Random random = new Random(System.currentTimeMillis());
    private Coordinates coordinates;

    public ReadWriteRunnable(long start, long stop, Map<String, Object> otherArguments) {
        this.start = start;
        this.stop = stop;
        session = (Session) otherArguments.get(Constants.SESSION);
        updateExistingPercentage = new Float(otherArguments.get(UPDATE_EXISTING_PERCENTAGE).toString());
        totalPartitionKeys = new Long(otherArguments.get(Constants.TOTAL_PARTITION_KEYS).toString());
        coordinates = new Coordinates();
    }

    @Override
    public Long call() throws Exception {
        long timeTaken = 0;
        for (long i = start; i < stop; i++) {
            long key = (i + Math.abs(random.nextLong())) % totalPartitionKeys;
            Stopwatch stopwatch = Stopwatch.createStarted();
            Select singleRead = QueryBuilder.select()
                    .all().from("test", "t1")
                    .where(QueryBuilder.eq("id", key))
                    .orderBy(QueryBuilder.asc("ts"))
                    .limit(1);
            ResultSet resultSet = session.execute(singleRead);

            Date insertDate = new Date();
            if (random.nextFloat() < updateExistingPercentage) {
                insertDate = resultSet.iterator().next().getDate(1);
            }
            Insert insert = QueryBuilder.insertInto("test", "t1")
                    .value("id", key)
                    .value("ts", insertDate)
                    .value("cat1", Categories.categoryValues())
                    .value("cat2", Categories.categoryValues())
                    .value("lat", coordinates.lat())
                    .value("lon", coordinates.lon())
                    .value("a", random.nextLong());

            session.execute(insert);
            timeTaken += stopwatch.elapsed(TimeUnit.MICROSECONDS);
        }
        return timeTaken / (stop - start);
    }
}
