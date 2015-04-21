package com.anishek.pattern2;

import com.anishek.Constants;
import com.anishek.threading.RunnerFactory;
import com.anishek.threading.Threaded;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class CassandraBitTables {

    String CONFIG_FILE_KEY = "contact.points";
    private Cluster cluster;

    @Before
    public void setup() {
        Cluster.Builder builder = Cluster.builder();
        for (String contactPoint : contactPoints()) {
            builder.addContactPoints(contactPoint);
        }

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 3);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 3);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 3);
        poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, 3);

        cluster = builder
                .withPoolingOptions(poolingOptions)
                .addContactPoint("localhost")
                .withSocketOptions(new SocketOptions().setTcpNoDelay(true).setKeepAlive(true))
                .build();
    }

    private void recreateKeyspace() throws InterruptedException {
        Session session = cluster.connect();
        try {
            assertTrue(session.execute("drop keyspace test;").wasApplied());
            Thread.sleep(2000l);
        } catch (InvalidQueryException exception) {
        }
        assertTrue(session.execute("create keyspace test with replication = {'class': 'NetworkTopologyStrategy', 'WDC' : 3};").wasApplied());
        session.close();
        session = cluster.connect("test");
        assertTrue(session.execute(" CREATE TABLE segments(id bigint primary key , segment_bits text) " +
                "with gc_grace_seconds=0 " +
                "and compaction = {'class': 'LeveledCompactionStrategy'} " +
                "and compression={'sstable_compression' : ''};").wasApplied());
        session.close();
    }

    @Test
    public void insertData() throws Exception {
        recreateKeyspace();
        Session session = cluster.connect("test");
        int NUM_OF_THREADS = 50;
        long NUM_OF_KEYS = 100000000;
        HashMap<String, Object> otherArguments = new HashMap<>();
        otherArguments.put(Constants.SESSION, session);

        Threaded threaded = new Threaded(NUM_OF_KEYS, NUM_OF_THREADS, new RunnerFactory(BitInsertRunnable.class, otherArguments));
        List<BitInsertRunnable.Callback> callbacks = threaded.run(new BitInsertRunnable.Callback());
        double sum = 0;
        for (BitInsertRunnable.Callback callback : callbacks) {
            sum += callback.timeTakenInMilliSeconds;
        }
        System.out.println("One insert for " + NUM_OF_KEYS + " keys across " + NUM_OF_THREADS + " threads : " + (sum / NUM_OF_KEYS));
    }

    @Test
    public void read() throws Exception {
        Session session = cluster.connect("test");
        int NUM_OF_THREADS = 25;
        long NUM_OF_KEYS = 100000000;
        HashMap<String, Object> otherArguments = new HashMap<>();
        otherArguments.put(Constants.SESSION, session);
        otherArguments.put(Constants.TOTAL_PARTITION_KEYS, NUM_OF_KEYS);
        Threaded threaded = new Threaded(NUM_OF_KEYS, NUM_OF_THREADS, new RunnerFactory(BitReadRunnable.class, otherArguments));
        List<BitInsertRunnable.Callback> callbacks = threaded.run(new BitInsertRunnable.Callback());
        long sum = 0;
        for (BitInsertRunnable.Callback callback : callbacks) {
            sum += callback.timeTakenInMilliSeconds;
        }
        System.out.println("One read across " + NUM_OF_KEYS + " keys across " + NUM_OF_THREADS + " threads : " + (sum / NUM_OF_THREADS) + " ms");
    }


    private Collection<String> contactPoints() {
        String value = System.getProperty(CONFIG_FILE_KEY);
        return Collections2.transform(Arrays.asList(value.split(",")), new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s.trim();
            }
        });
    }
}
