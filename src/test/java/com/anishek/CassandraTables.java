package com.anishek;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


/**
 * create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
 * CREATE TABLE t1(id bigint, ts timestamp, cat1 set<text>, cat2 set<text>, lat float, lon float, primary key (id, ts));
 */
public class CassandraTables {

    private static long NUMBER_OF_RECORDS = 10 * 1000;
    private Cluster localhost;

    @Before
    public void setUp() {
        localhost = Cluster.builder().addContactPoint("localhost").build();
    }

    private void recreateKeyspace() {
        Session session = localhost.connect();
        assertTrue(session.execute("drop keyspace test;").wasApplied());
        assertTrue(session.execute("create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};").wasApplied());
        session.close();
        session = localhost.connect("test");
        assertTrue(session.execute("CREATE TABLE t1(id bigint, ts timestamp, cat1 set<text>, cat2 set<text>, lat float, lon float, primary key (id, ts));").wasApplied());
        session.close();
    }

    @After
    public void tearDown() {
        localhost.close();
    }

    @Test
    public void testConnection() {
        assertThat(localhost.getMetadata().getAllHosts().size(), equalTo(1));
    }

    @Test
    public void findAverage() throws Exception {
        recreateKeyspace();
        Session testSession = localhost.connect("test");
        long averageTotal = 0;
        int NUM_OF_RUNS = 50;
        int NUM_OF_THREADS = 10;

        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(NUMBER_OF_RECORDS, NUM_OF_THREADS, new RunnerFactory(InsertRunnable.class, testSession));
            long runTime = threaded.run();
            averageTotal += runTime;
            System.out.println("Average for " + NUM_OF_THREADS + " threads inserting " + NUMBER_OF_RECORDS + " records : " + runTime);
        }
        System.out.println("Average for " + NUM_OF_RUNS + " runs: " + averageTotal / NUM_OF_RUNS);
        testSession.close();
    }
}