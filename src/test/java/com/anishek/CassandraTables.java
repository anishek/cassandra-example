package com.anishek;

import com.anishek.threading.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


/**
 * create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
 * CREATE TABLE t1(id bigint, ts timestamp, cat1 set<text>, cat2 set<text>, lat float, lon float, a bigint, primary key (id, ts));
 */
public class CassandraTables {


    private Cluster localhost;

    @Before
    public void setUp() {
        localhost = Cluster.builder().addContactPoint("localhost").build();
    }

    private void recreateKeyspace() {
        Session session = localhost.connect();
        try {
            assertTrue(session.execute("drop keyspace test;").wasApplied());
        } catch (InvalidQueryException exception) {
        }
        assertTrue(session.execute("create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};").wasApplied());
        session.close();
        session = localhost.connect("test");
        assertTrue(session.execute("CREATE TABLE t1(id bigint, ts timestamp, cat1 set<text>, cat2 set<text>, lat float, lon float, a bigint, primary key (id, ts));").wasApplied());
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
    public void findAverageForNewPartitionInsertions() throws Exception {
        recreateKeyspace();
        Session testSession = localhost.connect("test");
        long averageTotal = 0;

        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 40;
        long NUMBER_OF_RECORDS = 1000 * 1000;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);

        System.out.println("Average for " + NUM_OF_THREADS + " threads inserting " + NUMBER_OF_RECORDS + " records");

        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(NUMBER_OF_RECORDS, NUM_OF_THREADS, new RunnerFactory(InsertRunnable.class, otherArguments));
            List run = threaded.run(new DefaultCallback());
            averageTotal += new AverageTimeEvaluation().eval(run);
        }
        System.out.println("Average for " + NUM_OF_RUNS + " runs: " + averageTotal / NUM_OF_RUNS);
        testSession.close();
    }

    /**
     * about 11000 QPS with some write acknowledgement failures when
     * int NUM_OF_RUNS = 3;
     * int NUM_OF_THREADS = 50;
     * long NUMBER_OF_PARTITION_RECORDS = 250;
     * ENTRIES_PER_PARTITION = 20000
     */
    @Test
    public void findAverageWithRecordsInsertAcrossLessPartitions() throws Exception {
        recreateKeyspace();
        Session testSession = localhost.connect("test");
        long averageTotal = 0;

        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 30;
        long NUMBER_OF_PARTITION_RECORDS = 30000;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(Constants.ENTRIES_PER_PARTITION, 30);
        System.out.println("Average for " + NUM_OF_THREADS + " threads inserting " + NUMBER_OF_PARTITION_RECORDS + " records.");
        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(NUMBER_OF_PARTITION_RECORDS, NUM_OF_THREADS, new RunnerFactory(InsertSamePartitionRunnable.class, otherArguments));
            List run = threaded.run(new DefaultCallback());
            averageTotal += new AverageTimeEvaluation().eval(run);
        }
        System.out.println("Average for 1 record entry over " + NUM_OF_RUNS + " runs: " + averageTotal / NUM_OF_RUNS);
        testSession.close();
    }


    @Test
    public void readAcrossThreads() throws Exception {
        Session testSession = localhost.connect("test");
        long averageTotal = 0;
        long rowsRead = 0;

        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 10;
        long NUMBER_OF_PARTITION_RECORDS = 50;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(Constants.READ_OPERATIONS_PER_KEY, 1);
        otherArguments.put(Constants.ENTRIES_PER_PARTITION, 20000);

        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(NUMBER_OF_PARTITION_RECORDS, NUM_OF_THREADS, new RunnerFactory(ReadRunnable.class, otherArguments));
            List list = threaded.run(new ReadCallback());
            ReadRunnable.ReadResult result = new ReadEvaluation().eval(list);
            averageTotal += result.timeTaken;
            rowsRead += result.averageRowsRead;
            System.out.println("===================================================================");
        }
        System.out.println("Across Runs average rows read per user: " + (rowsRead / NUM_OF_RUNS) + " average time taken to read the records: " + (averageTotal / NUM_OF_RUNS));
        testSession.close();
    }
}