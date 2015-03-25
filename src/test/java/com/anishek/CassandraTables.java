package com.anishek;

import com.anishek.threading.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


/**
 * create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
 * CREATE TABLE t1(id bigint, ts timestamp, cat1 set<text>, cat2 set<text>, lat float, lon float, a bigint, primary key (id, ts)) with clustering order by (ts desc) and compression={'sstable_compression' : 'SnappyCompressor'};
 */
public class CassandraTables {


    private Cluster localhost;

    @Before
    public void setUp() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 30);
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 30);
        localhost = Cluster.builder()
                .withPoolingOptions(poolingOptions)
                .addContactPoint("localhost")
                .build();
    }

    private void recreateKeySpace() {
        Session session = localhost.connect();
        try {
            assertTrue(session.execute("drop keyspace test;").wasApplied());
        } catch (InvalidQueryException exception) {
        }
        assertTrue(session.execute("create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};").wasApplied());
        session.close();
        session = localhost.connect("test");
        assertTrue(session.execute("CREATE TABLE t1(id bigint, ts timestamp, cat1 set<text>, cat2 set<text>, lat float, lon float, a bigint, primary key (id, ts)) " +
                "with clustering order by (ts desc) and compression={'sstable_compression' : 'SnappyCompressor'} " +
                "and gc_grace_seconds=0;").wasApplied());
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
        recreateKeySpace();
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
            Long time = new AverageTimeEvaluation().eval(run);
            averageTotal += time;
            System.out.println("Average time: " + time);
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
        recreateKeySpace();
        Session testSession = localhost.connect("test");

        long NUMBER_OF_PARTITION_RECORDS = 40000;
        int NUMBER_OF_ENTRIES_PER_PARTITION = 30;

        insertWithoutTTL(testSession, NUMBER_OF_PARTITION_RECORDS, NUMBER_OF_ENTRIES_PER_PARTITION);
        testSession.close();
    }

    private void insertWithoutTTL(Session testSession, long partitionKeys, int entriesPerPartition) throws Exception {
        long averageTotal = 0;
        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 20;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(Constants.ENTRIES_PER_PARTITION, entriesPerPartition);
        System.out.println("Without TTL Average for " + NUM_OF_THREADS + " threads inserting " + partitionKeys + " records.");
        Stopwatch started = Stopwatch.createStarted();
        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(partitionKeys, NUM_OF_THREADS, new RunnerFactory(InsertSamePartitionRunnable.class, otherArguments));
            List run = threaded.run(new DefaultCallback());
            Long time = new AverageTimeEvaluation().eval(run);
            averageTotal += time;
            System.out.println("Average time: " + time);
        }
        long elapsed = started.elapsed(TimeUnit.SECONDS);
        System.out.print("total time taken in sec: " + elapsed + " for " + (NUM_OF_RUNS * partitionKeys * entriesPerPartition));
        System.out.println("Average for 1 record entry over " + NUM_OF_RUNS + " runs: " + averageTotal / NUM_OF_RUNS);
    }

    @Test
    public void readAcrossThreads() throws Exception {
        Session testSession = localhost.connect("test");
        long averageTotal = 0;
        long rowsRead = 0;

        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 20;
        long NUMBER_OF_PARTITION_RECORDS = 4000;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(ReadRunnable.READ_OPERATIONS_PER_KEY, 1);
        otherArguments.put(Constants.ENTRIES_PER_PARTITION, 300);

        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(NUMBER_OF_PARTITION_RECORDS, NUM_OF_THREADS, new RunnerFactory(ReadRunnable.class, otherArguments));
            List list = threaded.run(new ReadCallback());
            ReadResult result = new ReadEvaluation().eval(list);
            averageTotal += result.timeTaken;
            rowsRead += result.averageRowsRead;
            System.out.println("===================================================================");
        }
        System.out.println("Across Runs average rows read per user: " + (rowsRead / NUM_OF_RUNS) + " average time taken to read the records: " + (averageTotal / NUM_OF_RUNS));
        testSession.close();
    }

    private void readSpecificNumberOfRecordsEvenlySpreadOutAcrossPartitionKeys(Session testSession) throws Exception {
        long averageTotal = 0;

        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 20;
        long TOTAL_NUMBER_OF_READ_OPERATIONS = 100000;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(Constants.RECORDS_TO_READ, 10);
        otherArguments.put(Constants.TOTAL_PARTITION_KEYS, 4000);

        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(TOTAL_NUMBER_OF_READ_OPERATIONS, NUM_OF_THREADS,
                    new RunnerFactory(ReadSpecificNumberOfRecords.class, otherArguments));
            List list = threaded.run(new ReadCallback());
            ReadResult result = new ReadEvaluation().eval(list);
            averageTotal += result.timeTaken;
            System.out.println("time taken for reading one record when reading " + TOTAL_NUMBER_OF_READ_OPERATIONS + " records in run: " + result.timeTaken);
        }
        System.out.println("Across Runs average time taken to read one record: " + (averageTotal / NUM_OF_RUNS));
    }

    @Test
    public void readSpecificNumberOfRecordsEvenlySpreadOutAcrossPartitionKeys() throws Exception {
        Session testSession = localhost.connect("test");
        readSpecificNumberOfRecordsEvenlySpreadOutAcrossPartitionKeys(testSession);
        testSession.close();
    }

    @Test
    public void readWriteOperationsWithUpdatesHavingAPercentage() throws Exception {
        Session testSession = localhost.connect("test");
        long averageTotal = 0;

        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 25;
        long TOTAL_NUMBER_OF_READ_OPERATIONS = 1000000;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(Constants.TOTAL_PARTITION_KEYS, 4000);
        otherArguments.put(ReadWriteRunnable.UPDATE_EXISTING_PERCENTAGE, 0.7);

        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(TOTAL_NUMBER_OF_READ_OPERATIONS, NUM_OF_THREADS,
                    new RunnerFactory(ReadWriteRunnable.class, otherArguments));
            List list = threaded.run(new DefaultCallback());
            Long timeTaken = new AverageTimeEvaluation().eval(list);
            averageTotal += timeTaken;
            System.out.println("time taken for reading one record when reading " + TOTAL_NUMBER_OF_READ_OPERATIONS + " records in run: " + timeTaken);
        }
        System.out.println("Across Runs average time taken to read and write : " + (averageTotal / NUM_OF_RUNS));
        testSession.close();
    }

    /**
     * This test can be used to create the problem of too many tombstones and out of memory errors in cassandra
     *
     * @throws Exception
     */
    @Test
    public void insertMultipleColumnsWith_TTL_AndRead() throws Exception {
        recreateKeySpace();
        Session testSession = localhost.connect("test");
        int VARIABLE_TTL = 3 * 60;
        int DEFINITE_TTL_IN_SEC = 5 * 60;


        insertWithTTL(testSession, 40000, 30, DEFINITE_TTL_IN_SEC, VARIABLE_TTL);

        ///////////////////////////////////////////////////WE are starting READS////////////////////////////

        long averageTotal = 0;
        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 20;
        long TOTAL_NUMBER_OF_READ_OPERATIONS = 100000;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(Constants.RECORDS_TO_READ, 10);
        otherArguments.put(Constants.TOTAL_PARTITION_KEYS, 40000);

        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(TOTAL_NUMBER_OF_READ_OPERATIONS, NUM_OF_THREADS,
                    new RunnerFactory(ReadSpecificNumberOfRecords.class, otherArguments));
            List list = threaded.run(new ReadCallback());
            ReadResult result = new ReadEvaluation().eval(list);
            averageTotal += result.timeTaken;
            System.out.println("time taken for reading one record when reading " + TOTAL_NUMBER_OF_READ_OPERATIONS + " records in run: " + result.timeTaken);
        }
        System.out.println("Across Runs average time taken to read one record: " + (averageTotal / NUM_OF_RUNS));

        testSession.close();
    }

    @Test
    public void insertColumnsWithTTL_outsideOurReadRequests() throws Exception {
        recreateKeySpace();
        Session testSession = localhost.connect("test");

        int fixedTTL = 1;
        int variableTTL = 10;

        insertWithTTL(testSession, 40000, 30, fixedTTL, variableTTL);

        Thread.sleep((fixedTTL + variableTTL) * 1000);
        Date earliestDate = new Date();

        insertWithoutTTL(testSession, 40000, 30);


        ///////////////////////////////////////////////////WE are starting READS////////////////////////////

        long averageTotal = 0;
        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 20;
        long TOTAL_NUMBER_OF_READ_OPERATIONS = 100000;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(Constants.RECORDS_TO_READ, 30);
        otherArguments.put(Constants.TOTAL_PARTITION_KEYS, 40000);
        otherArguments.put(Constants.DATE_WITH_NO_COLUMN_EXPIRY_VIA_TTL, earliestDate);

        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(TOTAL_NUMBER_OF_READ_OPERATIONS, NUM_OF_THREADS,
                    new RunnerFactory(ReadSpecificNumberOfRecords.class, otherArguments));
            List list = threaded.run(new ReadCallback());
            ReadResult result = new ReadEvaluation().eval(list);
            averageTotal += result.timeTaken;
            System.out.println("time taken for reading one record when reading " + TOTAL_NUMBER_OF_READ_OPERATIONS + " records in run: " + result.timeTaken);
        }
        System.out.println("Across Runs average time taken to read one record: " + (averageTotal / NUM_OF_RUNS));

        testSession.close();
    }

    private void insertWithTTL(Session testSession, long numberOfPartitions, int entriesPerPartition, int fixedTTL, int variableTTLInSec) throws Exception {
        long averageTotal = 0;

        int NUM_OF_RUNS = 10;
        int NUM_OF_THREADS = 20;

        HashMap<String, Object> otherArguments = new HashMap<String, Object>();
        otherArguments.put(Constants.SESSION, testSession);
        otherArguments.put(Constants.ENTRIES_PER_PARTITION, entriesPerPartition);
        otherArguments.put(Constants.VARIABLE_RANGE_TTL, variableTTLInSec);
        otherArguments.put(Constants.DEFINITE_TTL_IN_SEC, fixedTTL);
        System.out.println("Average for " + NUM_OF_THREADS + " threads inserting " + numberOfPartitions + " records.");
        Stopwatch started = Stopwatch.createStarted();
        for (int i = 0; i < NUM_OF_RUNS; i++) {
            Threaded threaded = new Threaded(numberOfPartitions, NUM_OF_THREADS, new RunnerFactory(InsertSamePartitionWithTTLRunnable.class, otherArguments));
            List run = threaded.run(new DefaultCallback());
            Long time = new AverageTimeEvaluation().eval(run);
            averageTotal += time;
            System.out.println("Average time: " + time);
        }
        long elapsed = started.elapsed(TimeUnit.SECONDS);
        System.out.println("total time taken in sec: " + elapsed + " for " + (NUM_OF_RUNS * numberOfPartitions * entriesPerPartition));
        System.out.println("Average for 1 record entry over " + NUM_OF_RUNS + " runs: " + averageTotal / NUM_OF_RUNS);
    }
}