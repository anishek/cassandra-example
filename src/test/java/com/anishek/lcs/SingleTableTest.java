package com.anishek.lcs;

/*
create table test (id bigint, client_id bigint, attributes map<text, text> , segments set<text>,
primary key (id, client_id)) with clustering order by (client_id desc)
AND compression={'sstable_compression' : 'SnappyCompressor'}  AND compaction = {'class': 'LeveledCompactionStrategy'} ;
 */

import com.anishek.Constants;
import com.anishek.pattern2.Callback;
import com.anishek.threading.RunnerFactory;
import com.anishek.threading.Threaded;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

/**
 * This is to populate a single table with sections of the table that will have
 * mutually exclusive updates. segments and attributes are mutually exclusive per client
 * though both have to be per user.
 * should be more efficient than MultipleTablesTest
 */
public class SingleTableTest {

    private static final String CONTACT_POINT = "contact.point";
    private static final String NUM_THREADS = "num.threads";
    private static final String NUM_PARTITIONS = "num.partitions";
    private Cluster cluster;
    private Session session;

    @Before
    public void setUp() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 30);
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 30);
        String contactPoint = System.getProperty(CONTACT_POINT);
        System.out.println("using contact point: " + contactPoint);
        cluster = Cluster.builder()
                .withPoolingOptions(poolingOptions)
                .addContactPoint(contactPoint)
                .build();
        session = cluster.connect("activity_log");
    }

    @Test
    public void populateTTLedValuesForSpecifiedNumberOfIds() throws Exception {

        if (Boolean.parseBoolean(System.getProperty("recreate.table"))) {
            recreateTable();
        }
        int NUM_OF_THREADS = Integer.parseInt(System.getProperty(NUM_THREADS));
        long NUM_OF_KEYS = Long.parseLong(System.getProperty(NUM_PARTITIONS));

        HashMap<String, Object> otherArguments = new HashMap<>();
        otherArguments.put(SingleInsertRunnable.TTL, Integer.parseInt(System.getProperty(SingleInsertRunnable.TTL)));
        otherArguments.put(SingleInsertRunnable.SEGMENTS_TTL, Integer.parseInt(System.getProperty(SingleInsertRunnable.SEGMENTS_TTL)));
        otherArguments.put(Constants.SESSION, session);
        Threaded threaded = new Threaded(NUM_OF_KEYS, NUM_OF_THREADS, new RunnerFactory(SingleInsertRunnable.class, otherArguments));
        List run = threaded.run(new Callback<Long>());

    }

    private void recreateTable() {
        session.execute("drop table if exists test");
        session.execute("create table test (id bigint, client_id bigint, attributes map<text, text> , segments set<text>," +
                "primary key (id, client_id)) with clustering order by (client_id desc)" +
                "AND compression={'sstable_compression' : 'SnappyCompressor'}  AND compaction = {'class': 'LeveledCompactionStrategy'} ;");
    }
}
