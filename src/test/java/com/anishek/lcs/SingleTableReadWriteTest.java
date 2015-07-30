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
public class SingleTableReadWriteTest {

    private static final String CONTACT_POINT = "contact.point";
    private static final String NUM_THREADS = "num.threads";
    public static final String NUM_PARTITIONS = "num.partitions";
    public static final String NUM_OPERATIONS = "operations";
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
    public void doReadWrite() throws Exception {
        int threads = Integer.parseInt(System.getProperty(NUM_THREADS));
        long keys = Long.parseLong(System.getProperty(NUM_PARTITIONS));
        long operations = Long.parseLong(System.getProperty(NUM_OPERATIONS));

        HashMap<String, Object> otherArguments = new HashMap<>();
        otherArguments.put(Constants.TTL, Integer.parseInt(System.getProperty(Constants.TTL)));
        otherArguments.put(Constants.SEGMENTS_TTL, Integer.parseInt(System.getProperty(Constants.SEGMENTS_TTL)));
        otherArguments.put(Constants.SESSION, session);
        otherArguments.put(NUM_PARTITIONS, keys);
        Threaded threaded = new Threaded(operations, threads, new RunnerFactory(SingleReadWriteRunnable.class, otherArguments));
        List run = threaded.run(new Callback<Long>());

    }
}
