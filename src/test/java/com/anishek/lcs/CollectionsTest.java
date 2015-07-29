package com.anishek.lcs;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;

/*
Create table:

create table test (all_spark_id bigint, client_id bigint, attributes map<text, text> , segments set<text>,  primary key (all_spark_id, client_id)) with clustering order by (client_id desc);




 */
public class CollectionsTest {

    public static final int ELEMENTS_LIMIT_64K = 64 * 1024 * 1024;
    private static final String CONTACT_POINTS = "contact.points";
    private Cluster localhost;

    @Before
    public void setUp() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 30);
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 30);
        String contactPoints = System.getProperty(CONTACT_POINTS);
        System.out.println("using contact points: " + contactPoints);
        localhost = Cluster.builder()
                .withPoolingOptions(poolingOptions)
                .addContactPoints(contactPoints)
                .build();
    }


    @Test
    public void limitOfItemsInCollections() {
        Session session = localhost.connect("activity_log");
        session.execute("drop table test;");
        session.execute("Create table test(id bigint, col set<bigint>, primary key(id)); ");
        session.execute("insert into test(id) values (1)");

        for (int i = 0; i < ELEMENTS_LIMIT_64K; i++) {
            session.execute("update test set col = col + {" + i + "}");
        }
        session.close();
    }

}
