package com.mayikt;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author:chenhongyan
 * @date:2021/6/14 18:57
 */
public class TestKeySpace {

    Session session = null;


    @Before
    public void init() {
        String host = "192.168.229.103";
        int port = 9042;
        Cluster cluster = Cluster.builder()
                .addContactPoint(host)
                .withPort(port)
                .build();
        session = cluster.connect();

    }


    @Test
    public void findKeySpace() {
        List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
        for (KeyspaceMetadata keyspace : keyspaces) {
            System.out.println(keyspace.getName());
        }
    }

    @Test
    public void createKeySpace() {

        /*String sql = "create keyspace shcool with replication = {'class':'SimpleStratgy','replication_factor':3}";
        session.execute(sql);*/
        Map<String, Object> replication = new HashMap<String, Object>();
        replication.put("class", "SimpleStratgy");
        replication.put("replication_factor", 3);
        Statement options = SchemaBuilder.createKeyspace("school")
                .ifNotExists()
                .with()
                .replication(replication);
        session.execute(options);
    }

    @Test
    public void deleteKeySpace() {
        Statement options = SchemaBuilder.dropKeyspace("school")
                .ifExists();
        session.execute(options);
    }


    @Test
    public void alterKeySpace() {
        Map<String, Object> replication = new HashMap<String, Object>();
        replication.put("class", "SimpleStratgy");
        replication.put("replication_factor", 1);
        Statement options = SchemaBuilder.alterKeyspace("school")
                .with()
                .replication(replication);
        session.execute(options);
    }
}
