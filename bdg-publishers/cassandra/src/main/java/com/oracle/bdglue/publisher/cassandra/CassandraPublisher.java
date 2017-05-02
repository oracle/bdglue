/* ./src/main/java/com/oracle/bdglue/publisher/kafka/CassandraPublisher.java
 *
 * Copyright 2016 Oracle and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oracle.bdglue.publisher.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

import com.datastax.driver.core.Session;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.encoder.EventHeader;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;
import com.oracle.bdglue.publisher.BDGluePublisher;

import java.util.HashMap;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish event data to Cassandra. This implementation makes use of the
 * Cassandra java API developed by DataStax and is available for download
 * from GitHub at https://github.com/datastax/java-driver.
 *
 */
public class CassandraPublisher implements BDGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraPublisher.class);

    private Cluster cluster;
    private Session session;
    private Metadata clusterMetadata;
    private BatchStatement batch;
    private HashMap<String, CassandraTable> map;
    private int opCounter = 0;
    private int totalOps = 0;
    private int batchSize = 0;
    private boolean insertOnly;
    private int flushFreq = 500;
    private Timer timer;
    private TimerTask timerTask;


    private String cassandraNode;


    public CassandraPublisher() {
        super();

        PropertyManagement properties = PropertyManagement.getProperties();
        cassandraNode =
            properties.getProperty(BDGluePropertyValues.CASSANDRA_CONNECT_NODE,
                                   BDGluePropertyValues.CASSANDRA_CONNECT_NODE_DEFAULT);
        batchSize =
            properties.asInt(BDGluePropertyValues.CASSANDRA_BATCH_SIZE,
                             BDGluePropertyValues.CASSANDRA_BATCH_SIZE_DEFAULT);
        flushFreq =
            properties.asInt(BDGluePropertyValues.CASSANDRA_FLUSH_FREQ,
                             BDGluePropertyValues.CASSANDRA_FLUSH_FREQ_DEFAULT);
        insertOnly =
            properties.asBoolean(BDGluePropertyValues.CASSANDRA_INSERT_ONLY,
                                 BDGluePropertyValues.CASSANDRA_INSERT_ONLY_DEFAULT);

        batch = new BatchStatement(BatchStatement.Type.LOGGED);
        map = new HashMap<>();
        timer = new Timer();

        // reinitialize things
        publishEvents();
    }

    @Override
    public void connect() {
        LOG.info("Connecting to Cassandra cluster at node: {}", cassandraNode);
        cluster = Cluster.builder().addContactPoint(cassandraNode).build();
        clusterMetadata = cluster.getMetadata();
        LOG.info("Connected to cluster: {}", clusterMetadata.getClusterName());
        logClusterInfo();
        LOG.info("creating Cassandra session");
        session = cluster.connect();
    }
    

    @Override
    public void cleanup() {
        LOG.info("disconnecting from Cassandra cluster");
        // flush any pending events
        publishEvents();
        // clean up the timer
        timer.cancel();

        cluster.close();
    }

    @Override
    public void writeEvent(String threadName, EventData evt) {
        synchronized (this) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("event #{}", totalOps);
            }

            batch.add(processEvent(evt));

            opCounter++;
            totalOps++;
            // publish batch and commit.
            if (opCounter >= batchSize) {
                publishEvents();
            }
        }
    }

    /**
     * Simple timer to ensure that we periodically flush whatever we have queued
     * in the event that we haven't received "batchSize" events by the time
     * that the timer has expired.
     */
    private class FlushQueuedEvents extends TimerTask {
        public void run() {

            publishEvents();
        }
    }

    /**
     * publish all events that we have queued up to Kafka. This is called both by
     * the timer and by writeEvent(). Need to be sure they don't step on each other.
     */
    private void publishEvents() {
        synchronized (this) {
            if (timerTask != null) {
                timerTask.cancel();
                timerTask = null;
            }
            if (opCounter > 0) {
                session.execute(batch);
                opCounter = 0;
                batch.clear();
            }

            // ensure that we don't keep queued events around very long.
            timerTask = new FlushQueuedEvents();
            timer.schedule(timerTask, flushFreq);
        }
    }

    /**
     * Process the event, and creating necessary meta info if needed.
     * 
     * @param evt The event to process.
     * @return a BoundStatement representing this event.
     */
    private BoundStatement processEvent(EventData evt) {
        CassandraTable cassandraTable;
        String tableName = evt.getMetaValue(EventHeader.TABLE);

        if (!map.containsKey(tableName)) {
            cassandraTable = new CassandraTable(session, tableName);
            map.put(tableName, cassandraTable);
        } else {
            cassandraTable = map.get(tableName);
        }

        return cassandraTable.getBoundStatement((DownstreamOperation) evt.eventBody(), insertOnly);
    }

    /**
     * Log the information related to the hosts in this cluster.
     */
    private void logClusterInfo() {
        LOG.info("*** Cassandra Cluster host information ***");
        for (Host host : clusterMetadata.getAllHosts()) {
            LOG.info("Datacenter: {}; Host: {}; Rack: {}", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        LOG.info("*** END Cassandra Cluster host information ***");
    }
}
