/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafkaRegistryPublisher.java
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
package com.oracle.bdglue.publisher.kafka;

import com.oracle.bdglue.common.PropertyManagement;

import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.publisher.BDGluePublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRegistryPublisher implements BDGluePublisher {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRegistryPublisher.class);

    private String topic;
    private KafkaRegistryHelper helper;
    private int processedEvents = 0;


    public KafkaRegistryPublisher() {
        super();

        helper = new KafkaRegistryHelper();
        topic = KafkaProperties.getKafkaTopic();
        helper.configure(PropertyManagement.getProperties());
    }


    /**
     * Connect to Kafka.
     */
    @Override
    public void connect() {
        helper.connect();
    }

    /**
     * Prepare the event to be written to the Kafka topic. Flush the
     * accumulated events to Kafka once "batchSize" has been reached.
     * <p>
     * Note: accumulated events will also be flushed when a simple
     * timer expires to prevent data from laying around indefinitely
     * at times of low volume.
     *
     * @param threadName the name of this thread
     * @param evt the EventData to publish
     */
    @Override
    public void writeEvent(String threadName, EventData evt) {


        helper.process(evt);

        if (LOG.isDebugEnabled()) {
            LOG.debug("event #{}", processedEvents);
        }

        processedEvents++;
    }

    /**
     * Clean up and close the connection.
     */
    @Override
    public void cleanup() {
        // clean up Kafka
        helper.cleanup();
    }

}
