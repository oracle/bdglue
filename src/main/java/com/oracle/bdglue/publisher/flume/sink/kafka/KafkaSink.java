/* ./src/main/java/com/oracle/bdglue/target/flume/sink/kafka/KafkaSink.java 
 *
 * Copyright 2015 Oracle and/or its affiliates.
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
package com.oracle.bdglue.publisher.flume.sink.kafka;


import com.google.common.base.Throwables;

import com.oracle.bdglue.publisher.kafka.KafkaHelper;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unabashedly derrived from org.apache.flume.sink.kafka.KafkaSink. Modified from
 * the original to let us use the pre-existing table name that we are sending along
 * in the Flume Event Header as the Kafka "Event Key."
 *
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and
 * a channel.
 * The message can be any event and the key is a string that we read from the
 * header
 * For use of partitioning, use an interceptor to generate a header with the
 * partition key
 * <p>
 * Mandatory properties are:
 * brokerList -- can be a partial list, but at least 2 are recommended for HA
 * <p>
 * however, any property starting with "kafka." will be passed along to the
 * Kafka producer
 * Read the Kafka producer documentation to see which configurations can be used
 * <p>
 * Optional properties
 * topic - there's a default, and also - this can be in the event header if
 * you need to support events with
 * different topics
 * batchSize - how many messages to process in one batch. Larger batches
 * improve throughput while adding latency.
 * requiredAcks -- 0 (unsafe), 1 (accepted by at least one broker, default),
 * -1 (accepted by all brokers)
 * <p>
 * header properties (per event):
 * topic
 * key
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
 

    private KafkaHelper helper;
    private int batchSize;

    public KafkaSink() {
        super();

    }

    /**
     * Process the Flume events, batching as appropriate to improve performance.
     * 
     * @return the resulting status of this call.
     * @throws EventDeliveryException if an error occurs
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;


        try {
            long processedEvents = 0;

            transaction = channel.getTransaction();
            transaction.begin();

            helper.clearMessageList();
            for (; processedEvents < batchSize; processedEvents += 1) {
                event = channel.take();

                if (event == null) {
                    // no events available in channel at this time
                    // so leave and process what we have.
                    break;
                }
                helper.process(event);

                LOG.debug("event #{}", processedEvents);

            }

            // publish batch and commit.
            if (processedEvents > 0) {
                helper.sendToProducer();
            }

            transaction.commit();

        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            LOG.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    transaction.rollback();
                } catch (Exception e) {
                    LOG.error("Transaction rollback failed", e);
                    throw Throwables.propagate(e);
                }
            }
            throw new EventDeliveryException(errorMsg, ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

        return result;
    }

    /**
     * Start the sink and connect to Kafka.
     */
    @Override
    public synchronized void start() {
        helper.connect();

        super.start();
    }

    /**
     * Stop the sink and disconnect from Kafka.
     */
    @Override
    public synchronized void stop() {
        helper.cleanup();

        super.stop();
    }


    /**
     * Call the helper to configure the sink and generate properties for the Kafka Producer.
     *
     * @param context the configuration Context
     */
    @Override
    public void configure(Context context) {
        helper = new KafkaHelper();

        helper.configure(context);

    }
}
