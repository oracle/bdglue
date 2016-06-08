/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafkaPublisher.java 
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
package com.oracle.bdglue.publisher.kafka;

import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.encoder.ParallelEncoder;
import com.oracle.bdglue.publisher.BDGluePublisher;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish event data to Kafka. All events are delivered to a single
 * topic. Events are partitioned by topic "key". The key currently defaults 
 * to the table name. We may provide the ability to override this in the
 * future.
 * 
 */
public class KafkaPublisher implements BDGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisher.class);

    private String topic;
    private KafkaHelper helper;
    private int processedEvents = 0;
    private int batchSize = 5;
    private int flushFreq = 500;
    private Timer timer;
    private TimerTask timerTask;

    public KafkaPublisher() {
        super();
        helper = new KafkaHelper();
        timer = new Timer();
        topic = KafkaProperties.getKafkaTopic();
        batchSize = KafkaProperties.getKafkaBatchSize();
        flushFreq = KafkaProperties.getKafkaFlushFreq();
        helper.configure(PropertyManagement.getProperties());
        // reinitialize things
        publishEvents();
    }

    /**
     * Connect to Kafka.
     */
    @Override
    public void connect() {
        helper.connect();
    }

    /**
     * Prepare the event to be writtent to the Kafka topic. Flush the
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

        synchronized (helper) {
            try {

                helper.process(evt);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("event #{}", processedEvents);
                }
            } catch (Exception ex) {
                LOG.error("Failed to publish events", ex);
            }
            processedEvents++;
            // publish batch and commit.
            if (processedEvents >= batchSize) {
                publishEvents();
            }
        }

    }

    /**
     * Clean up and close the connection.
     */
    @Override
    public void cleanup() {
        // flush any pending events
        publishEvents();
        // clean up the timer
        timer.cancel();
        // clean up Kafka
        helper.cleanup();
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
    public void publishEvents() {
        synchronized (helper) {
            if (timerTask != null) {
                timerTask.cancel();
                timerTask = null;
            }
            if (processedEvents > 0) {
                helper.sendToProducer();
            }
            
          
            // now that we have written, clean up and prepare to queue
            // up the next batch.
            helper.clearMessageList();
            processedEvents = 0;
            // ensure that we don't keep queued events around very long.
            timerTask = new FlushQueuedEvents();
            timer.schedule(timerTask, flushFreq);
        }
    }
}
