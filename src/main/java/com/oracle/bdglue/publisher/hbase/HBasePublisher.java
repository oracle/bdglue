/* ./src/main/java/com/oracle/bdglue/publisher/hbase/HBasePublisher.java 
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
package com.oracle.bdglue.publisher.hbase;

import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;


import com.oracle.bdglue.publisher.BDGluePublisher;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a BDGlue Publisher coded to support the HBase 1.0 API.
 *
 *
 */
public class HBasePublisher implements BDGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(HBasePublisher.class);
    
    private int processedEvents = 0;
    private int batchSize = 5;
    private int flushFreq = 500;
    private Timer timer;
    private TimerTask timerTask;
    
    public HBasePublisher() {
        super();
        
        timer = new Timer();
        //batchSize = KafkaProperties.getKafkaBatchSize();
        //flushFreq = KafkaProperties.getKafkaFlushFreq();
        init();
        
        
        // reinitialize things
        publishEvents();
        
       
    }
    
    private void init() {
        // read and parse JSON mapping file
    }

    @Override
    public void connect() {
        // TODO Implement this method
    }

    @Override
    public void writeEvent(String threadName, EventData evt) {
        synchronized (this) {
            try {

                // TODO: process the event

                if (LOG.isDebugEnabled()) {
                    LOG.debug("event #{}", processedEvents);
                }
            } catch (Exception ex) {
                LOG.error("Failed to write to hbase", ex);
            }
            processedEvents++;
            // publish batch and commit.
            if (processedEvents >= batchSize) {
                publishEvents();
            }
        }

    }

    @Override
    public void cleanup() {
        // TODO Implement this method
    }
    
    private void writeToHbase() {
        // TODO: write queued events to HBase
    }
    
    private void clearHbaseQueue() {
        // TODO: not sure we'll need this one
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
        synchronized (this) {
            if (timerTask != null) {
                timerTask.cancel();
                timerTask = null;
            }
            if (processedEvents > 0) {
                writeToHbase();
            }

            // now that we have written, clean up and prepare to queue
            // up the next batch.
            clearHbaseQueue();
            processedEvents = 0;
            // ensure that we don't keep queued events around very long.
            timerTask = new FlushQueuedEvents();
            timer.schedule(timerTask, flushFreq);
        }
    }
}
