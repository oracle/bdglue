/* ./src/main/java/com/oracle/bdglue/publisher/asynchbase/AsyncHbasePublisher.java 
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
package com.oracle.bdglue.publisher.asynchbase;

import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.publisher.BDGluePublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish data to HBase using the Async HBase API. This API is faster than the
 * "normal" HBase API, but much more complex to implement and understand. It also 
 * does not support Kerberos authentication.
 * 
 */
public class AsyncHbasePublisher implements BDGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncHbasePublisher.class);

    private PropertyManagement properties;
    private int processedEvents = 0;
    private AsyncHbaseHelper helper;
    private int batchSize;
    
    public AsyncHbasePublisher() {
        super();
        
        LOG.info("HBasePublisher()");
        properties = PropertyManagement.getProperties();
        helper = new AsyncHbaseHelper();;
        helper.configure(properties);
        batchSize = helper.getBatchSize();
    }

    /**
     * Connect to HBase.
     */
    @Override
    public void connect() {
        helper.connect();
        helper.initialize();
    }

    /**
     * Write the event to HBase. 
     * 
     * @param threadName the name of this thread for logging purposes
     * @param evt the EventData that we will be processing
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
                processedEvents = 0;
            }
        }

    }

    /**
     * Clean up and disconnect from the target.
     */
    @Override
    public void cleanup() {
       helper.cleanup();
    }
    
    /**
     * Check to see if the last round of events completed successfully.
     * Prepare for the next round.
     */
    private void publishEvents() {
        // check return codes from the callbacks.
        if (helper.checkCallbacks()) {
            LOG.error("Could not write events to Hbase.");
        }
        
        // initialize the callbacks for processing.
        helper.initializeCallbacks();
    }
}
