/* ./src/main/java/com/oracle/bdglue/publisher/nosql/NoSQLPublisher.java 
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
package com.oracle.bdglue.publisher.nosql;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.publisher.BDGluePublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish data to the Oracle NoSQL database. This publisher supports
 * both the Table and KV APIs, which can be configured in the properties.
 */
public class NoSQLPublisher implements BDGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(NoSQLPublisher.class);

    private PropertyManagement properties;
    private NoSQLHelper helper; 
    
    /**
     * Create an instance of the Oracl NoSQL publisher for the specified
     * NoSQL API: TABLE or KV.
     */
    public NoSQLPublisher() {
        super();
        
        String kvAPI;
        
        LOG.info("NoSQLPublisher()");
        properties = PropertyManagement.getProperties();
        kvAPI = properties.getProperty(BDGluePropertyValues.NOSQL_API, BDGluePropertyValues.NOSQL_API_DEFAULT);
        helper = NoSQLHelper.helperFactory(kvAPI);
        helper.configure(properties);
    }

    /**
     * Connect to Oracle NoSQL.
     */
    @Override
    public void connect() {
        helper.connect();
        helper.initialize();
    }

    /**
     * Write the event to the appropriate Oracle NoSQL API.
     * 
     * @param threadName the name of the thread for this publisher
     * @param evt the BDGlue event we are processing.
     */
    @Override
    public void writeEvent(String threadName, EventData evt) {
        LOG.debug("New event coming in, begin processing...");
 

        // This try clause includes whatever Channel operations you want to do
        try {

            if (null != evt) {
                LOG.debug("Event received: " + evt.toString());
                helper.process(evt);
                LOG.trace("Event stored in KV store");
            } else {
                
            }
        } catch (Throwable t) {
            // Rollback transaction
            
            t.printStackTrace();

            LOG.error("Error processing event!");
            LOG.error(t.toString());
            LOG.error(t.getMessage());

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            LOG.trace("Transaction closed.");
        }

    }

    /**
     * Clean up and disconnect from Oracle NoSQL.
     */
    @Override
    public void cleanup() {
        helper.cleanup();
    }
}
