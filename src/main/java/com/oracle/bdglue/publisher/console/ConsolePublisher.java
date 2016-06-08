/* ./src/main/java/com/oracle/bdglue/publisher/console/ConsolePublisher.java 
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
package com.oracle.bdglue.publisher.console;

import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.encoder.EventHeader;

import com.oracle.bdglue.publisher.BDGluePublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple publisher that writes all data to the console (i.e. standard out). 
 * This class is useful for smoke testing a configuration. No validation of the
 * incoming data is performed. It is assumed that it has been encoded in a useful 
 * format. Use of the JSON encoder for this is highly recommended.
 */
public class ConsolePublisher implements BDGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(ConsolePublisher.class);
    
    public ConsolePublisher() {
        super();
        
        LOG.info("ConsolePublisher()");
    }
    /**
     * Connect. A NO OP in this case.
     */
    @Override
    public void connect() {
        // Nothing to do for console I/O
    }

    /**
     * Write the data to the console.
     * 
     * @param threadName the name of the thread, used for logging.
     * @param evt the event data
     */
    @Override
    public void writeEvent(String threadName, EventData evt) {
        System.out.format("%s:%s : %s %n", threadName, 
                          evt.getMetaValue(EventHeader.TABLE), (String)evt.eventBody());
    }

    /**
     * Clean up. A NO OP in this case.
     */
    @Override
    public void cleanup() {
        // Nothing to do for console I/O
    }
}
