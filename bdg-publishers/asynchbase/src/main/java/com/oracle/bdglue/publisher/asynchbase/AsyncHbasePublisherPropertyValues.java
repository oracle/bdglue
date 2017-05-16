/* ./src/main/java/com/oracle/bdglue/publisher/asynchbase/AsyncHbasePublisherPropertyValues.java 
 *
 * Copyright 2017 Oracle and/or its affiliates.
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


/**
 * This class contains configuration constants used by the
 * Big Data Glue Async Hbase Publisher, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 *
 */
public final class AsyncHbasePublisherPropertyValues {
    /**
     * Properties related to the AsyncHbasePublisher.
     */
    
    /**
     * The batchsize for writing records to HBase. Larger values can result in some improvement
     * of performance.
     */
    public static final String ASYNC_HBASE_BATCHSIZE = "bdglue.async-hbase.batchSize";
    /**
     * The default batchSize to use. 
     */
    public static final String DEFAULT_BATCHSIZE = "100";
   
    /**
     * Timeout value after which asynchronous calls that have not returned
     * are declared unsuccessful.
     */
    public static final String ASYNC_HBASE_TIMEOUT = "bdglue.async-hbase.timeout";
    /**
     * The default timeout to use. 
     */
    public static final String DEFAULT_TIMEOUT = "60000";

    
    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private AsyncHbasePublisherPropertyValues() {
        super();
    }
}
