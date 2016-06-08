/* ./src/main/java/com/oracle/bdglue/publisher/asynchbase/AsyncHbaseProperties.java 
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Properties related to connecting to the HBase asynchronous API via 
 * the AsyncHbasePbulsher and the BDGlueAsyncHbaseSink.
 */
public class AsyncHbaseProperties {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncHbaseProperties.class);

    /**
     * The batch size we want to use. This is for Flume BDGlueAsyncHbaseSink 
     * sink configuration. There is a corresponding property in the BDGlue properties 
     * file that is used by the AsyncHbasePublisher.
     */
    public static final String BATCHSIZE_FLUME = "batchSize";
    /**
     * The timeout value after which we will pronounce a transaction as failed. This is for 
     * Flume BDGlueAsyncHbaseSink sink configuration. There is a corresponding property 
     * in the BDGlue properties file that is used by the AsyncHbasePublisher.
     */
    public static final String TIMEOUT_FLUME = "timeout";


    /**
     * The default batchSize to use. Applies to both the BDGlueAsyncHbaseSink
     * and AsyncHbasePublisher.
     */
    public static final String DEFAULT_BATCHSIZE = "100";
    /**
     * The default timeout to use. Applies to both the BDGlueAsyncHbaseSink
     * and AsyncHbasePublisher.
     */
    public static final String DEFAULT_TIMEOUT = "60000";

    /**
     * The name of the Flume event header "property" that will contain
     * the target table name.
     */
    public static final String TABLE_HDR = "table";
    /**
     * The name of the Flume event header "property" that will contain
     * the target column family name for this event.
     */
    public static final String COLUMN_FAMILY_HDR = "columnFamily";
    /**
     * The name of the Flume event header "property" that will contain
     * the target key value for this event.
     */
    public static final String ROWKEY_HDR = "rowKey";
}
