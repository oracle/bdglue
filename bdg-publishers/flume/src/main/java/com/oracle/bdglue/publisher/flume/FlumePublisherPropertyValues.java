/* ./src/main/java/com/oracle/bdglue/publisher/flume/FlumeyPublisherPropertyValues.java 
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
package com.oracle.bdglue.publisher.flume;


/**
 * This class contains configuration constants used by the
 * Big Data Glue Flume Publisher, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 *
 */
public final class FlumePublisherPropertyValues {
    /**
     * Properties related to FlumePublisher.
     */
    
    /**
     * The name of the target host that we will connect to.
     */
    public static final String FLUME_HOST = "bdglue.flume.host";
    /**
     * The default target host name.
     */
    public static final String FLUME_HOST_DEFAULT = "localhost";
    
    /**
     * The type or RPC connection: avro-rpc, thrift-rpc.
     */
    public static final String FLUME_RPC_TYPE = "bdglue.flume.rpc.type";
    /**
     * The default RPC_TYPE.
     */
    public static final String FLUME_RPC_TYPE_DEFAULT = "avro-rpc";
    /**
     * The port number on the target host that we will connect to.
     */
    public static final String FLUME_PORT = "bdglue.flume.port";
    /**
     * The default target port number.
     */
    public static final String FLUME_PORT_DEFAULT = "41414";
        
    /**
     * number of times to try to reconnect on RPC connection failure.
     */
    public static final String FLUME_RPC_RETRIES = "bdglue.flume.rpc.retries";
    /**
     * the default number of times to retry a lost connection.
     */
    public static final String FLUME_RPC_RETRY_DEFAULT = "5";
    /**
     * The number of seconds to pause between each RPC connection retry.
     */
    public static final String FLUME_RPC_RETRY_DELAY = "bdglue.flume.rpc.retry-delay";
    /**
     * The default amount of time to pause before retrying a failed attempt to connect.
     */
    public static final String FLUME_RPC_RETRY_DELAY_DEFAULT = "10";

    
    
    
    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private FlumePublisherPropertyValues() {
        super();
    }
}
