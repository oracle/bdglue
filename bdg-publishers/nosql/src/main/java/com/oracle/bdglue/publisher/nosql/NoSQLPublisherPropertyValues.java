/* ./src/main/java/com/oracle/bdglue/publisher/nosql/NoSQLPublisherPropertyValues.java 
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
package com.oracle.bdglue.publisher.nosql;


/**
 * This class contains configuration constants used by the
 * Big Data Glue NoSQL Publisher, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 *
 */
public final class NoSQLPublisherPropertyValues {
    /**
     * Properties related to NoSQLPublisher.
     */
    /**
     * The name of the NoSQL host.
     */
    public static final String NOSQL_HOST = "bdglue.nosql.host";
    /**
     * The default host to connect to if one isn't specified.
     */
    public static final String NOSQL_HOST_DEFAULT = "localhost";
    /**
     * The port where NoSQL is listening.
     */
    public static final String NOSQL_PORT = "bdglue.nosql.port";
    /**
     * The default NoSQL port.
     */
    public static final String NOSQL_PORT_DEFAULT = "5000";
    /**
     * The name of the NoSQL KVStore.
     */
    public static final String NOSQL_KVSTORE = "bdglue.nosql.kvstore";
    /**
     * The default NoSQL datastore name.
     */
    public static final String NOSQL_KVSTORE_DEFAULT = "kvstore";
    /**
     * The durability model for NoSQL transactions.
     */
    public static final String NOSQL_DURABILITY = "bdglue.nosql.durability";
    /**
     * The default NoSQL durability model.
     */
    public static final String NOSQL_DURABILITY_DEFAULT = "WRITE_NO_SYNC";
    /**
     * Specify whether to use the KV or Table API when writing to NoSQL.
     */
    public static final String NOSQL_API = "bdglue.nosql.api";
    /**
     * The default NoSQL API to use if one isn't specified.
     */
    public static final String NOSQL_API_DEFAULT = "kv_api";
    
    
    
    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private NoSQLPublisherPropertyValues() {
        super();
    }
}
