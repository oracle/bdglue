/* ./src/main/java/com/oracle/bdglue/publisher/flume/sink/nosql/BDGlueNoSQLSinkConfig.java 
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
package com.oracle.bdglue.publisher.flume.sink.nosql;

/**
 * Class to help define configuration properties associated with the NoSQL Sink.
 */
public final class BDGlueNoSQLSinkConfig {
    /**
     * Default constructor. Private to prevent instantiation of the class.
     */
    private BDGlueNoSQLSinkConfig() {
        super();
    };

    /**
     * The host that should be used to connect to.
     */
    public static final String KVHOST = "kvHost";

    /**
     * The port that should be used to connect to.
     */
    public static final String KVPORT = "kvPort";

    /**
     * The kvStore name to connect to.
     */
    public static final String KVSTORE = "kvStoreName";

    /**
     * The durability policy that should be used.<br>
     * SYNC = Commit onto disk at master and replicate to simple majority of replicas<br>
     * WRITE_NO_SYNC = Commit onto disk at master but do not replicate<br>
     * NO_SYNC = Commit only into master memory and do not replicate
     */
    public static final String DURABILITY = "durability";

    /**
     * The policy of the key retrieval.
     */
    public static final String KVAPI = "kvapi";

    /**
     * The values for the kvapi property.<br>
     * KV_API = interface via the NoSQL KV API<br>
     * TABLE_API = interface via the NoSQL TABLE API<br>
     */
    public static enum API_TYPE {
        KV_API,
        TABLE_API
    };
 
}
