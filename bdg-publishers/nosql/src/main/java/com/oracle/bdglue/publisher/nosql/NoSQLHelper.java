/* ./src/main/java/com/oracle/bdglue/publisher/nosql/NoSQLHelper.java 
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

import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class for common code between the NoSQL KV and Table helper classes.
 */
public abstract class NoSQLHelper {
    private static final Logger LOG = LoggerFactory.getLogger(NoSQLHelper.class);
    public static enum API_TYPE {
        KV_API,
        TABLE_API
    }

    private String kvHost;
    private String kvPort;
    private String kvStoreName;
    private String kvStoreDurability;
    private String apiString;
    private API_TYPE apiType;
    private KVStore kvStore;

    
    public NoSQLHelper() {
        super();
    }

    /**
     * A factory methoid that returns an instance of the appropriate subclass.
     * 
     * @param kvAPI the API type (KV or Table)
     * @return the helper object for the appropriate NoSQL API
     */
    public static NoSQLHelper helperFactory(String kvAPI) {
        NoSQLHelper helper;
        
        if (kvAPI.equalsIgnoreCase("kv_api"))
            helper = new NoSQLKVHelper();
        else if (kvAPI.equalsIgnoreCase("table_api")) 
            helper = new NoSQLTableHelper();
        else {
            helper = new NoSQLKVHelper();
            LOG.error("Unknown API specified: {}. Defaulting to kv_api", kvAPI);
        }
        
        return helper;
    }



    /**
     * Configure the class based on the properties for the user exit.
     * @param properties the properties we want to leverage
     */
    public void configure(PropertyManagement properties) {
        kvHost = properties.getProperty(NoSQLPublisherPropertyValues.NOSQL_HOST, 
                                        NoSQLPublisherPropertyValues.NOSQL_HOST_DEFAULT);
        kvPort = properties.getProperty(NoSQLPublisherPropertyValues.NOSQL_PORT, 
                                        NoSQLPublisherPropertyValues.NOSQL_PORT_DEFAULT);
        kvStoreName = properties.getProperty(NoSQLPublisherPropertyValues.NOSQL_KVSTORE, 
                                             NoSQLPublisherPropertyValues.NOSQL_KVSTORE_DEFAULT);
        kvStoreDurability = properties.getProperty(NoSQLPublisherPropertyValues.NOSQL_DURABILITY,
                                                   NoSQLPublisherPropertyValues.NOSQL_DURABILITY_DEFAULT);
        setAPIType(properties.getProperty(NoSQLPublisherPropertyValues.NOSQL_API, 
                                          NoSQLPublisherPropertyValues.NOSQL_API_DEFAULT));
        
        logConfiguration();
    }
    
    /**
     * Log the configuraiton information.
     */
    public void logConfiguration() {
        LOG.info("Configuration settings:");
        LOG.info("host: " + kvHost);
        LOG.info("port: " + kvPort);
        LOG.info("kvStoreName: " + kvStoreName);
        LOG.info("durability: " + kvStoreDurability);
        LOG.info("API: " + apiString);
    }

    /**
     * Set the API type (Table or KV API).
     * 
     * @param kvAPI The API type
     */
    public void setAPIType(String kvAPI) {
        apiString = kvAPI;
        if (kvAPI.equalsIgnoreCase("kv_api"))
            apiType = API_TYPE.KV_API;
        else if (kvAPI.equalsIgnoreCase("table_api")) 
            apiType = API_TYPE.TABLE_API;
        else {
            apiType = API_TYPE.KV_API;
            LOG.error("Unknown API specified: {}. Defaulting to kv_api", kvAPI);
        }
    }

    /**
     * @return the API type (Table or KV) as an int.
     */
    public API_TYPE getAPIType() {
        return apiType;
    }
    /**
     *
     * @return  handle to the NoSQL KVStore
     */
    public KVStore getKVStore() {
        return kvStore;
    }
    
    /**
     * Connect to the NoSQL KVStore.
     */
    public void connect() {
        // Build KV store config
        KVStoreConfig config = new KVStoreConfig(kvStoreName, kvHost + ":" + kvPort);

        // Set durability configuration
        switch (kvStoreDurability) {
        case "SYNC":
            config.setDurability(Durability.COMMIT_SYNC);
            break;
        case "WRITE_NO_SYNC":
            config.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
            break;
        case "NO_SYNC":
            config.setDurability(Durability.COMMIT_NO_SYNC);
            break;
        default:
            LOG.info("Invalid durability setting: " + kvStoreDurability);
            LOG.info("Proceeding with default WRITE_NO_SYNC");
            config.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
            break;
        }

        // Connect to KV store
        try {
            kvStore = KVStoreFactory.getStore(config);
            LOG.info("Connection to KV store established");
        } catch (FaultException e) {
            LOG.error("Could not establish connection to KV store!");
            LOG.error(e.getMessage());
            // Throw error
            throw e;
        }
    }
    
    /**
     * Disconnect from the KVStore.
     */
    public void cleanup() {
        kvStore.close();
        LOG.trace("Connection to KV store closed");
    }
    
    /**
     * Perform any needed initialization of the serializer.
     * 
     */
    public abstract void initialize();
  
    
    /**
     * Process the received BDGlue event.
     * @param event the BDGlue event we want to process
     */
    public abstract void process(EventData event);
}
