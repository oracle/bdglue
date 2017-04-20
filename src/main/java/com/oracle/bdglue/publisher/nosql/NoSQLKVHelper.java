/* ./src/main/java/com/oracle/bdglue/publisher/nosql/NoSQLKVHelper.java 
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

import com.oracle.bdglue.encoder.EventData;

import java.util.Map;

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.avro.RawAvroBinding;
import oracle.kv.avro.RawRecord;

import org.apache.avro.Schema;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class that contains shared functionality for the NoSQL
 * KV API for Flulme and direct connections.
 */
public class NoSQLKVHelper extends NoSQLHelper {
    private static final Logger LOG = LoggerFactory.getLogger(NoSQLKVHelper.class);

    /**
     * Binding needed for storing already-encoded
     * Avro data as a "value".
     */
    RawAvroBinding binding;
    /**
     * all schemas from the kv store.
     */
    Map<String, Schema> schemas;

    public NoSQLKVHelper() {
        super();
    }


    /**
     * Perform any needed initialization of this event serializer.
     */
    @Override
    public void initialize() {
        binding = getKVStore().getAvroCatalog().getRawBinding();
        schemas = getKVStore().getAvroCatalog().getCurrentSchemas();
    }
    
    /**
     * Process the BDGlue event.
     * 
     * @param event the EventData
     */
    @Override
    public void process(EventData event) {
        processAvro(event.getHeaders(), (byte[])event.eventBody());
    }


    /**
     * Write the event data to NoSQL using the KV API.
     * 
     * @param hdrs the event header meta information
     * @param eventBody the encoded event body
     */
    public void processAvro(Map<String,String> hdrs, byte[] eventBody) {
        LOG.trace("processing KV event");
        
        String rowKeyStr;
        String tableName;
        
        rowKeyStr = hdrs.get("rowKey");
        if (rowKeyStr == null) {
            throw new RuntimeException("No row key found in headers!");
        }
        
        tableName = hdrs.get("table");
        if (tableName == null) {
            throw new RuntimeException("No table name found in headers!");
        }
        
        LOG.debug("processAvro): Table={} Key={}", tableName, rowKeyStr);
        
        getKVStore().put(this.getKey(rowKeyStr), 
                         this.getValue(tableName, eventBody));
    }


    /**
     * Perform any cleanup needed before exiting.
     */
    public void cleanup() {
        super.cleanup();
        LOG.info("cleaning up KV event serializer");
    }


    /**
     * Convert the String representation of the key into a
     * proper instance of a NoSQL Key.
     *
     * @param rowKeyStr the string that makes up the key
     * @return The NoSQL Key
     */
    protected Key getKey(String rowKeyStr) {

        return Key.fromString(rowKeyStr);
    }

    /**
     * Store the body of the event as the value of the key. The assumption
     * is that the event body will already be serialized as an Avro record
     * or in another suitable format.
     *
     * @param tableName the name of the table
     * @param avroBody the encoded event body
     * @return the body of the event as a Value object.
     */
    protected Value getValue(String tableName, byte[] avroBody) {
        RawRecord raw;
        Schema schema;
        
        LOG.debug("getValue(): tableName={}", tableName);

        
        schema = schemas.get(tableName);
        if (schema == null) {
            throw new RuntimeException("Avro schema not found: " + tableName);
        }
        raw = new RawRecord(avroBody, schema);
        
        return binding.toValue(raw);
    }
}
