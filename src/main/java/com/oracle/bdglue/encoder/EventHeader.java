/* ./src/main/java/com/oracle/bdglue/encoder/EventHeader.java 
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
package com.oracle.bdglue.encoder;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.avro.AvroSchema;
import com.oracle.bdglue.encoder.avro.AvroSchemaFactory;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.util.HashMap;

import org.apache.flume.sink.hdfs.AvroEventSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for manageing the event header / meta information. Note that this class does
 * not itself travel along with encoded data. Only the eventHeader HashMap.
 */
public class EventHeader {
    private static final Logger LOG = LoggerFactory.getLogger(EventHeader.class);
    
    public static final String avroURL = AvroEventSerializer.AVRO_SCHEMA_URL_HEADER;
    public static final String TABLE = "table";
    public static final String SHORT_NAME = "shortName";
    public static final String ROWKEY = "rowKey";
    public static final String COLUMN_FAMILY = "columnFamily";
    public static final String OPTYPE = "optype";
    public static final String TIMESTAMP = "timestamp";
    public static final String TOPIC = "topic";
    public static final String POSITION = "position";
    
    private PropertyManagement properties;
    private String avroURLPath;
    private boolean hdrOpType = false;
    private boolean hdrTimestamp = false;
    private boolean hdrRowKey = false;
    private boolean tableLongName = false;
    private boolean columnFamily = false;
    private boolean avroSchemaPath = false;

    // a map containing info for the event header
    private HashMap<String, String> eventHeader;

    public EventHeader() {
        super();
        String tstrg;
        eventHeader = new HashMap<>();
        properties = PropertyManagement.getProperties();
        hdrOpType = properties.asBoolean(BDGluePropertyValues.HEADER_OPTYPE, "true");
        hdrTimestamp = properties.asBoolean(BDGluePropertyValues.HEADER_TIMESTAMP, "true");
        hdrRowKey = properties.asBoolean(BDGluePropertyValues.HEADER_ROWKEY, "true");
        columnFamily = properties.asBoolean(BDGluePropertyValues.HEADER_COLUMNFAMIILY, "false");
        tstrg = properties.getProperty(BDGluePropertyValues.PUBLISHER_HASH);
        if ((tstrg != null) && tstrg.equalsIgnoreCase("rowkey")) {
            // force row key inclusion if we are hashing on it in the publisher
            hdrRowKey = true;
        }
        tableLongName = properties.asBoolean(BDGluePropertyValues.HEADER_LONGNAME, "true");
        avroSchemaPath = properties.asBoolean(BDGluePropertyValues.HEADER_AVROPATH, "true");
        
        avroURLPath = properties.getProperty(BDGluePropertyValues.AVRO_SCHEMA_URL);
        
    }

    /**
     * This is header data that will travel along with the encoded record
     * for downstream processing. For example, "Table" will be used to
     * help with the multiplexing for each channel.
     *
     * @param key String that specifies the name/key 
     * @param value String containing the value to be associated with the key
     */
    public void putHeaderData(String key, String value) {
        eventHeader.put(key, value);
    }

    /**
     * Set the column family in the event header.
     * @param value name of the hbase column family for this record
     */
    public void putColumnFamily(String value) {
        putHeaderData(COLUMN_FAMILY, value);
    }

    /**
     * Set the table name in the Avro event header.
     * @param value the table name
     */
    public void putTable(String value) {
        putHeaderData(TABLE, value);
    }
    
    /**
     * Set the simple table name (i.e. without the
     * schema name in the event header.
     * @param value the simple table name
     */
    public void putShortName(String value) {
        putHeaderData(SHORT_NAME, value);
    }

    /**
     * Set the operation type in the event header.
     * @param value the operation type
     */
    public void putOpType(String value) {
        putHeaderData(OPTYPE, value);
    }

    /**
     * Put the transaction timestamp in the Avro event header.
     * @param value the timestamp
     */
    public void putTimeStamp(String value) {
        putHeaderData(TIMESTAMP, value);
    }

    /**
     * Put the key value for the row in the Avro event header.
     * @param value the key to associate with the record on the target
     */
    public void putRowKey(String value) {
        putHeaderData(ROWKEY, value);
    }

    /**
     * Put the URL to the Avro schema file in HDFS in the
     * Avro event header.
     * @param value the URL/URI
     */
    public void putSchemaURL(String value) {
        putHeaderData(avroURL, avroURLPath + value);
    }

    /**
     * @return a Map containing the header key/value pairs for
     * inclusion with the encoded record.
     */
    public HashMap<String, String> getEventHeader() {
        return eventHeader;
    }

    /**
     * Set the Avro event header information for this operation based
     * on the encoder and target types, etc. Note that each call allocates a 
     * new eventHeader HashMap that will be passed along downstream to the
     * publisher.
     * 
     * @param op the current database operation.
     */
    public void setHeaderInfo(DownstreamOperation op) {
        String key;
 
        AvroSchema avroSchema;

        LOG.trace("setting event header");

        /*
         * need a new Map with each call because we pass the
         * Map along with the event to the publisher.
         */
        eventHeader = new HashMap<>();
        
        if (tableLongName) {
            putTable(op.getTableMeta().getBDLongTableName());
        } else {
            putTable(op.getTableMeta().getBDTableName());
        }
        
        if (avroSchemaPath) {
            key = op.getTableMeta().getKeyName();
            avroSchema = AvroSchemaFactory.getAvroSchemaFactory().getAvroSchema(key);
            putSchemaURL(avroSchema.getSchemaFileName());
        }
        
        if (columnFamily) {
            putColumnFamily("data"); // hard coded for now
        }

        if (hdrOpType) {
            putOpType(op.getOpType());
        }
        if (hdrTimestamp) {
            putTimeStamp(op.getTimeStamp());
        }
        if (hdrRowKey) {
            putRowKey(op.getRowKey());
        }
        // always set position info. Needed for knowing 
        // where we are in the queues on shutdown.
        putHeaderData(POSITION, op.getPosition());
    }
    

}
