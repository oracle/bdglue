/* ./src/main/java/com/oracle/bdglue/encoder/avro/AvroSchemaFactory.java 
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
package com.oracle.bdglue.encoder.avro;


import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.meta.schema.DownstreamSchemaMetaData;
import com.oracle.bdglue.meta.schema.DownstreamTableMetaData;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a factory object to provide an easy way to track and access
 * the schema meta data that we are collecting. It is a singleton.
 */
public class AvroSchemaFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaFactory.class);

    private static AvroSchemaFactory avroSchemaFactory; 

    private HashMap<String,AvroSchema> schemas;
    
    /**
     * This constructor is private because the factory is intended to be a 
     * singleton. Access is through the getAvroSchemaFactory method.
     */
    private AvroSchemaFactory() {
        super();
        
        schemas = new HashMap<>();
    }

    /**
     * Singleton Getter of the factory object.
     *
     * @return the schema factory object
     */
    public static AvroSchemaFactory getAvroSchemaFactory() {
        LOG.debug("getAvroSchemaFactory()");
        if (avroSchemaFactory == null) {

            avroSchemaFactory = new AvroSchemaFactory();
        }

        return avroSchemaFactory;
    }

    /**
     * Process the metadata for the table(s) in the meta data.
     * Note that this method only looks
     * for new table names and does not look for changed schema
     * (i.e. it does not support schema evolution).
     *
     * @param meta contains info on all metadata,
     *             not just new  or changed.
     * @return the number of new tables found, or
     *         0 if no new tablenames were found.
     */
    public int metaDataChanged(DownstreamSchemaMetaData meta) {
        int rval = 0;
        String keyName;

        LOG.debug("metaDataChanged(DownstreamSchemaMetaData)");
        // loop through the metadata for each table to find new ones
        for (Map.Entry<String, DownstreamTableMetaData> table : meta.getTableMetadata()) {
            keyName = table.getValue().getKeyName();
            if (!schemas.containsKey(keyName)) {
                // table name not found in schemas, so add a new one
                metaDataChanged(table.getValue());
                rval++;
            }
        }

        return rval;
    }
    
    /**
     * Process the metadata for the new/changed table.
     *
     * @param meta metadata for the new or changed table.
     *
     */
    public void metaDataChanged(DownstreamTableMetaData meta) {
        String tableName;
        String keyName;
        AvroSchema schema;
        boolean generateSchema =
            PropertyManagement.getProperties().asBoolean(BDGluePropertyValues.GENERATE_AVRO_SCHEMA, "false");
        tableName = meta.getBDTableName();
        keyName = meta.getKeyName();
        LOG.debug("metaDataChanged() event for table: {}", tableName);
        // loop through the metadata for each table to find new ones

        
        if (!schemas.containsKey(keyName)) {
            // table name not found in schemas, so add a new one
            LOG.debug("metaDataChanged(): adding new table: {}", tableName);
        } else {
            // table name found in schemas, so replace the old version
            LOG.debug("metaDataChanged(): replacing table: {}", tableName);
        }
        schema = new AvroSchema(keyName, meta, generateSchema);
        schemas.put(keyName, schema);

    }

    /**
     * Locate the AvroSchema object associated with the table we are processing.
     * 
     * @param key - the key to locate the schema in the hashmap
     * @return null if not found, otherwise the corresponding AvroSchema.
     */
    public AvroSchema getAvroSchema(String key) {
        AvroSchema rval = null;
        
        LOG.trace("getAvroSchema(): getting schema for: {}", key);
        
        if (schemas.containsKey(key)) {
            rval = schemas.get(key);
        }
        else {
            LOG.error("getAvroSchema(): Schema not found: {}", key);
        }
        
        return rval;
    }
    
}
