/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafkaRegistry.java 
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
package com.oracle.bdglue.publisher.kafka;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRegistry.class);
    
    // these will become properties
    String targetURL;
    int maxSchemas;
    byte magic_byte;
    int idSize;
    
    CachedSchemaRegistryClient client;
    
    public KafkaRegistry() {
        super();
        
        // get properties
        PropertyManagement properties = PropertyManagement.getProperties();
        targetURL = properties.getProperty(BDGluePropertyValues.KAFKA_REGISTRY_URL, "http://localhost:8081");
        maxSchemas = properties.asInt(BDGluePropertyValues.KAFKA_REGISTRY_MAX_SCHEMAS, "1000");
        magic_byte = (byte)(properties.asInt(BDGluePropertyValues.KAFKA_REGISTRY_MAGIC_BYTE, "0"));
        idSize = properties.asInt(BDGluePropertyValues.KAFKA_REGISTRY_ID_SIZE, "4");
        
        client = new CachedSchemaRegistryClient(targetURL, maxSchemas);    
    }
    
    /**
     * Register the schema with the kafka schema registry.
     * @param schemaName the name of the schema
     * @param schema the avro schema
     * @return the id for the schema returned from the schema registry.
     */
    public int registerSchema(String schemaName, Schema schema) {
        int id;
        
        try {
            /*
             * "-value" appended to schema name per Kafka documentation.
             * See link: : http://docs.confluent.io/2.0.0/schema-registry/docs/api.html
             */
            id = client.register(schemaName+"-value", schema);
        } catch (RestClientException | IOException e) {
            LOG.error("registerSchema(): failed. Returning ID=0", e);
            id = 0;
        }

        LOG.debug("RegisterSchema(): Schema ID returned from registry for {} was {}", schemaName, id);
        
        return id;
    }
    
    /**
     * Serialize as appropriate to send a record to Kafka that contains information
     * pertaining to the schema version that applies to this record.
     *
     * @param record a GenericRecord
     * @return a byte array representing the encoded Generic record + schema ID
     * @throws IOException if there is a problem with the encoding
     */
    public byte[] serialize(GenericRecord record) throws IOException {
          
        BinaryEncoder encoder = null;
        Schema schema = record.getSchema();
        
        byte[] rval;

        // register the schema. 
        // TODO: determine if we need getName() or getFullName()
        int schemaId = registerSchema(schema.getName(), schema);
        
        // serialize the record into a byte array
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(magic_byte);
        out.write(ByteBuffer.allocate(idSize).putInt(schemaId).array());
        //DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);

        encoder = org.apache.avro.io.EncoderFactory.get().directBinaryEncoder(out, encoder);
        writer.write(record, encoder);
        encoder.flush();
        rval = out.toByteArray();
        //out.close(); // noop in the Apache version, so not bothering
        
        return rval;
    }

    
}
