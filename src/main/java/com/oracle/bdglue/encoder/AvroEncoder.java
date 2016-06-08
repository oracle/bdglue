/* ./src/main/java/com/oracle/bdglue/encoder/AvroEncoder.java 
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

import com.oracle.bdglue.encoder.avro.AvroSchema;
import com.oracle.bdglue.encoder.avro.AvroSchemaFactory;
import com.oracle.bdglue.encoder.avro.column.AvroColumn;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.io.IOException;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.output.ByteArrayOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encodes the data as an Avro record before sending it downstream
 * for publishing.
 */
public class AvroEncoder implements BDGlueEncoder {

    private static final Logger LOG = LoggerFactory.getLogger(AvroEncoder.class);

    protected EncoderType encoderType;
    protected EventHeader eventHeader;

    public AvroEncoder() {
        super();
        LOG.info("AvroEncoder()");
        encoderType = EncoderType.AVRO_BYTE_ARRAY;
        eventHeader = new EventHeader();
    }


    /**
     * Populate transaction meta data into the record if requested.
     *
     * @param record the GenericRecord we will be writing to
     * @param op the database operation we are processing.
     */
    private void setTxInfo(GenericRecord record, DownstreamOperation op) {
        
        for(Map.Entry<String, String> col : op.getOpMetadata().entrySet()) {
            record.put(col.getKey(), new Utf8(col.getValue()));
        }
    }
    
    /**
     * Populate the fields in the Avro record.
     * 
     * @param ggAvroSchema the BDGlue AvroSchema we are working with.
     * @param op the database operation
     * @param schema the actual Avro schema we are working with
     * @return the populated Avro GenericRecord
     */
    protected GenericRecord populateAvroRecord(AvroSchema ggAvroSchema, DownstreamOperation op, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);

        Object value = null;
        AvroColumn column;
        String columnName;

        setTxInfo(record, op);

        for (DownstreamColumnData col : op.getColumns()) {
            columnName = col.getBDName();

            column = ggAvroSchema.getAvroColumn(columnName);

            if (col.checkForNULL() == true) {
                value = null;
            } else {

                try {
                    value = column.encodeForAvro(col);
                } catch (NumberFormatException e) {
                    value = null;
                }
            }


            if (value != null)
                record.put(col.getBDName(), value);
        }
        return record;
    }
    
    /**
     * Serialize the record to prepare for publishing.
     *
     * @param record the GenericRecord
     * @param schema the Avro Schema
     * @param ggAvroSchema the internal representation of the Avro schema
     * @return the serialized record
     * @throws IOException if there is a problem
     */
    private byte[] serializeRecord(GenericRecord record, Schema schema,
                                   @SuppressWarnings("unused") AvroSchema ggAvroSchema) throws IOException {
        byte[] rval;
        
        BinaryEncoder encoder = null;
        
        // serialize the record into a byte array
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        encoder = EncoderFactory.get().directBinaryEncoder(out, encoder);
        writer.write(record, encoder);
        encoder.flush();
        rval = out.toByteArray();
        //out.close(); // noop in the Apache version, so not bothering
        return rval;
    }
    
    /**
     * Dummy method. Overridden in classes that used a registry.
     * 
     * @param schemaName the schema name to register
     * @param schema the Avro scheam
     */
   // public int registerSchema(String schemaName, Schema schema, AvroSchema ggAvroSchema) {
   //     int id = 0;
   //     if (ggAvroSchema.getSchemaId() == AvroSchema.SCHEMA_ID_UNSET)
    //        ggAvroSchema.setSchemaId(id);
    //    return id;
    //}
    
    /**
     * Loop through operation to put data into a record.
     *
     * @param op the database operation to process
     * @return the fully populated GenericRecord ready for serialization
     * @throws IOException if output error occurs on Avro record
     */
    public EventData encodeDatabaseOperation(DownstreamOperation op) throws IOException {
        LOG.debug("populateBinaryRecord()");

        byte[] rval;


        // find the Schema for this table. This is an optimistic call and
        // assumes that the meta data has already been defined.
        String key = op.getTableMeta().getKeyName();
        AvroSchema ggAvroSchema = AvroSchemaFactory.getAvroSchemaFactory().getAvroSchema(key);
        Schema schema = ggAvroSchema.getAvroSchema();
        
        //registerSchema(key, schema, ggAvroSchema);
        

        eventHeader.setHeaderInfo(op);

        GenericRecord record = populateAvroRecord(ggAvroSchema, op, schema); 

        // find schema
        //Schema schema = getSchema(op);

        rval = serializeRecord(record, schema, ggAvroSchema);
        //out.close(); // noop in the Apache version, so not bothering

        return new EventData(encoderType, eventHeader.getEventHeader(), rval);
    }


    /**
     * Return the EncoderType.
     * 
     * @return the EncoderType for this Encoder. 
     */
    public EncoderType getEncoderType() {
        return encoderType;
    }


}
