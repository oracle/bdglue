/* ./src/main/java/com/oracle/bdglue/encoder/AvroGenericRecordEncoder.java 
 *
 * Copyright 2016 Oracle and/or its affiliates.
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
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroGenericRecordEncoder extends AvroEncoder {
    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordEncoder.class);

    
    public AvroGenericRecordEncoder() {
        super();
        
        super.encoderType = EncoderType.AVRO_GENERIC_RECORD;
    }
   
    /**
     * Loop through operation to put data into a record.
     *
     * @param op the database operation to process
     * @return the event ready to be passed to the Publisher 
     * @throws IOException if output error occurs on Avro record
     */
    @Override
    public EventData encodeDatabaseOperation(DownstreamOperation op) throws IOException {
        LOG.debug("set generic record");

        // find the Schema for this table. This is an optimistic call and
        // assumes that the meta data has already been defined.
        String schemaName = op.getTableMeta().getKeyName();
        AvroSchema ggAvroSchema = AvroSchemaFactory.getAvroSchemaFactory().getAvroSchema(schemaName);
        Schema schema = ggAvroSchema.getAvroSchema();
        
        super.eventHeader.setHeaderInfo(op);

        GenericRecord record = super.populateAvroRecord(ggAvroSchema, op, schema); 

        return new EventData(super.encoderType, super.eventHeader.getEventHeader(), record);
    }

}
