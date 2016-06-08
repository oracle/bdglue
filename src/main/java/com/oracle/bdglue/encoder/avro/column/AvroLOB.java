/* ./src/main/java/com/oracle/bdglue/encoder/avro/column/AvroLOB.java 
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
package com.oracle.bdglue.encoder.avro.column;


import com.oracle.bdglue.meta.schema.DownstreamColumnMetaData;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;

/**
 * Methods supporting data that should be treated as LOBs.
 *
 */
public class AvroLOB extends AvroColumn {
    /**
     * @param meta The meta data for this column.
     */
    public AvroLOB(DownstreamColumnMetaData meta) {
        super(meta);
        setColumnType();
    }


    /**
     * Set the default values for this column type.
     */
    @Override
    protected void setColumnType() {
        super.columnType = AvroColumnType.LOB;
        super.avroSchemaType = Schema.Type.BYTES;
        super.columnTypeString = "LOB";
    }
    /**
     * Encode the data for this column.
     * 
     * @param col The column data we want to encode
     * @return an instance of java.nio.ByteBuffer that will be added to 
     * the Avro-encoded data stream.
     */
    @Override
    public ByteBuffer encodeForAvro(DownstreamColumnData col) {
        return col.asByteBuffer();
    }
}
