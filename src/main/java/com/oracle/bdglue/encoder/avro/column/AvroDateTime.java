/* ./src/main/java/com/oracle/bdglue/encoder/avro/column/AvroDateTime.java 
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

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

/**
 * Methods supporting Date and Time columns.
 * <p>
 * Right now, just returning as a String. The problem is that
 * the different formats are very different. We may need to consider
 * separate classes for each type, or converting all to a common
 * format ... or not. We assume that the String representation of the 
 * data is passed in along with the binary representation.
 *
 */
public class AvroDateTime extends AvroColumn {
    /**
     * @param meta the meta data for this column.
     */
    public AvroDateTime(DownstreamColumnMetaData meta) {
        super(meta);
        setColumnType();
    }

    /**
     * Set the default values for this column type.
     */
    @Override
    protected void setColumnType() {
        super.columnType = AvroColumnType.DATE_TIME;
        super.avroSchemaType = Schema.Type.STRING;
        super.columnTypeString = "Date-Time";
    }

    /**
     * Encode the data for this column.
     * 
     * @param col The column data we want to encode
     * @return an Instance of Utf8 that will be added to 
     * the Avro-encoded data stream.
     */
    @Override
    public Utf8 encodeForAvro(DownstreamColumnData col) {
        return new Utf8(col.asString());
    }
}
