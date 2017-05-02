/* ./src/main/java/com/oracle/bdglue/encoder/avro/column/AvroNumeric.java
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

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.meta.schema.DownstreamColumnMetaData;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods supporting "numeric" values whose actual type must be
 * determined on the fly.
 *
 */
public class AvroNumeric extends AvroColumn {
    private static final Logger LOG = LoggerFactory.getLogger(AvroNumeric.class);

    /**
     * @param meta The meta data for this column.
     */
    public AvroNumeric(DownstreamColumnMetaData meta) {
        super(meta);
        setColumnType();
    }

    /**
     * Set the default values for this column type.
     */
    @Override
    protected void setColumnType() {
        PropertyManagement properties = PropertyManagement.getProperties();
        String type;

        super.columnType = AvroColumnType.NUMERIC;

        type =
            properties.getProperty(BDGluePropertyValues.NUMERIC_ENCODING,
                                   BDGluePropertyValues.NUMERIC_ENCODING_DEFAULT).trim();
        if (type.equalsIgnoreCase("string")) {
            super.avroSchemaType = Schema.Type.STRING;
        } else if (type.equalsIgnoreCase("double")) {
            super.avroSchemaType = Schema.Type.DOUBLE;
        } else {
            LOG.warn("unrecognized avro numeric encoding specified: {}. Defaulting to 'string'", type);
            super.avroSchemaType = Schema.Type.STRING;
        }

    }

    /**
     * Encode the data for this column. Note that the returned
     * type may vary based on what the Avro schema wants to see
     * for this field. This allows us to dynamcally change how
     * certain fields are handled.
     *
     * @param col The column data we want to encode
     * @return an instance of a numeric type (Integer, Float, etc.) or
     * a String that will be added to the Avro-encoded data stream
     * based on the type of the Avro field.
     */
    @Override
    public Object encodeForAvro(DownstreamColumnData col) {
        Object rval = null;
        switch (avroSchemaType) {
        case STRING:
            rval = new Utf8(col.asString());
            break;
        case INT:
            rval = new Integer(col.asInteger());
            break;
        case LONG:
            rval = new Long(col.asLong());
            break;
        case FLOAT:
            rval = new Float(col.asFloat());
            break;
        case DOUBLE:
            rval = new Double(col.asDouble());
            break;
        case BYTES:
            rval = col.asByteBuffer();
            break;
        default:
            LOG.warn("unexpected Avro schema type encountered: {}", avroSchemaType);
            rval = null;
            break;
        }

        return rval;
    }
}
