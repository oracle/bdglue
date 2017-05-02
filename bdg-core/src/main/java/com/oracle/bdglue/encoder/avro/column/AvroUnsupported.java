/* ./src/main/java/com/oracle/bdglue/encoder/avro/column/AvroUnsupported.java 
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods dealing with column types we don't (presently) support.
 * It is potentially possible to support anything, but the Avro schemas
 * for the columns are complex/compound, so not dealing with them at
 * the present time.
 *
 */
public class AvroUnsupported extends AvroColumn {
    private static final Logger LOG = LoggerFactory.getLogger(AvroUnsupported.class);

    /**
     * @param meta The meta data for this column.
     */
    public AvroUnsupported(DownstreamColumnMetaData meta) {
        super(meta);
        setColumnType();
    }

    /**
     * Set the default values for this column type.
     */
    @Override
    protected void setColumnType() {
        super.columnType = AvroColumnType.UNSUPPORTED;
        super.avroSchemaType = Schema.Type.NULL;
        super.columnTypeString = "UNSUPPORTED";
    }
    
    /**
     * Encode the data for this column. 
     * 
     * @param col The column data we want to encode
     * @return null becuase this is an unsupported or unrecognized type.
     */
    @Override
    public Object encodeForAvro(DownstreamColumnData col) {
        LOG.warn("unsupported column type encountered");
        return null;
    }
}
