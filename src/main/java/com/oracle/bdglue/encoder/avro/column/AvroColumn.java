/* ./src/main/java/com/oracle/bdglue/encoder/avro/column/AvroColumn.java 
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

import java.util.ArrayList;

import org.apache.avro.Schema;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This abstract class stores meta data about the columns that we are
 * processing / encoding for transport.
 *
 * @author John Neal
 */
public abstract class AvroColumn {
    private static final Logger LOG = LoggerFactory.getLogger(AvroColumn.class);

    private String columnName;
    private boolean keyCol;
    private boolean nullable;

    protected AvroColumnType columnType;
    protected Schema.Type avroSchemaType;
    /*
     * Used to provide doc info on "special" columns like LOBs,
     * Date-Time, Unsupported, etc.
     */
    protected String columnTypeString;


    /**
     * @param meta the metadata for this column
     */
    protected AvroColumn(DownstreamColumnMetaData meta) {
        super();

        columnTypeString = "";
        columnName = meta.getBDColumnName();
        nullable = meta.isNullable();
        keyCol = meta.isKeyCol();
    }

    /**
     * @return the Avro column type
     */
    public AvroColumnType getColumnType() {
        return columnType;
    }

    /**
     * @return the Avro column type as a String
     */
    public String getColumnTypeString() {
        return columnTypeString;
    }

    /**
     * @return whether or not this is a Key Column
     */
    public boolean isKeyCol() {
        return keyCol;
    }

    /**
     * @return true if this collumn is nullable
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Set all columns marked as nullable this way, including key columns.
     * Set all non-key columns as nullable as well, whether or not they
     * are marked as nullable.
     * 
     * @return the Avro schema for this column.
     *         Schemas are nested to create a record.
     */
    public Schema getFieldSchema() {
        Schema rval;
        if (isNullable()) {
            rval = nullableColumn();
        } else if (!isKeyCol()) {
            // mark non-key columns to be nullable regardless
            rval = nullableColumn();
        } else {
            // don't mark as nullable
            rval = Schema.create(avroSchemaType);
        }
        return rval;
    }

    /**
     * For a column to be nullable in an Avro schema, it is defined as a
     * union of the actual type and "null", accepting either as input.
     *
     * @param fieldSchema the schema for the actual column type
     * @return a "union" field of the column schema + null
     */
    private Schema nullableColumn() {
        LOG.trace("**** NULLABLE COLUMN FOUND: " + columnName);
        ArrayList<Schema> unionTypes = new ArrayList<>();
        unionTypes.add(Schema.create(Schema.Type.NULL));
        unionTypes.add(Schema.create(avroSchemaType));

        return Schema.createUnion(unionTypes);
    }


    /**
     * Static factory class. Converts the SQL column types that
     * we might encounter to Avro types.
     *
     * @param meta column meta data
     * @return an instance of AvroColumn
     *
     */
    public static AvroColumn newAvroColumn(DownstreamColumnMetaData meta) {
        AvroColumn rval;


        switch (meta.getJdbcType()) {

        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            rval = new AvroBoolean(meta);
            break;
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            rval = new AvroInteger(meta);
            break;
        case java.sql.Types.BIGINT:
            rval = new AvroLong(meta);
            break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CLOB:
            rval = new AvroString(meta);
            break;
        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
        case java.sql.Types.NCLOB:
            rval = new AvroMultiByte(meta);
            break;
        case java.sql.Types.BLOB:
        case java.sql.Types.BINARY:
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
            rval = new AvroLOB(meta);
            break;
        case java.sql.Types.REAL:
            // JDBC says only 7 digits of mantisa
            rval = new AvroFloat(meta);
            break;
        case java.sql.Types.FLOAT:
        case java.sql.Types.DOUBLE:
            // JDBC says these both FLOAT and DOUBLE 
            // have 15 digits of mantisa
            rval = new AvroDouble(meta);
            break;
        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
            rval = new AvroNumeric(meta);
            break;
        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
            rval = new AvroDateTime(meta);
            break;
        case java.sql.Types.DATALINK:
        case java.sql.Types.DISTINCT:
        case java.sql.Types.JAVA_OBJECT:
        case java.sql.Types.NULL:
        case java.sql.Types.ROWID:
        case java.sql.Types.SQLXML:
        case java.sql.Types.STRUCT:
        case java.sql.Types.ARRAY:
        case java.sql.Types.OTHER:
        case java.sql.Types.REF:
        default:
            rval = new AvroUnsupported(meta);
            break;
        }

        return rval;
    }

    /**
     * Override the default avro schema type with whatever
     * value was specified in the *.avsc file we read from disk.
     *
     * @param type the Avro Schema.Type for this field
     */
    public void setAvroColumnType(Schema.Type type) {
        avroSchemaType = type;
    }

    /**
     * Set the metadata type and the default Avro schema
     * type for this column. Should be called from the constructor
     * of the implementing class.
     */
    protected abstract void setColumnType();

    /**
     * Encode the data for this column.
     * 
     * @param col The column data we want to encode
     * @return the object that will be added to the Avro-encoded
     * data stream.
     */
    public abstract Object encodeForAvro(DownstreamColumnData col);

}
