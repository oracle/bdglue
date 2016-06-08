/* ./src/main/java/com/oracle/bdglue/utility/schemadef/FormatAvro.java 
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
package com.oracle.bdglue.utility.schemadef;

import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Format the column meta data as an Avro schema.
 *
 */
public class FormatAvro extends Format {
    private static final Logger LOG = LoggerFactory.getLogger(FormatAvro.class);


    private JsonFactory factory;
    private JsonGenerator jg;

    /**
     * @throws IOException if a JSON error occurs
     */
    public FormatAvro() throws IOException {
        super();

        factory = new JsonFactory();
        setFileSuffix("avsc");

        init();
    }

    /**
     * Reinitialize so we can use this again.
     * 
     * @throws IOException if a JSON error occurs
     */
    public void init() throws IOException {
        baos().reset();
        jg = factory.createJsonGenerator(baos());
        jg.useDefaultPrettyPrinter();
    }

    /**
     * Format the top portion of the Avro schema file.
     * 
     * @param schema the name of the schema
     * @param table the name of the table
     * @param noDoc true: omit the doc field from the schema
     * @throws IOException if a JSON error occurs
     */
    @Override
    public void top(String schema, String table, boolean noDoc) throws IOException {
        String file;
        /*
         * reinitialize so we can use this instance again.
         */
        init();
        file = String.format("%s.%s", schema, table);
        setFileName(file);

        jg.writeStartObject();
        jg.writeStringField("type", "record");
        jg.writeStringField("name", table);
        jg.writeStringField("namespace", schema);
        if (noDoc == false)
            jg.writeStringField("doc", "SchemaDef");
        jg.writeFieldName("fields");
        jg.writeStartArray();

    }

    /**
     * Format the bottom portion of the Avro schema file.
     * 
     * @param table the name of the table
     * @throws IOException if a JSON error occurs
     */
    @Override
    public void bottom(String table) throws IOException {
        jg.writeEndArray();
        jg.writeEndObject();
        jg.flush();
        jg.close();

    }

    /**
     * Format each of the fields that represent the columns in the
     * Avro schema file.
     *
     * @param column the name of the column
     * @param writeDefault whether or not to include default values
     * @throws IOException if a JSON error occurs
     */
    @Override
    public void writeField(ColumnInfo column, boolean writeDefault) throws IOException {
        jg.writeStartObject();
        jg.writeStringField("name", column.getName());
        if (column.isNullable()) {
            jg.writeFieldName("type");
            jg.writeStartArray();
            jg.writeString("null");
            jg.writeString(column.getType());
            jg.writeEndArray();
        } else if (column.getType().equals("array")) {
            jg.writeFieldName("type");
            jg.writeStartObject();
            jg.writeStringField("type", "array");
            jg.writeStringField("items", "string");
            jg.writeEndObject();
        } else if (column.getType().equals("map")) {
            jg.writeFieldName("type");
            jg.writeStartObject();
            jg.writeStringField("type", "map");
            jg.writeStringField("values", "string");
            jg.writeEndObject();
            jg.writeFieldName("default");
            jg.writeStartObject();
            jg.writeEndObject();
        } else {
            jg.writeStringField("type", column.getType());
        }
        if (column.isKeyCol()) {
            jg.writeStringField("doc", "keycol");
        }
        if (writeDefault) {
            if (column.isKeyCol() == false) {
                if (column.isNullable()) {
                    jg.writeNullField("default");
                } else {
                    setDefaultValue(column);
                }
                // TODO: actually need to format this

            }
        }
        jg.writeEndObject();
    }

    /**
     * Write out the default value for this column.
     *
     * @param column the name of the column we are working with
     * @throws IOException if a JSON error occurs
     */
    private void setDefaultValue(ColumnInfo column) throws IOException {
        switch (column.getJdbcType()) {
        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            jg.writeBooleanField("default", false);
            break;
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            jg.writeNumberField("default", -1);
            break;
        case java.sql.Types.BIGINT:
            jg.writeNumberField("default", -1);
            break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CLOB:
            jg.writeStringField("default", "NONE");
            break;
        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
        case java.sql.Types.NCLOB:
            // Note: Avro strings are Utf8
            jg.writeStringField("default", "NONE");
            break;
        case java.sql.Types.BLOB:
        case java.sql.Types.BINARY:
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
            jg.writeStringField("default", "\u00FF");
            break;
        case java.sql.Types.REAL:
            jg.writeNumberField("default", -1.1);
            break;
        case java.sql.Types.FLOAT:
        case java.sql.Types.DOUBLE:
            jg.writeNumberField("default", -1.1);
            break;
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            jg.writeNumberField("default", -1.1);
            break;
        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
            jg.writeStringField("default", "NONE");
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
            jg.writeStringField("default", "NONE");
            LOG.warn("setDefaultValue(): unsupported data type: {}. Defaulting to string", 
                     column.getJdbcType());
            // TODO: throw an exception here???
            break;
        }
    }


}
