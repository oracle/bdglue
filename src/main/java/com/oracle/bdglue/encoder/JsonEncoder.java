/* ./src/main/java/com/oracle/bdglue/encoder/JsonEncoder.java 
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

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.io.IOException;

import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.commons.io.output.ByteArrayOutputStream;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to encode data in a JSON format where field names correspond to
 * input field names, and values contain the data values for the fields.
 */
public class JsonEncoder implements BDGlueEncoder {
    private static final Logger LOG = LoggerFactory.getLogger(JsonEncoder.class);
    private static boolean includeBefores = 
        PropertyManagement.getProperties().asBoolean(BDGluePropertyValues.INCLUDE_BEFORES, 
                                                     BDGluePropertyValues.INCLUDE_BEFORES_DEFAULT);
    
    private boolean textOnly;
    private EncoderType encoderType;
    private EventHeader eventHeader;
    private ByteArrayOutputStream baos;
    private JsonFactory factory;


    public JsonEncoder() {
        super();
        LOG.info("JsonEncoder()");
        init();
        textOnly = PropertyManagement.getProperties().asBoolean(BDGluePropertyValues.JSON_TEXT, 
                                                                BDGluePropertyValues.JSON_TEXT_DEFAULT);
    }

    /**
     * @param textOnly flag to indicate whether to encode everything as text,
     * or encode non-text fields differently.
     */
    public JsonEncoder(boolean textOnly) {
        super();
        LOG.info("JsonEncoder(boolean)");
        init();
        
        this.textOnly = textOnly;
    }

    /**
     * Initialize the instance.
     */
    private void init() {
        encoderType = EncoderType.JSON;
        eventHeader = new EventHeader();
        baos = new ByteArrayOutputStream();
        factory = new JsonFactory();
    }

    /**
     * Populate transaction meta data into the record if requested.
     *
     * @param jg the handle to the JsonGenerator
     * @param op the database operation we are processing.
     * @throws IOException if a JSON encoding error occurs
     */
    private void setTxInfo(JsonGenerator jg, DownstreamOperation op) throws IOException {
        for(Map.Entry<String, String> col : op.getOpMetadata().entrySet()) {
            jg.writeStringField(col.getKey(), col.getValue());
        }
    }
    
    /**
     * Populate transaction meta data into the record if requested.
     *
     * @param jg the handle to the JsonGenerator
     * @param op the database operation we are processing.
     * @throws IOException if a JSON encoding error occurs
     */
    private void setBeforeInfo(JsonGenerator jg, DownstreamOperation op) throws IOException {
        LOG.debug("setBeforeInfo()");

        jg.writeFieldName("priorValues");
        jg.writeStartObject();
        // loop through operatons with calls to appropriate jg.write() methods
        for (DownstreamColumnData col : op.getBefores()) {
            if (textOnly == true) {
                jg.writeStringField(col.getBDName(), col.asString());
            } else {
                // Encode the data appropriately: handle numbers as numbers, etc.
                int jdbcType;
                jdbcType = op.getTableMeta().getColumn(col.getOrigName()).getJdbcType();
                encodeColumn(col, jdbcType, jg);
            }
        }
        jg.writeEndObject();
    }

    /**
     * Encode the column properly in Json.
     *
     * @param col the column data we are encoding
     * @param jdbcType the column type
     * @param jg handle to the JsonGenerator
     * @throws IOException if a JSON encodng error occurs
     */
    private void encodeColumn(DownstreamColumnData col, int jdbcType, JsonGenerator jg) throws IOException {
        if (col.checkForNULL()) {
            jg.writeNullField(col.getBDName());
        } else {
            switch (jdbcType) {

            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                jg.writeBooleanField(col.getBDName(), col.asBoolean());
                break;
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
                jg.writeNumberField(col.getBDName(), col.asInteger());
                break;
            case java.sql.Types.BIGINT:
                jg.writeNumberField(col.getBDName(), col.asLong());
                break;
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB:
                jg.writeStringField(col.getBDName(), col.asString());
                break;
            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NCLOB:
                jg.writeStringField(col.getBDName(), col.asString());
                break;
            case java.sql.Types.BLOB:
            case java.sql.Types.BINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.VARBINARY:
                jg.writeBinaryField(col.getBDName(), col.asBytes());
                break;
            case java.sql.Types.REAL: // JDBC says 7 digits of mantisa
                jg.writeNumberField(col.getBDName(), col.asFloat());
                break;
            case java.sql.Types.FLOAT: // JDBC says 15 digits of mantisa
            case java.sql.Types.DOUBLE:
                jg.writeNumberField(col.getBDName(), col.asDouble());
                break;
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                jg.writeNumberField(col.getBDName(), col.asBigDecimal());
                break;
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
                jg.writeStringField(col.getBDName(), col.asString());
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
                jg.writeStringField(col.getBDName(), "unsupported");
                break;
            }
        }
    }

    /**
     * Encode the DownstreamOperation as a JSON record.
     * 
     * @param op the operatoin we are encoding
     * @return an instance of EventData
     * @throws IOException if an encoding error occurs
     */
    public EventData encodeDatabaseOperation(DownstreamOperation op) throws IOException {
        JsonGenerator jg;

        eventHeader.setHeaderInfo(op);


        /*
         * need a new one of these with each call because we pass the byte array
         * from baos along with the event as it is passed to the publisher.
         */
        baos = new ByteArrayOutputStream();
        
        jg = factory.createJsonGenerator(baos);
        jg.writeStartObject();

        setTxInfo(jg, op);

        // loop through operatons with calls to appropriate jg.write() methods
        for (DownstreamColumnData col : op.getColumns()) {
            if (textOnly == true) {
                jg.writeStringField(col.getBDName(), col.asString());
            } else {
                // Encode the data appropriately: handle numbers as numbers, etc.
                int jdbcType;
                jdbcType = op.getTableMeta().getColumn(col.getOrigName()).getJdbcType();
                encodeColumn(col, jdbcType, jg);
            }
        }
        
        if (includeBefores) {
            setBeforeInfo(jg, op);
        }

        jg.writeEndObject();
        jg.flush();
        jg.close();

        return new EventData(encoderType, eventHeader.getEventHeader(), baos.toByteArray());
    }

    /**
     * Get the EncoderType. May be null if this is a custom encoder.
     * 
     * @return the EncoderType for this Encoder. 
     */
    public EncoderType getEncoderType() {
        return encoderType;
    }
}
