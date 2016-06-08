/* ./src/main/java/com/oracle/bdglue/encoder/DelimitedTextEncoder.java 
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Encodes the operation as delimited text.
 */
public class DelimitedTextEncoder implements BDGlueEncoder {
    private static final Logger LOG = LoggerFactory.getLogger(DelimitedTextEncoder.class);

    private char delimiter;
    private StringBuilder output;
    private boolean first = true;
    
    private EncoderType encoderType;
    private EventHeader eventHeader;

    public DelimitedTextEncoder() {
        
        super();
        LOG.info("DelimitedTextEncoder()");
        
        encoderType = EncoderType.TEXT;
        eventHeader = new EventHeader();

        PropertyManagement properties = PropertyManagement.getProperties();
        delimiter = (char)properties.asInt(BDGluePropertyValues.ENCODER_DELIMITER,
                                    BDGluePropertyValues.ENCODER_DELIMITER_DEFAULT);
    }

    /**
     * Append the transaction op type and/or timestamp if needed.
     *
     * @param op The database operation.
     */
    private void setTxInfo(DownstreamOperation op) {
        for(String value : op.getOpMetadata().values()) {
            delimiter();
            output.append(TextEncoderHelper.escapeChars(value));
        }
    }

    /**
     * Append a delimiter if this is not the first field.
     */
    private void delimiter() {
        if (!first) {
            output.append(delimiter);
        } else
            first = false;
    }


    /**
     * Put the columns into delimited text format for output.
     *
     * @param op the database operation
     * @return the encoded database operatoin as a byte array.
     *
     * @throws IOException if encoding error occurs
     */
    public EventData encodeDatabaseOperation(DownstreamOperation op) throws IOException {
        first = true;


        output = new StringBuilder(1000);

        eventHeader.setHeaderInfo(op);

        setTxInfo(op);

        for (DownstreamColumnData col : op.getColumns()) {
            delimiter();

            output.append(TextEncoderHelper.escapeChars(col.asString()));

        }

        return new EventData(encoderType, eventHeader.getEventHeader(), output.toString());
    }

    /**
     * Returns the EncoderType.
     * @return the EncoderType for this Encoder. 
     */
    public EncoderType getEncoderType() {
        return encoderType;
    }
}
