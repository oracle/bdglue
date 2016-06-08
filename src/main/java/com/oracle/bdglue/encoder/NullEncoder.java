/* ./src/main/java/com/oracle/bdglue/encoder/NullEncoder.java 
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

import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class does no encoding of the database operation. Instead
 * it passes the unencoded database operation along for use directly
 * against a target API. It should never be used to pass data to other
 * target environments (i.e. Flume, Kafka, etc.) as then the structure
 * of the operation would need to be understood by them and that would
 * violate one of the key intents of this effort ... the complete
 * decoupling of the source from the target.
 */
public class NullEncoder implements BDGlueEncoder {
    private static final Logger LOG = LoggerFactory.getLogger(NullEncoder.class);
    
    private EncoderType encoderType;
    private EventHeader eventHeader;
    
    public NullEncoder() {
        super();
        LOG.info("NullEncoder()");
        encoderType = EncoderType.NULL;
        eventHeader = new EventHeader();
    }

    /**
     * Perform no encoding of the DownstreamOperation. Just pass the info along.
     * 
     * @param op the operation we are encoding
     * @return  the instance of EventData
     * @throws IOException if an encoding error occurs. Not likely here, but needed for the interface.
     */
    public EventData encodeDatabaseOperation(DownstreamOperation op) throws IOException {
        LOG.trace("encodeDatabaseOperation()");
        EventData rval;
        
        eventHeader.setHeaderInfo(op);
        rval = new EventData(EncoderType.NULL, eventHeader.getEventHeader(), op);
        
        return rval;
    }
    
    /**
     * @return the EncoderType for this Encoder. 
     */
    public EncoderType getEncoderType() {
        return encoderType;
    }
}
