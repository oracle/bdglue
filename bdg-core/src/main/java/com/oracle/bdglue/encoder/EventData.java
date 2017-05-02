/* ./src/main/java/com/oracle/bdglue/encoder/EventData.java 
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


import java.util.Map;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the output of the encoding process.
 * It contains two elements: a Map of pertinent meta information
 * (table name, operation type, timestamp, etc.), and a byte
 * array that contains the encoded data.
 *
 */
public class EventData {
    private static final Logger LOG = LoggerFactory.getLogger(EventData.class);

    private Map<String, String> headerInfo;
    private Object eventBody;
    private EncoderType encoderType;
    
    public EventData() {
        super();
    }

    /**
     * Construct an event based on the provided meta data and body.
     *
     * @param type the type of encoder used to create this event.
     * @param meta header/meta data
     * @param body the body of the event
     */
    public EventData(EncoderType type, Map<String, String> meta, Object body) {
        super();
        headerInfo = meta;
        eventBody = body;
        encoderType = type;
    }


    /**
     * Set the meta information in the header for this Event.
     * 
     * @param map a Map containing informatoin for the header
     */
    public void setHeaders(Map<String, String> map) {
        headerInfo = map;
    }

    /**
     * Get the header information for this operation.
     * 
     * @return the meta information
     */
    public Map<String, String> getHeaders() {
        return headerInfo;
    }
    /**
     * Set the event body for this event.
     * 
     * @param body an Object representing the body of the event
     */
    public void eventBody(Object body) {
        eventBody = body;
    }
    
    /**
     * Return the body of the event as the Object that was passed into the
     * constructor. Caller must cast this to the appropriate type, and it is
     * the responsibility of the caller to know what type was set in the constructor.
     * 
     * @return the event body as an Object
     */
    public Object eventBody() {
        return eventBody;
    }


    /**
     * Return the value of the specified key from the operation's
     * meta information.
     * 
     * @param key the name of the field we are looking for
     * @return the value associated with the key
     */
    public String getMetaValue(String key) {
        String rval;
        
        if (headerInfo.containsKey(key)) {
            rval = headerInfo.get(key);
        }
        else {
            LOG.warn("getMetaValue(): Key not found: {}", key);
            rval = "KeyNotFound";
        }
        return rval;
    }

    /**
     * Get the encoder type we used to create this event. Note that 
     * use of encoder type is not actually required when implementing an
     * encoder, so a value might be returned as Null.
     * 
     * @return the type of encoder that created this Event.
     */
    public EncoderType getEncoderType() {
        return encoderType;
    }
}
