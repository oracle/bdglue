/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafaMessageDefaultMetaBb.java 
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
package com.oracle.bdglue.publisher.kafka;

import com.oracle.bdglue.encoder.EventData;

import com.oracle.bdglue.encoder.EventHeader;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Return the short table name as the topic, and a representation of the
 * primary key as the Kafka message key, prefixed by the schema name.
 * This class assumes that the "long table name" is available for parsing
 * in the event header (for both flume and bdglue).
 * 
 */
public class KafkaMessageTableKeyBb implements KafkaMessageHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageTableKeyBb.class);
    
    public KafkaMessageTableKeyBb() {
        super();
        
        LOG.debug("KafkaMessageTableKeyBb() has been loaded");
    }

    @Override
    public String getTopic(EventData evt) {
        return getTableName(evt.getMetaValue(EventHeader.TABLE));
        
    }


    @Override
    public String getMessageKey(EventData evt) {
        return getMessageKey(evt.getMetaValue(EventHeader.TABLE), 
                             evt.getMetaValue(EventHeader.ROWKEY));
    }



    @Override
    public void setTopic(String topic) {
        // not implemented
    }

    @Override
    public void setMessageKey(String key) {
        // not implemented
    }
    
    private String getTableName(String longname) {
        String rval = null;
        int index;
        if (longname != null) {
            // we want to return the table name without the schema.
            // If the schema isn't present, it will just return the
            // table name.
            index = longname.indexOf(".");
            if (index == -1) {
                // schema name is missing
                rval = longname;
            } else {
                rval = longname.substring(longname.indexOf(".") + 1);
            }
        } else {
            rval = "UNSET";
            LOG.warn("table name is not present in the event header");
        }
        return rval;
    }
    
    private String getMessageKey(String longname, String rowkey) {
        String schemaName;
        int index;
        if (longname != null) {
            // we want to return the schema name without the table.
            // If the schema isn't present, it will just return the
            // table name.
            index = longname.indexOf(".");
            if (index <=0) {
                // schema name is missing
                schemaName = "NOSCHEMA";
            } else {
                schemaName = longname.substring(0, longname.indexOf("."));
            }
        }
        else {
            schemaName = "UNSET";
            LOG.warn("table name is not present in the event header");
        }
        return schemaName+rowkey;
    }
}
