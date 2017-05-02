/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafaMessageDefaultMeta.java 
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
 * Default class for getting metadata for the Kafka message. By default,
 * the topic is passed in via the Flume header as documented, or as a property
 * for the KafkaPublisher. The message key in both cases is the table name.
 */
public class KafkaMessageDefaultMeta implements KafkaMessageHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageDefaultMeta.class);
    
        
    String topic = null;
    String key = null;
    
    public KafkaMessageDefaultMeta() {
        super();
        
        LOG.debug("KafkaMessageDefaultMeta() has been loaded");
    }

    @Override
    public String getTopic(EventData evt) {
        return topic; // topic will always be the same in this handler.
    }

    @Override
    public String getMessageKey(EventData evt) {
        return evt.getMetaValue(EventHeader.TABLE);
    }



    @Override
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public void setMessageKey(String key) {
        this.key = key;
    }
}
