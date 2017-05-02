/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafkaMessageHelper.java 
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


/**
 * A simple interface used for defining helper classes that allow for
 * custom logic for defining Kafka message Topics and Keys. This interface
 * support customizations for both the KafkaPublisher and for publishing
 * Kafka messages via the Flume KafkaSink.
 */
public interface KafkaMessageHelper {
    /**
     * Get the token for this message and return it to the KafkaPublisher.
     *
     * @param evt the EventData for this message
     * @return the topic
     */
    public String getTopic(EventData evt);
    
    /**
     * Get the message key for this message and return it to the KafkaPublisher.
     *
     * @param evt the EventData for this message
     * @return the message key
     */
    public String getMessageKey(EventData evt);

    /**
     * Set the topic value in use cases where it will always be the same
     * and not determined dynamically.
     *
     * @param topic the topic value
     */
    public void setTopic(String topic);
    
    /**
     * Set the message key value in use cases where it will always be the same
     * and not determined dynamically.
     *
     * @param key the message key value
     */
    public void setMessageKey(String key);
}
