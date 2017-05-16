/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafkaPublisherPropertyValues.java 
 *
 * Copyright 2017 Oracle and/or its affiliates.
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

/**
 * This class contains configuration constants used by the
 * Big Data Glue Kafka Publisher, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 * 
 */
public final class KafkaPublisherPropertyValues {
    /**
     * Properties related to KafkaPublisher.
     */
    /**
     * A default Kafka topic. Used by the Kafka Flume sink.
     */
    public static final String KAFKA_TOPIC = "bdglue.kafka.topic";
    /**
     * The batchize used for writes to Kafka. Larger values will perform better 
     * in high volume situations.
     */
    public static final String KAFKA_BATCH_SIZE = "bdglue.kafka.batchSize";
    /**
     * The frequency to flush data if the batch size specified hasn't been reached.
     * This prevents data from getting stale if there is a lull in volume.
     */
    public static final String KAFKA_FLUSH_FREQ = "bdglue.kafka.flushFreq";
    /**
     * Used to override the default serializer for Kafka event payloads. It is not
     * likely that this will be required.
     */
    public static final String KAFKA_MESSAGE_SERIALIZER = "bdglue.kafka.serializer.class";
    /**
     * Used to override the default serializer for Kafka event keys. It is not
     * likely that this will be required.
     */
    public static final String KAFKA_KEY_SERIALIZER = "bdglue.kafka.key.serializer.class";
    /**
     * A comma separated list of Kakfa brokers.
     */
    public static final String KAFKA_BROKER_LIST = "bdglue.kafka.metadata.broker.list";
    /**
     * Tell the producer to require an acknowledgement from the Broker that
     * a message was received. Default is to require acknowledgement. Overriding
     * this results in "fire and forget" which could result in data loss.
     */
    public static final String KAFKA_REQUIRED_ACKS = "bdglue.kafka.request.required.acks";
    /**
     * A class that implements KafkaMessageHelper to return token/message key info.
     */
    public static final String KAFKA_MESSAGE_METADATA = "bdglue.kafka.metadata.helper.class";
    /**
     * URL where we can find the registry.
     */
    public static final String KAFKA_REGISTRY_URL = "bdglue.kafka.producer.schema.registry.url";
    /**
     * The maximum schemas per subject to store.
     */
    public static final String KAFKA_REGISTRY_MAX_SCHEMAS = "bdglue.kafka.registry.max-schemas-per-subject";
    /**
     * The magic byte to add to a serialized message. This is defined here because the actual magic 
     * byte defined in the schema registry source is not publicly available.
     */
    public static final String KAFKA_REGISTRY_MAGIC_BYTE = "bdglue.kafka.registry.magic-byte";
    /**
     * The lenght of the id field to add to a serialized message. This is defined here because the actual  
     * size defined in the schema registry source is not publicly available.
     */
    public static final String KAFKA_REGISTRY_ID_SIZE = "bdglue.kafka.registry.id-size";
    
    
    
    
    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private KafkaPublisherPropertyValues() {
        super();
    }
}
