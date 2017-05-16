/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafkaRegistryHelper.java
 *
 * Copyright 2016 Oracle and/or its affiliates.
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

import com.google.common.base.Throwables;

import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class that contains functionality needed by the
 * KafkaRegistryPulisher. This functionality is abstracted here to
 * facilitate other uses (such as Flume) at some point in the future.
 */
public class KafkaRegistryHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHelper.class);


    private KafkaProducer<Object, Object> producer;
    private Properties kafkaProps;
    private String topic;
    private int batchSize;
    private KafkaMessageHelper messageHelper;

    public KafkaRegistryHelper() {
        super();
        messageHelper = loadMessageHelper();
    }

    /**
     * Load the KafkaMessageHelper class specified in the properties file so we can
     * generate the desired topic and message key values.
     * @return the KafkaMessageHelper from the properties file
     */
    @SuppressWarnings("unchecked")
    private KafkaMessageHelper loadMessageHelper() {
        KafkaMessageHelper rval = null;
        PropertyManagement properties;
        String className = null;
        Class<KafkaMessageHelper> clazz = null;
        properties = PropertyManagement.getProperties();
        className = properties.getProperty(KafkaPublisherPropertyValues.KAFKA_MESSAGE_METADATA);
        if (className == null) {
            LOG.info("Message Helper not set. Defaulting to KafkaMessageHelper");
            className = "com.oracle.bdglue.publisher.kafka.KafkaMessageHelper";
        }
        try {
            LOG.info("loading KafkaMessageHelper: {}", className);
            clazz = (Class<KafkaMessageHelper>) Class.forName(className);
            rval = clazz.newInstance();

        } catch (Exception e) {
            LOG.error("Could not instantiate publisher.", e);
            Throwables.propagate(e);
        }
        return rval;
    }


    /**
     * Configure the class based on the properties for the user exit.
     * @param properties the properties we want to configure.
     */
    public void configure(PropertyManagement properties) {
        batchSize = KafkaProperties.getKafkaBatchSize();
        topic = KafkaProperties.getKafkaTopic();
        if (topic.equals(KafkaProperties.DEFAULT_TOPIC)) {
            LOG.warn("The Property 'topic' is not set. " + "Using the default topic name: " +
                     KafkaProperties.DEFAULT_TOPIC);
        }

        kafkaProps = KafkaProperties.getKafkaBDGlueProperties();

        messageHelper.setTopic(topic);

        logConfiguration();
    }

    /**
     * Log the configuraton information.
     */
    public void logConfiguration() {
        LOG.info("Configuration settings:");
        LOG.info("topic: " + topic);
        LOG.info("batchSize: " + batchSize);
        LOG.debug("Kafka producer properties: " + kafkaProps);
    }

    /**
     *
     * @return  handle to the Kafka producer
     */
    public KafkaProducer<Object, Object> getProducer() {
        return producer;
    }


    /**
     * connect to the Kafka producer.
     */
    public void connect() {
        // instantiate the producer
        producer = new KafkaProducer<Object, Object>(kafkaProps);
    }

    /**
     * Disconnect from the producer and clean up.
     */
    public void cleanup() {
        producer.close();
        LOG.debug("Connection to Kafka has been closed");
    }


    /**
     * Perform any needed initialization.
     *
     */
    public void initialize() {
        // TODO Implement this method
    }


    /**
     * Process the received BDGlue event and send to Kafka. Assumes the data is
     * already formatted in the event body.
     * @param event the BDGlue event we want to process.
     */
    public void process(EventData event) {
        String messageKey = null;
        String messageTopic = null;

        Object eventBody = event.eventBody();

        messageTopic = messageHelper.getTopic(event);
        messageKey = messageHelper.getMessageKey(event);

        if (LOG.isDebugEnabled()) {   
            LOG.debug("{Event} " + messageTopic + " : " + messageKey + " : " + eventBody );     
        }

        // create a message and send it

        ProducerRecord<Object, Object> record = 
            new ProducerRecord<Object, Object>(messageTopic, messageKey, eventBody);

        /*
         * send() is actually asynchronous. By default it will block if the receiving buffer gets full.
         * "null" as the second argument implies do not issue a callback. If it is not null, the
         * specified callback will be called once the message has successfully been accepted into the buffer.
         */
        producer.send(record, null);
    }
}
