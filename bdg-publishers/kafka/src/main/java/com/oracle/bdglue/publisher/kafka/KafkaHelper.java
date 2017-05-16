/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafkaHelper.java 
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

import com.google.common.base.Throwables;

import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class that contains common functionality leveraged by both the
 * Flume KafkaSink and by the KafkaPulisher.
 */
public class KafkaHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHelper.class);
    

    private Producer<Object, Object> producer;
    private Properties kafkaProps;
    private String topic;
    private int batchSize;
    private KafkaMessageHelper messageHelper;
    private List<KeyedMessage<Object, Object>> messageList;
    
    public KafkaHelper() {
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
            LOG.info("Message helper not set. Defaulting to KafkaMessageHelper");
            className = "com.oracle.bdglue.publisher.karka.KafkaMessageHelper";
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
        messageList = new ArrayList<KeyedMessage<Object, Object>>(batchSize);
        
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
    public Producer<Object, Object> getProducer() {
        return producer;
    }


    /**
     * @return the messageList.
     */
    public List<KeyedMessage<Object, Object>> messageList() {
        return messageList;
    }
    
    /**
     * clear the message list.
     */
    public void clearMessageList() {
        messageList.clear();
    }
    
    /**
     * Send the messageList to the Kafka Producer.
     */
    public void sendToProducer() {
        LOG.trace("Sending messageList to producer");
        
        producer.send(messageList);
        
    }

    /**
     * connect to the Kafka producer.
     */
    public void connect() {
        // instantiate the producer
        ProducerConfig config = new ProducerConfig(kafkaProps);
        producer = new Producer<Object, Object>(config);  
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
     * Process the received BDGlue event. Assumes the data is already formatted
     * in the event body.
     * @param event the BDGlue event we want to process.
     */
    public void process(EventData event) {
        String messageKey = null;
        String messageTopic = null;

        //byte[] eventBody = (byte[])event.eventBody();
        Object eventBody = event.eventBody();

        messageTopic = messageHelper.getTopic(event);
        messageKey = messageHelper.getMessageKey(event);
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("{Event} " + messageTopic + " : " + messageKey + " : " + eventBody );
        }

        // create a message and add to buffer
        
        KeyedMessage<Object, Object> data = new KeyedMessage<Object, Object>(messageTopic, messageKey, eventBody);
        
        messageList.add(data);
    }
}
