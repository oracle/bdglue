/* ./src/main/java/com/oracle/bdglue/publisher/kafka/KafkaProperties.java 
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

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;

import java.util.Map;
import java.util.Properties;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Inspired by org.apache.flume.sink.kafka.KafkaSinkConstants. Manage properties
 * associated with delivery of data to Kafka.
 */
public class KafkaProperties {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProperties.class);

    public static final String FLUME_PROPERTY_PREFIX = "kafka.";
    public static final String BDGLUE_PROPERTY_PREFIX = "bdglue.kafka.producer.";

    /* Properties */

    /**
     * The Flume event property that will contain the topic name.
     */
    public static final String TOPIC_FLUME = "topic";
    /**
     * The Flume event property that sets the batch size for writes to Kafka.
     */
    public static final String BATCH_SIZE_FLUME = "batchSize";
    /**
     * Property that identifies the serializer to use for writing the 
     * Kafka event body. Note that there is property may be set either in the
     * Flume configuration file for Flume delivery, or in the BDGlue
     * properties file for use by the KafkaPublisher.
     */
    public static final String MESSAGE_SERIALIZER = "serializer.class";
    /**
     * Property that identifies the serializer to use for writing the 
     * Kafka partitioning key value. Note that there is property may be set either in the
     * Flume configuration file for Flume delivery, or in the BDGlue
     * properties file for use by the KafkaPublisher.
     */
    public static final String KEY_SERIALIZER = "key.serializer.class";
    /**
     * Property that identifies the Kafka broker list to use when deliverying to 
     * Kafka. Note that there is property may be set either in the
     * Flume configuration file for Flume delivery, or in the BDGlue
     * properties file for use by the KafkaPublisher. The broker list is 
     * comma separated.
     */
    public static final String BROKER_LIST = "metadata.broker.list";
    /**
     * Property that identifies how to require acks from Kafka. 
     * Note that there is property may be set either in the
     * Flume configuration file for Flume delivery, or in the BDGlue
     * properties file for use by the KafkaPublisher.
     */
    public static final String REQUIRED_ACKS = "request.required.acks";
    /**
     * The Flume coniguration property for specifying the broker list to the
     * KafkaSink.
     */
    public static final String BROKER_LIST_FLUME = "brokerList";
    /**
     * The Flume coniguration property for specifying the required acks value
     * to the KafkaSink.
     */
    public static final String REQUIRED_ACKS_FLUME = "requiredAcks";


    /**
     * The default batch size.
     */
    public static final int DEFAULT_BATCH_SIZE = 100;
    /**
     * Force flush of accumulated records after ths number of
     * milliseconds in low volume situations where batch size has 
     * note yet been met.
     */
    public static final int DEFAULT_FLUSH_FREQ = 500;  // force publish after n milliseconds
    /**
     * The default topic to publish to in the event that one is not specified.
     */
    public static final String DEFAULT_TOPIC = "default-gg-topic";
    /**
     * The default message serializer to use when writing the event body. It is not
     * likely that this value will need to be overridden.
     */
    public static final String DEFAULT_MESSAGE_SERIALIZER = "kafka.serializer.DefaultEncoder";
    /**
     * The default serializer for the Kafka partitioning key. It is not likely that this
     * value will need to be overridden.
     */
    public static final String DEFAULT_KEY_SERIALIZER = "kafka.serializer.StringEncoder";
    /**
     * The default value for requiring acks from Kafka after write.
     * <p>
     * <tt>0:</tt> producer never waits for an ack.<p>
     * <tt>1:</tt> producer waits for an ack from the replica leader.<p>
     * <tt>-1:</tt> producer waits for an ack from all replicas.
     * 
     */
    public static final String DEFAULT_REQUIRED_ACKS = "1";
    
    /**
     * Get the name of the Kafka topic from the properties.
     * @return the topic as a String
     */
    public static String getKafkaTopic() {
        return PropertyManagement.getProperties().getProperty(BDGluePropertyValues.KAFKA_TOPIC,
                                                              KafkaProperties.DEFAULT_TOPIC);
    }
    /**
     * Get the batch size from the properties.
     * @return the batch size as an integer
     */
    public static int getKafkaBatchSize() {
        return PropertyManagement.getProperties().asInt(BDGluePropertyValues.KAFKA_BATCH_SIZE, 
                                     String.valueOf(KafkaProperties.DEFAULT_BATCH_SIZE));
    }
    
    /**
     * Get the flush frequency from the properties.
     * @return the flush frequency in milliseconds as an integer
     */
    public static int getKafkaFlushFreq() {
        return PropertyManagement.getProperties().asInt(BDGluePropertyValues.KAFKA_FLUSH_FREQ, 
                                     String.valueOf(KafkaProperties.DEFAULT_FLUSH_FREQ));
    }
    
    /**
     * Get the Kafka properties as configured by BDGlue.
     * 
     * @return as Properties
     */
    public static Properties getKafkaBDGlueProperties() {
        String value;
        Properties tprops;
        
        PropertyManagement properties = PropertyManagement.getProperties();
        Properties kafkaProperties = new Properties();
    
        value = properties.getProperty(BDGluePropertyValues.KAFKA_MESSAGE_SERIALIZER,
                                   KafkaProperties.DEFAULT_MESSAGE_SERIALIZER);
        kafkaProperties.setProperty(KafkaProperties.MESSAGE_SERIALIZER, value);

        value = properties.getProperty(BDGluePropertyValues.KAFKA_KEY_SERIALIZER, KafkaProperties.DEFAULT_KEY_SERIALIZER);
        kafkaProperties.setProperty(KafkaProperties.KEY_SERIALIZER, value);


        value = properties.getProperty(BDGluePropertyValues.KAFKA_BROKER_LIST, 
                                       "broker-not-specified");
        kafkaProperties.setProperty(KafkaProperties.BROKER_LIST, value);
        
        value = properties.getProperty(BDGluePropertyValues.KAFKA_REQUIRED_ACKS, KafkaProperties.DEFAULT_REQUIRED_ACKS);
        kafkaProperties.setProperty(KafkaProperties.REQUIRED_ACKS, value);
        
        tprops = properties.getPropertySubset(BDGLUE_PROPERTY_PREFIX, true);
        
        kafkaProperties.putAll(tprops);
                
        return kafkaProperties;
    }
    
    /**
     * Generate producer properties object with some defaults
     * to augment properties that may be set by Flume.
     * 
     * @return as Properties
     */
    private static Properties generateDefaultKafkaProps() {
      Properties props = new Properties();
      props.put(KafkaProperties.MESSAGE_SERIALIZER, KafkaProperties.DEFAULT_MESSAGE_SERIALIZER);
      props.put(KafkaProperties.KEY_SERIALIZER, KafkaProperties.DEFAULT_KEY_SERIALIZER);
      props.put(KafkaProperties.REQUIRED_ACKS, KafkaProperties.DEFAULT_REQUIRED_ACKS);
      return props;
    }
    
    /**
     * Get the Kafka-specific properties from the Flume configuration context.
     * 
     * @param context the Flume configuration context.
     * @return as Properties
     */
    public static Properties getKafkaFlumeProperties(Context context) {
      LOG.info("context={}",context.toString());
      Properties props =  generateDefaultKafkaProps();
      setKafkaProps(context, props);
      addDocumentedKafkaProps(context, props);
      return props;
    }

    /**
     * Some of the producer properties are especially important
     * We documented them and gave them a camel-case name to match Flume config
     * If user set these, we will override any existing parameters with these
     * settings.
     * Knowledge of which properties are documented is maintained here for now.
     * If this will become a maintenance issue we'll set a proper data structure.
     */
    private static void addDocumentedKafkaProps(Context context,
                                                Properties kafkaProps)
            throws ConfigurationException {
      String brokerList = context.getString(KafkaProperties.BROKER_LIST_FLUME);
      if (brokerList == null) {
        throw new ConfigurationException("brokerList must contain at least " +
                "one Kafka broker");
      }
      kafkaProps.put(KafkaProperties.BROKER_LIST, brokerList);

      String requiredAcks = context.getString(KafkaProperties.REQUIRED_ACKS_FLUME);

      if (requiredAcks != null ) {
        kafkaProps.put(KafkaProperties.REQUIRED_ACKS, requiredAcks);
      }
    }

    /**
     * Add all configuration parameters starting with "kafka"
     * to producer properties
     */
    private static void setKafkaProps(Context context, Properties kafkaProps) {

      Map<String,String> kafkaProperties =
              context.getSubProperties(KafkaProperties.FLUME_PROPERTY_PREFIX);

      for (Map.Entry<String,String> prop : kafkaProperties.entrySet()) {

        kafkaProps.put(prop.getKey(), prop.getValue());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reading a Kafka Producer Property: key: "
                  + prop.getKey() + ", value: " + prop.getValue());
        }
      }
    }
}
