/* ./src/main/java/com/oracle/bdglue/publisher/flume/FlumePublisher.java 
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
package com.oracle.bdglue.publisher.flume;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.publisher.BDGluePublisher;

import java.util.Map;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of Publisher to communicate to Flume.
 * <p>
 * This will establish an RPC connection with Flume using either Avro or Thrift.
 * The client will create this connection using the host an port of the target
 * Flume agent ... or conversely the target Flume agent needs to listen at
 * the host and port passed as parameters of the init() function.
 */
public class FlumePublisher implements BDGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(FlumePublisher.class);
    private PropertyManagement properties;
    private Properties rpcProperties;
    private RpcClient client;
    private String hostname; 
    private int port;
    private int rpcMaxRetries = 0;
    private int rpcRetryDelaySecs = 0;
    private RPCType rpcType;
    private int rpcBatchSize = RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE;
    
    public FlumePublisher() {
        super();
        
        LOG.info("FlumePublisher()");
        client = null;
        properties = PropertyManagement.getProperties();
        rpcType = getRPCType();
        rpcMaxRetries = properties.asInt(BDGluePropertyValues.FLUME_RPC_RETRIES, BDGluePropertyValues.FLUME_RPC_RETRY_DEFAULT);
        rpcRetryDelaySecs = properties.asInt(BDGluePropertyValues.FLUME_RPC_RETRY_DELAY, BDGluePropertyValues.FLUME_RPC_RETRY_DELAY_DEFAULT);
        hostname = properties.getProperty(BDGluePropertyValues.FLUME_HOST, BDGluePropertyValues.FLUME_HOST_DEFAULT);
        port = properties.asInt(BDGluePropertyValues.FLUME_PORT, BDGluePropertyValues.FLUME_PORT_DEFAULT);
    }

    /**
     * Connect to Flume. Each connection will get its own socket on a port which should
     * improve I/O performance a bit, but the data all ultimately flow through the same
     * Flume agent (source-channel-sink configuration).
     */
    @Override
    public void connect() {
        LOG.info("connect(): " + hostname +":" + port);
        // Setup the RPC connection
        setProperties();
        getNewClient();
    }

    /**
     * Write the event data to Flume.
     * 
     * @param threadName the name of this thread.
     * @param evt the EventData
     */
    @Override
    public void writeEvent(String threadName, EventData evt) {
        Map<String, String> headerInfo;
        byte[] data;
        
        
        LOG.debug("writeEvent(): {}", threadName);
        headerInfo = evt.getHeaders();
        data = (byte[])evt.eventBody();
        
        // Create a Flume Event object that encapsulates the data
        // and include a map that contains the header info: Table name, 
        // op type, etc.
        
        Event event = EventBuilder.withBody(data, headerInfo);
        
                
        // Send the event
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            LOG.warn("sendDataToFlume(): EventDeliveryException: {}", 
                     e.getMessage());
            // clean up and recreate the client
            cleanup();
            getNewClient();
            
            // TODO: should we retry the event that failed? Or just move on?
        }
    }

    /**
     * Clean up the connection and disconnect.
     */
    @Override
    public void cleanup() {
        LOG.info("cleanUp()");

        if (client != null) {
            // Close the RPC connection
            client.close();
            client = null;
        }
    }
    
    /**
     * get a new RPC client
     */
    private void getNewClient() {
        boolean retry = true;
        int retryCount = 0;
        cleanup();
        
        LOG.info("getNewClient(): establishing RPC connection");

        while (retry) {

            try {
                switch (rpcType) {
                case THRIFT_RPC:
                    client = RpcClientFactory.getThriftInstance(hostname, 
                                                                port, rpcBatchSize);
                    LOG.info("getNewClient(): Thrift RPC client connection succeeded");

                    break;
                case AVRO_RPC:
                default:
                    // note: we could also set properties and call
                    // RpcClientFactory.getInstance(Properties props);
                    //client = RpcClientFactory.getDefaultInstance(hostname, 
                    //                                             port, rpcBatchSize);
                    client = RpcClientFactory.getInstance(rpcProperties);
                    LOG.info("getNewClient(): Avro RPC client connection succeeded");
                    retry = false;
                    break;
                }
            } catch (FlumeException fe) {
                LOG.warn("getNewClient(): FlumeException encountered: {}", 
                         fe.getMessage());
                LOG.info("*** Retrying connection ...");
                retryCount++;
                if (retryCount < rpcMaxRetries) {
                    try {
                        Thread.sleep(rpcRetryDelaySecs * 1000);
                    } catch (InterruptedException e) {
                        // in theory we should never be interrupted
                        // so we will just contine
                        retry = true;
                    }
                } else {
                    LOG.error("*** Max retries exceeded. Giving up.");
                    retry = false;
                    throw fe;
                }
            }
        }
    }

    
    /**
     * set the properties for the RPC connection. There are many
     * more properties than are reflected here. See 
     * RpcClientConfiguratonConstants for more info.
     */
    private void setProperties() {
        /* this is the default for NiOClientSocketChannelFactory, which is
         * called by NettyAvroRpcClient which is called from the 
         * RpcClientFactory. Setting it explicitly here to avoid a warning
         * message that would otherwise be printed. We shouldn't really
         * have to do this. :-)
         */  
        int maxIoWorkers = Runtime.getRuntime().availableProcessors() * 2;
        
        rpcProperties = new Properties();
        rpcProperties.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
        rpcProperties.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",
                 hostname + ":" + port);
        rpcProperties.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, 
                                  String.valueOf(rpcBatchSize));
        rpcProperties.setProperty(RpcClientConfigurationConstants.MAX_IO_WORKERS, 
                                 String.valueOf(maxIoWorkers));
    }

    /**
     * Get the type of RPC connection we are supposed to make.
     * 
     * @return the RPCType
     */
    public RPCType getRPCType() {
        RPCType rval;
        String rpcString = properties.getProperty(BDGluePropertyValues.FLUME_RPC_TYPE, BDGluePropertyValues.FLUME_RPC_TYPE_DEFAULT);
        if (rpcString.equalsIgnoreCase("avro-rpc"))
            rval = RPCType.AVRO_RPC;
        else if (rpcString.equalsIgnoreCase("thrift-rpc"))
            rval = RPCType.THRIFT_RPC;
        else {
            // default to AVRO_RPC
            rval = RPCType.AVRO_RPC;
            LOG.info("getRPCType(): defaulting to avro-rpc");
        }
        return rval;
    }
}
