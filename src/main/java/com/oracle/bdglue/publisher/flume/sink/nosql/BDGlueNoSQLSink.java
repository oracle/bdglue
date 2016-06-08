/* ./src/main/java/com/oracle/bdglue/publisher/flume/sink/nosql/BDGlueNoSQLSink.java 
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
package com.oracle.bdglue.publisher.flume.sink.nosql;


import com.oracle.bdglue.publisher.nosql.NoSQLHelper;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inspired by:
 *        https://github.com/gvenzl/FlumeKVStoreIntegration.
 *
 * A simple sink which reads events from a channel and writes them
 * to Oracle's NoSQL database. At the present time, this commits on 
 * every Flume event. This should likely be enhanced to support 
 * batching of operations to improve performance.
 * <p>
 * To use this sink, it needs to be configured with certain
 * parameters:
 * <p>
 * <tt>kvHost: </tt> The host where NoSQL is running. The default is
 * "localhost". <p>
 * <tt>kvPort: </tt> The port number at hostname to connect to.
 * The default is "5000".<p>
 * <tt>kvStoreName: </tt> The name of the KV Store. The default
 * is "kvStore".<p>
 * <tt>durability: </tt> The transaction durability model. The default
 * is "WRITE_NO_SYNC".<p>
 * <tt>kvapi: </tt> The NoSQL API (kv or table) we will use. The default
 * is "kv_api".<p>
 * Other optional parameters are:<p>
 * <tt>none:</tt> None as of yet. May include transaction optimizations
 * such as 'batchsize' at some point.
 *
 *
 */
public class BDGlueNoSQLSink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(BDGlueNoSQLSink.class);
    private NoSQLHelper helper;

    public BDGlueNoSQLSink() {
        super();

    }

    /**
     * Set the context of this Sink based on properties specified
     * in the Flume configuration file.
     * 
     * @param context the configuraton Context
     */
    @Override
    public void configure(Context context) {
        String kvAPI;
        
        kvAPI = context.getString(BDGlueNoSQLSinkConfig.KVAPI, "kv_api");
        helper = NoSQLHelper.helperFactory(kvAPI); 
 
        helper.configure(context);
    }

    /**
     * Start the sink and connect to Oracle NoSQL.
     */
    @Override
    public final void start() {

        helper.connect(); /* connect to the KVStore */

        helper.initialize(); /* initialize the KVStore environment */
    }

    /**
     * Stop the sink and disconnect from Oracle NoSQL.
     */
    @Override
    public final void stop() {
        helper.cleanup();
        LOG.trace("Connection to KV store closed");
    }

    /**
     * Process the event and commit the data to Oracle NoSQL.
     * 
     * @return a Status
     * @throws EventDeliveryException if a Flume error occurs
     */
    @Override
    public final Status process() throws EventDeliveryException {
        LOG.debug("New event coming in, begin processing...");
        Status status = Status.READY;

        LOG.trace("Get Flume channel");
        Channel ch = getChannel();
        LOG.trace("Start transaction");
        Transaction txn = ch.getTransaction();
        txn.begin();
        LOG.trace("Transaction context established");

        // This try clause includes whatever Channel operations you want to do
        try {
            Event event = ch.take();
            if (event != null) {
                LOG.debug("Event received: " + event.toString());
                helper.process(event);
                LOG.trace("Event stored in KV store");
                txn.commit();
                LOG.trace("Transaction commited!");
            } else {
                txn.rollback();
                status = Status.BACKOFF;
            }
        } catch (Throwable t) {
            // Rollback transaction
            txn.rollback();
            status = Status.BACKOFF;
            
            t.printStackTrace();

            LOG.error("Error processing event!");
            LOG.error(t.toString());
            LOG.error(t.getMessage());

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            txn.close();
            LOG.trace("Transaction closed.");
        }

        // Return status to Flume (either Status.READY or Status.BACKOFF)
        return status;
    }

}
