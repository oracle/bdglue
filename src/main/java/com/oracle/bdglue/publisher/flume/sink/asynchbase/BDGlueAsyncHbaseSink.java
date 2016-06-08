/* ./src/main/java/com/oracle/bdglue/publisher/flume/sink/asynchbase/BDGlueAsyncHbaseSink.java 
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
package com.oracle.bdglue.publisher.flume.sink.asynchbase;


import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import com.oracle.bdglue.publisher.asynchbase.AsyncHbaseHelper;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import org.hbase.async.HBaseClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starting point for this source:
 * org.apache.flume.sink.hbase.AsyncHBaseSink.java.
 *
 * A simple sink which reads events from a channel and writes them to HBase.
 * This Sink uses an aysnchronous API internally and is likely to
 * perform better.
 * The Hbase configution is picked up from the first <tt>hbase-site.xml</tt>
 * encountered in the classpath. This sink supports batch reading of
 * events from the channel, and writing them to Hbase, to minimize the number
 * of flushes on the hbase tables. There are no mandatory parameters required
 * to use this sink.<p>
 *
 * Optional parameters are:<p>
 *
 * <tt>batchSize: </tt>This is the batch size used by the client. This is the
 * maximum number of events the sink will commit per transaction. The default
 * batch size is 100 events.
 * <p>
 * <tt>timeout: </tt> The length of time in milliseconds the sink waits for
 * callbacks from hbase for all events in a transaction.
 * If no timeout is specified, the sink will wait forever.<p>
 *
 * Note: Hbase does not guarantee atomic commits on multiple
 * rows. So if a subset of events in a batch are written to disk by Hbase and
 * Hbase fails, the flume transaction is rolled back, causing flume to write
 * all the events in the transaction all over again, which may cause
 * duplicates. 
 */
public class BDGlueAsyncHbaseSink 
                extends AbstractSink implements Configurable {

    private AsyncHbaseHelper helper;
    private long batchSize;
    private static final Logger LOG = LoggerFactory.getLogger(BDGlueAsyncHbaseSink.class);
    private HBaseClient client;
    private Transaction txn;
    private volatile boolean open = false;
    private SinkCounter sinkCounter;

    public BDGlueAsyncHbaseSink() {
        super();
    }



    /**
     * Process the Flume event.
     * 
     * @return the Status
     * @throws EventDeliveryException if we encounter a Flume related error
     */
    @Override
    public Status process() throws EventDeliveryException {
        /**
         * Reference to the boolean representing failure of the 
         * current transaction. Since each txn gets a new boolean, 
         * failure of one txn will not affect the next even if 
         * errbacks for the current txn get called while
         * the next one is being processed.
         */
        if (!open) {
            throw new EventDeliveryException("Sink was never opened. " + 
                                             "Please fix the configuration.");
        }
        
        /**
         * Callbacks can be reused per transaction, since they share the same
         * locks and conditions.
         */
        helper.initializeCallbacks();
        

        Status status = Status.READY;
        Channel channel = getChannel();
        int i = 0;
        try {
            txn = channel.getTransaction();
            txn.begin();
            for (; i < helper.getBatchSize(); i++) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                    if (i == 0) {
                        sinkCounter.incrementBatchEmptyCount();
                    } else {
                        sinkCounter.incrementBatchUnderflowCount();
                    }
                    break;
                } else {
                    helper.process(event);
                    
                }
            }
        } catch (Throwable e) {
            this.handleTransactionFailure(txn);
            this.checkIfChannelExceptionAndThrow(e);
        }
        if (i == batchSize) {
            sinkCounter.incrementBatchCompleteCount();
        }
        sinkCounter.addToEventDrainAttemptCount(i);

 

        /**
         * At this point, either the txn has failed
         * or all callbacks received and txn is successful.
         *
         * This need not be in the monitor, since all callbacks for this txn
         * have been received. So txnFail will not be modified any more(even if
         * it is, it is set from true to true only - false happens only
         * in the next process call).
         */
        if (helper.checkCallbacks()) {
            this.handleTransactionFailure(txn);
            throw new EventDeliveryException("Could not write events to Hbase. " +
                                             "Flume transaction failed, and rolled back");
        } else {
            try {
                txn.commit();
                txn.close();
                sinkCounter.addToEventDrainSuccessCount(i);
            } catch (Throwable e) {
                this.handleTransactionFailure(txn);
                this.checkIfChannelExceptionAndThrow(e);
            }
        }

        return status;
    }

    /**
     * Configure based on the specified Context.
     * 
     * @param context the properties context
     */
    @Override
    public void configure(Context context) {
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(this.getName());
        }
        
        helper = new AsyncHbaseHelper();
        helper.configure(context);
        
    }

    /**
     * Start the sink and connect to HBase.
     */
    @Override
    public void start() {
        Preconditions.checkArgument(client == null, "Please call stop " + 
                                                    "before calling start on an old instance.");
        sinkCounter.start();
        sinkCounter.incrementConnectionCreatedCount();
        helper.connect();
        
        open = true;
        super.start();
    }

    /**
     * Stop the sink and disconnect from HBase.
     */
    @Override
    public void stop() {
        helper.cleanup();
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        open = false;
 
        super.stop();
    }

    /**
     * Handle a transaction failure. This will roll back the flume transaction.
     * 
     * @param txn the Flume transaction
     * @throws EventDeliveryException to indicate a failure
     */
    private void handleTransactionFailure(Transaction txn) 
                    throws EventDeliveryException {
        try {
            txn.rollback();
        } catch (Throwable e) {
            LOG.error("Failed to commit transaction." +
                         "Transaction rolled back.", e);
            if (e instanceof Error || e instanceof RuntimeException) {
                LOG.error("Failed to commit transaction." + 
                             "Transaction rolled back.", e);
                Throwables.propagate(e);
            } else {
                LOG.error("Failed to commit transaction." + 
                             "Transaction rolled back.", e);
                throw new EventDeliveryException("Failed to commit transaction." + 
                                                 "Transaction rolled back.", e);
            }
        } finally {
            txn.close();
        }
    }

 

    /**
     * Check the exception and either rethrow it, or propagate a new one.
     * 
     * @param e the exception to check.
     * @throws EventDeliveryException indicating a failure.
     */
    private void checkIfChannelExceptionAndThrow(Throwable e) 
                        throws EventDeliveryException {
        if (e instanceof ChannelException) {
            throw new EventDeliveryException("Error in processing transaction.", e);
        } else if (e instanceof Error || e instanceof RuntimeException) {
            Throwables.propagate(e);
        }
        throw new EventDeliveryException("Error in processing transaction.", e);
    }
}
