/* ./src/main/java/com/oracle/gghadoop/GG12Handler.java 
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
package com.oracle.gghadoop;

import oracle.goldengate.datasource.AbstractHandler;
import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.DsEvent;
import oracle.goldengate.datasource.DsOperation;
import oracle.goldengate.datasource.DsTransaction;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.TxOpMode;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.adapt.Tx;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass of AbstractHandler class as defined by the GoldenGate
 * Java User Exit documentation. This is the entry point into the
 * custom code implemented to process trail data. This handler is
 * for the GoldenGate 12.2 Java adapter and later. 
 * <p>
 * Use the GG11Handler if you are running GG 12.1 for big data or 
 * earlier. 
 * <p>
 * The implementation of code in this handler class has been
 * deliberately kept as simple as possible. The goal is to
 * abstract away all logic not directly tied to interfacing
 * to the core GoldenGate product into the GGHandlerClient
 * class. Database operations are received and immediately
 * handed off. The reason for this approach is to decouple
 * as much as possitlbe to minimize impact to the bulk of
 * the logic as the GoldenGate Java Adapter evolves
 * over time.
 *
 *
 */
public class GG12Handler extends AbstractHandler {
    private static final Logger LOG = LoggerFactory.getLogger(GG12Handler.class);
    private static boolean shutdown = false;
    private static Status shutdownStatus = Status.ABORT;
    
    private long numOps = 0;  // total number of operations processed
    private long numTxn = 0;  // total number of transactions processed
    private long numCommits = 0; // total number of commits processed
    private long numRollback = 0; // total number of rollbacks processed
    
    private GG12HandlerMapper client; 
    
    /**
     * Construct a handler using the specified operation mode.
     * 
     * @param txOpMode -- set operation (op) or transaction (tx) mode 
     *        in the superclass.
     */
    public GG12Handler(TxOpMode txOpMode) {
        super(txOpMode);
        
        LOG.info("Create GG12Handler: default mode: {}", getMode());
    }

    /**
     * Construct the handler defaulting to operation (op) mode; 
     * Setters should be called if needed  to set additional properties 
     * on the handler.
     */
    public GG12Handler() {
        // default to operation mode, which is best for big data
        super(TxOpMode.op); 
        
        LOG.info("Create GG12Handler: setting default mode: {}",
                    getMode());
    }

    /**
     * Initialize the base class and then do any other additional 
     * initialization specific to this user exit.
     * 
     * @param conf configuration parameters
     * @param metaData datasource metadata 
     */
    @Override
    public void init(DsConfiguration conf, DsMetaData metaData) {
        super.init(conf, metaData);
              
        LOG.info("init(): initializing environment");
        
        // ... do any additional config that may be needed ...
        client = new GG12HandlerMapper();
        client.init();
        LOG.debug("init(): returning");
        
    }

    /**
     * Process this new operation.
     *
     * @param evnt type of datasource event (transaction, operation, metadata)
     * @param txn  a database transaction
     * @param op  a database operation  (insert, update, delete)
     * @return  GGDataSource.Status (abend, ok; abort, redo, unknown unused)
     */
    @Override
    public Status operationAdded(DsEvent evnt, DsTransaction txn, DsOperation op) {
        Status rval;

        if (shutdown) {
            LOG.error("operationAdded(): shutting down");
            rval = shutdownStatus;
            clientCleanup();
        } else {

            rval = super.operationAdded(evnt, txn, op);

            LOG.debug("GG12Handler.operationAdded()");

            Op dbOperation;
            TableMetaData tMeta;

            numOps++;

            // since this is a Big Data related implementation, the
            // reality is that we probably always want to be in operation
            // mode, but handling this way just in case.
            if (isOperationMode() && rval == Status.OK) {
                // if call to super.operationAdded() failed, don't do this.
                tMeta = getMetaData().getTableMetaData(op.getTableName());
                dbOperation = new Op(op, tMeta, getConfig());
                rval = client.processOperation(dbOperation);
            }
        }

        return rval;
    }

    /**
     * Called at the beginning of a transaction. As we should really
     * always be in "operation mode", we don't really care where the
     * beginning of a transaction is in the context of the hadoop
     * world.
     *
     * @param evnt type of datasource event (transaction, operation, metadata)
     * @param txn database transaction
     * @return GGDataSource.Status (abend, ok; abort, redo, unknown unused)
     */
    @Override
    public Status transactionBegin(DsEvent evnt, DsTransaction txn) {
        Status rval;

        if (shutdown) {
            LOG.error("transactionBegin(): shutting down");
            rval = shutdownStatus;
            clientCleanup();
        } else {

            rval = super.transactionBegin(evnt, txn);

            LOG.debug("GG12Handler.transactionBegin()");
            numTxn++;
        }

        return rval;
    }
    /**
     * If we are in transaction mode (i.e. not in operation mode),
     * then process the operations that make up the transaction. The
     * reality of this adapter is that we really should always be in
     * operation mode, and so don't really care about where the
     * end of a transaction is.
     *
     * @param evnt type of datasource event (transaction, operation, metadata)
     * @param txn database transaction
     * @return GGDataSource.Status (abend, ok; abort, redo, unknown unused)
     */
    @Override
    public Status transactionCommit(DsEvent evnt, DsTransaction txn) {
        Status rval;

        if (shutdown) {
            LOG.error("transactionCommit(): shutting down");
            rval = shutdownStatus;
            clientCleanup();
        } else {

            rval = super.transactionCommit(evnt, txn);

            LOG.debug("transactionCommit()");
            numCommits++;

            Tx transaction = new Tx(txn, getMetaData(), getConfig());

            if (!isOperationMode() && rval == Status.OK) {
                // if call to super.transactionCommit() failed, don't do this.
                // Otherwise, iterate over the operations that make up the
                // transaction.
                for (Op op : transaction) {
                    // TODO: should probably set rval to worst case
                    // rather than last???
                    rval = client.processOperation(op);
                }
            }
        }

        return rval;
    }
    
    /**
     * Called if the transaction has to be rolled back for some reason.
     * Given the nature of data that is in the trail, I would not think
     * we will ever see a rollback.
     *
     * @param evnt type of datasource event (transaction, operation, metadata)
     * @param txn database transaction
     * @return GGDataSource.Status (abend, ok; abort, redo, unknown unused)
     */
    @Override
    public Status transactionRollback(DsEvent evnt, DsTransaction txn) {
        Status rval;

        if (shutdown) {
            LOG.error("transactionRollback(): shutting down");
            rval = shutdownStatus;
            clientCleanup();
        } else {

            rval = super.transactionRollback(evnt, txn);

            LOG.debug("transactionRollback()");
            numRollback++;
        }

        return rval;
    }

    /**
     * New metadata received for an object. This event fires when
     * processing an operation for a table that has not yet been seen.
     * <p>
     * It could also fire if the metadata were to change.
     * Going forward in GoldenGate for Big Data 12.2 and onward, dynamic
     * schema changes will be supported.
     *
     * @param evnt type of datasource event (transaction, operation, metadata)
     * @param meta datasource metadata
     * @return GGDataSource.Status (abend, ok; abort, redo, unknown unused)
     */
    @Override
    public Status metaDataChanged(DsEvent evnt, DsMetaData meta) {
        Status rval;

        if (shutdown) {
            LOG.error("metadataChanged(): shutting down");
            rval = shutdownStatus;
            clientCleanup();
        } else {

            rval = super.metaDataChanged(evnt, meta);

            //client.metaDataChanged(meta);
            client.metaDataChanged((TableMetaData) evnt.getEventSource());
        }


        return rval;
    }
    

    /**
     * Get the status of this handler.
     * 
     * @return a String that gives current throughput info
     */
    @Override
    public String reportStatus() { 
        String handlerStatus = 
            String.format("GG12Handler Status: Txn(%d), " +
                          "Ops(%d), Commits(%d), Rollbacks(%d)", 
                             numTxn, numOps, numCommits, numRollback); 
        LOG.info(handlerStatus);
        
        return client.reportStatus();
    }

    /**
     * Clean up on exit as the handler is being destroyed.
     */
    @Override
    public void destroy() {
        LOG.debug("destroy(): shutting down BDGlue");

        /* ... do cleanup ... */ 
        
        clientCleanup();
        
        super.destroy();  // do this last
    }
    
    private void clientCleanup() {
        LOG.debug("clientCleanup(): cleaning up BDGlue threads");
        try {
            // pause for a bit to allow queues to drain if they can.
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOG.trace("cleanup(): drain timer expired");
        }
        try {
            client.cleanup();
        } catch (InterruptedException e) {
            LOG.debug("cleanup(): InterruptedException");
        }
        try {
            // pause for a bit to allow threads to exit before GG pulls the plug.
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            LOG.trace("cleanup(): thread wait timer expired");
        }
    }

    /**
     * Static function to allow a decision to stop processing to be made anywhere in the BDGlue code.
     * This function will set a class variable to "true", which will be checked by by other
     * methods when called by GoldenGate before processing any data. These methods will return
     * a status of "ABORT" back to the caller, which in turn should cause GoldenGate to call 
     * the class "destroy()" method to clean things up.
     * 
     * @param msg a message to log
     */
    public static void shutdown(String msg) {
        LOG.error("shutdown(): {}", msg);
        shutdown = true;
    }

}
