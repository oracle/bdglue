/* ./src/main/java/com/oracle/bdglue/publisher/asynchbase/AsyncHbaseHelper.java 
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
package com.oracle.bdglue.publisher.asynchbase;

import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import com.stumbleupon.async.Callback;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class that contains common funtionality that is 
 * leveraged both by the direct apply functionality
 * in AsyncHbasePublisher and by the Flume BDGlueAsyncHbaseSink.
 */
public class AsyncHbaseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncHbaseHelper.class);
    
    /**
     * The name of the event header "property" that will contain
     * the target table name.
     */
    private static final String TABLE_HDR = "table";
    /**
     * The name of the event header "property" that will contain
     * the target column family name for this event.
     */
    private static final String COLUMN_FAMILY_HDR = "columnFamily";
    /**
     * The name of the event header "property" that will contain
     * the target key value for this event.
     */
    private static final String ROWKEY_HDR = "rowKey";
    
    
    private HBaseClient client;
    private Configuration conf;
    
    AtomicBoolean txnFail = null;
    AtomicInteger callbacksReceived = null;
    AtomicInteger callbacksExpected = null;
    Lock lock = null;
    Condition condition = null;
    Callback<Object, Object> putSuccessCallback = null;
    Callback<Object, Object> putFailureCallback = null;
    Callback<Long, Long> incrementSuccessCallback = null;
    Callback<Long, Long> incrementFailureCallback = null;

    JsonFactory factory = null; 
    JsonParser parser = null;
    private byte[] table;
    private byte[] colFam;
    private byte[][] columnNames;
    private final List<PutRequest> puts = new ArrayList<>();
    private final List<AtomicIncrementRequest> incs = new ArrayList<>();
    private byte[] currentRowKey;
    private final byte[] eventCountCol = "eventCount".getBytes();
    private int batchSize;
    private long timeout;
 

    public AsyncHbaseHelper() {
        super();
        conf = HBaseConfiguration.create();
    }

   
    /**
     * Configure the class based on the properties for the user exit.
     * @param properties the properties we want to explicity configure
     */
    public void configure(PropertyManagement properties) {
        batchSize =
            properties.asInt(AsyncHbasePublisherPropertyValues.ASYNC_HBASE_BATCHSIZE, 
                             AsyncHbasePublisherPropertyValues.DEFAULT_BATCHSIZE);

        timeout =
            properties.asInt(AsyncHbasePublisherPropertyValues.ASYNC_HBASE_TIMEOUT, 
                             AsyncHbasePublisherPropertyValues.DEFAULT_TIMEOUT);
        if (timeout <= 0) {
            LOG.warn("Timeout should be positive for Hbase sink. " + "Sink will not timeout.");
            timeout = Integer.parseInt(AsyncHbasePublisherPropertyValues.DEFAULT_TIMEOUT);
        }

        logConfiguration();
    }

    /**
     * log the configuratoin settings.
     */
    public void logConfiguration() {
        LOG.info("Configuration settings:");
        LOG.info("timeout: " + timeout);
        LOG.info("batchSize: " + batchSize);
    }

    /**
     * Connect to the client.
     */
    public void connect() {

        String zkQuorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
        String zkBaseDir = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
        
        if (zkBaseDir != null) {
            client = new HBaseClient(zkQuorum, zkBaseDir);
        } else {
            client = new HBaseClient(zkQuorum);
        }

        client.setFlushInterval((short) 0);

    }

    /**
     * Disconnect from the client.
     */
    public void cleanup() {
        table = null;
        colFam = null;
        columnNames = null;
        currentRowKey = null;
        try {
            if (parser != null) {
                parser.close();
            }
        } catch (IOException e) {
            LOG.error("Error closing JSON parser", e);
        }
        client.shutdown();
        
        client = null;
    }


    /**
     * Perform any needed initialization.
     *
     */
    public void initialize() {
        initializeCallbacks();
    }

  

    /**
     * Process the received BDGlue event.
     * @param event The BDGlue event we want to process
     * 
     */
    public void process(EventData event) {
        List<PutRequest> actions;
        /*
         * Extracts the needed information from the event header
         */
        String rowKeyStr = event.getHeaders().get(ROWKEY_HDR);
        if (rowKeyStr == null) {
           throw new RuntimeException("No row key found in headers!");
        }
        currentRowKey = rowKeyStr.getBytes();
        
        String tableStr = event.getHeaders().get(TABLE_HDR);
        if (tableStr == null) {
           throw new RuntimeException("No table name found in headers!");
        }
        table = tableStr.getBytes();
        
        String cf = event.getHeaders().get(COLUMN_FAMILY_HDR);
        if (cf == null) {
           throw new RuntimeException("No column family found in headers!");
        }
        colFam = cf.getBytes();
        

        actions = setRowValues(event);
        List<AtomicIncrementRequest> increments = getIncrements();
        callbacksExpected.addAndGet(actions.size() + increments.size());

        for (PutRequest action : actions) {
            client.put(action).addCallbacks(putSuccessCallback, putFailureCallback);
        }
        for (AtomicIncrementRequest increment : increments) {
            client.atomicIncrement(increment).
                addCallbacks(incrementSuccessCallback,
                             incrementFailureCallback);
        }

  
    }
    
    /**
     *
     * @return the transaction batch size
     */
    public int getBatchSize() {
        return batchSize;
    }
    
    /**
     * check the status of the callbacks.
     * 
     * @return true on failure
     */
    public boolean checkCallbacks() {
        lock.lock();
        try {
            while ((callbacksReceived.get() < callbacksExpected.get()) 
                   && !txnFail.get()) {
                try {
                    if (!condition.await(timeout, TimeUnit.MILLISECONDS)) {
                        txnFail.set(true);
                        LOG.warn("HBase callbacks timed out. " + 
                                    "Transaction not completed successfully.");
                    }
                } catch (Exception e) {
                    LOG.error("Exception while waiting for callbacks from HBase.", e);
                    txnFail.set(true);
                }
            }
        } finally {
            lock.unlock();
        }
        
        return txnFail.get();
    }


    /**
     * Initialize the callbacks for this transaction.
     *
     */
    public void initializeCallbacks() {
        txnFail = new AtomicBoolean(false);
        callbacksReceived = new AtomicInteger(0);
        callbacksExpected = new AtomicInteger(0);
        lock = new ReentrantLock();
        condition = lock.newCondition();
        
        putSuccessCallback =
            new SuccessCallback<Object, Object>(lock, callbacksReceived, condition);
       putFailureCallback =
            new FailureCallback<Object, Object>(lock, callbacksReceived, txnFail, condition);

        incrementSuccessCallback =
            new SuccessCallback<Long, Long>(lock, callbacksReceived, condition);
        incrementFailureCallback =
            new FailureCallback<Long, Long>(lock, callbacksReceived, txnFail, condition);
    }


    /**
     * Loop through the data for this operation.
     *
     * @param event the BDGlue event we are processing
     * @return the list of actions that should be written out to hbase
     *
     */
    public List<PutRequest> setRowValues(EventData event) {
       
        puts.clear();
        processOperation((DownstreamOperation)event.eventBody());
        
 
        return puts;
    }
    
    /**
     * Iterate through the operation and write to NoSQL.
     *
     * @param op the operation we are processing.
     */
    public void processOperation(DownstreamOperation op) {
        byte[] data;
        byte[] columnName;


        String origColumnName;
        int jdbcType;

        for (DownstreamColumnData col : op.getColumns()) {
                origColumnName = col.getOrigName();
                columnName = col.getBDName().getBytes();
                jdbcType = op.getTableMeta().getColumn(origColumnName).getJdbcType();

                switch (jdbcType) {
                case java.sql.Types.BOOLEAN:
                case java.sql.Types.BIT:
                    int val = (col.asBoolean()) ? 1 : 0;
                    data = ByteBuffer.allocate(4).putInt(val).array();
                    break;
                case java.sql.Types.SMALLINT:
                case java.sql.Types.TINYINT:
                case java.sql.Types.INTEGER:
                    data = ByteBuffer.allocate(4).putInt(col.asInteger()).array();
                    break;
                case java.sql.Types.BIGINT:
                    data = ByteBuffer.allocate(8).putLong(col.asLong()).array();
                    break;
                case java.sql.Types.CHAR:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.CLOB:
                    data = col.asString().getBytes();
                    break;
                case java.sql.Types.NCHAR:
                case java.sql.Types.NVARCHAR:
                case java.sql.Types.LONGNVARCHAR:
                case java.sql.Types.NCLOB:
                    data =  col.asString().getBytes();
                    break;
                case java.sql.Types.BLOB:
                case java.sql.Types.BINARY:
                case java.sql.Types.LONGVARBINARY:
                case java.sql.Types.VARBINARY:
                    data = col.asBytes();
                    break;
                case java.sql.Types.REAL: // JDBC says 7 digits of mantisa
                    data = ByteBuffer.allocate(4).putFloat(col.asFloat()).array();
                    break;
                case java.sql.Types.FLOAT: // JDBC says 15 digits of mantisa
                case java.sql.Types.DOUBLE:
                    data = ByteBuffer.allocate(8).
                        putDouble(col.asDouble()).array();
                    break;
                case java.sql.Types.NUMERIC:
                case java.sql.Types.DECIMAL: 
                    if (col.asString().contains(".")) {
                        // don't know how to do fixed decimal in NoSQL
                        data = ByteBuffer.allocate(8).
                            putDouble(col.asDouble()).array();
                    }
                    else {
                        // work around because GG reports type "numeric" for some integeger fields
                        data = ByteBuffer.allocate(8).putLong(col.asLong()).array();
                        op.getTableMeta().getColumn(origColumnName).setJdbcType(java.sql.Types.INTEGER);
                        LOG.warn("{}.{} : JDBC NUMERIC type mismatch. Switching to INTEGER", 
                                 table, origColumnName);
                    }
                    
                    break;
                case java.sql.Types.DATE:
                case java.sql.Types.TIME:
                case java.sql.Types.TIMESTAMP:
                    data = col.asString().getBytes();
                    break;
                case java.sql.Types.DATALINK:
                case java.sql.Types.DISTINCT:
                case java.sql.Types.JAVA_OBJECT:
                case java.sql.Types.NULL:
                case java.sql.Types.ROWID:
                case java.sql.Types.SQLXML:
                case java.sql.Types.STRUCT:
                case java.sql.Types.ARRAY:
                case java.sql.Types.OTHER:
                case java.sql.Types.REF:
                default:
                    LOG.warn("Unsupported JDBC Type: {} for column: {}. Treating as String.", 
                              jdbcType, origColumnName);
                    data = col.asString().getBytes();
                    break;
                }

                //Generate a PutRequest for each column.
                PutRequest req = new PutRequest(table, currentRowKey, 
                                                colFam, columnName, data);
                puts.add(req);
            }

        LOG.debug("writing row for table {}", table);

    }


    /**
     * @return the list of increments that should be made in hbase
     * as the result of this event.
     */
    public List<AtomicIncrementRequest> getIncrements() {
        incs.clear();
        //Increment the number of events received
        incs.add(new AtomicIncrementRequest(table, 
                                            "totalEvents".getBytes(), 
                                            colFam, eventCountCol));
        return incs;
    }

    private class SuccessCallback<R, T> implements Callback<R, T> {
        private Lock lock;
        private AtomicInteger callbacksReceived;
        private Condition condition;

        /**
         * @param lck
         * @param callbacksReceived
         * @param condition
         */
        public SuccessCallback(Lock lck, 
                               AtomicInteger callbacksReceived, 
                               Condition condition) {
            lock = lck;
            this.callbacksReceived = callbacksReceived;
            this.condition = condition;
        }

        /**
         * @param arg
         * @return
         * @throws Exception
         */
        @Override
        public R call(T arg) throws Exception {
            callbacksReceived.incrementAndGet();
            lock.lock();
            try {
                condition.signal();
            } finally {
                lock.unlock();
            }
            return null;
        }
    }

    private class FailureCallback<R, T> implements Callback<R, T> {
        private Lock lock;
        private AtomicInteger callbacksReceived;
        private AtomicBoolean txnFail;
        private Condition condition;

        /**
         * @param lck
         * @param callbacksReceived
         * @param txnFail
         * @param condition
         */
        public FailureCallback(Lock lck, 
                               AtomicInteger callbacksReceived, 
                               AtomicBoolean txnFail, 
                               Condition condition) {
            this.lock = lck;
            this.callbacksReceived = callbacksReceived;
            this.txnFail = txnFail;
            this.condition = condition;
        }

        /**
         * Class related to the asynchronous callback processing
         * @param arg the arguments
         * @return null
         * @throws Exception may throw an exception
         */
        @Override
        public R call(T arg) throws Exception {
            callbacksReceived.incrementAndGet();
            this.txnFail.set(true);
            if (arg instanceof Exception) {
                Exception e = (Exception) arg;
                LOG.error("GGFlumeAsyncHbaseSink: callback excpetion", e);
            } else {
                LOG.error("GGFlumeAsyncHbaseSink: callback failure", arg);
            }
            
            lock.lock();
            try {
                condition.signal();
            } finally {
                lock.unlock();
            }
            return null;
        }
    }
    
}
