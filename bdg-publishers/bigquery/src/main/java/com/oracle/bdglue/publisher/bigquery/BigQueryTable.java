/* ./src/main/java/com/oracle/bdglue/publisher/bigquery/BigQueryTable.java 
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
package com.oracle.bdglue.publisher.bigquery;


// Imports the Google Cloud client library


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Table;

import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.meta.schema.DownstreamColumnMetaData;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BigQueryTable {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTable.class);
    
    private int opCounter = 0;
    private int totalOps = 0;
    private int batchSize = 0;
    private boolean insertOnly;
    private int flushFreq = 500;
    private Timer timer;
    private TimerTask timerTask;
    
    private Table table;
    private BigQuery bigquery;
        
    private String tableName;
    
    private List<InsertAllRequest.RowToInsert> batch;
    private Map<String, Object> rowContent;
    
    

    /**
     * Construct an instance that represents this table.
     * 
     * @param table the BigQuery Table
     */
    public BigQueryTable(Table table) {
        super();
        
        this.bigquery = table.getBigquery();
        this.tableName = table.getTableId().getTable();
        this.table = table;
        init();
    }
    
    private void init() {
        PropertyManagement properties = PropertyManagement.getProperties();
        
        batchSize =
            properties.asInt(BigQueryPublisherPropertyValues.BIGQUERY_BATCH_SIZE,
                             BigQueryPublisherPropertyValues.BIGQUERY_BATCH_SIZE_DEFAULT);
        flushFreq =
            properties.asInt(BigQueryPublisherPropertyValues.BIGQUERY_FLUSH_FREQ,
                             BigQueryPublisherPropertyValues.BIGQUERY_FLUSH_FREQ_DEFAULT);
        insertOnly =
            properties.asBoolean(BigQueryPublisherPropertyValues.BIGQUERY_INSERT_ONLY,
                                 BigQueryPublisherPropertyValues.BIGQUERY_INSERT_ONLY_DEFAULT);
        
        /*
         * Currently, best practice recommendation from Google is to create something similar to
         * an audit table that will be post processed into final destination via an ETL job.
         */
        if (insertOnly == false) {
            LOG.warn("BigQueryTable(): updates and deletes not currently supported. Defaulting to insertOnly");
            insertOnly = true;
        }
        batch = new ArrayList<>();
        rowContent = new HashMap<>();
        
        timer = new Timer();

        // reinitialize things
        publishEvents();
    }


    /**
     * Create a row for this operation and return it.
     *
     * @param op the DownstreamOperation we are processing.
     */
    public void createRowToInsert(DownstreamOperation op) {
        InsertAllRequest.RowToInsert row = null;

        if (insertOnly) {
            row = insertOnly(op);
        } else {
            switch (op.getOpTypeId()) {
            case DownstreamOperation.INSERT_ID:
                row = insert(op);
                break;
            case DownstreamOperation.UPDATE_ID:
                row = update(op);
                break;
            case DownstreamOperation.DELETE_ID:
                row = delete(op);
                break;
            default:
                LOG.error("unrecognized operation type: {}", op.getOpType());
            }
        }

        if (row != null) {
            synchronized (this) {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("event #{}", totalOps);
                }

                batch.add(row);

                opCounter++;
                totalOps++;
                // publish batch and commit.
                if (opCounter >= batchSize) {
                    publishEvents();
                }
            }

        } else {
            LOG.error("createRowToInsert(): failed to create row from DownstreamOperation");
        }

        return;
    }
    
    /**
     * Flush any queued events and clean up timer in
     * preparation for shutdown.
     */
    public void cleanup() {
        LOG.info("Cleaning up table {} in preparation for shutdown", tableName);
        // flush any pending events
        publishEvents();
        // clean up the timer
        timer.cancel();
        
    }
    
    /**
     * Simple timer to ensure that we periodically flush whatever we have queued
     * in the event that we haven't received "batchSize" events by the time
     * that the timer has expired.
     */
    private class FlushQueuedEvents extends TimerTask {
        public void run() {

            publishEvents();
        }
    }

    /**
     * publish all events that we have queued up to BigQuery. This is called both by
     * the timer and by writeEvent(). Need to be sure they don't step on each other.
     */
    private void publishEvents() {
        InsertAllRequest request;
        InsertAllResponse response;
        
        synchronized (this) {
            if (timerTask != null) {
                timerTask.cancel();
                timerTask = null;
            }
            if (opCounter > 0) {
                request = InsertAllRequest.of(table, batch);
                response = bigquery.insertAll(request);
                logWriteErrors(response);
                opCounter = 0;
                batch.clear();
            }

            // ensure that we don't keep queued events around very long.
            timerTask = new FlushQueuedEvents();
            timer.schedule(timerTask, flushFreq);
        }
    }

    /**
     * Log any write errors that were encountered.
     * 
     * @param response from the InsertAllRequest()
     */
    private void logWriteErrors(InsertAllResponse response) {
        if (response.hasErrors()) {
            // One or more inserts failed. Inspect the data
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                int index = entry.getKey().intValue();
                String rowID = batch.get(index).getId();
                for(BigQueryError error : entry.getValue()) {
                    LOG.error("BigQuery Insert Error: Table={} RowId={} Message={}", 
                              tableName, rowID, error.getMessage());
                }
            }
        }
    }

    /**
     * Create row to insert in support of use of an InsertAllRequest.
     *
     * @param op the DownstreamOperation
     * @return a RowToInsert
     */
    private InsertAllRequest.RowToInsert insertOnly(DownstreamOperation op) {
        InsertAllRequest.RowToInsert row = null;
        Object value = null;
        DownstreamColumnData col = null;
        DownstreamColumnMetaData meta = null;
        
        // reuse rowContent for efficiency. Clear it here.
        rowContent.clear();

        /*
         * populate the meta columns requested in the properties (op type, timestamp, etc.)
         */
        for(Map.Entry<String, String> opMeta : op.getOpMetadata().entrySet()) {
            rowContent.put(opMeta.getKey(), opMeta.getValue());
        }

       
        ArrayList<DownstreamColumnMetaData> colsMeta = op.getTableMeta().getColumns();
        ArrayList<DownstreamColumnData> cols = op.getColumns();


        for (int i = 0; i < cols.size(); i++) {
            col = cols.get(i);
            meta = colsMeta.get(i);
            /*
             * col data and meta data should always be in the same order. 
             * I would check, but we will be doing this for every column on every 
             * row, which would be very expensive.
             */
            value = getDataValue(meta.getJdbcType(), col);
            rowContent.put(col.getBDName(), value);
        }

        row = InsertAllRequest.RowToInsert.of(rowContent);

        return row;
    }


    /**
     * Create an insert operation. Currently not implemented. May support
     * generation of DML to be issued via queries in support of I/U/D operations
     * at some point in the future.
     *
     * @param op the DownstreamOperation
     * @return a RowToInsert
     */
    private InsertAllRequest.RowToInsert insert(DownstreamOperation op) {
         return insertOnly(op);
    }
    
    /**
     * Create an update operation. Currently not implemented. May support 
     * generation of DML to be issued via queries in support of I/U/D operations
     * at some point in the future.
     * 
     * @param op the DownstreamOperation
     * @return a RowToInsert
     */
    private InsertAllRequest.RowToInsert update(DownstreamOperation op) {
        return insertOnly(op);
    }
    
    /**
     * Create a delete operation. Currently not implemented. May support 
     * generation of DML to be issued via queries in support of I/U/D operations
     * at some point in the future.
     * 
     * @param op the DownstreamOperation
     * @return a RowToInsert
     */
    private InsertAllRequest.RowToInsert delete(DownstreamOperation op) {
        return insertOnly(op);
    }
    
   
    /**
     * Get the value of the column formatted for BigQuery based on JDBC type.
     * 
     * @param jdbcType
     * @return return the vale for the column, null if not supported.
     */
    private Object getDataValue(int jdbcType, DownstreamColumnData col) {
        Object value = null;

        switch (jdbcType) {
        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            value = col.asBoolean();
            break;
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            value = col.asLong();
            break;
        case java.sql.Types.BIGINT:
            value = col.asLong();
            break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CLOB:
            value = col.asString();
            break;
        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
        case java.sql.Types.NCLOB:
            value = col.asString();
            break;
        case java.sql.Types.BLOB:
        case java.sql.Types.BINARY:
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
            // should be in base-64 format already
            value = col.asString();
            break;
        case java.sql.Types.REAL: // JDBC says 7 digits of mantisa
            value = col.asDouble();
            break;
        case java.sql.Types.FLOAT: // JDBC says 15 digits of mantisa
        case java.sql.Types.DOUBLE:
            value = col.asDouble();
            break;
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            // return as a String since fixed decimal format is not supported
            value = col.asString();
            break;
        case java.sql.Types.DATE:
            value = col.asString();
            break;
        case java.sql.Types.TIME:
            value = col.asString();
            break;
        case java.sql.Types.TIMESTAMP:
            value = col.asString();
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
            value = null;
            break;
        }
        return value;
    }
    
}
