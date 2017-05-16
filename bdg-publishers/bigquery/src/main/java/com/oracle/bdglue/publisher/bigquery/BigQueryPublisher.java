/* ./src/main/java/com/oracle/bdglue/publisher/bigquery/BigQueryPublisher.java
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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;

import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.encoder.EventHeader;
import com.oracle.bdglue.meta.schema.DownstreamColumnMetaData;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;
import com.oracle.bdglue.publisher.BDGluePublisher;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish event data to Google BigQuery.. This implementation makes use of the V2
 * BigQuery java API developed by Google.  
 *
 */
public class BigQueryPublisher implements BDGluePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryPublisher.class);

    private BigQuery bigquery = null;
    private Dataset dataset = null;
    private DatasetInfo datasetInfo = null;
    private String datasetName = null;

    private HashMap<String, BigQueryTable> tables;



    public BigQueryPublisher() {
        super();

        PropertyManagement properties = PropertyManagement.getProperties();
        datasetName =
            properties.getProperty(BigQueryPublisherPropertyValues.BIGQUERY_DATASET,
                                   BigQueryPublisherPropertyValues.BIGQUERY_DATASET_DEFAULT);

        // batch = new xBatchStatement(BatchStatement.Type.LOGGED);
        tables = new HashMap<>();
    }

    @Override
    public void connect() {
        LOG.info("Connecting to BigQuery");
        // Instantiates a client
        bigquery = BigQueryOptions.getDefaultInstance().getService(); 
     
        // check to see if the dataset already exists
        dataset = bigquery.getDataset(datasetName);
        if (dataset == null) {
            // dataset was not found, so create it
            LOG.info("Dataset {} was not found. Creating it ...", datasetName);
            
            // Prepares a new dataset
            datasetInfo = DatasetInfo.newBuilder(datasetName).build();

            // Creates the dataset.
            dataset = bigquery.create(datasetInfo);
            
            LOG.info("Dataset {} created.", dataset.getDatasetId().getDataset());
        } else {
            LOG.info("Dataset {} found.", dataset.getDatasetId().getDataset());
        }
        
        logBigQueryInfo();
    }
    

    @Override
    public void cleanup() {
        LOG.info("disconnecting from BigQuery");
        
        for(BigQueryTable bqTable : tables.values()) {
            bqTable.cleanup();
        }

        // TODO: there does not appear to be a close / disconnect API. Confirm. 
        //bigquery.close();
    }

    @Override
    public void writeEvent(String threadName, EventData evt) {
        BigQueryTable bqTable;
        /*
         * Assuming we have the long name (schema.table), make the BQ table name
         * "schema_table" to ensure unique table names since BQ doesn't really have
         * schemas ... we would have to create a separate dataset for each schema otherwise.
         */
        String tableName = evt.getMetaValue(EventHeader.TABLE).replace('.','_');
        
        if (!tables.containsKey(tableName)) {
            // table hasn't yet been processed.
            LOG.info("writeEvent(): Processing table {} for the first time", tableName);
            bqTable = new BigQueryTable(getTable(tableName, evt));
            tables.put(tableName, bqTable);
        } else {
            bqTable = tables.get(tableName);
        }
        
        bqTable.createRowToInsert((DownstreamOperation) evt.eventBody());
    }
    
    /**
     * Locate the table in the Big Query dataset and create it if the
     * table isn't already there.
     * 
     * @param tableName the name of the table
     * @param evt the event data, which will include schema structure info.
     * @return a handle to the table in the dataset
     */
    private Table getTable(String tableName, EventData evt) {
        Table table = dataset.get(tableName);
        if (table == null) {
            // table hasn't yet been created in the Big Query data set. 
            table = createBigQueryTable(tableName, (DownstreamOperation)evt.eventBody());
        }
        return table;
    }
    
    /**
     * Create a table in the Big Query dataset based on the structure of 
     * the source records.
     * 
     * @param tableName The name of the table.
     * @param op A hande to the operation, which in turn links to the underlying schema.
     * @return a handle to a BigQuery Table.
     */
    private Table createBigQueryTable(String tableName, DownstreamOperation op) {
        Table table = null;
        Field.Type type = null;
        Field.Mode mode = null;
        List<Field> fields = new ArrayList<>();
        LOG.info("createBigQueryTable(): Table {} not found in dataset. Creating one.", tableName);
        for (String col : op.getMetadataCols()) {
            type = Field.Type.string();
            mode = Field.Mode.REQUIRED;
            fields.add(Field.newBuilder(col, type).setMode(mode).build());
        }
        for (DownstreamColumnMetaData col : op.getTableMeta().getColumns()) {
            type = getFieldType(col.getJdbcType());
            if (type != null) {
                if (col.isNullable()) {
                    mode = Field.Mode.NULLABLE;
                } else {
                    mode = Field.Mode.REQUIRED;
                }
              fields.add(Field.newBuilder(col.getBDColumnName(), type).setMode(mode).build());
            }
            else {
                LOG.warn("createBigQueryTable(): unsupported JDBC type for column {}. Column ignored.", 
                         col.getOrigColumnName());
            }
        }
        
        Schema schema = Schema.newBuilder().setFields(fields).build();
        try {
        table = dataset.create(tableName, StandardTableDefinition.of(schema));
        } catch (BigQueryException e) {
            if (e.getMessage().contains("Already Exists:")) {
                // Race conditiont: must have been creeated in another thread.
                LOG.info("createBigQueryTable(): table {} was already created in another thread.", tableName);
                table = dataset.get(tableName);
            } else {
                throw(e);
            }
        }
        
        return table;
    }
    
    /**
     * Get the BigQuery field type for this column.
     * 
     * @param jdbcType
     * @return return the Field.Type for the column, null if not supported.
     */
    private Field.Type getFieldType(int jdbcType) {
        Field.Type type = null;

        switch (jdbcType) {
        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            type = Field.Type.bool();
            break;
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            type = Field.Type.integer();
            break;
        case java.sql.Types.BIGINT:
            type = Field.Type.integer();
            break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CLOB:
            type = Field.Type.string();
            break;
        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
        case java.sql.Types.NCLOB:
            type = Field.Type.string();
            break;
        case java.sql.Types.BLOB:
        case java.sql.Types.BINARY:
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
            type = Field.Type.bytes();
            break;
        case java.sql.Types.REAL: // JDBC says 7 digits of mantisa
            type = Field.Type.floatingPoint();
            break;
        case java.sql.Types.FLOAT: // JDBC says 15 digits of mantisa
        case java.sql.Types.DOUBLE:
            type = Field.Type.floatingPoint();
            break;
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            // return as a String since fixed decimal format is not supported
            type = Field.Type.string();
            break;
        case java.sql.Types.DATE:
            type = Field.Type.date();
            break;
        case java.sql.Types.TIME:
            type = Field.Type.time();
            break;
        case java.sql.Types.TIMESTAMP:
            type = Field.Type.timestamp();
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
            type = null;
            break;
        }
        return type;
    }



    /**
     * Log the information related to our BigQuery connection.
     */
    private void logBigQueryInfo() {
        LOG.info("*** BigQuery information ***");
        LOG.info("Dataset Info: {}", dataset.toString());
        LOG.info("*** END BigQuery information ***");
    }
}
