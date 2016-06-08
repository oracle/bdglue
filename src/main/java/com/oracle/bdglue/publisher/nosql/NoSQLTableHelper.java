/* ./src/main/java/com/oracle/bdglue/publisher/nosql/NoSQLTableHelper.java 
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
package com.oracle.bdglue.publisher.nosql;

import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.io.IOException;

import java.util.HashMap;

import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class containing common functionality for the Table API.
 */
public class NoSQLTableHelper extends NoSQLHelper {
    private static final Logger LOG = LoggerFactory.getLogger(NoSQLTableHelper.class);


    /**
     * handle to the NoSQL Table API
     */
    TableAPI tableHandle;

    /**
     * A Map of tables we are processing
     */
    HashMap<String, Table> tables;

    /**
     * Factory for JSON parsers.
     */
    JsonFactory factory;


    public NoSQLTableHelper() {
        super();
    }

    /**
     * Perform any needed initialization.
     *
     */
    @Override
    public void initialize() {
        tableHandle = getKVStore().getTableAPI();
        tables = new HashMap<>();
        factory = new JsonFactory();
    }

    /**
     * Process the BDGlue event. It can handle JSON-encoded data, but 
     * the preferred (and most efficient) interace is to leverage the NullEncoder.
     * 
     * @param event the BDGlue event we want to process.
     * @throws EventDeliveryException if we encounter an error
     */
    @Override
    public void process(EventData event) throws EventDeliveryException {
        String tableName;
        
        LOG.trace("processing TABLE event");
        tableName = event.getHeaders().get("table");
        if (tableName == null) {
            throw new EventDeliveryException("No table name found in headers!");
        }
        
        switch(event.getEncoderType()) {
        case JSON:
            processJSON(tableName, (byte[])event.eventBody());
            break;
        case NULL:
            processOperation(tableName, (DownstreamOperation)event.eventBody());
            break;
        default:
            throw new EventDeliveryException("Unexpected EncoderType encountered");
        }
    }
    
    /**
     * Process the Flume event and generate a new row in the table.
     *
     * @param event the Flume event we want to process
     * @throws EventDeliveryException if we encounter an error.
     */
    @Override
    public void process(Event event) throws EventDeliveryException {
        String tableName;


        LOG.trace("processing TABLE event");
        tableName = event.getHeaders().get("table");
        if (tableName == null) {
            throw new FlumeException("No table name found in headers!");
        }
        /*
         * At present, this only gets called via the Flume sink,
         * and data for the table API will always be in JSON format.
         */
        processJSON(tableName, event.getBody());
    }

    /**
     * Iterate through the operation and write to NoSQL.
     *
     * @param tableName the name of the target table
     * @param op the operation we are processing
     */
    public void processOperation(String tableName, DownstreamOperation op) {
        Table table;
        Row row;
        String columnName;
        String origColumnName;
        int jdbcType;

        table = getTable(tableName);
        row = table.createRow();

        for (DownstreamColumnData col : op.getColumns()) {
            origColumnName = col.getOrigName();
            columnName = col.getBDName();
            jdbcType = op.getTableMeta().getColumn(origColumnName).getJdbcType();

            switch (jdbcType) {
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                row.put(columnName, col.asBoolean());
                break;
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
                row.put(columnName, col.asInteger());
                break;
            case java.sql.Types.BIGINT:
                row.put(columnName, col.asLong());
                break;
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB:
                row.put(columnName, col.asString());
                break;
            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NCLOB:
                row.put(columnName, col.asString());
                break;
            case java.sql.Types.BLOB:
            case java.sql.Types.BINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.VARBINARY:
                row.put(columnName, col.asBytes());
                break;
            case java.sql.Types.REAL: // JDBC says 7 digits of mantisa
                row.put(columnName, col.asFloat());
                break;
            case java.sql.Types.FLOAT: // JDBC says 15 digits of mantisa
            case java.sql.Types.DOUBLE:
                row.put(columnName, col.asDouble());
                break;
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                // don't know how to do fixed decimal in NoSQL
                try {
                    row.put(columnName, col.asFloat());
                }
                catch (java.lang.IllegalArgumentException iae) {
                    // work around because GG reports type "numeric" for some integeger fields
                    row.put(columnName, col.asInteger());
                    op.getTableMeta().getColumn(origColumnName).setJdbcType(java.sql.Types.INTEGER);
                    LOG.warn("{}.{} : JDBC NUMERIC type mismatch. Switching to INTEGER", 
                             tableName, columnName);
                }
                break;
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
                row.put(columnName, col.asString());
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
                row.put(columnName, col.asString());
                break;
            }
        }
        LOG.debug("writing row for table {}", tableName);
        tableHandle.put(row, null, null);
    }

    /**
     * Parse JSON-encoded event body and write the row to NoSQL.
     * 
     * @param tableName the name of the table
     * @param jsonBody the operation we are processing encoded as JSON
     * @throws EventDeliveryException if we encounter an error
     */
    public void processJSON(String tableName, byte[] jsonBody) throws EventDeliveryException {
        Table table;
        Row row;
        String columnName;

        JsonParser parser;

        table = getTable(tableName);
        row = table.createRow();


        try {
            parser = factory.createJsonParser(jsonBody);

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                // This approach does not support arrays and other
                // complex types. Shouldn't be an issue in this case.
                if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                    parser.nextToken();
                    columnName = parser.getCurrentName();
                    switch (parser.getCurrentToken()) {
                    case VALUE_TRUE:
                        row.put(columnName, true);
                        break;
                    case VALUE_FALSE:
                        row.put(columnName, false);
                        break;
                    case VALUE_NULL:
                        // not sure how to handle null ...
                        //row.put(columnName, null);
                        break;
                    case VALUE_NUMBER_FLOAT:
                        // TODO: perhaps need to worry about
                        // double, float, Decimal???
                        row.put(columnName, parser.getFloatValue());
                        break;
                    case VALUE_NUMBER_INT:
                        // TODO: perhaps need to worry about
                        // int, long, etc.?
                        row.put(columnName, parser.getIntValue());
                        break;
                    case VALUE_STRING:
                        row.put(columnName, parser.getText());
                        break;
                    default:
                        // not a token we care about right now
                        LOG.error("Unhandled Json value type encountered during parsing");
                        throw new FlumeException("Unrecognized JSON value type");
                    }

                }
            }
            parser.close();
        } catch (IOException e) {
            LOG.error("process() exception:", e.getMessage());
            throw new EventDeliveryException("Json parsing error");
        }

        tableHandle.put(row, null, null);
    }

    /**
     * Perform any needed cleanup before exiting.
     */
    @Override
    public void cleanup() {
        LOG.info("cleaning up Table event serializer");
        tableHandle = null;
        super.cleanup();
    }


    /**
     * Get the KV Table object for this event.
     *
     * @param tableName the name of the table
     * @return the KV Table handle
     */
    protected Table getTable(String tableName) {
        Table rval;

        
        rval = tables.get(tableName);
        if (rval == null) {
            /*
             * Haven't seen this table yet. Make a new entry.
             */
            LOG.debug("First time for table: {}", tableName);
            rval = tableHandle.getTable(tableName);
            tables.put(tableName, rval);
        }
        return rval;
    }

 
}
