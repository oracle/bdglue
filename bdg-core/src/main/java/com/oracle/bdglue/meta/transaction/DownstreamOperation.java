/* ./src/main/java/com/oracle/bdglue/meta/transaction/DownstreamOperation.java 
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
package com.oracle.bdglue.meta.transaction;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.MetadataHelper;
import com.oracle.bdglue.meta.schema.DownstreamTableMetaData;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.Map;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The data that represents the operation type for this record. If this is 
 * database "change data", then will will represent database CRUD operations
 * (insert, update, delete). If BDGlue is being used to load data from somewhere, then
 * the type should be set to "INSERT" for insert. 
 *
 */
public class DownstreamOperation {
    private static final Logger LOG = LoggerFactory.getLogger(DownstreamOperation.class);

    private static boolean includeBefores = 
        PropertyManagement.getProperties().asBoolean(BDGluePropertyValues.INCLUDE_BEFORES, 
                                                     BDGluePropertyValues.INCLUDE_BEFORES_DEFAULT);
    private static boolean ignoreUnchanged = 
        PropertyManagement.getProperties().asBoolean(BDGluePropertyValues.IGNORE_UNCHANGED, 
                                                     BDGluePropertyValues.IGNORE_UNCHANGED_DEFAULT);
    
    private static MetadataHelper metadataHelper = MetadataHelper.getMetadataHelper();
    
    public static final String INSERT_VALUE = "INSERT";
    public static final String UPDATE_VALUE = "UPDATE";
    public static final String DELETE_VALUE = "DELETE";
    
    public static final int INSERT_ID = 1;
    public static final int UPDATE_ID = 2;
    public static final int DELETE_ID = 3;
    public static final int INVALID_ID = -1;
    
    private String opType;
    private int opTypeId;
    private String timestamp;
    private String position;
    private String userTokens;
    private String txId;
    private DownstreamTableMetaData table;
    private ArrayList<DownstreamColumnData> columns;
    private ArrayList<DownstreamColumnData> befores;

    /**
     * Create an operation to pass along for processing from the supplied meta data.
     *
     * @param opType the database operation type (insert, update, delete)
     * @param timestamp the transaction timestamp
     * @param position the relative position of this operation
     * @param userTokens the user tokens associated with this operation
     * @param txId the transaction identifier
     * @param tableMeta metadata for the table to which this operation applies
     */
    public DownstreamOperation(String opType, String timestamp, String position,
                               String userTokens, String txId, 
                               DownstreamTableMetaData tableMeta) {
        super();
        
        columns = new ArrayList<>();
        if (includeBefores || ignoreUnchanged) 
            befores = new ArrayList<>();
        else befores = null;
        
        this.opType = opType;
        setOpTypeId();
        this.timestamp = timestamp;
        this.userTokens = userTokens;
        this.position = position;
        this.txId = txId;
        this.table = tableMeta;
    }


    /**
     * Get the operation type that was passed into the constructor. This
     * operation type is used downstream in BDGlue to assist with applying the data
     * when interacting directly with a target. It is also passed along as metadata
     * for downstream use or audit/historical logging.
     * 
     * @return the SQL operation type (INSERT, UPDATE, DELETE)
     */
    public String getOpType() {
        return opType;
    }
    
    /**
     * Set the operation type as an integer that may be more efficient
     * in case statements downstream. This method is simple and bases its
     * decision on the first character of the op type that is passed in.
     */
    private void setOpTypeId() {
        char firstChar = opType.charAt(0);
        int val;

        switch (firstChar) {
        case 'I':
            val = INSERT_ID;
            break;
        case 'U':
            val = UPDATE_ID;
            break;
        case 'D':
            val = DELETE_ID;
            break;
        default:
            val = INVALID_ID;
            LOG.error("Unrecognized OpType: {}", opType);
        }

        opTypeId = val;
    }
    
    /**
     * Get the operation type as an integer that may be more efficient
     * in case statements downstream.
     * 
     * @return the operation ID as an integer
     */
    public int getOpTypeId() {
        return opTypeId;
    }
    
    /**
     * Get the transaction ID that was passed into the constructor. This
     * value is passed along as metadata for downstream use or 
     * audit/historical logging.
     * 
     * @return the transaction ID
     */
    public String getTxId() {
        return txId;
    }

    /**
     * Get the transaction timestamp that was passed into the constructor.
     * It is only passed along as received and not used directly by
     * BDGlue.
     * 
     * @return the timestamp of this transaction as a String
     */
    public String getTimeStamp() {
        return timestamp;
    }

    /**
     * Get the relative position of this operation from the source. It is
     * only passed along as received and not used by BDGlue.
     * 
     * @return the relative position of this operation as a String
     */
    public String getPosition() {
        return position;
    }

    /**
     * Add information pertaining to a column to this operation.
     * 
     * @param col column info
     */
    public void addColumn(DownstreamColumnData col) {
        columns.add(col);
    }
    /**
     * Add before image data pertaining to a column to this operation.
     * 
     * @param col column info
     */
    public void addBefore(DownstreamColumnData col) {
        befores.add(col);
    }

    /**
     * Compares the before and after images of update operations and
     * returns true if all of the column values in the after image
     * (which may not be a complete column list) DO NOT match the
     * corresponding values in the before image.
     *
     * @return true if the data has changed.
     */
    public boolean isDataChanged() {
        boolean rval = false;
        boolean beforeValueIsNull;
        boolean afterValueIsNull;
        
        DownstreamColumnData beforeCol;

        if (opTypeId == UPDATE_ID) {
            // this is only relevant for an update, not a delete or insert
            Map<String, DownstreamColumnData> beforeMap = columnMap(befores);

            // loop through the after images and compare with the befores
            LOG.debug("DownstreamOperation: before for loop");
            for (DownstreamColumnData afterCol : columns) {
                LOG.debug("DownstreamOperation: top of for loop {}", afterCol.getOrigName());
                beforeCol = beforeMap.get(afterCol.getOrigName());
                if (beforeCol != null) {
                    // we have before and after data, so compare the values
                    beforeValueIsNull = beforeCol.checkForNULL();
                    afterValueIsNull = afterCol.checkForNULL();
                    if (beforeValueIsNull || afterValueIsNull) {
                        // if either is null and if they don't match, then we are done.
                        if (beforeValueIsNull != afterValueIsNull) {
                            LOG.debug("DownstreamOperation: null column did not match {}", afterCol.getOrigName());
                            LOG.debug("DownstreamOperation: before: {}, after: {}", 
                                      beforeCol.asString(), afterCol.asString());
                            rval = true;
                            break;
                        }

                    } else {
                        if (afterCol.asString().equals(beforeCol.asString()) == false) {
                            LOG.debug("DownstreamOperation: column value did not match {}", afterCol.getOrigName());
                            // data did not match, so data has changed.
                            rval = true;
                            // only need one column to be different, so return now.
                            break;
                        }
                    }
                }
            }
        } else {
            // deletes and inserts are by definition "changed"
            rval = true;
        }

        return rval;
    }
    private Map<String, DownstreamColumnData> columnMap(ArrayList<DownstreamColumnData> cols) {
        Map<String, DownstreamColumnData> map = new HashMap<>();
        
        for(DownstreamColumnData column : cols) {
            map.put(column.getOrigName(), column);
        }
        return map;
    }
    /**
     * Create a contcatenation of the key columns into a 
     * single String, separating each value from the one preceding
     * it with a '/'. This is a simple implementation and assumes 
     * no slashes in the data.
     * 
     * @return a "key" as a String
     */
    public String getRowKey() {
        StringBuilder key = new StringBuilder();
        for(DownstreamColumnData col : columns) {
            if (col.isKeyCol()) {
                key.append('/');
                key.append(col.asString());
            }
        }
        return key.toString();
    }
    
    /**
     * Get any runtime supplied meta data. This is essentially a free form
     * string of information. In GoldenGate parlance, this is meta information
     * that is passed along with a record, represented as a comma delimited list
     * of name=value pairs.
     * 
     * @return the user tokens for this operation
     */
    public String getUserTokens() {
        return userTokens;
    }


    /**
     * Get the table meta data that corresponds to this operation.
     * 
     * @return the meta data for this table
     */
    public DownstreamTableMetaData getTableMeta() {
        return table;
    }

    /**
     * Get the short table name to which this operation pertains.
     * 
     * @return the short table name (w/o schema name)
     */
    public String getTableName() {
        return table.getBDTableName();
    }
    /**
     * Get the long table name to which this operation pertains.
     * 
     * @return the long table name (schema.table)
     */
    public String getLongTableName() {
        return table.getBDLongTableName();
    }

    /**
     * Get the table name to which this operation pertains and which is used as
     * a key to locate the meta data.
     * 
     * @return the table name used as a key in the meta data map (schema.table)
     */
    public String getKeyName() {
        return table.getKeyName();
    }

    /**
     * Get the column information associated with this operation and return
     * it as an ArrayList.
     * 
     * @return the column data as a List
     */
    public ArrayList<DownstreamColumnData> getColumns() {
        return columns;
    }
    
    /**
     * Get the before image column information associated with this operation and return
     * it as an ArrayList.
     * 
     * @return the column data as a List
     */
    public ArrayList<DownstreamColumnData> getBefores() {
        LOG.debug("getBefores()");
        return befores;
    }
    
    /**
     * Get the metadata column data for this operation as configured in the properties file.
     * 
     * @return The metadata as a Map.
     */
    public Map<String,String> getOpMetadata() {
        return metadataHelper.getOpMetadata(this);
    }
    
    /**
     * Get the metadata column names  as configured in the properties file.
     * 
     * @return The metadata as a Map.
     */
    public Set<String> getMetadataCols() {
        return metadataHelper.getMetadataCols();
    }
    

}