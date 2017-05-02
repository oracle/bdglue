/* ./src/main/java/com/oracle/bdglue/encoder/MetadataHelper.java 
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
package com.oracle.bdglue.encoder;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;

import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.util.HashMap;


import java.util.LinkedHashSet;
import java.util.Map;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class helps deterimine what metadata should be encoded into any record.
 * It is a singleton.
 */
    

public class MetadataHelper {
   
    private static final Logger LOG = LoggerFactory.getLogger(MetadataHelper.class);
    private static MetadataHelper mdh = null;

    private PropertyManagement properties;
    private boolean tablename;
    private String tablenameCol;
    private boolean txId;
    private String txIdCol;
    private boolean txOpType;
    private String txOptypeCol;
    private boolean txTimestamp;
    private String txTimestampCol;
    private boolean txPosition;
    private String txPositionCol;
    private boolean userToken;
    private String userTokenCol;
    private Set<String> metadataCols;

    /**
     * Useful for all Encoders.
     */
    private MetadataHelper() {
        super();
        
        LOG.info("EventEncoder()");

        properties = PropertyManagement.getProperties();
        
        tablename = properties.asBoolean(BDGluePropertyValues.TABLENAME, "false");
        tablenameCol = properties.getProperty(BDGluePropertyValues.TABLENAME_COLUMN, "tablename");
        txId = properties.asBoolean(BDGluePropertyValues.TXID, "false");
        txIdCol = properties.getProperty(BDGluePropertyValues.TXID_COLUMN, "txid");
        txOpType = properties.asBoolean(BDGluePropertyValues.TX_OPTYPE, "false");
        txOptypeCol = properties.getProperty(BDGluePropertyValues.TX_OPTYPE_COLUMN, "txoptype");
        txTimestamp = properties.asBoolean(BDGluePropertyValues.TX_TIMESTAMP, "false");
        txTimestampCol = properties.getProperty(BDGluePropertyValues.TX_TIMESTAMP_COLUMN, "txtimestamp");
        txPosition = properties.asBoolean(BDGluePropertyValues.TX_POSITION, "false");
        txPositionCol = properties.getProperty(BDGluePropertyValues.TX_POSITION_COLUMN, "txposition");
        userToken = properties.asBoolean(BDGluePropertyValues.USERTOKEN, "false");
        userTokenCol = properties.getProperty(BDGluePropertyValues.USERTOKEN_COLUMN, "usertokens");
        
        metadataCols = setMetadataCols();
    }
    
    /**
     * Determines whether to include the table name in the record.
     * 
     * @return boolean that indicates whether to include the 
     * table name in a column along with the encoded data.
     */
    public boolean tablename() {
        return tablename;
    }
     /**
      * The column name to store the table name in.
      * 
      * @return the name of the table name column
      */
    public String tablenameCol() {
        return tablenameCol;
    }
    /**
     * Determines whether to include a transaction identifier in the record.
     * 
     * @return boolean that indicates whether to include the 
     * transactin ID in a column along with the encoded data.
     */
    public boolean txId() {
        return txId;
    }
     /**
      * The column name to store the transaction id in.
      * 
      * @return the name of the transactin id column
      */
    public String txIdCol() {
        return txIdCol;
    }
    /**
     * Determines whether to include position information in the record.
     * 
     * @return boolean that indicates whether to include the 
     * relative position of the operation in a column along 
     * with the encoded data.
     */
    public boolean txPosition() {
        return txPosition;
    }

    /**
     * The column name to store relative position data in.
     * 
     * @return the name of the relative position column
     */
    public String txPositionCol() {
        return txPositionCol;
    }
    /**
     * Determines whether to include user data
     * (supporting meta data) information in the record.
     * 
     * @return boolean that indicates whether to include the 
     * user tokens in a column along with the encoded data.
     */
    public boolean userToken() {
        return userToken;
    }

    /**
     * The column name for token information.
     * 
     * @return the name of the user token column
     */
    public String userTokenCol() {
        return userTokenCol;
    }
    /**
     * Determines whether to include timestamp information in the record.
     * 
     * @return boolean that indicates whether to include the 
     * timestamp along with the encoded data.
     */
    public boolean txTimestamp() {
        return txTimestamp;
    }

    /**
     * Get the column name for timestamp information.
     * 
     * @return the name of the timestamp column
     */
    public String txTimestampCol() {
        return txTimestampCol;
    }
    
    /**
     * Determines whether to include operation type in the record.
     * 
     * @return boolean that indicates whether to include the 
     * operation type along with the encoded column data.
     */
    public boolean txOpType() {
        return txOpType;
    }


    /**
     * Get the name of the operation type column.
     * 
     * @return the name of the optype column
     */
    public String txOptypeCol() {
        return txOptypeCol;
    }
    
    /**
     * Create a Set containing the metadata column names
     * that we want to include with this operation. Note that
     * a valid Set will always be returned, even if it has no
     * data. This is so iterators on the data can just work without
     * having to check for null.
     * 
     * @param op the DownstreaOperation
     * @return a Map containing the column name (key) and column value.
     */
    private Set<String> setMetadataCols() {
        // using a LinkdHashSet to preserve order.  
        LinkedHashSet<String> rval = new LinkedHashSet<>();
        
        if (tablename) {
            rval.add(tablenameCol);
        }
        if (txId) {
            rval.add(txIdCol);
        }
        if (txOpType) {
            rval.add(txOptypeCol);
        }
        if (txTimestamp) {
            rval.add(txTimestampCol);
        }
        if (txPosition) {
            rval.add(txPositionCol);
        }
        if (userToken) {
            rval.add(userTokenCol);
        }
        
        LOG.debug("setMetadataCols(): {}", rval.toString());
        
        return rval;
    }

    /**
     * Return a Set that contains the names of the metadata columns
     * that have been configured in the properties file.
     * 
     * @return a set of meta data column names
     */
    public Set<String> getMetadataCols() {
        return metadataCols;
    }


    /**
     * Create a Map containing the metadata columns
     * that we want to include with this operation. Note that
     * a valid Map will always be returned, even if it has no
     * data. This is so iterators on the data can just work without
     * having to check for null.
     * 
     * @param op the DownstreaOperation
     * @return a Map containing the column name (key) and column value.
     */
    public Map<String,String> getOpMetadata(DownstreamOperation op) {
        // using HashMap for now because it is a little faster. 
        // May change to LinkedHashMap to preserver order if the need arises.
        HashMap<String, String> rval = new HashMap<>();
        
        if (tablename) {
            rval.put(tablenameCol, op.getLongTableName());
        }
        if (txId) {
            rval.put(txIdCol, op.getTxId());
        }
        if (txOpType) {
            rval.put(txOptypeCol, op.getOpType());
        }
        if (txTimestamp) {
            rval.put(txTimestampCol, op.getTimeStamp());
        }
        if (txPosition) {
            rval.put(txPositionCol, op.getPosition());
        }
        if (userToken) {
            rval.put(userTokenCol, op.getUserTokens());
        }
        
        return rval;
    }

    /**
     * Get the metadata helper. Create one if this is the first call.
     * @return an instance of MetadataHelper
     */
    public static MetadataHelper getMetadataHelper() {
        if (mdh == null)
            mdh = new MetadataHelper();
        
        return mdh;
    }

 
}
