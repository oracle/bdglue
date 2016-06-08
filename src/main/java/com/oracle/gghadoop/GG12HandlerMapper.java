/* ./src/main/java/com/oracle/gghadoop/GG12HandlerMapper.java 
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

import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsOperation;
import oracle.goldengate.datasource.DsToken;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.adapt.Col;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.DsType;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.BDGlueVersion;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EncoderFactory;
import com.oracle.bdglue.encoder.EncoderType;
import com.oracle.bdglue.encoder.ParallelEncoder;
import com.oracle.bdglue.encoder.avro.AvroSchemaFactory;
import com.oracle.bdglue.meta.schema.DownstreamColumnMetaData;
import com.oracle.bdglue.meta.schema.DownstreamSchemaMetaData;
import com.oracle.bdglue.meta.schema.DownstreamTableMetaData;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.sql.Types;

import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class essentially takes care of mapping the classes that
 * are defined and used by the GoldenGate Java Adapter to the classes
 * that are used by BDGlue. The intent is to completely decouple the two sides
 * to hopefully make it easier to change as the GoldenGate Java API evolves.
 */
public class GG12HandlerMapper {
    private static final Logger LOG = LoggerFactory.getLogger(GG12HandlerMapper.class);
    private boolean includeBefores; 
    private boolean ignoreUnchangedRows;

    private ParallelEncoder encoder;
    private DownstreamSchemaMetaData downstreamSchemaMeta;
    private BDGlueVersion version;
    private String lastTxID = "unset";
    
    public GG12HandlerMapper() {
        super();
        version = new BDGlueVersion();
        LOG.info(version.format());
    }
    
    /**
     * Create the appropriate encoder and initialize.
     */
    public void init() {

        LOG.info("init(): initalizing the client interface");
       
        encoder = new ParallelEncoder();
        encoder.start();
        
        // create an instance of the class that we will use to keep 
        // track of the schema meta data
        downstreamSchemaMeta = new DownstreamSchemaMetaData();
        
        includeBefores = 
                PropertyManagement.getProperties().asBoolean(BDGluePropertyValues.INCLUDE_BEFORES, 
                                                             BDGluePropertyValues.INCLUDE_BEFORES_DEFAULT);
        ignoreUnchangedRows = 
                PropertyManagement.getProperties().asBoolean(BDGluePropertyValues.IGNORE_UNCHANGED, 
                                                             BDGluePropertyValues.IGNORE_UNCHANGED_DEFAULT);
    }
    
    
    /**
     * Perform all necessary clean up prior to exiting. This would
     * include draining queues, shutting down threads, etc.
     * 
     * @throws InterruptedException if interrupted while shutting down
     */
    public void cleanup() throws InterruptedException {
        encoder.cancel();
    }

    
    /**
     * Process the metadata for a table and tell BDGlue about it.
     * 
     * @param tableMeta the meta data for the table that changed
     */
    public void metaDataChanged(TableMetaData tableMeta) {
        LOG.debug("metaDataChanged(TableMetaData)");
        
        setMetaData(tableMeta);      
    }
    
    /**
     * Process the database operation and put it into a form
     * that BDGlue can understand.
     * 
     * @param ggOp the database operation
     * @return the resulting status of processing
     *
     */
    public Status processOperation(Op ggOp) {
        String opType;
        String timestamp;
        String position;
        String userTokens;
        String txId;
        
        Status rval = Status.OK;
        
        LOG.debug("processOperation()");
        
        String tableName = 
            getTableKeyName(ggOp.getTableName());

        DownstreamTableMetaData tableMeta = 
            downstreamSchemaMeta.getTableMetaData(tableName);
        
        opType = ggOp.getSqlType();
        timestamp = ggOp.getTimestamp();
        position = ggOp.getPosition();
        userTokens = setUserTokens(ggOp.getOperation());
        
        DsToken token = ggOp.getToken("L");
        if (token.isSet()) {
            txId = token.getValue();
            if (txId != null) {
                lastTxID = txId;
            }
        } else {
            LOG.trace("LOGCSN is not set: Key='{}' value='{}'", token.getKey(), token.getValue());
            Map<String, DsToken> tokenMap = ggOp.getOperation().getTokens();
            for (Map.Entry<String, DsToken> foo : tokenMap.entrySet()) {
                DsToken bar = foo.getValue();
                LOG.trace("     Token key='{}' value='{}' ", bar.getKey(), bar.getValue());
            }
        }
        

        DownstreamOperation dbOperation = 
            new DownstreamOperation(opType, timestamp, position, userTokens, lastTxID, tableMeta);
        setColumns(dbOperation, ggOp);

        if ((ignoreUnchangedRows == false) || dbOperation.isDataChanged() == true) {
            try {
                encoder.put(dbOperation);
            } catch (InterruptedException e) {
                LOG.debug("processOperation: InterruptedException");
            }
        } else {
            LOG.debug("processOperation(): Ignoring unchanged row for table: {}", tableName);
        }
        
        return rval;
    }

    /**
     * Report the status of this handler, including all encoder and
     * publisher threads running in BDGlue.
     * 
     * @return throughput stats for the handler
     */
    public String reportStatus() {
        StringBuilder status = new StringBuilder();
        status.append("************* Begin GGHandlerClient Status *************\n");
        status.append(encoder.status());
        status.append("*************  End GGHandlerClient Status  *************\n");      
        
        return status.toString();  
    }
    
    /********************************************************/
    /** private methods for mapping GG Java User Exit meta  */
    /** data to the downstream meta data used by the        */
    /** encoders and publishers. This allows us to          */
    /** completely decouple the user exit from the bulk of  */
    /** the code, better facilitating unit test as well as  */
    /** reuse by other "upstream" users.                    */
    /********************************************************/
    
   
    /**
     * Set the metadata for a table so BDGlue can understand it.
     * 
     * @param ggTableMeta the high level table meta data object from the Java User Exit
     * 
     */
    private void setMetaData(TableMetaData ggTableMeta) {
        TableName tableName;
        String key;
        DownstreamTableMetaData downstreamTableMeta;
        
        tableName = ggTableMeta.getTableName();
        key = getTableKeyName(tableName);
            
        downstreamTableMeta = addTable(key, ggTableMeta);
        
        /**
         * TODO: figure out how to make this same decision without knowing about EncoderFactory.
         */
        if (EncoderFactory.getEncoderType() == EncoderType.AVRO_BYTE_ARRAY ||
            EncoderFactory.getEncoderType() == EncoderType.AVRO_GENERIC_RECORD) {
            AvroSchemaFactory.getAvroSchemaFactory().metaDataChanged(downstreamTableMeta);
        }

    }

    /**
     * Add a new table definition to the list of tables that BDGlue 
     * keeps track of.
     * 
     * @param key the key that will be used in the map for looking 
     *            up table information
     * @param ggTableMetaData GG Java UE meta data for the table.
     * @return the metadata for the table we added / replaced.
     */
    private DownstreamTableMetaData addTable(String key, TableMetaData ggTableMetaData) {
        DownstreamTableMetaData downstreamTable;
        String tableName = ggTableMetaData.getTableName().getOriginalShortName();
        String longTableName = ggTableMetaData.getTableName().getOriginalName();

        downstreamTable = new DownstreamTableMetaData(key, tableName, longTableName);
        setColumnInfo(downstreamTable, ggTableMetaData);
        
        
        downstreamSchemaMeta.addTable(key, downstreamTable);
        return downstreamTable;
    }
    
    /**
     * Get the value that we use as a key to locate
     * a table definition to see if we already have it.
     * 
     * @param tableName the TableName object
     * @return the string representing the table name that will be 
     *         used as a key.
     */
    private String getTableKeyName(TableName tableName) {
        return tableName.getOriginalName();
    }
    
    /**
     * Set the column info in the table so BDGlue can reference it.
     * 
     * @param table the downstream meta data for the table
     * @param ggTableMetaData the GG Java UE meta data for the table
     */
    private void setColumnInfo(DownstreamTableMetaData table, TableMetaData ggTableMetaData) {
        ArrayList<ColumnMetaData> ggColumnMetaDataArray;
        DownstreamColumnMetaData downstreamColumnMeta;
        String columnName; 
        boolean keyCol;
        boolean nullable;
        int jdbcType;
        
        ggColumnMetaDataArray = ggTableMetaData.getColumnMetaData();
        
        for (ColumnMetaData ggColumnMetaData : ggColumnMetaDataArray) {
            columnName = ggColumnMetaData.getOriginalColumnName();
            keyCol = ggColumnMetaData.isKeyCol();
            /*
             * GG is not reliable in setting this value at the present time. 
             * Force to always be true for now. This will allow Avro schema 
             * generation to generate all fields in the schema as "nullable".
             */
            /* nullable = ggColumnMetaData.getNullable(); */
            nullable = true;
            
            jdbcType = getJdbcType(ggColumnMetaData);
            LOG.trace("**** nullabe: {}   type: {}", nullable, jdbcType);
            downstreamColumnMeta = new DownstreamColumnMetaData(columnName, keyCol, nullable, jdbcType);
            table.addColumn(downstreamColumnMeta);
        }
    }
    
    /**
     * Look at the metadata for the column and override the given JDBC type
     * in certain situations where it doesn't currently truly reflect  the 
     * data type. Essentailly, this is getting around what in my opinion
     * is a bug.
     * 
     * @param cmd the ColumnMetaData
     * @return the potentially overridden JDBC Type of the column.
     */
    private int getJdbcType(ColumnMetaData cmd) {
        int jdbcType;

        DsType.GGType ggType;
        DsType.GGSubType ggSubType;

        jdbcType = cmd.getDataType().getJDBCType();
        ggType = cmd.getDataType().getGGDataType();
        ggSubType = cmd.getDataType().getGGDataSubType();


        if (ggSubType == DsType.GGSubType.GG_SUBTYPE_FIXED_PREC) {
            jdbcType = Types.DECIMAL;
        } else {
            switch (ggType) {
            case GG_16BIT_S:
            case GG_16BIT_U:
                if (ggSubType == DsType.GGSubType.GG_SUBTYPE_DEFAULT) {
                    jdbcType = Types.SMALLINT;
                }
                break;
            case GG_32BIT_S:
            case GG_32BIT_U:
                if (ggSubType == DsType.GGSubType.GG_SUBTYPE_DEFAULT) {
                    jdbcType = Types.INTEGER;
                }
                break;
            case GG_64BIT_S:
            case GG_64BIT_U:
                if (ggSubType == DsType.GGSubType.GG_SUBTYPE_DEFAULT) {
                    jdbcType = Types.BIGINT;
                }
                break;
            case GG_DOUBLE:
                jdbcType = Types.DOUBLE;
                break;
            case GG_ASCII_V:
                if (ggSubType == DsType.GGSubType.GG_SUBTYPE_FLOAT) {
                    jdbcType = Types.DOUBLE;
                }
                break;
            default:
                // fall through and accept what jdbcType was set to
                break;
            }
        }

        return jdbcType;
    }
    
    /**
     * Set the column data values for the current operation.
     * 
     * @param downstreamOp the operation we are encoding into
     * @param ggOp the GG database operation we are encoding from.
     */
    public void setColumns(DownstreamOperation downstreamOp, Op ggOp) {
        DownstreamColumnData downstreamCol;
        
        String columnName;
        String stringValue;
        byte[] binaryValue;
        boolean keyCol;
        
        for(Col ggOpCol : ggOp) {
            DsColumn tcol;
            
            keyCol = ggOpCol.getMeta().isKeyCol();
            columnName = ggOpCol.getOriginalName();

            if (ggOpCol.hasAfterValue()) {
                tcol = ggOpCol.getAfter();

                if (tcol.isValueNull())
                    stringValue = null;
                else stringValue = tcol.getValue();
                if (tcol.hasBinaryValue())
                    binaryValue = tcol.getBinary();
                else binaryValue = null;
            } else if (ggOpCol.hasBeforeValue()) {
                tcol = ggOpCol.getBefore();
                if (tcol.isValueNull())
                    stringValue = null;
                else stringValue = tcol.getValue();
                if (tcol.hasBinaryValue())
                    binaryValue = tcol.getBinary();
                else binaryValue = null;
            }
            else { 
                //binaryValue = null;
                //stringValue = null;
                // both before and after images are missing from the trail,
                // so we are opting to ignore this column downstream.
                LOG.trace("Column not present in trail: {}", columnName);
                continue; // column is missing from the trail, so ignore it here.
            }
            
            if (isBitField(ggOpCol)) {
                stringValue = getBitData(stringValue, columnName);
            }
            
            downstreamCol = new DownstreamColumnData(columnName, stringValue, 
                                                     binaryValue, keyCol);
            downstreamOp.addColumn(downstreamCol);
            
            if (includeBefores | ignoreUnchangedRows) {
                if (ggOpCol.hasBeforeValue()) {
                    tcol = ggOpCol.getBefore();
                    stringValue = tcol.getValue();
                    if (tcol.hasBinaryValue())
                        binaryValue = tcol.getBinary();
                    else
                        binaryValue = null;
                } else {
                    binaryValue = null;
                    stringValue = null;
                }
                if (isBitField(ggOpCol)) {
                    stringValue = getBitData(stringValue, columnName);
                }
                downstreamCol = new DownstreamColumnData(columnName, stringValue, 
                                                         binaryValue, keyCol);
                LOG.trace("adding before image: " + columnName + ":" + stringValue);
                downstreamOp.addBefore(downstreamCol);
            }
        }
    }
    
    /**
     * Hack to check to see if this might be a bit field.
     * 
     * @param ggOpCol the column data
     * @return boolean true if it looks like a bit field.
     */
    private boolean isBitField(Col ggOpCol) {
        DsType dst = ggOpCol.getMeta().getDataType();
        long len = ggOpCol.getMeta().getColumnLength();
        boolean rval = false;
        
        if ((dst.getGGDataType() == DsType.GGType.GG_ASCII_F) &&
            (dst.getGGDataSubType() == DsType.GGSubType.GG_SUBTYPE_BINARY) && len == 1) {
            rval = true;
        }
        
        return rval;
    }
    /**
     * Continuation of the bit field hack around.
     * @param ggOpCol
     * @return the value of the field.
     */
    private String getBitData(String strg, String colName) {
        LOG.debug("converting to bit type for column: {}", colName);

        if (strg != null) {
            if (strg.equalsIgnoreCase("AA==")) {
                strg = "0";
            } else if (strg.equalsIgnoreCase("AQ==")) {
                strg = "1";
            } else {
                // must not be so leave it alone
            }
        }

        return strg;
    }
    /**
     * Return a String containing a comma separated list of user tokens.
     * 
     * @param dsOp the database operation
     * @return the list of tokens. Null if there are none.
     */
    public String setUserTokens(DsOperation dsOp) {
        String rval = null;
        Map<String, DsToken> tokens = dsOp.getTokens();
        
        LOG.trace("setUserTokens(): tokens.size={}", tokens.size());
        if (tokens.size() > 0) {
            // there are tokens to process
            for(Map.Entry<String, DsToken> token : tokens.entrySet()) {
                if (rval == null) {
                    // first one
                    rval = String.format("%s=\"%s\"", token.getKey(), token.getValue());
                }
                else {
                    // separate the rest with a comma
                    rval = rval + String.format(",%s=\"%s\"", token.getKey(), token.getValue());
                }
            }
        }
        else rval = "NONE";
        
        LOG.trace("setUserTokens(): rval={}", rval);
        
        return rval;
    }


}
