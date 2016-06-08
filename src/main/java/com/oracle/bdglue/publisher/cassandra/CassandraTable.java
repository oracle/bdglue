/* ./src/main/java/com/oracle/bdglue/publisher/kafka/CassndraTable.java 
 *
 * Copyright 2016 Oracle and/or its affiliates.
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
package com.oracle.bdglue.publisher.cassandra;


import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import com.datastax.driver.core.Session;

import com.oracle.bdglue.meta.schema.DownstreamColumnMetaData;
import com.oracle.bdglue.meta.schema.DownstreamTableMetaData;
import com.oracle.bdglue.meta.transaction.DownstreamColumnData;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.util.Map;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CassandraTable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraTable.class);
    // index into preparedStatements
    private static int INSERT = 0;
    private static int UPDATE = 1;
    private static int DELETE = 2;
    
    private String longName;
    private Session session;
    
    private PreparedStatement preparedStatements [] = {null, null, null};

    /**
     * Construct an instance that represents this table. It creates
     * the long table name (schema.table) from the arguments.
     * 
     * @param session our current Cassandra Session
     * @param schemaName the schema name
     * @param tableName the table name
     */
    public CassandraTable(Session session, String schemaName, String tableName) {
        super();
        
        this.session = session;
        this.longName = schemaName + "." + tableName;;
    }

    /**
     * Construct an instance that represents this table.
     * 
     * @param session our current Cassandra Session
     * @param longName the "long" table name (schema.table)
     */
    public CassandraTable(Session session, String longName) {
        super();
        
        this.session = session;
        this.longName = longName;
    }
    
    

    /**
     * Create a bound statement for this operation and return it.
     * 
     * @param op the DownstreamOperation we are processing.
     * @param insertOnly true implies all operations should be treated as inserts.
     * @return a BoundStatement representing this operation
     */
    public BoundStatement getBoundStatement(DownstreamOperation op, boolean insertOnly) {
        BoundStatement rval = null;

        if (insertOnly) {
            rval = insert(op);
        } else {
            switch (op.getOpTypeId()) {
            case DownstreamOperation.INSERT_ID:
                rval = insert(op);
                break;
            case DownstreamOperation.UPDATE_ID:
                rval = update(op);
                break;
            case DownstreamOperation.DELETE_ID:
                rval = delete(op);
                break;
            default:
                LOG.error("unrecognized operation type: {}", op.getOpType());
            }
        }

        return rval;
    }
    
    /**
     * Create an insert operation.
     * Format per: http://www.treselle.com/blog/cassandra-datastax-java-driver/
     * 
     * insert into mytable (dept_name, emp_id, emp_name ) VALUES (?,?,?);
     * 
     * @param op the DownstreamOperation
     * @return a BoundStatement
     */
    private BoundStatement insert(DownstreamOperation op) {
        String stmt;
        String cols;
        String values;
        BoundStatement boundStatement;
        
        if (preparedStatements[INSERT] == null) {
            cols = insertColumns(op);
            values = insertPrepareValues(op);
            stmt = String.format("INSERT INTO %s (%s) VALUES (%s);", longName, cols, values);
            LOG.debug("Preparing statement: {}", stmt);
            preparedStatements[INSERT] = session.prepare(stmt);

        }
        boundStatement = new BoundStatement(preparedStatements[INSERT]);
        
        return bindColumnValues(boundStatement, op);
    }
    
    /**
     * Create an update operation.
     * Format per http://www.treselle.com/blog/cassandra-datastax-java-driver/
     * 
     * update mytable SET emp_name =? where dept_name=? and emp_id=?;
     * 
     * @param op the DownstreamOperation
     * @return a BoundStatement
     */
    private BoundStatement update(DownstreamOperation op) {
        String stmt;
        String cols;
        String keys;
        BoundStatement boundStatement;
        
        if (preparedStatements[UPDATE] == null) {
            cols = updateColumns(op);
            keys = getKey(op);
            stmt = String.format("UPDATE %s SET %s WHERE %s;", longName, cols, keys);
            LOG.debug("Preparing statement: {}", stmt);
            preparedStatements[UPDATE] = session.prepare(stmt);

        }
        boundStatement = new BoundStatement(preparedStatements[UPDATE]);
        
        return bindColumnValues(boundStatement, op);
    }
    
    /**
     * Create a delete operation. 
     * Format per: http://www.treselle.com/blog/cassandra-datastax-java-driver/
     * 
     * delete from mytable where dept_name=? and emp_id=?;
     * 
     * @param op the DownstreamOperation
     * @return a BoundStatement
     */
    private BoundStatement delete(DownstreamOperation op) {
        String stmt;

        String key;
        BoundStatement boundStatement;
        
        if (preparedStatements[DELETE] == null) {

            key = getKey(op);
            stmt = String.format("DELETE FROM %s WHERE %s;", longName, key);
            LOG.debug("Preparing statement: {}", stmt);
            preparedStatements[DELETE] = session.prepare(stmt);
        }
        boundStatement = new BoundStatement(preparedStatements[DELETE]);
        
        return bindKeyValues(boundStatement, op);
    }
    
    /**
     * Create the string needed to generate a prepared statement
     * for the columns of an insert operation.
     * 
     * @param op the DownstreamOperation we are processing
     * @return the string representing the column info
     */
    private String insertColumns(DownstreamOperation op) {
        String comma = "";
        String rval = "";
        DownstreamTableMetaData table = op.getTableMeta();
        for (DownstreamColumnMetaData col : table.getColumns()) {
            rval = String.format("%s%s%s", rval, comma, col.getBDColumnName());
            comma = ", ";
        }
        
        // now take care of the metadata
        for(String col : op.getMetadataCols()) {
            rval = String.format("%s, %s", rval, col);
        }
        
        return rval;
    }
    
    /**
     * Create the bind variables for the insert operation.
     * 
     * @param op The DownstreamOperation we are processing.
     * @return the string to be used in the prepared statement.
     */
    private String insertPrepareValues(DownstreamOperation op) {
        String comma = "";
        String rval = "";
        DownstreamTableMetaData table = op.getTableMeta();
        for (@SuppressWarnings("unused")
             DownstreamColumnMetaData col : table.getColumns()) {
            rval = String.format("%s%s?", rval, comma);
            comma = ", ";
        }
        
        // now take care of the metadata
        for (@SuppressWarnings("unused")
             String col : op.getMetadataCols()) {
            rval = String.format("%s, ?", rval);
        }
        
        return rval;
    }
    
    /**
     * Create the prepared statement for the update operation. It currently
     * does not support PK updates.
     * 
     * @param op the DownstreamOperation we are processing.
     * @return a string to be used in the prepared statement
     */
    private String updateColumns(DownstreamOperation op) {
        String comma = "";
        String rval = "";
        DownstreamTableMetaData table = op.getTableMeta();
        for (DownstreamColumnMetaData col : table.getColumns()) {
            /*
             * Not supporting PK updates right now, so no need to "set".
             */
            if (col.isKeyCol()) 
                continue;
            
            rval = String.format("%s%s%s=?", rval, comma, col.getBDColumnName());
            comma = ", ";
        }
        
        // now take care of the metadata
        for(String col : op.getMetadataCols()) {
            rval = String.format("%s, %s=?", rval, col);
        }
        return rval;
    }
    
    /**
     * Generate the key-related meta information for the prepared statement. This is 
     * called only if there isn't a prepared statement for this operation type.
     * 
     * @param op the current DownstreamOperation we are processing.
     * 
     * @return a string with the key information
     */
    private String getKey(DownstreamOperation op) {
        String rval = "";
        String and = "";
        DownstreamTableMetaData table = op.getTableMeta();
        for (DownstreamColumnMetaData col : table.getColumns()) {
            /*
             * Not supporting PK updates right now, so no need to "set".
             */
            if (!col.isKeyCol()) 
                continue;
            
            rval = String.format("%s%s%s=?", rval, and, col.getBDColumnName());
            and = "AND ";
        }
        return rval;
    }
    
    /**
     * Process and bind all column values in the operation.
     *
     * @param stmt the BoundStatement we are working with
     * @param op the current DownstreamOperation we are processing
     * @return a BoundStatement
     */
    private BoundStatement bindColumnValues(BoundStatement stmt, DownstreamOperation op) {
        int jdbcType = 0;

        DownstreamTableMetaData table = op.getTableMeta();
        DownstreamColumnMetaData metaCol;

        for (DownstreamColumnData col : op.getColumns()) {
            metaCol = table.getColumn(col.getOrigName());
            jdbcType = metaCol.getJdbcType();
            stmt = bindValue(stmt, jdbcType, col);
        }

        // take care of the operation meta data
        for (Map.Entry<String, String> col : op.getOpMetadata().entrySet()) {
            stmt =
                bindValue(stmt, java.sql.Types.CHAR,
                          new DownstreamColumnData(col.getKey(), col.getValue(), null, false));
        }

        return stmt;
    }
    
    /**
     * Process and bind only the columns marked as a key column.
     * 
     * @param stmt the BoundStatement we are working with
     * @param op the current DownstreamOperation we are processing
     * @return a BoundStatement
     */
    private BoundStatement bindKeyValues(BoundStatement stmt, DownstreamOperation op) {;
        int jdbcType = 0;
    
        DownstreamTableMetaData table = op.getTableMeta();
        DownstreamColumnMetaData metaCol;
        
        for (DownstreamColumnData col : op.getColumns()) {
            /*
             * Ignore if this isn't a key column.
             */
            if (!col.isKeyCol())
                continue;
            
            metaCol = table.getColumn(col.getOrigName());
            jdbcType = metaCol.getJdbcType();
            stmt = bindValue(stmt, jdbcType, col);
        }
        return stmt;
    }

    /**
     * Bind this column to the current BoundStatement.
     * @param stmt the BoundStatement we are working on
     * @param jdbcType the java.sql.types column type
     * @param col the column data
     * @return the current BoundStatement
     */
    private BoundStatement bindValue(BoundStatement stmt, int jdbcType, DownstreamColumnData col) {

        switch (jdbcType) {
        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            stmt.setBool(col.getBDName(), col.asBoolean());
            break;
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            stmt.setInt(col.getBDName(), col.asInteger());
            break;
        case java.sql.Types.BIGINT:
            stmt.setLong(col.getBDName(), col.asLong());
            break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CLOB:
            stmt.setString(col.getBDName(), col.asString());
            break;
        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
        case java.sql.Types.NCLOB:
            stmt.setString(col.getBDName(), col.asString());
            break;
        case java.sql.Types.BLOB:
        case java.sql.Types.BINARY:
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
            stmt.setBytesUnsafe(col.getBDName(), col.asByteBuffer());
            break;
        case java.sql.Types.REAL: // JDBC says 7 digits of mantisa
            stmt.setFloat(col.getBDName(), col.asFloat());
            break;
        case java.sql.Types.FLOAT: // JDBC says 15 digits of mantisa
        case java.sql.Types.DOUBLE:
            stmt.setDouble(col.getBDName(), col.asDouble());
            break;
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            stmt.setDecimal(col.getBDName(), col.asBigDecimal());
            break;
        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
            stmt.setString(col.getBDName(), col.asString());
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
            stmt.setString(col.getBDName(), "unsupported");
            break;
        }
        return stmt;
    }
}
