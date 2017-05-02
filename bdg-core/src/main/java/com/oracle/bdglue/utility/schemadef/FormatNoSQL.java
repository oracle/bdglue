/* ./src/main/java/com/oracle/bdglue/utility/schemadef/FormatNoSQL.java 
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
package com.oracle.bdglue.utility.schemadef;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Format the metadata for this table as "ddl" for the Oracle
 * NoSQL database's "table" API.
 *
 */
public class FormatNoSQL extends Format {
    private static final Logger LOG = LoggerFactory.getLogger(FormatNoSQL.class);

    
    public FormatNoSQL() {
        super();
        setFileSuffix("nosql");
        
        init();
    }
    
    /**
     * Reset things so we can reuse this instance.
     */
    public void init() {
        baos().reset();
    }
    
    /**
     * Get the type of this column and return it as a String.
     * 
     * @param jdbcType the java.sql.Types of this column
     * @return the type that we will output in the schema
     */
    private String getType(int jdbcType) {
        String rval;
        
        switch (jdbcType) {

        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            rval = "BOOLEAN";
            break;
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            rval = "INTEGER";
            break;
        case java.sql.Types.BIGINT:
            rval = "LONG";
            break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
            rval = "STRING";
            break;

        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
            // Note: Avro strings are Utf8
            rval = "STRING";
            break;
        case java.sql.Types.CLOB:
        case java.sql.Types.NCLOB:
            rval = "STRING";
            break;
        case java.sql.Types.BLOB:
        case java.sql.Types.BINARY:
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
            rval = "BINARY";
            break;
        case java.sql.Types.FLOAT:
        case java.sql.Types.REAL:
            rval = "FLOAT";
            break;
        case java.sql.Types.DOUBLE:
            rval = "DOUBLE";
            break;
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            rval = "FLOAT";
            break;
        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
            rval = "STRING";
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
            rval = "STRING";
            LOG.warn("getType(): unsupported data type: {}. Defaulting to String.", jdbcType);;
            // TODO: throw an exception here????
            break;
        }
        return rval;
    }

    /**
     * Format the top portion of the NoSQL Table API schema file.
     * 
     * @param schema the name of the schema
     * @param table the name of the table
     * @param noDoc specific to Avro. Unused here.
     * @throws IOException if an IO error occurs
     */
    @Override
    public void top(String schema, String table, boolean noDoc) throws IOException {
        setFileName(table);
        
        String comment = 
            String.format("## enter into table creation mode %n");
        String create = 
            String.format("table create -name %s %n", table);
        init();
        baos().write(comment.getBytes());
        baos().write(create.getBytes());

    }

    /**
     * Format the bottom portion of the schema file.
     * 
     * @param table the table name
     * @throws IOException if an IO error occurs
     */
    @Override
    public void bottom(String table) throws IOException {
        String comment1 = 
            String.format("## exit table creation mode %n");
        String exit =
            String.format("exit %n");
        String comment2 = 
            String.format("## add the table to the store and wait for completion %n");
        String plan = 
            String.format("plan add-table -name %s -wait %n", table);
        
        baos().write(comment1.getBytes());
        baos().write(exit.getBytes());
        baos().write(comment2.getBytes());
        baos().write(plan.getBytes());


    }

    /**
     * Format each of the fields that represent the columns in the
     * NoSQL schema file.
     * 
     * @param column the name of the column
     * @param writeDefault whether or not to include default values
     * @throws IOException an IO error occurs
     */
    @Override
    public void writeField(ColumnInfo column, boolean writeDefault) throws IOException {
        String addField = String.format("add-field -type %s -name %s %n", 
                          getType(column.getJdbcType()), column.getName());
        
        baos().write(addField.getBytes());
        
        if (column.isKeyCol()) {
            String pk = String.format("primary-key -field %s %n", 
                                      column.getName());
            baos().write(pk.getBytes());
        }
    }

}
