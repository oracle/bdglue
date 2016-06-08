/* ./src/main/java/com/oracle/bdglue/utility/schemadef/FormatCassandra.java 
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
package com.oracle.bdglue.utility.schemadef;

import java.io.IOException;

/**
 * Format the Cassandra schema meta data.
 */
public class FormatCassandra extends Format {
    
    private String keyString = "";
    private String comma = "";
    
    public FormatCassandra() {
        super();
        
        setFileSuffix("cql");
        
        init();
    }
    
    /**
     * Reset so we can use this again.
     */
    public void init() {
        baos().reset();
        keyString = "";
        comma = "";
    }

    /**
    * Format the top portion of the Cassandra schema file.
    *
    * @param schema the name of the schema
    * @param table the name of the table
    * @param noDoc unused. Specific to avro.
    * @throws IOException if an IO error occurs
    */
    @Override
    public void top(String schema, String table, boolean noDoc) throws IOException {
        init(); // reset the baos
        
        String schemaTable = String.format("%s.%s", schema, table);
        setFileName(schemaTable);
        
        String replicationStrategy = getProperties().getProperty(SchemaDefPropertyValues.REPLICATION_STRATEGY,
                                                            SchemaDefPropertyValues.REPLICATION_STRATEGY_DEFAULT);
        
        String createKeyspace = String.format("CREATE KEYSPACE IF NOT EXISTS \"%s\" %n    WITH REPLICATION = %s;%n%n",
                                              schema, replicationStrategy);
        
        String dropTable = String.format("DROP TABLE IF EXISTS %s;%n%n", schemaTable);
        String createTable = String.format("CREATE TABLE %s %n ( ", schemaTable);
        
        baos().write(createKeyspace.getBytes());
        baos().write(dropTable.getBytes());
        baos().write(createTable.getBytes());    
    }

    @Override
    public void bottom(String table) throws IOException {
        String closeString = String.format(",%n   PRIMARY KEY (%s) %n );", keyString);
        
        baos().write(closeString.getBytes());
    }

    @Override
    public void writeField(ColumnInfo column, boolean writeDefault) throws IOException {
        String outputString;
        
        if (column.isKeyCol()) {
            if (keyString != "")
                keyString += ", ";
            keyString += column.getName();
        }
        
        outputString = String.format("%s%n   %s %s", 
                                     comma, column.getName(), getColumnType(column.getJdbcType())); 
        
        baos().write(outputString.getBytes());
        
        comma = ",";  // set after first time through

    }
    
    /**
     * Convert jdbcType to a valid Cassandra type.
     * 
     * @param jdbcType the java.sql.Types jdbc type
     * @return the Cassandra type as a String.
     */
    private String getColumnType(int jdbcType) {
        String rval = null;
        
        switch (jdbcType) {
        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            rval = "boolean";
            break;
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            // rval = "varint"; there was some sort of codec issue with this conversion in GG
            rval = "int";
            break;
        case java.sql.Types.BIGINT:
            rval = "bigint";
            break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CLOB:
            rval = "text";
            break;
        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
        case java.sql.Types.NCLOB:
            rval = "text";
            break;
        case java.sql.Types.BLOB:
        case java.sql.Types.BINARY:
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
            rval = "blob";
            break;
        case java.sql.Types.REAL: // JDBC says 7 digits of mantisa
            rval = "float";
            break;
        case java.sql.Types.FLOAT: // JDBC says 15 digits of mantisa
        case java.sql.Types.DOUBLE:
            rval = "double";
            break;
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            rval = "decimal";
            break;
        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
            rval = "text";
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
            rval = "unsupported";
            break;
        }
        return rval;
    }
}
