/* ./src/main/java/com/oracle/bdglue/utility/schemadef/ColumnInfo.java 
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains meta data about the column we are currently working with.
 *
 */
public class ColumnInfo {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnInfo.class);

    String columnName;
    String columnType;
    String defaultValue;
    boolean keyCol;
    boolean nullable;
    int jdbcType;

    public ColumnInfo() {
        super();
    }

    /**
     * Set the column name.
     * 
     * @param name the column name
     */
    public void setName(String name) {
        columnName = name;
    }

    /**
     * Get the column name.
     * 
     * @return the column name
     */
    public String getName() {
        return columnName;
    }

    /**
     * Get the type of column (string, integer, float, etc.).
     * 
     * @return the string value for the column type
     */
    public String getType() {
        return columnType;
    }

    /**
     * Set the default value for the column.
     * @param def  the default value as a string
     */
    public void setDefaultValue(String def) {
        defaultValue = def;
    }

    /**
     * Get the default value for this column.
     * 
     * @return the default value for this column as a String
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Identify this column as a key column. A value of true
     * implies that this is a key column.
     * 
     * @param value boolean indicates whether this is a key column or not.
     */
    public void setKeyCol(boolean value) {
        keyCol = value;
    }

    /**
     * Indicate whether or not this is a key column.
     * @return true if this is a key column, false otherwise.
     */
    public boolean isKeyCol() {
        return keyCol;
    }

    /**
     * Indicate whether this column can be null. It is based on value returned
     * from JDBC on this matter, and is converted to a boolean value here.
     * See the documentation for java.sql.DatabaseMetadata for more info.
     * 
     * @param value indicates whether or not this is a nullable column.

     */
    public void setNullable(String value) {
        if (value.equalsIgnoreCase("yes")) {
            nullable = true;
        } else {
            nullable = false;
        }
    }

    /**
     * Indicate if this column can be null. 
     * @return true if it can be null, false otherwise.
     */
    public boolean isNullable() {
        return nullable;
    }


    /**
     * Get the jdbc type for this column as an integer. See
     * java.sql.Types for more information.
     * 
     * @return the java.sql.Types as an integer
     */
    public int getJdbcType() {
        return jdbcType;
    }


    /**
     * Set the type for this column based on the incoming parameters. If a target 
     * platform doesn't understand a "fixed decimal" type, then the numericEncoding
     * parameter allows us to override the JDBC type received from the database 
     * and cause the fields to be formatted as specified (i.e. as a Float or String).
     * 
     * @param type the java.sql.Types jdbc type for this column.
     * @param numericEncoding encode numeric (fixed decimal) types as Float or String
     */
    public void setJdbcType(int type, String numericEncoding) {
        jdbcType = type;

        switch (jdbcType) {

        case java.sql.Types.BOOLEAN:
        case java.sql.Types.BIT:
            columnType = "boolean";
            break;
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            columnType = "int";
            break;
        case java.sql.Types.BIGINT:
            columnType = "long";
            break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CLOB:
            columnType = "string";
            break;
        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
        case java.sql.Types.NCLOB:
            // Note: Avro strings are Utf8
            columnType = "string";
            break;
        case java.sql.Types.BLOB:
        case java.sql.Types.BINARY:
        case java.sql.Types.LONGVARBINARY:
        case java.sql.Types.VARBINARY:
            columnType = "bytes";
            break;
        case java.sql.Types.REAL:
            columnType = "float";
            break;
        case java.sql.Types.FLOAT:
        case java.sql.Types.DOUBLE:
            columnType = "double";
            break;
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            columnType = numericEncoding;
            break;
        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
            columnType = "string";
            break;
        case java.sql.Types.ARRAY:
            columnType = "array";
            break;
        case java.sql.Types.OTHER:
            columnType = "map";
            break;
        case java.sql.Types.DATALINK:
        case java.sql.Types.DISTINCT:
        case java.sql.Types.JAVA_OBJECT:
        case java.sql.Types.NULL:
        case java.sql.Types.ROWID:
        case java.sql.Types.SQLXML:
        case java.sql.Types.STRUCT:
        case java.sql.Types.REF:
        default:
            columnType = "string";
            LOG.warn("setJdbcType(): unsupported data type: {}. Defaulting to string.", 
                     jdbcType);
            // TODO: throw an exception here ?????
            break;
        }

    }
}
