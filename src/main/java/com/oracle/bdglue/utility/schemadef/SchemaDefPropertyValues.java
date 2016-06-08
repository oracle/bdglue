/* ./src/main/java/com/oracle/bdglue/utility/schemadef/SchemaDefPropertyValues.java 
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


/**
 * The properties for SchemaDef.
 *
 */
public class SchemaDefPropertyValues {
    public static final String defaultProperties = "/schemadefDefault.properties";
    public static final String externalProperties = "schemadef.properties";

    /**
     * Specify whether to generate default values or not.
     */
    public static final String SET_DEFAULTS = "schemadef.set-defaults";

    /**
     * specify whether to include a column for the table name
     */
    public static final String SET_TABLENAME = "schemadef.tablename";
    /**
     * The name for the generated tablename column.
     */
    public static final String TABLENAME_COLUMN = "schemadef.tablename-col";
    /**
     * specify whether to include a column for a transaction identifier (SCN)
     */
    public static final String SET_TXID = "schemadef.txid";
    /**
     * The name for the generated transaction ID column.
     */
    public static final String TXID_COLUMN = "schemadef.txid-col";
    /**
     * specify whether to include a column for the operation
     * type (insert, update, delete).
     */
    public static final String SET_OPTYPE = "schemadef.tx-optype";

    /**
     * The name for the generated optype column.
     */
    public static final String OPTYPE_COLUMN = "schemadef.tx-optype-name";

    /**
     * Specify whether to include a column for the operation timestamp.
     */
    public static final String SET_TIMESTAMP = "schemadef.tx-timestamp";

    /**
     * The name for the generated timestamp column.
     */
    public static final String TIMESTAMP_COLUMN = "schemadef.tx-timestamp-name";
    
    /**
     * Specify whether to include a column for the relative position of the op.
     */
    public static final String SET_POSITION = "schemadef.tx-position";

    /**
     * The name for the generated position column.
     */
    public static final String POSITION_COLUMN = "schemadef.tx-position-name";


    /**
     * Specify whether to include a column for the operation user tokens.
     */
    public static final String SET_USERTOKEN = "schemadef.user-token";

    /**
     * The name for the generated user token column.
     */
    public static final String USERTOKEN_COLUMN = "schemadef.user-token-name";
    /**
     * Set the type we should encode numeric/decimal types to. We
     * need to have this flexibility because Avro does not support
     * these types directly, so users will have to decide how they
     * want this data represented on the other end: string, double,
     * float, etc.
     */
    public static final String NUMERIC_ENCODING = "schemadef.numeric-encoding";

    /**
     *  The path of the output directory.
     */
    public static final String OUTPUT_PATH = "schemadef.output.path";
    /**
     * The URL (path) where the target database (i.e. Hive) will be able to 
     * locate the avro schema files.
     */
    public static final String AVRO_URL = "schemadef.avro-url";
    /**
     * The URL (path) where the target database (i.e. Hive) will be able to 
     * locate the avro data files.
     */
    public static final String LOCATION = "schemadef.data-location";

    /**
     * The output fomat for the generated files.
     */
    public static final String OUTPUT_FORMAT = "schemadef.output.format";
    
    /**
     * The replication strategy for Cassandra. Format is a key:value array.
     */
    public static final String REPLICATION_STRATEGY = "schemadef.cassandra.replication-strategy";
    public static final String REPLICATION_STRATEGY_DEFAULT = 
        "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";

    /**
     * The User Name for the JDBC connection to the database.
     */
    public static final String JDBC_USER = "schemadef.jdbc.username";

    /**
     * The User Name for the JDBC connection to the database.
     */
    public static final String JDBC_PASSWORD = "schemadef.jdbc.password";

    /**
     * The JDBC driver.
     */
    public static final String JDBC_DRIVER = "schemadef.jdbc.driver";

    /**
     * The URL for the JDBC connection to the database.
     */
    public static final String JDBC_URL = "schemadef.jdbc.url";

    /**
     * The list of tables to generate meta data for. Table names are
     * delimited by whitespace.
     */
    public static final String JDBC_TABLES = "schemadef.jdbc.tables";
    
    /**
     * Replace special characters that are valid in RDBMS table/column
     * names, but are not valid in hive/hbase/nosql/etc. names.
     */
    public static final String REPLACE_CHAR = "schemadef.replace.invalid_char";
    /**
     * The default character to use when replacing invalid characters that are
     * not at the beginning of a field name.
     */
    public static final String REPLACE_CHAR_DEFAULT = "_";
    /**
     * Replace special characters at the beginning of a field name
     * that are valid in RDBMS table/column
     * names, but are not valid in hive/hbase/nosql/etc. names.
     */
    public static final String REPLACE_FIRST = "schemadef.replace.invalid_first_char";
    /**
     * The default character to use when replacing an invalid first character of an
     * field name.
     */
    public static final String REPLACE_FIRST_DEFAULT = "x";
    /**
     * Specifies the regular expression to use to identify invalid characters
     * in a field name.
     */
    public static final String REPLACE_REGEX = "schemadef.replace.regex";
    /**
     * The default regular expression. Note that the "^" in the expression is
     * required. It represents characters "that are NOT in this list".
     */
    public static final String REPLACE_REGEX_DEFAULT = "[^a-zA-Z0-9_\\.]";
}
