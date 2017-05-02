/* ./src/main/java/com/oracle/bdglue/utility/schemadef/SchemaDef.java 
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


import com.oracle.bdglue.common.PropertyManagement;

import java.io.IOException;
import java.io.Console;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple tool for generating schema meta data files for the target environment. 
 * It reads the metadata for the specified tables from the source database 
 * via JDBC and writes out the schema files as appropriate. It is based on the idea
 * that schema files have a beginning, middle, and end. "Beginning" opens things up; 
 * "middle" represents each of the fields; and "end" completes the schema and closes 
 * things out.
 * <p>
 * Currently supported targets include: 
 * <ul>
 * <li>Avro *.avsc files </li>
 * <li>Table creation scripts for Oracle NoSQL </li>
 * <li>Hive schema metadata for mapping HDFS files stored in Avro
 * format as external tables in Hive.</li>
 * </ul>
 *
 */
public class SchemaDef {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaDef.class);

    private PropertyManagement properties;
    private Format format;
    private BigDataName bdname;


    /**
     * @throws IOException if a JSON or other IO error occurs
     */
    public SchemaDef() throws IOException {
        super();
        properties = 
            PropertyManagement.getProperties(SchemaDefPropertyValues.defaultProperties, 
                                             SchemaDefPropertyValues.externalProperties);
        bdname = new BigDataName();
        setFormat();

    }

    /**
     *  Get a JDBC connection to the database.
     *  
     * @return the connection to the database
     * @throws ClassNotFoundException if the JDBC driver specified can't be found
     * @throws SQLException if a JDBC error occurs
     */
    public Connection getDBConnection() throws ClassNotFoundException, SQLException {
        String driver = properties.getProperty(SchemaDefPropertyValues.JDBC_DRIVER, 
                                               "**NOT_SET**");
        String url = properties.getProperty(SchemaDefPropertyValues.JDBC_URL, 
                                            "**NOT_SET**");
        String userName = properties.getProperty(SchemaDefPropertyValues.JDBC_USER, 
                                                   "**NOT_SET**");
        String password = properties.getProperty(SchemaDefPropertyValues.JDBC_PASSWORD, 
                                                 "**NOT_SET**");

        Connection connection;
        
        
        if (password.equalsIgnoreCase("prompt")) {
            Console cons;
            char[] passwd;
            if ((cons = System.console()) != null && 
                (passwd = cons.readPassword("%n**********%n[%s] ", 
                                            "Enter JDBC Password:")) != null) {
                password = new String(passwd);
            }
            else {
                LOG.error("Failed to read passord from the command line");
            }
        }

        // load the class for the JDBC driver
        Class.forName(driver);

        // get the database connection
        connection = DriverManager.getConnection(url, userName, password);

        return connection;
    }

    /**
     * Determine the proper formatter to invoke.
     * 
     * @throws IOException if a JSON formatting error occurs
     */
    public void setFormat() throws IOException {
        String formatStrg = properties.getProperty(SchemaDefPropertyValues.OUTPUT_FORMAT, 
                                               "**NOT_SET**");
        if (formatStrg.equalsIgnoreCase("avro")) {
            // format avro schema files
            format = new FormatAvro();
        }
        else if (formatStrg.equalsIgnoreCase("nosql")) {
            // format table API schema for Oracle NoSQL
            format = new FormatNoSQL();
        }
        else if (formatStrg.equalsIgnoreCase("hive_avro")) {
            // format Hive Avro SerDe schemas 
            format = new FormatHiveAvro();
        }
        else if (formatStrg.equalsIgnoreCase("cassandra")) {
            // format Hive Avro SerDe schemas 
            format = new FormatCassandra();
        }
        else {
            LOG.info("unrecognized format {}. Defaulting to avro.", formatStrg);
            format = new FormatAvro();
        }
        
        
    }

    /**
     * Determine how to encode fixed decimal types. "Double" is the default.
     * 
     * @return the property that tells us how to encode numeric/decimal data.
     */
    public String numericEncoding() {
        return properties.getProperty(SchemaDefPropertyValues.NUMERIC_ENCODING, 
                                      "double");
    }

    /**
     * This is a hack specific to GG "out of the box" processing. It allows schemadef to be 
     * used to generate avro schema files in the format that GG uses.
     * 
     * @throws IOException if a JSON format error occurs.
     */
    public void setGG122TxInfo() throws IOException {
 
        ColumnInfo col = new ColumnInfo();
        col.setJdbcType(java.sql.Types.VARCHAR, numericEncoding());
        col.setNullable("no");
        col.setDefaultValue("undefined");
        
        col.setName("table");
        format.writeField(col, false);
        
        col.setName("op_type");
        format.writeField(col, false);
        
        col.setName("op_ts");
        format.writeField(col, false);
        
        col.setName("current_ts");
        format.writeField(col, false);
        
        col.setName("pos");
        format.writeField(col, false);
        
        col.setJdbcType(java.sql.Types.ARRAY, null);
        col.setName("primary_keys");
        format.writeField(col, false);
        
        // for now, use "OTHER" for a map
        col.setJdbcType(java.sql.Types.OTHER, null);
        col.setName("tokens");
        format.writeField(col, false);
    }

    /**
     * Create columns for the transaction metadata information including
     * operation type and/or the transaction timestamp, etc. The column names are 
     * specified in the properties file.
     * 
     * @throws IOException if a JSON formatting error occurs
     */
    public void setBdglueTxInfo() throws IOException {
        String tName;
        ColumnInfo col = new ColumnInfo();
        col.setJdbcType(java.sql.Types.VARCHAR, numericEncoding());
        col.setNullable("no");
        col.setDefaultValue("undefined");
        if (properties.asBoolean(SchemaDefPropertyValues.SET_TABLENAME, "false")) {
           tName = properties.getProperty(SchemaDefPropertyValues.TABLENAME_COLUMN, 
                                               "tablename");
            col.setName(bdname.validName(tName));
            format.writeField(col, false);
        }
        if (properties.asBoolean(SchemaDefPropertyValues.SET_TXID, "false")) {
           tName = properties.getProperty(SchemaDefPropertyValues.TXID_COLUMN, 
                                               "txid");
            col.setName(bdname.validName(tName));
            format.writeField(col, false);
        }
        if (properties.asBoolean(SchemaDefPropertyValues.SET_OPTYPE, "true")) {
           tName = properties.getProperty(SchemaDefPropertyValues.OPTYPE_COLUMN, 
                                               "txoptpye");
            col.setName(bdname.validName(tName));
            format.writeField(col, false);
        }
        if (properties.asBoolean(SchemaDefPropertyValues.SET_TIMESTAMP, "true")) {
            tName = properties.getProperty(SchemaDefPropertyValues.TIMESTAMP_COLUMN, 
                                               "txtimestamp");
            col.setName(bdname.validName(tName));
            format.writeField(col, false);
        }
        if (properties.asBoolean(SchemaDefPropertyValues.SET_POSITION, "true")) {
            tName = properties.getProperty(SchemaDefPropertyValues.POSITION_COLUMN, 
                                               "txposition");
            col.setName(bdname.validName(tName));
            format.writeField(col, false);
        }
        if (properties.asBoolean(SchemaDefPropertyValues.SET_USERTOKEN, "true")) {
            tName = properties.getProperty(SchemaDefPropertyValues.USERTOKEN_COLUMN, 
                                               "usertokens");
            col.setName(bdname.validName(tName));
            format.writeField(col, false);
        }
    }
    


    /**
     * Main entry point for this utility.
     * 
     * @param args command line arguments. Not presently used.
     * @throws Exception if one occurs
     */
    public static void main(String[] args) throws Exception {
        SchemaDef schemaDef = new SchemaDef();
        ColumnInfo column;
        Connection connection = null;
        boolean defaults = false;
        ArrayList<String> keyCols;
        String catalog = null;
        String schema = null;
        String table = null;
        String columnPattern = null;
        String columnName;
        String tableList;
        String[] tables;
        String[] schemaTable;
        int columnCount;
        boolean useGG12 = false;

        
        for(String arg : args) {
            if (arg.equalsIgnoreCase("gg12")) {
                useGG12 = true;
            }
        }


        try {
            connection = schemaDef.getDBConnection();
            if (connection != null) {
                LOG.debug("Got JDBC connection: {}", connection.toString());
            }
            else {
                LOG.debug("JDBC connection is null!!!!!");
            }
            tableList = schemaDef.properties.getProperty(SchemaDefPropertyValues.JDBC_TABLES);
            tables = schemaDef.tables(tableList);
 
            
            defaults = schemaDef.properties.asBoolean(SchemaDefPropertyValues.SET_DEFAULTS, 
                                                      "true");

            DatabaseMetaData metadata = connection.getMetaData();
            for (String value : tables) {
                keyCols = new ArrayList<>();
                
                LOG.info("formatting table: {}", value);

                schemaTable = value.split("\\.", 2);
                schema = schemaTable[0];
                table = schemaTable[1];
                
                schemaDef.format.top(schemaDef.bdname.validName(schema), 
                                     schemaDef.bdname.validName(table), useGG12);

                /*
                 * get a list of columns that are part of the primary key
                 */
                ResultSet keys = metadata.getPrimaryKeys(catalog, schema, table);
                
                while (keys.next()) {
                    keyCols.add(keys.getString("COLUMN_NAME"));
                }
                if (keyCols.size() > 0) {
                    LOG.debug("*** {} key columns found", keyCols.size());
                }
                else {
                    LOG.warn("No key columns found for {}.{}", schema, table);
                }

                ResultSet columns = metadata.getColumns(catalog, schema, table, columnPattern);
                if (useGG12 == false) 
                    schemaDef.setBdglueTxInfo();
                else schemaDef.setGG122TxInfo();
                
                columnCount = 0;
                while (columns.next()) {
                    LOG.trace("top of loop");
                    column = new ColumnInfo();
                    columnName = columns.getString("COLUMN_NAME");
                    LOG.debug("*** Column found: {}", columnName);
                    LOG.trace("setName()");
                    column.setName(schemaDef.bdname.validName(columnName));
                    LOG.trace("setJdbcType()");
                    column.setJdbcType(columns.getInt("DATA_TYPE"), 
                                       schemaDef.numericEncoding());
                    LOG.trace("setNullable()");
                    column.setNullable(columns.getString("IS_NULLABLE"));
                    
                    // had problems with this call sometimes, and wound
                    // up not using it anyway.
                    //LOG.trace("setDefaultValue()");
                    //column.setDefaultValue(columns.getString("COLUMN_DEF"));

                    /*
                     * check to see if this column is one of the PK columns
                     */
                    column.setKeyCol(false);
                    for (String str : keyCols) {
                        if (str.matches(columnName)) {
                            LOG.debug("*** Column {} is a key column", columnName);
                            column.setKeyCol(true);
                            break;
                        }
                    }
                    LOG.trace("schemaDef.format.writeField");
                    schemaDef.format.writeField(column, defaults);
                    columnCount++;
                    LOG.trace("bottom of loop");
                }
                if (columnCount > 0) {
                    LOG.debug("*** {} columns found", columnCount);
                }
                else {
                    LOG.error("No columns found for {}.{}", schema, table);
                }
                schemaDef.format.bottom(table);
                
                schemaDef.format.writFile();
            }    
        } catch (Exception e) {
            LOG.error("Got Exception: {}", e.getMessage());
            LOG.error("Exception backtrace:", e);
        } finally {
            LOG.debug("finally: Closing JDBC connection");
            if (connection != null)
                connection.close();
        }
    }
    
    /**
     * Generate an array of table names from a String.
     * <p>
     * The input string contains a whitespace delimited list
     * of table names.
     * @param list the list of tables from the properties to process
     * @return the list of schema.table entries
     */
    public String[] tables(String list) {
    String tables[] = null;
    tables = list.split("\\s+");
        return tables;
    }

}
