/* ./src/main/java/com/oracle/bdglue/encoder/avro/AvroSchema.java
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
package com.oracle.bdglue.encoder.avro;


import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.MetadataHelper;
import com.oracle.bdglue.encoder.avro.column.AvroColumn;
import com.oracle.bdglue.encoder.avro.column.AvroColumnType;
import com.oracle.bdglue.meta.schema.DownstreamColumnMetaData;
import com.oracle.bdglue.meta.schema.DownstreamTableMetaData;
import com.oracle.bdglue.utility.schemadef.BigDataName;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class keeps up with the avro-related meta data that
 * we care about for processing.
 *
 */
public class AvroSchema {
    public static final int SCHEMA_ID_UNSET = -9876;

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchema.class);
    private static BigDataName bdname = null;
    private static String namespace =
        PropertyManagement.getProperties().getProperty(BDGluePropertyValues.AVRO_NAMESPACE, "default");
    private static String path =
        PropertyManagement.getProperties().getProperty(BDGluePropertyValues.AVRO_LOCAL_PATH, "./avro");

    private Schema schema; // the Avro schema
    private String tableName;
    private String bdTableName;
    private int schemaId;
    private HashMap<String, AvroColumn> columns;


    /**
     * Construct the internal representation of the Avro schema.
     * It will generate the schema on the fly if asked, but will most
     * typically get the Avro schema representation from an externa
     * Avro schema file (tablename.avsc).
     *
     * @param name the name of this table
     * @param meta the table meta data
     * @param generateAvroSchema
     *        true: generate Avro Schema and write to file.
     *        false: read Avro Schema from file.
     */
    public AvroSchema(String name, DownstreamTableMetaData meta, boolean generateAvroSchema) {
        super();


        ArrayList<DownstreamColumnMetaData> cols;
        Schema.Field field;
        Schema colSchema;
        Schema.Type colType;
        String columnName;
        AvroColumn columnInfo;
        AvroColumnType columnType;

        if (bdname == null) {
            bdname = new BigDataName();
        }

        schemaId = SCHEMA_ID_UNSET;


        columns = new HashMap<>();
        tableName = name;
        bdTableName = bdname.validName(tableName);
        cols = meta.getColumns();

        if (generateAvroSchema) {
            // generate the *.avsc file. We'll read it back in the next step.
            generateAvroSchema(cols);
        }

        // read the schema for the table from a JSON file
        schema = readSchemaFile(getSchemaFilePath());
        if (schema == null) {
            LOG.error("Avro Schema for {} is NULL!!!", tableName);
        }

        for (DownstreamColumnMetaData myCol : cols) {
            columnName = myCol.getBDColumnName();
            columnInfo = AvroColumn.newAvroColumn(myCol);
            columnType = columnInfo.getColumnType();

            if (columnType != AvroColumnType.UNSUPPORTED) {
                // ignore the column if it is in an unsupported format

                columns.put(columnName, columnInfo);

                /*
                 * need to set the meta data for the Avro schema field
                 * based on what we read from the Avro *.avsc file
                 * instead of assuming the field type
                 */

                field = schema.getField(columnName);
                if (field == null) {
                    LOG.error("Field {} not found in avro schema {}", columnName, tableName);
                }
                colSchema = field.schema();
                colType = Schema.Type.NULL;
                if (colSchema.getType() == Schema.Type.UNION) {
                    // this implies a nullable column in our case,so
                    // we need to find the type of the non-null value.
                    List<Schema> list = colSchema.getTypes();
                    for (Schema s : list) {
                        colType = s.getType();
                        if (colType != Schema.Type.NULL) {
                            // we have worked past the null value
                            break;
                        }
                    }

                } else {
                    colType = colSchema.getType();
                }
                columnInfo.setAvroColumnType(colType);

            }
        }

    }

    /**
     * Generate the avro schema from the metadata and
     * write it to a file.
     */
    private void generateAvroSchema(ArrayList<DownstreamColumnMetaData> cols) {
        MetadataHelper metadataHelper = MetadataHelper.getMetadataHelper();
        ArrayList<Schema.Field> schemaFields;
        String columnName;
        AvroColumn columnInfo;
        Schema.Field field;
        Schema colSchema;
        Schema recordSchema;
        String fieldDoc;
        //JsonNode defValue;
        Object defValue;


        schemaFields = new ArrayList<>();

        // Set the metadata fields
        fieldDoc = "metadata"; 
        for(String tcol : metadataHelper.getMetadataCols()) {
            columnName = tcol;
            colSchema = Schema.create(Schema.Type.STRING);
            // deprecated - field = new Schema.Field(columnName, colSchema, fieldDoc, null);
            field = new Schema.Field(columnName, colSchema, fieldDoc, (Object)null);
            schemaFields.add(field);
        }
        
        // set the column fields
        for (DownstreamColumnMetaData myCol : cols) {
            columnName = myCol.getBDColumnName();
            columnInfo = AvroColumn.newAvroColumn(myCol);

            // generate the Avro schema element from the
            // table metadata for each column
            fieldDoc = generateFieldDoc(columnInfo.isKeyCol(), columnInfo.getColumnTypeString());

            colSchema = columnInfo.getFieldSchema();
            
            if (colSchema.getType() == Schema.Type.UNION)
                defValue = JsonProperties.NULL_VALUE;
            else defValue = null;
                
            field = new Schema.Field(columnName, colSchema, fieldDoc, defValue);



            schemaFields.add(field);
        }

        // create the Schema file for the table on the fly
        recordSchema = Schema.createRecord(bdTableName, "bdglue", namespace, false);
        recordSchema.setFields(schemaFields);
        writeSchemaFile(recordSchema, getSchemaFilePath(), false);
    }


    /**
     * Generate the String used to populate the "doc" property for a column.
     *
     * @param keyCol - indicates this is a key column
     * @param columnTypeString - a String representing the type
     *        (INT, LONG, STRING, etc.)
     * @return a (potentially null) string for inclusion in the doc
     *         property of a field.
     */
    public String generateFieldDoc(boolean keyCol, String columnTypeString) {
        String doc = null;

        if (keyCol) {
            doc = "keycol";
            if (!columnTypeString.equals(""))
                doc = doc + " " + columnTypeString;
        } else {
            if (!columnTypeString.equals(""))
                doc = columnTypeString;
            else
                doc = null;
        }
        return doc;
    }

    /**
     * Write the generated scheam to a file in JSON format.
     *
     * @param schemaIn the schema to format and write
     * @param path the relative path + actual file name
     * @param wrapAsEvent wrap schema as Avro Event prior to output
     * @return success or failure
     *
     */
    public boolean writeSchemaFile(Schema schemaIn, String path, boolean wrapAsEvent) {
        Boolean rval = false;
        Schema schemaOut;
        File file;
        BufferedWriter output;

        // trim off the file name at the end of the path
        if (validateDirectory(path.substring(0, path.lastIndexOf('/')))) {
            try {
                if (wrapAsEvent)
                    schemaOut = wrapAsEventRecord(schemaIn);
                else
                    schemaOut = schemaIn;

                file = new File(path);
                output = new BufferedWriter(new FileWriter(file));
                output.write(schemaOut.toString(true));
                output.close();
                rval = true;
            } catch (IOException e) {
                rval = false;
                LOG.error("writeSchemaFile(): IOException on path: {}", path);
                e.printStackTrace();
            }
        } else {
            rval = false;
        }

        return rval;
    }

    /**
     * Wrap schema for a table as an Avro Event prior to output of JSON
     * data.
     * @param schemaIn schema to wrap as an Avro event
     * @return wrapped schema
     */
    public Schema wrapAsEventRecord(Schema schemaIn) {
        Schema schemaOut = null;
        ArrayList<Schema.Field> schemaFields;
        Schema.Field field;
        Schema stringSchema = Schema.create(Schema.Type.STRING);

        schemaFields = new ArrayList<>();
        // deprecated - field = new Schema.Field("headers", Schema.createMap(stringSchema), "", null);
        field = new Schema.Field("headers", Schema.createMap(stringSchema), "", (Object)null);
        schemaFields.add(field);

        // deprecated - field = new Schema.Field("body", schemaIn, "body doc", null);
        field = new Schema.Field("body", schemaIn, "body doc", (Object)null);
        schemaFields.add(field);

        schemaOut = Schema.createRecord("Event", "", "", false);
        schemaOut.setFields(schemaFields);

        return schemaOut;
    }

    /**
     * Make sure that the directory exists, and if not, create it.
     *
     * @param path the directory path we want to access
     * @return true if the directory exists or we were able to create it;
     * false otherwise.
     */
    public boolean validateDirectory(String path) {
        boolean success = false;

        // make sure that the directory path exists
        File pathFile = new File(path);
        if (pathFile.exists()) {
            success = true;
        } else {
            if (pathFile.mkdirs()) {
                success = true;
            } else {
                LOG.error("validateDirectory: " + "directory creation failed: {}", path);
                success = false;
            }
        }
        return success;
    }

    /**
     * Get the name of this table.
     *
     * @return returns the name of this table. Note that this may be
     * a variant of the actual table name in the relational schema.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Read the JSON formated schema from an avro schema file.
     *
     * @param fileName the name of the file we want to read the schema from
     * @return the instance of Schema generated from the file.
     * null if not found, or the parse failed.
     */
    public Schema readSchemaFile(String fileName) {
        Schema newSchema = null;
        Schema.Parser parser;

        LOG.info("readSchemaFile(): Parsing schema file: {}", fileName);


        File file = new File(fileName);
        if (file.exists()) {
            parser = new Schema.Parser();

            try {
                newSchema = parser.parse(file);
            } catch (IOException e) {
                LOG.error("readSchema(): " + "IO Excpetion on: {}", fileName);
                e.printStackTrace();
                newSchema = null;
            }
        } else {
            LOG.error("readSchemaFile(): file does not exist: {}", fileName);
            newSchema = null;
        }

        return newSchema;
    }

    /**
     * Generate path + fileName for reading or writing an avro schema.
     *
     * @return the path to the file
     */
    public String getSchemaFilePath() {
        String fullPath;


        fullPath = path + "/" + getSchemaFileName();

        LOG.debug("getSchemaFilePath(): {}", fullPath);

        return fullPath;
    }

    /**
     * Get the name of the avro schema file we want to read or write. It
     * is based table name with a *.avsc file suffix.
     *
     * @return schema file name
     */
    public String getSchemaFileName() {
        String fileName;

        fileName = bdTableName + ".avsc";

        LOG.debug("getSchemaFileName(): {}", fileName);

        return fileName;
    }

    /**
     * Return the meta data we are tracking for this column.
     *
     * @param columnName - the key
     * @return the AvroColumn object, null if not found
     */
    public AvroColumn getAvroColumn(String columnName) {
        AvroColumn rval = null;

        if (columns.containsKey(columnName))
            rval = columns.get(columnName);
        else
            LOG.error("getAvroColumn: column name not found: {}", columnName);

        return rval;
    }

    /**
     * Get the avro schema.
     *
     * @return the actual Avro schema that we are working with.
     */
    public Schema getAvroSchema() {
        return schema;
    }

    /**
     * @return the schema id from the schema registry.
     */
    public int getSchemaId() {
        return schemaId;
    }

    /**
     * Set the schema ID.
     * @param val the value to set.
     */
    public void setSchemaId(int val) {
        schemaId = val;
    }


}
