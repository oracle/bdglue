/* ./src/main/java/com/oracle/bdglue/utility/schemadef/FormatHiveAvro.java 
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


/**
 * Format the Hive schema meta data based on the assumption that we will be
 * storing data in HDFS as Avro files. It formats the proper syntax to leverage
 * the Hive Avro SerDe that handles mapping this data as external tables.
 */
public class FormatHiveAvro extends Format {
    public FormatHiveAvro() {
        super();        
        setFileSuffix("hql");
        
        init();
    }
    
    /**
     * Reset so we can use this again.
     */
    public void init() {
        baos().reset();
    }

     /**
     * Format the top portion of the Hive schema file.
     *
     * @param schema the name of the schema
     * @param table the name of the table
     * @param noDoc unused. Specific to avro.
     * @throws IOException if an IO error occurs
     */
    @Override
    public void top(String schema, String table, boolean noDoc) throws IOException {
        String schemaTable = String.format("%s.%s", schema, table);
        setFileName(schemaTable);
        
        String pathToData = getProperties().getProperty(SchemaDefPropertyValues.LOCATION);
        String avroSchemaURL = String.format("%s/%s.avsc", 
                                             getProperties().getProperty(SchemaDefPropertyValues.AVRO_URL),  
                                             schemaTable);
        
        if (schema == null) {
            schema = "default";
        }
        
        String createSchema = String.format("CREATE SCHEMA IF NOT EXISTS %s; %n", schema);
        String use =  String.format("USE %s; %n", schema);
        String drop = String.format("DROP TABLE IF EXISTS %s; %n", table);
        String create = 
            String.format("CREATE EXTERNAL TABLE %s %n", table);
        String comment = 
            String.format("COMMENT \"Table backed by Avro data with the Avro schema stored in HDFS\"%n");
        String rowFormat = 
            String.format("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'%n");
        String storedAs = String.format("STORED AS %n");
        String inputFormat = 
            String.format("INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'%n");
        String outputFormat = 
            String.format("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'%n");
        String location = String.format("LOCATION '%s/%s/' %n", pathToData, schemaTable);
        String tableProperties = String.format("TBLPROPERTIES ( 'avro.schema.url'='%s' ); %n",
                                               avroSchemaURL);
        init();
        baos().write(createSchema.getBytes());
        baos().write(use.getBytes());
        baos().write(drop.getBytes());
        baos().write(create.getBytes());
        baos().write(comment.getBytes());
        baos().write(rowFormat.getBytes());
        baos().write(storedAs.getBytes());
        baos().write(inputFormat.getBytes());
        baos().write(outputFormat.getBytes());
        baos().write(location.getBytes());
        baos().write(tableProperties.getBytes());

    }

    /**
     * Format the bottom of the schema. In this case, nothing occurs in this method 
     * because there is no "middle" either.
     * 
     * @param table the table name
     */
    @Override
    public void bottom(String table)  {
        // nothing to do here

    }

    /**
     * Format the "middle" of the schema. In this case, there is no middle as 
     * individual fields are not represented in the schema.
     * 
     * @param column the name of the column
     * @param writeDefault whether or not to include default values
     */
    @Override
    public void writeField(ColumnInfo column, boolean writeDefault)  {
        // nothing to do here

    }
}
