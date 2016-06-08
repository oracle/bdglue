/* ./src/main/java/com/oracle/bdglue/utility/schemadef/Format.java 
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the schema formaters that we support. This is abstract and must
 * be extended.
 *
 */
public abstract class Format {
    private static final Logger LOG = LoggerFactory.getLogger(Format.class);
    private String fileName;
    private String fileSuffix;
    private String path;
    private PropertyManagement properties;
    private ByteArrayOutputStream baos;
    

    public Format() {
        super();
        properties = PropertyManagement.getProperties();
        path = properties.getProperty(SchemaDefPropertyValues.OUTPUT_PATH, "./output");
        baos = new ByteArrayOutputStream(); 
    }

    /**
     * Get the output stream we will be writing to.
     * 
     * @return the ByteaArrayOutputStream we are working with
     */
    public ByteArrayOutputStream baos() {
        return baos;
    }

    /**
     * Set the name of the target output file.
     * 
     * @param name the name of the file we we will write to
     */
    public void setFileName(String name) {
        fileName = name;
    }


    /**
     * Set the file suffix that identifies the output file type.
     * 
     * @param suffix the file extension (suffix) that we will
     * append to the name of the file.
     */
    public void setFileSuffix(String suffix) {
        fileSuffix = suffix;
    }

    /**
     * Get the output stream as a String.
     * 
     * @return the ByteArrayOutputStream as a String
     */
    public String getBuffer() {
        return baos.toString();
    }
    
    /**
     * Get the properties for the utility.
     * 
     * @return an instance of PropertyManagement
     */
    public PropertyManagement getProperties() {
        return properties;
    }
    
 

     /**
      * write the generated file to disk.
      *
      */
     public void writFile() {
         File file;
         BufferedWriter output;
         String fqPath;

         fqPath = String.format("%s/%s.%s", path, fileName, fileSuffix);

         // trim off the file name at the end of the path
         if (validateDirectory(path)) {
             try {

                 file = new File(fqPath);
                 output = new BufferedWriter(new FileWriter(file));
                 output.write(getBuffer());
                 output.close();
             } catch (IOException e) {
                 LOG.error("writeSchemaFile(): IOException on path: {}", fqPath);
                 e.printStackTrace();
             }
         }
     }
     

     
     /**
      * Make sure that the directory exists, and if not, create it.
      *
      * @param path the path to the directory we want to acccess
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
                 LOG.error("validateDirectory: " +
                              "directory creation failed: {}", path);
                 success = false;
             }
         }
         return success;
     }


    /**
     * Format the "top" portion of the output file.
     *
     * @param schema the name of the schema that contains the table we are working with
     * @param table the name of the table we are working with
     * @param noDoc don't generate the doc field. This is specific to avro, and a hack
     * needed to avoid a schema incompaitibiliy issue in the Hive SerDe if the old
     * schema did not also have the doc field.
     * @throws IOException if a JSON encoding issue occurs
     */
    public abstract void top(String schema, String table, boolean noDoc) throws IOException;

    /**
     * Format the "bottom" portion of the output file.
     * 
     * @param table the name of the table we are working with
     * @throws IOException if a JSON encoding issue occurs
     */
    public abstract void bottom(String table) throws IOException;

    /**
     * Format the fields that make up the "middle" of the output file.
     * 
     * @param column the name of the column we are working with
     * @param writeDefault whether or not to include a default value for the column.
     * @throws IOException if a JSON encoding issue occurs
     */
    public abstract void writeField(ColumnInfo column, boolean writeDefault) throws IOException;

}
