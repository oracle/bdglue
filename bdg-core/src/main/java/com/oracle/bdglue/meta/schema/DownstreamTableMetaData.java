/* ./src/main/java/com/oracle/bdglue/meta/schema/DownstreamTableMetaData.java 
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
package com.oracle.bdglue.meta.schema;

import com.oracle.bdglue.utility.schemadef.BigDataName;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reduced subset of TableMetaData information that only contains the data
 * that we care about downstream. This lets us decouple the "big data" code
 * from upstream "meta data", whereever that may be coming from. It will
 * also make it easier for us to test the downstream packages independently,
 * as well as hopefully making BDGlue more generally useful.
 *
 *
 */
public class DownstreamTableMetaData {
    private static final Logger LOG = 
        LoggerFactory.getLogger(DownstreamTableMetaData.class);
    private static BigDataName bdname = null;

    private String tableName;
    private String longTableName;
    private String keyName;
    private ArrayList<DownstreamColumnMetaData> columns;

    /**
     * Create a table from the given meta data.
     * 
     * @param key the key we will use for looking up this table.
     * @param tableName the table name
     * @param longTableName the long table name (schema.name)
     */
    public DownstreamTableMetaData(String key, String tableName, String longTableName) {
        super();
        if (bdname == null) {
            bdname = new BigDataName();
        }
        columns = new ArrayList<>();
        this.keyName = key;
        this.tableName = bdname.validName(tableName);
        this.longTableName = bdname.validName(longTableName);
        LOG.info("Mapping table: " + keyName);
    }
    
 

    /**
     * Add a column to the table's meta data.
     * 
     * @param col meta data info for this column
     */
    public void addColumn(DownstreamColumnMetaData col) {
        columns.add(col);
    }

    /**
     * Get the "short" name of this table.
     * 
     * @return the "short" name of this table.
     */
    public String getBDTableName() {
        return tableName;
    }
    
    /**
     * Get the "long" (schema.table) version of the table name, which
     * in some cases could very well be the same as the short name.
     * BDGlue won't care if that is the case.
     *
     * @return the "schema.table" name
     */
    public String getBDLongTableName() {
        return longTableName;
    }


    /**
     * Get the "key" value for this table that is used to locate it
     * in the Set of tables we are managing.
     * 
     * @return the name for this table when used as a key. This may
     * or may not be the same value returned by getTableName();
     */
    public String getKeyName() {
        return keyName;
    }

    /**
     * Get the columns for this table as an array.
     * 
     * @return the meta data for the column requested, null if not found.
     */
    public ArrayList<DownstreamColumnMetaData> getColumns() {
        return columns;
    }
    
    /**
     * Find the meta data for the requested column.
     *
     * @param columnName the name of the column
     * @return the meta data for the column
     */
    public DownstreamColumnMetaData getColumn(String columnName) {
        DownstreamColumnMetaData rval = null;
        
        for (DownstreamColumnMetaData col: columns) {
            if (col.getOrigColumnName().equals(columnName)) {
                rval = col;
                break;
            }
        }
        
        return rval;
    }
}
