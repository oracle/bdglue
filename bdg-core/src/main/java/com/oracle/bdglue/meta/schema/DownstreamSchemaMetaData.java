/* ./src/main/java/com/oracle/bdglue/meta/schema/DownstreamSchemaMetaData.java 
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container class holding the meta data for all of the tables that
 * have been mapped. This meta data is a subset consisting only of the
 * information we need and is used to decouple the code in BDGlue
 * from the upstream representations of meta data classes that are. 
 * This decoupling makes the code more indepent and thus hopefully
 * more generally useful.
 *
 */
public class DownstreamSchemaMetaData {
    private static final Logger LOG = LoggerFactory.getLogger(DownstreamSchemaMetaData.class);

    private HashMap<String, DownstreamTableMetaData> tables;
    
    public DownstreamSchemaMetaData() {
        super();
   
        tables = new HashMap<>();
    }
    
    /**
     * Get the meta data for this table as a Set.
     * 
     * @return the collection of tables that we have mapped meta data for.
     */
    public Set<Map.Entry<String, DownstreamTableMetaData>>getTableMetadata() {
        return tables.entrySet();
    }
    
    /**
     * Get the meta data for an individual table.
     * 
     * @param key the table name we want to look up
     * @return the meta data for that table
     */
    public DownstreamTableMetaData getTableMetaData(String key) {
        return tables.get(key);
    }

    /**
     * Determine if we have already added this table to the meta data
     * we are managing.
     * 
     * @param key the table name
     * @return true if the table has already been defined, false otherwise.
     */
    public boolean containsTable(String key) {
        return tables.containsKey(key);
    }

    /** 
     * Add this table to the list of tables we are managing, or
     * replace it if the table is already present in the list.
     * 
     * @param key the table name
     * @param table the table meta data
     */
    public void addTable(String key, DownstreamTableMetaData table) {
        tables.put(key, table);
    }
}
