/* ./src/main/java/com/oracle/bdglue/meta/schema/DownstreamColumnMetaData.java 
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Column meta data containing only the info we need downstream for
 * processing. This allows us to decouple the code from any upstream
 * meta data definitions. This makes for cleaner code and will
 * facilitate easier debugging and unit testing.
 *
 */
public class DownstreamColumnMetaData {
    private static final Logger LOG = 
        LoggerFactory.getLogger(DownstreamColumnMetaData.class);
    private static BigDataName bdname;

    private String origColumnName;
    private String bdColumnName;
    private boolean keyCol;
    private boolean nullable;
    private int jdbcType;

    /**
     * Build an instance of this class from the supplied column meta data.
     * 
     * @param columnName name of the column 
     * @param keyCol true if this is part of the table's "key"
     * @param nullable true if this column can be null
     * @param jdbcType java.sql.TYpes column type
     */
    public DownstreamColumnMetaData(String columnName, boolean keyCol, boolean nullable, int jdbcType) {
        super();
        
        if (bdname == null) {
            bdname = new BigDataName();
        }
        this.origColumnName = columnName;
        this.bdColumnName = bdname.validName(columnName);
        this.keyCol = keyCol;
        this.nullable = nullable;
        this.jdbcType = jdbcType;
    }

    /**
     * Get the original column name.
     * 
     * @return the column name
     */
    public String getOrigColumnName() {
        return origColumnName;
    }
    
    /**
     * Get the column name in an appropriate format for
     * big data targets, which don't support special characters.
     * Special characters will be substituted based on the configured
     * properties for renaming.
     * 
     * @return the a valid big data column/field name.
     */
    public String getBDColumnName() {
        return bdColumnName;
    }

    /**
     * Is this a key column?
     * 
     * @return true if the column is a keycol.
     */
    public boolean isKeyCol() {
        return keyCol;
    }

    /**
     * Can this column be null?
     * 
     * @return true if the column is nullable.
     */
    public boolean isNullable() {
        return nullable;
    }
    
    /**
     * Get the type associated with this column. We use
     * java.sql.Types for this.
     * 
     * @return the java.sql.Types value for the column.
     */
    public int getJdbcType() {
        return jdbcType;
    }
    
    /**
     * Override jdbc type where type mismatch is found at runtime. So
     * far, this has been particular to GoldenGate with some integer types
     * actually being reported as "numeric". We attempt to figure this out 
     * and override this when appropriate. In theory, this will only be
     * called the first time a numeric type is encountered for a column, and 
     * then will be overriden and treated normally thereafter.
     * 
     * @param type the new jdbc type.
     */
    public void setJdbcType(int type) {
        jdbcType = type;
    }
}
