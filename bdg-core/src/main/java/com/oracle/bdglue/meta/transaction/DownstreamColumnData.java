/* ./src/main/java/com/oracle/bdglue/meta/transaction/DownstreamColumnData.java 
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
package com.oracle.bdglue.meta.transaction;


import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.utility.schemadef.BigDataName;

import java.math.BigDecimal;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that contains the data captured for a column. We are using this
 * class to decouple from the GG Java User Exit types to make testing
 * easier.
 *
 */
public class DownstreamColumnData {
    private static final Logger LOG = 
        LoggerFactory.getLogger(DownstreamColumnData.class);
    private static BigDataName bdname = null;
    
    private static boolean replaceNewline = 
        PropertyManagement.getProperties().asBoolean(BDGluePropertyValues.REPLACE_NEWLINE, 
                                                     BDGluePropertyValues.REPLACE_NEWLINE_DEFAULT);
    private static String replaceChar = 
        PropertyManagement.getProperties().getProperty(BDGluePropertyValues.NEWLINE_CHAR, " ");

    
    private String bdColumnName;
    private String originalColumnName;
    private String stringValue;
    private byte[] binaryValue;
    private boolean keyCol;
    

    /**
     * Construct an object that contains the data associated with a particular
     * column that we will be processing.
     * 
     * @param columnName the name of the column
     * @param stringValue the value of the column as a String
     * @param binaryValue the value of the column as a byte array for those
     *        circumstances where a String isn't appropriate.
     * @param keyCol true if this column is a key column
     */
    public DownstreamColumnData(String columnName, String stringValue, byte[] binaryValue, boolean keyCol) {
        super();

        if (bdname == null) {
            bdname = new BigDataName();
        }

        this.originalColumnName = columnName;
        this.bdColumnName = bdname.validName(columnName);
        this.stringValue = stringValue;
        this.binaryValue = binaryValue;
        this.keyCol = keyCol;
    }

     /**
      * Get the name of this column in a "Big Data Friendly" format. If there
      * are no special characters, it will be the same as the original column name.
      * 
      * @return a valid big data column/field name
      */
    public String getBDName() {
        return bdColumnName;
    }
    
    /**
     * Get the original column name. 
     * 
     * @return the original RDBMS column name
     */
    public String getOrigName() {
        return originalColumnName;
    }
    
    /**
     * Is this a key column?
     * 
     * @return true if this is a key column
     */
    public boolean isKeyCol() {
        return keyCol;
    }

    /**
     * This is a bit of a hack ... it seems that the the GG Java Adapter
     * sets the string value of a "Null" column to "NULL" rather than
     * making the string itself a null value in some cases. It seems that
     * string types get a null value, and numeric types get set to the
     * string "NULL", but I'm not entirely sure of the logic.
     * 
     * NOTE: may have figured out a way around the hack. Comment to be
     * cleaned up later.
     * 
     * @return true if the value is "NULL", false otherwise.
     */
    public boolean checkForNULL() {
        boolean rval = false;
        
        if (stringValue == null) {
            rval = true;
        }
        /*
         * Added additional check by calling isValueNull() in
         * GG11HandlerMapper.setColumns() that hopefully will 
         * eliminate the need for this check.
         * 
        else if (stringValue.equals("NULL")) {
            rval = true;
        }
        */
        
        return rval;
    }
    /**
     * Return the value from the String representation
     * of the field parsed as an integer.
     * 
     * @return the data encoded as an integer
     */
    public int asInteger() {
        return Integer.parseInt(stringValue);
    }
    /**
     * Return the value from the String representation 
     * of the field parsed as a long.
     * 
     * @return the data encoded as a long
     */
    public long asLong() {
        return Long.parseLong(stringValue);
    }
    /**
     * Return the value from the String representaion of
     * the field parsed as a float.
     * 
     * @return the data encoded as a float
     */
    public float asFloat() {
        return Float.parseFloat(stringValue);
    }
    
    /**
     * Return the value from the String representation
     * of the field parsed as a double.
     * @return the data encoded as a double
     */
    public double asDouble() {
        return Double.parseDouble(stringValue);
    }
    
    /**
     * Use this for mapping "packed decimal" types. Return the value
     * from the String representation of the field parsed as an
     * instance of BigDecimal.
     * 
     * @return the data encoded as an instance of Big Decimal
     */
    public BigDecimal asBigDecimal() {
        return new BigDecimal(stringValue);
    }
    
    /**
     * Return the data from the String representation of the data
     * encoded as a boolean value. This is based
     * on the first character in the string data, and will
     * default to false if the conversion isn't obvious. It recognizes
     * t, y, and 1 for "true", and f, n, and 0 for "false".
     * 
     * @return the data encoded as a boolean value.
     */
    public boolean asBoolean() {
        boolean rval = false;
        char firstChar;
        firstChar = stringValue.toLowerCase().charAt(0);

        switch (firstChar) {
        case 't':
        case 'y':
        case '1':
            rval = true;
            break;
        case 'f':
        case 'n':
        case '0':
            rval = false;
            break;
        default:
            rval = false;
        }
        return rval;
    }
    
    /**
     * Get the string representation of this column
     * as a String. The data is returned without any 
     * conversion with the exception that newlines will be 
     * replaced with a different character if the appropriate 
     * property is set. This is needed because embedded newlines 
     * in a field can cause challenges in some targets.
     * 
     * @return the string value for this column
     */
    public String asString() {
        String rval;
        
        if (replaceNewline == true) {
            rval = stringValue.replaceAll("\\r\\n|\\r|\\n", replaceChar);
        }
        else {
            rval = stringValue;
        }
        return rval;
    }
    
    /**
     * Get the binary value that was given to the constructor
     * and return it as a byte array.
     * 
     * @return the raw binary data for this column
     */
    public byte[] asBytes() {
        return binaryValue;
    }

    /**
     * Get the binary value that was given to the constructor
     * and return it as an instance of ByteBuffer.
     * 
     * @return the raw binary data for this column
     */
    public ByteBuffer asByteBuffer() {
        return ByteBuffer.wrap(binaryValue);
    }

}
