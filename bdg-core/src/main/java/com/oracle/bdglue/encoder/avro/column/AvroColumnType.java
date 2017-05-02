/* ./src/main/java/com/oracle/bdglue/encoder/avro/column/AvroColumnType.java 
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
package com.oracle.bdglue.encoder.avro.column;

/**
 * These are the types of columns that we are currently supporting. 
 * There are more complex Avro types that we could support, but don't 
 * at this time. See AvroColumn.setColumnType() for a better idea 
 * of all of this.
 */
public enum AvroColumnType {
    /** A column type of NULL */
    NULL,
    /** A column type of Boolean */
    BOOLEAN,
    /** An number of type and precisoin Integer */
    INT,
    /** A number of type and precision Long */
    LONG,
    /** A number of type and precision Float */
    FLOAT,
    /** A number of type and precision Double */
    DOUBLE,
    /** A large object represented as binary data */
    LOB,
    /** 
     * A numeric column that actually might have varying types, 
     * at least when coming from GoldenGate.
     * We actually inspect the data and sort it out at run time.
     */
    NUMERIC,    
    /**
     * String data, including CLOBs. Data is converted to type Utf8() when
     * passed to Avro.
     */
    STRING,
    /**
     * This is not an actual Avro type. All string data is converted
     * to Utf8() and passed as a String. We have this here as a 
     * placeholderin the event that in the future we need / want some
     * special processing.
     */
    MULTIBYTE,
    /**
     * Not an Avro type. Avro does not know about date / time / timestamp
     * values. We pass this data thru as a String.
     */
    DATE_TIME,  
    /**
     * Set in the event we encounter a data type that we don't know
     * what to do with. An error will be logged and we will attempt
     * to treat the data as a String.
     */
    UNSUPPORTED
}
