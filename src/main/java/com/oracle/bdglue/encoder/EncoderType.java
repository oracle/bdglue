/* ./src/main/java/com/oracle/bdglue/encoder/EncoderType.java 
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
package com.oracle.bdglue.encoder;

/**
 * An enumeration that lists the encoder types that are inerently part of BDGlue, 
 * and provides an option to identify a custom encoder. Note that use of the
 * CUSTOM type is not specifically required, but encouraged.
 */
public enum EncoderType {
    /** This is an avro encoder that encodes a record into a byte array */
    AVRO_BYTE_ARRAY,
    /** An Avro encoder that passes along a Generic Record */
    AVRO_GENERIC_RECORD,
    /** This is a json encoder */
    JSON,
    /** This is a (delimited) text encoder */
    TEXT,
    /** THis is a null encoder */
    NULL,
   
    /** Indicates that this custom encoder.  */
    CUSTOM
}
