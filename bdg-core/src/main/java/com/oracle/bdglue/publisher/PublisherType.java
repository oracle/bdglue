/* ./src/main/java/com/oracle/bdglue/publisher/PublisherType.java 
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
package com.oracle.bdglue.publisher;

/**
 * Enumeration that indicates the type of publisher. 
 */
public enum PublisherType {
    /** 
     * Indicates this is the CONSOLE publisher. It prints "ecnoded"
     * output to standard out. JSON encoding is recommended.
     * Useful for smoke testing and debug.
     */ 
    CONSOLE,
    /** Indicates this is the FLUME publisher */
    FLUME,
    /** Indicates this is the KAFKA publisher */
    KAFKA,
    /** Indicates this is the HBase publisher */
    HBASE,
    /** Indicates this is the Oracle NoSQL publisher */
    NOSQL,
    /** 
     * Indicates this is a custom publisher that is not
     * one of the publishers included with BDGlue by default
     */ 
    CUSTOM
}
