/* ./src/main/java/com/oracle/bdglue/target/flume/RPCType.java 
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
package com.oracle.bdglue.publisher.flume;

/**
 * Enum identifying which RPC protocol to use when talking to Flume.
 */
public enum RPCType {
    /**
     * Use the Avro RPC. This is the default and most common.
     */
    AVRO_RPC,
    /**
     * Use the Thrift RPC.
     */
    THRIFT_RPC
}
