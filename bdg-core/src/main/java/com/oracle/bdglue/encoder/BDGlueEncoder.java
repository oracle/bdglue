/* ./src/main/java/com/oracle/bdglue/encoder/BDGlueEncoder.java 
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

import com.oracle.bdglue.meta.transaction.DownstreamOperation;

import java.io.IOException;

/**
 * An interface that must be implemented by all BDGlue encoders.
 */
public interface BDGlueEncoder {
    /**
     * @param op DownstreamOperation to be encoded/processed
     * @return the encoded operation
     * @throws IOException if encoding error occurs
     */
    public EventData encodeDatabaseOperation(DownstreamOperation op) throws IOException;

    /**
     * @return the EncoderType for this encoder. 
     */
    public EncoderType getEncoderType();
}
