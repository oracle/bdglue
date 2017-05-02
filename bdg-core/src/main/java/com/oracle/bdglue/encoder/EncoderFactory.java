/* ./src/main/java/com/oracle/bdglue/encoder/EncoderFactory.java 
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

import com.google.common.base.Throwables;

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory class that instantiates encoders of the type 
 * specified in the properties file on request.
 */
public class EncoderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(EncoderFactory.class);
    
    private static EncoderType encoderType;

    private static EncoderFactory myFactory = null;
    

    public EncoderFactory() {
        super();
    }
    
    /**
     * Creates an instance of the class specified in the properties file.
     * An error is logged and the exception rethrown if the class cannont be 
     * found.
     * 
     * @return an instance of a class that has implemented BDGlueEncoder.
     */
    @SuppressWarnings("unchecked")
    public static BDGlueEncoder encoderFactory() {
        PropertyManagement properties;
        BDGlueEncoder encoder = null;
        String className = null;
        Class<BDGlueEncoder> clazz = null;
        properties = PropertyManagement.getProperties();
        className = properties.getProperty(BDGluePropertyValues.ENCODER_CLASS);
        try {
            clazz = (Class<BDGlueEncoder>) Class.forName(className);
            encoder = clazz.newInstance();

        } catch (Exception e) {
            LOG.error("Could not instantiate encoder.", e);
            Throwables.propagate(e);
        }
        
        encoderType = encoder.getEncoderType();
        
        return encoder;
    }

    /**
     * Return the EncoderType for specified encoder.
     * @return the EncoderType 
     */
    public static EncoderType getEncoderType() {
        return encoderType;
    }
    
}
