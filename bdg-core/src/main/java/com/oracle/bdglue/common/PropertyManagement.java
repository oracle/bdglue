/* ./src/main/java/com/oracle/bdglue/common/PropertyManagement.java 
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
package com.oracle.bdglue.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage properties for the application.
 */
public class PropertyManagement {
    private static final Logger LOG = 
        LoggerFactory.getLogger(PropertyManagement.class);
    
    private static PropertyManagement myProperties = null;


    private Properties properties;
    private Properties defaults;

    private PropertyManagement() {
        super();
    }

    /**
     * Load the properties from the properties file.
     * 
     * @param defaultProperties name of the default properties file
     * @param externalProperties name of the external properties file
     */
    public PropertyManagement(String defaultProperties, String externalProperties) {
        super();
        loadProperties(defaultProperties, externalProperties);
    }

    /**
     * Set a property for this environment. If the property doesn't exist,
     * a new one is created.
     *
     * @param property the name of the property
     * @param value the value associated with this property
     */
    public void setProperty(String property, String value) {
        LOG.info("setProperty: {} : {} ", property, value);
        properties.setProperty(property, value);
    }

    /**
     * Get the value of the specified property.
     *
     * @param property the name of the property whose value is to be
     *        retrieved.
     * @return the value of the requested property
     */
    public String getProperty(String property) {
        String rval = properties.getProperty(property);

        if (rval == null) {
            LOG.error("getProperty: Property not found: {}", property);
        }

        return rval.trim();
    }
    
    /**
     * Get the value of the specified property, setting a default value
     * if the property has not been intitialized.
     * 
     * @param property the name of the property
     * @param defaultValue the default value to set if it has not been initialized.
     * @return the value of the requested property.
     */
    public String getProperty(String property, String defaultValue) {
        String rval;
        
        rval = properties.getProperty(property);
        
        if (rval == null) {
            LOG.info("getProperty: Property not set: {}. Using default:", 
                      property, defaultValue);
            properties.setProperty(property, defaultValue);
            rval = defaultValue;
        }
        
        return rval.trim();
    }
    
    /**
     * Return the requested property as an integer, setting a default 
     * value if it hasn't been initialized. No error checking is performed
     * here. If the value of the property is not in fact representative 
     * of an integer, results will be indeterminent.
     * 
     * @param property the name of the property
     * @param defaultValue the default value
     * @return the value of the property as an integer.
     */
    public int asInt(String property, String defaultValue) {
        String value = getProperty(property, defaultValue);
        
        return Integer.parseInt(value.trim());
    }
    
    /**
     * Return the requested property as a boolean value, setting a default 
     * value if it hasn't been initialized. No error checking is performed
     * here. If the value of the property is not in fact representative 
     * of a a boolean, results will be indeterminent.
     * 
     * @param property the name of the property
     * @param defaultValue the default value
     * @return the value of the property as a boolean value.
     */
    public boolean asBoolean(String property, String defaultValue) {
        String value = getProperty(property, defaultValue);
        
        return Boolean.parseBoolean(value.trim());
    }
    
    /**
     * Get a list of all of the property names.
     * 
     * @return a Set of those names.
     */
    public Set<String> getKeyNames() {
        return properties.stringPropertyNames();
    }

    /**
     * Load the properties from their storage locations.
     * 
     * @param defaultProperties name of the default properties resource
     * @param externalProperties name of the system property that has the 
     *        file name of the external properties file.
     */
    public void loadProperties(String defaultProperties, 
                               String externalProperties) {
        defaults = new Properties();
        loadDefaultProperties(defaultProperties);
        properties = new Properties(defaults);
        loadExternalProperties(externalProperties);
        
        //printProperties(properties);

    }
    


    /**
     * Load the default properties for this project.
     *
     * @param property the property that contains the name of the
     * class resource containing the properties.
     */
    private void loadDefaultProperties(String property) {
        InputStream in = this.getClass().getResourceAsStream(property);
        
        try {
            LOG.info("loadDefaultProperties: properties resource found");
            defaults.load(in);
        } catch (IOException e) {
            LOG.error("loadDefaultProperties: resource not found {}",
                      property);
        }

    }

    /**
     * Load proerties from an external properties file. It first 
     * looks for a system property that tells us where to look, 
     * and if not found looks for a file that has the name of
     * the system property.
     *
     * @param property the property that contains the file name.
     */
    private void loadExternalProperties(String property) {

        InputStream fin = null;
        String externalFileName = System.getProperty(property);

        if (externalFileName == null) {
            // property not found, so see if we can just
            // find a file by the property name
            LOG.info("loadExternaProperties: " + 
                     "System property {} not defined. " + 
                     "Looking for default file.",
                     property);
            externalFileName = property;
        }


        try {
            fin = new FileInputStream(new File(externalFileName));

            try {
                properties.load(fin);
                LOG.info("loadExternalProperties: {}", externalFileName);
            } catch (IOException e) {
                LOG.error("loadExternalProperties: IO Exception", e);
            }
        } catch (FileNotFoundException e) {
            LOG.warn("loadExternaProperties: file not found: {}", 
                     externalFileName);
        }
    }
    
    /**
     * Return a subset of the properties that begin with "prefix".
     * 
     * @param prefix the prefix of the properties to return.
     * @param trimPrefix true: return the subset with "prefix" removed.
     * @return the subset of the properties.
     */
    public Properties getPropertySubset(String prefix, boolean trimPrefix) {
        Properties subset = new Properties();

        if (prefix.charAt(prefix.length() - 1) != '.') {
            // prefix does not end in a dot, so add one.
            prefix = prefix + '.';
        }

        for (String key : myProperties.getKeyNames()) {

            if (trimPrefix) {
                // remove the prefix from the result and return that.
                if (key.startsWith(prefix)) {
                    subset.setProperty(key.substring(prefix.length()), myProperties.getProperty(key));
                }

            } else {
                // return the property as is.
                if (key.startsWith(prefix)) {
                    subset.setProperty(key, myProperties.getProperty(key));
                }
            }
        }

        return subset;
    }
    
    /**
     * Print the properties for this instance of PropertyManagement.
     */
    public void printProperties() {
        printProperties(properties);
    }

    /**
     * Print the properties found in the specified 
     * instance of Properties in sorted order.
     * 
     * @param props the properties to print
     */
    public void printProperties(Properties props) {
        
        SortedSet<String> keySet = new TreeSet<String>();
        keySet.addAll(props.stringPropertyNames());
        
        LOG.info("************************");
        LOG.info("*** Begin Properties ***");
        LOG.info("************************");
        
        for (String s : keySet) {
            LOG.info("*** {} = {}", s, props.getProperty(s));
        }
        LOG.info("*************************");
        LOG.info("*** End of Properties ***");
        LOG.info("*************************");
    }
    
    /**
     * Typically, we want a singleton in our application that
     * will hold all of our properties. This supports
     * that use case.
     *
     * @param defaultProperties the name of the default properties file
     * @param externalProperties the name of the external properties file
     * @return the singleton instance of PropertyManagement
     */
    public static PropertyManagement getProperties(String defaultProperties, 
                                               String externalProperties) {
        if (myProperties == null) {
            myProperties = new PropertyManagement(defaultProperties, 
                                            externalProperties);
        }
        return myProperties;
    }
    /**
     * Return the singleton instance. This method assumes that the 
     * overloaded method of the same name has already been called
     * to construct the properties instance.
     * 
     * @return the singleton properties instance
     */
    public static PropertyManagement getProperties() {
        return myProperties;
    }

   

}
