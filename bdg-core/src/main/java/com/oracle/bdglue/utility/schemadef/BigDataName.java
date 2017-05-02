/* ./src/main/java/com/oracle/bdglue/utility/schemadef/BigDataName.java 
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
package com.oracle.bdglue.utility.schemadef;

import com.oracle.bdglue.common.PropertyManagement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class responsible for converting invalid table or column names to valid ones.
 * It makes use of a regular expression to identify the invalid characters
 * and replaces them with the specified "valid" character.
 * 
 * <p>
 * In most cases, the only valid non-alphanumeric character is "_",
 * and names may not begin with a number of '_'. This is a utility class
 * that takes care of the transformations.
 * <p>
 * This class is used by both SchemaDef and by BDGlue itself. 
 */
public class BigDataName {
    private static final Logger LOG = LoggerFactory.getLogger(BigDataName.class);

    private String regEx;
    private String replaceChar;
    private String firstChar;


    /**
     * Instantiate in instance of BigDataName. 
     * Find the relevant configuration parameters in the properties file.
     */
    public BigDataName() {
        String regEx;
        String replaceChar;
        String firstChar;
        PropertyManagement properties = PropertyManagement.getProperties();
        regEx = properties.getProperty(SchemaDefPropertyValues.REPLACE_REGEX, 
                                       SchemaDefPropertyValues.REPLACE_REGEX_DEFAULT);
        replaceChar = properties.getProperty(SchemaDefPropertyValues.REPLACE_CHAR, 
                                       SchemaDefPropertyValues.REPLACE_CHAR_DEFAULT);
        firstChar = properties.getProperty(SchemaDefPropertyValues.REPLACE_FIRST, 
                                       SchemaDefPropertyValues.REPLACE_FIRST_DEFAULT);
        
        init(regEx, replaceChar, firstChar);
    }
    /**
     * Instantiate in instance of BigDataName, using the parameters passed
     * in to configure it as desired.
     * 
     * @param regEx the regular expression to use in substituion.
     * @param replaceChar the character to substitute for invalid characters.
     * @param firstChar the character to prepend to names that begin with an invalid char.
     */
    public BigDataName(String regEx, String replaceChar, String firstChar) {
        super();
        init(regEx, replaceChar, firstChar);
    }
    
    /**
     * Initialize the class based on the given parameters.
     * 
     * @param regEx the regular expression to use in substituion.
     * @param replaceChar the character to substitute for invalid characters.
     * @param firstChar the character to prepend to names that begin with an invalid char.
     */
    private void init(String regEx, String replaceChar, String firstChar) {
        this.regEx = regEx;
        this.replaceChar = replaceChar;
        this.firstChar = firstChar;
        LOG.info("regEX: {}", regEx);
        LOG.info("replaceChar: {}", replaceChar);
        LOG.info("firstChar: {}", firstChar);
    }

    /**
     * Convert a potentially invalid name to a valid one.
     * @param name the name we want to make valid if needed
     * @return the potentially reformatted name.
     */
    public String validName(String name) {
        String rval;
        char tchar;
        
        rval = name.replaceAll(regEx, replaceChar);
        // if the first character is not a letter, insert one.
        tchar = rval.charAt(0);
        if (!Character.isLetter(tchar)) {
            rval = firstChar + rval;
        }
        
        
        if (!rval.equals(name)) {
            LOG.debug("validName(): Mapping {} to {}", name, rval);
        }
        
        return rval;
    }
}
