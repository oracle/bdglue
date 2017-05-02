/* ./src/main/java/com/oracle/bdglue/encoder/TextEncoderHelper.java 
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

import org.apache.commons.lang.StringEscapeUtils;

/**
 * A helper class that handles basic escaping of special characters that might
 * cause problems when handled downstream.
 */

public class TextEncoderHelper {
    
    private static final char doubleQuote = '"';
    private static final char backslash = '\\';

    
    public TextEncoderHelper() {
        super();
    }
    
    /**
     * wrap the input string in quotes, and escape any characters
     * that need escaping.
     * 
     * @param strg input string
     * @return quoted string
     */
    public static StringBuilder quoted(String strg) {
        StringBuilder rval = new StringBuilder();
        
        rval.append(doubleQuote);

        rval.append(TextEncoderHelper.escapeChars(strg));

        rval.append(doubleQuote);
        
        return rval;
    }

    /**
     * Escape any characters in the string that need escaping.
     * 
     * @param strg the string that we want to escape
     * @return the (potentially escaped) string.
     */
    public static String escapeChars(String strg) {
        
                
        return StringEscapeUtils.escapeJava(strg);
    }
    
}
