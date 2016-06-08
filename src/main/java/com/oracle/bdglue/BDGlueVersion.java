/* ./src/main/java/com/oracle/bdglue/BDGlueVersion.java 
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
package com.oracle.bdglue;

/**
 * A simple class that allows us to store the current version number. This is also where
 * we keep tabs on changes to the code.
 * <p>
 *  Version: 1.0.1.0 09/22/2015
 *  <ul>
 *  <li>09/22/2015: 1.0.0.0: Updated javadoc comments to make things more complete and Maven happy.</li>
 *  <li>09/24/2015: 1.0.1.0: Added logic to optionally replace newline characters in string fields.</li>
 *  <li>10/07/2015: 1.0.1.1: Added support for before images of data in JSON encoding.</li>
 *  <li>10/23/2015: 1.0.1.2: Fixed issue with negative hash value in ParallelPublisher.</li>
 *  <li>10/27/2015: 1.0.1.3: Changes to make Kafka topics and message keys customizable.</li>
 *  <li>12/07/2015: 1.0.1.4: Added prompt for password in schemadef.</li>
 *  <li>12/09/2015: 1.0.1.5: Added support for including table name in encoded data.</li>
 *  <li>12/10/2015: 1.0.1.6: Added support for passthrough of properties from bdglue.properties to Kafka.</li>
 *  <li>12/16/2015: 1.0.1.7: Added code to better deal with compressed records and null column values.</li>
 *  <li>01/13/2016: 1.0.1.8: Added code to ignore updates where nothing changed.</li>
 *  <li>01/22/2016: 1.0.1.9: Added support for the Kafka Schema Registry.</li>
 *  <li>01/26/2016: 1.0.1.10: Added calls to trim() when retrieving properties.</li>
 *  <li>03/01/2016: 1.0.1.11: Changes to support GGforBD 12.2 and schema change events.</li>
 *  <li>03/10/2016: 1.0.1.12: Automatic generation of Avro schemas based on schema change events.</li>
 *  <li>03/23/2016: 1.0.1.13: Support specifying numeric output type in Avro schema generation.</li>
 *  <li>04/07/2016: 1.0.1.14: Added default null for avro schemas. Fixed issue with avro table name valid chars.</li>
 *  <li>04/09/2016: 1.0.1.15: Reworked schema registry encoding to pass Avro GenericRecord to serializers.</li>
 *  <li>04/21/2016: 1.1.0.0: Refactored pubishers with goal of more selecitve building at some point.</li>
 *  <li>05/15/2016: 1.1.1.0: Added Cassandra support.</li>
 *  <li>05/27/2016: 1.1.1.1: Added logic to force shutdowns when needed.</li>
 *  <li>06/01/2016: 1.1.1.2: Changed queue logic to use drainTo() to hopefully reduce latch waits.</li>
 *  <li>06/08/2016: 1.1.1.3: Changed BDGlue dynamic avro schema generation to make all columns nullable.</li>
 *  <li>06/08/2016: 1.2.0.0: Initial release to GitHub.</li>
 * </ul>
 */
public class BDGlueVersion {

    private static final String name = "BDGlue";
    private static final String major = "1";
    private static final String minor = "2";
    private static final String fix = "0";
    private static final String build = "0";
    private static final String date = "2016/06/08 13:45";
    
    /**
     * Track the version number.
     */
    public BDGlueVersion() {
        super();
    }

    /**
     * Main entry point to return the version information.
     * 
     * @param args not used at this time
     */
    public static void main(String[] args) {
        BDGlueVersion version = new BDGlueVersion();
        
        version.versionInfo();
    }

    /**
     * Format the version information. This is useful for logging
     * the version information using LOG4J.
     * 
     * @return a formatted String containing version info.
     */
    public String format() {
        String rval;
        
        rval = String.format("%s Version: %s.%s.%s.%s  Date: %s", 
                    name, major, minor, fix, build, date);
        
        return  rval;
    }
    
    /**
     * Write the version info to stdout.
     */
    public void versionInfo() {
        System.out.println(format());
    }
}
