package com.oracle.bdglue.publisher.cassandra;

/**
 * This class contains configuration constants used by the
 * Big Data Glue BigQuery Publisher, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 *
 */
public final class CassandraPublisherPropertyValues {
    
    /**
     * Properties related to CassandraPublisher.
     */
    /**
     * The node in the Cassandra cluster to connect to.
     */
    public static final String CASSANDRA_CONNECT_NODE = "bdglue.cassandra.node";
    public static final String CASSANDRA_CONNECT_NODE_DEFAULT = "localhost";
    /**
     * The number of operations to batch together.
     */
    public static final String CASSANDRA_BATCH_SIZE = "bdglue.cassandra.batch-size";
    public static final String CASSANDRA_BATCH_SIZE_DEFAULT = "5";
    /**
     * The frequency to flush operations if the batch-size isn't reached.
     */
    public static final String CASSANDRA_FLUSH_FREQ = "bdglue.cassandra.flush-frequency";
    public static final String CASSANDRA_FLUSH_FREQ_DEFAULT = "500";
    /**
     * Boolean: True if we want to convert deletes and updates into inserts. Assumes that
     * inclusion of operation type and timestamp has been specified in the properties.
     */
    public static final String CASSANDRA_INSERT_ONLY = "bdglue.cassandra.insert-only";
    public static final String CASSANDRA_INSERT_ONLY_DEFAULT = "false";
    
    /*******************************************************************/
    /**
     * private to prevent explicit object creation
     */
    private CassandraPublisherPropertyValues() {
        super();
    }
}
