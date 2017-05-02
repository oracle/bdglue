/* ./src/main/java/com/oracle/bdglue/publisher/ParallelPublisher.java 
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

import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.encoder.EncoderThread;
import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.encoder.EventHeader;
import com.oracle.bdglue.encoder.EventQueue;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that manages a pool of publisher threads and itself executes as a thread.
 */
public class ParallelPublisher extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelPublisher.class);
    private static final String publisherName = "Publisher-main";
    
    private EventQueue<EventData> inputQueue; 
    private PropertyManagement properties;
    private int numPublishers;
    private ArrayList<PublisherThread> publishers;
    private boolean hashRowKey;

    /**
     * construct a Publisher with the defualt number of
     * threads for processing.
     */
    public ParallelPublisher() {
        super(publisherName);
        properties = PropertyManagement.getProperties();
        numPublishers = properties.asInt(BDGluePropertyValues.PUBLISHER_THREADS, BDGluePropertyValues.PUBLISHER_THREADS_DEFAULT);
        init();
    }

    /**
     * Construct a Publisher with the specified number
     * of threads for processing.
     * 
     * @param numPublishers the number of publisher threads to create
     */
    public ParallelPublisher(int numPublishers) {
        super(publisherName);
        this.numPublishers = numPublishers;
        init();
    }
    
    /**
     * Initializes the ecoder with the specified number
     * of Threads, etc.
     */
    private void init() {
        PropertyManagement properties = PropertyManagement.getProperties();
        PublisherThread publisher;
        String name;
        
       inputQueue = new EventQueue<>(publisherName, EventQueue.publisherQueueSize);
    
        publishers = new ArrayList<>(numPublishers);
        for(int i = 0; i < numPublishers; i++) {
            name = "Publisher-" + i;
            publisher = new PublisherThread(name);
            publisher.start();
            publishers.add(publisher);
        }
        
        String tstrg = properties.getProperty(BDGluePropertyValues.PUBLISHER_HASH);
        if ((tstrg != null) && tstrg.equalsIgnoreCase("rowkey")) {
            // force row key inclusion if we are hashing on it in the publisher
            hashRowKey = true;
        }
    }


    /**
     * Puts the event the appropriate queue for
     * subsequent processing.
     * 
     * @param evt the EventData we are putting on a queue for the publishers
     * @throws InterruptedException if we are interrupted while waiting to put
     * the event on the queue.
     */
    public void put(EventData evt) throws InterruptedException {
        inputQueue.put(evt);
    }
    
    /**
     * This is the thread that routes the encoded events
     * to the appropriate PublisherThread for delivery.
     *
     */
    @Override
    public void run() {
        ArrayList<EventData> list = new ArrayList<>(10);
        int rval = 0;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                //evt = inputQueue.take();
                //publish(evt);
                // block until there is an event to read and put
                // that first element on the list.
                list.add(inputQueue.take());
                // now get everything else that may be queued up
                rval = inputQueue.drainTo(list);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("drained {} elements from input queue.", rval);
                }

                for (EventData evt : list) {
                    publish(evt);
                }
                list.clear();
            } catch (InterruptedException ie) {
                // clean up and allow thread to exit
                LOG.debug("Thread run(): Interrupted Exception in ParallelPublisher");
                // set the interrupted flag for this thread to be sure we fall out of the loop.
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private void shutdown() {
        LOG.info("Thread {} : shutting down ParallelPublisher", publisherName); 
        // pause briefly to allow things to drain
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            LOG.trace("shutdown() timer");
        }
        // clean up the publisher threads
        for (PublisherThread publisher : publishers) {
            publisher.cancel();
        }
        // now wait for them to finish
        for (PublisherThread publisher : publishers) {
            try {
                publisher.join(1000);
            } catch (InterruptedException e) {
                LOG.warn("shutdown: join of publisher thread interrupted");
            }
        }
        if (inputQueue.size() != 0) {
            LOG.warn("shutdown(): ParallelPublisher input queue NOT drained. Size={}", 
                     inputQueue.size());
        } else {
            LOG.info("shutdown(): ParallelPublisher input queue has been drained. Size={}", 
                     inputQueue.size());
        }
    }


    /**
     * Call this method to terminate the thread and exit.
     * 
     */
    public void cancel()  {
        shutdown();
        
        interrupt();
    }

    /**
     * Route the event to the appropriate PublisherThread for processing.
     *
     * @param evt the event we want to publish
     * @throws InterruptedException if we are interrupted while waiting to put the event 
     * on the publisher's queue.
     */
    public void publish(EventData evt) throws InterruptedException {
        int hashCode;
        int threadNumber;
        String hashString;
        
        if (hashRowKey) {
            hashString = evt.getMetaValue(EventHeader.ROWKEY);
        }
        else {
            hashString = evt.getMetaValue(EventHeader.TABLE);
        }
        // hashcodes can be negative. Need a positive result below.
        hashCode = Math.abs(hashString.hashCode());
        
        threadNumber = (hashCode % numPublishers);
        if (threadNumber >= publishers.size()) {
            LOG.warn("threadNumber {} out of bounds. Defaulting to: {}", 
                     threadNumber, publishers.size()-1);
            threadNumber = publishers.size()-1;
        }
        publishers.get(threadNumber).put(evt);
     }
    
    /**
     * Get the current status of the thread as a String for logging purposes.
     * 
     * @return a status string for this thread.
     */
    public String status() {
        StringBuilder rval = new StringBuilder();

        rval.append(inputQueue.status());
        for (PublisherThread publisher : publishers) {
            rval.append(publisher.status());
        }
        
        return rval.toString();
    }
}
