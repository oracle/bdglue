/* ./src/main/java/com/oracle/bdglue/encoder/EventQueue.java 
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

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that manages a queue of events for processing. It leverages a
 * BlockingQueue to do that.
 * 
 * @param <T> The type of data we are handling in the queue
 */
public class EventQueue<T> {
    public static final int publisherQueueSize = 10;
    public static final int publisherThreadQueueSize = 10;
    public static final int encoderInputQueueSize = 5;
    public static final int encoderOutputQueueSize = 5;
    
    private static final Logger LOG = LoggerFactory.getLogger(EventQueue.class);
    private String name;
    private int queueSize = 0;
    private long opCount = 0;
    private int avgQueueDepth = 0;
    private int peakQueueDepth = 0;
    private BlockingQueue<T> queue;


    /**
     * Create a queue for managing EventData.
     * <p>
     * This is a wrapper around an instance of BlockingQueue to make it
     * easier to collect statistics.
     * 
     * @param name the name of the queue
     * @param size the size / depth of the queue
     */
    public EventQueue(String name, int size) {
        super();
        LOG.debug("Creating event queue: {} of size {}", name, size);
        this.name = name;
        queueSize = size;
        queue = new ArrayBlockingQueue<>(queueSize);
    }

    /**
     * Put an event on this queue.
     * 
     * @param evt the event we are putting on the queue
     * @throws InterruptedException if interrupted while waiting to put data on the queue
     */
    public void put(T evt) throws InterruptedException {
        int sz;

        queue.put(evt); // will wait here if queue is full
        
        /* calculate stats for the queue */
        sz = queue.size();
        peakQueueDepth = (sz > peakQueueDepth) ? sz : peakQueueDepth;
        opCount++;
        avgQueueDepth += (sz - avgQueueDepth) / opCount;
        
    }

    /**
     * Take an event off of this queue. 
     * <p>
     * This call will block and wait for data if there is nothing
     * on the queue.
     * 
     * @return the event at the head of the queue.
     * @throws InterruptedException if interrupted while waiting for data to take from the queue
     */
    public T take() throws InterruptedException {
        return queue.take();
    }

    /**
     * Peek at the object at the head of the queue.
     * <p>
     * This call is similar to take except that it does
     * not block and the returned object is not removed from 
     * the queue.
     * @return the object at the head of the queue, null otherwise.
     */
    public T peek() {
        return queue.peek();
    }

    /**
     * @return the number of objects currently in the queue.
     */
    public int size() {
        return queue.size();
    }

    /**
     * Drain the queue to the specified collection. This may be more efficient than "taking"
     * one element from the queue at a time. Note that this does not block and will return 0 
     * if the queue is empty.
     * 
     * @param collection the java.util.Collection to drain the queued elements to.
     * @return the number of elements transferred
     */
    public int drainTo(Collection<T> collection) {
        return queue.drainTo(collection);
    }

    /**
     * Return the status of this queue formatted as a String. It contains information to help
     * make determinations on whether the the queue should be larger, or more threads might 
     * be required, etc.
     * 
     * @return the status of this queue as a String
     */
    public String status() {
        String tstrg;
        
        tstrg = String.format("*** %s: Total Operations(%d) Queue: Size(%d) Peak(%d) Avg(%d)%n", 
                             name, opCount, queueSize, peakQueueDepth, avgQueueDepth);
        
        return tstrg;
    }
}
