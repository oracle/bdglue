/* ./src/main/java/com/oracle/bdglue/publisher/PublisherThread.java 
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

import com.oracle.bdglue.encoder.EventData;
import com.oracle.bdglue.encoder.EventHeader;
import com.oracle.bdglue.encoder.EventQueue;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that manages an instance of a Publisher in its own thread.
 */
public class PublisherThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(PublisherThread.class);
    private EventQueue<EventData> inputQueue;  
    private String threadName;
    private BDGluePublisher publisher;
 

    /**
     * Create an instance with the default thread name of "myThread".
     */
    public PublisherThread() {
        super("myThread");
        threadName = "myThread";
        init();
    }

    /**
     * Create an instance with this thread name.
     * 
     * @param string the name of this thread
     */
    public PublisherThread(String string) {
        super(string);
        threadName = string;
        init();
    }
    
    /**
     * (Re)initialize this thread and create the Publisher.
     */
    public void init() {
        inputQueue = new EventQueue<>(threadName, EventQueue.publisherThreadQueueSize);
        publisher = PublisherFactory.publisherFactory();     
        publisher.connect();
    }

    /**
     * Put an event on the inputQueue.
     * 
     * @param evt the event we want to process
     * @throws InterruptedException if we are interrupted while waiting to put the event on
     * the queue for processing
     */
    public void put(EventData evt) throws InterruptedException {
        inputQueue.put(evt);
    }

    /**
     * Run the thread. This runner takes an event from the input queue
     * and delivers it to the Publisher.
     */
    @Override
    public void run() {
        ArrayList<EventData> list = new ArrayList<>(10);
        int rval = 0;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                //evt = inputQueue.take();
                //deliver(evt);
                // block until there is an event to read and put
                // that first element on the list.
                list.add(inputQueue.take());
                // now get everything else that may be queued up
                rval = inputQueue.drainTo(list);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("drained {} elements from input queue.", rval);
                }

                for (EventData evt : list) {
                    deliver(evt);
                }
                list.clear();
            } catch (InterruptedException ie) {
                // clean up and allow thread to exit
                LOG.debug("Thread {} : Interrupted Exception. Cleaning up.", threadName);
                // set the interrupted flag for this thread to be sure we fall out of the loop.
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private void shutdown() {
        EventData evt;
        String oldestRecord = "**unset**";
        
        LOG.info("Thread {} : shutting down PublisherThread", threadName);

        // pause briefly to allow things to drain
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            LOG.trace("shutdown() timer");
        }

        if ((inputQueue.size() != 0)) {
            LOG.warn("shutdown(): Thread {} Input queue HAS NOT been drained. Size={}", 
                     threadName, inputQueue.size());
            try {
                evt = inputQueue.take();
                if (evt.getHeaders().containsKey(EventHeader.POSITION)) {
                    oldestRecord = evt.getHeaders().get(EventHeader.POSITION);
                }
            } catch (InterruptedException e) {
            }
            LOG.warn("shutdown(): Thread {}: Source position of oldest record in queue: {}", 
                     threadName, oldestRecord);
        } else {
            LOG.info("shutdown(): Thread {} Input queue has been drained. Size={}", 
                     threadName, inputQueue.size());
        }
        // shutdown publisher connections, etc.
        publisher.cleanup();

    }
    
    /**
     * Call this method to terminate the thread and exit.
     */
    public void cancel() {
        shutdown();

        interrupt();
    }

    /**
     * Take the input operation event and hand it over
     * to the Publisher. This is called by the thread runner 
     * and as a part of clean up process when the runner is 
     * cancelled.
     * 
     * @param evt the event we want to deliver ot the target
     */
    private void deliver(EventData evt) {
        publisher.writeEvent(threadName, evt);
    }
    
    /**
     * @return a status string for this thread.
     */
    public String status() {
        
        return inputQueue.status();
    }
}
