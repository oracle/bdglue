/* ./src/main/java/com/oracle/bdglue/encoder/EncoderThread.java 
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

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that manages the individual encoders that get instantiated for the 
 * encoder pool.
 */
public class EncoderThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(EncoderThread.class);
    
    private EventQueue<DownstreamOperation> inputQueue;
    private EventQueue<EventData> outputQueue;
    private String threadName;
    private BDGlueEncoder encoder;

    public EncoderThread() {
        super();
        threadName = "myThread";
        init();
    }

    /**
     * Create an instance with this thread name.
     * 
     * @param string the name of the thread
     */
    public EncoderThread(String string) {
        super(string);
        threadName = string;
        init();
    }
    
    /**
     * Initialize this class.
     */
    private void init() {
        inputQueue = new EventQueue<>(threadName+"-in", 
                                      EventQueue.encoderInputQueueSize);
        outputQueue = new EventQueue<>(threadName+"-out", 
                                       EventQueue.encoderOutputQueueSize);
        encoder = EncoderFactory.encoderFactory();
    }

    /**
     * Put an input operation on the inputQueue.
     * 
     * @param op the operatoin we are processing
     * @throws InterruptedException occurs when this thread is interrupted while waiting to put data on the queue
     */
    public void put(DownstreamOperation op) throws InterruptedException {
        inputQueue.put(op);
    }

    /**
     * Take an event from the outputQueue and return it.
     * 
     * @return an instance of EventData from the queue
     * @throws InterruptedException when this thread is interrupted while waiting for data from the queue
     */
    public EventData take() throws InterruptedException {
        return outputQueue.take();
    }

    /**
     * Take a look at what is next in the queue without removing it, if anything.
     * 
     * @return an instance of EventData from the queue.
     */
    public EventData peek() {
        return outputQueue.peek();
    }

    /**
     * Execute the Encoder thread.
     */
    @Override
    public void run() {
        ArrayList<DownstreamOperation> list = new ArrayList<>(10);
        int rval = 0;
        //DownstreamOperation op = null;
        EventData evt = null;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                
                //op = inputQueue.take();
                // convert the operation into an event
                //evt = sendToEncoder(op);
                //outputQueue.put(evt);
                
                // block until there is an event to read and put
                // that first element on the list.
                list.add(inputQueue.take());
                // now get everything else that may be queued up
                rval = inputQueue.drainTo(list);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("drained {} elements from input queue.", rval);
                }

                for (DownstreamOperation op : list) {
                    // convert the operation into an event and send it on
                    evt = sendToEncoder(op);
                    outputQueue.put(evt);
                }
                list.clear();
                
            } catch (IOException e) {
                LOG.error("run(): IOException. Encoding error.", e);
            } catch (InterruptedException ie) {
                // clean up and allow thread to exit
                LOG.debug("Thread.run {} : Interrupted Exception. Cleaning up.", threadName);
                // set the interrupted flag for this thread to be sure we fall out of the loop.
                Thread.currentThread().interrupt();
            }
        }

    }

    private void shutdown() {
        LOG.info("Thread {} : shutting down EncoderThread", threadName);

        // pause briefly to allow things to drain
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            LOG.trace("shutdown() timer");
        }

        if ((inputQueue.size() != 0) || (outputQueue.size() != 0)) {
            LOG.warn("shutdown(): Thread {} queues HAVE NOT been drained. Input: {} Output: {}", 
                     threadName, inputQueue.size(), outputQueue.size());
        } else {
            LOG.info("shutdown(): Thread {} queues have been drained. Input: {} Output: {}", 
                     threadName, inputQueue.size(), outputQueue.size());
        }

    }

    /**
     * Call this method to terminate the thread and exit.
     */
    public void cancel() {
        shutdown();
        
        interrupt();
    }

    /**
     * Convert the input operation into an event.
     *
     * @param op the operation we are processing
     * @return the encoded event
     * @throws IOException if an encoding error occurs
     */
    public EventData sendToEncoder(DownstreamOperation op) throws IOException {
        EventData rval = encoder.encodeDatabaseOperation(op);
        
        return rval;
    }
    
    /**
     * Get a snapshot of current statistics for this thread.
     * 
     * @return a status string for this thread.
     */
    public String status() {
        StringBuilder rval = new StringBuilder();
        rval.append(inputQueue.status());
        rval.append(outputQueue.status());
        
        return rval.toString();
    }
}
