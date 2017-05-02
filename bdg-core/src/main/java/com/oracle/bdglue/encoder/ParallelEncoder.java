/* ./src/main/java/com/oracle/bdglue/encoder/ParallelEncoder.java
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


import com.oracle.bdglue.BDGluePropertyValues;
import com.oracle.bdglue.common.PropertyManagement;
import com.oracle.bdglue.meta.transaction.DownstreamOperation;
import com.oracle.bdglue.publisher.ParallelPublisher;

import com.oracle.gghadoop.GG12Handler;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that manages the encoder thread pool, and itself runs as a thread.
 */
public class ParallelEncoder extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelEncoder.class);
    private static final String encoderName = "Encoder-main";


    private PropertyManagement properties = null;

    private ParallelPublisher publisher;

    private int numEncoders;
    private ArrayList<EncoderThread> encoders;
    private int currentEncoder = 0;
    private int opCount = 0;

    /**
     * construct an encoder with the number of
     * threads specified in the properties for processing.
     */
    public ParallelEncoder() {
        super(encoderName);
        // Initialize the properties for the application
        properties =
            PropertyManagement.getProperties(BDGluePropertyValues.defaultProperties,
                                             BDGluePropertyValues.externalProperties);

        // create an instance of the class that will serialize
        // the DB operations
        numEncoders =
            properties.asInt(BDGluePropertyValues.ENCODER_THREADS, BDGluePropertyValues.ENCODER_THREADS_DEFAULT);

        init();
    }

    /**
     * Construct an encoder with the specified number
     * of threads for processing.
     *
     * @param numEncoders the number of encoders to create
     */
    public ParallelEncoder(int numEncoders) {
        super(encoderName);
        // Initialize the properties for the application
        properties =
            PropertyManagement.getProperties(BDGluePropertyValues.defaultProperties,
                                             BDGluePropertyValues.externalProperties);


        // override any property value that may have been specified
        this.numEncoders = numEncoders;
        init();
    }

    /**
     * Initializes the ecoder with the specified number
     * of Threads, etc.
     */
    public void init() {
        EncoderThread encoder;
        String name;

        publisher = new ParallelPublisher();
        publisher.start();

        encoders = new ArrayList<>(numEncoders);
        for (int i = 0; i < numEncoders; i++) {
            name = "Encoder-" + i;
            encoder = new EncoderThread(name);
            encoder.start();
            encoders.add(encoder);
        }

    }


    /**
     * Puts the operation on the input queue for subsequent processing.
     *
     * @param op the operation we are giving to a thread to encode
     * @throws InterruptedException if interrupted while waiting to put an operation on the queue
     */
    public void put(DownstreamOperation op) throws InterruptedException {
        LOG.debug("put(): operation on encoder: {}", currentEncoder);
        opCount++;
        EncoderThread encoder = encoders.get(currentEncoder);
        encoder.put(op);
        currentEncoder++;
        if (currentEncoder >= encoders.size())
            currentEncoder = 0;
    }

    /**
     * This is the thread that harvests the encoded events
     * from each of the encoders' queues and sends them
     * onward for publishing.
     *
     */
    @Override
    public void run() {

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // this approach ensures that order of operation is maintained
                // until the data gets to the PublisherThreads, and even then 
                // order of operation is maintained within the threads.
                for (EncoderThread encoder : encoders) {
                    sendToPublisher(encoder);
                }
            } catch (InterruptedException ie) {
                // clean up and allow thread to exit
                LOG.debug("Thread.run : Interrupted Exception. Cleaning up ParallelEncoder.");
                // Set the interrupted flag for this thread.
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Thread is getting ready to exit. Clean up other threads.
     */
    private void shutdown() {
        LOG.info("Thread {} : shutting down ParallelEncoder", encoderName); 
        // We have stopped adding events to the encoder input queues.
        // Pause briefly to hopefully allow things to drain
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            LOG.trace("shutdown() timer");
        }
        // clean up the encoders first while the publishers continue to process
        for (EncoderThread encoder : encoders) {
            encoder.cancel();
        }
        // now wait for the threads to finish
        for (EncoderThread encoder : encoders) {
            try {
                encoder.join(1000);
            } catch (InterruptedException e) {
                LOG.warn("shutdown: join of encoder thread interrupted");
            }
        }
        publisher.cancel();
        try {
            publisher.join(1000);
        } catch (InterruptedException e) {
            LOG.warn("shutdown: join of publisher thread interrupted");
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
     * Pull the next event off of the specified encoder and
     * send it downstream for publishing.
     *
     * @param encoder the encoder we are working with
     * @throws InterruptedException if we are interrupted while wating to
     * take an event or put it on the publisher's queue
     */
    public void sendToPublisher(EncoderThread encoder) throws InterruptedException {
        // Have to do this one "take" at a time vs. "drainTo" because we have to be
        // sure that order of operation is maintained through this process. Each
        // call is for the "next" EncoderThread in the list.
        EventData evt = encoder.take();

        publisher.put(evt);

    }

    /**
     * Generates the status of the encoder as a String.
     *
     * @return the status as a string
     */
    public String status() {
        StringBuilder rval = new StringBuilder();
        String tstrg;

        tstrg = String.format("*** %s: Total operations(%d)%n", encoderName, opCount);
        rval.append(tstrg);
        for (EncoderThread encoder : encoders) {
            rval.append(encoder.status());
        }
        rval.append(publisher.status());

        return rval.toString();
    }

    /**
     * Static function that decouples downstream callers from the upstream handler.
     * It simply forwards the message along to the upstream handler.
     * 
     * @param msg a message to log
     */
    public static void forceShutdown(String msg) {
        GG12Handler.shutdown(msg);
    }

}
