/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.io.nats.source;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.source.SourceEventListener;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.siddhi.extension.io.nats.util.NATSConstants.SEQUENCE_NUMBER;

/**
 * Process the NATS subject subscription channel in concurrent safe manner.
 */
public class NATSMessageProcessor implements MessageHandler {
    private static final Logger log = Logger.getLogger(NATSMessageProcessor.class);
    private SourceEventListener sourceEventListener;
    private boolean paused;
    private ReentrantLock lock;
    private Condition condition;
    private AtomicInteger messageSequenceTracker;
    private String[] requestedTransportPropertyNames;

    public NATSMessageProcessor(SourceEventListener sourceEventListener, String[] requestedTransportPropertyNames,
                                AtomicInteger messageSequenceTracker) {
        this.sourceEventListener = sourceEventListener;
        this.messageSequenceTracker = messageSequenceTracker;
        this.requestedTransportPropertyNames = requestedTransportPropertyNames.clone();
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    @Override
    public void onMessage(Message msg) {
        if (paused) {
            lock.lock();
            try {
                condition.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
        messageSequenceTracker.incrementAndGet();
        String[] properties = new String[requestedTransportPropertyNames.length];
        for (int i = 0; i < requestedTransportPropertyNames.length; i++) {
            if (requestedTransportPropertyNames[i].equalsIgnoreCase(SEQUENCE_NUMBER)) {
                properties[i] = String.valueOf(messageSequenceTracker.get());
            }
        }
        sourceEventListener.onEvent(msg.getData(), properties);
        try {
            msg.ack();
        } catch (IOException e) {
            String message = new String(msg.getData(), StandardCharsets.UTF_8);
            throw new SiddhiAppRuntimeException("Error occurred while sending the ack for message : " + message
                    + ".Received to the stream: " + sourceEventListener.getStreamDefinition().getId(), e);
        }
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
