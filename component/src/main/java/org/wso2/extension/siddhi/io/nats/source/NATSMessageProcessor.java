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
package org.wso2.extension.siddhi.io.nats.source;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.siddhi.core.stream.input.source.SourceEventListener;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.nats.source.exception.NATSInputAdaptorRuntimeException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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

    protected NATSMessageProcessor(SourceEventListener sourceEventListener, AtomicInteger messageSequenceTracker) {
        this.sourceEventListener = sourceEventListener;
        this.messageSequenceTracker = messageSequenceTracker;
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
        sourceEventListener.onEvent(msg.getData(), new String[0]);
        messageSequenceTracker.incrementAndGet();

        try {
            msg.ack();
        } catch (IOException e) {
            String message = new String(msg.getData(), StandardCharsets.UTF_8);
            log.error("Error occurred while sending the ack for message : " + message + ".Received to the stream: "
                    + sourceEventListener.getStreamDefinition().getId());
            throw new NATSInputAdaptorRuntimeException("Error occurred while sending the ack for message : " + message
                    + ".Received to the stream: " + sourceEventListener.getStreamDefinition().getId(), e);
        }
    }

    protected AtomicInteger getMessageSequenceTracker() {
        return messageSequenceTracker;
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
