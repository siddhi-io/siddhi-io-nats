/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package io.siddhi.extension.io.nats.source.nats;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.siddhi.extension.io.nats.util.NATSConstants.SEQUENCE_NUMBER;

/**
 * NATSCore is to create a nats client to receive message from nats subject.
 */
public class NATSCore {

    private static final Logger log = Logger.getLogger(NATSCore.class);
    protected String destination;
    protected String[] natsUrls;
    protected String queueGroupName;
    protected String siddhiAppName;
    protected String streamId;
    protected Options.Builder natsOptionBuilder;
    protected String authType;
    private Connection natsClient;

    protected SourceEventListener sourceEventListener;
    protected ReentrantLock lock;
    protected Condition condition;
    protected volatile boolean pause;
    protected AtomicInteger messageSequenceTracker;
    protected String[] requestedTransportPropertyNames;

    public StateFactory initiateNatsClient(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                           String[] requestedTransportPropertyNames, ConfigReader configReader,
                                           SiddhiAppContext siddhiAppContext) {
        this.siddhiAppName = siddhiAppContext.getName();
        this.streamId = sourceEventListener.getStreamDefinition().getId();
        this.destination = optionHolder.validateAndGetStaticValue(NATSConstants.DESTINATION);
        this.queueGroupName = optionHolder.validateAndGetStaticValue(NATSConstants.QUEUE_GROUP_NAME, null);
        String serverUrls;
        if (optionHolder.isOptionExists(NATSConstants.BOOTSTRAP_SERVERS)) {
            serverUrls = optionHolder.validateAndGetStaticValue(NATSConstants.BOOTSTRAP_SERVERS);
        } else {
            serverUrls = optionHolder.validateAndGetStaticValue(NATSConstants.SERVER_URLS);
        }
        natsUrls = serverUrls.split(",");
        for (String url: natsUrls) {
            NATSUtils.validateNatsUrl(url, siddhiAppName);
        }
        Properties properties = new Properties();
        if (optionHolder.isOptionExists(NATSConstants.OPTIONAL_CONFIGURATION)) {
            String optionalConfigs = optionHolder.validateAndGetStaticValue(NATSConstants.OPTIONAL_CONFIGURATION);
            NATSUtils.splitHeaderValues(optionalConfigs, properties);
        }
        natsOptionBuilder = new Options.Builder(properties);
        natsOptionBuilder.servers(this.natsUrls);
        if (optionHolder.isOptionExists(NATSConstants.AUTH_TYPE)) {
            authType = optionHolder.validateAndGetStaticValue(NATSConstants.AUTH_TYPE);
            NATSUtils.addAuthentication(optionHolder, natsOptionBuilder, authType, siddhiAppName, streamId);
        }
        this.sourceEventListener = sourceEventListener;
        this.messageSequenceTracker = new AtomicInteger(0);
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.requestedTransportPropertyNames = requestedTransportPropertyNames.clone();
        return null;
    }

    public void createConnection(Source.ConnectionCallback connectionCallback, State state)
            throws ConnectionUnavailableException {
        try {
            natsClient = Nats.connect(natsOptionBuilder.build());
            Dispatcher dispatcher = natsClient.createDispatcher((msg) -> {
                if (pause) {
                    lock.lock();
                    try {
                        condition.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();
                    }
                }
                messageSequenceTracker.incrementAndGet();
                Object[] properties = new String[requestedTransportPropertyNames.length];
                for (int i = 0; i < requestedTransportPropertyNames.length; i++) {
                    if (requestedTransportPropertyNames[i].equalsIgnoreCase(SEQUENCE_NUMBER)) {
                        properties[i] = String.valueOf(messageSequenceTracker.get());
                    }
                }
                sourceEventListener.onEvent(msg.getData(), properties);
            });
            if (queueGroupName != null) {
                dispatcher.subscribe(destination, queueGroupName);
            } else {
                dispatcher.subscribe(destination);
            }
        } catch (IOException e) {
            throw new ConnectionUnavailableException("Error occurred in initializing the NATS receiver in " +
                    siddhiAppName + " for stream: " + streamId);
        } catch (InterruptedException e) {
            throw new SiddhiAppRuntimeException("Error occurred in initializing the NATS receiver in " +
                    siddhiAppName + " for stream: " + streamId + ".The calling thread is interrupted before the " +
                    "connection completes.");
        }
    }

    public void disconnect() {
        if (natsClient != null && natsClient.getStatus() != Connection.Status.CLOSED) {
            try {
                natsClient.close();
            } catch (InterruptedException e) {
                log.error("Error while disconnecting the nats client. Thread was interrupted before closing the " +
                        "connection.");
            } finally {
                natsClient = null;
            }
        }
    }
    public void pause() {
        pause = true;
    }
    public void resume() {
        pause = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
