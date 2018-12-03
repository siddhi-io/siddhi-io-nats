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

import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.nats.source.exception.NATSInputAdaptorRuntimeException;
import org.wso2.extension.siddhi.io.nats.util.NATSConstants;
import org.wso2.extension.siddhi.io.nats.util.NATSUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 */
@Extension(
        name = "nats",
        namespace = "source",
        description = "NATS Source allows users to subscribe to a NATS broker and receive messages. It has the "
                + "ability to receive all the message types supported by NATS.",
        parameters = {
                @Parameter(name = NATSConstants.DESTINATION,
                        description = "Subject name which NATS Source should subscribe to.",
                        type = DataType.STRING
                ),
                @Parameter(name = NATSConstants.BOOTSTRAP_SERVERS,
                        description = "The NATS based url of the NATS server.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = NATSConstants.DEFAULT_SERVER_URL
                ),
                @Parameter(name = NATSConstants.CLIENT_ID,
                        description = "The identifier of the client subscribing/connecting to the NATS broker.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "None"
                ),
                @Parameter(name = NATSConstants.CLUSTER_ID,
                        description = "The identifier of the NATS server/cluster.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = NATSConstants.DEFAULT_CLUSTER_ID
                ),
                @Parameter(name = NATSConstants.QUEUE_GROUP_NAME,
                        description = "This can be used when there is a requirement to share the load of a NATS "
                                + "subject. Clients belongs to the same queue group share the subscription load." ,
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "None"
                ),
                @Parameter(name = NATSConstants.DURABLE_NAME,
                        description = "This can be used to subscribe to a subject from the last acknowledged message "
                                + "when a client or connection failure happens. The client can be uniquely identified "
                                + "using the tuple (" + NATSConstants.CLIENT_ID + ", " + NATSConstants.DURABLE_NAME
                                + ").",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "None"
                ),
                @Parameter(name = NATSConstants.SUBSCRIPTION_SEQUENCE,
                        description = "This can be used to subscribe to a subject from a given number of message "
                                + "sequence. All the messages from the given point of sequence number will be passed to"
                                + " the client. If not provided then the either the persisted value or 0 will be used.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "None"
                )

        },
        examples = {
                @Example(description = "This example shows how to subscribe to a NATS subject with all supporting "
                        + "configurations.With the following configuration the source identified as 'nats-client' will "
                        + "subscribes to a subject named as 'SP_NATS_INPUT_TEST' which resides in a nats instance "
                        + "with a cluster id of 'test-cluster', running in localhost and listening to the port 4222 for"
                        + " client connection. This subscription will receive all the messages from 100th in the "
                        + "subject.",
                        syntax = "@source(type='nats', @map(type='text'), "
                                + "destination='SP_NATS_INPUT_TEST', "
                                + "bootstrap.servers='nats://localhost:4222',"
                                + "client.id='nats_client',"
                                + "server.id='test-cluster',"
                                + "queue.group.name = 'group_nats',"
                                + "durable.name = 'nats-durable',"
                                + "subscription.sequence = '100'"
                                + ")\n"
                                + "define stream inputStream (name string, age int, country string);"),

                @Example(description = "This example shows how to subscribe to a NATS subject with mandatory "
                        + "configurations.With the following configuration the source identified with an auto generated"
                        + " client id will subscribes to a subject named as 'SP_NATS_INTPUT_TEST' which resides in a "
                        + "nats instance with a cluster id of 'test-cluster', running in localhost and listening to"
                        + " the port 4222 for client connection. This will receive all available messages in the "
                        + "subject",
                        syntax = "@source(type='nats', @map(type='text'), "
                                + "destination='SP_NATS_INPUT_TEST', "
                                + ")\n"
                                + "define stream inputStream (name string, age int, country string);")
        }
)

public class NATSSource extends Source {
    private static final Logger log = Logger.getLogger(NATSSource.class);
    private SourceEventListener sourceEventListener;
    private OptionHolder optionHolder;
    private StreamingConnection streamingConnection;
    private String destination;
    private String clusterId;
    private String clientId;
    private String natsUrl;
    private String queueGroupName;
    private String durableName;
    private String  sequenceNumber;
    private Subscription subscription;
    private NATSMessageProcessor natsMessageProcessor;
    private static final AtomicInteger lastSentSequenceNo = new AtomicInteger(1);
    private String siddhiAppName;
    /**
     * The initialization method for {@link Source}, will be called before other methods. Validates and initiates the
     * NATS properties and other required fields.
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     *                            Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        ConfigReader is used to read the {@link Source} related system configuration.
     * @param siddhiAppContext    the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get Siddhi
     *                            related utility functions.
     */
    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.optionHolder = optionHolder;
        this.natsMessageProcessor = new NATSMessageProcessor(sourceEventListener, lastSentSequenceNo);
        this.siddhiAppName = siddhiAppContext.getName();
        initNATSProperties();
    }

    /**
     * Returns the list of classes which NATS source can output.
     * @return Array of classes that will be output by the source.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, Map.class};
    }

    /**
     * Initially Called to connect to the NATS server for start retrieving the messages asynchronously .
     * @param connectionCallback Callback to pass the ConnectionUnavailableException in case of connection failure after
     *                           initial successful connection. (can be used when events are receiving asynchronously)
     * @throws ConnectionUnavailableException if it cannot connect to the source backend immediately.
     */
    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        try {
            StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(this.clusterId,
                    this.clientId);
            streamingConnectionFactory.setNatsUrl(this.natsUrl);
            streamingConnection =  streamingConnectionFactory.createConnection();
        } catch (IOException e) {
            log.error("Error while connecting to NATS server at destination: " + destination);
            throw new ConnectionUnavailableException("Error while connecting to NATS server at destination: "
                    + destination, e);
        } catch (InterruptedException e) {
            log.error("Error while connecting to NATS server at destination: " + destination + ".The calling thread "
                    + "is interrupted before the connection can be established.");
            throw new ConnectionUnavailableException("Error while connecting to NATS server at destination: "
                    + destination + " .The calling thread is interrupted before the connection can be established.", e);
        }
        subscribe();
    }

    /**
     * This method can be called when it is needed to disconnect from NATS server.
     */
    @Override
    public void disconnect() {
        lastSentSequenceNo.set(natsMessageProcessor.getMessageSequenceTracker().get());
        try {
            if (subscription != null) {
                subscription.close();
            }
            if (streamingConnection != null) {
                streamingConnection.close();
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("Error disconnecting the Stan receiver", e);
        }
    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}.
     */
    @Override
    public void destroy() {

    }

    /**
     * Called to pause event consumption.
     */
    @Override
    public void pause() {
        natsMessageProcessor.pause();
    }

    /**
     * Called to resume event consumption.
     */
    @Override
    public void resume() {
        natsMessageProcessor.resume();
    }

    /**
     * Used to serialize and persist {@link #lastSentSequenceNo} in a configurable interval.
     * @return stateful objects of the processing element as a map
     */
    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        state.put(siddhiAppName, lastSentSequenceNo.get());
        return state;
    }

    /**
     * Used to get the persisted {@link #lastSentSequenceNo} value in case of client connection failure so that
     * replay the missing messages/events.
     * @param map the stateful objects of the processing element as a map.
     */
    @Override
    public void restoreState(Map<String, Object> map) {
         Object seqObject = map.get(siddhiAppName);
         if (seqObject != null && sequenceNumber == null) {
             lastSentSequenceNo.set((int) seqObject);
         }
    }

    private void subscribe() {
        SubscriptionOptions.Builder subscriptionOptionsBuilder = new SubscriptionOptions.Builder();
        if (sequenceNumber != null) {
            lastSentSequenceNo.set(Integer.parseInt(sequenceNumber));
        }
        subscriptionOptionsBuilder.startAtSequence(lastSentSequenceNo.get());
        try {

            if (durableName != null) {
                subscriptionOptionsBuilder.durableName(durableName);
            }

            if (queueGroupName != null) {
                subscription =  streamingConnection.subscribe(destination , queueGroupName, natsMessageProcessor,
                        subscriptionOptionsBuilder.build());
            } else {
                subscription =  streamingConnection.subscribe(destination , natsMessageProcessor,
                        subscriptionOptionsBuilder.build());
            }

        } catch (IOException e) {
            log.error("Error occurred in initializing the NATS receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId());
            throw new NATSInputAdaptorRuntimeException("Error occurred in initializing the NATS receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId(), e);
        } catch (InterruptedException e) {
            log.error("Error occurred in initializing the NATS receiver for stream: " + sourceEventListener
                    .getStreamDefinition().getId() + ".The calling thread is interrupted before the connection "
                    + "completes.");
            throw new NATSInputAdaptorRuntimeException("Error occurred in initializing the NATS receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId() + ".The calling thread is interrupted before "
                    + "the connection completes.", e);
        } catch (TimeoutException e) {
            log.error("Error occurred in initializing the NATS receiver for stream: " + sourceEventListener
                    .getStreamDefinition().getId() + ".The server request cannot be completed within the subscription"
                    + " timeout.");
            throw new NATSInputAdaptorRuntimeException("Error occurred in initializing the NATS receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId() + ".The server request cannot be completed "
                    + "within the subscription timeout.", e);
        }
    }

    private void initNATSProperties() {
        this.destination = optionHolder.validateAndGetStaticValue(NATSConstants.DESTINATION);
        this.clusterId = optionHolder.validateAndGetStaticValue(NATSConstants.CLUSTER_ID,
                NATSConstants.DEFAULT_CLUSTER_ID);
        this.clientId = optionHolder.validateAndGetStaticValue(NATSConstants.CLIENT_ID, NATSUtils.createClientId());
        this.natsUrl = optionHolder.validateAndGetStaticValue(NATSConstants.BOOTSTRAP_SERVERS,
                NATSConstants.DEFAULT_SERVER_URL);
        if (optionHolder.isOptionExists(NATSConstants.DURABLE_NAME)) {
            this.durableName = optionHolder.validateAndGetStaticValue(NATSConstants.DURABLE_NAME);
        }

        if (optionHolder.isOptionExists(NATSConstants.QUEUE_GROUP_NAME)) {
            this.queueGroupName = optionHolder.validateAndGetStaticValue(NATSConstants.QUEUE_GROUP_NAME);
        }

        if (optionHolder.isOptionExists(NATSConstants.SUBSCRIPTION_SEQUENCE)) {
            this.sequenceNumber = optionHolder.validateAndGetStaticValue(NATSConstants.SUBSCRIPTION_SEQUENCE);
        }
        NATSUtils.validateNatsUrl(natsUrl, sourceEventListener.getStreamDefinition().getId());
    }
}


