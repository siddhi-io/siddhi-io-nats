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

import com.google.protobuf.GeneratedMessageV3;
import io.nats.streaming.ConnectionLostHandler;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.source.exception.NATSInputAdaptorRuntimeException;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

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
                                + "define stream inputStream (name string, age int, country string);"),
                @Example(description = "This example shows how to pass NATS Streaming sequence number to the event.",
                        syntax = "@source(type='nats', @map(type='json', @attributes(name='$.name', age='$.age', " +
                                "country='$.country', sequenceNum='trp:sequenceNumber')), " +
                                "destination='SIDDHI_NATS_SOURCE_TEST_DEST', " +
                                "client.id='nats_client', " +
                                "bootstrap.servers='nats://localhost:4222', " +
                                "cluster.id='test-cluster'" +
                                ")\n" +
                                "define stream inputStream (name string, age int, country string, sequenceNum string);")
        }
)

public class NATSSource extends Source {
/*    private static final Logger log = Logger.getLogger(NATSSource.class);
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
    private String siddhiAppName;
    private String[] reqTransportPropertyNames;*/

    private NATS nats;

    @Override
    public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                             String[] requestedTransportPropertyNames, ConfigReader configReader,
                             SiddhiAppContext siddhiAppContext) {
        if (optionHolder.isOptionExists("cluster.id") || optionHolder.isOptionExists(
                "streaming.cluster.id")) {
            nats = NATS.getNATS(true);
        } else {
            nats = NATS.getNATS(false);
        }
        return nats.initiateNatsClient(sourceEventListener,optionHolder,requestedTransportPropertyNames,configReader,
                siddhiAppContext);
        /*this.sourceEventListener = sourceEventListener;
        this.optionHolder = optionHolder;
        this.siddhiAppName = siddhiAppContext.getName();
        this.reqTransportPropertyNames = requestedTransportPropertyNames.clone();
        initNATSProperties();
        return NATSSourceState::new;*/
    }

    @Override protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, Map.class, GeneratedMessageV3.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, State natsSourceState)
            throws ConnectionUnavailableException {
        nats.createConnection(connectionCallback, natsSourceState);

        /*try {
            Options options = new Options.Builder().natsUrl(this.natsUrl).
                    clientId(this.clientId).clusterId(this.clusterId).
                    connectionLostHandler(new NATSConnectionLostHandler(connectionCallback)).build();
            StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(options);
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
        subscribe(natsSourceState);*/
    }

    @Override
    public void disconnect() {
        nats.disconnect();
        /*try {
            if (streamingConnection != null) {
                streamingConnection.close();
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("Error disconnecting the Stan receiver", e);
        }*/
    }

    @Override
    public void destroy() {

    }

    @Override
    public void pause() {
        nats.pause();
        /*if (natsMessageProcessor != null) {
            natsMessageProcessor.pause();
            if (log.isDebugEnabled()) {
                log.debug("Nats source paused for destination: " + destination);
            }
        }*/
    }

    @Override
    public void resume() {
        nats.resume();
        /*if (natsMessageProcessor != null) {
            natsMessageProcessor.resume();
            if (log.isDebugEnabled()) {
                log.debug("Nats source resumed for destination: " + destination);
            }
        }*/
    }

    /*private void subscribe(NATSSourceState natsSourceState) {
        SubscriptionOptions.Builder subscriptionOptionsBuilder = new SubscriptionOptions.Builder();
        if (sequenceNumber != null && natsSourceState.lastSentSequenceNo.intValue() <
                Integer.parseInt(sequenceNumber)) {
            natsSourceState.lastSentSequenceNo.set(Integer.parseInt(sequenceNumber));
        }
        subscriptionOptionsBuilder.startAtSequence(natsSourceState.lastSentSequenceNo.get());
        try {

            if (durableName != null) {
                subscriptionOptionsBuilder.durableName(durableName);
            }
            natsMessageProcessor = new NATSMessageProcessor(sourceEventListener, reqTransportPropertyNames,
                    natsSourceState.lastSentSequenceNo);
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
    }*/

    /*private void initNATSProperties() {
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
    }*/

    /*class NATSSourceState extends State {
        private AtomicInteger lastSentSequenceNo = new AtomicInteger(0);

        @Override public boolean canDestroy() {
            return lastSentSequenceNo.intValue() == 0;
        }

        *//**
         * Used to serialize and persist {@link #lastSentSequenceNo} in a configurable interval.
         * @return stateful objects of the processing element as a map
         *//*
        @Override public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put(siddhiAppName, lastSentSequenceNo.get());
            return state;
        }

        *//**
         * Used to get the persisted {@link #lastSentSequenceNo} value in case of client connection failure so that
         * replay the missing messages/events.
         * @param map the stateful objects of the processing element as a map.
         *//*
        @Override public void restore(Map<String, Object> map) {
            Object seqObject = map.get(siddhiAppName);
            if (seqObject != null && sequenceNumber == null) {
                lastSentSequenceNo.set((int) seqObject);
            }
        }
    }*/

    /*class NATSConnectionLostHandler implements ConnectionLostHandler {
        private ConnectionCallback connectionCallback;

        NATSConnectionLostHandler(ConnectionCallback connectionCallback) {
            this.connectionCallback = connectionCallback;
        }

        @Override
        public void connectionLost(StreamingConnection streamingConnection, Exception e) {
            log.error("Exception occurred in Siddhi App" + siddhiAppName +
                    " when consuming messages from NATS endpoint " + natsUrl + " . " + e.getMessage(), e);
            Thread thread = new Thread() {
                public void run() {
                    connectionCallback.onError(new ConnectionUnavailableException(e));
                }
            };

            thread.start();
        }
    }*/
}


