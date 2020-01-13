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
package io.siddhi.extension.io.nats.sink.nats;

import io.nats.client.Nats;
import io.nats.streaming.ConnectionLostHandler;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.sink.AsyncAckHandler;
import io.siddhi.extension.io.nats.sink.NATSSink;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class which extends NATSCore to create nats streaming client and publish messages to relevant subject.
 */
public class NATSStreaming extends NATSCore {

    private static final Logger log = Logger.getLogger(NATSStreaming.class);
    private StreamingConnection streamingConnection;
    private Options.Builder optionsBuilder;
    private String clusterId;
    private NATSSink natsSink;

    public NATSStreaming(NATSSink sink) {
        this.natsSink = sink;
    }

    @Override
    public void initiateClient(OptionHolder optionHolder, String siddhiAppName, String streamId) {
        super.initiateClient(optionHolder, siddhiAppName, streamId);
        if (optionHolder.isOptionExists(NATSConstants.CLUSTER_ID)) {
            this.clusterId = optionHolder.validateAndGetStaticValue(NATSConstants.CLUSTER_ID);
        } else if (optionHolder.isOptionExists(NATSConstants.STREAMING_CLUSTER_ID)) {
            this.clusterId = optionHolder.validateAndGetStaticValue(NATSConstants.STREAMING_CLUSTER_ID);
        }
        this.clientId = optionHolder.validateAndGetStaticValue(NATSConstants.CLIENT_ID, NATSUtils.createClientId(
                siddhiAppName, streamId));
        this.optionsBuilder = new Options.Builder().clientId(this.clientId).clusterId(this.clusterId).
                connectionLostHandler(new NATSStreaming.NATSConnectionLostHandler());
    }

    @Override
    public void createNATSClient() throws ConnectionUnavailableException {
        try {
            optionsBuilder.natsConn(Nats.connect(natsOptionBuilder.build()));
            StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(
                    optionsBuilder.build());
            streamingConnection = streamingConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new ConnectionUnavailableException("Error in Siddhi App '" + siddhiAppName + "' while connecting to "
                    + "NATS server endpoint " + Arrays.toString(natsUrls) + " at destination: " + destination.getValue()
                    , e);
        } catch (InterruptedException e) {
            throw new ConnectionUnavailableException("Error in Siddhi App '" + siddhiAppName + "' while connecting to" +
                    " NATS server endpoint " + Arrays.toString(natsUrls) + " at destination: " +
                    destination.getValue() + ". The calling thread is interrupted before the connection " +
                    "can be established.", e);
        }
        isConnected.set(true);
    }

    @Override
    public void publishMessages(Object payload, byte[] messageBytes, DynamicOptions dynamicOptions)
            throws ConnectionUnavailableException {
        String subjectName = destination.getValue(dynamicOptions);
        try {
            if (!isConnected.get()) {
                if (streamingConnection != null) {
                    streamingConnection.close();
                }
                createNATSClient(); // this method is only use to create the client
            }
            streamingConnection.publish(subjectName, messageBytes,
                    new AsyncAckHandler(siddhiAppName, natsUrls, payload, natsSink, dynamicOptions));
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Error sending message to destination:" + subjectName, e);
        } catch (InterruptedException e) {
            throw new SiddhiAppRuntimeException("Error sending message to destination:" + subjectName
                    + ".The calling thread is interrupted before the call completes.", e);
        } catch (TimeoutException e) {
            throw new SiddhiAppRuntimeException("Error sending message to destination:" + subjectName
                    + ".Timeout occured while trying to ack.", e);
        }
    }

    @Override
    public void disconnect() {
        if (streamingConnection != null) {
            try {
                streamingConnection.close();
            } catch (IOException | TimeoutException | InterruptedException e) {
                log.error("Error disconnecting the Stan receiver in Siddhi App '" + siddhiAppName +
                        "' when publishing messages to NATS endpoint " + Arrays.toString(natsUrls) + " . " +
                        e.getMessage(), e);
            }
        }
    }

    class NATSConnectionLostHandler implements ConnectionLostHandler {

        @Override
        public void connectionLost(StreamingConnection streamingConnection, Exception e) {
            log.error("Exception occurred in Siddhi" + " App '" + siddhiAppName + "' when publishing messages " +
                    "to NATS endpoints " + Arrays.toString(natsUrls) + " . " + e.getMessage(), e);
            isConnected = new AtomicBoolean(false);
        }
    }
}


