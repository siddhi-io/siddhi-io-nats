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
package io.siddhi.extension.io.nats.sink;

import io.nats.streaming.ConnectionLostHandler;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.sink.exception.NATSSinkAdaptorRuntimeException;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NATS output transport(Handle the publishing process) class.
 */
@Extension(
        name = "nats",
        namespace = "sink",
        description = "NATS Sink allows users to subscribe to a NATS broker and publish messages.",
        parameters = {
                @Parameter(name = NATSConstants.DESTINATION,
                        description = "Subject name which NATS sink should publish to.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(name = NATSConstants.BOOTSTRAP_SERVERS,
                        description = "The NATS based url of the NATS server.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = NATSConstants.DEFAULT_SERVER_URL
                ),
                @Parameter(name = NATSConstants.CLIENT_ID,
                        description = "The identifier of the client publishing/connecting to the NATS broker. Should " +
                                "be unique for each client connecting to the server/cluster.",
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
        },
        examples = {
                @Example(description = "This example shows how to publish to a NATS subject with all supporting "
                        + "configurations. With the following configuration the sink identified as 'nats-client' will "
                        + "publish to a subject named as 'SP_NATS_OUTPUT_TEST' which resides in a nats instance with "
                        + "a cluster id of 'test-cluster', running in localhost and listening to the port 4222 for "
                        + "client connection.",
                        syntax = "@sink(type='nats', @map(type='xml'), "
                                + "destination='SP_NATS_OUTPUT_TEST', "
                                + "bootstrap.servers='nats://localhost:4222',"
                                + "client.id='nats_client',"
                                + "server.id='test-cluster'"
                                + ")\n"
                                + "define stream outputStream (name string, age int, country string);"),

                @Example(description = "This example shows how to publish to a NATS subject with mandatory "
                        + "configurations. With the following configuration the sink identified with an auto generated "
                        + "client id will publish to a subject named as 'SP_NATS_OUTPUT_TEST' which resides in a "
                        + "nats instance with a cluster id of 'test-cluster', running in localhost and listening to "
                        + "the port 4222 for client connection.",
                        syntax = "@sink(type='nats', @map(type='xml'), "
                                + "destination='SP_NATS_OUTPUT_TEST')\n"
                                + "define stream outputStream (name string, age int, country string);")
        }
)

public class NATSSink extends Sink {
    private static final Logger log = Logger.getLogger(NATSSink.class);
    private StreamingConnection streamingConnection;
    private OptionHolder optionHolder;
    private StreamDefinition streamDefinition;
    private Option destination;
    private String clusterId;
    private String clientId;
    private String natsUrl;
    private String siddhiAppName;
    private AtomicBoolean isConnectionClosed = new AtomicBoolean(false);

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    @Override protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
            return new String[]{NATSConstants.DESTINATION};
    }

    @Override
    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
                                SiddhiAppContext siddhiAppContext) {
        this.siddhiAppName = siddhiAppContext.getName();
        this.optionHolder = optionHolder;
        this.streamDefinition = streamDefinition;
        validateAndInitNatsProperties();
        return null;
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state) throws
                                                                                   ConnectionUnavailableException  {
        String message = (String) payload;
        String subjectName = destination.getValue();
        try {
            if (isConnectionClosed.get()) {
                streamingConnection.close();
                connect();
            }
            streamingConnection.publish(subjectName, message.getBytes(StandardCharsets.UTF_8),
                    new AsyncAckHandler(siddhiAppName, natsUrl, payload, this, dynamicOptions));
        } catch (IOException e) {
            log.error("Error sending message to destination: " + subjectName);
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName, e);
        } catch (InterruptedException e) {
            log.error("Error sending message to destination: " + subjectName + ".The calling thread is "
                    + "interrupted before the call completes.");
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName
                    + ".The calling thread is interrupted before the call completes.", e);
        } catch (TimeoutException e) {
            log.error("Error sending message to destination: " + subjectName + ".Timeout occured while trying to ack.");
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName
                    + ".Timeout occured while trying to ack.", e);
        }
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        try {
            Options options = new Options.Builder().natsUrl(this.natsUrl).
                    clientId(this.clientId).clusterId(this.clusterId).
                    connectionLostHandler(new NATSSink.NATSConnectionLostHandler()).build();
            StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(options);
            streamingConnection = streamingConnectionFactory.createConnection();
            isConnectionClosed.set(false);
        } catch (IOException e) {
            String errorMessage = "Error in Siddhi App " + siddhiAppName + " while connecting to NATS server " +
                    "endpoint " + natsUrl + " at destination: " + destination.getValue();
            log.error(errorMessage);
            throw new ConnectionUnavailableException(errorMessage, e);
        } catch (InterruptedException e) {
            String errorMessage = "Error in Siddhi App " + siddhiAppName + " while connecting to NATS server " +
                    "endpoint " + natsUrl + " at destination: " + destination.getValue() + ". The calling thread is " +
                    "interrupted before the connection can be established.";
            log.error(errorMessage);
            throw new ConnectionUnavailableException(errorMessage, e);
        }
    }

    @Override
    public void disconnect() {
        try {
            if (streamingConnection != null) {
                streamingConnection.close();
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("Error disconnecting the Stan receiver in Siddhi App " + siddhiAppName +
                    " when publishing messages to NATS endpoint " + natsUrl + " . " + e.getMessage(), e);
        }
    }

    @Override
    public void destroy() {

    }

    private void validateAndInitNatsProperties() {
        this.destination = optionHolder.validateAndGetOption(NATSConstants.DESTINATION);
        this.clusterId = optionHolder.validateAndGetStaticValue(NATSConstants.CLUSTER_ID, NATSConstants
                .DEFAULT_CLUSTER_ID);
        this.clientId = optionHolder.validateAndGetStaticValue(NATSConstants.CLIENT_ID, NATSUtils.createClientId());
        this.natsUrl = optionHolder.validateAndGetStaticValue(NATSConstants.BOOTSTRAP_SERVERS,
                NATSConstants.DEFAULT_SERVER_URL);

        NATSUtils.validateNatsUrl(natsUrl, streamDefinition.getId());
    }

    class NATSConnectionLostHandler implements ConnectionLostHandler {
        @Override
        public void connectionLost(StreamingConnection streamingConnection, Exception e) {
            log.error("Exception occurred in Siddhi App " + siddhiAppName +
                    " when publishing messages to NATS endpoint " + natsUrl + " . " + e.getMessage(), e);
            isConnectionClosed = new AtomicBoolean(true);
        }
    }
}

