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
package org.wso2.extension.siddhi.io.nats.sink;

import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.nats.sink.exception.NATSSinkAdaptorRuntimeException;
import org.wso2.extension.siddhi.io.nats.util.NATSConstants;
import org.wso2.extension.siddhi.io.nats.util.NATSUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeoutException;

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

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
            return new String[]{NATSConstants.DESTINATION};
    }

    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
            SiddhiAppContext siddhiAppContext) {
        this.optionHolder = optionHolder;
        this.streamDefinition = streamDefinition;
        validateAndInitNatsProperties();
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions) {
        String message = (String) payload;
        String subjectName = destination.getValue();
        try {
            streamingConnection.publish(subjectName, message.getBytes(StandardCharsets.UTF_8), new AsyncAckHandler());
        } catch (IOException e) {
            log.error("Error sending message to destination: " + subjectName);
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName, e);
        } catch (InterruptedException e) {
            log.error("Error sending message to destination: " + subjectName +  ".The calling thread is "
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
        StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(this.clusterId,
                this.clientId);
        streamingConnectionFactory.setNatsUrl(this.natsUrl);

        try {
            streamingConnection =  streamingConnectionFactory.createConnection();
        } catch (IOException e) {
            log.error("Error while connecting to NATS server at destination: " + destination.getValue());
            throw new ConnectionUnavailableException("Error while connecting to NATS server at destination: "
                    + destination.getValue(), e);
        } catch (InterruptedException e) {
            log.error("Error while connecting to NATS server at destination: " + destination.getValue() +
                              ".The calling thread is interrupted before the connection can be established.");
            throw new ConnectionUnavailableException("Error while connecting to NATS server at destination: "
                    + destination.getValue() + " .The calling thread is interrupted before the connection can be "
                                                             + "established.", e);
        }
    }

    @Override
    public void disconnect() {
        try {
            if (streamingConnection != null) {
                streamingConnection.close();
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("Error disconnecting the Stan receiver", e);
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public Map<String, Object> currentState() {
            return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

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
}

