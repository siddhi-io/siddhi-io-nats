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

import com.google.protobuf.GeneratedMessageV3;
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
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.sink.nats.NATS;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.io.IOException;
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
    private String siddhiAppName;
    private NATS nats;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, Map.class, GeneratedMessageV3.class};
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
        if (optionHolder.isOptionExists(NATSConstants.CLUSTER_ID) || optionHolder.isOptionExists(
                NATSConstants.STREAMING_CLUSTER_ID)) {
            nats = NATS.getNats(true);
            nats.setNatsSink(this);
        } else {
            nats = NATS.getNats(false);
        }
        nats.initiateClient(optionHolder, siddhiAppName, streamDefinition.getId());
        return null;
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state) throws
            ConnectionUnavailableException  {
            nats.publishMessages(payload, dynamicOptions, state);
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        try {
            nats.createNATSClient();
        } catch (IOException e) {
            String errorMessage = "Error in Siddhi App " + siddhiAppName + " while connecting to NATS server " +
                    "endpoint " + nats.getNatsUrl()[0] + " at destination: " + nats.getDestination();
            log.error(errorMessage);
            throw new ConnectionUnavailableException(errorMessage, e);
        } catch (InterruptedException e) {
            String errorMessage = "Error in Siddhi App " + siddhiAppName + " while connecting to NATS server " +
                    "endpoint " + nats.getNatsUrl()[0] + " at destination: " + nats.getDestination().getValue() +
                    ". The calling thread is interrupted before the connection can be established.";
            log.error(errorMessage);
            throw new ConnectionUnavailableException(errorMessage, e);
        }
    }

    @Override
    public void disconnect() {
        try {
            nats.disconnect();
        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("Error disconnecting the Stan receiver in Siddhi App " + siddhiAppName +
                    " when publishing messages to NATS endpoint " + nats.getNatsUrl()[0] + " . " + e.getMessage(), e);
        }
    }

    @Override
    public void destroy() {

    }

}

