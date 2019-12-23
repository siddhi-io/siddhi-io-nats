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
import io.siddhi.extension.io.nats.sink.nats.AbstractNats;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * NATS output transport(Handle the publishing process) class.
 */
@Extension(
        name = "nats",
        namespace = "sink",
        description = "NATS Sink allows users to subscribe to a Nats or Nats streaming broker and publish messages.",
        parameters = {
                @Parameter(name = NATSConstants.DESTINATION,
                        description = "Subject name which NATS sink should publish to.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(name = NATSConstants.BOOTSTRAP_SERVERS,
                        description = "Deprecated, use `server.urls` instead, The NATS based urls of the NATS server." +
                                " Can be provided multiple urls separated by commas(`,`).",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = NATSConstants.DEFAULT_SERVER_URL
                ),
                @Parameter(name = NATSConstants.SERVER_URLS,
                        description = "The NATS based urls of the NATS server. Can be provided multiple urls " +
                                "separated by commas(`,`).",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = NATSConstants.DEFAULT_SERVER_URL
                ),
                @Parameter(name = NATSConstants.CLIENT_ID,
                        description = "The identifier of the client publishing/connecting to the NATS streaming " +
                                "broker. Should be unique for each client connecting to the server/cluster." +
                                "(supported only for nats streaming connections).",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "None"
                ),
                @Parameter(name = NATSConstants.CLUSTER_ID,
                        description = "Deprecated, use `" + NATSConstants.STREAMING_CLUSTER_ID + "` instead. The " +
                                "identifier of the NATS server/cluster. Should be provided when using nats " +
                                "streaming broker.",
                        type = DataType.STRING
                ),
                @Parameter(name = NATSConstants.STREAMING_CLUSTER_ID,
                        description = "The identifier of the NATS server/cluster. Should be provided when using nats " +
                                "streaming broker",
                        type = DataType.STRING
                ),
                @Parameter(name = NATSConstants.CONNECTION_TIME_OUT,
                        description = "Configure the connection time out in seconds.",
                        optional = true,
                        type = DataType.LONG,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.PING_INTERVAL,
                        description = "Configure the ping interval in seconds.",
                        optional = true,
                        type = DataType.LONG,
                        defaultValue = "-"

                ),
                @Parameter(name = NATSConstants.MAX_PING_OUTS,
                        description = "Configure the no of pings than can be cached.",
                        optional = true,
                        type = DataType.INT,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.MAX_RETRY_ATTEMPTS,
                        description = "Configure the no of retry attempts.",
                        optional = true,
                        type = DataType.INT,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.RECONNECT_WAIT,
                        description = "Set the no of seconds that should wait before the next reconnect attempt.",
                        optional = true,
                        type = DataType.LONG,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.RETRY_BUFFER_SIZE,
                        description = "Configure the buffer size in bytes",
                        optional = true,
                        type = DataType.LONG,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.AUTH_TYPE,
                        description = "Set the authentication type. Should be provided when using secure connection." +
                                " Supported authentication types: `user, token, tls`",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.USERNAME,
                        description = "Set the username, should be provided if `auth.type` is set as `user`",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.PASSWORD,
                        description = "Set the password, should be provided if `auth.type` is set as `user`",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.TOKEN,
                        description = "Set the token, should be provided if `auth.type` is set as `token`",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.TRUSTSTORE_FILE,
                        description = "Configure the truststore file",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "`${carbon.home}/resources/security/client-truststore.jks`"
                ),
                @Parameter(name = NATSConstants.STORE_TYPE,
                        description = "TLS store type.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "JKS"
                ),
                @Parameter(name = NATSConstants.TRUSTSTORE_PASSWORD,
                        description = "The password for the client truststore",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "wso2carbon"
                ),
                @Parameter(name = NATSConstants.TRUSTSTORE_ALGORITHM,
                        description = "The encryption algorithm of the truststore.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "SunX509"
                ),
                @Parameter(name = NATSConstants.CLIENT_VERIFY,
                        description = "Enable the client verification, should be set to `true` if client needs to be " +
                                "verify by the server.",
                        optional = true,
                        type = DataType.BOOL,
                        defaultValue = "false"
                ),
                @Parameter(name = NATSConstants.KEYSTORE_FILE,
                        description = "Configure the Keystore file, only if client verification is needed.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "`${carbon.home}/resources/security/wso2carbon.jks`"
                ),
                @Parameter(name = NATSConstants.KEYSTORE_ALGORITHM,
                        description = "The encryption algorithm of the keystore.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "SunX509"
                ),
                @Parameter(name = NATSConstants.KEYSTORE_PASSWORD,
                        description = "The password for the keystore.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "wso2carbon"
                ),
        },
        examples = {
                @Example(syntax = "@sink(type='nats', @map(type='xml'), "
                        + "destination='SP_NATS_OUTPUT_TEST', "
                        + "server.urls='nats://localhost:4222',"
                        + "client.id='nats_client',"
                        + "streaming.cluster.id='test-cluster'"
                        + ")\n"
                        + "define stream outputStream (name string, age int, country string);",
                        description = "This example shows how to publish events to a `nats streaming` broker with " +
                                "basic configurations. Here the nats sink will publish events into the " +
                                "`SP_NATS_OUTPUT_TEST` subject. Nats streaming server should be runs on the " +
                                "`localhost:4222` address. `streaming.cluster.id` should be provided if wer want to " +
                                "publish events into a nats streaming broker."
                ),
                @Example(syntax = "@sink(type='nats', @map(type='xml'), "
                                + "destination='nats-test1', "
                                + "server.urls='nats://localhost:4222')\n"
                                + "define stream inputStream (name string, age int, country string)",
                        description = "This example shows how to publish events into a nats broker with basic " +
                                "configurations. Nats server should be running on `localhost:4222` and this sink will" +
                                " publish events to the `nats-test1` subject."
                ),
                @Example(syntax = "@sink(type='nats',@map(type='protobuf', class='io.siddhi.extension.io.nats.utils."
                                + "protobuf.Person'),\n "
                                + "destination='nats-test1', "
                                + "server.urls='nats://localhost:4222')\n"
                                + "define stream inputStream (nic long, name string)",
                        description = "Above query shows how to use nats sink to publish protobuf messages into a " +
                                "nats broker."
                )
        }
)

public class NATSSink extends Sink {
    private static final Logger log = Logger.getLogger(NATSSink.class);
    private String siddhiAppName;
    private AbstractNats nats;

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
        nats = AbstractNats.getNats(optionHolder);
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
            throw new ConnectionUnavailableException("Error in Siddhi App " + siddhiAppName + " while connecting to " +
                    "NATS server endpoint " + Arrays.toString(nats.getNatsUrl()) + " at destination: " +
                    nats.getDestination(), e);
        } catch (InterruptedException e) {
            throw new ConnectionUnavailableException("Error in Siddhi App " + siddhiAppName + " while connecting to" +
                    " NATS server endpoint " + Arrays.toString(nats.getNatsUrl()) + " at destination: " +
                    nats.getDestination().getValue() + ". The calling thread is interrupted before the connection " +
                    "can be established.", e);
        }
    }

    @Override
    public void disconnect() {
        try {
            nats.disconnect();
        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("Error disconnecting the Stan receiver in Siddhi App " + siddhiAppName +
                    " when publishing messages to NATS endpoint " + Arrays.toString(nats.getNatsUrl()) + " . " +
                    e.getMessage(), e);
        }
    }

    @Override
    public void destroy() {

    }

}

