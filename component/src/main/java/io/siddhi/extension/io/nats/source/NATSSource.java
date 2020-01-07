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
import io.siddhi.extension.io.nats.source.nats.NATSCore;
import io.siddhi.extension.io.nats.source.nats.NATSStreaming;
import io.siddhi.extension.io.nats.util.NATSConstants;

import java.util.Map;

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
                        description = "The identifier of the client subscribing/connecting to the NATS streaming " +
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
                        type = DataType.STRING,
                        defaultValue = "-"
                ),
                @Parameter(name = NATSConstants.STREAMING_CLUSTER_ID,
                        description = "The identifier of the NATS server/cluster. Should be provided when using nats " +
                                "streaming broker",
                        type = DataType.STRING
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
                                + ").[supported only with nats streaming connections]",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "None"
                ),
                @Parameter(name = NATSConstants.SUBSCRIPTION_SEQUENCE,
                        description = "This can be used to subscribe to a subject from a given number of message "
                                + "sequence. All the messages from the given point of sequence number will be passed to"
                                + " the client. If not provided then the either the persisted value or 0 will be " +
                                "used. [supported only with nats streaming connection]",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "None"
                ),
                @Parameter(name = NATSConstants.OPTIONAL_CONFIGURATION,
                        description = "This parameter contains all the other possible configurations that the nats" +
                                " client can be created with. \n `io.nats.client.reconnect.max:8, io.nats.client." +
                                "timeout:5000`",
                        optional = true,
                        type = DataType.STRING,
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
                @Example(syntax = "@source(type='nats', @map(type='text'), "
                                + "destination='SP_NATS_INPUT_TEST', "
                                + "server.urls='nats://localhost:4222',"
                                + "client.id='nats_client',"
                                + "streaming.cluster.id='test-cluster',"
                                + "queue.group.name = 'group_nats',"
                                + "durable.name = 'nats-durable',"
                                + "subscription.sequence = '100'"
                                + ")\n"
                                + "define stream inputStream (name string, age int, country string);",
                        description = "This example shows how to subscribe to a NATS subject in nats streaming " +
                                "broker with some basic configurations.With the above configuration the source " +
                                "identified as 'nats-client' will subscribes to a subject named as " +
                                "'SP_NATS_INPUT_TEST' which resides in a nats instance with a cluster id of " +
                                "'test-cluster', running in localhost and listening to the port 4222 for client " +
                                "connection. This subscription will receive all the messages from 100th in the " +
                                "subject. Since this is using a nats streaming broker it's mandatory to provide the " +
                                "`streaming.cluster.id` parameter."),

                @Example(syntax = "@source(type='nats', @map(type='xml'), "
                                + "destination='nats-test', "
                                + "server.urls='" + "nats://localhost:4222')\n"
                                + "define stream inputStream1 (name string, age int, country string);",
                        description = "This will subscribe to a Nats subject in nats broker with some basic " +
                                "configurations. Nats server should be running on the `localhost:4222` address and " +
                                "this source will keep listening to messages which receives into the `nats-test` " +
                                "subject"),

                @Example(description = "This example shows how to pass Nats sequence number to the event.",
                        syntax = "@source(type='nats', @map(type='json', @attributes(name='$.name', age='$.age', " +
                                "country='$.country', sequenceNum='trp:sequenceNumber')), " +
                                "destination='SIDDHI_NATS_SOURCE_TEST_DEST', " +
                                "client.id='nats_client', " +
                                "server.urls='nats://localhost:4222', " +
                                "streaming.cluster.id='test-cluster'" +
                                ")\n" +
                                "define stream inputStream (name string, age int, country string, sequenceNum string);")
        }
)

public class NATSSource extends Source {

    private NATSCore nats;

    @Override
    public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                             String[] requestedTransportPropertyNames, ConfigReader configReader,
                             SiddhiAppContext siddhiAppContext) {
        if (optionHolder.isOptionExists(NATSConstants.CLUSTER_ID) || optionHolder.isOptionExists(
                NATSConstants.STREAMING_CLUSTER_ID)) {
            nats = new NATSStreaming();
        } else {
            nats = new NATSCore();
        }
        return nats.initiateNatsClient(sourceEventListener, optionHolder, requestedTransportPropertyNames, configReader,
                siddhiAppContext);
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
    }

    @Override
    public void disconnect() {
        nats.disconnect();
    }

    @Override
    public void destroy() {

    }

    @Override
    public void pause() {
        nats.pause();
    }

    @Override
    public void resume() {
        nats.resume();
    }
}


