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

//import com.google.protobuf.GeneratedMessageV3;

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
import io.siddhi.extension.io.nats.source.nats.NATS;
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

    private NATS nats;

    @Override
    public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                             String[] requestedTransportPropertyNames, ConfigReader configReader,
                             SiddhiAppContext siddhiAppContext) {
        if (optionHolder.isOptionExists(NATSConstants.CLUSTER_ID) || optionHolder.isOptionExists(
                NATSConstants.STREAMING_CLUSTER_ID)) {
            nats = NATS.getNATS(true);
        } else {
            nats = NATS.getNATS(false);
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


