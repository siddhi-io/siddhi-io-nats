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

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class which use to create and handle nats client for publishing message.
 */
public class NATSCore {

    private static final Logger log = LogManager.getLogger(NATSCore.class);
    protected Option destination;
    protected String[] natsUrls;
    protected String streamId;
    protected String siddhiAppName;
    protected Options.Builder natsOptionBuilder;
    protected Connection connection;
    protected AtomicBoolean isConnected = new AtomicBoolean(true);
    protected String authType;


    public void initiateClient(OptionHolder optionHolder, String siddhiAppName, String streamId) {
        this.destination = optionHolder.validateAndGetOption(NATSConstants.DESTINATION);
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
        this.siddhiAppName = siddhiAppName;
        this.streamId = streamId;
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
    }

    public void createNATSClient() throws ConnectionUnavailableException {
        natsOptionBuilder.connectionListener((conn, type) -> {
            if (type == ConnectionListener.Events.CLOSED) {
                isConnected =  new AtomicBoolean(false);
            }
        });
        this.connection = createNatsConnection();
        isConnected.set(true);
    }

    public void publishMessages(Object payload, byte[] messageBytes, DynamicOptions dynamicOptions)
            throws ConnectionUnavailableException {
        if (!isConnected.get()) {
            this.connection = createNatsConnection();
        }
        String subjectName = destination.getValue(dynamicOptions);
        connection.publish(subjectName, messageBytes);
    }

    public void disconnect() {
        if (connection != null) {
            if (isConnected.get()) {
                try {
                    connection.flush(Duration.ofMillis(50));
                    connection.close();
                } catch (TimeoutException | InterruptedException e) {
                    log.error("Error disconnecting the nats receiver in Siddhi App '" + siddhiAppName +
                            "' when publishing messages to NATS endpoint " + Arrays.toString(natsUrls) + " . " +
                            e.getMessage(), e);
                }
            }
        }
    }

    private Connection createNatsConnection() throws ConnectionUnavailableException {
        try {
            return Nats.connect(natsOptionBuilder.build());
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
    }
}
