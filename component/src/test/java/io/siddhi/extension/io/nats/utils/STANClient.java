/*
 *  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.nats.utils;

import io.nats.streaming.Message;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Nats streaming client for running test cases.
 */
public class STANClient {
    private String cluserId;
    private String clientId;
    private String natsUrl;
    private ResultContainer resultContainer;
    private StreamingConnection streamingConnection;
    private Subscription subscription;
    private static Log log = LogFactory.getLog(STANClient.class);

    public STANClient(String clusterId, String clientId, String natsUrl, ResultContainer resultContainer) {
        this.cluserId = clusterId;
        this.clientId = clientId;
        this.natsUrl = natsUrl;
        this.resultContainer = resultContainer;
    }

    public STANClient(String clusterId, String clientId, String natsUrl) {
        this.cluserId = clusterId;
        this.clientId = clientId;
        this.natsUrl = natsUrl;
    }

    public void connect() {
        StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(this.cluserId, this
                .clientId);
        streamingConnectionFactory.setNatsUrl(this.natsUrl);
        try {
            streamingConnection =  streamingConnectionFactory.createConnection();
        } catch (IOException | InterruptedException e) {
            log.error("Unable to make a connection to NATS broker at " + natsUrl + ". " + e.getMessage());
        }
    }

    public void publish(String subjectName, String message) {
        try {
            streamingConnection.publish(subjectName, message.getBytes(StandardCharsets.UTF_8));
        } catch (IOException | InterruptedException | TimeoutException e) {
            log.error(e.getMessage());
        }
    }

    public void publishProtobufMessage(String subjectName, Object message) {
        try {
            streamingConnection.publish(subjectName, (byte[]) message);
        } catch (IOException | InterruptedException | TimeoutException e) {
            log.error(e.getMessage());
        }
    }

    public void subsripeFromNow(String subject) throws InterruptedException, TimeoutException, IOException {
        subscription = streamingConnection.subscribe(subject, (Message m) ->
                        resultContainer.eventReceived(new String(m.getData(), StandardCharsets.UTF_8)),
                new SubscriptionOptions.Builder().startAtTime(Instant.now()).build());
    }

    public void subscribe(String subject) throws InterruptedException, IOException, TimeoutException {
        subscription =  streamingConnection.subscribe(subject, (Message m) ->
                        resultContainer.eventReceived(new String(m.getData(), StandardCharsets.UTF_8)),
                new SubscriptionOptions.Builder().deliverAllAvailable().build());
    }

    public void subscribeFromLastPublished(String subject) throws InterruptedException, IOException, TimeoutException {
        subscription = streamingConnection.subscribe(subject, (Message m) ->
                        resultContainer.eventReceived(new String(m.getData(), StandardCharsets.UTF_8)),
                new SubscriptionOptions.Builder().startWithLastReceived().build());
    }

    public void subscribeFromGivenSequence(String subject, int sequence) throws InterruptedException, IOException,
            TimeoutException {
        subscription = streamingConnection.subscribe(subject, (Message m) ->
                        resultContainer.eventReceived(new String(m.getData(), StandardCharsets.UTF_8)),
                new SubscriptionOptions.Builder().startAtSequence(sequence).build());
    }

    public void subscrbeFromGivenTime(String subject,  Instant instant) throws InterruptedException, IOException,
            TimeoutException {
        subscription = streamingConnection.subscribe(subject, (Message m) ->
                        resultContainer.eventReceived(new String(m.getData(), StandardCharsets.UTF_8)),
                new SubscriptionOptions.Builder().startAtTime(instant).build());

    }

    public void subscribeDurable(String subject, String durableName) throws InterruptedException, IOException,
            TimeoutException {
        subscription = streamingConnection.subscribe(subject, (Message m) ->
                        resultContainer.eventReceived(new String(m.getData(), StandardCharsets.UTF_8)),
                new SubscriptionOptions.Builder().durableName(durableName).build());
    }

    public void subscribeWithQueueGroupFromSequence(String subject, String queueGroup, int sequence)
            throws InterruptedException, TimeoutException, IOException {
        subscription = streamingConnection.subscribe(subject, queueGroup, (Message m) ->
                        resultContainer.eventReceived(new String(m.getData(), StandardCharsets.UTF_8)) ,
                new SubscriptionOptions.Builder().startAtSequence(sequence).build());
    }

    private String createClientId() {
        return new Date().getTime() + "_" + new Random().nextInt(99999) + "_" + new Random().nextInt(99999);
    }

    public void close() throws InterruptedException, TimeoutException, IOException {
        streamingConnection.close();
    }

    public void unsubscribe() throws IOException {
        subscription.unsubscribe();
    }
}
