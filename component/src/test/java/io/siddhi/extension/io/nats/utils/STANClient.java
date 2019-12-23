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

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.streaming.Message;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

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
    private Options.Builder optionBuilder;
    private io.nats.client.Options.Builder natsOptionsBuilder;

    public STANClient(String clusterId, String clientId, String natsUrl, ResultContainer resultContainer) {
        optionBuilder = new Options.Builder();
        natsOptionsBuilder = new io.nats.client.Options.Builder();
        this.cluserId = clusterId;
        this.clientId = clientId;
        this.natsUrl = natsUrl;
        this.resultContainer = resultContainer;
    }

    public STANClient(String clusterId, String clientId, String natsUrl) {
        this.cluserId = clusterId;
        this.clientId = clientId;
        this.natsUrl = natsUrl;
        optionBuilder = new Options.Builder();
        natsOptionsBuilder = new io.nats.client.Options.Builder();
    }

    public void setUsernameAndPassword(char[] username, char[] password) {
        natsOptionsBuilder.userInfo(username, password);
    }

    public void setToken(char[] token) {
        natsOptionsBuilder.token(token);
    }

    public void addSslConnection() throws Exception {
        natsOptionsBuilder.sslContext(createTestSSLContext());
    }

    public void connect() throws IOException, InterruptedException {
        natsOptionsBuilder.server(this.natsUrl);
        Connection con = Nats.connect(natsOptionsBuilder.build());
        optionBuilder.natsConn(con).clientId(clientId).clusterId(cluserId);
        StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(optionBuilder.build());
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

    public static KeyStore loadKeystore(String path) throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(path))) {
            String storePassword = "password";
            store.load(in, storePassword.toCharArray());
        }
        return store;
    }

    private static TrustManager[] createTestTrustManagers() throws Exception {
        String truststorePath = "src/test/resources/truststore.jks";
        KeyStore store = loadKeystore(truststorePath);
        String algorithm = "SunX509";
        TrustManagerFactory factory = TrustManagerFactory.getInstance(algorithm);
        factory.init(store);
        return factory.getTrustManagers();
    }

    private static SSLContext createTestSSLContext() throws Exception {
        SSLContext ctx = SSLContext.getInstance(io.nats.client.Options.DEFAULT_SSL_PROTOCOL);
        ctx.init(null, createTestTrustManagers(), new SecureRandom());
        return ctx;
    }
}
