/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Nats client for running test cases.
 */
public class NATSClient {

    private String natsUrl = "nats://localhost:";
    private Connection nc;
    private String subject;
    private ResultContainer resultContainer;
    private boolean isProtobuf;
    private Options.Builder optionsBuilder = new Options.Builder();

    public NATSClient(String subject, ResultContainer resultContainer, int port) {
        this.subject = subject;
        this.resultContainer = resultContainer;
        this.natsUrl = natsUrl + port;
    }

    public NATSClient(String subject, int port) {
        this.subject = subject;
        this.natsUrl = natsUrl + port;
    }

    public NATSClient(String subject, ResultContainer resultContainer, int port, boolean isProtobuf) {
        this.subject = subject;
        this.resultContainer = resultContainer;
        this.isProtobuf = isProtobuf;
        this.natsUrl = natsUrl + port;
    }

    public void setUsernameAndPassword(char[] username, char[] password) {
        optionsBuilder.userInfo(username, password);
    }

    public void setToken(char[] token) {
        optionsBuilder.token(token);
    }

    public void addSSL() throws Exception {
        optionsBuilder.sslContext(createTestSSLContext());
    }

    public void connectClient() {
        optionsBuilder.server(natsUrl);
        try {
            nc = Nats.connect(optionsBuilder.build());
        } catch (IOException | InterruptedException ignored) {
            ignored.printStackTrace();
        }
    }

    public void subscribe() {
        Dispatcher d = nc.createDispatcher((msg) -> {
            if (isProtobuf) {
                resultContainer.eventReceived(msg.getData());
            } else {
                resultContainer.eventReceived(new String(msg.getData(), StandardCharsets.UTF_8));
            }
        });
        d.subscribe(subject);
    }

    public void publish(String message) {
        nc.publish(subject, message.getBytes());
    }
    public void publishProtoBuf(byte[] message) {
        nc.publish(subject, message);
    }

    public void close() throws InterruptedException {
        nc.close();
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
        SSLContext ctx = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
        ctx.init(null, createTestTrustManagers(), new SecureRandom());
        return ctx;
    }

}
