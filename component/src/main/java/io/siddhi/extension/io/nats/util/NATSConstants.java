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
package io.siddhi.extension.io.nats.util;

/**
 * Contains the property key values of NATS connection.
 */
public class NATSConstants {
    public static final String DESTINATION = "destination";
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String CLIENT_ID = "client.id";
    public static final String CLUSTER_ID = "cluster.id";
    public static final String DEFAULT_SERVER_URL = "nats://localhost:4222";
    public static final String QUEUE_GROUP_NAME = "queue.group.name";
    public static final String DURABLE_NAME = "durable.name";
    public static final String SUBSCRIPTION_SEQUENCE = "subscription.sequence";
    public static final String SEQUENCE_NUMBER  = "sequenceNumber";
    public static final String SERVER_URLS = "server.urls";
    public static final String STREAMING_CLUSTER_ID = "streaming.cluster.id";
    public static final String AUTH_TYPE = "auth.type";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String TOKEN = "token";
    public static final String KEYSTORE_FILE = "keystore.file";
    public static final String KEYSTORE_PASSWORD = "keystore.password";
    public static final String KEYSTORE_ALGORITHM = "keystore.algorithm";
    public static final String TRUSTSTORE_FILE = "truststore.file";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";
    public static final String TRUSTSTORE_ALGORITHM = "truststore.algorithm";
    public static final String STORE_TYPE = "tls.store.type";
    public static final String CLIENT_VERIFY = "client.verify";
    public static final String OPTIONAL_CONFIGURATION = "optional.configuration";
    public static final String ACK_WAIT = "ack.wait";

    public static final String DEFAULT_KEYSTORE_PATH = "${carbon.home}/resources/security/wso2carbon.jks";
    public static final String DEFAULT_TRUSTSTORE_PATH = "${carbon.home}/resources/security/client-truststore.jks";
    public static final String DEFAULT_STORE_PASSWORD = "wso2carbon";
    public static final String DEFAULT_KEY_PASSWORD = "wso2carbon";
    public static final String DEFAULT_KEYSTORE_ALGORITHM = "SunX509";
    public static final String DEFAULT_TRUSTSTORE_ALGORITHM = "SunX509";
    public static final String DEFAULT_STORE_TYPE = "JKS";

}
