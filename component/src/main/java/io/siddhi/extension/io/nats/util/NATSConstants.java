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
    public static final String DEFAULT_CLUSTER_ID = "test-cluster";
    public static final String QUEUE_GROUP_NAME = "queue.group.name";
    public static final String DURABLE_NAME = "durable.name";
    public static final String SUBSCRIPTION_SEQUENCE = "subscription.sequence";
    public static final String SEQUENCE_NUMBER  = "sequenceNumber";
    public static final String CONNECTION_TIME_OUT  = "connection.timeout";
    public static final String PING_INTERVAL  = "ping.interval";
    public static final String MAX_PING_OUTS  = "max.ping.outs";
    public static final String MAX_RETRY_ATTEMPTS  = "max.retry.attempts";
    public static final String RETRY_BUFFER_SIZE = "max.buffer.size";
    public static final String SERVER_URLS = "server.urls";
}
