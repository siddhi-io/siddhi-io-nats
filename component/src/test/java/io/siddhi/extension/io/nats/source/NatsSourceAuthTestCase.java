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

package io.siddhi.extension.io.nats.source;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.io.nats.utils.NATSClient;
import io.siddhi.extension.io.nats.utils.ResultContainer;
import io.siddhi.extension.io.nats.utils.STANClient;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Test case to receive messages while authentication unable.
 */
public class NatsSourceAuthTestCase {

    GenericContainer simpleWebServer;
    private int port = 4222;

    private void startDocker(boolean isTls, String dockerImgName, String... commands) throws InterruptedException {
        simpleWebServer = new GenericContainer(dockerImgName);
        simpleWebServer.setPrivilegedMode(true);
        if (isTls) {
            simpleWebServer.addFileSystemBind("src/test/resources/certs", "/certs", BindMode.READ_WRITE);
        }
        simpleWebServer.setCommand(commands);
        simpleWebServer.start();
        port = simpleWebServer.getMappedPort(4222);
        Thread.sleep(500);
    }

    @Test
    public void natsSubscribeWithUsernameAndPassword() throws InterruptedException {
        startDocker(false, "nats", "--user", "test", "--pass", "1234");
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("nats-test1", resultContainer, port);
        natsClient.setUsernameAndPassword("test".toCharArray(), "1234".toCharArray());
        natsClient.connectClient();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "server.urls='" + "nats://localhost:" + port + "',"
                + "auth.type='user', username='test', password='1234')"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(1000);

        natsClient.publish("<events><event><name>JAMES</name><age>23</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>MIKE</name><age>23</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(100);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
        simpleWebServer.close();
    }

    @Test(dependsOnMethods = "natsSubscribeWithUsernameAndPassword")
    public void natsSubscribeWithToken() throws InterruptedException {
        startDocker(false, "nats", "--auth", "test");
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("nats-test1", resultContainer, port);
        natsClient.setToken("test".toCharArray());
        natsClient.connectClient();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "server.urls='" + "nats://localhost:" + port + "',"
                + "auth.type='token', token='test')"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(1000);

        natsClient.publish("<events><event><name>JAMES</name><age>23</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>MIKE</name><age>23</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(100);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
        simpleWebServer.close();
    }

    @Test(dependsOnMethods = "natsSubscribeWithToken")
    public void natsSubscribeWithTLS() throws Exception {
        startDocker(true, "nats", "--tls", "--tlscert", "certs/server.pem", "--tlskey", "certs/key.pem");
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("nats-test1", resultContainer, port);
        natsClient.addSSL();
        natsClient.connectClient();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "server.urls='" + "nats://localhost:" + port + "',"
                + "auth.type='tls', truststore.file='src/test/resources/truststore.jks', " +
                "truststore.password='password', truststore.algorithm='SunX509')"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(1000);

        natsClient.publish("<events><event><name>JAMES</name><age>23</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>MIKE</name><age>23</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(100);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
        simpleWebServer.close();
    }

    @Test(dependsOnMethods = "natsSubscribeWithTLS")
    public void testStanWithUsernameAndPassword() throws InterruptedException, TimeoutException, IOException {
        startDocker(true, "nats-streaming", "--user", "test", "--pass", "1234");
        ResultContainer resultContainer = new ResultContainer(2, 3);
        STANClient stanClient = new STANClient("test-cluster", "nats-source-test1",
                "nats://localhost:" + port);
        stanClient.setUsernameAndPassword("test".toCharArray(), "1234".toCharArray());
        stanClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "client.id='nats-source-test1-siddhi', "
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "auth.type='user', username='test', password='1234',"
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(100);

        stanClient.publish("nats-test1", "<events><event><name>JAMES</name><age>23</age>"
                + "<country>US</country></event></events>");
        stanClient.publish("nats-test1", "<events><event><name>MIKE</name><age>23</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(100);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        stanClient.close();
        simpleWebServer.close();
    }

    @Test(dependsOnMethods = "testStanWithUsernameAndPassword")
    public void testStanWithToken() throws InterruptedException, TimeoutException, IOException {
        startDocker(true, "nats-streaming", "--auth", "test");
        ResultContainer resultContainer = new ResultContainer(2, 3);
        STANClient stanClient = new STANClient("test-cluster", "nats-source-test1",
                "nats://localhost:" + port);
        stanClient.setToken("test".toCharArray());
        stanClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "client.id='nats-source-test1-siddhi', "
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "auth.type='token', token='test',"
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(100);

        stanClient.publish("nats-test1", "<events><event><name>JAMES</name><age>23</age>"
                + "<country>US</country></event></events>");
        stanClient.publish("nats-test1", "<events><event><name>MIKE</name><age>23</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(100);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        stanClient.close();
        simpleWebServer.close();
    }

}
