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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.extension.io.nats.utils.ResultContainer;
import io.siddhi.extension.io.nats.utils.STANClient;
import io.siddhi.extension.io.nats.utils.UnitTestAppender;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Contains test cases for Nats Streaming sink.
 */
public class STANSinkTestCase {
    private Logger log = Logger.getLogger(STANSinkTestCase.class);
    private int port;


    @BeforeClass
    private void initializeDockerContainer() throws InterruptedException {
        GenericContainer simpleWebServer
                = new GenericContainer("nats-streaming:0.11.2");
        simpleWebServer.setPrivilegedMode(true);
        simpleWebServer.start();
        port = simpleWebServer.getMappedPort(4222);
        Thread.sleep(500);
    }

    /**
     * Test for configure the NATS Sink to publish the message to a NATS-streaming subject.
     */
    @Test
    public void natsSimplePublishTest() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        STANClient stanClient = new STANClient("test-cluster", "stan_test1", "nats://localhost:"
                + port, resultContainer);
        stanClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan1')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "client.id='test-plan1-siddhi',"
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "streaming.cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        stanClient.subsripeFromNow("nats-test1");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        Thread.sleep(500);
        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
    }

    /**
     * if a property missing from the siddhi stan sink which defined as mandatory in the extension definition, then
     * {@link SiddhiAppValidationException} will be thrown.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class,
            dependsOnMethods = "natsSimplePublishTest")
    public void testMissingNatsMandatoryProperty() {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name('Test-plan2')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "client.id='test-plan2',"
                + "streaming.cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        siddhiManager.shutdown();
    }

    /**
     * If invalid NATS url provided then {@link SiddhiAppValidationException} will be thrown.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class,
            dependsOnMethods = "testMissingNatsMandatoryProperty")
    public void testInvalidNatsUrl() {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name('Test-plan3')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test3', "
                + "server.urls='natss://localhost:4222',"
                + "client.id='stan_client',"
                + "streaming.cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        siddhiManager.shutdown();
    }

    /**
     * if the client.id is not given by the user in the extension headers, then a randomly generated client id will
     * be used.
     */
    @Test(dependsOnMethods = "testInvalidNatsUrl")
    public void testOptionalClientId() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        STANClient stanClient = new STANClient("test-cluster", "test-plan4", "nats://localhost:"
                + port, resultContainer);
        stanClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan1')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='test-plan4', "
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "streaming.cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        stanClient.subsripeFromNow("test-plan4");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        Thread.sleep(500);
        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
    }

    /**
     * If a single stream has multiple sink annotations then all the events from the stream should be passed to
     * the given subjects.
     */
    @Test(dependsOnMethods = "testOptionalClientId")
    public void testMultipleSinkSingleStream() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(8, 3);
        STANClient stanClient = new STANClient("test-cluster", "nats-test-plan5",
                "nats://localhost:" + port, resultContainer);
        stanClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan5')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-source-test-siddhi-1', "
                + "client.id='test-plan5-siddhi-sink-pub1',"
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "streaming.cluster.id='test-cluster'"
                + ")"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-source-test-siddhi-2', "
                + "client.id='test-plan5-siddhi-sink-pub2',"
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "streaming.cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        stanClient.subsripeFromNow("nats-source-test-siddhi-1");
        stanClient.subsripeFromNow("nats-source-test-siddhi-2");

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        inputStream.send(new Object[]{"SMITH", 23, "RSA"});
        inputStream.send(new Object[]{"MILLER", 23, "WI"});
        Thread.sleep(500);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        Assert.assertTrue(resultContainer.assertMessageContent("SMITH"));
        Assert.assertTrue(resultContainer.assertMessageContent("MILLER"));
        siddhiManager.shutdown();
    }

    /**
     * Test the NATS sink configurations with mandatory parameters only.
     */
    @Test(dependsOnMethods = "testMultipleSinkSingleStream")
    public void testNatsSinkWithMandatoryConfigurations() throws InterruptedException, IOException, TimeoutException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        STANClient stanClient = new STANClient("test-cluster", "stan_test6", "nats://localhost:"
                + port, resultContainer);
        stanClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan6')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "server.urls='" + "nats://localhost:" + port + "', streaming.cluster.id='test-cluster',"
                + "destination='nats-test6' "
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        stanClient.subsripeFromNow("nats-test6");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        Thread.sleep(500);
        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
    }

    /**
     * If invalid cluster name is provided in NATS sink configurations then ConnectionUnavailableException
     * should have been thrown. Here incorrect cluster id provided. Hence the connection will fail.
     */
    @Test(dependsOnMethods = "testNatsSinkWithMandatoryConfigurations")
    public void testIncorrectClusterName() throws InterruptedException {
        log.info("Test with connection unavailable exception");
        log = Logger.getLogger(Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan7')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test7', "
                + "client.id='test-plan7-siddhi',"
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "streaming.cluster.id='nats-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        executionPlanRuntime.start();
        Thread.sleep(500);
        Assert.assertTrue(appender.getMessages().contains("Error in Siddhi App 'Test-plan7' while " +
                "connecting to NATS server "));
        siddhiManager.shutdown();
    }

    /**
     * If incorrect bootstrap server url is provided in NATS sink configurations then
     * ConnectionUnavailableException should have been thrown. Here incorrect cluster url is provided hence the
     * connection will fail.
     */
    @Test(dependsOnMethods = "testIncorrectClusterName")
    public void testIncorrectNatsServerUrl() throws InterruptedException {
        log.info("Test with connection unavailable exception");
        log = Logger.getLogger(Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan8\")"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test8', "
                + "client.id='nats-source-test8-siddhi', "
                + "server.urls='nats://localhost:5223', "
                + "streaming.cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.start();
        Thread.sleep(500);
        Assert.assertTrue(appender.getMessages().contains("Error in Siddhi App 'Test-plan8' while connecting to NATS " +
                "server endpoint [nats://localhost:5223]"));
        siddhiManager.shutdown();
    }

    /**
     * If the map annotation is not included in the sink annotation then {@link SiddhiAppCreationException} should be
     * thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = "testIncorrectNatsServerUrl")
    public void testMissingMappingAnnotation() {
        SiddhiAppRuntime executionPlanRuntime = null;
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            String inStreamDefinition = "@App:name('test-plan9')\n"
                    + "@sink(type='nats',"
                    + "destination='nats-sink-test9', "
                    + "client.id='test-plan9-siddhi',"
                    + "server.urls='nats://localhost:4222',"
                    + "streaming.cluster.id='test-cluster'"
                    + ")"
                    + "define stream inputStream (name string, age int, country string);";

            executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        } finally {
            if (executionPlanRuntime != null) {
                executionPlanRuntime.shutdown();
            }
        }
    }

    /**
     * Test for configure the NATS Sink to publish the message to a NATS-streaming subject.
     */
    @Test(dependsOnMethods = "testMissingMappingAnnotation")
    public void testNatsProtobuf() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 10);
        STANClient stanClient = new STANClient("test-cluster", "stan-test10",
                "nats://localhost:" + port, resultContainer);
        stanClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan10')\n"
                + "@sink(type='nats', " +
                "@map(type='protobuf', class='io.siddhi.extension.io.nats.utils.protobuf.Person'), "
                + "destination='nats-test10', "
                + "client.id='test-plan10-siddhi',"
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "streaming.cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (nic long, name string);";

        stanClient.subsripeFromNow("nats-test10");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        long nic1 = 1222;
        long nic2 = 1223;
        inputStream.send(new Object[] {nic1, "Jimmy"});
        inputStream.send(new Object[] {nic2, "Natalie"});

        Thread.sleep(500);
        AssertJUnit.assertEquals(resultContainer.getEventCount(), 2);
        siddhiManager.shutdown();
    }

    @Test(dependsOnMethods = "testNatsProtobuf")
    public void testDistributedSink() throws InterruptedException, TimeoutException, IOException {
        log.info("Test distributed Nats Sink");
        ResultContainer topic1ResultContainer = new ResultContainer(2, 20);
        STANClient topic1STANClient = new STANClient("test-cluster", "stan-distributed-sink-1",
                "nats://localhost:" + port, topic1ResultContainer);
        topic1STANClient.connect();

        ResultContainer topic2ResultContainer = new ResultContainer(4, 20);
        STANClient topic2STANClient = new STANClient("test-cluster", "stan-distributed-sink-2",
                "nats://localhost:" + port, topic2ResultContainer);
        topic2STANClient.connect();

        String streams = "" +
                "@app:name('TestSiddhiApp') \n" +
                "define stream FooStream (symbol string, price float, volume long); "
                + "@sink(type='nats', "
                + "client.id='test-distributed-sink', "
                + "@distribution(strategy='partitioned', partitionKey='symbol', "
                + "@destination(destination = 'nats-topic1'), @destination(destination = 'nats-topic2')), "
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "streaming.cluster.id='test-cluster', "
                + "@map(type='json')) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        topic1STANClient.subsripeFromNow("nats-topic1");
        topic2STANClient.subsripeFromNow("nats-topic2");

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(5000);
        AssertJUnit.assertEquals("Number of IBM events received at 'nats-topic1'", 2,
                topic1ResultContainer.getEventCount());
        AssertJUnit.assertEquals("Number of WSO2 events received at 'nats-topic2'", 4,
                topic2ResultContainer.getEventCount());
        siddhiManager.shutdown();
    }
}

