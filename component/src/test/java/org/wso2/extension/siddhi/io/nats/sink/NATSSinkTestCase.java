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
package org.wso2.extension.siddhi.io.nats.sink;

import org.apache.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.nats.utils.NATSClient;
import org.wso2.extension.siddhi.io.nats.utils.ResultContainer;
import org.wso2.extension.siddhi.io.nats.utils.UnitTestAppender;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Contains test cases for NATS sink.
 */
public class NATSSinkTestCase {
    private Logger log = Logger.getLogger(NATSSinkTestCase.class);
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
        ResultContainer resultContainer = new ResultContainer(2,3);
        NATSClient NATSClient = new NATSClient("test-cluster","stan_test1","nats://localhost:"
                + port, resultContainer);
        NATSClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan1')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "client.id='test-plan1-siddhi',"
                + "bootstrap.servers='" + "nats://localhost:"+ port +"', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        NATSClient.subsripeFromNow("nats-test1");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        Thread.sleep(1000);
        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));

        siddhiManager.shutdown();
    }

    /**
     * if a property missing from the siddhi stan sink which defined as mandatory in the extension definition, then
     * {@link SiddhiAppValidationException} will be thrown.
     */
    @Test
    public void testMissingNatsMandatoryProperty(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name('Test-plan2')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "bootstrap.servers='" + "nats://localhost:"+ port +"', "
                + "client.id='test-plan2',"
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        try {
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
            Assert.fail();
        } catch (SiddhiAppValidationException e) {
            Assert.assertTrue(e.getMessage().contains("Option 'destination' does not exist in the configuration of "
                    + "'sink:nats'"));
        }
    }

    /**
     * If invalid NATS url provided then {@link SiddhiAppCreationException} will be thrown
     */
    @Test
    public void testInvalidNatsUrl(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name('Test-plan3')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test3', "
                + "bootstrap.servers='natss://localhost:4222',"
                + "client.id='stan_client',"
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        try {
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
            Assert.fail();
        } catch (SiddhiAppValidationException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid NATS url"));
        }
    }

    /**
     * if the client.id is not given by the user in the extension headers, then a randomly generated client id will
     * be used.
     */
    @Test()
    public void testOptionalClientId() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2,3);
        NATSClient NATSClient = new NATSClient("test-cluster","test-plan4","nats://localhost:"
                + port, resultContainer);
        NATSClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan1')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='test-plan4', "
                + "bootstrap.servers='" + "nats://localhost:"+ port +"', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        NATSClient.subsripeFromNow("test-plan4");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        Thread.sleep(1000);
        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));

        siddhiManager.shutdown();
    }

    /**
     * If a single stream has multiple sink annotations then all the events from the stream should be passed to
     * the given subjects.
     */
    @Test
    public void testMultipleSinkSingleStream() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(8,3);
        NATSClient NATSClient = new NATSClient("test-cluster","nats-test-plan5",
                "nats://localhost:" + port, resultContainer);
        NATSClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan5')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-source-test-siddhi-1', "
                + "client.id='test-plan5-siddhi-sink-pub1',"
                + "bootstrap.servers='" + "nats://localhost:"+ port +"', "
                + "cluster.id='test-cluster'"
                + ")"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-source-test-siddhi-2', "
                + "client.id='test-plan5-siddhi-sink-pub2',"
                + "bootstrap.servers='" + "nats://localhost:"+ port +"', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        NATSClient.subsripeFromNow("nats-source-test-siddhi-1");
        NATSClient.subsripeFromNow("nats-source-test-siddhi-2");

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        inputStream.send(new Object[]{"SMITH", 23, "RSA"});
        inputStream.send(new Object[]{"MILLER", 23, "WI"});

        Thread.sleep(1000);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        Assert.assertTrue(resultContainer.assertMessageContent("SMITH"));
        Assert.assertTrue(resultContainer.assertMessageContent("MILLER"));

        siddhiManager.shutdown();
    }

    /**
     * Test the NATS sink configurations with mandatory parameters only.
     */
    @Test
    public void testNatsSinkWithMandatoryConfigurations() throws InterruptedException, IOException, TimeoutException {
        ResultContainer resultContainer = new ResultContainer(2,3);
        NATSClient NATSClient = new NATSClient("test-cluster","stan_test6","nats://localhost:"
                + port, resultContainer);
        NATSClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan6')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "bootstrap.servers='" + "nats://localhost:"+ port +"', "
                + "destination='nats-test6' "
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        NATSClient.subsripeFromNow("nats-test6");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputStream.send(new Object[]{"JAMES", 23, "USA"});
        inputStream.send(new Object[]{"MIKE", 23, "Germany"});
        Thread.sleep(1000);
        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));

        siddhiManager.shutdown();
    }

    /**
     * If invalid cluster name is provided in NATS sink configurations then {@link ConnectionUnavailableException}
     * should have been thrown. Here incorrect cluster id provided. Hence the connection will fail.
     */
    @Test
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
                + "bootstrap.servers='" + "nats://localhost:"+ port +"', "
                + "cluster.id='nats-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        executionPlanRuntime.start();
        Thread.sleep(1000);

        Assert.assertTrue(appender.getMessages().contains("Error while connecting to NATS server at destination:"));
        siddhiManager.shutdown();
    }

    /**
     * If incorrect bootstrap server url is provided in NATS sink configurations then
     * {@link ConnectionUnavailableException} should have been thrown. Here incorrect cluster url is provided hence the
     * connection will fail.
     */
    @Test
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
                + "bootstrap.servers='nats://localhost:5223', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.start();
        Thread.sleep(1000);

        Assert.assertTrue(appender.getMessages().contains("Error while connecting to NATS server at destination:"));
        siddhiManager.shutdown();
    }

    /**
     * If the map annotation is not included in the sink annotation then {@link SiddhiAppCreationException} should be
     * thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testMissingMappingAnnotation() {
        SiddhiAppRuntime executionPlanRuntime = null;
        try {
            SiddhiManager siddhiManager = new SiddhiManager();
            String inStreamDefinition = "@App:name('test-plan9')\n"
                    + "@sink(type='nats',"
                    + "destination='nats-sink-test9', "
                    + "client.id='test-plan9-siddhi',"
                    + "bootstrap.servers='nats://localhost:4222',"
                    + "cluster.id='test-cluster'"
                    + ")"
                    + "define stream inputStream (name string, age int, country string);";

            executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        } finally {
            if (executionPlanRuntime != null) {
                executionPlanRuntime.shutdown();
            }
        }
    }
}

