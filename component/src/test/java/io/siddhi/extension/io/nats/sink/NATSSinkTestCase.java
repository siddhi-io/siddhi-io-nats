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

package io.siddhi.extension.io.nats.sink;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.extension.io.nats.utils.NATSClient;
import io.siddhi.extension.io.nats.utils.ResultContainer;
import io.siddhi.extension.io.nats.utils.UnitTestAppender;
import io.siddhi.extension.io.nats.utils.protobuf.Person;
import org.apache.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Contains test cases for Nats sink.
 */
public class NATSSinkTestCase {

    private Logger log = Logger.getLogger(STANSinkTestCase.class);
    private int port;

    @BeforeClass
    private void initializeDockerContainer() throws InterruptedException {
        GenericContainer simpleWebServer
                = new GenericContainer("nats");
        simpleWebServer.setPrivilegedMode(true);
        simpleWebServer.start();
        port = simpleWebServer.getMappedPort(4222);
        Thread.sleep(500);
    }

    @Test
    public void natsCoreSimplePublishTest() throws InterruptedException {
        ResultContainer resultContainer = new ResultContainer(20, 8);
        NATSClient natsClient = new NATSClient("nats-test1", resultContainer, port);
        natsClient.connectClient();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan1')\n"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "server.urls='" + "nats://localhost:" + port + "'" +
                 ")"
                + "define stream inputStream (name string, age int, country string);";

        natsClient.subscribe();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        for (int i = 0; i < 20; i++) {
            inputStream.send(new Object[]{"MIKE", i, "Germany"});
        }
        Thread.sleep(1000);
        Assert.assertTrue(resultContainer.assertMessageContent("<events><event><name>MIKE</name><age>19</age>" +
                "<country>Germany</country></event></events>"));
        siddhiManager.shutdown();
    }

    @Test(dependsOnMethods = "natsCoreSimplePublishTest")
    public void testNatsProtobuf() throws InterruptedException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 10);
        NATSClient natsClient = new NATSClient("nats-test10", resultContainer, port, true);
        natsClient.connectClient();
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@App:name('Test-plan10')\n"
                + "@sink(type='nats', " +
                "@map(type='protobuf', class='io.siddhi.extension.io.nats.utils.protobuf.Person'), "
                + "destination='nats-test10', "
                + "server.urls='" + "nats://localhost:" + port + "'"
                + ")"
                + "define stream inputStream (nic long, name string);";

        natsClient.subscribe();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        InputHandler inputStream = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        long nic1 = 1222;
        long nic2 = 1223;
        inputStream.send(new Object[] {nic1, "Jimmy"});
        inputStream.send(new Object[] {nic2, "Natalie"});

        Thread.sleep(500);
        AssertJUnit.assertEquals(resultContainer.getEventCount(), 2);
        Person person = Person.newBuilder().setName("Jimmy").setNic(nic1).build();
        AssertJUnit.assertTrue(resultContainer.asserProtobufContent(person));
        siddhiManager.shutdown();
    }

    @Test(dependsOnMethods = "testNatsProtobuf")
    public void testIncorrectNatsServerUrl() throws InterruptedException {
        log.info("Test with connection unavailable exception");
        log = Logger.getLogger(Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan8\")"
                + "@sink(type='nats', @map(type='xml'), "
                + "destination='nats-test8', "
                + "server.urls='nats://localhost:5223'"
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

}

