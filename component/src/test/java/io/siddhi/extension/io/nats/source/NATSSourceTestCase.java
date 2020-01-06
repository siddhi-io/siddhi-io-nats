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
package io.siddhi.extension.io.nats.source;

import com.google.protobuf.AbstractMessageLite;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import io.siddhi.extension.io.nats.utils.NATSClient;
import io.siddhi.extension.io.nats.utils.ResultContainer;
import io.siddhi.extension.io.nats.utils.UnitTestAppender;
import io.siddhi.extension.io.nats.utils.protobuf.Person;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contains test cases for NATS source.
 */
public class NATSSourceTestCase {
    private Logger log = Logger.getLogger(NATSSourceTestCase.class);
    private String clientId;
    private AtomicInteger eventCounter = new AtomicInteger(0);
    private int port;

    @BeforeMethod
    private void setUp() {
        eventCounter.set(0);
    }

    @BeforeClass
    private void initializeDockerContainer() throws InterruptedException {
        GenericContainer simpleWebServer
                = new GenericContainer("nats-streaming:0.11.2");
        eventCounter.set(0);
        simpleWebServer.setPrivilegedMode(true);
        simpleWebServer.start();
        port = simpleWebServer.getMappedPort(4222);
        Thread.sleep(500);
    }

    /**
     * Test the ability to subscripe to a NATS subject from the beginning.
     */
    @Test
    public void testNatsBasicSubscribtion() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test1",
                "nats://localhost:" + port);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "client.id='nats-source-test1-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
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

        natsClient.publish("nats-test1", "<events><event><name>JAMES</name><age>22</age>"
               + "<country>US</country></event></events>");
        natsClient.publish("nats-test1", "<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(100);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * if a property missing from the siddhi stan source which defined as mandatory in the extension definition, then
     * {@link SiddhiAppValidationException} will be thrown.
     */
    @Test(dependsOnMethods = "testNatsBasicSubscribtion", expectedExceptions = SiddhiAppValidationException.class)
    public void testMissingNatsMandatoryProperty() {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name(\"Test-plan2\")"
                + "@source(type='nats', @map(type='xml'), "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "client.id='nats-source-test2-siddhi', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        siddhiManager.shutdown();
    }

    /**
     * If invalid NATS url provided then {@link SiddhiAppValidationException} will be thrown.
     */
    @Test(dependsOnMethods = "testMissingNatsMandatoryProperty",
            expectedExceptions = SiddhiAppValidationException.class)
    public void testInvalidNatsUrl() {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@App:name('Test-plan3')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "bootstrap.servers='natss://localhost:4222', "
                + "client.id='nats-source-test1-siddhi', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition);
        siddhiManager.shutdown();
    }

    /**
     * The load of a subject should be shared between clients when more than one clients subscribes with a same queue
     * group name.
     */
    @Test(dependsOnMethods = "testInvalidNatsUrl")
    public void testQueueGroupSubscription() throws InterruptedException, IOException, TimeoutException {
        clientId = "Test-Plan-4_" + new Date().getTime();
        Thread.sleep(100);
        AtomicInteger instream1Count = new AtomicInteger(0);
        AtomicInteger instream2Count = new AtomicInteger(0);
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition1 = "@App:name('Test-plan4-1')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test4', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "client.id='" + clientId +  "', "
                + "cluster.id='test-cluster',"
                + "queue.group.name = 'test-plan4'"
                + ")"
                + "define stream inputStream1 (name string, age int, country string);";

        clientId = "Test-Plan-5_" + new Date().getTime();
        String inStreamDefinition2 = "@App:name('Test-plan4-2')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test4', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "client.id='" + clientId +  "', "
                + "cluster.id='test-cluster',"
                + "queue.group.name = 'test-plan4'"
                + ")"
                + "define stream inputStream2 (name string, age int, country string);";

        clientId = "Test-Plan-4_" + new Date().getTime();
        NATSClient natsClient = new NATSClient("test-cluster", clientId,
                "nats://localhost:" + port);
        natsClient.connect();

        SiddhiAppRuntime inStream1RT = siddhiManager.createSiddhiAppRuntime(inStreamDefinition1);
        SiddhiAppRuntime inStream2RT = siddhiManager.createSiddhiAppRuntime(inStreamDefinition2);

        inStream1RT.addCallback("inputStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    instream1Count.incrementAndGet();
                }
            }
        });
        inStream2RT.addCallback("inputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    instream2Count.incrementAndGet();
                }
            }
        });
        inStream1RT.start();
        inStream2RT.start();

        natsClient.publish("nats-test4", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>MIKE</name><age>30</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>JHON</name><age>25</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>ARUN</name><age>52</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>ALICE</name><age>32</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>BOP</name><age>28</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>JAKE</name><age>52</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>RAHEEM</name><age>47</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>JANE</name><age>36</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test4", "<events><event><name>LAKE</name><age>19</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(instream1Count.get() != 0, "Total events should be shared between clients");
        Assert.assertTrue(instream2Count.get() != 0, "Total events should be shared between clients");
        Assert.assertEquals(instream1Count.get() + instream2Count.get(), 10);
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * if the client.id is not given by the user in the extension headers, then a randomly generated client id will
     * be used.
     */
    @Test(dependsOnMethods = "testQueueGroupSubscription")
    public void testOptionalClientId() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test-5",
                "nats://localhost:" + port);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan5\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
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
        Thread.sleep(1000);
        natsClient.publish("nats-test1", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test1", "<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * If a single stream has multiple source annotations then all the events from those subjects should be passed to
     * the stream.
     */
    @Test(dependsOnMethods = "testOptionalClientId")
    public void testMultipleSourceSingleStream() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(4, 3);
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test6",
                "nats://localhost:" + port);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan6\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test6-sub1', "
                + "client.id='nats-source-test6-siddhi-1', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "cluster.id='test-cluster'"
                + ")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test6-sub2', "
                + "client.id='nats-source-test6-siddhi-2', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
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
        Thread.sleep(300);

        natsClient.publish("nats-test6-sub1", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test6-sub1", "<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test6-sub2", "<events><event><name>JHON</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test6-sub2", "<events><event><name>SMITH</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(300);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        Assert.assertTrue(resultContainer.assertMessageContent("JHON"));
        Assert.assertTrue(resultContainer.assertMessageContent("SMITH"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Evaluate the subject subscription configuration with the source pause and resume.
     */
    @Test(dependsOnMethods = "testMultipleSourceSingleStream")
    public void testNatsSourcePause() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test7",
                "nats://localhost:" + port);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan7\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test7', "
                + "client.id='nats-source-test7-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
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

        Collection<List<Source>> sources = executionPlanRuntime.getSources();
        executionPlanRuntime.start();
        sources.forEach(e -> e.forEach(Source::pause));
        Thread.sleep(300);

        natsClient.publish("nats-test7", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        sources.forEach(e -> e.forEach(Source::resume));
        natsClient.publish("nats-test7", "<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(300);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Test subscription to a NATS topic based on sequence number with mandatory configurations only.
     */
    @Test(dependsOnMethods = "testNatsSourcePause")
    public void testNatsSequenceSubscribtionWithMandatoryConfigs() throws InterruptedException, IOException,
            TimeoutException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test8",
                "nats://localhost:" + port);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan8\")"
                + "@source(type='nats', @map(type='xml'), "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "destination='nats-test8' "
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
        Thread.sleep(300);

        natsClient.publish("nats-test8", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test8", "<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(300);
        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * If invalid cluster name is provided in NATS source configurations then ConnectionUnavailableException
     * should have been thrown. Here incorrect cluster id provided hence the connection will fail.
     */
    @Test(dependsOnMethods = "testNatsSequenceSubscribtionWithMandatoryConfigs")
    public void testInvalidClusterName() throws InterruptedException {
        log.info("Test with connection unavailable exception");
        log = Logger.getLogger(Source.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan9\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test9', "
                + "client.id='nats-source-test9-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "cluster.id='nats-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.start();
        Thread.sleep(300);
        Assert.assertTrue(appender.getMessages().contains("Error while connecting to NATS server at destination: "
                + "nats-test9"));
        siddhiManager.shutdown();
    }

    /**
     * If incorrect bootstrap server url is provided in NATS source configurations then
     * ConnectionUnavailableException should have been thrown. Here incorrect cluster url is provided hence the
     * connection will fail.
     */
    @Test(dependsOnMethods = "testInvalidClusterName")
    public void testIncorrectNatsServerUrl() throws InterruptedException {
        log.info("Test with connection unavailable exception");
        log = Logger.getLogger(Source.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan10\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test10', "
                + "client.id='nats-source-test10-siddhi', "
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
        Thread.sleep(300);
        Assert.assertTrue(appender.getMessages().contains("Error while connecting to NATS server at destination: "
                + "nats-test10"));
        siddhiManager.shutdown();
    }

    /**
     * Tests the ability to subscribe to a NATS subject from given sequence number.
     */
    @Test(dependsOnMethods = "testIncorrectNatsServerUrl")
    public void testNatsSequenceSubscribtion() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(6, 3);
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test1",
                "nats://localhost:" + port);
        natsClient.connect();

        natsClient.publish("nats-test11", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>MIKE</name><age>30</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>JHON</name><age>25</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>ARUN</name><age>52</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>ALICE</name><age>32</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>BOP</name><age>28</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>JAKE</name><age>52</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>RAHEEM</name><age>47</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>JANE</name><age>36</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test11", "<events><event><name>LAKE</name><age>19</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan11\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test11', "
                + "client.id='nats-source-test11-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "cluster.id='test-cluster',"
                + "subscription.sequence = '5'"
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
        Thread.sleep(300);

        Assert.assertTrue(resultContainer.assertMessageContent("ALICE"));
        Assert.assertTrue(resultContainer.assertMessageContent("BOP"));
        Assert.assertTrue(resultContainer.assertMessageContent("JAKE"));
        Assert.assertTrue(resultContainer.assertMessageContent("RAHEEM"));
        Assert.assertTrue(resultContainer.assertMessageContent("JANE"));
        Assert.assertTrue(resultContainer.assertMessageContent("LAKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * If a sequence subscription is made with a queue group then the corresponding events should be shared between
     * the queue group members.
     */
    @Test(dependsOnMethods = "testNatsSequenceSubscribtion")
    public void testSequenceSubscriptionWithQueueGroup() throws InterruptedException, TimeoutException, IOException {
        clientId = "Test-Plan-12_" + new Date().getTime();
        Thread.sleep(100);

        NATSClient natsClient = new NATSClient("test-cluster", clientId,
                "nats://localhost:" + port);
        natsClient.connect();
        natsClient.publish("nats-test12", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test12", "<events><event><name>MIKE</name><age>30</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test12", "<events><event><name>JHON</name><age>25</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test12", "<events><event><name>ARUN</name><age>52</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test12", "<events><event><name>ALICE</name><age>32</age>"
                + "<country>US</country></event></events>");

        AtomicInteger instream1Count = new AtomicInteger(0);
        AtomicInteger instream2Count = new AtomicInteger(0);
        SiddhiManager siddhiManager = new SiddhiManager();

        clientId = "Test-Plan-12-1_" + new Date().getTime();
        String inStreamDefinition1 = "@App:name('Test-plan12-1')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test12', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "client.id='" + clientId +  "', "
                + "cluster.id='test-cluster',"
                + "subscription.sequence = '4',"
                + "queue.group.name = 'test-plan12'"
                + ")"
                + "define stream inputStream1 (name string, age int, country string);";

        clientId = "Test-Plan-12-2_" + new Date().getTime();
        String inStreamDefinition2 = "@App:name('Test-plan12-2')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test12', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "client.id='" + clientId +  "', "
                + "cluster.id='test-cluster',"
                + "subscription.sequence = '4',"
                + "queue.group.name = 'test-plan12'"
                + ")"
                + "define stream inputStream2 (name string, age int, country string);";

        clientId = "Test-Plan-12_" + new Date().getTime();
        SiddhiAppRuntime inStream1RT = siddhiManager.createSiddhiAppRuntime(inStreamDefinition1);
        SiddhiAppRuntime inStream2RT = siddhiManager.createSiddhiAppRuntime(inStreamDefinition2);

        inStream1RT.addCallback("inputStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    instream1Count.incrementAndGet();
                }
            }
        });
        inStream2RT.addCallback("inputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    instream2Count.incrementAndGet();
                }
            }
        });
        inStream1RT.start();
        inStream2RT.start();

        natsClient.publish("nats-test12", "<events><event><name>BOP</name><age>28</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test12", "<events><event><name>JAKE</name><age>52</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test12", "<events><event><name>RAHEEM</name><age>47</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test12", "<events><event><name>JANE</name><age>36</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test12", "<events><event><name>LAKE</name><age>19</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(instream1Count.get() != 0, "Total events should be shared between clients");
        Assert.assertTrue(instream2Count.get() != 0, "Total events should be shared between clients");
        Assert.assertEquals(instream1Count.get() + instream2Count.get(), 7);
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Tests the ability to persist and retrieve the message sequence number.
     */
    @Test(dependsOnMethods = "testSequenceSubscriptionWithQueueGroup")
    public void testNatsSequencePersistency() throws InterruptedException, TimeoutException, IOException,
            CannotRestoreSiddhiAppStateException {
        ResultContainer resultContainer = new ResultContainer(10, 3);
        InMemoryPersistenceStore inMemoryPersistenceStore = new InMemoryPersistenceStore();
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test13",
                "nats://localhost:" + port);
        natsClient.connect();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(inMemoryPersistenceStore);
        String siddhiApp = "@App:name(\"Test-plan13\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test13', "
                + "client.id='nats-source-test13-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        };
        executionPlanRuntime.addCallback("inputStream", streamCallback);
        executionPlanRuntime.start();

        natsClient.publish("nats-test13", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test13", "<events><event><name>MIKE</name><age>30</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test13", "<events><event><name>JHON</name><age>25</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test13", "<events><event><name>ARUN</name><age>52</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test13", "<events><event><name>ALICE</name><age>32</age>"
                + "<country>US</country></event></events>");
        Thread.sleep(500);

        executionPlanRuntime.persist();
        executionPlanRuntime.shutdown();
        Thread.sleep(300);
        executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", streamCallback);
        natsClient.publish("nats-test13", "<events><event><name>BOP</name><age>28</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test13", "<events><event><name>JAKE</name><age>52</age>"
                + "<country>US</country></event></events>");
        executionPlanRuntime.start();
        executionPlanRuntime.restoreLastRevision();

        natsClient.publish("nats-test13", "<events><event><name>RAHEEM</name><age>47</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test13", "<events><event><name>JANE</name><age>36</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test13", "<events><event><name>LAKE</name><age>19</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(500);

        Assert.assertTrue(resultContainer.assertMessageContent("ALICE"));
        Assert.assertTrue(resultContainer.assertMessageContent("BOP"));
        Assert.assertTrue(resultContainer.assertMessageContent("JAKE"));
        Assert.assertTrue(resultContainer.assertMessageContent("RAHEEM"));
        Assert.assertTrue(resultContainer.assertMessageContent("JANE"));
        Assert.assertTrue(resultContainer.assertMessageContent("LAKE"));

        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Tests the ability to subscribe to a subject with durability. Even the client disconnects in the middle of the
     * subscription then can start to subscribe from the point of failure.
     */
    @Test(dependsOnMethods = "testNatsSequencePersistency")
    public void testDurableSubscription() throws InterruptedException, CannotRestoreSiddhiAppStateException,
            TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(10, 3);
        InMemoryPersistenceStore inMemoryPersistenceStore = new InMemoryPersistenceStore();
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test14",
                "nats://localhost:" + port);
        natsClient.connect();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(inMemoryPersistenceStore);
        String siddhiApp = "@App:name(\"Test-plan14\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test14', "
                + "client.id='nats-source-test14-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "cluster.id='test-cluster',"
                + "durable.name='durability-test'"
                + ")"
                + "define stream inputStream (name string, age int, country string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    resultContainer.eventReceived(event.toString());
                }
            }
        };
        executionPlanRuntime.addCallback("inputStream", streamCallback);
        executionPlanRuntime.start();

        natsClient.publish("nats-test14", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test14", "<events><event><name>MIKE</name><age>30</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test14", "<events><event><name>JHON</name><age>25</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test14", "<events><event><name>ARUN</name><age>52</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test14", "<events><event><name>ALICE</name><age>32</age>"
                + "<country>US</country></event></events>");
        Thread.sleep(500);

        executionPlanRuntime.persist();
        executionPlanRuntime.shutdown();
        Thread.sleep(300);
        executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", streamCallback);
        executionPlanRuntime.start();
        executionPlanRuntime.restoreLastRevision();

        natsClient.publish("nats-test14", "<events><event><name>BOP</name><age>28</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test14", "<events><event><name>JAKE</name><age>52</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test14", "<events><event><name>RAHEEM</name><age>47</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("nats-test14", "<events><event><name>JANE</name><age>36</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test14", "<events><event><name>LAKE</name><age>19</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(500);

        Assert.assertTrue(resultContainer.assertMessageContent("ALICE"));
        Assert.assertTrue(resultContainer.assertMessageContent("BOP"));
        Assert.assertTrue(resultContainer.assertMessageContent("JAKE"));
        Assert.assertTrue(resultContainer.assertMessageContent("RAHEEM"));
        Assert.assertTrue(resultContainer.assertMessageContent("JANE"));
        Assert.assertTrue(resultContainer.assertMessageContent("LAKE"));

        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Test the checks the capability of the NATS source to send protobuf events.
     */
    @Test
    public void testNatsProtobuf()
            throws InterruptedException, TimeoutException,
            IOException, NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test1",
                "nats://localhost:" + port);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan15\")"
                + "@source(type='nats', " +
                "@map(type='protobuf', class='io.siddhi.extension.io.nats.utils.protobuf.Person'), "
                + "destination='nats-test15', "
                + "client.id='nats-source-test15-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "cluster.id='test-cluster'"
                + ")"
                + "define stream inputStream (nic long, name string);"
                + "@info(name = 'query1') "
                + "from inputStream "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        executionPlanRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (int i = 0; i < events.length; i++) {
                    eventCounter.incrementAndGet();
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(100);

        long nic1 = 1222;
        Person person1 = Person.newBuilder().setNic(nic1).setName("Jimmy").build();
        byte[] messageObjectByteArray1 = (byte[]) AbstractMessageLite.class
                .getDeclaredMethod("toByteArray").invoke(person1);
        long nic2 = 1222;
        Person person2 = Person.newBuilder().setNic(nic2).setName("Natalie").build();
        byte[] messageObjectByteArray2 = (byte[]) AbstractMessageLite.class
                .getDeclaredMethod("toByteArray").invoke(person2);
        natsClient.publishProtobufMessage("nats-test15", messageObjectByteArray1);
        natsClient.publishProtobufMessage("nats-test15", messageObjectByteArray2);

        Thread.sleep(100);
        AssertJUnit.assertEquals(eventCounter.get(), 2);
        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Test passing NATS Streaming sequence number as an event attribute.
     */
    @Test
    public void testUsingSeqNumber() throws InterruptedException, TimeoutException, IOException {
        AtomicBoolean eventReceived = new AtomicBoolean(false);
        ResultContainer eventsAtSource = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test1",
                "nats://localhost:" + port);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml', @attributes(name='//events/event/name', " +
                "age='//events/event/age', country='//events/event/country', sequenceNum='trp:sequenceNumber')), "
                + "destination='nats-test1', "
                + "client.id='nats-source-test1-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "cluster.id='test-cluster'"
                + ") "
                + "define stream inputStream (name string, age int, country string, sequenceNum string);"
                + "@info(name = 'query1') "
                + "from inputStream[(math:parseInt(sequenceNum) == 2)] "
                + "select *  "
                + "insert into outputStream;";

        SiddhiAppRuntime siddhiRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    eventsAtSource.eventReceived(event.toString());
                }
            }
        });
        siddhiRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    eventReceived.set(true);
                    Assert.assertTrue(event.getData(0).equals("MIKE"));
                }
            }
        });
        siddhiRuntime.start();
        Thread.sleep(100);

        natsClient.publish("nats-test1", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test1", "<events><event><name>MIKE</name><age>25</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(100);

        Assert.assertTrue(eventsAtSource.assertMessageContent("JAMES"));
        Assert.assertTrue(eventsAtSource.assertMessageContent("MIKE"));
        Assert.assertTrue(eventReceived.get());

        siddhiManager.shutdown();
        natsClient.close();
    }

    /**
     * Test passing NATS Streaming sequence number as an event attribute.
     */
    @Test
    public void testStatePersistence() throws InterruptedException, TimeoutException, IOException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        NATSClient natsClient = new NATSClient("test-cluster", "nats-source-test1",
                "nats://localhost:" + port);
        natsClient.connect();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "client.id='nats-source-test1-siddhi', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "', "
                + "cluster.id='test-cluster'"
                + ") "
                + "define stream inputStream (name string, age int, country string);";

        SiddhiAppRuntime siddhiRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventCounter.incrementAndGet();
            }
        });

        natsClient.publish("nats-test1", "<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test1", "<events><event><name>MIKE</name><age>25</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        siddhiRuntime.start();

        //persisting
        Thread.sleep(2000);
        siddhiRuntime.persist();
        Thread.sleep(1000);
        siddhiRuntime.shutdown();

        natsClient.publish("nats-test1", "<events><event><name>JAKE</name><age>19</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("nats-test1", "<events><event><name>CHARLIE</name><age>30</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(2000);

        siddhiRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiRuntime.addCallback("inputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventCounter.incrementAndGet();
            }
        });

        //loading
        try {
            siddhiRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiRuntime.getName() + " failed", e);
        }
        siddhiRuntime.start();
        Thread.sleep(3000);

        AssertJUnit.assertTrue(eventCounter.get() == 4);
        siddhiManager.shutdown();
        natsClient.close();
    }
}


