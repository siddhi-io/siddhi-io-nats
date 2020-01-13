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
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.io.nats.utils.NATSClient;
import io.siddhi.extension.io.nats.utils.ResultContainer;
import io.siddhi.extension.io.nats.utils.protobuf.Person;
import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contains test cases for Nats source.
 */
public class NATSSourceTestCase {

    private AtomicInteger eventCounter = new AtomicInteger(0);
    private int port = 4222;

    @BeforeMethod
    private void setUp() {
        eventCounter.set(0);
    }

    @BeforeClass
    private void initializeDockerContainer() throws InterruptedException {
        GenericContainer simpleWebServer = new GenericContainer("nats");
        eventCounter.set(0);
        simpleWebServer.setPrivilegedMode(true);
        simpleWebServer.start();
        port = simpleWebServer.getMappedPort(4222);
        Thread.sleep(500);
    }

    @Test
    public void testNatsBasicSubscribtion() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("nats-test1", resultContainer, port);
        natsClient.connectClient();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan1\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test1', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "'"
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

        natsClient.publish("<events><event><name>JAMES</name><age>23</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>MIKE</name><age>23</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(100);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }

    @Test
    public void testNatsProtobuf()
            throws InterruptedException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("nats-test15", resultContainer, port, true);
        natsClient.connectClient();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan15\")"
                + "@source(type='nats', " +
                "@map(type='protobuf', class='io.siddhi.extension.io.nats.utils.protobuf.Person'), "
                + "destination='nats-test15', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "' "
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
        byte[] messageObjectByteArray1 = person1.toByteArray();
        long nic2 = 1222;
        Person person2 = Person.newBuilder().setNic(nic2).setName("Natalie").build();
        byte[] messageObjectByteArray2 = person2.toByteArray();
        natsClient.publishProtoBuf(messageObjectByteArray1);
        natsClient.publishProtoBuf(messageObjectByteArray2);

        Thread.sleep(100);
        AssertJUnit.assertEquals(eventCounter.get(), 2);
        siddhiManager.shutdown();
        natsClient.close();
    }

    @Test
    public void testQueueGroupSubscription() throws InterruptedException {
        Thread.sleep(100);
        AtomicInteger instream1Count = new AtomicInteger(0);
        AtomicInteger instream2Count = new AtomicInteger(0);
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition1 = "@App:name('Test-plan3-1')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test3', "
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "queue.group.name = 'test-group'"
                + ")"
                + "define stream inputStream1 (name string, age int, country string);";

        String inStreamDefinition2 = "@App:name('Test-plan3-2')"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test3', "
                + "server.urls='" + "nats://localhost:" + port + "', "
                + "queue.group.name = 'test-group'"
                + ")"
                + "define stream inputStream2 (name string, age int, country string);";

        NATSClient natsClient = new NATSClient("nats-test3", port);
        natsClient.connectClient();

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

        natsClient.publish("<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>MIKE</name><age>30</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("<events><event><name>JHON</name><age>25</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>ARUN</name><age>52</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("<events><event><name>ALICE</name><age>32</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>BOP</name><age>28</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("<events><event><name>JAKE</name><age>52</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>RAHEEM</name><age>47</age>"
                + "<country>GERMANY</country></event></events>");
        natsClient.publish("<events><event><name>JANE</name><age>36</age>"
                + "<country>US</country></event></events>");
        natsClient.publish("<events><event><name>LAKE</name><age>19</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(1000);

        Assert.assertTrue(instream1Count.get() != 0, "Total events should be shared between clients");
        Assert.assertTrue(instream2Count.get() != 0, "Total events should be shared between clients");
        Assert.assertEquals(instream1Count.get() + instream2Count.get(), 10);
        siddhiManager.shutdown();
        natsClient.close();
    }

    @Test
    public void testNatsSourcePause() throws InterruptedException, TimeoutException, IOException {
        ResultContainer resultContainer = new ResultContainer(2, 3);
        NATSClient natsClient = new NATSClient("nats-test7", resultContainer, port);
        natsClient.connectClient();
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name(\"Test-plan7\")"
                + "@source(type='nats', @map(type='xml'), "
                + "destination='nats-test7', "
                + "bootstrap.servers='" + "nats://localhost:" + port + "' "
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

        natsClient.publish("<events><event><name>JAMES</name><age>22</age>"
                + "<country>US</country></event></events>");
        sources.forEach(e -> e.forEach(Source::resume));
        natsClient.publish("<events><event><name>MIKE</name><age>22</age>"
                + "<country>GERMANY</country></event></events>");
        Thread.sleep(300);

        Assert.assertTrue(resultContainer.assertMessageContent("JAMES"));
        Assert.assertTrue(resultContainer.assertMessageContent("MIKE"));
        siddhiManager.shutdown();
        natsClient.close();
    }
}
