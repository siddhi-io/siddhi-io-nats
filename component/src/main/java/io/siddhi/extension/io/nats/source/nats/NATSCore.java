package io.siddhi.extension.io.nats.source.nats;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.siddhi.extension.io.nats.util.NATSConstants.SEQUENCE_NUMBER;

/**
 * Class which extends NATS to create nats client and receive messages from relevant subjects.
 */
public class NATSCore extends AbstractNats {

    private static final Logger log = Logger.getLogger(NATSCore.class);
    private Connection natsClient;
    private SourceEventListener sourceEventListener;
    private Lock lock;
    private Condition condition;
    private boolean pause;
    private AtomicInteger messageSequenceTracker; // TODO: 12/18/19 check a way to get the message number
    private String[] requestedTransportPropertyNames;

    @Override
    public StateFactory initiateNatsClient(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                           String[] requestedTransportPropertyNames, ConfigReader configReader,
                                           SiddhiAppContext siddhiAppContext) {
        super.initiateNatsClient(sourceEventListener, optionHolder, requestedTransportPropertyNames, configReader,
                siddhiAppContext);
        this.sourceEventListener = sourceEventListener;
        this.messageSequenceTracker = new AtomicInteger(0);
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.requestedTransportPropertyNames = requestedTransportPropertyNames.clone();
        return null;
    }

    @Override
    public void createConnection(Source.ConnectionCallback connectionCallback, State state)
            throws ConnectionUnavailableException {
        try {
            natsClient = Nats.connect(natsOptionBuilder.build());
            Dispatcher dispatcher = natsClient.createDispatcher((msg) -> {
                if (pause) {
                    lock.lock();
                    try {
                        condition.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();
                    }
                }
                messageSequenceTracker.incrementAndGet();
                String[] properties = new String[requestedTransportPropertyNames.length];
                for (int i = 0; i < requestedTransportPropertyNames.length; i++) {
                    if (requestedTransportPropertyNames[i].equalsIgnoreCase(SEQUENCE_NUMBER)) {
                        properties[i] = String.valueOf(messageSequenceTracker.get());
                    }
                }
                sourceEventListener.onEvent(msg.getData(), properties);
            });
            if (queueGroupName != null) {
                dispatcher.subscribe(destination, queueGroupName);
            } else {
                dispatcher.subscribe(destination);
            }
        } catch (IOException e) {
            throw new ConnectionUnavailableException("Error occurred in initializing the NATS receiver in " +
                    siddhiAppName + " for stream: " + streamId);
        } catch (InterruptedException e) {
            throw new SiddhiAppRuntimeException("Error occurred in initializing the NATS receiver in " +
                    siddhiAppName + " for stream: " + streamId + ".The calling thread is interrupted before the " +
                    "connection completes.");
        }
    }

    @Override
    public void disconnect() {
        if (natsClient != null && natsClient.getStatus() != Connection.Status.CLOSED) {
            try {
                natsClient.close();
            } catch (InterruptedException e) {
                log.error("Error while disconnecting the nats client. Thread was interrupted before closing the " +
                        "connection.");
            } finally {
                natsClient = null;
            }
        }
    }

    @Override
    public void pause() {
        pause = true;
    }

    @Override
    public void resume() {
        pause = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
