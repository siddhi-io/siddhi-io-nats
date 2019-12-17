package io.siddhi.extension.io.nats.source.nats;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.source.exception.NATSInputAdaptorRuntimeException;
import io.siddhi.extension.io.nats.util.NATSConstants;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.siddhi.extension.io.nats.util.NATSConstants.SEQUENCE_NUMBER;

/**
 * Class which extends NATS to create nats client and receive messages from relevant subjects.
 */
public class NATSCore extends NATS {

    private static final Logger log = Logger.getLogger(NATSCore.class);
    private Options options;
    private Connection natsClient;
    private SourceEventListener sourceEventListener;
    private Lock lock;
    private Condition condition;
    private boolean pause;
    private AtomicInteger messageSequenceTracker;
    private String[] requestedTransportPropertyNames;

    @Override
    public StateFactory initiateNatsClient(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                           String[] requestedTransportPropertyNames, ConfigReader configReader,
                                           SiddhiAppContext siddhiAppContext) {
        super.initiateNatsClient(sourceEventListener, optionHolder, requestedTransportPropertyNames, configReader,
                siddhiAppContext);
        this.sourceEventListener = sourceEventListener;
        this.messageSequenceTracker = new AtomicInteger(0);
        Options.Builder optionBuilder = new Options.Builder();
        optionBuilder.servers(this.natsUrl);
        if (optionHolder.isOptionExists(NATSConstants.CONNECTION_TIME_OUT)) {
            optionBuilder.connectionTimeout(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.CONNECTION_TIME_OUT))));
        }
        if (optionHolder.isOptionExists(NATSConstants.PING_INTERVAL)) {
            optionBuilder.pingInterval(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.PING_INTERVAL))));
        }
        if (optionHolder.isOptionExists(NATSConstants.MAX_PING_OUTS)) {
            optionBuilder.pingInterval(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.MAX_PING_OUTS))));
        }
        if (optionHolder.isOptionExists(NATSConstants.MAX_RETRY_ATTEMPTS)) {
            optionBuilder.pingInterval(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.MAX_RETRY_ATTEMPTS))));
        }
        if (optionHolder.isOptionExists(NATSConstants.RETRY_BUFFER_SIZE)) {
            optionBuilder.pingInterval(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.RETRY_BUFFER_SIZE))));
        }
        this.options = optionBuilder.build();
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.requestedTransportPropertyNames = requestedTransportPropertyNames;
        return null;
    }

    @Override
    public void createConnection(Source.ConnectionCallback connectionCallback, State state)
            throws ConnectionUnavailableException {
        try {
            natsClient = Nats.connect(options);
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
                } // TODO: 12/16/19 can be removed
                String str = new String(msg.getData(), StandardCharsets.UTF_8);
                sourceEventListener.onEvent(str, properties); //todo are there any transport properties???
                if (log.isDebugEnabled()) {
                    log.debug("message: " + msg);
                }
            });
            if (queueGroupName != null) {
                dispatcher.subscribe(destination, queueGroupName);
            } else {
                dispatcher.subscribe(destination);
            }
        } catch (IOException e) {
            log.error("Error occurred in initializing the NATS receiver in " + siddhiAppName + " for stream: " +
                    streamId);
            throw new ConnectionUnavailableException("Error occurred in initializing the NATS receiver in " +
                    siddhiAppName + " for stream: " + streamId);
        } catch (InterruptedException e) {
            log.error("Error occurred in initializing the NATS receiver in " + siddhiAppName + " for stream: " +
                    streamId + ".The calling thread is interrupted before the connection completes.");
            throw new NATSInputAdaptorRuntimeException("Error occurred in initializing the NATS receiver in " +
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
