package io.siddhi.extension.io.nats.source;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NATSCore extends NATS {

    private Options options;
    private Connection natsClient;
    private SourceEventListener sourceEventListener;
    private Lock lock;
    private Condition condition;
    private boolean pause;

    @Override
    public StateFactory initiateNatsClient(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                           String[] requestedTransportPropertyNames, ConfigReader configReader,
                                           SiddhiAppContext siddhiAppContext) {
        super.initiateNatsClient(sourceEventListener, optionHolder, requestedTransportPropertyNames, configReader,
                siddhiAppContext);
        this.sourceEventListener = sourceEventListener;
        options = new Options.Builder()
                .servers(this.natsUrl)
                .maxReconnects(-1)
                .build();
        lock = new ReentrantLock();
        condition = lock.newCondition();
        return null;
    }

    @Override
    public void createConnection(Source.ConnectionCallback connectionCallback, State state) throws ConnectionUnavailableException {
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
                String str = new String(msg.getData(), StandardCharsets.UTF_8);
                sourceEventListener.onEvent(str, null); //todo are there any transport properties???
                System.out.println(str + " " + msg.getReplyTo());
            });
            dispatcher.subscribe(destination); // TODO: 12/15/19 queue group
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void disconnect() {
        if (natsClient != null && natsClient.getStatus() != Connection.Status.CLOSED) {
            try {
                natsClient.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
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
