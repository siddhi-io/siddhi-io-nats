package io.siddhi.extension.io.nats.utils;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Nats client for running test cases.
 */
public class NATSClient {

    private String natsUrl = "nats://localhost:4222";
    private Connection nc;
    private String subject;
    private ResultContainer resultContainer;
    private boolean isProtobuf;

    public NATSClient(String subject, ResultContainer resultContainer) {
        this.subject = subject;
        this.resultContainer = resultContainer;
    }

    public NATSClient(String subject, ResultContainer resultContainer, boolean isProtobuf) {
        this.subject = subject;
        this.resultContainer = resultContainer;
        this.isProtobuf = isProtobuf;
    }

    public void connectClient() {
        Options o = new Options.Builder().server(natsUrl).build();
        try {
            nc = Nats.connect(o);
        } catch (IOException | InterruptedException e) {
        }
    }

    public void subscribe() {
        Dispatcher d = nc.createDispatcher((msg) -> {
            if (isProtobuf) {
                resultContainer.eventReceived(msg.getData());
            } else {
                resultContainer.eventReceived(new String(msg.getData(), StandardCharsets.UTF_8));
            }
        });
        d.subscribe(subject);
    }

    public void publish(String message) {
        nc.publish(subject, message.getBytes());
    }
    public void publishProtoBuf(byte[] message) {
        nc.publish(subject, message);
    }

    public void close() throws InterruptedException {
        nc.close();
    }

}
