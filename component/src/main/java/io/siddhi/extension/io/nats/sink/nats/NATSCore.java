package io.siddhi.extension.io.nats.sink.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.transport.DynamicOptions;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Class which extends NATS to create nats client and publish messages to relevant subjects.
 */
public class NATSCore extends AbstractNats {

    private static final Logger log = Logger.getLogger(NATSCore.class);


    @Override
    public void createNATSClient() throws IOException, InterruptedException {
        this.connection = Nats.connect(natsOptionBuilder.build());
    }

    @Override
    public void publishMessages(Object payload, DynamicOptions dynamicOptions, State state) {
        byte[] messageBytes;
        String subjectName = destination.getValue(dynamicOptions);
        if (payload instanceof byte[]) {
            messageBytes = (byte[]) payload;
        } else {
            String message = (String) payload;
            messageBytes = message.getBytes(StandardCharsets.UTF_8);
        }
        try {
            if (isClientConnected()) {
                createNATSClient();
            }
            connection.publish(subjectName, messageBytes);
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Error sending message to destination:" + subjectName, e); //
        } catch (InterruptedException e) {
            throw new SiddhiAppRuntimeException("Error sending message to destination:" + subjectName
                    + ".The calling thread is interrupted before the call completes.", e);
        }

    }

    @Override
    public void disconnect() throws  TimeoutException, InterruptedException {
        if (connection != null) {
            if (isClientConnected()) {
                connection.flush(Duration.ofMillis(50));
                connection.close();
            }
        }
     }

     private boolean isClientConnected() {
         return connection.getStatus() != Connection.Status.DISCONNECTED && connection.getStatus() != Connection.Status
                 .CLOSED;
     }
}
