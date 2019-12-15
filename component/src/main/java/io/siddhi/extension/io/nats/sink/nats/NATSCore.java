package io.siddhi.extension.io.nats.sink.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.sink.exception.NATSSinkAdaptorRuntimeException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class NATSCore extends NATS {

    private static final Logger log = Logger.getLogger(NATSCore.class);
    private Options options;
    private Connection connection;

    @Override
    public void initiateClient(OptionHolder optionHolder, String siddhiAppName, String streamId) {
        super.initiateClient(optionHolder, siddhiAppName, streamId);
        this.options = new Options.Builder().servers(natsUrl)
                .maxReconnects(-1).build();
        // TODO: 12/13/19 Validate and set other parameters
    }

    @Override
    public void createNATSClient() throws IOException, InterruptedException {
        this.connection = Nats.connect(options);
    }

    @Override
    public void publishMessages(Object payload, DynamicOptions dynamicOptions, State state) {
        String message = (String)payload;
        String subjectName = destination.getValue();
        try {
            if (connection.getStatus() == Connection.Status.DISCONNECTED || connection.getStatus() == Connection.Status
                    .CLOSED) {
                createNATSClient();
            }
            connection.publish(subjectName, message.getBytes());
        } catch (IOException e) {
            log.error("Error sending message to destination: " + subjectName);
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName, e);
        } catch (InterruptedException e) {
            log.error("Error sending message to destination: " + subjectName + ".The calling thread is "
                    + "interrupted before the call completes.");
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName
                    + ".The calling thread is interrupted before the call completes.", e);
        }

    }

    @Override
    public void disconnect() throws  TimeoutException, InterruptedException {
        if (connection != null) {
            if (connection.getStatus() != Connection.Status.DISCONNECTED && connection.getStatus() != Connection.Status
                    .CLOSED) {
                connection.flush(Duration.ofMillis(50));
                connection.close();
            }
        }
     }
}
