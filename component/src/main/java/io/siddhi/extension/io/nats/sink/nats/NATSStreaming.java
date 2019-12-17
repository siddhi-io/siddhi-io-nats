package io.siddhi.extension.io.nats.sink.nats;

import io.nats.streaming.ConnectionLostHandler;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.sink.AsyncAckHandler;
import io.siddhi.extension.io.nats.sink.exception.NATSSinkAdaptorRuntimeException;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class which extends NATS to create nats streaming client and publish messages to relevant subject.
 */
public class NATSStreaming extends NATS {

    private static final Logger log = Logger.getLogger(NATS.class);
    private AtomicBoolean isConnectionClosed = new AtomicBoolean(false);
    private StreamingConnection streamingConnection;
    private Options options;
    private String clusterId;

    @Override
    public void initiateClient(OptionHolder optionHolder, String siddhiAppName, String streamId) {
        super.initiateClient(optionHolder, siddhiAppName, streamId);
        if (optionHolder.isOptionExists("cluster.id")) {
            this.clusterId = optionHolder.validateAndGetStaticValue("cluster.id");
        } else if (optionHolder.isOptionExists("streaming.cluster.id")) {
            this.clusterId = optionHolder.validateAndGetStaticValue("streaming.cluster.id");
        }
        this.clientId = optionHolder.validateAndGetStaticValue(NATSConstants.CLIENT_ID, NATSUtils.createClientId());
        if (natsUrl.length > 1) {
            log.warn("NATS streaming does not support for multiple urls, hence getting the first url: '" + natsUrl[0]
                    + "'.");
        }
        this.options = new Options.Builder().natsUrl(natsUrl[0]).
                clientId(this.clientId).clusterId(this.clusterId).
                connectionLostHandler(new NATSStreaming.NATSConnectionLostHandler()).build();
    }

    @Override
    public void createNATSClient() throws IOException, InterruptedException {
        StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(options);
        streamingConnection = streamingConnectionFactory.createConnection();
        isConnectionClosed.set(false);
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
            if (isConnectionClosed.get()) {
                streamingConnection.close();
                createNATSClient();
            }
            streamingConnection.publish(subjectName, messageBytes,
                    new AsyncAckHandler(siddhiAppName, natsUrl[0], payload, natsSink, dynamicOptions));
        } catch (IOException e) {
            log.error("Error sending message to destination: " + subjectName);
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName, e);
        } catch (InterruptedException e) {
            log.error("Error sending message to destination: " + subjectName + ".The calling thread is "
                    + "interrupted before the call completes.");
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName
                    + ".The calling thread is interrupted before the call completes.", e);
        } catch (TimeoutException e) {
            log.error("Error sending message to destination: " + subjectName + ".Timeout occured while trying to ack.");
            throw new NATSSinkAdaptorRuntimeException("Error sending message to destination:" + subjectName
                    + ".Timeout occured while trying to ack.", e);
        }

    }

    @Override
    public void disconnect() throws IOException, TimeoutException, InterruptedException {
        if (streamingConnection != null) {
            streamingConnection.close();
        }
    }

    class NATSConnectionLostHandler implements ConnectionLostHandler {
        @Override
        public void connectionLost(StreamingConnection streamingConnection, Exception e) {
            log.error("Exception occurred in Siddhi App " + siddhiAppName +
                    " when publishing messages to NATS endpoint " + natsUrl + " . " + e.getMessage(), e);
            isConnectionClosed = new AtomicBoolean(true);
        }
    }
}


