package io.siddhi.extension.io.nats.sink.nats;

import io.nats.client.Connection;
import io.nats.client.Options;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.sink.NATSSink;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * An abstract class which holds methods to create and managing nats client.
 */
public abstract class AbstractNats {

    private static final Logger log = Logger.getLogger(AbstractNats.class);
    protected Option destination;
    protected String clientId;
    protected String[] natsUrl;
    protected String streamId;
    protected String siddhiAppName;
    protected NATSSink natsSink;
    protected Options.Builder natsOptionBuilder;
    protected Connection connection;

    public void initiateClient(OptionHolder optionHolder, String siddhiAppName, String streamId) {
        this.destination = optionHolder.validateAndGetOption(NATSConstants.DESTINATION);
        String serverUrls;
        if (optionHolder.isOptionExists(NATSConstants.BOOTSTRAP_SERVERS)) {
            serverUrls = optionHolder.validateAndGetStaticValue(NATSConstants.BOOTSTRAP_SERVERS); // TODO: 12/18/19 Deprecated
        } else {
            serverUrls = optionHolder.validateAndGetStaticValue(NATSConstants.SERVER_URLS);
        }
        natsUrl = serverUrls.split(",");
        for (String url:natsUrl) {
            NATSUtils.validateNatsUrl(url, siddhiAppName);
        }
        this.siddhiAppName = siddhiAppName;
        this.streamId = streamId;
        natsOptionBuilder = new Options.Builder();
        natsOptionBuilder.servers(this.natsUrl);
        if (optionHolder.isOptionExists(NATSConstants.CONNECTION_TIME_OUT)) {
            natsOptionBuilder.connectionTimeout(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.CONNECTION_TIME_OUT))));
        }
        if (optionHolder.isOptionExists(NATSConstants.PING_INTERVAL)) {
            natsOptionBuilder.pingInterval(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.PING_INTERVAL))));
        }
        if (optionHolder.isOptionExists(NATSConstants.MAX_PING_OUTS)) {
            natsOptionBuilder.pingInterval(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.MAX_PING_OUTS))));
        }
        if (optionHolder.isOptionExists(NATSConstants.MAX_RETRY_ATTEMPTS)) {
            natsOptionBuilder.pingInterval(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.MAX_RETRY_ATTEMPTS))));
        }
        if (optionHolder.isOptionExists(NATSConstants.RETRY_BUFFER_SIZE)) {
            natsOptionBuilder.pingInterval(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.RETRY_BUFFER_SIZE))));
        }
    }

    public abstract void createNATSClient() throws IOException, InterruptedException;

    public abstract void publishMessages(Object payload, DynamicOptions dynamicOptions, State state);

    public abstract void disconnect() throws IOException, TimeoutException, InterruptedException;

    public static AbstractNats getNats(OptionHolder optionHolder) {
        if (optionHolder.isOptionExists(NATSConstants.CLUSTER_ID) || optionHolder.isOptionExists(
                NATSConstants.STREAMING_CLUSTER_ID)) {
            return new NATSStreaming();
        }
        return new NATSCore();
    }

    public Option getDestination() {
        return destination;
    }

    public String[] getNatsUrl() {
        return natsUrl.clone();
    }
}
