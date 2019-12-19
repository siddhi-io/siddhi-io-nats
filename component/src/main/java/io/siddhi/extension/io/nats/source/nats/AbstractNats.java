package io.siddhi.extension.io.nats.source.nats;

import io.nats.client.Options;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

import java.time.Duration;

/**
 * An abstract class which holds methods to create and managing nats client.
 */
public abstract class AbstractNats {

    private static final Logger log = Logger.getLogger(AbstractNats.class);
    protected String destination;
    protected String[] natsUrl;
    protected String queueGroupName;
    protected String siddhiAppName;
    protected String streamId;
    protected Options.Builder natsOptionBuilder;

    public StateFactory initiateNatsClient(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                           String[] requestedTransportPropertyNames, ConfigReader configReader,
                                           SiddhiAppContext siddhiAppContext) {
        this.siddhiAppName = siddhiAppContext.getName();
        this.streamId = sourceEventListener.getStreamDefinition().getId();
        this.destination = optionHolder.validateAndGetStaticValue(NATSConstants.DESTINATION);
        this.queueGroupName = optionHolder.validateAndGetStaticValue(NATSConstants.QUEUE_GROUP_NAME, null);
        String serverUrls;
        if (optionHolder.isOptionExists(NATSConstants.BOOTSTRAP_SERVERS)) {
            serverUrls = optionHolder.validateAndGetStaticValue(NATSConstants.BOOTSTRAP_SERVERS);
        } else {
            serverUrls = optionHolder.validateAndGetStaticValue(NATSConstants.SERVER_URLS);
        }
        natsUrl = serverUrls.split(",");
        for (String url:natsUrl) {
            NATSUtils.validateNatsUrl(url, siddhiAppName);
        }
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
        return null;
    }

    public abstract void createConnection(Source.ConnectionCallback connectionCallback, State state)
            throws ConnectionUnavailableException;

    public abstract void disconnect();
    public abstract void pause();
    public abstract void resume();

    public static AbstractNats getNATS(OptionHolder optionHolder) {
        if (optionHolder.isOptionExists(NATSConstants.CLUSTER_ID) || optionHolder.isOptionExists(
                NATSConstants.STREAMING_CLUSTER_ID)) {
            return new NATSStreaming();
        }
        return new NATSCore();
    }

}
