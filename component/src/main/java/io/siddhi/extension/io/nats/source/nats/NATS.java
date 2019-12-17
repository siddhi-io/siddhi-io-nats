package io.siddhi.extension.io.nats.source.nats;

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

/**
 * An abstract class which holds methods to create and managing nats client.
 */
public abstract class NATS {

    private static final Logger log = Logger.getLogger(NATS.class);
    protected String destination;
    protected String[] natsUrl;
    protected String queueGroupName;
    protected String siddhiAppName;
    protected String streamId;

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
        return null;
    }

    public abstract void createConnection(Source.ConnectionCallback connectionCallback, State state)
            throws ConnectionUnavailableException;

    public abstract void disconnect();
    public abstract void pause();
    public abstract void resume();

    public static NATS getNATS(boolean isStreaming) {
        if (isStreaming) {
            return new NATSStreaming();
        }
        return new NATSCore();
    }

}
