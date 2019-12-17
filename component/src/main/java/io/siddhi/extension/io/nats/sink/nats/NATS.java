package io.siddhi.extension.io.nats.sink.nats;

import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.sink.NATSSink;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * An abstract class which holds methods to create and managing nats client.
 */
public abstract class NATS {

    private static final Logger log = Logger.getLogger(NATS.class);
    protected Option destination;
    protected String clientId;
    protected String[] natsUrl;
    protected String streamId;
    protected String siddhiAppName;
    protected NATSSink natsSink;

    public void initiateClient(OptionHolder optionHolder, String siddhiAppName, String streamId) {
        this.destination = optionHolder.validateAndGetOption(NATSConstants.DESTINATION);
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
        this.siddhiAppName = siddhiAppName;
        this.streamId = streamId;
    }

    public abstract void createNATSClient() throws IOException, InterruptedException;

    public abstract void publishMessages(Object payload, DynamicOptions dynamicOptions, State state);

    public abstract void disconnect() throws IOException, TimeoutException, InterruptedException;

    public void setNatsSink(NATSSink natsSink) {
        this.natsSink = natsSink;
    }

    public static NATS getNats(boolean isNATSStreaming) {
        if (isNATSStreaming) {
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
