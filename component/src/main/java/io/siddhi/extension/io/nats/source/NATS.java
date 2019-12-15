package io.siddhi.extension.io.nats.source;

import io.nats.streaming.StreamingConnection;
import io.nats.streaming.Subscription;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

public abstract class NATS {

    private static final Logger log = Logger.getLogger(NATS.class);
    protected String destination;
    protected String[] natsUrl;
    protected String queueGroupName;
    protected String siddhiAppName;

    public StateFactory initiateNatsClient(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                           String[] requestedTransportPropertyNames, ConfigReader configReader,
                                           SiddhiAppContext siddhiAppContext) {
        this.siddhiAppName = siddhiAppContext.getName();
        this.destination = optionHolder.validateAndGetStaticValue("destination");

        this.queueGroupName = optionHolder.validateAndGetStaticValue("queue.group.name", null);
        String serverUrls;
        if (optionHolder.isOptionExists("bootstrap.servers")) {
            serverUrls = optionHolder.validateAndGetStaticValue("bootstrap.servers");
        } else {
            serverUrls = optionHolder.validateAndGetStaticValue("server.urls");
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
