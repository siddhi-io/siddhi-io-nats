package io.siddhi.extension.io.nats.source.nats;

import io.nats.streaming.ConnectionLostHandler;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.source.NATSMessageProcessor;
import io.siddhi.extension.io.nats.source.exception.NATSInputAdaptorRuntimeException;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class which extends NATS to create nats streaming client and receive messages from relevant subject.
 */
public class NATSStreaming extends NATS {

    private static final Logger log = Logger.getLogger(NATSStreaming.class);
    private String durableName;
    private String  sequenceNumber;
    private Subscription subscription;
    private NATSMessageProcessor natsMessageProcessor;
    private String[] reqTransportPropertyNames;
    private String clusterId;
    private String clientId;
    private StreamingConnection streamingConnection;
    private SourceEventListener sourceEventListener;

    @Override
    public StateFactory<NATSSourceState> initiateNatsClient(SourceEventListener sourceEventListener
            , OptionHolder optionHolder, String[] requestedTransportPropertyNames, ConfigReader configReader
            , SiddhiAppContext siddhiAppContext) {
        super.initiateNatsClient(sourceEventListener, optionHolder, requestedTransportPropertyNames, configReader,
                siddhiAppContext);
        this.sourceEventListener = sourceEventListener;
        this.reqTransportPropertyNames = requestedTransportPropertyNames.clone();
        this.clusterId = optionHolder.validateAndGetStaticValue(NATSConstants.CLUSTER_ID,
                NATSConstants.DEFAULT_CLUSTER_ID);
        this.clientId = optionHolder.validateAndGetStaticValue(NATSConstants.CLIENT_ID, NATSUtils.createClientId());
        if (optionHolder.isOptionExists(NATSConstants.DURABLE_NAME)) {
            this.durableName = optionHolder.validateAndGetStaticValue(NATSConstants.DURABLE_NAME);
        }
        if (optionHolder.isOptionExists(NATSConstants.SUBSCRIPTION_SEQUENCE)) {
            this.sequenceNumber = optionHolder.validateAndGetStaticValue(NATSConstants.SUBSCRIPTION_SEQUENCE);
        }
        return NATSSourceState::new;
    }

    @Override
    public void createConnection(Source.ConnectionCallback connectionCallback, State state)
            throws ConnectionUnavailableException {
        try {
            if (natsUrl.length > 1) {
                log.warn("NATS streaming does not support for multiple urls, hence getting the first url: '"
                        + natsUrl[0] + "'.");
            }
            Options options = new Options.Builder().natsUrl(this.natsUrl[0]).
                    clientId(this.clientId).clusterId(this.clusterId).
                    connectionLostHandler(new NATSConnectionLostHandler(connectionCallback)).build();
            StreamingConnectionFactory streamingConnectionFactory = new StreamingConnectionFactory(options);
            streamingConnection =  streamingConnectionFactory.createConnection();
        } catch (IOException e) {
            log.error("Error while connecting to NATS server at destination: " + destination);
            throw new ConnectionUnavailableException("Error while connecting to NATS server at destination: "
                    + destination, e);
        } catch (InterruptedException e) {
            log.error("Error while connecting to NATS server at destination: " + destination + ".The calling thread "
                    + "is interrupted before the connection can be established.");
            throw new ConnectionUnavailableException("Error while connecting to NATS server at destination: "
                    + destination + " .The calling thread is interrupted before the connection can be established.", e);
        }
        subscribe((NATSSourceState) state);
    }

    private void subscribe(NATSSourceState natsSourceState) {
        SubscriptionOptions.Builder subscriptionOptionsBuilder = new SubscriptionOptions.Builder();
        if (sequenceNumber != null && natsSourceState.lastSentSequenceNo.intValue() <
                Integer.parseInt(sequenceNumber)) {
            natsSourceState.lastSentSequenceNo.set(Integer.parseInt(sequenceNumber));
        }
        subscriptionOptionsBuilder.startAtSequence(natsSourceState.lastSentSequenceNo.get());
        try {

            if (durableName != null) {
                subscriptionOptionsBuilder.durableName(durableName);
            }
            natsMessageProcessor = new NATSMessageProcessor(sourceEventListener, reqTransportPropertyNames,
                    natsSourceState.lastSentSequenceNo);
            if (queueGroupName != null) {
                subscription =  streamingConnection.subscribe(destination , queueGroupName, natsMessageProcessor,
                        subscriptionOptionsBuilder.build());
            } else {
                subscription =  streamingConnection.subscribe(destination , natsMessageProcessor,
                        subscriptionOptionsBuilder.build());
            }

        } catch (IOException e) {
            log.error("Error occurred in initializing the NATS receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId());
            throw new NATSInputAdaptorRuntimeException("Error occurred in initializing the NATS receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId(), e);
        } catch (InterruptedException e) {
            log.error("Error occurred in initializing the NATS receiver for stream: " + sourceEventListener
                    .getStreamDefinition().getId() + ".The calling thread is interrupted before the connection "
                    + "completes.");
            throw new NATSInputAdaptorRuntimeException("Error occurred in initializing the NATS receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId() + ".The calling thread is interrupted before "
                    + "the connection completes.", e);
        } catch (TimeoutException e) {
            log.error("Error occurred in initializing the NATS receiver for stream: " + sourceEventListener
                    .getStreamDefinition().getId() + ".The server request cannot be completed within the subscription"
                    + " timeout.");
            throw new NATSInputAdaptorRuntimeException("Error occurred in initializing the NATS receiver for stream: "
                    + sourceEventListener.getStreamDefinition().getId() + ".The server request cannot be completed "
                    + "within the subscription timeout.", e);
        }
    }

    @Override
    public void disconnect() {
        try {
            if (streamingConnection != null) {
                streamingConnection.close();
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("Error disconnecting the Stan receiver", e);
        }
    }

    @Override
    public void pause() {
        if (natsMessageProcessor != null) {
            natsMessageProcessor.pause();
            if (log.isDebugEnabled()) {
                log.debug("Nats source paused for destination: " + destination);
            }
        }
    }

    @Override
    public void resume() {
        if (natsMessageProcessor != null) {
            natsMessageProcessor.resume();
            if (log.isDebugEnabled()) {
                log.debug("Nats source resumed for destination: " + destination);
            }
        }
    }

    class NATSSourceState extends State {
        private AtomicInteger lastSentSequenceNo = new AtomicInteger(0);

        @Override public boolean canDestroy() {
            return lastSentSequenceNo.intValue() == 0;
        }

        /**
         * Used to serialize and persist {@link #lastSentSequenceNo} in a configurable interval.
         * @return stateful objects of the processing element as a map
         */
        @Override public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put(siddhiAppName, lastSentSequenceNo.get());
            return state;
        }

        /**
         * Used to get the persisted {@link #lastSentSequenceNo} value in case of client connection failure so that
         * replay the missing messages/events.
         * @param map the stateful objects of the processing element as a map.
         */
        @Override public void restore(Map<String, Object> map) {
            Object seqObject = map.get(siddhiAppName);
            if (seqObject != null && sequenceNumber == null) {
                lastSentSequenceNo.set((int) seqObject);
            }
        }
    }

    class NATSConnectionLostHandler implements ConnectionLostHandler {
        private Source.ConnectionCallback connectionCallback;

        NATSConnectionLostHandler(Source.ConnectionCallback connectionCallback) {
            this.connectionCallback = connectionCallback;
        }

        @Override
        public void connectionLost(StreamingConnection streamingConnection, Exception e) {
            log.error("Exception occurred in Siddhi App" + siddhiAppName +
                    " when consuming messages from NATS endpoint " + natsUrl[0] + " . " + e.getMessage(), e);
            Thread thread = new Thread() {
                public void run() {
                    connectionCallback.onError(new ConnectionUnavailableException(e));
                }
            };

            thread.start();
        }
    }
}
