package io.siddhi.extension.io.nats.source.nats;

import io.nats.client.Options;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.nats.util.NATSConstants;
import io.siddhi.extension.io.nats.util.NATSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Duration;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

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
    protected String authType;
    protected String keyStorePath = "${carbon.home}/resources/security/wso2carbon.jks";
    protected String trustStorePath = "${carbon.home}/resources/security/client-truststore.jks";
    protected char[] storePassword = "wso2carbon".toCharArray();
    protected char[] keyPassword = "wso2carbon".toCharArray();
    protected String keyStoreAlgorithm = "SunX509";
    protected String trustStoreAlgorithm = "SunX509";
    protected String tlsStoreType = "JKS";

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
            natsOptionBuilder.maxPingsOut(Integer.parseInt(optionHolder.validateAndGetStaticValue
                    (NATSConstants.MAX_PING_OUTS)));
        }
        if (optionHolder.isOptionExists(NATSConstants.MAX_RETRY_ATTEMPTS)) {
            natsOptionBuilder.maxReconnects(Integer.parseInt(optionHolder.validateAndGetStaticValue
                    (NATSConstants.MAX_RETRY_ATTEMPTS)));
        }
        if (optionHolder.isOptionExists(NATSConstants.RECONNECT_WAIT)) {
            natsOptionBuilder.reconnectWait(Duration.ofSeconds(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.RECONNECT_WAIT))));
        }
        if (optionHolder.isOptionExists(NATSConstants.RETRY_BUFFER_SIZE)) {
            natsOptionBuilder.reconnectBufferSize(Long.parseLong(optionHolder.validateAndGetStaticValue
                    (NATSConstants.RETRY_BUFFER_SIZE)));
        }
        if (optionHolder.isOptionExists(NATSConstants.AUTH_TYPE)) {
            authType = optionHolder.validateAndGetStaticValue(NATSConstants.AUTH_TYPE);
            addAuthentication(optionHolder);
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

    protected void addAuthentication(OptionHolder optionHolder) {
        if (authType.equalsIgnoreCase(NATSConstants.AUTH_TYPE_LIST.get(0))) { //username and password
            char[] username = optionHolder.validateAndGetStaticValue("username").toCharArray();
            char[] password = optionHolder.validateAndGetStaticValue("password").toCharArray();
            natsOptionBuilder.userInfo(username, password);
        } else if (authType.equalsIgnoreCase(NATSConstants.AUTH_TYPE_LIST.get(1))) {
            char[] token = optionHolder.validateAndGetStaticValue("token").toCharArray();
            natsOptionBuilder.token(token);
        } else if (authType.equalsIgnoreCase(NATSConstants.AUTH_TYPE_LIST.get(2))) {
            initializeTrustStoreAttributes(optionHolder);
            try {
                TrustManager[] trustManagers = NATSUtils.createTrustManagers(trustStorePath, storePassword,
                        trustStoreAlgorithm, tlsStoreType);
                KeyManager[] keyManagers = null;
                if (optionHolder.isOptionExists(NATSConstants.CLIENT_VERIFY)) {
                    initializeKeyStoreAttributes(optionHolder);
                    keyManagers = NATSUtils.createKeyManagers(keyStorePath, storePassword,
                            keyStoreAlgorithm, keyPassword, tlsStoreType);
                }
                natsOptionBuilder.sslContext(NATSUtils.createSSLContext(keyManagers, trustManagers));
            } catch (CertificateException | KeyStoreException | IOException | NoSuchAlgorithmException |
                    UnrecoverableKeyException | KeyManagementException e) {
                throw new SiddhiAppCreationException(siddhiAppName + ": " + streamId + ": Error while " +
                        "creating SslContext. " + e.getMessage(), e);
            }
        } else {
            log.error("Unknown authentication type, siddhi-io-nats only gives support for " +
                    NATSConstants.AUTH_TYPE_LIST + " types. given 'user.auth' type: " + authType + ".");
        }
    }

    private void initializeTrustStoreAttributes(OptionHolder optionHolder) {
        if (optionHolder.isOptionExists(NATSConstants.TRUSTSTORE_FILE)) {
            this.trustStorePath = optionHolder.validateAndGetStaticValue(NATSConstants.TRUSTSTORE_FILE);
        }
        if (optionHolder.isOptionExists(NATSConstants.TRUSTSTORE_PASSWORD)) {
            this.storePassword = optionHolder.validateAndGetStaticValue(NATSConstants.TRUSTSTORE_PASSWORD)
                    .toCharArray();
        }
        if (optionHolder.isOptionExists(NATSConstants.TRUSTSTORE_ALGORITHM)) {
            this.trustStoreAlgorithm = optionHolder.validateAndGetStaticValue(NATSConstants.TRUSTSTORE_ALGORITHM);
        }
        if (optionHolder.isOptionExists(NATSConstants.STORE_TYPE)) {
            this.tlsStoreType = optionHolder.validateAndGetStaticValue(NATSConstants.STORE_TYPE);
        }
    }

    private void initializeKeyStoreAttributes(OptionHolder optionHolder) {
        if (optionHolder.isOptionExists(NATSConstants.KEYSTORE_FILE)) {
            this.keyStorePath = optionHolder.validateAndGetStaticValue(NATSConstants.KEYSTORE_FILE);
        }
        if (optionHolder.isOptionExists(NATSConstants.KEYSTORE_ALGORITHM)) {
            this.keyStoreAlgorithm = optionHolder.validateAndGetStaticValue(NATSConstants.KEYSTORE_ALGORITHM);
        }
        if (optionHolder.isOptionExists(NATSConstants.KEYSTORE_PASSWORD)) {
            this.keyPassword = optionHolder.validateAndGetStaticValue(NATSConstants.KEYSTORE_PASSWORD).toCharArray();
        }
    }

}
