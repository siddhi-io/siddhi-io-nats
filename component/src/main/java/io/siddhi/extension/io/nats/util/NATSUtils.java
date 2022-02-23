/*
 *  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.siddhi.extension.io.nats.util;

import io.nats.client.Options;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Contains the utility functions required to the NATS extension.
 */
public class NATSUtils {

    private static final Pattern varPattern = Pattern.compile("\\$\\{([^}]*)}");
    private static final Logger log = LogManager.getLogger(NATSUtils.class);

    public static void validateNatsUrl(String natsServerUrl, String siddhiStreamName) {
        try {
            URI uri = new URI(natsServerUrl);
            String uriScheme = uri.getScheme();
            if (!uri.getScheme().equals("nats")) {
                throw new URISyntaxException(uri.toString(),
                        "The provided URI scheme '" + uriScheme + "' is invalid; expected 'nats'");
            }
            if (uri.getHost() == null || uri.getPort() == -1) {
                throw new URISyntaxException(uri.toString(),
                        "URI must have host and port parts");
            }
        } catch (URISyntaxException e) {
            throw new SiddhiAppValidationException("Invalid NATS url: " + natsServerUrl + " received for stream: "
                    + siddhiStreamName + ". Expected url format: nats://<host>:<port>", e);
        }
    }

    public static String createClientId(String siddhiAppName, String streamId) {
        return siddhiAppName + "_" + streamId + "_" + new Random().nextInt(99999);
    }

    private static KeyManager[] createKeyManagers(String path, char[] storePassword, String algorithm,
                                                 char[] keyPassword, String storeType) throws IOException,
            KeyStoreException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyStore store = KeyStore.getInstance(storeType);
        path = substituteVariables(path);
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(path))) {
            store.load(in, storePassword);
        }
        KeyManagerFactory factory = KeyManagerFactory.getInstance(algorithm);
        factory.init(store, keyPassword);
        return factory.getKeyManagers();
    }

    private static TrustManager[] createTrustManagers(String path, char[] storePassword, String turstStoreAlgorithm,
                                                     String storeType)
            throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
        KeyStore store = KeyStore.getInstance(storeType);
        path = substituteVariables(path);
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(path))) {
            store.load(in, storePassword);
        }
        TrustManagerFactory factory = TrustManagerFactory.getInstance(turstStoreAlgorithm);
        factory.init(store);
        return factory.getTrustManagers();
    }

    public static SSLContext createSSLContext(KeyManager[] keyManagers, TrustManager[] trustManagers)
            throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext ctx = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
        ctx.init(keyManagers, trustManagers, new SecureRandom());
        return ctx;
    }

    public static void addAuthentication(OptionHolder optionHolder, Options.Builder natsOptionBuilder, String authType,
                                   String siddhiAppName, String streamId) {
        switch (authType.toLowerCase(Locale.ENGLISH)) {
            case "user": {
                char[] username = optionHolder.validateAndGetStaticValue(NATSConstants.USERNAME).toCharArray();
                char[] password = optionHolder.validateAndGetStaticValue(NATSConstants.PASSWORD).toCharArray();
                natsOptionBuilder.userInfo(username, password);
                break;
            }
            case "token": {
                char[] token = optionHolder.validateAndGetStaticValue(NATSConstants.TOKEN).toCharArray();
                natsOptionBuilder.token(token);
                break;
            }
            case "tls": {
                String trustStorePath = optionHolder.validateAndGetStaticValue(
                        NATSConstants.TRUSTSTORE_FILE, NATSConstants.DEFAULT_TRUSTSTORE_PATH);
                char[] storePassword = optionHolder.validateAndGetStaticValue(NATSConstants.TRUSTSTORE_PASSWORD,
                        NATSConstants.DEFAULT_STORE_PASSWORD).toCharArray();
                String trustStoreAlgorithm = optionHolder.validateAndGetStaticValue(NATSConstants.TRUSTSTORE_ALGORITHM,
                        NATSConstants.DEFAULT_TRUSTSTORE_ALGORITHM);
                String tlsStoreType = optionHolder.validateAndGetStaticValue(NATSConstants.STORE_TYPE,
                        NATSConstants.DEFAULT_STORE_TYPE);
                try {
                    TrustManager[] trustManagers = NATSUtils.createTrustManagers(trustStorePath, storePassword,
                            trustStoreAlgorithm, tlsStoreType);
                    KeyManager[] keyManagers = null;
                    if (optionHolder.isOptionExists(NATSConstants.CLIENT_VERIFY)) {
                        String keyStorePath = optionHolder.validateAndGetStaticValue(NATSConstants.KEYSTORE_FILE,
                                NATSConstants.DEFAULT_KEYSTORE_PATH);
                        String keyStoreAlgorithm = optionHolder.validateAndGetStaticValue(
                                NATSConstants.KEYSTORE_ALGORITHM, NATSConstants.DEFAULT_KEYSTORE_ALGORITHM);
                        char[] keyPassword = optionHolder.validateAndGetStaticValue(NATSConstants.KEYSTORE_PASSWORD,
                                NATSConstants.DEFAULT_KEY_PASSWORD).toCharArray();
                        keyManagers = NATSUtils.createKeyManagers(keyStorePath, storePassword,
                                keyStoreAlgorithm, keyPassword, tlsStoreType);
                    }
                    natsOptionBuilder.sslContext(NATSUtils.createSSLContext(keyManagers, trustManagers));
                } catch (CertificateException | KeyStoreException | IOException | NoSuchAlgorithmException |
                        UnrecoverableKeyException | KeyManagementException e) {
                    throw new SiddhiAppCreationException(siddhiAppName + ": Error while " +
                            "creating SslContext for nats sink associated with " + streamId + " stream" +
                            e.getMessage(), e);
                }
                break;
            }
            default: {
                log.warn("Found unknown authentication type, siddhi-io-nats only supports for ['user', 'token'," +
                        " 'tls'] authentication types. Given 'user.auth' type: '" + authType + "'. Hence " +
                        "creating the connection without authentication");
            }
        }

    }

    /**
     * Replace the env variable with the real value.
     */
    public static String substituteVariables(String value) {
        Matcher matcher = varPattern.matcher(value);
        boolean found = matcher.find();
        if (!found) {
            return value;
        }
        StringBuffer sb = new StringBuffer();
        do {
            String sysPropKey = matcher.group(1);
            String sysPropValue = System.getProperty(sysPropKey);
            if (sysPropValue == null || sysPropValue.length() == 0) {
                throw new RuntimeException("System property " + sysPropKey + " is not specified");
            }
            sysPropValue = sysPropValue.replace("\\", "\\\\");
            matcher.appendReplacement(sb, sysPropValue);
        } while (matcher.find());
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static void splitHeaderValues(String optionalConfigs, Properties configProperties) {
        if (optionalConfigs != null && !optionalConfigs.isEmpty()) {
            String[] optionalProperties = optionalConfigs.split(",");
            if (optionalProperties.length > 0) {
                for (String header : optionalProperties) {
                    try {
                        String[] configPropertyWithValue = header.split(":", 2);
                        configProperties.put(configPropertyWithValue[0].trim(), configPropertyWithValue[1].trim());
                    } catch (Exception e) {
                        log.warn("Optional property '" + header + "' is not defined in the correct format.",
                                e);
                    }
                }
            }
        }
    }

}
