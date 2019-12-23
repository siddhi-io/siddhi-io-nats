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
import io.siddhi.query.api.exception.SiddhiAppValidationException;

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

    public static KeyManager[] createKeyManagers(String path, char[] storePassword, String algorithm,
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

    public static TrustManager[] createTrustManagers(String path, char[] storePassword, String turstStoreAlgorithm,
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
            // Due to reported bug under CARBON-14746
            sysPropValue = sysPropValue.replace("\\", "\\\\");
            matcher.appendReplacement(sb, sysPropValue);
        } while (matcher.find());
        matcher.appendTail(sb);
        return sb.toString();
    }

}
