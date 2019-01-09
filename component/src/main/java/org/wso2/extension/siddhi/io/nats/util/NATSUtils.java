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

package org.wso2.extension.siddhi.io.nats.util;


import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Random;

/**
 * Contains the utility functions required to the NATS extension.
 */
public class NATSUtils {
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

    public static String createClientId() {
        return new Date().getTime() + "_" + new Random().nextInt(99999) + "_" + new Random().nextInt(99999);
    }
}
