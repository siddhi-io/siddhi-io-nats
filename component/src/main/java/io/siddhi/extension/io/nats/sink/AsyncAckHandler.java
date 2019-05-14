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

package io.siddhi.extension.io.nats.sink;

import io.nats.streaming.AckHandler;
import org.apache.log4j.Logger;

/**
 * Handle the acknowledgement for the published messages in an asynchronous manner.
 */
public class AsyncAckHandler implements AckHandler {
    private static final Logger log = Logger.getLogger(AsyncAckHandler.class);

    @Override
    public void onAck(String nuid, Exception exception) {
        if (exception != null) {
            log.error("Error publishing msg id " + nuid + " : " + exception.getMessage());
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Received ack for msg id " + nuid);
            }
        }
    }
}

