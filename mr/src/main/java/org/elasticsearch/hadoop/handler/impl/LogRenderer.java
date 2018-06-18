/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.hadoop.handler.impl;

import java.util.List;

import org.elasticsearch.hadoop.handler.Exceptional;

public abstract class LogRenderer<I extends Exceptional> {

    public String renderLog(I entry) {
        // Render the previous handler messages
        List<String> handlerMessages = entry.previousHandlerMessages();
        String tailMessage;
        if (!handlerMessages.isEmpty()) {
            StringBuilder tail = new StringBuilder("Previous handler messages:");
            for (String handlerMessage : handlerMessages) {
                tail.append("\n\t").append(handlerMessage);
            }
            tailMessage = tail.toString();
        } else {
            tailMessage = "";
        }

        String logData = convert(entry);

        // Log the failure
        return logData + "\n" + tailMessage;
    }

    public abstract String convert(I entry);
}
