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

package org.elasticsearch.hadoop.handler;

import java.util.Properties;

/**
 * Handler interface to be notified of and handle recoverable errors during connector execution.
 */
public interface ErrorHandler<I extends Exceptional, O, C extends ErrorCollector<O>> {

    /**
     * Called at the handler creation time to initialize any internal state or resources.
     * @param properties Properties for this handler with handler name prefix stripped away.
     */
    void init(Properties properties);

    /**
     * Called when an exception or failure occurs in a part of the connector.
     * @param entry information about the failure, normally includes operational data and error information
     * @param collector handler for accepting user reactions to the failure, like retrying with modified parameters
     * @return An enum that describes the handler's result, either that it handled the error or if the error should be
     * passed on to the next handler.
     * @throws Exception In the event that the current failure should not be handled, and should halt the connector
     * processing.
     */
    HandlerResult onError(I entry, C collector) throws Exception;

    /**
     * Called at the close of the connector to clean up any internal resources.
     */
    void close();
}
