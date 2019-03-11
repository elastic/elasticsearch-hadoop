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

package org.elasticsearch.hadoop.rest.bulk.handler;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * Any {@link ErrorCollector} instance that allows for a backoff period before executing a retry.
 */
public interface DelayableErrorCollector<T> extends ErrorCollector<T> {

    /**
     * Retry an operation (after the given backoff period) using the same data from the previous operation. Documents
     * retried through this method will be retried at the longest specified backoff time across all retries from its
     * bulk request.
     * @param timeAmount amount of time units to wait before retrying this document
     * @param timeUnits time units to measure backoff period in
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult backoffAndRetry(long timeAmount, TimeUnit timeUnits);

    /**
     * Retry an operation (after the given backoff period) using the given retry data. Documents retried through
     * this method will be retried at the longest specified backoff time across all retries from its bulk request.
     * @param retryData operation/data to retry
     * @param timeAmount amount of time units to wait before retrying this document
     * @param timeUnits time units to measure backoff period in
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult backoffAndRetry(T retryData, long timeAmount, TimeUnit timeUnits);
}
