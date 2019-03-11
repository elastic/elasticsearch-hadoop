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

/**
 * An interface for accepting modified information to be retried in the event of a recoverable failure.
 */
public interface ErrorCollector<T> {

    /**
     * Retry an operation using the same data from the previous operation.
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult retry();

    /**
     * Retry an operation using the given retry data
     * @param retryData operation/data to retry
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult retry(T retryData);

    /**
     * Signal that a failure could not be handled by this handler and pass the record on to the next handler.
     * An optional string can be provided explaining the reason for passing on handling the error.
     * @param reason optional reason for passing on data. Reasons are used in the final report if an error could not be
     *               handled.
     * @return Appropriate handler result value stating the failure should be passed on to the next handler. In
     * most cases this is {@link HandlerResult#PASS} but can vary depending on the use case.
     */
    public HandlerResult pass(String reason);

}
