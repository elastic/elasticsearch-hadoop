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
 * Values that denote the result of an action taken by an {@link ErrorHandler}.
 */
public enum HandlerResult {

    /**
     * Signals to the connector that the given {@link ErrorHandler} was successfully able to handle the failure
     * scenario, either by swallowing the failure, persisting the operation for examination, or attempting to
     * retry the operation. In this case, the handling for this failure instance is stopped and no further handlers
     * in the chain will be called.
     */
    HANDLED,

    /**
     * Signals to the connector that the given {@link ErrorHandler} was unable to handle the failure scenario.
     * In this case, the failure information is passed on to the next configured handler in the chain.
     */
    PASS,

    /**
     * Signals to the connector that the given {@link ErrorHandler} determined that the given failure scenario
     * is grounds to abort the job. In this case, the original error information will be extracted and thrown
     * with no further handlers in the chain being called.
     */
    ABORT
}
