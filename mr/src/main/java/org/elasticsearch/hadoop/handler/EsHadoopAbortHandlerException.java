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

import org.elasticsearch.hadoop.EsHadoopException;

/**
 * Denotes that a handler has decided to abort the job on a potentially retryable value. Instead of displaying the
 * original error as the reason for aborting the operation, the message specified from this exception is used.
 */
public class EsHadoopAbortHandlerException extends EsHadoopException {

    public EsHadoopAbortHandlerException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsHadoopAbortHandlerException(String message) {
        super(message);
    }
}
