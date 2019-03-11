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

import java.util.Properties;

import org.elasticsearch.hadoop.handler.ErrorHandler;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * {@link ErrorHandler} subclass that allows for the interception of failures related to bulk write failures.
 */
public abstract class BulkWriteErrorHandler implements IBulkWriteErrorHandler {
    public void init(Properties properties) {}

    public abstract HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector) throws Exception;

    public void close() {}
}
