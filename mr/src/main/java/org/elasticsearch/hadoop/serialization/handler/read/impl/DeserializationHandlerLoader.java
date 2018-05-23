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

package org.elasticsearch.hadoop.serialization.handler.read.impl;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.handler.impl.AbstractHandlerLoader;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationErrorHandler;

public class DeserializationHandlerLoader extends AbstractHandlerLoader<DeserializationErrorHandler> {

    public static final String ES_READ_DATA_ERROR_HANDLERS = "es.read.data.error.handlers";
    public static final String ES_READ_DATA_ERROR_HANDLER = "es.read.data.error.handler";

    public DeserializationHandlerLoader() {
        super(DeserializationErrorHandler.class);
    }

    @Override
    protected String getHandlersPropertyName() {
        return ES_READ_DATA_ERROR_HANDLERS;
    }

    @Override
    protected String getHandlerPropertyName() {
        return ES_READ_DATA_ERROR_HANDLER;
    }

    @Override
    protected DeserializationErrorHandler loadBuiltInHandler(AbstractHandlerLoader.NamedHandlers handlerName) {
        switch (handlerName) {
            case FAIL:
                return new DeserializationAbortOnFailure();
            case LOG:
                return new DeserializationDropAndLog();
            default:
                throw new EsHadoopIllegalArgumentException(
                        "Could not find default implementation for built in handler type [" + handlerName + "]"
                );
        }
    }
}
