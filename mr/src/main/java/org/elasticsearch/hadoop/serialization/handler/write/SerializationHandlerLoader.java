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

package org.elasticsearch.hadoop.serialization.handler.write;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.handler.AbstractHandlerLoader;

public class SerializationHandlerLoader extends AbstractHandlerLoader<SerializationErrorHandler> {

    public static final String ES_WRITE_DATA_ERROR_HANDLERS = "";
    public static final String ES_WRITE_DATA_ERROR_HANDLER = "";

    public SerializationHandlerLoader() {
        super(SerializationErrorHandler.class);
    }

    @Override
    protected String getHandlersPropertyName() {
        return ES_WRITE_DATA_ERROR_HANDLERS;
    }

    @Override
    protected String getHandlerPropertyName() {
        return ES_WRITE_DATA_ERROR_HANDLER;
    }

    @Override
    protected SerializationErrorHandler loadBuiltInHandler(NamedHandlers handlerName) {
        switch (handlerName) {
            case FAIL:
                return null; // TODO
            case LOG:
                return null; // TODO
            default:
                throw new EsHadoopIllegalArgumentException(
                        "Could not find default implementation for built in handler type [" + handlerName + "]"
                );
        }
    }
}
