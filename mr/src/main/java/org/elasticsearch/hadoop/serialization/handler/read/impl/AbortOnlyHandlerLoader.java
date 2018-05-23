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

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.hadoop.serialization.handler.read.IDeserializationErrorHandler;

/**
 * A handler loader that ignores the configured error handlers and only loads the abort handler.
 */
public class AbortOnlyHandlerLoader extends DeserializationHandlerLoader {

    @Override
    public List<IDeserializationErrorHandler> loadHandlers() {
        List<IDeserializationErrorHandler> handlers = new ArrayList<IDeserializationErrorHandler>();
        handlers.add(loadBuiltInHandler(NamedHandlers.FAIL));
        return handlers;
    }
}
