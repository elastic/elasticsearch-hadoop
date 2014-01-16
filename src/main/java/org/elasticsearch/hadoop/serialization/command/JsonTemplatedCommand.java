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
package org.elasticsearch.hadoop.serialization.command;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.serialization.builder.NoOpValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.serialization.field.JsonFieldExtractors;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;

/**
 * Dedicated JSON command that skips the content generation phase (since the data is already JSON).
 */
class JsonTemplatedCommand extends TemplatedCommand {

    private static Log log = LogFactory.getLog(JsonTemplatedCommand.class);

    private final JsonFieldExtractors jsonExtractors;

    public JsonTemplatedCommand(Collection<Object> beforeObject, Collection<Object> afterObject, JsonFieldExtractors jsonExtractors) {
        super(beforeObject, afterObject, new NoOpValueWriter());
        this.jsonExtractors = jsonExtractors;
    }

    @Override
    protected Object preProcess(Object object, BytesArray storage) {
        // serialize the json early on and copy it to storage
        Assert.notNull(object, "Empty/null JSON document given...");
        Assert.isTrue(object instanceof Writable,
                String.format("Class [%s] not supported; only Hadoop Writables", object.getClass()));

        // handle common cases
        if (object instanceof Text) {
            Text t = (Text) object;
            storage.bytes(t.getBytes(), t.getLength());
        }
        else if (object instanceof BytesWritable) {
            BytesWritable b = (BytesWritable) object;
            storage.bytes(b.getBytes(), b.getLength());
        }
        else {
        // fall-back to generic toString contract
            if (log.isTraceEnabled()) {
                log.trace(String.format("Unknown Writable type for object [%s], using default toString()", object));
            }

            storage.bytes(object.toString());
        }

        if (log.isTraceEnabled()) {
            log.trace(String.format("About to extract information from [%s]", storage));
        }

        jsonExtractors.process(storage);
        return storage;
    }

    @Override
    protected void doWriteObject(Object object, BytesArray storage, ValueWriter<?> writer) {
        // no-op - the object has been already serialized to storage
    }
}
