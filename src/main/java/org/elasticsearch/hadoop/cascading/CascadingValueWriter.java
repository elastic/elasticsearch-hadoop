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
package org.elasticsearch.hadoop.cascading;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.mr.WritableValueWriter;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;

import cascading.scheme.SinkCall;
import cascading.tuple.Tuple;

/**
 * Basic delegate around {@link JdkValueWriter} that handles the unwraping of {@link SinkCall}
 */
public class CascadingValueWriter implements ValueWriter<SinkCall<Object[], ?>> {

    private final ValueWriter<Object> jdkWriter;
    private final ValueWriter<Writable> writableWriter;

    public CascadingValueWriter() {
        this(false);
    }

    public CascadingValueWriter(boolean writeUnknownTypes) {
        jdkWriter = new JdkValueWriter(writeUnknownTypes);
        writableWriter = new WritableValueWriter(writeUnknownTypes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean write(SinkCall<Object[], ?> sinkCall, Generator generator) {
        Tuple tuple = CascadingUtils.coerceToString(sinkCall);
        // consider names (in case of aliases these are already applied)
        List<String> names = (List<String>) sinkCall.getContext()[0];

        generator.writeBeginObject();
        for (int i = 0; i < tuple.size(); i++) {
            String name = (i < names.size() ? names.get(i) : "tuple" + i);
            generator.writeFieldName(name);
            Object object = tuple.getObject(i);
            if (!jdkWriter.write(object, generator)) {
                if (!(object instanceof Writable) || !writableWriter.write((Writable) object, generator)) {
                    return false;
                }
            }
        }
        generator.writeEndObject();
        return true;
    }
}
