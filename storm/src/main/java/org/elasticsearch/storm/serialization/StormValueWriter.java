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
package org.elasticsearch.storm.serialization;

import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class StormValueWriter implements ValueWriter<Tuple> {

    private final ValueWriter<Object> jdkWriter;

    public StormValueWriter() {
        this(false);
    }

    public StormValueWriter(boolean writeUnknownTypes) {
        jdkWriter = new JdkValueWriter(writeUnknownTypes);
    }

    @Override
    public boolean write(Tuple tuple, Generator generator) {
        Fields fields = tuple.getFields();

        generator.writeBeginObject();
        for (String field : fields) {
            generator.writeFieldName(field);
            Object value = tuple.getValueByField(field);

            if (value instanceof Tuple) {
                if (!write((Tuple) value, generator)) {
                    return false;
                }
            }

            else if (!jdkWriter.write(value, generator)) {
                return false;
            }
        }
        generator.writeEndObject();
        return true;
    }
}
