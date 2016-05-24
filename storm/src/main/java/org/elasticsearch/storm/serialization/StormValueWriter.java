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

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.builder.FilteringValueWriter;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;

public class StormValueWriter extends FilteringValueWriter<Tuple> {

    private final ValueWriter<Object> jdkWriter;

    public StormValueWriter() {
        this(false);
    }

    public StormValueWriter(boolean writeUnknownTypes) {
        jdkWriter = new JdkValueWriter(writeUnknownTypes);
    }

    @Override
    public Result write(Tuple value, Generator generator) {
        return doWrite(value, generator, null);
    }

    protected Result doWrite(Tuple tuple, Generator generator, String parentField) {
        Fields fields = tuple.getFields();

        generator.writeBeginObject();
        for (String field : fields) {
            if (shouldKeep(parentField, field)) {
                generator.writeFieldName(field);
                Object value = tuple.getValueByField(field);

                if (value instanceof Tuple) {
                    Result result = write((Tuple) value, generator);
                    if (!result.isSuccesful()) {
                        return result;
                    }
                }

                else {
                    Result result = jdkWriter.write(value, generator);
                    if (!result.isSuccesful()) {
                        return result;
                    }
                }
            }
        }
        generator.writeEndObject();
        return Result.SUCCESFUL();
    }
}
