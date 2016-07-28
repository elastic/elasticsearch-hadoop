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
package org.elasticsearch.hadoop.rest.query;


import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;

/**
 * QueryBuilder for Elasticsearch query DSL
 */
public abstract class QueryBuilder {
    /**
     * Converts this QueryBuilder to a JSON string.
     *
     * @param out The JSON generator
     * @return The JSON string which represents the query
     */
    public abstract void toJson(Generator out);

    @Override
    public String toString() {
        FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
        JacksonJsonGenerator generator = new JacksonJsonGenerator(out);
        generator.writeBeginObject();
        toJson(generator);
        generator.writeEndObject();
        generator.close();
        return out.toString();
    }
}
