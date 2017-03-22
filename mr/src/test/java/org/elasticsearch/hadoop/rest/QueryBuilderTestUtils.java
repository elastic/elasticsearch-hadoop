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
package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.rest.query.QueryBuilder;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;

public class QueryBuilderTestUtils {
    public static String printQueryBuilder(QueryBuilder builder, boolean addQuery) {
        FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
        Generator generator = new JacksonJsonGenerator(out);
        if (addQuery) {
            generator.writeBeginObject().writeFieldName("query");
        }
        generator.writeBeginObject();
        builder.toJson(generator);
        generator.writeEndObject();
        if (addQuery) {
            generator.writeEndObject();
        }
        generator.close();
        return out.toString();
    }
}
