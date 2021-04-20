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

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.thirdparty.codehaus.jackson.JsonParser;
import org.elasticsearch.hadoop.thirdparty.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A Query builder which allows building a query given JSON string as input. This is useful when you want
 * to use the Java Builder API but still have JSON query strings at hand that you want to combine with other
 * query builders.
 * <p>
 * Example:
 * <pre>
 * <code>
 *      BoolQueryBuilder bool = new BoolQueryBuilder();
 *      bool.must(new RawQueryBuilder("{\"term\": {\"field\":\"value\"}}", true);
 *      bool.must(new TermQueryBuilder("field2","value2");
 * </code>
 * </pre>
 */
public class RawQueryBuilder extends QueryBuilder {
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    private final String queryString;

    /**
     *
     * @param value the JSON string representing the query
     * @param isQuery true if the JSON string is a query otherwise the string is considered as
     *                a filter (only relevant for elasticsearch version prior to 2.x).
     * @throws IOException if the JSON string is not valid
     */
    public RawQueryBuilder(String value, boolean isQuery) throws IOException {
        this((Map<String, Object>) MAPPER.readValue(value, HashMap.class), isQuery);
    }

    /**
     *
     * @param map A map representation of the query
     * @param isQuery true if the JSON string is a query otherwise the string is considered as
     *                a filter (only relevant for elasticsearch version prior to 2.x).
     * @throws IOException if the JSON string is not valid
     */
    public RawQueryBuilder(Map<String, Object> map, boolean isQuery) throws IOException {
        Object query = map;
        if (isQuery && map.containsKey("query")) {
            query = map.remove("query");
        }
        String raw = MAPPER.writeValueAsString(query);
        int begin = raw.indexOf('{');
        int end = raw.lastIndexOf('}');
        if (begin == -1 || end == -1) {
            throw new EsHadoopIllegalArgumentException("failed to parse query: " + raw);
        }
        this.queryString = raw.substring(begin+1, end);
    }

    @Override
    public void toJson(Generator out) {
        out.writeRaw(queryString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RawQueryBuilder that = (RawQueryBuilder) o;

        return queryString != null ? queryString.equals(that.queryString) : that.queryString == null;
    }

    @Override
    public int hashCode() {
        return queryString.hashCode();
    }
}
