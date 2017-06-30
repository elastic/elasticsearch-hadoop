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
package org.elasticsearch.hadoop.rest.request;

import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.serialization.dto.IndicesAliases;
import org.elasticsearch.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GetAliasesRequestBuilder extends RequestBuilder<GetAliasesRequestBuilder.Response> {
    private final List<String> indices = new ArrayList<String>();

    public GetAliasesRequestBuilder(RestClient client) {
        super(client);
    }

    public GetAliasesRequestBuilder indices(String... values) {
        Collections.addAll(indices, values);
        return this;
    }

    @Override
    public Response execute() {
        StringBuilder path = new StringBuilder();
        if (indices.size() > 0) {
            path.append(StringUtils.concatenate(indices));
        } else {
            path.append("_all");
        }
        path.append("/_alias");
        return new Response((Map<String, Object>) client.get(path.toString(), null));
    }

    public static class Response implements RequestBuilder.Response {
        private final IndicesAliases indicesAliases;

        public Response(Map<String, Object> map) {
            this.indicesAliases = IndicesAliases.parse(map);
        }

        public IndicesAliases getIndices() {
            return indicesAliases;
        }
    }
}
