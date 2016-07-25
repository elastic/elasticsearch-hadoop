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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A Query that matches documents matching boolean combinations of other queries.
 */
public class BoolQueryBuilder extends QueryBuilder {
    private final List<QueryBuilder> mustClauses = new ArrayList<QueryBuilder> ();
    private final List<QueryBuilder> mustNotClauses = new ArrayList<QueryBuilder> ();
    private final List<QueryBuilder> filterClauses = new ArrayList<QueryBuilder> ();
    private final List<QueryBuilder> shouldClauses = new ArrayList<QueryBuilder> ();

    public BoolQueryBuilder should(QueryBuilder query) {
        shouldClauses.add(query);
        return this;
    }

    public BoolQueryBuilder filter(QueryBuilder filter) {
        filterClauses.add(filter);
        return this;
    }

    public BoolQueryBuilder filters(Collection<QueryBuilder> filters) {
        filterClauses.addAll(filters);
        return this;
    }

    public BoolQueryBuilder must(QueryBuilder query) {
        mustClauses.add(query);
        return this;
    }

    public BoolQueryBuilder mustNot(QueryBuilder query) {
        mustNotClauses.add(query);
        return this;
    }

    @Override
    public void toJson(Generator out) {
        out.writeFieldName("bool");
        out.writeBeginObject();
        if (mustClauses.size() > 0) {
            out.writeFieldName("must");
            out.writeBeginArray();
            for (QueryBuilder innerQuery : mustClauses) {
                out.writeBeginObject();
                innerQuery.toJson(out);
                out.writeEndObject();
            }
            out.writeEndArray();
        }

        if (shouldClauses.size() > 0) {
            out.writeFieldName("should");
            out.writeBeginArray();
            for (QueryBuilder innerQuery : shouldClauses) {
                out.writeBeginObject();
                innerQuery.toJson(out);
                out.writeEndObject();
            }
            out.writeEndArray();
        }

        if (filterClauses.size() > 0) {
            out.writeFieldName("filter");
            out.writeBeginArray();
            for (QueryBuilder innerQuery : filterClauses) {
                out.writeBeginObject();
                innerQuery.toJson(out);
                out.writeEndObject();
            }
            out.writeEndArray();
        }

        if (mustNotClauses.size() > 0) {
            out.writeFieldName("must_not");
            out.writeBeginArray();
            for (QueryBuilder innerQuery : mustNotClauses) {
                out.writeBeginObject();
                innerQuery.toJson(out);
                out.writeEndObject();
            }
            out.writeEndArray();
        }
        out.writeEndObject();
    }
}