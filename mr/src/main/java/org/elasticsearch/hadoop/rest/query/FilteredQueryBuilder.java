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
 * A query that applies a filter to the results of another query.
 */
public class FilteredQueryBuilder extends QueryBuilder {
    private QueryBuilder query;
    private final List<QueryBuilder> filters = new ArrayList<QueryBuilder>();

    public FilteredQueryBuilder query(QueryBuilder value) {
        if (value == null) {
            throw new IllegalArgumentException("inner clause [query] cannot be null.");
        }
        this.query = value;
        return this;
    }

    public FilteredQueryBuilder filters(Collection<QueryBuilder> values) {
        this.filters.addAll(values);
        return this;
    }

    @Override
    public void toJson(Generator out) {
        if (query == null) {
            throw new IllegalArgumentException("inner clause [query] cannot be null.");
        }
        out.writeFieldName("filtered");
        out.writeBeginObject();
        out.writeFieldName("query");
        out.writeBeginObject();
        query.toJson(out);
        out.writeEndObject();
        if (filters.isEmpty() == false) {
            out.writeFieldName("filter");
            out.writeBeginObject();
            if (filters.size() == 1) {
                filters.get(0).toJson(out);
            } else {
                BoolQueryBuilder inner = new BoolQueryBuilder();
                for (QueryBuilder filter : filters) {
                    inner.must(filter);
                }
                inner.toJson(out);
            }
            out.writeEndObject();
        }
        out.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FilteredQueryBuilder that = (FilteredQueryBuilder) o;

        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        return filters != null ? filters.equals(that.filters) : that.filters == null;

    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (filters != null ? filters.hashCode() : 0);
        return result;
    }
}
