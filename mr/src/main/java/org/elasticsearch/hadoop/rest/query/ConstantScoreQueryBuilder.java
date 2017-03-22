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

/**
 * A query that wraps a filter and simply returns a constant score equal to the
 * query boost for every document in the filter.
 */
public class ConstantScoreQueryBuilder extends QueryBuilder {
    public static final float DEFAULT_BOOST = 1.0f;
    private QueryBuilder filter;
    private float boost = DEFAULT_BOOST;

    public ConstantScoreQueryBuilder filter(QueryBuilder value) {
        if (value == null) {
            throw new IllegalArgumentException("inner clause [filter] cannot be null.");
        }
        this.filter = value;
        return this;
    }

    public ConstantScoreQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConstantScoreQueryBuilder that = (ConstantScoreQueryBuilder) o;

        if (Float.compare(that.boost, boost) != 0) return false;
        return filter != null ? filter.equals(that.filter) : that.filter == null;

    }

    @Override
    public int hashCode() {
        int result = filter != null ? filter.hashCode() : 0;
        result = 31 * result + (boost != +0.0f ? Float.floatToIntBits(boost) : 0);
        return result;
    }

    @Override
    public void toJson(Generator out) {
        if (filter == null) {
            throw new IllegalArgumentException("inner clause [filter] cannot be null.");
        }
        out.writeFieldName("constant_score");
        out.writeBeginObject();
        out.writeFieldName("filter");
        out.writeBeginObject();
        filter.toJson(out);
        out.writeEndObject();
        out.writeFieldName("boost");
        out.writeNumber(boost);
        out.writeEndObject();
    }
}
