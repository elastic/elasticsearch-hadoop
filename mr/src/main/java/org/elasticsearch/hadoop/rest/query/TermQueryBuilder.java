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
 * A Query that matches documents containing a term.
 */
public class TermQueryBuilder extends QueryBuilder {
    /** Name of field to match against. */
    private String field;
    /** Value to find matches for. */
    private String term;

    public TermQueryBuilder field(String value) {
        if (value == null) {
            throw new IllegalArgumentException("inner clause [field] cannot be null");
        }
        this.field = value;
        return this;
    }

    public TermQueryBuilder term(String value) {
        if (value == null) {
            throw new IllegalArgumentException("inner clause [term] cannot be null");
        }
        this.term = value;
        return this;
    }

    @Override
    public void toJson(Generator out) {
        if (field == null) {
            throw new IllegalArgumentException("inner clause [field] cannot be null");
        }
        if (term == null) {
            throw new IllegalArgumentException("inner clause [term] cannot be null");
        }
        out.writeFieldName("term")
                .writeBeginObject()
                    .writeFieldName(field)
                    .writeString(term)
                .writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TermQueryBuilder that = (TermQueryBuilder) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        return term != null ? term.equals(that.term) : that.term == null;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (term != null ? term.hashCode() : 0);
        return result;
    }
}
