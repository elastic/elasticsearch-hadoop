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
 * A query that parses a query string.
 */
public class QueryStringQueryBuilder extends QueryBuilder {
    private String query;
    private String defaultField;
    private String analyzer;
    private Boolean lowercaseExpandedTerms;
    private Boolean analyzeWildcard;
    private String defaultOperator;
    private Boolean lenient;

    public QueryStringQueryBuilder query(String value) {
        if (value == null) {
            throw new IllegalArgumentException("inner clause [query] cannot be null");
        }
        this.query = value;
        return this;
    }

    public QueryStringQueryBuilder defaultField(String defaultField) {
        this.defaultField = defaultField;
        return this;
    }

    public QueryStringQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public QueryStringQueryBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        return this;
    }

    public QueryStringQueryBuilder analyzeWildcard(boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
        return this;
    }

    public QueryStringQueryBuilder defaultOperator(String defaultOperator) {
        this.defaultOperator = defaultOperator;
        return this;
    }

    @Override
    public void toJson(Generator out) {
        if (query == null) {
            throw new IllegalArgumentException("inner clause [query] cannot be null");
        }
        out.writeFieldName("query_string");
        out.writeBeginObject();
        out.writeFieldName("query");
        out.writeString(query);
        if (defaultField != null) {
            out.writeFieldName("default_field");
            out.writeString(defaultField);
        }
        if (analyzer != null) {
            out.writeFieldName("analyzer");
            out.writeString(analyzer);
        }
        if (lowercaseExpandedTerms != null) {
            out.writeFieldName("lowercaseExpandedTerms");
            out.writeBoolean(lowercaseExpandedTerms);
        }
        if (analyzeWildcard != null) {
            out.writeFieldName("analyzeWildcard");
            out.writeBoolean(analyzeWildcard);
        }
        if (defaultOperator != null) {
            out.writeFieldName("defaultOperator");
            out.writeString(defaultOperator);
        }
        if (lenient != null) {
            out.writeFieldName("lenient");
            out.writeBoolean(lenient);
        }
        out.writeEndObject();
    }

    public void lenient(boolean lenient) {
        this.lenient = lenient;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryStringQueryBuilder that = (QueryStringQueryBuilder) o;

        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        if (defaultField != null ? !defaultField.equals(that.defaultField) : that.defaultField != null) return false;
        if (analyzer != null ? !analyzer.equals(that.analyzer) : that.analyzer != null) return false;
        if (lowercaseExpandedTerms != null ? !lowercaseExpandedTerms.equals(that.lowercaseExpandedTerms) : that.lowercaseExpandedTerms != null)
            return false;
        if (analyzeWildcard != null ? !analyzeWildcard.equals(that.analyzeWildcard) : that.analyzeWildcard != null)
            return false;
        if (defaultOperator != null ? !defaultOperator.equals(that.defaultOperator) : that.defaultOperator != null)
            return false;
        return lenient != null ? lenient.equals(that.lenient) : that.lenient == null;

    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (defaultField != null ? defaultField.hashCode() : 0);
        result = 31 * result + (analyzer != null ? analyzer.hashCode() : 0);
        result = 31 * result + (lowercaseExpandedTerms != null ? lowercaseExpandedTerms.hashCode() : 0);
        result = 31 * result + (analyzeWildcard != null ? analyzeWildcard.hashCode() : 0);
        result = 31 * result + (defaultOperator != null ? defaultOperator.hashCode() : 0);
        result = 31 * result + (lenient != null ? lenient.hashCode() : 0);
        return result;
    }
}
