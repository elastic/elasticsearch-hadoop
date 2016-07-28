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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.hadoop.rest.query.MatchAllQueryBuilder.MATCH_ALL;

public class RandomQueryBuilders {
    static final int MAX_LEVEL = 3;
    static QueryBuilder randomQuery(Random rand) {
        return randomQuery(rand, 0);
    }

    static QueryBuilder randomQuery(Random rand, int level) {
        if (level > MAX_LEVEL) {
            return MATCH_ALL;
        }
        int next = rand.nextInt(7);
        switch (next) {
            case 0:
                return randomBoolQuery(rand, level);
            case 1:
                return randomFilteredQuery(rand, level);
            case 2:
                return randomMatchAllQuery(rand, level);
            case 3:
                return randomQueryStringQuery(rand, level);
            case 4:
                return randomRawQueryStringQuery(rand, level);
            case 5:
                return randomTermQuery(rand, level);
            case 6:
                return randomConstantScoreQuery(rand, level);
            default:
                throw new IllegalArgumentException();
        }
    }

    static QueryBuilder randomTermQuery(Random rand, int level) {
        TermQueryBuilder termQuery = new TermQueryBuilder();
        termQuery.field(Integer.toString(rand.nextInt()));
        termQuery.term(Integer.toString(rand.nextInt()));
        return termQuery;
    }

    static QueryBuilder randomFilteredQuery(Random rand, int level) {
        FilteredQueryBuilder query = new FilteredQueryBuilder();
        query.query(randomQuery(rand, level+1));
        List<QueryBuilder> filters = new ArrayList<QueryBuilder> ();
        int numFilters = rand.nextInt(3);
        for (int i = 0; i < numFilters; i++) {
            filters.add(randomQuery(rand, level+1));
        }
        query.filters(filters);
        return query;
    }

    static QueryBuilder randomMatchAllQuery(Random rand, int level) {
        return MATCH_ALL;
    }

    static QueryBuilder randomQueryStringQuery(Random rand, int level) {
        QueryStringQueryBuilder query = new QueryStringQueryBuilder();
        query.lenient(rand.nextBoolean());
        query.analyzeWildcard(rand.nextBoolean());
        query.query(Integer.toString(rand.nextInt()));
        query.analyzer(Integer.toString(rand.nextInt()));
        query.defaultOperator(rand.nextBoolean() ? "AND" : "OR");
        return query;
    }

    static QueryBuilder randomRawQueryStringQuery(Random rand, int level) {
        QueryBuilder query = randomQuery(rand);
        FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
        Generator generator = new JacksonJsonGenerator(out);
        generator.writeBeginObject();
        query.toJson(generator);
        generator.writeEndObject();
        generator.close();
        try {
            return new RawQueryBuilder(out.toString().trim(), false);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse raw query", e);
        }
    }

    static QueryBuilder randomBoolQuery(Random rand, int level) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        int numMust = rand.nextInt(3);
        for (int i = 0; i < numMust; i++) {
            query.must(randomQuery(rand, level+1));
        }
        int numShould = rand.nextInt(3);
        for (int i = 0; i < numShould; i++) {
            query.should(randomQuery(rand, level+1));
        }
        int numFilter = rand.nextInt(3);
        for (int i = 0; i < numFilter; i++) {
            query.filter(randomQuery(rand, level+1));
        }
        int numMustNot = rand.nextInt(3);
        for (int i = 0; i < numMustNot; i++) {
            query.mustNot(randomQuery(rand, level+1));
        }
        return query;
    }

    static QueryBuilder randomConstantScoreQuery(Random rand, int level) {
        return new ConstantScoreQueryBuilder().filter(randomQuery(rand, level+1)).boost(rand.nextFloat());
    }
}
