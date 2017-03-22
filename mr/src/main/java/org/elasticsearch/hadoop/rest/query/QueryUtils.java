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
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.regex.Regex;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class QueryUtils {
    public static QueryBuilder parseQuery(Settings settings) {
        String query = settings.getQuery();
        if (!StringUtils.hasText(query)) {
            return MatchAllQueryBuilder.MATCH_ALL;
        }
        query = query.trim();
        if (query.startsWith("?") == false && query.startsWith("{") == false) {
            try {
                // must be a resource
                InputStream in = settings.loadResource(query);
                if (in == null) {
                    throw new IOException();
                }
                query = IOUtils.asString(in);
            } catch (IOException ex) {
                throw new EsHadoopIllegalArgumentException(
                        String.format(
                                "Cannot determine specified query - doesn't appear to be URI or JSON based and location [%s] cannot be opened",
                                query));
            }
        }
        try {
            return SimpleQueryParser.parse(query, true);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse query: " + query, e);
        }
    }

    public static List<QueryBuilder> parseFilters(Settings settings) {
        String[] rawFilters = SettingsUtils.getFilters(settings);
        if (rawFilters == null) {
            return Collections.emptyList();
        }
        List<QueryBuilder> filters = new ArrayList<QueryBuilder>();
        for (String filter : rawFilters) {
            filter = filter.trim();
            try {
                filters.add(SimpleQueryParser.parse(filter, false));
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to parse filter: " + filter, e);
            }
        }
        return filters;
    }

    public static QueryBuilder parseQueryAndFilters(Settings settings) {
        QueryBuilder query = QueryUtils.parseQuery(settings);
        List<QueryBuilder> filters = QueryUtils.parseFilters(settings);
        if (filters == null || filters.isEmpty()) {
            return query;
        }
        return new BoolQueryBuilder().must(query).filters(filters);
    }

    /**
     * Checks if the provided candidate is explicitly contained in the provided indices.
     */
    public static boolean isExplicitlyRequested(String candidate, String... indices) {
        boolean result = false;
        for (String indexOrAlias : indices) {
            boolean include = true;
            if (indexOrAlias.charAt(0) == '+' || indexOrAlias.charAt(0) == '-') {
                include = indexOrAlias.charAt(0) == '+';
                indexOrAlias = indexOrAlias.substring(1);
            }
            if (indexOrAlias.equals("*") || indexOrAlias.equals("_all")) {
                return false;
            }
            if (Regex.isSimpleMatchPattern(indexOrAlias)) {
                if (Regex.simpleMatch(indexOrAlias, candidate)) {
                    if (include) {
                        result = true;
                    } else {
                        return false;
                    }
                }
            } else {
                if (candidate.equals(indexOrAlias)) {
                    if (include) {
                        result = true;
                    } else {
                        return false;
                    }
                }
            }
        }
        return result;
    }
}
