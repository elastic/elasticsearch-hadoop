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
package org.elasticsearch.hadoop.integration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

public class QueryTestParams {

    public static Collection<Object[]> jsonParams() {
        return Arrays.asList(new Object[][] {
                // standard
                { "", "" }, // empty
                { "", "?q=m*" }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "", "src/test/resources/org/elasticsearch/hadoop/integration/query.uri" }, // nested uri
                { "", "src/test/resources/org/elasticsearch/hadoop/integration/query.dsl" }, // nested dsl
                // json
                { "json-", "" }, // empty
                { "json-", "?q=m*" }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "json-", "src/test/resources/org/elasticsearch/hadoop/integration/query.uri" }, // nested uri
                { "json-", "src/test/resources/org/elasticsearch/hadoop/integration/query.dsl" } // nested dsl

        });
    }

    public static Collection<Object[]> jsonLocalParams() {
        return Arrays.asList(new Object[][] {
                { "", "" }, // empty
                { "", "?q=m*" }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "", "org/elasticsearch/hadoop/integration/query.uri" }, // nested uri
                { "", "org/elasticsearch/hadoop/integration/query.dsl" }, // nested dsl

                { "json-", "" }, // empty
                { "json-", "?q=m*" }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "json-", "org/elasticsearch/hadoop/integration/query.uri" }, // nested uri
                { "json-", "org/elasticsearch/hadoop/integration/query.dsl" } // nested dsl
                });
    }

    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[][] {
                { "" }, // empty
                { "?q=m*" }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "src/test/resources/org/elasticsearch/hadoop/integration/query.uri" }, // nested uri
                { "src/test/resources/org/elasticsearch/hadoop/integration/query.dsl" } // nested dsl
        });
    }

    public static Collection<Object[]> localParams() {
        return Arrays.asList(new Object[][] {
                { "" }, // empty
                { "?q=m*" }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "org/elasticsearch/hadoop/integration/query.uri" }, // nested uri
                { "org/elasticsearch/hadoop/integration/query.dsl" } // nested dsl
                });
    }

    public static <T extends Configuration> T provisionQueries(T cfg) {
        try {
            DistributedCache.addFileToClassPath(new Path("src/test/resources/org/elasticsearch/hadoop/integration/query.dsl"), cfg);
            DistributedCache.addFileToClassPath(new Path("src/test/resources/org/elasticsearch/hadoop/integration/query.uri"), cfg);
        } catch (IOException ex) {
        }
        return cfg;
    }
}
